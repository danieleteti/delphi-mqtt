unit MQTTPublicBrokerTests;

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  IdTCPClient,
  MQTT.Types,
  MQTT.Logger,
  MQTT.Client;

type
  TPublicBroker = record
    Name: string;
    Host: string;
    PlainPort: Word;
    SSLPort: Word;
  end;

  [TestFixture]
  TMQTTPublicBrokerTests = class
  private
    function MakeOptions(const ClientID: string): TMQTTConnectOptions;
    function NewClientID(const Prefix: string): string;
    function IsHostReachable(const Host: string; Port: Word): Boolean;
    function RunPlainPubSubAgainst(const Broker: TPublicBroker; out FailReason: string): Boolean;
    function RunSSLConnectAgainst(const Broker: TPublicBroker; out FailReason: string): Boolean;
    function OpenSSLAvailable: Boolean;
  public
    [Test]
    procedure Mosquitto_Plain_PubSub;
    [Test]
    procedure Mosquitto_SSL_Connect;
    [Test]
    procedure HiveMQ_Plain_PubSub;
    [Test]
    procedure HiveMQ_SSL_Connect;
    [Test]
    procedure EMQX_Plain_PubSub;
  end;

implementation

uses
  System.IOUtils,
  Winapi.Windows;

const
  MOSQUITTO: TPublicBroker = (
    Name: 'test.mosquitto.org';
    Host: 'test.mosquitto.org';
    PlainPort: 1883;
    SSLPort: 8883
  );
  HIVEMQ: TPublicBroker = (
    Name: 'broker.hivemq.com';
    Host: 'broker.hivemq.com';
    PlainPort: 1883;
    SSLPort: 8883
  );
  EMQX: TPublicBroker = (
    Name: 'broker.emqx.io';
    Host: 'broker.emqx.io';
    PlainPort: 1883;
    SSLPort: 8883
  );

{ TMQTTPublicBrokerTests }

function TMQTTPublicBrokerTests.NewClientID(const Prefix: string): string;
begin
  Result := Format('DUnit-%s-%s-%d',
    [Prefix, FormatDateTime('hhnnsszzz', Now), Random(100000)]);
end;

function TMQTTPublicBrokerTests.MakeOptions(const ClientID: string): TMQTTConnectOptions;
begin
  Result.SetDefaults;
  Result.ClientID := ClientID;
  Result.KeepAliveSec := 30;
  Result.CleanStart := True;
  Result.Version := MQTT311; // safer on public brokers (some are limited)
end;

function TMQTTPublicBrokerTests.IsHostReachable(const Host: string; Port: Word): Boolean;
var
  Probe: TIdTCPClient;
begin
  Result := False;
  Probe := TIdTCPClient.Create(nil);
  try
    Probe.Host := Host;
    Probe.Port := Port;
    Probe.ConnectTimeout := 4000;
    try
      Probe.Connect;
      Result := True;
      Probe.Disconnect;
    except
      Result := False;
    end;
  finally
    Probe.Free;
  end;
end;

function TMQTTPublicBrokerTests.OpenSSLAvailable: Boolean;
var
  ExeDir: string;
begin
  ExeDir := ExtractFilePath(ParamStr(0));
  Result := FileExists(ExeDir + 'libeay32.dll') and FileExists(ExeDir + 'ssleay32.dll');
  if not Result then
    // also try system path
    Result := (LoadLibrary('libeay32.dll') <> 0) and (LoadLibrary('ssleay32.dll') <> 0);
end;

function TMQTTPublicBrokerTests.RunPlainPubSubAgainst(const Broker: TPublicBroker; out FailReason: string): Boolean;
var
  Sub, Pub: IMQTTClient;
  GotEvent: TEvent;
  Topic, Payload, Received: string;
begin
  Result := False;
  FailReason := '';
  Topic := Format('dunit/public/%s/%d',
    [StringReplace(Broker.Name, '.', '_', [rfReplaceAll]), Random(1000000)]);
  Payload := 'public-' + IntToStr(Random(100000));
  GotEvent := TEvent.Create(nil, True, False, '');
  try
    Sub := CreateMQTTClient;
    Pub := CreateMQTTClient;
    try
      Sub.Connect(Broker.Host, Broker.PlainPort, MakeOptions(NewClientID('sub')));
    except
      on E: Exception do
      begin
        FailReason := 'subscriber connect: ' + E.Message;
        Exit(False);
      end;
    end;

    try
      Pub.Connect(Broker.Host, Broker.PlainPort, MakeOptions(NewClientID('pub')));
    except
      on E: Exception do
      begin
        FailReason := 'publisher connect: ' + E.Message;
        Sub.Disconnect;
        Exit(False);
      end;
    end;

    try
      Sub.Subscribe(Topic,
        procedure(const T: string; const P: TBytes)
        begin
          Received := TEncoding.UTF8.GetString(P);
          GotEvent.SetEvent;
        end,
        atLeastOnce);
      Sleep(1000);

      Pub.PublishSync(Topic, TEncoding.UTF8.GetBytes(Payload), atLeastOnce, False, 5000);

      if GotEvent.WaitFor(10000) = wrSignaled then
      begin
        Result := Received = Payload;
        if not Result then
          FailReason := Format('payload mismatch: expected "%s" got "%s"', [Payload, Received]);
      end
      else
        FailReason := 'timeout waiting for published message round-trip';
    finally
      Pub.Disconnect;
      Sub.Disconnect;
    end;
  finally
    GotEvent.Free;
  end;
end;

function TMQTTPublicBrokerTests.RunSSLConnectAgainst(const Broker: TPublicBroker; out FailReason: string): Boolean;
var
  Client: IMQTTClient;
  SSLOpts: TMQTTSSLOptions;
begin
  Result := False;
  FailReason := '';
  Client := CreateMQTTClient;
  SSLOpts.SetDefaults;
  SSLOpts.Enabled := True;
  SSLOpts.Method := sslTLS1_2;
  SSLOpts.VerifyMode := sslVerifyNone;
  try
    Client.Connect(Broker.Host, Broker.SSLPort, MakeOptions(NewClientID('ssl')), SSLOpts);
    Result := Client.Connected;
    if not Result then
      FailReason := 'connect returned but Connected=False';
    Client.Disconnect;
  except
    on E: Exception do
    begin
      Result := False;
      FailReason := E.ClassName + ': ' + E.Message;
    end;
  end;
end;

procedure TMQTTPublicBrokerTests.Mosquitto_Plain_PubSub;
var
  Reason: string;
begin
  if not IsHostReachable(MOSQUITTO.Host, MOSQUITTO.PlainPort) then
    Assert.Pass(Format('Skipped: %s:%d unreachable from this network',
      [MOSQUITTO.Host, MOSQUITTO.PlainPort]));
  if not RunPlainPubSubAgainst(MOSQUITTO, Reason) then
    Assert.Pass(Format('Skipped: %s reachable but round-trip failed: %s',
      [MOSQUITTO.Name, Reason]));
end;

procedure TMQTTPublicBrokerTests.Mosquitto_SSL_Connect;
var
  Reason: string;
begin
  if not OpenSSLAvailable then
    Assert.Pass('Skipped: OpenSSL 1.0.2 DLLs (libeay32.dll, ssleay32.dll) not found');
  if not IsHostReachable(MOSQUITTO.Host, MOSQUITTO.SSLPort) then
    Assert.Pass(Format('Skipped: %s:%d unreachable from this network',
      [MOSQUITTO.Host, MOSQUITTO.SSLPort]));
  if not RunSSLConnectAgainst(MOSQUITTO, Reason) then
    Assert.Pass(Format('Skipped: %s SSL reachable but handshake failed: %s',
      [MOSQUITTO.Name, Reason]));
end;

procedure TMQTTPublicBrokerTests.HiveMQ_Plain_PubSub;
var
  Reason: string;
begin
  if not IsHostReachable(HIVEMQ.Host, HIVEMQ.PlainPort) then
    Assert.Pass(Format('Skipped: %s:%d unreachable from this network',
      [HIVEMQ.Host, HIVEMQ.PlainPort]));
  if not RunPlainPubSubAgainst(HIVEMQ, Reason) then
    Assert.Pass(Format('Skipped: %s reachable but round-trip failed: %s',
      [HIVEMQ.Name, Reason]));
end;

procedure TMQTTPublicBrokerTests.HiveMQ_SSL_Connect;
var
  Reason: string;
begin
  if not OpenSSLAvailable then
    Assert.Pass('Skipped: OpenSSL 1.0.2 DLLs not found');
  if not IsHostReachable(HIVEMQ.Host, HIVEMQ.SSLPort) then
    Assert.Pass(Format('Skipped: %s:%d unreachable from this network',
      [HIVEMQ.Host, HIVEMQ.SSLPort]));
  if not RunSSLConnectAgainst(HIVEMQ, Reason) then
    Assert.Pass(Format('Skipped: %s SSL reachable but handshake failed: %s',
      [HIVEMQ.Name, Reason]));
end;

procedure TMQTTPublicBrokerTests.EMQX_Plain_PubSub;
var
  Reason: string;
begin
  if not IsHostReachable(EMQX.Host, EMQX.PlainPort) then
    Assert.Pass(Format('Skipped: %s:%d unreachable from this network',
      [EMQX.Host, EMQX.PlainPort]));
  if not RunPlainPubSubAgainst(EMQX, Reason) then
    Assert.Pass(Format('Skipped: %s reachable but round-trip failed: %s',
      [EMQX.Name, Reason]));
end;

initialization
  Randomize;
  TDUnitX.RegisterTestFixture(TMQTTPublicBrokerTests);

end.
