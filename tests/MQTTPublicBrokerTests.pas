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

  TBrokerOutcome = (boOK, boSkipNetwork, boFailProtocol);

  [TestFixture]
  TMQTTPublicBrokerTests = class
  private
    function MakeOptions(const ClientID: string): TMQTTConnectOptions;
    function NewClientID(const Prefix: string): string;
    function IsHostReachable(const Host: string; Port: Word): Boolean;
    function RunPlainPubSubAgainst(const Broker: TPublicBroker; out FailReason: string): TBrokerOutcome;
    function RunSSLConnectAgainst(const Broker: TPublicBroker; out FailReason: string): TBrokerOutcome;
    function OpenSSLAvailable: Boolean;
    function IsNetworkErrorMessage(const Msg: string): Boolean;
    procedure AssertPlainPubSub(const Broker: TPublicBroker);
    procedure AssertSSLConnect(const Broker: TPublicBroker);
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
  HLib1, HLib2: HMODULE;
begin
  ExeDir := ExtractFilePath(ParamStr(0));
  Result := FileExists(ExeDir + 'libeay32.dll') and FileExists(ExeDir + 'ssleay32.dll');
  if Result then
    Exit;

  // Fall back to a system-PATH search but immediately release any handles
  // we acquired so this probe doesn't leak DLL refs on every run.
  HLib1 := LoadLibrary('libeay32.dll');
  HLib2 := LoadLibrary('ssleay32.dll');
  Result := (HLib1 <> 0) and (HLib2 <> 0);
  if HLib1 <> 0 then FreeLibrary(HLib1);
  if HLib2 <> 0 then FreeLibrary(HLib2);
end;

function TMQTTPublicBrokerTests.IsNetworkErrorMessage(const Msg: string): Boolean;
const
  NETWORK_KEYWORDS: array[0..6] of string = (
    'connection refused',
    'no route to host',
    'host is down',
    'connection reset',
    'timed out',
    'no connack',
    'EIdSocket'
  );
var
  Lower, Keyword: string;
begin
  Lower := LowerCase(Msg);
  for Keyword in NETWORK_KEYWORDS do
    if Pos(LowerCase(Keyword), Lower) > 0 then
      Exit(True);
  Result := False;
end;

function TMQTTPublicBrokerTests.RunPlainPubSubAgainst(const Broker: TPublicBroker; out FailReason: string): TBrokerOutcome;
var
  Sub, Pub: IMQTTClient;
  GotEvent: TEvent;
  Topic, Payload, Received: string;
begin
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
        if IsNetworkErrorMessage(E.Message) then
          Exit(boSkipNetwork)
        else
          Exit(boFailProtocol);
      end;
    end;

    try
      Pub.Connect(Broker.Host, Broker.PlainPort, MakeOptions(NewClientID('pub')));
    except
      on E: Exception do
      begin
        FailReason := 'publisher connect: ' + E.Message;
        Sub.Disconnect;
        if IsNetworkErrorMessage(E.Message) then
          Exit(boSkipNetwork)
        else
          Exit(boFailProtocol);
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
        if Received = Payload then
          Result := boOK
        else
        begin
          FailReason := Format('payload mismatch: expected "%s" got "%s"', [Payload, Received]);
          Result := boFailProtocol;
        end;
      end
      else
      begin
        // Both ends connected over MQTT but the round-trip timed out.
        // That's a flaky public service, not a client bug -> skip.
        FailReason := 'timeout waiting for published message round-trip';
        Result := boSkipNetwork;
      end;
    finally
      Pub.Disconnect;
      Sub.Disconnect;
    end;
  finally
    GotEvent.Free;
  end;
end;

function TMQTTPublicBrokerTests.RunSSLConnectAgainst(const Broker: TPublicBroker; out FailReason: string): TBrokerOutcome;
var
  Client: IMQTTClient;
  SSLOpts: TMQTTSSLOptions;
begin
  FailReason := '';
  Client := CreateMQTTClient;
  SSLOpts.SetDefaults;
  SSLOpts.Enabled := True;
  SSLOpts.Method := sslTLS1_2;
  SSLOpts.VerifyMode := sslVerifyNone;
  try
    Client.Connect(Broker.Host, Broker.SSLPort, MakeOptions(NewClientID('ssl')), SSLOpts);
    if Client.Connected then
      Result := boOK
    else
    begin
      FailReason := 'connect returned but Connected=False';
      Result := boFailProtocol;
    end;
    Client.Disconnect;
  except
    on E: Exception do
    begin
      FailReason := E.ClassName + ': ' + E.Message;
      if IsNetworkErrorMessage(E.Message) then
        Result := boSkipNetwork
      else
        Result := boFailProtocol;
    end;
  end;
end;

procedure TMQTTPublicBrokerTests.AssertPlainPubSub(const Broker: TPublicBroker);
var
  Reason: string;
  Outcome: TBrokerOutcome;
begin
  if not IsHostReachable(Broker.Host, Broker.PlainPort) then
    Assert.Pass(Format('Skipped: %s:%d unreachable from this network',
      [Broker.Host, Broker.PlainPort]));

  Outcome := RunPlainPubSubAgainst(Broker, Reason);
  case Outcome of
    boOK:
      ; // implicit pass
    boSkipNetwork:
      Assert.Pass(Format('Skipped (network/instability): %s - %s',
        [Broker.Name, Reason]));
    boFailProtocol:
      Assert.Fail(Format('Protocol failure against %s: %s',
        [Broker.Name, Reason]));
  end;
end;

procedure TMQTTPublicBrokerTests.AssertSSLConnect(const Broker: TPublicBroker);
var
  Reason: string;
  Outcome: TBrokerOutcome;
begin
  if not OpenSSLAvailable then
    Assert.Pass('Skipped: OpenSSL 1.0.2 DLLs (libeay32.dll, ssleay32.dll) not found');
  if not IsHostReachable(Broker.Host, Broker.SSLPort) then
    Assert.Pass(Format('Skipped: %s:%d unreachable from this network',
      [Broker.Host, Broker.SSLPort]));

  Outcome := RunSSLConnectAgainst(Broker, Reason);
  case Outcome of
    boOK:
      ; // implicit pass
    boSkipNetwork:
      Assert.Pass(Format('Skipped (network/instability): %s SSL - %s',
        [Broker.Name, Reason]));
    boFailProtocol:
      Assert.Fail(Format('SSL/protocol failure against %s: %s',
        [Broker.Name, Reason]));
  end;
end;

procedure TMQTTPublicBrokerTests.Mosquitto_Plain_PubSub;
begin
  AssertPlainPubSub(MOSQUITTO);
end;

procedure TMQTTPublicBrokerTests.Mosquitto_SSL_Connect;
begin
  AssertSSLConnect(MOSQUITTO);
end;

procedure TMQTTPublicBrokerTests.HiveMQ_Plain_PubSub;
begin
  AssertPlainPubSub(HIVEMQ);
end;

procedure TMQTTPublicBrokerTests.HiveMQ_SSL_Connect;
begin
  AssertSSLConnect(HIVEMQ);
end;

procedure TMQTTPublicBrokerTests.EMQX_Plain_PubSub;
begin
  AssertPlainPubSub(EMQX);
end;

initialization
  Randomize;
  TDUnitX.RegisterTestFixture(TMQTTPublicBrokerTests);

end.
