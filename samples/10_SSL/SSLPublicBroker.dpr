program SSLPublicBroker;

{$APPTYPE CONSOLE}

{
  SSL/TLS connection to public MQTT brokers — cascade fallback.

  Public test brokers are unstable (rate limit, ACL, maintenance windows).
  To make the demo reliable we try the well-known ones IN CASCADE: the first
  one that responds wins.

  TLS 1.2 with sslVerifyNone (test-only). For production use sslVerifyPeer +
  RootCertFile.

  Requirements:
    - OpenSSL legacy 1.0.2 DLLs next to the .exe (or in PATH):
        libeay32.dll, ssleay32.dll
    - Download: https://slproweb.com/products/Win32OpenSSL.html (1.0.2u Light)
}

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.StrUtils,
  IdSSLOpenSSL,
  IdSSLOpenSSLHeaders,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

type
  TBrokerEndpoint = record
    Name: string;
    Host: string;
    Port: Word;
  end;

const
  BROKERS: array[0..2] of TBrokerEndpoint = (
    (Name: 'Eclipse Mosquitto'; Host: 'test.mosquitto.org'; Port: 8883),
    (Name: 'EMQX';              Host: 'broker.emqx.io';     Port: 8883),
    (Name: 'HiveMQ';            Host: 'broker.hivemq.com';  Port: 8883)
  );

function TryConnect(out Endpoint: TBrokerEndpoint;
  const Options: TMQTTConnectOptions; const SSLOptions: TMQTTSSLOptions): IMQTTClient;
var
  Client: IMQTTClient;
  I: Integer;
begin
  Result := nil;
  for I := Low(BROKERS) to High(BROKERS) do
  begin
    Writeln(Format('-> Trying %s (%s:%d) ...',
      [BROKERS[I].Name, BROKERS[I].Host, BROKERS[I].Port]));
    Client := CreateMQTTClient;

    Client.SetOnDisconnect(
      procedure(ReasonCode: TMQTTReasonCode; const ReasonString: string)
      begin
        Writeln('  Event: Disconnected - ' + ReasonString);
      end);

    Client.SetOnError(
      procedure(const ErrorMsg: string)
      begin
        Writeln('  Event: Error - ' + ErrorMsg);
      end);

    try
      Client.Connect(BROKERS[I].Host, BROKERS[I].Port, Options, SSLOptions);
      Endpoint := BROKERS[I];
      Result := Client;
      Exit;
    except
      on E: Exception do
      begin
        Writeln(Format('   FAILED (%s: %s)', [E.ClassName, E.Message]));
        Client := nil; // releases SSL handler and socket
      end;
    end;
  end;
end;

var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
  SSLOptions: TMQTTSSLOptions;
  Endpoint: TBrokerEndpoint;
  MessageReceived: TEvent;
  ReceivedCount: Integer;

begin
  ReportMemoryLeaksOnShutdown := True;
  Randomize;
  ReceivedCount := 0;
  MessageReceived := TEvent.Create(nil, False, False, '');

  try
    Writeln('================================================');
    Writeln('  Delphi MQTT - SSL/TLS Public Broker Demo');
    Writeln('================================================');
    Writeln;

    Write('Checking OpenSSL... ');
    if not IdSSLOpenSSL.LoadOpenSSLLibrary then
    begin
      Writeln('FAILED');
      Writeln;
      Writeln('OpenSSL libraries not found. Need libeay32.dll + ssleay32.dll');
      Writeln('(OpenSSL 1.0.2) next to the .exe.');
      Writeln('Download: https://slproweb.com/products/Win32OpenSSL.html');
      Writeln;
      Writeln('Press Enter to exit...');
      Readln;
      Halt(1);
    end;
    Writeln('OK (' + String(IdSSLOpenSSL.OpenSSLVersion) + ')');
    Writeln;

    Options.SetDefaults;
    Options.ClientID := 'DelphiSSL_' + IntToStr(Random(100000));
    Options.KeepAliveSec := 60;
    Options.CleanStart := True;
    Options.Version := MQTT5;

    SSLOptions.SetDefaults;
    SSLOptions.Enabled := True;
    SSLOptions.Method := sslTLS1_2;
    SSLOptions.VerifyMode := sslVerifyNone; // OK for public test brokers

    Writeln('Trying public brokers in cascade (TLS 1.2)...');
    Client := TryConnect(Endpoint, Options, SSLOptions);
    if Client = nil then
    begin
      Writeln;
      Writeln('No public broker responded. All unreachable or rate-limited.');
      Writeln('Retry later or use a local broker (mosquitto on 8883).');
      Writeln;
      Writeln('Press Enter to exit...');
      Readln;
      Halt(1);
    end;

    Writeln;
    Writeln('Connection Details:');
    Writeln('  Broker:    ' + Endpoint.Name + ' (' + Endpoint.Host + ':' + IntToStr(Endpoint.Port) + ')');
    Writeln('  Client ID: ' + Options.ClientID);
    Writeln('  Protocol:  MQTT 5.0 over TLS 1.2');
    Writeln('  Status:    ' + IfThen(Client.Connected, 'Connected', 'Disconnected'));
    Writeln;

    var TestTopic := 'delphi/mqtt/ssl/test/' + Options.ClientID;

    Writeln('Subscribing to: ' + TestTopic);
    Client.Subscribe(TestTopic,
      procedure(const Topic: string; const Payload: TBytes)
      begin
        Inc(ReceivedCount);
        Writeln;
        Writeln('>>> MESSAGE RECEIVED (#' + IntToStr(ReceivedCount) + ')');
        Writeln('    Topic:   ' + Topic);
        Writeln('    Payload: ' + TEncoding.UTF8.GetString(Payload));
        MessageReceived.SetEvent;
      end,
      atLeastOnce);
    Writeln('Subscribed with QoS 1');
    Writeln;

    Writeln('Publishing test messages...');
    Writeln;

    // Message 1: Simple text
    var Msg1 := 'Hello from Delphi SSL! Time: ' + FormatDateTime('hh:nn:ss', Now);
    Client.Publish(TestTopic, Msg1, atLeastOnce);
    Writeln('Sent: ' + Msg1);
    if MessageReceived.WaitFor(3000) = wrSignaled then
      Writeln('Echo received!')
    else
      Writeln('Warning: No echo received (might be broker delay)');
    Writeln;

    // Message 2: JSON payload
    var Msg2 := '{"sensor":"temperature","value":23.5,"unit":"C","timestamp":"' +
                FormatDateTime('yyyy-mm-dd"T"hh:nn:ss', Now) + '"}';
    Client.Publish(TestTopic, Msg2, atLeastOnce);
    Writeln('Sent JSON: ' + Msg2);
    MessageReceived.WaitFor(3000);
    Writeln;

    // Message 3: QoS 2
    var Msg3 := 'QoS 2 message - Exactly Once Delivery';
    Client.Publish(TestTopic, Msg3, exactlyOnce);
    Writeln('Sent QoS 2: ' + Msg3);
    MessageReceived.WaitFor(3000);
    Writeln;

    Writeln('================================================');
    Writeln('Interactive mode - Type messages to send');
    Writeln('Press Enter on empty line to exit');
    Writeln('================================================');
    Writeln;

    while True do
    begin
      Write('> ');
      var Input: string;
      Readln(Input);
      if Input = '' then
        Break;
      Client.Publish(TestTopic, Input, atLeastOnce);
      Writeln('Sent: ' + Input);
      Sleep(500);
    end;

    Writeln;
    Writeln('Disconnecting...');
    Client.Disconnect;
    Writeln('Disconnected. Total messages received: ' + IntToStr(ReceivedCount));

  except
    on E: Exception do
    begin
      Writeln;
      Writeln('ERROR: ' + E.ClassName + ': ' + E.Message);
      Writeln;
      Writeln('Press Enter to exit...');
      Readln;
    end;
  end;

  MessageReceived.Free;
end.
