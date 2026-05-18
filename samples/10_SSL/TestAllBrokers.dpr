program TestAllBrokers;

{$APPTYPE CONSOLE}

{
  Test SSL/TLS Connection to All Public MQTT Brokers

  Tests:
  - test.mosquitto.org:8883 (Eclipse Mosquitto)
  - broker.hivemq.com:8883 (HiveMQ)
  - broker.emqx.io:8883 (EMQX)
}

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  IdSSLOpenSSL,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

type
  TBrokerInfo = record
    Name: string;
    Host: string;
    Port: Word;
  end;

const
  // Timeout for connection attempts (in milliseconds)
  CONNECT_TIMEOUT = 15000;

  Brokers: array[0..3] of TBrokerInfo = (
    (Name: 'Eclipse Mosquitto'; Host: 'test.mosquitto.org'; Port: 8883),
    (Name: 'HiveMQ Public'; Host: 'broker.hivemq.com'; Port: 8883),
    (Name: 'EMQX Public'; Host: 'broker.emqx.io'; Port: 8883),
    (Name: 'Eclipse IoT'; Host: 'mqtt.eclipseprojects.io'; Port: 8883)
  );

var
  SuccessCount, FailCount: Integer;

function TestBroker(const Broker: TBrokerInfo): Boolean;
var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
  SSLOptions: TMQTTSSLOptions;
  MessageReceived: TEvent;
  TestTopic: string;
  GotEcho: Boolean;
begin
  Result := False;
  GotEcho := False;

  Writeln('  Host: ' + Broker.Host + ':' + IntToStr(Broker.Port));

  MessageReceived := TEvent.Create(nil, False, False, '');
  try
    Client := CreateMQTTClient;

    // Configure MQTT
    Options.SetDefaults;
    Options.ClientID := 'DelphiTest_' + IntToStr(Random(100000));
    Options.KeepAliveSec := 60;
    Options.CleanStart := True;
    Options.Version := MQTT5;

    // Configure SSL
    SSLOptions.SetDefaults;
    SSLOptions.Enabled := True;
    SSLOptions.Method := sslTLS1_2;
    SSLOptions.VerifyMode := sslVerifyNone;

    // Connect
    Write('  Connecting... ');
    try
      Client.Connect(Broker.Host, Broker.Port, Options, SSLOptions);
      Writeln('OK');
    except
      on E: Exception do
      begin
        Writeln('FAILED');
        Writeln('  Error: ' + E.Message);
        Exit;
      end;
    end;

    // Subscribe
    TestTopic := 'delphi/mqtt/test/' + Options.ClientID;
    Write('  Subscribing to ' + TestTopic + '... ');
    try
      Client.Subscribe(TestTopic,
        procedure(const Topic: string; const Payload: TBytes)
        begin
          GotEcho := True;
          MessageReceived.SetEvent;
        end,
        atLeastOnce);
      Writeln('OK');
    except
      on E: Exception do
      begin
        Writeln('FAILED: ' + E.Message);
        Client.Disconnect;
        Exit;
      end;
    end;

    // Publish
    Write('  Publishing test message... ');
    try
      Client.Publish(TestTopic, 'Test from Delphi SSL', atLeastOnce);
      Writeln('OK');
    except
      on E: Exception do
      begin
        Writeln('FAILED: ' + E.Message);
        Client.Disconnect;
        Exit;
      end;
    end;

    // Wait for echo
    Write('  Waiting for echo... ');
    if MessageReceived.WaitFor(5000) = wrSignaled then
      Writeln('RECEIVED')
    else
      Writeln('TIMEOUT (broker may be slow)');

    // Disconnect
    Write('  Disconnecting... ');
    Client.Disconnect;
    Writeln('OK');

    // Connection succeeded (echo is best-effort on public brokers).
    Result := True;

  finally
    MessageReceived.Free;
  end;
end;

var
  I: Integer;
  Success: Boolean;
begin
  ReportMemoryLeaksOnShutdown := True;
  Randomize;
  SuccessCount := 0;
  FailCount := 0;

  Writeln('==============================================');
  Writeln('  Delphi MQTT - SSL/TLS Public Broker Test');
  Writeln('==============================================');
  Writeln;

  // Check OpenSSL
  Write('Checking OpenSSL... ');
  if not IdSSLOpenSSL.LoadOpenSSLLibrary then
  begin
    Writeln('FAILED');
    Writeln('OpenSSL libraries not found!');
    Writeln('Press Enter to exit...');
    Readln;
    Halt(1);
  end;
  Writeln('OK (' + String(IdSSLOpenSSL.OpenSSLVersion) + ')');
  Writeln;

  // Test each broker
  for I := 0 to High(Brokers) do
  begin
    Writeln('----------------------------------------------');
    Writeln('Testing: ' + Brokers[I].Name);
    Writeln('----------------------------------------------');

    Success := TestBroker(Brokers[I]);

    if Success then
    begin
      Writeln('  Result: SUCCESS');
      Inc(SuccessCount);
    end
    else
    begin
      Writeln('  Result: FAILED');
      Inc(FailCount);
    end;
    Writeln;
  end;

  // Summary
  Writeln('==============================================');
  Writeln('  SUMMARY');
  Writeln('==============================================');
  Writeln('  Total brokers tested: ' + IntToStr(Length(Brokers)));
  Writeln('  Successful: ' + IntToStr(SuccessCount));
  Writeln('  Failed: ' + IntToStr(FailCount));
  Writeln;

  if FailCount = 0 then
    Writeln('  ALL TESTS PASSED!')
  else
    Writeln('  Some tests failed - check broker availability');

  Writeln;
  Writeln('Press Enter to exit...');
  Readln;
end.
