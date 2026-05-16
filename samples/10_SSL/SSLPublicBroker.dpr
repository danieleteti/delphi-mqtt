program SSLPublicBroker;

{$APPTYPE CONSOLE}

{
  SSL Connection to Public MQTT Brokers

  This sample connects to free public MQTT brokers using SSL/TLS.
  No registration or certificates required!

  Public brokers tested:
  - broker.hivemq.com:8883 (HiveMQ)
  - broker.emqx.io:8883 (EMQX)
  - test.mosquitto.org:8883 (Eclipse Mosquitto)

  Requirements:
  - OpenSSL DLLs in executable path or system PATH
    For 64-bit: libssl-1_1-x64.dll, libcrypto-1_1-x64.dll
    For 32-bit: libeay32.dll, ssleay32.dll
}

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.StrUtils,
  IdSSLOpenSSL,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

const
  // Choose your broker (all work with SSL, no auth required)
  BROKER_HOST = 'test.mosquitto.org';
  BROKER_PORT = 8883;

  // Alternative brokers:
  // BROKER_HOST = 'broker.emqx.io'; BROKER_PORT = 8883;
  // BROKER_HOST = 'test.mosquitto.org'; BROKER_PORT = 8883;

var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
  SSLOptions: TMQTTSSLOptions;
  MessageReceived: TEvent;
  ReceivedCount: Integer;

procedure PrintHeader;
begin
  Writeln('================================================');
  Writeln('  Delphi MQTT - SSL/TLS Public Broker Demo');
  Writeln('================================================');
  Writeln;
end;

procedure CheckOpenSSL;
begin
  Write('Checking OpenSSL... ');
  if IdSSLOpenSSL.LoadOpenSSLLibrary then
  begin
    Writeln('OK');
    Writeln('  Version: ' + String(IdSSLOpenSSL.OpenSSLVersion));
  end
  else
  begin
    Writeln('FAILED');
    Writeln;
    Writeln('OpenSSL libraries not found!');
    Writeln;
    Writeln('Please download OpenSSL DLLs and place them in the executable folder:');
    Writeln('  https://slproweb.com/products/Win32OpenSSL.html');
    Writeln;
    {$IFDEF WIN64}
    Writeln('For 64-bit, you need:');
    Writeln('  - libssl-1_1-x64.dll');
    Writeln('  - libcrypto-1_1-x64.dll');
    {$ELSE}
    Writeln('For 32-bit, you need:');
    Writeln('  - libeay32.dll');
    Writeln('  - ssleay32.dll');
    {$ENDIF}
    Writeln;
    Writeln('Press Enter to exit...');
    Readln;
    Halt(1);
  end;
end;

begin
  ReportMemoryLeaksOnShutdown := True;
  Randomize;
  ReceivedCount := 0;
  MessageReceived := TEvent.Create(nil, False, False, '');

  try
    PrintHeader;
    CheckOpenSSL;
    Writeln;

    // Create client
    Client := CreateMQTTClient;

    // Configure MQTT options
    Options.SetDefaults;
    Options.ClientID := 'DelphiSSL_' + IntToStr(Random(100000));
    Options.KeepAliveSec := 60;
    Options.CleanStart := True;
    Options.Version := MQTT5;

    // Configure SSL - minimal config for public brokers
    SSLOptions.SetDefaults;
    SSLOptions.Enabled := True;
    SSLOptions.Method := sslTLS1_2;
    SSLOptions.VerifyMode := sslVerifyNone;  // Public brokers don't need cert verification

    // Set up event handlers
    Client.SetOnConnect(
      procedure(ReasonCode: TMQTTReasonCode)
      begin
        Writeln('  Event: Connected (ReasonCode=' + IntToStr(Ord(ReasonCode)) + ')');
      end);

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

    // Connect with SSL
    Writeln('Connecting to ' + BROKER_HOST + ':' + IntToStr(BROKER_PORT) + ' (TLS 1.2)...');
    try
      Client.Connect(BROKER_HOST, BROKER_PORT, Options, SSLOptions);
      Writeln('Connected successfully!');
    except
      on E: Exception do
      begin
        Writeln('Connection failed: ' + E.Message);
        Writeln;
        Writeln('Press Enter to exit...');
        Readln;
        Halt(1);
      end;
    end;
    Writeln;

    // Show connection info
    Writeln('Connection Details:');
    Writeln('  Broker:    ' + BROKER_HOST + ':' + IntToStr(BROKER_PORT));
    Writeln('  Client ID: ' + Options.ClientID);
    Writeln('  Protocol:  MQTT 5.0 over TLS 1.2');
    Writeln('  Status:    ' + IfThen(Client.Connected, 'Connected', 'Disconnected'));
    Writeln;

    // Create unique topic for this session
    var TestTopic := 'delphi/mqtt/ssl/test/' + Options.ClientID;

    // Subscribe to test topic
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

    // Publish test messages
    Writeln('Publishing test messages...');
    Writeln;

    // Message 1: Simple text
    var Msg1 := 'Hello from Delphi SSL! Time: ' + FormatDateTime('hh:nn:ss', Now);
    Client.Publish(TestTopic, Msg1, atLeastOnce);
    Writeln('Sent: ' + Msg1);

    // Wait for echo
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

    // Wait for echo
    MessageReceived.WaitFor(3000);
    Writeln;

    // Message 3: With QoS 2
    var Msg3 := 'QoS 2 message - Exactly Once Delivery';
    Client.Publish(TestTopic, Msg3, exactlyOnce);
    Writeln('Sent QoS 2: ' + Msg3);

    // Wait for echo
    MessageReceived.WaitFor(3000);
    Writeln;

    // Interactive mode
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

      // Brief wait to see echo
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
