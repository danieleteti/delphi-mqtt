program SSLDemo;

{$APPTYPE CONSOLE}

{
  SSL/TLS Connection Example

  Demonstrates:
  - Connecting to MQTT broker using SSL/TLS
  - Different SSL configuration options
  - Using test.mosquitto.org public broker (port 8883)

  Requirements:
  - OpenSSL DLLs (libeay32.dll/ssleay32.dll or libssl-1_1.dll/libcrypto-1_1.dll)
    must be in the executable path or system PATH
  - For TLS 1.2: Standard Indy OpenSSL binaries work
  - For TLS 1.3: Requires TaurusTLS with OpenSSL 1.1.1+

  Public test brokers with SSL:
  - test.mosquitto.org:8883 (no client cert required)
  - broker.hivemq.com:8883
}

uses
  System.SysUtils,
  IdSSLOpenSSL,
  IdSSLOpenSSLHeaders,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
  SSLOptions: TMQTTSSLOptions;
begin
  try
    Writeln('Delphi MQTT Client - SSL/TLS Connection Sample');
    Writeln('-----------------------------------------------');
    Writeln;

    // Check if OpenSSL is available
    if not IdSSLOpenSSL.LoadOpenSSLLibrary then
    begin
      Writeln('ERROR: OpenSSL libraries not found!');
      Writeln('');
      Writeln('Please ensure the OpenSSL DLLs are available:');
      Writeln('  - For 32-bit: libeay32.dll, ssleay32.dll');
      Writeln('  - For 64-bit: libssl-1_1-x64.dll, libcrypto-1_1-x64.dll');
      Writeln('');
      Writeln('You can download them from:');
      Writeln('  https://wiki.openssl.org/index.php/Binaries');
      Writeln('  https://slproweb.com/products/Win32OpenSSL.html');
      Readln;
      Exit;
    end;

    Writeln('OpenSSL loaded successfully');
    Writeln('  Version: ' + String(IdSSLOpenSSL.OpenSSLVersion));
    Writeln;

    // Create client
    Client := CreateMQTTClient;

    // Configure MQTT options
    Options.SetDefaults;
    Options.ClientID := 'DelphiSSLDemo_' + IntToStr(Random(10000));
    Options.KeepAliveSec := 60;
    Options.CleanStart := True;
    Options.Version := MQTT5;

    // Configure SSL options
    SSLOptions.SetDefaults;
    SSLOptions.Enabled := True;
    SSLOptions.Method := sslTLS1_2;           // TLS 1.2 (widely supported)
    SSLOptions.VerifyMode := sslVerifyNone;   // For test broker, no cert verification
    // For production with proper CA validation:
    // SSLOptions.VerifyMode := sslVerifyPeer;
    // SSLOptions.RootCertFile := 'path/to/ca-certificates.crt';

    // Connect using SSL to test.mosquitto.org
    Writeln('Connecting to test.mosquitto.org:8883 (TLS 1.2)...');
    Client.Connect('test.mosquitto.org', 8883, Options, SSLOptions);
    Writeln('Connected successfully!');
    Writeln;

    if Client.Connected then
    begin
      Writeln('SSL Connection Details:');
      Writeln('  Broker: test.mosquitto.org:8883');
      Writeln('  Client ID: ' + Options.ClientID);
      Writeln('  Protocol: MQTT 5.0 over TLS 1.2');
      Writeln;

      // Subscribe to a test topic
      Client.Subscribe('delphi/ssl/test',
        procedure(const Topic: string; const Payload: TBytes)
        begin
          Writeln('Received: ' + TEncoding.UTF8.GetString(Payload));
        end);
      Writeln('Subscribed to: delphi/ssl/test');

      // Publish a test message
      Client.Publish('delphi/ssl/test', 'Hello from Delphi SSL!');
      Writeln('Published test message');
      Writeln;

      // Wait a moment for the message to be received
      Sleep(2000);
    end;

    Writeln;
    Writeln('Press Enter to disconnect and exit.');
    Readln;

    Client.Disconnect;
    Writeln('Disconnected.');

  except
    on E: Exception do
    begin
      Writeln('Error: ' + E.ClassName + ': ' + E.Message);
      Readln;
    end;
  end;
end.
