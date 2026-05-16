program SSLClientCert;

{$APPTYPE CONSOLE}

{
  SSL/TLS with Client Certificate (Mutual TLS) Example

  Demonstrates:
  - Connecting with client certificate authentication
  - Configuring certificate files and private key
  - Using password-protected private key

  This is commonly required for:
  - AWS IoT Core
  - Azure IoT Hub (with X.509 certificates)
  - Enterprise MQTT brokers

  Certificate files needed:
  - Client certificate (PEM format): client.crt
  - Client private key (PEM format): client.key
  - CA root certificate (PEM format): ca.crt (optional, for server verification)
}

uses
  System.SysUtils,
  System.IOUtils,
  IdSSLOpenSSL,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

const
  // Change these paths to your actual certificate files
  BROKER_HOST = 'your-iot-endpoint.amazonaws.com';
  BROKER_PORT = 8883;
  CLIENT_CERT = 'certs\client.crt';
  CLIENT_KEY  = 'certs\client.key';
  CA_CERT     = 'certs\AmazonRootCA1.pem';  // For AWS IoT
  KEY_PASSWORD = '';  // If your private key is encrypted

var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
  SSLOptions: TMQTTSSLOptions;
begin
  try
    Writeln('Delphi MQTT Client - Mutual TLS (Client Certificate) Sample');
    Writeln('-------------------------------------------------------------');
    Writeln;

    // Check if OpenSSL is available
    if not IdSSLOpenSSL.LoadOpenSSLLibrary then
    begin
      Writeln('ERROR: OpenSSL libraries not found!');
      Readln;
      Exit;
    end;

    Writeln('OpenSSL Version: ' + String(IdSSLOpenSSL.OpenSSLVersion));
    Writeln;

    // Verify certificate files exist
    if not TFile.Exists(CLIENT_CERT) then
    begin
      Writeln('ERROR: Client certificate not found: ' + CLIENT_CERT);
      Writeln;
      Writeln('Please create a "certs" folder with your certificate files:');
      Writeln('  - client.crt (client certificate)');
      Writeln('  - client.key (private key)');
      Writeln('  - ca.crt or AmazonRootCA1.pem (CA root certificate)');
      Readln;
      Exit;
    end;

    if not TFile.Exists(CLIENT_KEY) then
    begin
      Writeln('ERROR: Client private key not found: ' + CLIENT_KEY);
      Readln;
      Exit;
    end;

    // Create client
    Client := CreateMQTTClient;

    // Configure MQTT options
    Options.SetDefaults;
    Options.ClientID := 'DelphiMTLS_' + IntToStr(Random(10000));
    Options.KeepAliveSec := 60;
    Options.CleanStart := True;
    Options.Version := MQTT5;

    // Configure SSL with client certificate
    SSLOptions.SetDefaults;
    SSLOptions.Enabled := True;
    SSLOptions.Method := sslTLS1_2;
    SSLOptions.VerifyMode := sslVerifyPeer;  // Verify server certificate
    SSLOptions.VerifyDepth := 9;

    // Set certificate files
    SSLOptions.CertFile := CLIENT_CERT;
    SSLOptions.KeyFile := CLIENT_KEY;
    SSLOptions.KeyPassword := KEY_PASSWORD;

    // CA certificate for server verification
    if TFile.Exists(CA_CERT) then
      SSLOptions.RootCertFile := CA_CERT;

    Writeln('Certificate Configuration:');
    Writeln('  Client Cert: ' + SSLOptions.CertFile);
    Writeln('  Private Key: ' + SSLOptions.KeyFile);
    if SSLOptions.RootCertFile <> '' then
      Writeln('  CA Cert:     ' + SSLOptions.RootCertFile);
    Writeln;

    // Connect
    Writeln('Connecting to ' + BROKER_HOST + ':' + IntToStr(BROKER_PORT) + '...');
    Client.Connect(BROKER_HOST, BROKER_PORT, Options, SSLOptions);
    Writeln('Connected successfully with mutual TLS!');
    Writeln;

    if Client.Connected then
    begin
      // For AWS IoT, topic structure is typically:
      // $aws/things/{thing-name}/shadow/...
      // or custom topics

      Client.Subscribe('test/topic',
        procedure(const Topic: string; const Payload: TBytes)
        begin
          Writeln('Received on ' + Topic + ': ' + TEncoding.UTF8.GetString(Payload));
        end);
      Writeln('Subscribed to: test/topic');

      Client.Publish('test/topic', '{"message": "Hello from Delphi with mTLS!"}');
      Writeln('Published test message');
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
