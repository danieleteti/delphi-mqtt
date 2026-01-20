program Connect;

{$APPTYPE CONSOLE}

{
  Basic Connection Example

  Demonstrates:
  - Creating an MQTT client using interface
  - Connecting to broker with custom options
  - Checking connection status
  - Graceful disconnection

  Requirements: MQTT broker running on localhost:1883 (e.g., Mosquitto)
}

uses
  System.SysUtils,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
begin
  try
    Writeln('Delphi MQTT Client - Connection Sample');
    Writeln('---------------------------------------');
    Writeln;

    // Create client using factory function (returns interface)
    Client := CreateMQTTClient;

    // Configure connection options
    Options.SetDefaults;
    Options.ClientID := 'DelphiConnectSample';
    Options.KeepAliveSec := 60;
    Options.CleanStart := True;
    Options.Version := MQTT5;  // Use MQTT 5.0

    Write('Connecting to localhost (MQTT 5.0)... ');
    Client.Connect('localhost', 1883, Options);
    Writeln('OK!');

    if Client.Connected then
    begin
      Writeln;
      Writeln('Successfully connected to MQTT broker!');
      Writeln('  Client ID: ' + Options.ClientID);
      Writeln('  Protocol:  MQTT 5.0');
      Writeln('  Keep-Alive: ' + IntToStr(Options.KeepAliveSec) + ' seconds');
    end;

    Writeln;
    Writeln('Press Enter to disconnect and exit.');
    Readln;

    Client.Disconnect;
    Writeln('Disconnected.');

  except
    on E: Exception do
      Writeln('Error: ', E.Message);
  end;
end.
