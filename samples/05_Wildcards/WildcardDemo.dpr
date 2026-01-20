program WildcardDemo;

{$APPTYPE CONSOLE}

{
  Wildcard Demo - Demonstrates MQTT topic wildcards

  Single-level wildcard (+): Matches exactly one topic level
  Multi-level wildcard (#): Matches any number of levels (must be last)

  Examples:
    home/+/temp     matches: home/kitchen/temp, home/bedroom/temp
    home/#          matches: home, home/kitchen, home/kitchen/temp/sensor1
    +/status        matches: device1/status, device2/status

  Requirements: MQTT broker running on localhost:1883 (e.g., Mosquitto)
}

uses
  System.SysUtils,
  System.Classes,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
  MessageCount: Integer;

procedure OnSingleLevel(const Topic: string; const Payload: TBytes);
begin
  Inc(MessageCount);
  Writeln(Format('  [+] Single-level match: %s = %s', [Topic, TEncoding.UTF8.GetString(Payload)]));
end;

procedure OnMultiLevel(const Topic: string; const Payload: TBytes);
begin
  Inc(MessageCount);
  Writeln(Format('  [#] Multi-level match: %s = %s', [Topic, TEncoding.UTF8.GetString(Payload)]));
end;

procedure OnAllSensors(const Topic: string; const Payload: TBytes);
begin
  Inc(MessageCount);
  Writeln(Format('  [sensors/#] All sensors: %s = %s', [Topic, TEncoding.UTF8.GetString(Payload)]));
end;

procedure OnRoomTemp(const Topic: string; const Payload: TBytes);
begin
  Inc(MessageCount);
  Writeln(Format('  [home/+/temperature] Room temp: %s = %s', [Topic, TEncoding.UTF8.GetString(Payload)]));
end;

begin
  Randomize;
  MessageCount := 0;

  try
    Client := CreateMQTTClient;

    Options.SetDefaults;
    Options.ClientID := 'Wildcard_Demo_' + IntToStr(Random(10000));
    Options.CleanStart := True;

    Writeln('=== MQTT Wildcard Topic Demonstration ===');
    Writeln;

    // Connect
    Writeln('Connecting to broker...');
    Client.Connect('localhost', 1883, Options);
    Writeln('Connected!');
    Writeln;

    // ===== Subscribe with wildcards =====
    Writeln('--- Setting up wildcard subscriptions ---');
    Writeln;

    Writeln('1. Subscribe to "home/+/temperature" (single-level wildcard)');
    Writeln('   Matches: home/kitchen/temperature, home/bedroom/temperature');
    Writeln('   Does NOT match: home/temperature, home/floor1/kitchen/temperature');
    Client.Subscribe('home/+/temperature', OnRoomTemp, atLeastOnce);
    Writeln;

    Writeln('2. Subscribe to "sensors/#" (multi-level wildcard)');
    Writeln('   Matches: sensors, sensors/temp, sensors/temp/indoor, sensors/humidity/bathroom');
    Client.Subscribe('sensors/#', OnAllSensors, atLeastOnce);
    Writeln;

    Writeln('3. Subscribe to "+/status" (single-level at start)');
    Writeln('   Matches: device1/status, pump/status, fan/status');
    Client.Subscribe('+/status', OnSingleLevel, atLeastOnce);
    Writeln;

    Writeln('4. Subscribe to "alerts/#" (all alerts)');
    Client.Subscribe('alerts/#', OnMultiLevel, atLeastOnce);
    Writeln;

    Sleep(500);

    // ===== Publish test messages =====
    Writeln('--- Publishing test messages ---');
    Writeln;

    // Messages for home/+/temperature
    Writeln('Publishing to home/kitchen/temperature...');
    Client.Publish('home/kitchen/temperature', '23.5', atLeastOnce);
    Sleep(300);

    Writeln('Publishing to home/bedroom/temperature...');
    Client.Publish('home/bedroom/temperature', '21.0', atLeastOnce);
    Sleep(300);

    Writeln('Publishing to home/bathroom/temperature...');
    Client.Publish('home/bathroom/temperature', '25.2', atLeastOnce);
    Sleep(300);

    // This should NOT match home/+/temperature
    Writeln('Publishing to home/temperature (no match for +)...');
    Client.Publish('home/temperature', '22.0', atLeastOnce);
    Sleep(300);

    Writeln;

    // Messages for sensors/#
    Writeln('Publishing to sensors/temp...');
    Client.Publish('sensors/temp', '24.1', atLeastOnce);
    Sleep(300);

    Writeln('Publishing to sensors/humidity/indoor...');
    Client.Publish('sensors/humidity/indoor', '65%', atLeastOnce);
    Sleep(300);

    Writeln('Publishing to sensors/pressure/outdoor/garden...');
    Client.Publish('sensors/pressure/outdoor/garden', '1013 hPa', atLeastOnce);
    Sleep(300);

    Writeln;

    // Messages for +/status
    Writeln('Publishing to device1/status...');
    Client.Publish('device1/status', 'online', atLeastOnce);
    Sleep(300);

    Writeln('Publishing to pump/status...');
    Client.Publish('pump/status', 'running', atLeastOnce);
    Sleep(300);

    Writeln;

    // Messages for alerts/#
    Writeln('Publishing to alerts/fire...');
    Client.Publish('alerts/fire', 'ALARM!', atLeastOnce);
    Sleep(300);

    Writeln('Publishing to alerts/security/door...');
    Client.Publish('alerts/security/door', 'open', atLeastOnce);
    Sleep(300);

    // Wait for all messages
    Sleep(1000);

    Writeln;
    Writeln('=== Summary ===');
    Writeln(Format('Total messages received via wildcards: %d', [MessageCount]));
    Writeln;

    Writeln('Wildcard Rules:');
    Writeln('  + (plus)  : Matches exactly ONE level');
    Writeln('  # (hash)  : Matches ZERO or more levels (must be last)');
    Writeln('  +/+/+     : Matches exactly 3 levels');
    Writeln('  topic/#   : Matches topic and all sub-topics');
    Writeln;

    // Cleanup
    Writeln('Disconnecting...');
    Client.Disconnect;
    Writeln('Done!');

  except
    on E: Exception do
      Writeln('Error: ', E.Message);
  end;

  Writeln;
  Writeln('Press Enter to exit...');
  Readln;
end.
