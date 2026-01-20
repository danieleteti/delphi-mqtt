program WillDemo;

{$APPTYPE CONSOLE}

{
  Will Message (Last Will and Testament) Demo

  The Will Message is sent by the broker when:
  - The client disconnects unexpectedly (network failure, crash)
  - The keep-alive timeout expires
  - The client closes connection without sending DISCONNECT

  The Will Message is NOT sent when:
  - The client sends a proper DISCONNECT packet
  - Clean disconnect is performed

  Use cases:
  - Device offline notifications
  - Presence detection
  - Graceful vs ungraceful disconnect detection

  Requirements: MQTT broker running on localhost:1883 (e.g., Mosquitto)

  To test: Run two instances - one publisher with will, one subscriber.
           Then kill the publisher (Ctrl+C or close window) to see the will message.
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
  Mode: string;
  DeviceID: string;

procedure RunPublisher;
var
  Counter: Integer;
begin
  Writeln('=== Will Message Demo - PUBLISHER ===');
  Writeln;
  Writeln('This client has a Will Message configured.');
  Writeln('If this client disconnects abnormally, the broker will');
  Writeln('publish the Will Message to "devices/' + DeviceID + '/status"');
  Writeln;

  Options.SetDefaults;
  Options.ClientID := 'WillDemo_Pub_' + DeviceID;
  Options.KeepAliveSec := 5;  // Short keep-alive for faster will trigger
  Options.CleanStart := True;

  // Configure Will Message
  Options.Will.Enabled := True;
  Options.Will.Topic := 'devices/' + DeviceID + '/status';
  Options.Will.Payload := TEncoding.UTF8.GetBytes('OFFLINE (unexpected disconnect)');
  Options.Will.QoS := atLeastOnce;
  Options.Will.Retain := True;  // Retain so new subscribers see the status

  Writeln('Will Configuration:');
  Writeln('  Topic: ' + Options.Will.Topic);
  Writeln('  Payload: ' + TEncoding.UTF8.GetString(Options.Will.Payload));
  Writeln('  QoS: 1 (At Least Once)');
  Writeln('  Retain: True');
  Writeln;

  Client.Connect('localhost', 1883, Options);
  Writeln('Connected to broker!');
  Writeln;

  // Publish online status (with retain)
  Client.Publish('devices/' + DeviceID + '/status', 'ONLINE', atLeastOnce, True);
  Writeln('Published ONLINE status (retained)');
  Writeln;

  Writeln('Options:');
  Writeln('  1. Press Enter for GRACEFUL disconnect (Will NOT be sent)');
  Writeln('  2. Press Ctrl+C or close window for UNGRACEFUL disconnect (Will IS sent)');
  Writeln;

  Counter := 0;
  Writeln('Publishing heartbeats every 2 seconds...');
  while True do
  begin
    Inc(Counter);
    Client.Publish('devices/' + DeviceID + '/heartbeat',
      Format('Heartbeat #%d at %s', [Counter, FormatDateTime('hh:nn:ss', Now)]),
      atMostOnce);
    Write('.');

    // Check for user input (non-blocking on Windows would need more code)
    // For simplicity, just loop - user can press Ctrl+C
    Sleep(2000);
  end;
end;

procedure RunSubscriber;
begin
  Writeln('=== Will Message Demo - SUBSCRIBER ===');
  Writeln;
  Writeln('This client subscribes to device status and heartbeat topics.');
  Writeln('It will receive the Will Message if the publisher disconnects unexpectedly.');
  Writeln;

  Options.SetDefaults;
  Options.ClientID := 'WillDemo_Sub_' + IntToStr(Random(10000));
  Options.CleanStart := True;

  Client.Connect('localhost', 1883, Options);
  Writeln('Connected to broker!');
  Writeln;

  Writeln('Subscribing to devices/+/status and devices/+/heartbeat...');
  Writeln;

  Client.Subscribe('devices/+/status', procedure(const Topic: string; const Payload: TBytes)
  begin
    Writeln(Format('[STATUS] %s: %s', [Topic, TEncoding.UTF8.GetString(Payload)]));
  end, atLeastOnce);

  Client.Subscribe('devices/+/heartbeat', procedure(const Topic: string; const Payload: TBytes)
  begin
    Writeln(Format('[HEARTBEAT] %s: %s', [Topic, TEncoding.UTF8.GetString(Payload)]));
  end, atMostOnce);

  Writeln('Waiting for messages... Press Ctrl+C to exit.');
  Writeln;

  while True do
    Sleep(1000);
end;

begin
  Randomize;
  DeviceID := 'device_' + IntToStr(Random(1000));

  Writeln('=== MQTT Will Message (Last Will and Testament) Demo ===');
  Writeln;
  Writeln('Run this program in two modes:');
  Writeln('  1. PUBLISHER - Has a Will Message configured');
  Writeln('  2. SUBSCRIBER - Receives status updates and Will Messages');
  Writeln;
  Write('Select mode (P)ublisher or (S)ubscriber: ');
  Readln(Mode);
  Writeln;

  try
    Client := CreateMQTTClient;

    if (Mode = 'P') or (Mode = 'p') then
      RunPublisher
    else if (Mode = 'S') or (Mode = 's') then
      RunSubscriber
    else
    begin
      Writeln('Invalid mode. Use P for Publisher or S for Subscriber.');
      Exit;
    end;

  except
    on E: Exception do
      Writeln('Error: ', E.Message);
  end;

  // Cleanup
  if (Client <> nil) and Client.Connected then
  begin
    Writeln;
    Writeln('Disconnecting gracefully (Will NOT be sent)...');
    Client.Disconnect;
  end;

  Writeln('Done!');
end.
