program QoSDemo;

{$APPTYPE CONSOLE}

{
  QoS Demo - Demonstrates Quality of Service levels in MQTT

  QoS 0 (At Most Once): Fire and forget, no acknowledgment
  QoS 1 (At Least Once): Guaranteed delivery with PUBACK
  QoS 2 (Exactly Once): Guaranteed single delivery with 4-way handshake

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
  ReceivedCount: Integer;
  Acked: Boolean;

procedure OnMessage(const Topic: string; const Payload: TBytes);
begin
  Inc(ReceivedCount);
  Writeln(Format('  Received [%s]: %s', [Topic, TEncoding.UTF8.GetString(Payload)]));
end;

begin
  Randomize;
  ReceivedCount := 0;

  try
    Client := CreateMQTTClient;

    // Configure connection
    Options.SetDefaults;
    Options.ClientID := 'QoS_Demo_' + IntToStr(Random(10000));
    Options.KeepAliveSec := 60;
    Options.CleanStart := True;

    Writeln('=== MQTT QoS Demonstration ===');
    Writeln;

    // Connect to broker
    Writeln('Connecting to broker...');
    Client.Connect('localhost', 1883, Options);
    Writeln('Connected!');
    Writeln;

    // Subscribe to test topics with different QoS levels
    Writeln('Subscribing to topics...');
    Client.Subscribe('test/qos0', OnMessage, atMostOnce);
    Client.Subscribe('test/qos1', OnMessage, atLeastOnce);
    Client.Subscribe('test/qos2', OnMessage, exactlyOnce);
    Sleep(500);
    Writeln;

    // ===== QoS 0: At Most Once =====
    Writeln('--- QoS 0: At Most Once (Fire and Forget) ---');
    Writeln('Publishing message with QoS 0...');
    Client.Publish('test/qos0', 'Hello QoS 0!', atMostOnce);
    Writeln('Message sent (no acknowledgment expected)');
    Sleep(1000);
    Writeln;

    // ===== QoS 1: At Least Once =====
    Writeln('--- QoS 1: At Least Once (Guaranteed Delivery) ---');
    Writeln('Publishing message with QoS 1...');
    Client.Publish('test/qos1', 'Hello QoS 1!', atLeastOnce);
    Writeln('Message sent, waiting for PUBACK...');
    Sleep(1000);
    Writeln;

    // ===== QoS 1: Synchronous Publish =====
    Writeln('--- QoS 1: Synchronous Publish with Timeout ---');
    Writeln('Publishing with PublishSync (waits for ACK)...');
    Acked := Client.PublishSync('test/qos1',
      TEncoding.UTF8.GetBytes('Hello QoS 1 Sync!'),
      atLeastOnce, False, 5000);
    if Acked then
      Writeln('Message acknowledged by broker!')
    else
      Writeln('Timeout waiting for acknowledgment');
    Sleep(500);
    Writeln;

    // ===== QoS 2: Exactly Once =====
    Writeln('--- QoS 2: Exactly Once (4-way Handshake) ---');
    Writeln('Publishing message with QoS 2...');
    Writeln('Flow: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP');
    Client.Publish('test/qos2', 'Hello QoS 2!', exactlyOnce);
    Writeln('Message sent, 4-way handshake in progress...');
    Sleep(1000);
    Writeln;

    // ===== QoS 2: Synchronous Publish =====
    Writeln('--- QoS 2: Synchronous Publish ---');
    Acked := Client.PublishSync('test/qos2',
      TEncoding.UTF8.GetBytes('Hello QoS 2 Sync!'),
      exactlyOnce, False, 5000);
    if Acked then
      Writeln('QoS 2 message fully acknowledged (PUBCOMP received)!')
    else
      Writeln('Timeout waiting for PUBCOMP');
    Sleep(500);
    Writeln;

    // Summary
    Writeln('=== Summary ===');
    Writeln(Format('Total messages received: %d', [ReceivedCount]));
    Writeln;
    Writeln('QoS Level Comparison:');
    Writeln('  QoS 0: Fastest, no guarantee, possible loss');
    Writeln('  QoS 1: Guaranteed delivery, possible duplicates');
    Writeln('  QoS 2: Guaranteed exactly-once, highest overhead');
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
