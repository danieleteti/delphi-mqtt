program PubSub;

{$APPTYPE CONSOLE}

{
  Publish/Subscribe Example

  Demonstrates:
  - Subscribing to topics with anonymous method handlers
  - Publishing messages with different QoS levels
  - Basic pub/sub message flow
  - Thread-safe console output

  Requirements: MQTT broker running on localhost:1883 (e.g., Mosquitto)
}

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

var
  Client: IMQTTClient;
  MessageCount: Integer;
  ConsoleLock: TCriticalSection;

procedure SafeWriteln(const S: string);
begin
  ConsoleLock.Enter;
  try
    Writeln(S);
  finally
    ConsoleLock.Leave;
  end;
end;

begin
  ConsoleLock := TCriticalSection.Create;
  try
    Randomize;
    MessageCount := 0;

    try
      SafeWriteln('Delphi MQTT Client - Publish/Subscribe Sample');
      SafeWriteln('----------------------------------------------');
      SafeWriteln('');

      Client := CreateMQTTClient;

      SafeWriteln('Connecting to localhost...');
      Client.Connect('localhost', 1883);
      SafeWriteln('Connected!');
      SafeWriteln('');

      // Subscribe with anonymous method handler
      SafeWriteln('Subscribing to "delphi/test"...');
      Client.Subscribe('delphi/test',
        procedure(const Topic: string; const Payload: TBytes)
        begin
          TInterlocked.Increment(MessageCount);
          SafeWriteln(Format('>>> Received [%s]: %s', [Topic, TEncoding.UTF8.GetString(Payload)]));
        end,
        atLeastOnce);  // QoS 1 for guaranteed delivery

      Sleep(500);  // Give broker time to process subscription

      // Publish some messages
      SafeWriteln('');
      SafeWriteln('Publishing messages...');

      Client.Publish('delphi/test', 'Hello from Delphi!', atMostOnce);
      SafeWriteln('Sent: Hello from Delphi! (QoS 0)');

      Client.Publish('delphi/test', 'This is a QoS 1 message', atLeastOnce);
      SafeWriteln('Sent: This is a QoS 1 message (QoS 1)');

      Client.Publish('delphi/test', 'Exactly once delivery', exactlyOnce);
      SafeWriteln('Sent: Exactly once delivery (QoS 2)');

      SafeWriteln('');
      SafeWriteln('Waiting for messages... Press Enter to quit.');
      Readln;

      SafeWriteln('');
      SafeWriteln(Format('Total messages received: %d', [MessageCount]));

      Client.Disconnect;
      SafeWriteln('Disconnected.');

    except
      on E: Exception do
        SafeWriteln('Error: ' + E.Message);
    end;
  finally
    ConsoleLock.Free;
  end;
end.
