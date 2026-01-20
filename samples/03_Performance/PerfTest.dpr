program PerfTest;

{$APPTYPE CONSOLE}

{
  Performance Benchmark Example

  Demonstrates:
  - High-throughput message publishing
  - Performance measurement with TStopwatch
  - QoS 0 for maximum speed

  Requirements: MQTT broker running on localhost:1883 (e.g., Mosquitto)
}

uses
  System.SysUtils,
  System.Diagnostics,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

const
  ITERATIONS = 10000;

var
  Client: IMQTTClient;
  Stopwatch: TStopwatch;
  I: Integer;
  MsgPerSec: Double;
begin
  try
    Writeln('Delphi MQTT Client - Performance Benchmark');
    Writeln('-------------------------------------------');
    Writeln;

    Client := CreateMQTTClient;

    Writeln('Connecting to localhost...');
    Client.Connect('localhost', 1883);
    Writeln('Connected!');
    Writeln;

    Writeln(Format('Starting throughput test: %d messages...', [ITERATIONS]));
    Writeln;

    // QoS 0 - Fire and forget for maximum throughput
    Stopwatch := TStopwatch.StartNew;
    for I := 1 to ITERATIONS do
      Client.Publish('perf/test', 'benchmarking payload data', atMostOnce);
    Stopwatch.Stop;

    MsgPerSec := ITERATIONS / Stopwatch.Elapsed.TotalSeconds;

    Writeln('=== Results (QoS 0 - At Most Once) ===');
    Writeln(Format('  Messages sent:  %d', [ITERATIONS]));
    Writeln(Format('  Elapsed time:   %d ms', [Stopwatch.ElapsedMilliseconds]));
    Writeln(Format('  Throughput:     %.2f messages/sec', [MsgPerSec]));
    Writeln;

    // QoS 1 - With acknowledgments (slower but guaranteed)
    Writeln(Format('Testing QoS 1 with %d messages...', [ITERATIONS div 10]));
    Stopwatch := TStopwatch.StartNew;
    for I := 1 to ITERATIONS div 10 do
      Client.Publish('perf/test', 'qos1 payload', atLeastOnce);
    Stopwatch.Stop;

    MsgPerSec := (ITERATIONS div 10) / Stopwatch.Elapsed.TotalSeconds;

    Writeln('=== Results (QoS 1 - At Least Once) ===');
    Writeln(Format('  Messages sent:  %d', [ITERATIONS div 10]));
    Writeln(Format('  Elapsed time:   %d ms', [Stopwatch.ElapsedMilliseconds]));
    Writeln(Format('  Throughput:     %.2f messages/sec', [MsgPerSec]));
    Writeln;

    Client.Disconnect;
    Writeln('Disconnected.');

  except
    on E: Exception do
      Writeln('Error: ', E.Message);
  end;

  Writeln;
  Writeln('Press Enter to exit.');
  Readln;
end.
