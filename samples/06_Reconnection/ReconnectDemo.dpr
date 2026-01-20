program ReconnectDemo;

{$APPTYPE CONSOLE}

{
  Reconnection Demo - Demonstrates automatic reconnection

  Features:
  - AutoReconnect property enables automatic reconnection on disconnect
  - Exponential backoff: starts at 1s, doubles up to 30s max
  - OnConnect callback fires on initial connect and reconnect
  - OnDisconnect callback fires when connection is lost
  - OnError callback fires on errors
  - Pending QoS 1/2 messages are retried after reconnection

  Test: Stop and restart the MQTT broker while this demo is running
        to see automatic reconnection in action.

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
  Running: Boolean;
  ConnectCount: Integer;
  MessageCount: Integer;

procedure OnConnect(ReasonCode: TMQTTReasonCode);
begin
  Inc(ConnectCount);
  Writeln(Format('[%s] Connected! (connection #%d, reason: %d)',
    [FormatDateTime('hh:nn:ss', Now), ConnectCount, Ord(ReasonCode)]));

  // Re-subscribe after reconnection (subscriptions are not persisted)
  if Client.Connected then
  begin
    Writeln('  Re-subscribing to topics...');
    Client.Subscribe('demo/reconnect', procedure(const Topic: string; const Payload: TBytes)
    begin
      Inc(MessageCount);
      Writeln(Format('  [%s] Received: %s', [FormatDateTime('hh:nn:ss', Now),
        TEncoding.UTF8.GetString(Payload)]));
    end, atLeastOnce);
  end;
end;

procedure OnDisconnect(ReasonCode: TMQTTReasonCode; const ReasonString: string);
begin
  Writeln(Format('[%s] Disconnected: %s (code: %d)',
    [FormatDateTime('hh:nn:ss', Now), ReasonString, Ord(ReasonCode)]));
  Writeln('  Auto-reconnect is enabled, will try to reconnect...');
end;

procedure OnError(const ErrorMsg: string);
begin
  Writeln(Format('[%s] Error: %s', [FormatDateTime('hh:nn:ss', Now), ErrorMsg]));
end;

begin
  Randomize;
  Running := True;
  ConnectCount := 0;
  MessageCount := 0;

  try
    Client := CreateMQTTClient;

    // Configure connection
    Options.SetDefaults;
    Options.ClientID := 'Reconnect_Demo_' + IntToStr(Random(10000));
    Options.KeepAliveSec := 10;  // Short keep-alive to detect disconnects faster
    Options.CleanStart := True;

    Writeln('=== MQTT Automatic Reconnection Demo ===');
    Writeln;
    Writeln('This demo shows automatic reconnection when the broker');
    Writeln('connection is lost. Try stopping/starting your MQTT broker!');
    Writeln;
    Writeln('Keep-alive interval: 10 seconds');
    Writeln('Reconnect backoff: 1s -> 2s -> 4s -> 8s -> ... -> 30s max');
    Writeln;

    // Set up event handlers BEFORE connecting
    Client.SetOnConnect(OnConnect);
    Client.SetOnDisconnect(OnDisconnect);
    Client.SetOnError(OnError);

    // Enable auto-reconnect
    Client.AutoReconnect := True;
    Writeln('Auto-reconnect: ENABLED');
    Writeln;

    // Connect
    Writeln('Connecting to broker...');
    Client.Connect('localhost', 1883, Options);
    Writeln;

    // Main loop - publish messages periodically
    Writeln('Publishing a message every 3 seconds...');
    Writeln('Press Ctrl+C to exit.');
    Writeln;

    while Running do
    begin
      try
        if Client.Connected then
        begin
          Client.Publish('demo/reconnect',
            Format('Message at %s (msg #%d)', [FormatDateTime('hh:nn:ss', Now), MessageCount + 1]),
            atLeastOnce);
        end
        else
        begin
          case Client.State of
            Disconnected:
              Writeln(Format('[%s] State: Disconnected', [FormatDateTime('hh:nn:ss', Now)]));
            Connecting:
              Writeln(Format('[%s] State: Connecting...', [FormatDateTime('hh:nn:ss', Now)]));
            Reconnecting:
              Writeln(Format('[%s] State: Reconnecting...', [FormatDateTime('hh:nn:ss', Now)]));
          end;
        end;
      except
        on E: Exception do
          Writeln(Format('[%s] Publish error: %s', [FormatDateTime('hh:nn:ss', Now), E.Message]));
      end;

      Sleep(3000);
    end;

  except
    on E: Exception do
      Writeln('Fatal error: ', E.Message);
  end;

  // Cleanup
  if Client <> nil then
  begin
    Writeln;
    Writeln('Disconnecting...');
    Client.Disconnect;
  end;

  Writeln('Done!');
end.
