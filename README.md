# Delphi MQTT Library

[![Version](https://img.shields.io/badge/version-0.8.0-blue.svg)](https://github.com/your-repo/delphimqtt)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

A native MQTT client library for Delphi supporting both MQTT 3.1.1 and MQTT 5.0.

## Features

- **Full MQTT 5.0 Support**: Protocol version 5 with properties, reason codes, and enhanced features
- **Complete QoS Support**: QoS 0, 1, and 2 with proper acknowledgment flows
- **Topic Wildcards**: Full support for `+` (single-level) and `#` (multi-level) wildcards
- **Automatic Reconnection**: Configurable auto-reconnect with exponential backoff
- **Keep-Alive**: Automatic PINGREQ/PINGRESP to maintain connection
- **Last Will & Testament**: Configure messages sent on unexpected disconnect
- **Asynchronous Architecture**: Non-blocking I/O with Delphi's Parallel Programming Library
- **Thread-Safe**: Proper synchronization for multi-threaded access
- **Flexible Event Handlers**: Support for both anonymous methods and method pointers (`of object`)
- **Extended Subscriptions**: Access to Dup flag and QoS for redelivery detection
- **Manual Acknowledgment**: Fine-grained control over message acknowledgment
- **Synchronous Publishing**: Optional blocking publish with acknowledgment wait
- **Minimal Dependencies**: Only requires Indy (bundled with Delphi)

## Quick Start

```pascal
uses
  MQTT.Client, MQTT.Types;

var
  Client: IMQTTClient;
begin
  Client := CreateMQTTClient;

  // Connect to broker
  Client.Connect('localhost', 1883);

  // Subscribe with wildcard and anonymous handler
  Client.Subscribe('sensors/+/temperature',
    procedure(const Topic: string; const Payload: TBytes)
    begin
      Writeln(Format('%s = %s', [Topic, TEncoding.UTF8.GetString(Payload)]));
    end,
    atLeastOnce);

  // Publish a message
  Client.Publish('sensors/kitchen/temperature', '23.5', atLeastOnce);

  // Disconnect when done
  Client.Disconnect;
end;
```

## Subscription Modes

The library offers three subscription modes to fit different use cases:

### Standard Subscribe
Simple handler receiving topic and payload. Messages are automatically acknowledged.

```pascal
Client.Subscribe('topic/#',
  procedure(const Topic: string; const Payload: TBytes)
  begin
    Writeln('Received: ', TEncoding.UTF8.GetString(Payload));
  end,
  atLeastOnce);
```

### Extended Subscribe (SubscribeEx)
Handler receives additional `Dup` flag and `QoS` level. Useful for detecting redelivered messages.

```pascal
Client.SubscribeEx('topic/#',
  procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS)
  begin
    if Dup then
      Writeln('WARNING: This is a redelivered message!');
    Writeln('Topic: ', Topic, ' QoS: ', Ord(QoS));
  end,
  atLeastOnce);
```

### Manual Acknowledgment (SubscribeManualAck)
Full control over message acknowledgment. Set `Ack := True` to acknowledge, or leave as `False` to have the broker redeliver the message.

```pascal
Client.SubscribeManualAck('critical/events',
  procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; var Ack: Boolean)
  begin
    try
      ProcessMessage(Payload);  // Your processing logic
      Ack := True;              // Acknowledge on success
    except
      Ack := False;             // Don't acknowledge - broker will redeliver
    end;
  end,
  atLeastOnce);
```

## Method Pointer Handlers

All subscription and event methods support both anonymous methods and traditional method pointers (`of object`):

```pascal
type
  TMyForm = class(TForm)
  private
    FClient: IMQTTClient;
    procedure HandleMessage(const Topic: string; const Payload: TBytes);
    procedure HandleDisconnect(ReasonCode: TMQTTReasonCode; const ReasonString: string);
  end;

procedure TMyForm.FormCreate(Sender: TObject);
begin
  FClient := CreateMQTTClient;

  // Using method pointers (of object)
  FClient.Subscribe('my/topic', HandleMessage, atLeastOnce);
  FClient.SetOnDisconnect(HandleDisconnect);
end;

procedure TMyForm.HandleMessage(const Topic: string; const Payload: TBytes);
begin
  Memo1.Lines.Add(TEncoding.UTF8.GetString(Payload));
end;

procedure TMyForm.HandleDisconnect(ReasonCode: TMQTTReasonCode; const ReasonString: string);
begin
  StatusBar.SimpleText := 'Disconnected: ' + ReasonString;
end;
```

## Advanced Connection Options

```pascal
var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
begin
  Client := CreateMQTTClient;

  // Configure connection
  Options.SetDefaults;
  Options.ClientID := 'MyDelphiApp';
  Options.KeepAliveSec := 60;
  Options.CleanStart := True;
  Options.Version := MQTT5;
  Options.Username := 'user';
  Options.Password := 'pass';

  // Configure Last Will message
  Options.Will.Enabled := True;
  Options.Will.Topic := 'clients/MyDelphiApp/status';
  Options.Will.Payload := TEncoding.UTF8.GetBytes('OFFLINE');
  Options.Will.QoS := atLeastOnce;
  Options.Will.Retain := True;

  // Enable auto-reconnect
  Client.AutoReconnect := True;

  // Set up event handlers
  Client.SetOnConnect(procedure(ReasonCode: TMQTTReasonCode)
  begin
    Writeln('Connected!');
  end);

  Client.SetOnDisconnect(procedure(ReasonCode: TMQTTReasonCode; const Reason: string)
  begin
    Writeln('Disconnected: ' + Reason);
  end);

  Client.SetOnError(procedure(const ErrorMsg: string)
  begin
    Writeln('Error: ' + ErrorMsg);
  end);

  // Connect with options
  Client.Connect('localhost', 1883, Options);
end;
```

## Quality of Service (QoS)

| Level | Name | Description |
|-------|------|-------------|
| 0 | At Most Once | Fire and forget, no acknowledgment |
| 1 | At Least Once | Guaranteed delivery with PUBACK |
| 2 | Exactly Once | Guaranteed single delivery (4-way handshake) |

```pascal
// QoS 0 - Fast, no guarantee
Client.Publish('topic', 'data', atMostOnce);

// QoS 1 - Guaranteed delivery (may arrive multiple times)
Client.Publish('topic', 'important', atLeastOnce);

// QoS 2 - Exactly once delivery
Client.Publish('topic', 'critical', exactlyOnce);

// Synchronous publish - wait for acknowledgment
if Client.PublishSync('topic', Payload, atLeastOnce, False, 5000) then
  Writeln('Message acknowledged!')
else
  Writeln('Timeout!');
```

### Understanding Dup Flag and Redelivery

With QoS 1 and 2, the broker guarantees delivery but messages may be redelivered if acknowledgment is lost. The `Dup` flag indicates a redelivered message:

```pascal
Client.SubscribeEx('orders/#',
  procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS)
  var
    OrderID: string;
  begin
    OrderID := ExtractOrderID(Payload);

    if Dup then
    begin
      // This message was redelivered - check if we already processed it
      if OrderAlreadyProcessed(OrderID) then
        Exit;  // Skip duplicate processing
    end;

    ProcessOrder(Payload);
    MarkOrderAsProcessed(OrderID);
  end,
  atLeastOnce);
```

## Topic Wildcards

```pascal
// Single-level wildcard (+) - matches exactly one level
Client.Subscribe('home/+/temperature', Handler);
// Matches: home/kitchen/temperature, home/bedroom/temperature
// Does NOT match: home/temperature, home/floor1/room2/temperature

// Multi-level wildcard (#) - matches any number of levels
Client.Subscribe('sensors/#', Handler);
// Matches: sensors, sensors/temp, sensors/temp/indoor

// Combined wildcards
Client.Subscribe('+/status/#', Handler);
// Matches: device1/status, device1/status/cpu, pump/status/flow/rate
```

## Automatic Reconnection

```pascal
Client.AutoReconnect := True;  // Enable auto-reconnect

// Reconnection uses exponential backoff:
// 1s -> 2s -> 4s -> 8s -> 16s -> 30s (max)

// OnConnect fires on initial connect AND reconnect
Client.SetOnConnect(procedure(ReasonCode: TMQTTReasonCode)
begin
  // Re-subscribe after reconnection (subscriptions not persisted)
  Client.Subscribe('my/topic', MyHandler);
end);
```

## Threading Model

All message handlers execute in a **background thread**. For UI updates, use `TThread.Queue`:

```pascal
Client.Subscribe('topic',
  procedure(const Topic: string; const Payload: TBytes)
  begin
    // This runs in background thread
    TThread.Queue(nil, procedure
    begin
      // This runs in main UI thread
      Memo1.Lines.Add(TEncoding.UTF8.GetString(Payload));
    end);
  end);
```

## API Reference

### IMQTTClient Interface

```pascal
// Connection
procedure Connect(const Host: string; Port: Word = 1883);
procedure Connect(const Host: string; Port: Word; const Options: TMQTTConnectOptions);
procedure Disconnect;
function IsConnected: Boolean;
function GetState: TMQTTConnectionState;

// Publishing
procedure Publish(const Topic, Payload: string; QoS: TMQTTQoS = atMostOnce; Retain: Boolean = False);
procedure Publish(const Topic: string; const Payload: TBytes; QoS: TMQTTQoS = atMostOnce; Retain: Boolean = False);
function PublishSync(const Topic: string; const Payload: TBytes; QoS: TMQTTQoS; Retain: Boolean; TimeoutMs: Cardinal = 5000): Boolean;

// Subscribing - Standard (auto-ACK)
procedure Subscribe(const Topic: string; Handler: TMQTTHandler; QoS: TMQTTQoS = atMostOnce);
procedure Subscribe(const Topic: string; Handler: TMQTTMessageEvent; QoS: TMQTTQoS = atMostOnce);

// Subscribing - Extended with Dup/QoS (auto-ACK)
procedure SubscribeEx(const Topic: string; Handler: TMQTTExtendedHandler; QoS: TMQTTQoS = atMostOnce);
procedure SubscribeEx(const Topic: string; Handler: TMQTTExtendedEvent; QoS: TMQTTQoS = atMostOnce);

// Subscribing - Manual acknowledgment
procedure SubscribeManualAck(const Topic: string; Handler: TMQTTManualAckHandler; QoS: TMQTTQoS = atLeastOnce);
procedure SubscribeManualAck(const Topic: string; Handler: TMQTTManualAckEvent; QoS: TMQTTQoS = atLeastOnce);

procedure Unsubscribe(const Topic: string);

// Configuration
property AutoReconnect: Boolean;
procedure SetOnConnect(Handler: TMQTTConnectHandler);
procedure SetOnDisconnect(Handler: TMQTTDisconnectHandler);
procedure SetOnError(Handler: TMQTTErrorHandler);
```

### Handler Types

```pascal
// Anonymous method handlers
TMQTTHandler = reference to procedure(const Topic: string; const Payload: TBytes);
TMQTTExtendedHandler = reference to procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS);
TMQTTManualAckHandler = reference to procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; var Ack: Boolean);

// Method pointer handlers (of object)
TMQTTMessageEvent = procedure(const Topic: string; const Payload: TBytes) of object;
TMQTTExtendedEvent = procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS) of object;
TMQTTManualAckEvent = procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; var Ack: Boolean) of object;
```

### TMQTTConnectOptions Record

```pascal
ClientID: string;              // Client identifier
KeepAliveSec: Word;           // Keep-alive interval (default: 60)
CleanStart: Boolean;          // Clean session flag (default: True)
Version: TMQTTVersion;        // MQTT311 or MQTT5 (default: MQTT5)
Username: string;             // Authentication username
Password: string;             // Authentication password
Will: TMQTTWillOptions;       // Last Will and Testament
SessionExpiryInterval: Cardinal;  // MQTT 5.0 session expiry
ReceiveMaximum: Word;         // MQTT 5.0 receive maximum
MaxPacketSize: Cardinal;      // MQTT 5.0 max packet size
```

### TMQTTWillOptions Record

```pascal
Enabled: Boolean;             // Enable will message
Topic: string;                // Will topic
Payload: TBytes;              // Will message content
QoS: TMQTTQoS;               // Will QoS level
Retain: Boolean;              // Will retain flag
DelayInterval: Cardinal;      // MQTT 5.0 will delay
```

## Project Structure

```
delphimqtt/
├── src/
│   ├── MQTT.Types.pas       # Type definitions, exceptions, enums
│   ├── MQTT.Protocol.pas    # MQTT protocol encoding/decoding
│   └── MQTT.Client.pas      # High-level client implementation
├── samples/
│   ├── 01_Connect/          # Basic connection example
│   ├── 02_PublishSubscribe/ # Pub/Sub messaging
│   ├── 03_Performance/      # Throughput benchmarking
│   ├── 04_QoS/              # QoS levels demonstration
│   ├── 05_Wildcards/        # Topic wildcard patterns
│   ├── 06_Reconnection/     # Auto-reconnect feature
│   ├── 07_WillMessage/      # Last Will and Testament
│   ├── 08_MethodPointers/   # Using method pointers (of object)
│   └── 09_VCL_Chat/         # Multi-instance VCL chat application
├── tests/
│   └── MqttTests.dpr        # DUnitX unit tests
└── scripts/
    └── build.py             # Build automation
```

## Examples

### 08_MethodPointers - Using Method Pointers
Demonstrates how to use traditional Delphi method pointers (`of object`) instead of anonymous methods for event handlers.

### 09_VCL_Chat - Multi-Instance Chat
A complete VCL chat application demonstrating:
- Multiple clients in separate application instances
- Real-time message exchange with JSON payloads
- Presence announcements (online/offline status)
- Room-based conversations
- Thread-safe UI updates

Run multiple instances and join the same room to chat!

## Requirements

- Delphi 10.3 Rio or newer
- Indy TCP components (standard in Delphi)
- MQTT broker (e.g., Mosquitto) for testing

## Version History

### 0.8.0 (2026-01-19)
- Added `SubscribeEx` for extended handlers with Dup flag and QoS info
- Added `SubscribeManualAck` for manual message acknowledgment control
- Added method pointer (`of object`) overloads for all handlers
- Added VCL Chat sample (09_VCL_Chat)
- Added Method Pointers sample (08_MethodPointers)
- Fixed race condition in message dispatch
- Fixed thread safety for console applications
- Improved QoS 2 message handling with Dup flag preservation

### 0.7.0
- Initial public release
- Full MQTT 3.1.1 and 5.0 support
- QoS 0, 1, 2 with proper acknowledgment flows
- Topic wildcards (+ and #)
- Automatic reconnection with exponential backoff
- Last Will and Testament support

## License

Apache License 2.0
