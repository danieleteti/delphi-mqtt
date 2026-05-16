# Delphi MQTT Library

[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](https://github.com/your-repo/delphimqtt)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

A native MQTT client library for Delphi supporting both MQTT 3.1.1 and MQTT 5.0.

## Features

- **Full MQTT 5.0 Support**: Protocol version 5 with properties, reason codes, and enhanced features
- **SSL/TLS Support**: Secure connections with TLS 1.2 and client certificate authentication
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
- **Pluggable Logging**: `IMQTTLogger` interface with console + file implementations
- **Packet Monitoring**: `OnPacketReceived` / `OnPacketSent` hooks for debugging and metrics
- **SUBACK Visibility**: `OnSubscribeAck` exposes the QoS granted by the broker per topic
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

## SSL/TLS Connections

Connect securely to MQTT brokers using TLS encryption.

### Basic SSL Connection

```pascal
var
  Client: IMQTTClient;
begin
  Client := CreateMQTTClient;

  // Simple SSL connection to public broker
  Client.ConnectSSL('test.mosquitto.org', 8883);

  // Or with explicit SSL options
  var SSLOptions: TMQTTSSLOptions;
  SSLOptions.SetDefaults;
  SSLOptions.Enabled := True;
  SSLOptions.Method := sslTLS1_2;
  SSLOptions.VerifyMode := sslVerifyNone;  // For testing only

  Client.Connect('test.mosquitto.org', 8883, Options, SSLOptions);
end;
```

### Client Certificate Authentication (Mutual TLS)

For AWS IoT, Azure IoT Hub, and enterprise brokers:

```pascal
var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
  SSLOptions: TMQTTSSLOptions;
begin
  Client := CreateMQTTClient;

  Options.SetDefaults;
  Options.ClientID := 'MyIoTDevice';

  SSLOptions.SetDefaults;
  SSLOptions.Enabled := True;
  SSLOptions.Method := sslTLS1_2;
  SSLOptions.VerifyMode := sslVerifyPeer;

  // Certificate files (PEM format)
  SSLOptions.CertFile := 'certs/client.crt';
  SSLOptions.KeyFile := 'certs/client.key';
  SSLOptions.RootCertFile := 'certs/AmazonRootCA1.pem';
  SSLOptions.KeyPassword := '';  // If key is encrypted

  Client.Connect('your-endpoint.iot.region.amazonaws.com', 8883, Options, SSLOptions);
end;
```

### SSL Options Reference

```pascal
TMQTTSSLOptions = record
  Enabled: Boolean;              // Enable SSL/TLS
  CertFile: string;              // Client certificate (PEM)
  KeyFile: string;               // Private key (PEM)
  RootCertFile: string;          // CA root certificate (PEM)
  KeyPassword: string;           // Private key password
  Method: TMQTTSSLMethod;        // TLS version
  VerifyMode: TMQTTSSLVerifyMode; // Certificate verification
  VerifyDepth: Integer;          // Verification chain depth
end;

// TLS versions
TMQTTSSLMethod = (sslAuto, sslTLS1, sslTLS1_1, sslTLS1_2, sslTLS1_3);

// Verification modes
TMQTTSSLVerifyMode = (sslVerifyNone, sslVerifyPeer);
```

### Tested Public SSL Brokers

The following public MQTT brokers have been tested with SSL/TLS support:

| Broker | Host | Port | Status |
|--------|------|------|--------|
| Eclipse Mosquitto | test.mosquitto.org | 8883 | ✅ Working |
| HiveMQ Public | broker.hivemq.com | 8883 | ✅ Working |
| EMQX Public | broker.emqx.io | 8883 | ⚠️ May have connectivity issues |
| Eclipse IoT | mqtt.eclipseprojects.io | 8883 | ⚠️ May have connectivity issues |

Run the `TestAllBrokers.dpr` sample in `samples/10_SSL` to verify connectivity from your network.

### OpenSSL Requirements

SSL/TLS requires OpenSSL 1.0.2 DLLs (Indy uses the older API):
- **32-bit and 64-bit**: `libeay32.dll`, `ssleay32.dll`

Download OpenSSL 1.0.2 from: https://slproweb.com/products/Win32OpenSSL.html

Place the DLLs in the same folder as your executable or in the system PATH.

**Note**: Newer OpenSSL 1.1.x/3.x DLLs (`libssl-1_1-x64.dll`, `libcrypto-1_1-x64.dll`) are NOT compatible with Indy's default SSL implementation. Use the legacy 1.0.2 DLLs.

### SSL Samples

Three SSL samples are provided in `samples/10_SSL/`:

1. **SSLPublicBroker.dpr** - Interactive demo connecting to a public broker with TLS
2. **SSLClientCert.dpr** - Template for AWS IoT / Azure IoT Hub with mutual TLS
3. **TestAllBrokers.dpr** - Automated test of all public SSL brokers

## Logging

The client exposes an `IMQTTLogger` interface so the core library stays free
of any external logging dependency. `MQTT.Logger` ships only two trivial
implementations:

- `TMQTTNullLogger` — the default. Zero-overhead no-op when no logger is set.
- `TMQTTProcLogger` — forwards every call to a `reference to procedure`
  callback. Use it from sample code or quick scripts when you want to see
  what the client is doing without pulling in a real logger.

For production, plug your existing logging framework via an adapter. A ready
adapter for **[LoggerPro](https://github.com/danieleteti/loggerpro)**
(async, multi-appender, thread-safe) ships in
`src/MQTT.Logger.LoggerPro.pas`. Other frameworks (CodeSiteLogging, log4d,
your in-house writer) take ~30 lines to wrap — see the LoggerPro adapter as
a template.

Log levels (lowest to highest): `llDebug`, `llInfo`, `llWarning`, `llError`, `llNone`.

### Quick & dirty: inline procedure logger

```pascal
uses MQTT.Client, MQTT.Logger, MQTT.Types;

var
  Client: IMQTTClient;
begin
  Client := CreateMQTTClient;

  Client.Logger := CreateProcLogger(
    procedure(Level: TMQTTLogLevel; const Msg: string)
    begin
      Writeln(Format('[%-5s] %s', [LogLevelName(Level), Msg]));
    end,
    llDebug);

  Client.Connect('localhost', 1883);
end;
```

### Production: LoggerPro adapter

```pascal
uses
  LoggerPro, LoggerPro.ConsoleAppender, LoggerPro.FileAppender,
  MQTT.Client, MQTT.Logger, MQTT.Logger.LoggerPro, MQTT.Types;

var
  Log: ILogWriter;
  Client: IMQTTClient;
begin
  Log := BuildLogWriter([
    TLoggerProConsoleAppender.Create,
    TLoggerProFileAppender.Create(5, 1000, 'logs')   // 5 backups, 1 MB each
  ]);

  Client := CreateMQTTClient;
  Client.Logger := WrapLoggerPro(Log, 'MQTT', llDebug);

  Client.Connect('localhost', 1883);
end;
```

The adapter tags every entry (default `'MQTT'`) so file/console formatters can
split MQTT traffic from the rest of your app. See
`samples/13_LoggerPro/` for a runnable example.

> **Note**: `MQTT.Logger.LoggerPro.pas` is optional. Add it to your project
> only when you want LoggerPro to receive the client's log messages — the
> core unit `MQTT.Client` never links against LoggerPro.

## Packet Monitoring

Hook every inbound and outbound packet for debugging, metrics or audit:

```pascal
Client.SetOnPacketSent(
  procedure(PT: TMQTTPacketType; const Raw: TBytes)
  begin
    Writeln('TX ', GetEnumName(TypeInfo(TMQTTPacketType), Ord(PT)),
            ' (', Length(Raw), ' bytes)');
  end);

Client.SetOnPacketReceived(
  procedure(PT: TMQTTPacketType; const Raw: TBytes)
  begin
    // count, dump, store, ...
  end);
```

## Subscription Acknowledgment

Get the QoS actually granted by the broker per topic filter (or detect rejection):

```pascal
Client.SetOnSubscribeAck(
  procedure(PacketID: Word; const GrantedQoS: TArray<Byte>)
  var
    Code: Byte;
  begin
    for Code in GrantedQoS do
      if Code >= $80 then
        Writeln('Subscription rejected: code=', Code)
      else
        Writeln('Granted QoS=', Code);
  end);
```

A value `>= $80` (typically `128`) means the broker refused the subscription
(e.g. not authorized, wildcard not supported).

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
│   ├── MQTT.Types.pas              # Type definitions, exceptions, enums
│   ├── MQTT.Protocol.pas           # MQTT protocol encoding/decoding
│   ├── MQTT.Logger.pas             # IMQTTLogger + Null/Proc impls (zero deps)
│   ├── MQTT.Logger.LoggerPro.pas   # OPTIONAL adapter for LoggerPro
│   └── MQTT.Client.pas             # High-level client implementation
├── samples/
│   ├── 01_Connect/          # Basic connection example
│   ├── 02_PublishSubscribe/ # Pub/Sub messaging
│   ├── 03_Performance/      # Throughput benchmarking
│   ├── 04_QoS/              # QoS levels demonstration
│   ├── 05_Wildcards/        # Topic wildcard patterns
│   ├── 06_Reconnection/     # Auto-reconnect feature
│   ├── 07_WillMessage/      # Last Will and Testament
│   ├── 08_MethodPointers/   # Using method pointers (of object)
│   ├── 09_VCL_Chat/         # Multi-instance VCL chat application
│   ├── 10_SSL/              # SSL/TLS secure connections
│   │   ├── SSLPublicBroker.dpr   # Connect to public SSL brokers
│   │   ├── SSLClientCert.dpr     # Mutual TLS with client certs
│   │   └── TestAllBrokers.dpr    # Test all public SSL brokers
│   ├── 11_Logging/          # IMQTTLogger interface + packet events + SUBACK event
│   ├── 12_WebChat/          # Delphi (TCP) <-> 2 browser pages (WebSocket)
│   └── 13_LoggerPro/        # Wiring LoggerPro as the MQTT logger (async)
├── tests/
│   ├── MqttTests.dpr               # DUnitX runner
│   ├── MQTTProtocolTests.pas       # Protocol unit tests (no broker)
│   ├── MQTTLoggerTests.pas         # Logger unit tests (no broker)
│   ├── MQTTClientTests.pas         # Client integration tests (localhost:1883)
│   └── MQTTPublicBrokerTests.pas   # Public broker tests (Mosquitto/HiveMQ/EMQX, plain + SSL)
└── scripts/
    └── build.py             # Build automation
```

## Examples

### 08_MethodPointers - Using Method Pointers
Demonstrates how to use traditional Delphi method pointers (`of object`) instead of anonymous methods for event handlers.

### 09_VCL_Chat - Multi-Instance Chat
A complete VCL chat application demonstrating multiple clients exchanging
JSON messages with presence announcements and thread-safe UI updates. Run
multiple instances and join the same room to chat.

### 11_Logging - IMQTTLogger interface
Shows how to subscribe to every log line, every TX/RX packet and every
SUBACK confirmation using only the trivial inline `TMQTTProcLogger`. No
external logging dependency required.

### 12_WebChat - Delphi ↔ 2 browser pages
Three-way chat where a Delphi console client and two HTML pages all talk
through one Mosquitto broker — the Delphi side over **TCP**, the browser
pages over **WebSocket** (via [MQTT.js](https://github.com/mqttjs/MQTT.js)).
Demonstrates retained presence + Last Will. See
`scripts/mosquitto/apply-websockets.py` to enable the WebSocket listener.

### 13_LoggerPro - Production logging via LoggerPro
Plugs [LoggerPro](https://github.com/danieleteti/loggerpro) into the MQTT
client through `WrapLoggerPro(...)`. Async worker threads, console +
rotated file appenders, tag-based filtering — all wired in three lines.

## Requirements

- Delphi 12 (Athens) — older versions work for the core library but the
  shipped `.dproj` files target RAD Studio 37.0
- Indy TCP components (standard in Delphi)
- MQTT broker (e.g., Mosquitto) for testing
- OpenSSL DLLs (for SSL/TLS connections only)
- LoggerPro (only for `samples/13_LoggerPro` and any project using the
  `MQTT.Logger.LoggerPro` adapter)

## Build (msbuild)

All projects build via **msbuild** against their `.dproj` (Win64 by
default). The repository ships a Python orchestrator:

```cmd
python scripts\build-all.py                     :: Debug / Win64, every .dproj
python scripts\build-all.py --config Release
python scripts\build-all.py --platform Win32
python scripts\build-all.py --only tests        :: filter by path substring
```

The orchestrator initializes the RAD Studio environment (`rsvars.bat`), adds
the .NET `FrameworkDir` to `PATH` for `msbuild.exe`, then runs the build
serially with a one-line PASS/FAIL summary per project.

Behind the scenes, each project is a regular Delphi `.dproj` you can also
open in the IDE. Helper scripts keep them in sync:

- `scripts/patch_dprojs.py` — idempotent patcher that ensures every `.dproj`
  has the `MQTT.Logger.pas` DCCReference, the right unit search path and
  the Win64 platform groups enabled.
- `scripts/regen_dprojs.py` + `scripts/dproj_template.py` — generate the
  `.dproj` files for new samples or tests from a Win32+Win64 console
  template.

## Testing

A DUnitX test suite ships in `tests/`. It covers four layers:

| Suite | What it covers | Broker needed |
|-------|----------------|---------------|
| `MQTTProtocolTests` | Variable-length encoding, UTF-8 strings, wildcard matching, packet build/parse round-trips | No |
| `MQTTLoggerTests` | `TMQTTNullLogger`, `TMQTTProcLogger`, level filtering, exception swallowing | No |
| `MQTTClientTests` | Connect lifecycle, QoS 0/1/2 round-trip, wildcard subscribe, packet events, SUBACK event, retain flag, logger integration | Yes — `localhost:1883` |
| `MQTTPublicBrokerTests` | Plain + SSL connect/round-trip against `test.mosquitto.org`, `broker.hivemq.com`, `broker.emqx.io` | Internet, OpenSSL DLLs for SSL |

Public broker tests soft-skip (`Assert.Pass` with reason) if the host is
unreachable or the round-trip times out, so flaky public infrastructure
does not fail the build.

### Build & run

```cmd
python scripts\build-all.py --only tests
tests\MqttTests.exe --exitbehavior:Continue
```

For SSL public-broker tests, copy `libeay32.dll` and `ssleay32.dll` next to
`MqttTests.exe` (the same DLLs used in `samples/10_SSL/`).

### Cross-protocol tests (TCP ↔ WebSocket)

`scripts/cross-protocol-tests/run_cross_tests.py` orchestrates three
round-trip scenarios between the Delphi MQTT client and a Python
paho-mqtt WebSocket client through the same Mosquitto broker:

```
delphi -> delphi      (TCP -> TCP)
delphi -> websocket   (TCP pub -> WS sub)
websocket -> delphi   (WS pub -> TCP sub)
```

Run it after enabling the Mosquitto WebSocket listener:

```cmd
python scripts\mosquitto\apply-websockets.py        :: requires admin
python scripts\build-all.py --only cross-protocol-tests
python scripts\cross-protocol-tests\run_cross_tests.py
```

## Version History

### 1.0.0 (2026-05-16)
- **Production-ready release**
- Added `IMQTTLogger` interface with `TMQTTNullLogger` (default, zero overhead)
  and `TMQTTProcLogger` (inline `reference to procedure` forwarder for samples)
- Added optional **LoggerPro adapter** in `MQTT.Logger.LoggerPro.pas`
  (`WrapLoggerPro(ILogWriter, Tag, MinLevel)`) — async, multi-appender logging
  without any dependency in the core library
- Added `Logger` property on `IMQTTClient`
- Added `OnPacketSent` / `OnPacketReceived` events for per-packet monitoring
- Added `OnSubscribeAck` event exposing the QoS granted by the broker
- Added internal log lines on connect, disconnect, reconnect, subscribe, error
- Added 11_Logging sample (inline `TMQTTProcLogger` demo)
- Added 13_LoggerPro sample (LoggerPro adapter with console + rotated file
  appenders)
- Added 12_WebChat sample (3-way chat: Delphi TCP + 2 browser pages over
  WebSocket on the same Mosquitto broker)
- Migrated build system to **msbuild** against `.dproj` (Win64 by default).
  `scripts/build-all.py` orchestrates an end-to-end build; helper scripts
  patch / regenerate `.dproj` files
- Added DUnitX test suite (61 tests covering protocol, logger, client
  integration, public-broker round-trips) and Python cross-protocol
  orchestrator (TCP↔WebSocket)
- Verified retain flag exposure in `Publish` API

### 0.9.0 (2026-01-20)
- **Added SSL/TLS support** with TLS 1.2
- Added `ConnectSSL()` methods for easy secure connections
- Added `TMQTTSSLOptions` for fine-grained SSL configuration
- Added client certificate authentication (Mutual TLS)
- Added SSL samples (10_SSL):
  - `SSLPublicBroker.dpr` - Connect to public SSL brokers
  - `SSLClientCert.dpr` - Mutual TLS for AWS/Azure IoT
  - `TestAllBrokers.dpr` - Automated testing of all public brokers
- Tested with Eclipse Mosquitto, HiveMQ, EMQX, Eclipse IoT
- Requires OpenSSL 1.0.2 DLLs (libeay32.dll, ssleay32.dll)

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
