# Delphi MQTT - MQTT Client for Delphi

**Delphi MQTT** is a native MQTT 3.1.1 / 5.0 client for Embarcadero Delphi
and Object Pascal, built on top of Indy. Connect Delphi apps to any MQTT
broker (Mosquitto, HiveMQ, EMQX, AWS IoT, Azure IoT Hub) over plain TCP or
TLS, publish and subscribe with QoS 0/1/2, react to events from a few
lines of code.

[![Version](https://img.shields.io/badge/version-1.1.0-blue.svg)](https://github.com/danieleteti/delphi-mqtt)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Delphi](https://img.shields.io/badge/Delphi-10.3+-orange.svg)](https://www.embarcadero.com/products/delphi)

## What is Delphi MQTT?

A production-ready MQTT client written in Object Pascal. Use it from
console apps, services, VCL and FMX UIs, or wherever you need to talk to
an MQTT broker without dragging in a wrapper around a C library.

**Use it for:**
- IoT device telemetry and remote control
- Real-time messaging between Delphi processes
- Bridging Delphi apps to web / mobile clients over WebSocket (browser side)
- Integration with AWS IoT Core, Azure IoT Hub, HiveMQ, Mosquitto, EMQX
- Event-driven backends (sensor data, market data, chat)

**Supported Delphi versions:** 10.3 Rio and later (VCL and FireMonkey).

## Features

- **MQTT 3.1.1 and 5.0** — protocol version 5 with properties and reason codes
- **QoS 0, 1, 2** — full acknowledgment flows including 4-way QoS 2 handshake
- **SSL / TLS** — TLS 1.2, mutual TLS for AWS IoT / Azure IoT Hub
- **Topic wildcards** — `+` (single-level) and `#` (multi-level)
- **Automatic reconnection** with exponential backoff and re-subscribe
- **Last Will & Testament** — including MQTT 5 will delay interval
- **Keep-alive** — automatic PINGREQ / PINGRESP
- **Three subscribe modes** — standard auto-ACK, extended (Dup + QoS info),
  and manual ACK for fine-grained control
- **Anonymous methods AND method pointers (`of object`)** on every handler
- **Synchronous publish** with timeout (`PublishSync`)
- **Pluggable logger** — `IMQTTLogger` interface with an optional adapter
  for [LoggerPro](https://github.com/danieleteti/loggerpro) (async,
  multi-appender, zero dependency in the core)
- **Packet monitoring** — `OnPacketSent` / `OnPacketReceived` for debugging
- **SUBACK visibility** — `OnSubscribeAck` exposes broker-granted QoS
- **Thread-safe**, asynchronous I/O, zero external dependencies in the core

## Quick Start

```pascal
uses MQTT.Client, MQTT.Types;

var
  Client: IMQTTClient;
begin
  Client := CreateMQTTClient;
  Client.Connect('localhost', 1883);

  Client.Subscribe('sensors/+/temperature',
    procedure(const Topic: string; const Payload: TBytes)
    begin
      Writeln(Topic, ' = ', TEncoding.UTF8.GetString(Payload));
    end,
    atLeastOnce);

  Client.Publish('sensors/kitchen/temperature', '23.5', atLeastOnce);
end;
```

## Documentation

For the complete developer guide — installation, every API, every option,
SSL/TLS setup, logging, samples walk-through, building with msbuild,
running the DUnitX test suite, troubleshooting — see the official
documentation on the blog:

**[https://www.danieleteti.it/delphimqtt/](https://www.danieleteti.it/delphimqtt/)**

## Samples

`samples/` contains 13 runnable examples covering connection, pub/sub,
QoS, wildcards, reconnection, Last Will, SSL, logging, and a three-way
chat between a Delphi client and two browser pages over WebSocket. See
the documentation for a guided tour.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Author

**Daniele Teti** — [danieleteti.it](https://www.danieleteti.it)

---

*Keywords: Delphi MQTT client, Object Pascal MQTT, MQTT 5.0 Delphi, MQTT
3.1.1 Delphi, Delphi IoT, AWS IoT Delphi, Azure IoT Hub Delphi, Mosquitto
Delphi, HiveMQ Delphi, EMQX Delphi, Delphi MQTT over WebSocket, Indy MQTT,
TLS MQTT Delphi, mutual TLS Delphi IoT, RAD Studio MQTT.*
