# 13_LoggerPro - LoggerPro integration

Demonstrates how to plug [LoggerPro](https://github.com/danieleteti/loggerpro)
into the Delphi MQTT client via the adapter shipped in
`src/MQTT.Logger.LoggerPro.pas`.

LoggerPro is asynchronous (worker thread per appender), thread-safe, and
ships with many ready-made appenders (console, file with rotation,
OutputDebugString, ElasticSearch, e-mail, NSQ, ...). Once it's wired in, the
MQTT client's `Logger` simply forwards each call to a LoggerPro
`ILogWriter` — your existing logging pipeline catches everything: connect,
disconnect, reconnect, subscribe, SUBACK, every TX/RX packet.

## Why a separate sample?

The library core (`MQTT.Logger` unit) ships with only:

- `TMQTTNullLogger` — the zero-overhead default
- `TMQTTProcLogger` — a trivial `reference to procedure` forwarder, useful
  for samples and tests

This keeps the core dependency-free. Production users plug in their own
logger via an adapter — this sample shows exactly that for LoggerPro.

## How the adapter looks

`src/MQTT.Logger.LoggerPro.pas` wraps an `ILogWriter` as an `IMQTTLogger`:

| `IMQTTLogger`    | `ILogWriter`          |
|------------------|-----------------------|
| `Debug(Msg)`     | `Writer.Debug(Msg, Tag)` |
| `Info(Msg)`      | `Writer.Info(Msg, Tag)`  |
| `Warning(Msg)`   | `Writer.Warn(Msg, Tag)`  |
| `Error(Msg)`     | `Writer.Error(Msg, Tag)` |

A tag (default `'MQTT'`) is attached to every entry so LoggerPro
file/console formatters can highlight or split MQTT traffic from the rest of
the app.

## Wiring it up

```pascal
uses
  LoggerPro, LoggerPro.ConsoleAppender, LoggerPro.FileAppender,
  MQTT.Logger, MQTT.Logger.LoggerPro, MQTT.Client;

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
  ...
```

## Build

```cmd
cd samples\13_LoggerPro
"C:\Program Files (x86)\Embarcadero\Studio\37.0\bin\rsvars.bat"
msbuild LoggerProDemo.dproj /p:Config=Debug /p:Platform=Win64 /t:Build
LoggerProDemo.exe
```

Console output is printed live (async via LoggerPro's worker thread), and a
rotating log file appears under `logs/LoggerProDemo.*.log`.

## Requirements

- LoggerPro source on disk. The sample's project file references it via
  `C:\DEV\LoggerPro\` — change that path to wherever you installed LoggerPro.
- Mosquitto on `localhost:1883`.
