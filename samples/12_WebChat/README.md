# 12_WebChat - Delphi ↔ Browser MQTT Chat

A three-way chat demo that bridges:

- **2 web pages** running in any modern browser, using
  [MQTT.js](https://github.com/mqttjs/MQTT.js) over **WebSocket** (port 9001)
- **1 Delphi console client** using this library's native TCP transport
  (port 1883)

All three participants subscribe to the same MQTT topic, so a message typed
in the Delphi terminal appears instantly in both browser windows — and vice
versa.

## Topology

```
    +-------------------+      ws://localhost:9001/mqtt     +-----------+
    | Browser - alice   |  <--------------------------->  |           |
    +-------------------+                                  |           |
                                                          |           |
    +-------------------+      ws://localhost:9001/mqtt    | Mosquitto |
    | Browser - bob     |  <--------------------------->  |           |
    +-------------------+                                  |           |
                                                          |           |
    +-------------------+      tcp://localhost:1883        |           |
    | Delphi - WebChat  |  <--------------------------->  |           |
    +-------------------+                                  +-----------+
```

Topics used:

| Topic                                 | What                                                       |
|---------------------------------------|------------------------------------------------------------|
| `samples/webchat/messages`            | Chat messages (JSON `{from,text,ts}`), QoS 1               |
| `samples/webchat/presence/<user>`     | Retained presence (`online` JSON; empty payload = offline) |

Last Will is set so an unexpected disconnect clears the retained presence.

## Prerequisites

1. **Mosquitto with WebSocket listener** on port 9001. From the project root:

   ```cmd
   py scripts\mosquitto\apply-websockets.py
   ```

   (requires admin; appends a listener block to `mosquitto.conf` and
   restarts the service)

2. **Python 3** (for the static file server).

3. **Delphi 12 (Athens)** to compile the console client.

## Build & Run

### 1. Compile the Delphi client

```cmd
cd samples\12_WebChat
dcc64 WebChat.dpr -U"..\..\src"
```

### 2. Start the static web server

In a separate terminal, from `samples/12_WebChat/`:

```cmd
python serve.py
```

You should see:

```
Serving samples/12_WebChat/web
  http://localhost:8080/alice.html
  http://localhost:8080/bob.html
```

### 3. Open the two browser windows

- http://localhost:8080/alice.html
- http://localhost:8080/bob.html

### 4. Launch the Delphi client

In a third terminal:

```cmd
WebChat.exe
```

Type your name (e.g. `delphi`) and start chatting. Anything you type appears
in both browser windows; messages from `alice` and `bob` appear in the Delphi
console:

```
Logged in as "delphi". Type a message and press Enter. Empty line + Enter to quit.
-------------------------------------------------------------
[2026-05-16T18:42:11.123] alice: hey from the browser
[2026-05-16T18:42:25.456] bob: same here
hello from delphi
[2026-05-16T18:42:40.789] alice: I see you :)
```

## Files

```
12_WebChat/
├── WebChat.dpr             # Delphi console client (TCP, this library)
├── serve.py                # Minimal static HTTP server for web/
├── README.md               # This file
└── web/
    ├── alice.html          # Pre-configured page for "alice"
    ├── bob.html            # Pre-configured page for "bob"
    ├── chat.js             # Shared MQTT.js startChat() helper
    └── style.css           # Shared styles
```

## Notes

- MQTT.js connects with `ws://localhost:9001/mqtt`. If you serve the pages
  over HTTPS, switch to `wss://` and configure TLS in Mosquitto accordingly.
- The pages use ES modules (`<script type="module">`), so they must be loaded
  over `http(s)://`, not `file://`. That's why `serve.py` is provided.
- Open multiple `alice.html` tabs to test concurrent clients sharing the same
  username (each gets a unique MQTT clientId).
