// Shared MQTT chat logic for the WebChat sample.
// Uses MQTT.js loaded from a CDN (see alice.html / bob.html).
//
// Requires a Mosquitto broker on the same host with a WebSocket listener:
//     listener 9001
//     protocol websockets
//     allow_anonymous true
//
// See ../README.md for the full setup steps.

const TOPIC_MESSAGES = "samples/webchat/messages";
const TOPIC_PRESENCE_PREFIX = "samples/webchat/presence/";

export function startChat({ user, brokerUrl, ui }) {
  const clientId = `web-${user}-${Math.floor(Math.random() * 1e6)}`;
  const presenceTopic = TOPIC_PRESENCE_PREFIX + user;

  ui.status(`connecting to ${brokerUrl} as "${user}"…`);

  const client = mqtt.connect(brokerUrl, {
    clientId,
    clean: true,
    keepalive: 30,
    reconnectPeriod: 2000,
    // last will: clear our retained presence when we drop unexpectedly
    will: {
      topic: presenceTopic,
      payload: "",
      qos: 1,
      retain: true,
    },
  });

  client.on("connect", () => {
    ui.status(`connected to ${brokerUrl} as "${user}"`);
    client.subscribe(TOPIC_MESSAGES, { qos: 1 });
    client.subscribe(TOPIC_PRESENCE_PREFIX + "+", { qos: 1 });

    publishPresence("online");
  });

  client.on("reconnect", () => ui.status("reconnecting…"));
  client.on("close", () => ui.status("disconnected"));
  client.on("error", (err) => ui.status("ERROR: " + err.message));

  client.on("message", (topic, payloadBuf) => {
    const raw = payloadBuf.toString("utf-8");

    if (topic.startsWith(TOPIC_PRESENCE_PREFIX)) {
      const who = topic.substring(TOPIC_PRESENCE_PREFIX.length);
      if (raw === "") {
        ui.presence(`${who} left`);
      } else {
        try {
          const p = JSON.parse(raw);
          ui.presence(`${p.from || who}: ${p.status || "online"}`);
        } catch {
          ui.presence(`${who}: ${raw}`);
        }
      }
      return;
    }

    try {
      const m = JSON.parse(raw);
      if (m.from === user) return; // skip own echo
      ui.message({ from: m.from, text: m.text, ts: m.ts });
    } catch {
      ui.message({ from: "?", text: raw, ts: new Date().toISOString() });
    }
  });

  function publishChat(text) {
    const payload = JSON.stringify({
      from: user,
      text,
      ts: new Date().toISOString(),
    });
    client.publish(TOPIC_MESSAGES, payload, { qos: 1 });
    ui.message({ from: user, text, ts: new Date().toISOString(), self: true });
  }

  function publishPresence(status) {
    const payload = JSON.stringify({
      from: user,
      status,
      ts: new Date().toISOString(),
    });
    client.publish(presenceTopic, payload, { qos: 1, retain: true });
  }

  function disconnect() {
    // clear retained presence then close
    client.publish(presenceTopic, "", { qos: 1, retain: true }, () => {
      client.end();
    });
  }

  // Allow the page to react when user closes the tab
  window.addEventListener("beforeunload", disconnect);

  return { publishChat, disconnect };
}
