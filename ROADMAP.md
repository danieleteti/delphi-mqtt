# delphi-mqtt — Roadmap

## v1.2 — Closing the gap with TMS MQTT

Items ordered by value/effort ratio (top = highest leverage).

### 1. Non-visual component wrapper (`TMQTTClient: TComponent`)
- **Value:** high — RAD users expect drop-on-form. Closes the biggest perceived gap vs TMS.
- **Effort:** low — thin `TComponent` wrapper over the existing `IMQTTClient`. Published properties for Host/Port/ClientID/KeepAlive/CleanStart/SSL options/Will. Design-time package.
- **Notes:** keep the interface-based API as the primary; the component is sugar. Provide both VCL and FMX packages.

### 2. WebSocket transport (`ws://`, `wss://`)
- **Value:** high — required by some cloud brokers (AWS IoT alternative endpoint, HiveMQ Cloud over WS), enables browser-bridge scenarios, runs through corporate proxies that block 1883/8883.
- **Effort:** medium — implement MQTT-over-WebSocket framing (RFC 6455 + MQTT sub-protocol `mqtt`). Indy has `TIdHTTP`/WebSocket bits, or use a small custom framer over `TIdTCPClient`.
- **Notes:** keep transport pluggable so future QUIC/UDP isn't a rewrite.

### 3. Explicit cross-platform support (iOS / Android / Linux / macOS)
- **Value:** high marketing impact — TMS sells on this. Indy already supports these targets.
- **Effort:** low to medium — declare supported targets, add a sample compiled for each, gate platform-specific bits (OpenSSL DLL loading is Windows-only path). Document TLS story per platform (LibreSSL on macOS/iOS, OpenSSL on Android via JNI, native on Linux).
- **Notes:** even a CI matrix that builds for each target is enough to claim parity.

### 4. Disk-backed store-and-forward for pending QoS 1/2 publishes
- **Value:** high for industrial use cases — survives app crash, not just broker disconnect.
- **Effort:** medium — pluggable `IMQTTPersistence` interface; default impl writes pending packets to a directory (one file per PacketID) and replays on next connect. In-memory remains the default.
- **Notes:** must integrate with the existing `FPendingPublishes` flow. Atomic write + rename pattern to survive crashes mid-write.

### 5. MQTT 5 Enhanced Authentication (AUTH packet, SCRAM/OAuth flows)
- **Value:** medium — enterprise IoT (Azure, custom brokers with JWT/OAuth). Nicchia oggi, crescente.
- **Effort:** medium — emit/parse AUTH packets, expose `OnAuthChallenge` callback for the multi-step flow, plumb `AuthenticationMethod` / `AuthenticationData` properties.
- **Notes:** spec §3.15. Test against EMQX with SCRAM-SHA-256.

### 6. Public CI + test coverage report
- **Value:** medium — credibility. Tests already exist in `tests/`; expose results.
- **Effort:** low — GitHub Actions matrix (Delphi via Docker image or self-hosted), badge on README, integration tests against Mosquitto in a service container.
- **Notes:** even smoke tests + a coverage badge move the needle.

### 7. Shared subscriptions ergonomics (`$share/group/topic`)
- **Value:** low to medium — works today (broker-side), but a helper API + sample makes it visible.
- **Effort:** very low — `SubscribeShared(Group, Topic, Handler, QoS)` that builds `$share/<group>/<topic>` and a worked example.
- **Notes:** zero protocol work, pure DX.

### 8. Connection pool / multi-broker failover helper
- **Value:** medium — common ask for HA setups. Today users build it themselves (see `SSLPublicBroker` cascade sample).
- **Effort:** medium — helper that takes an array of endpoints, tries them in order on connect failure, exposes `OnBrokerChanged`. Optional sticky-broker affinity.
- **Notes:** keep it as a separate unit so the core stays slim.

### 9. Documentation site (typed API reference + concept docs)
- **Value:** medium — TMS bundles a PDF dev guide; we have README only.
- **Effort:** medium — PasDoc or DocFX over the source, GitHub Pages, plus narrative pages for each concept (LWT, session, QoS, TLS, security).
- **Notes:** course slides already cover this content; reformat as docs.

### Out of scope for v1.2 (parked)

- Embedded broker / server — large effort, TMS doesn't have it either; not a competitive gap.
- QUIC transport (MQTT over QUIC, draft) — premature, no broker support widespread.
- Sparkplug B payload helper — separate add-on package if demand emerges.
