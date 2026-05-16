#!/usr/bin/env python3
"""
Cross-protocol round-trip tests for the Delphi MQTT library.

Scenarios:
  1. delphi-delphi      : Delphi TCP publisher  -> Delphi TCP subscriber
  2. delphi-websocket   : Delphi TCP publisher  -> Python WebSocket subscriber
  3. websocket-delphi   : Python WebSocket pub  -> Delphi TCP subscriber

The Delphi side is driven via the helper executables
  scripts/cross-protocol-tests/mqtt-pub-tcp.exe
  scripts/cross-protocol-tests/mqtt-sub-tcp.exe

The WebSocket side uses paho-mqtt (transport="websockets").

Prerequisites:
  - Mosquitto with TCP listener on localhost:1883
  - Mosquitto with WebSocket listener on localhost:9001
    (run scripts/mosquitto/apply-websockets.py as admin to enable it)
  - paho-mqtt (`pip install paho-mqtt`)
  - Delphi helpers already compiled (.exe files next to this script)

Usage:
    python run_cross_tests.py
    python run_cross_tests.py --verbose
"""

from __future__ import annotations

import argparse
import random
import socket
import string
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import paho.mqtt.client as mqtt

HERE = Path(__file__).resolve().parent
PUB_TCP = HERE / "mqtt-pub-tcp.exe"
SUB_TCP = HERE / "mqtt-sub-tcp.exe"

TCP_HOST = "localhost"
TCP_PORT = 1883
WS_HOST = "localhost"
WS_PORT = 9001

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
BOLD = "\033[1m"
RESET = "\033[0m"


def colour(s: str, code: str) -> str:
    if sys.stdout.isatty():
        return f"{code}{s}{RESET}"
    return s


def port_open(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def random_topic(scenario: str) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
    return f"cross/{scenario}/{suffix}"


def random_payload() -> str:
    return "payload-" + "".join(random.choices(string.ascii_letters + string.digits, k=12))


@dataclass
class Result:
    name: str
    ok: bool
    detail: str = ""
    elapsed_ms: int = 0


def run_pub_helper(topic: str, payload: str, qos: int = 1, timeout: float = 8.0) -> tuple[bool, str]:
    if not PUB_TCP.exists():
        return False, f"helper not built: {PUB_TCP}"
    try:
        proc = subprocess.run(
            [str(PUB_TCP), TCP_HOST, str(TCP_PORT), topic, payload, str(qos)],
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False, "publisher helper timed out"
    if proc.returncode != 0:
        return False, f"helper exit {proc.returncode}: {proc.stdout.strip() or proc.stderr.strip()}"
    return True, proc.stdout.strip()


def run_sub_helper(topic: str, qos: int = 1, timeout_ms: int = 10_000) -> tuple[subprocess.Popen, threading.Event]:
    """Spawn the subscribe helper and return (process, ready_event)."""
    if not SUB_TCP.exists():
        raise RuntimeError(f"helper not built: {SUB_TCP}")
    proc = subprocess.Popen(
        [str(SUB_TCP), TCP_HOST, str(TCP_PORT), topic, str(qos), str(timeout_ms)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
    )
    ready = threading.Event()

    def reader():
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip("\n")
            if line == "READY":
                ready.set()
            proc._captured = getattr(proc, "_captured", []) + [line]
        # mark ready on EOF too so callers don't hang on errors
        ready.set()

    threading.Thread(target=reader, daemon=True).start()
    return proc, ready


def collect_sub_helper(proc: subprocess.Popen, timeout: float = 12.0) -> tuple[int, list[str]]:
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    captured = getattr(proc, "_captured", [])
    return proc.returncode if proc.returncode is not None else -1, captured


def make_paho(transport: str, port: int) -> mqtt.Client:
    cli = mqtt.Client(
        client_id=f"py-{transport}-{random.randint(0, 1_000_000)}",
        transport=transport,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv311,
    )
    if transport == "websockets":
        cli.ws_set_options(path="/mqtt")
    return cli


# ---------------------------------------------------------------------------
# Scenario 1: Delphi TCP pub -> Delphi TCP sub
# ---------------------------------------------------------------------------

def scenario_delphi_delphi(verbose: bool) -> Result:
    name = "delphi -> delphi (TCP)"
    started = time.time()
    topic = random_topic("dd")
    payload = random_payload()

    try:
        sub_proc, ready = run_sub_helper(topic, qos=1, timeout_ms=10_000)
        if not ready.wait(timeout=8.0):
            sub_proc.kill()
            return Result(name, False, "subscriber never reached READY")

        ok, msg = run_pub_helper(topic, payload, qos=1)
        if not ok:
            sub_proc.kill()
            return Result(name, False, f"publisher: {msg}")

        rc, lines = collect_sub_helper(sub_proc)
        if verbose:
            print(f"    sub output: {lines}")
        if rc != 0:
            return Result(name, False, f"subscriber exit={rc} output={lines}")

        received_lines = [l for l in lines if l.startswith("MSG ")]
        if not received_lines:
            return Result(name, False, "no MSG line in subscriber output")
        received = received_lines[-1][4:]
        ok = received == payload
        elapsed = int((time.time() - started) * 1000)
        return Result(name, ok,
                      "" if ok else f"payload mismatch: got '{received}' want '{payload}'",
                      elapsed)
    except Exception as e:
        return Result(name, False, f"{type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# Scenario 2: Delphi TCP pub -> Python WebSocket sub
# ---------------------------------------------------------------------------

def scenario_delphi_ws(verbose: bool) -> Result:
    name = "delphi -> websocket"
    started = time.time()
    topic = random_topic("dw")
    payload = random_payload()
    received_holder: dict = {"value": None}
    received_event = threading.Event()

    cli = make_paho("websockets", WS_PORT)

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            client.subscribe(topic, qos=1)
        else:
            received_holder["error"] = f"WS connect rc={reason_code}"
            received_event.set()

    def on_subscribe(client, userdata, mid, reason_codes, properties):
        received_holder["subscribed"] = True

    def on_message(client, userdata, msg):
        received_holder["value"] = msg.payload.decode("utf-8", errors="replace")
        received_event.set()

    cli.on_connect = on_connect
    cli.on_subscribe = on_subscribe
    cli.on_message = on_message

    try:
        cli.connect(WS_HOST, WS_PORT, keepalive=30)
        cli.loop_start()

        # wait for subscription
        for _ in range(50):
            if received_holder.get("subscribed"):
                break
            time.sleep(0.05)

        ok, msg = run_pub_helper(topic, payload, qos=1)
        if not ok:
            return Result(name, False, f"publisher: {msg}")

        if not received_event.wait(timeout=8.0):
            return Result(name, False, "websocket subscriber never received the message")

        received = received_holder.get("value")
        ok = received == payload
        elapsed = int((time.time() - started) * 1000)
        return Result(name, ok,
                      "" if ok else f"payload mismatch: got '{received}' want '{payload}'",
                      elapsed)
    except Exception as e:
        return Result(name, False, f"{type(e).__name__}: {e}")
    finally:
        try:
            cli.loop_stop()
            cli.disconnect()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Scenario 3: Python WebSocket pub -> Delphi TCP sub
# ---------------------------------------------------------------------------

def scenario_ws_delphi(verbose: bool) -> Result:
    name = "websocket -> delphi"
    started = time.time()
    topic = random_topic("wd")
    payload = random_payload()

    try:
        sub_proc, ready = run_sub_helper(topic, qos=1, timeout_ms=10_000)
        if not ready.wait(timeout=8.0):
            sub_proc.kill()
            return Result(name, False, "subscriber never reached READY")

        cli = make_paho("websockets", WS_PORT)
        connected = threading.Event()

        def on_connect(c, u, flags, reason_code, props):
            if reason_code == 0:
                connected.set()

        cli.on_connect = on_connect

        try:
            cli.connect(WS_HOST, WS_PORT, keepalive=30)
            cli.loop_start()
            if not connected.wait(timeout=5.0):
                sub_proc.kill()
                return Result(name, False, "WS publisher could not connect")

            info = cli.publish(topic, payload, qos=1)
            info.wait_for_publish(timeout=5.0)
        finally:
            cli.loop_stop()
            cli.disconnect()

        rc, lines = collect_sub_helper(sub_proc)
        if verbose:
            print(f"    sub output: {lines}")
        if rc != 0:
            return Result(name, False, f"subscriber exit={rc} output={lines}")

        received_lines = [l for l in lines if l.startswith("MSG ")]
        if not received_lines:
            return Result(name, False, "no MSG line in subscriber output")
        received = received_lines[-1][4:]
        ok = received == payload
        elapsed = int((time.time() - started) * 1000)
        return Result(name, ok,
                      "" if ok else f"payload mismatch: got '{received}' want '{payload}'",
                      elapsed)
    except Exception as e:
        return Result(name, False, f"{type(e).__name__}: {e}")


# ---------------------------------------------------------------------------

def preflight() -> Optional[str]:
    if not PUB_TCP.exists() or not SUB_TCP.exists():
        return (f"Delphi helpers not built. Compile first:\n"
                f"  cd {HERE}\n"
                f'  dcc64 mqtt-pub-tcp.dpr -U"../../src"\n'
                f'  dcc64 mqtt-sub-tcp.dpr -U"../../src"')
    if not port_open(TCP_HOST, TCP_PORT):
        return f"TCP listener {TCP_HOST}:{TCP_PORT} not reachable. Is mosquitto running?"
    if not port_open(WS_HOST, WS_PORT):
        return (f"WebSocket listener {WS_HOST}:{WS_PORT} not reachable.\n"
                f"  Apply WebSocket config first (as admin):\n"
                f"    py scripts\\mosquitto\\apply-websockets.py")
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show subscriber helper stdout per scenario")
    args = parser.parse_args()

    print(colour("delphi-mqtt cross-protocol tests", BOLD))
    print(f"  TCP        : {TCP_HOST}:{TCP_PORT}")
    print(f"  WebSocket  : {WS_HOST}:{WS_PORT}")
    print()

    problem = preflight()
    if problem:
        print(colour("PREFLIGHT FAILED", RED))
        print(problem)
        return 4

    scenarios = [
        scenario_delphi_delphi,
        scenario_delphi_ws,
        scenario_ws_delphi,
    ]

    results: list[Result] = []
    for fn in scenarios:
        r = fn(args.verbose)
        results.append(r)
        status = colour("PASS", GREEN) if r.ok else colour("FAIL", RED)
        elapsed = f" ({r.elapsed_ms} ms)" if r.elapsed_ms else ""
        print(f"  [{status}] {r.name}{elapsed}")
        if not r.ok and r.detail:
            print(f"         {colour(r.detail, YELLOW)}")

    print()
    passed = sum(1 for r in results if r.ok)
    summary_colour = GREEN if passed == len(results) else RED
    print(colour(f"Summary: {passed}/{len(results)} scenarios passed", summary_colour))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
