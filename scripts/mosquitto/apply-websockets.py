#!/usr/bin/env python3
"""
Append a WebSocket listener (port 9001) to the system Mosquitto config and
restart the service.

Must be run with administrator privileges (writes to C:\\Program Files\\mosquitto
and restarts a Windows service). On UAC-protected systems re-launch with:

    python -c "import subprocess, sys; subprocess.run(['powershell','-Command','Start-Process','python','-ArgumentList',\"'apply-websockets.py'\",'-Verb','RunAs'])"

or simply right-click the .py file and pick "Run as administrator" from an
elevated terminal:

    py apply-websockets.py
"""

from __future__ import annotations

import datetime as _dt
import shutil
import socket
import subprocess
import sys
from pathlib import Path

MOSQUITTO_DIR = Path(r"C:\Program Files\mosquitto")
MAIN_CONF = MOSQUITTO_DIR / "mosquitto.conf"
WS_CONF = Path(__file__).parent / "websockets.conf"
MARKER = "# === Custom listeners added by delphi-mqtt apply-websockets.py ==="


def _die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def _port_open(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _run(cmd: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, capture_output=capture, text=True)


def main() -> int:
    if not MAIN_CONF.exists():
        _die(f"main config not found: {MAIN_CONF}")
    if not WS_CONF.exists():
        _die(f"websocket config not found: {WS_CONF}")

    timestamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = MAIN_CONF.with_suffix(f".conf.bak-{timestamp}")

    print(f"Backing up {MAIN_CONF} -> {backup}")
    try:
        shutil.copy2(MAIN_CONF, backup)
    except PermissionError:
        _die("permission denied creating backup. Re-run this script as administrator.")

    existing = MAIN_CONF.read_text(encoding="utf-8", errors="replace")
    if MARKER in existing:
        print("Marker already present. Skipping append step.")
    else:
        ws_content = WS_CONF.read_text(encoding="utf-8")
        append_block = f"\n\n{MARKER}\n{ws_content}"
        print(f"Appending listener block ({len(append_block)} bytes)")
        try:
            with MAIN_CONF.open("a", encoding="utf-8", newline="") as fh:
                fh.write(append_block)
        except PermissionError:
            _die("permission denied writing config. Re-run this script as administrator.")

    print("Restarting mosquitto service")
    try:
        _run(["sc.exe", "stop", "mosquitto"], check=False)
        _run(["sc.exe", "start", "mosquitto"])
    except subprocess.CalledProcessError as exc:
        _die(f"failed to restart service (admin?): {exc}")

    # Give mosquitto a moment to bind sockets
    import time
    for _ in range(20):
        if _port_open("localhost", 9001) and _port_open("localhost", 1883):
            break
        time.sleep(0.25)

    tcp_up = _port_open("localhost", 1883)
    ws_up = _port_open("localhost", 9001)
    print()
    print("Verifying listeners:")
    print(f"  MQTT      localhost:1883 -> {'OK' if tcp_up else 'DOWN'}")
    print(f"  WebSocket localhost:9001 -> {'OK' if ws_up else 'DOWN'}")

    if tcp_up and ws_up:
        print("\nDone.")
        return 0

    print(
        "\nWARNING: at least one listener is down. Try running mosquitto manually to see the log:\n"
        f'  & "{MOSQUITTO_DIR / "mosquitto.exe"}" -c "{MAIN_CONF}" -v'
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
