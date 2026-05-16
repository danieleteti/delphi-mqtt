#!/usr/bin/env python3
"""
Create the .dproj files needed by msbuild for projects that ship only a .dpr
(tests, 11_Logging, 13_LoggerPro, cross-protocol helpers).

Idempotent: writes each .dproj only if missing.
"""

from __future__ import annotations
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
from dproj_template import make_dproj  # noqa: E402

SRC = "..\\..\\src"
DUNITX = r"C:\Program Files (x86)\Embarcadero\Studio\37.0\source\DUnitX"
LOGGERPRO = r"C:\DEV\LoggerPro"

PROJECTS = [
    {
        "dproj": REPO / "samples" / "11_Logging" / "LoggingDemo.dproj",
        "main_source": "LoggingDemo.dpr",
        "project_name": "LoggingDemo",
        "dcc_references": [
            f"{SRC}\\MQTT.Types.pas",
            f"{SRC}\\MQTT.Protocol.pas",
            f"{SRC}\\MQTT.Logger.pas",
            f"{SRC}\\MQTT.Client.pas",
        ],
        "unit_search_path": SRC,
    },
    {
        "dproj": REPO / "samples" / "13_LoggerPro" / "LoggerProDemo.dproj",
        "main_source": "LoggerProDemo.dpr",
        "project_name": "LoggerProDemo",
        "dcc_references": [
            f"{SRC}\\MQTT.Types.pas",
            f"{SRC}\\MQTT.Protocol.pas",
            f"{SRC}\\MQTT.Logger.pas",
            f"{SRC}\\MQTT.Logger.LoggerPro.pas",
            f"{SRC}\\MQTT.Client.pas",
        ],
        "unit_search_path": f"{SRC};{LOGGERPRO}",
    },
    {
        "dproj": REPO / "samples" / "06_Reconnection" / "ReconnectDemo.dproj",
        "main_source": "ReconnectDemo.dpr",
        "project_name": "ReconnectDemo",
        "dcc_references": [
            f"{SRC}\\MQTT.Types.pas",
            f"{SRC}\\MQTT.Protocol.pas",
            f"{SRC}\\MQTT.Logger.pas",
            f"{SRC}\\MQTT.Client.pas",
        ],
        "unit_search_path": SRC,
    },
    {
        "dproj": REPO / "tests" / "MqttTests.dproj",
        "main_source": "MqttTests.dpr",
        "project_name": "MqttTests",
        "dcc_references": [
            "..\\src\\MQTT.Types.pas",
            "..\\src\\MQTT.Protocol.pas",
            "..\\src\\MQTT.Logger.pas",
            "..\\src\\MQTT.Client.pas",
            "MQTTProtocolTests.pas",
            "MQTTLoggerTests.pas",
            "MQTTClientTests.pas",
            "MQTTPublicBrokerTests.pas",
        ],
        "unit_search_path": f"..\\src;{DUNITX}",
    },
    {
        "dproj": REPO / "scripts" / "cross-protocol-tests" / "mqtt-pub-tcp.dproj",
        "main_source": "mqtt-pub-tcp.dpr",
        "project_name": "mqtt-pub-tcp",
        "dcc_references": [
            "..\\..\\src\\MQTT.Types.pas",
            "..\\..\\src\\MQTT.Protocol.pas",
            "..\\..\\src\\MQTT.Logger.pas",
            "..\\..\\src\\MQTT.Client.pas",
        ],
        "unit_search_path": "..\\..\\src",
    },
    {
        "dproj": REPO / "scripts" / "cross-protocol-tests" / "mqtt-sub-tcp.dproj",
        "main_source": "mqtt-sub-tcp.dpr",
        "project_name": "mqtt-sub-tcp",
        "dcc_references": [
            "..\\..\\src\\MQTT.Types.pas",
            "..\\..\\src\\MQTT.Protocol.pas",
            "..\\..\\src\\MQTT.Logger.pas",
            "..\\..\\src\\MQTT.Client.pas",
        ],
        "unit_search_path": "..\\..\\src",
    },
]


def main() -> int:
    created = 0
    for proj in PROJECTS:
        path: Path = proj["dproj"]
        if path.exists():
            print(f"OK     {path.relative_to(REPO)}")
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        xml = make_dproj(
            main_source=proj["main_source"],
            project_name=proj["project_name"],
            dcc_references=proj["dcc_references"],
            unit_search_path=proj.get("unit_search_path", ""),
        )
        path.write_text(xml, encoding="utf-8")
        created += 1
        print(f"WROTE  {path.relative_to(REPO)}")

    print()
    print(f"Created {created} new .dproj file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
