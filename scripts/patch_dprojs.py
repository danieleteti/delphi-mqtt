#!/usr/bin/env python3
"""
One-shot patcher to bring all existing .dproj files in sync with:
  * the current src/ unit list (adds MQTT.Logger.pas as a DCCReference if missing)
  * Win64 platform enabled (in the IDE Platforms list and as a build target)

Idempotent: re-running the script does not duplicate references or property
groups.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DPROJS = sorted(REPO.rglob("*.dproj"))

WIN64_GROUP_MARKER = "Base_Win64"

WIN64_BASE_GROUP = """    <PropertyGroup Condition="('$(Platform)'=='Win64' and '$(Base)'=='true') or '$(Base_Win64)'!=''">
        <Base_Win64>true</Base_Win64>
        <CfgParent>Base</CfgParent>
        <Base>true</Base>
    </PropertyGroup>
"""

WIN64_NAMESPACE_GROUP = """    <PropertyGroup Condition="'$(Base_Win64)'!=''">
        <DCC_Namespace>Winapi;System.Win;Data.Win;Datasnap.Win;Web.Win;Soap.Win;Xml.Win;$(DCC_Namespace)</DCC_Namespace>
        <BT_BuildType>Debug</BT_BuildType>
        <VerInfo_Keys>CompanyName=;FileDescription=$(MSBuildProjectName);FileVersion=1.0.0.0;InternalName=;LegalCopyright=;LegalTrademarks=;OriginalFilename=;ProgramID=com.embarcadero.$(MSBuildProjectName);ProductName=$(MSBuildProjectName);ProductVersion=1.0.0.0;Comments=</VerInfo_Keys>
        <VerInfo_Locale>1033</VerInfo_Locale>
    </PropertyGroup>
"""


def patch_one(path: Path) -> list[str]:
    notes: list[str] = []
    text = path.read_text(encoding="utf-8")
    orig = text

    # ----------------------------------------------------------------------
    # 1. Add MQTT.Logger.pas DCCReference if missing AND if MQTT.Client.pas
    #    is already referenced. Insert BEFORE MQTT.Client.pas so the unit
    #    list compiles in dependency order (Logger is used by Client).
    # ----------------------------------------------------------------------
    if re.search(r"MQTT\.Client\.pas", text, re.IGNORECASE) and not re.search(r"MQTT\.Logger\.pas", text, re.IGNORECASE):
        m = re.search(r'([ \t]*)<DCCReference Include="([^"]*?)MQTT\.Client\.pas"/>',
                      text, re.IGNORECASE)
        if m:
            indent = m.group(1)
            src_prefix = m.group(2)
            insertion = f'{indent}<DCCReference Include="{src_prefix}MQTT.Logger.pas"/>\n'
            text = text[:m.start()] + insertion + text[m.start():]
            notes.append("added MQTT.Logger.pas DCCReference (before Client)")

    # If MQTT.Logger.pas IS present but appears AFTER MQTT.Client.pas, reorder.
    if re.search(r"MQTT\.Logger\.pas", text, re.IGNORECASE) and re.search(r"MQTT\.Client\.pas", text, re.IGNORECASE):
        logger_m = re.search(r'^[ \t]*<DCCReference Include="[^"]*?MQTT\.Logger\.pas"/>\s*\n',
                             text, flags=re.MULTILINE | re.IGNORECASE)
        client_m = re.search(r'^[ \t]*<DCCReference Include="[^"]*?MQTT\.Client\.pas"/>\s*\n',
                             text, flags=re.MULTILINE | re.IGNORECASE)
        if logger_m and client_m and logger_m.start() > client_m.start():
            logger_line = logger_m.group(0)
            text = text[:logger_m.start()] + text[logger_m.end():]
            client_m = re.search(r'^[ \t]*<DCCReference Include="[^"]*?MQTT\.Client\.pas"/>\s*\n',
                                 text, flags=re.MULTILINE | re.IGNORECASE)
            text = text[:client_m.start()] + logger_line + text[client_m.start():]
            notes.append("reordered MQTT.Logger.pas before MQTT.Client.pas")

    # ----------------------------------------------------------------------
    # 1b. Ensure ..\..\src is on the unit search path so Client.pas can
    #     find Logger.pas via `uses MQTT.Logger`. Add to the Base group
    #     near DCC_Namespace.
    # ----------------------------------------------------------------------
    if re.search(r"MQTT\.Client\.pas", text, re.IGNORECASE):
        # find the src prefix used by DCCReferences
        ref_m = re.search(r'<DCCReference Include="([^"]*?)MQTT\.Client\.pas"/>',
                          text, re.IGNORECASE)
        if ref_m:
            src_path = ref_m.group(1).rstrip("\\/")  # e.g. "..\..\src"
            if "DCC_UnitSearchPath" not in text:
                # inject inside the base PropertyGroup right next to DCC_Namespace
                ns_m = re.search(r"(<DCC_Namespace>[^<]*</DCC_Namespace>)",
                                 text)
                if ns_m:
                    inject = f'\n        <DCC_UnitSearchPath>{src_path};$(DCC_UnitSearchPath)</DCC_UnitSearchPath>'
                    text = text[:ns_m.end()] + inject + text[ns_m.end():]
                    notes.append(f"added DCC_UnitSearchPath ({src_path})")

    # ----------------------------------------------------------------------
    # 2. Flip <Platform value="Win64">False</Platform> -> True
    # ----------------------------------------------------------------------
    new_text, n = re.subn(
        r'<Platform value="Win64">False</Platform>',
        '<Platform value="Win64">True</Platform>',
        text,
    )
    if n:
        text = new_text
        notes.append("enabled Win64 in IDE Platforms list")

    # ----------------------------------------------------------------------
    # 3. Add Base_Win64 PropertyGroup pair if missing.
    #    Insert them right after the Base_Win32 ones for predictability.
    # ----------------------------------------------------------------------
    if WIN64_GROUP_MARKER not in text:
        # Insert "Base group" after the equivalent Win32 one
        marker = re.search(
            r'<PropertyGroup Condition="\(\'\$\(Platform\)\'==\'Win32\' and \'\$\(Base\)\'==\'true\'\) or \'\$\(Base_Win32\)\'!=\'\'">.*?</PropertyGroup>',
            text, flags=re.DOTALL)
        if marker:
            insert_at = marker.end()
            text = text[:insert_at] + "\n" + WIN64_BASE_GROUP + text[insert_at:]
            notes.append("added Base_Win64 base property group")

        # Insert "namespace group" after the equivalent Win32 namespace group
        marker = re.search(
            r"<PropertyGroup Condition=\"'\$\(Base_Win32\)'!=''\">.*?</PropertyGroup>",
            text, flags=re.DOTALL)
        if marker:
            insert_at = marker.end()
            text = text[:insert_at] + "\n" + WIN64_NAMESPACE_GROUP + text[insert_at:]
            notes.append("added Base_Win64 namespace property group")

    if text != orig:
        path.write_text(text, encoding="utf-8")
    return notes


def main() -> int:
    if not DPROJS:
        print("No .dproj files found.")
        return 1

    print(f"Found {len(DPROJS)} .dproj files under {REPO}")
    print()

    total_changed = 0
    for dproj in DPROJS:
        rel = dproj.relative_to(REPO)
        notes = patch_one(dproj)
        if notes:
            total_changed += 1
            print(f"PATCHED  {rel}")
            for n in notes:
                print(f"    - {n}")
        else:
            print(f"OK       {rel}")

    print()
    print(f"Patched {total_changed}/{len(DPROJS)} files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
