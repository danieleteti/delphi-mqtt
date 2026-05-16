#!/usr/bin/env python3
"""
Build every .dproj in the repository via msbuild (Win64 by default).

Why a Python wrapper:
  * msbuild has to inherit the RAD Studio env vars set by rsvars.bat;
  * we want one tidy summary instead of pages of MSBuild noise;
  * we want a non-zero exit code if any single project fails.

Usage:
    python scripts/build-all.py                  # Debug / Win64
    python scripts/build-all.py --config Release
    python scripts/build-all.py --platform Win32
    python scripts/build-all.py --verbose
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

BDS_DEFAULT = r"C:\Program Files (x86)\Embarcadero\Studio\37.0"
BDS = os.environ.get("BDS", BDS_DEFAULT)
RSVARS = Path(BDS) / "bin" / "rsvars.bat"

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
BOLD = "\033[1m"
RESET = "\033[0m"


def colour(s: str, code: str) -> str:
    return f"{code}{s}{RESET}" if sys.stdout.isatty() else s


@dataclass
class BuildResult:
    project: Path
    ok: bool
    elapsed_ms: int
    tail: str


def run_msbuild(dproj: Path, config: str, platform: str, verbose: bool) -> BuildResult:
    import tempfile, time
    started = time.time()
    verbosity = "normal" if verbose else "minimal"
    # Write a tiny .bat to sidestep cmd.exe's lossy quote handling.
    bat = tempfile.NamedTemporaryFile(
        mode="w", suffix=".bat", delete=False, encoding="ascii"
    )
    # rsvars.bat on this install only sets BDS; we need to add the .NET
    # framework directory (msbuild lives there) and BDS\bin to PATH ourselves.
    bat.write(
        f'@echo off\r\n'
        f'call "{RSVARS}" >nul\r\n'
        f'set "FrameworkDir=%SystemRoot%\\Microsoft.NET\\Framework\\v4.0.30319"\r\n'
        f'set "PATH=%BDS%\\bin;%FrameworkDir%;%PATH%"\r\n'
        f'msbuild "{dproj}" /p:Config={config} /p:Platform={platform} '
        f'/v:{verbosity} /nologo /t:Build\r\n'
    )
    bat.close()
    try:
        proc = subprocess.run([bat.name], capture_output=True, text=True)
    finally:
        try:
            Path(bat.name).unlink()
        except Exception:
            pass
    elapsed = int((time.time() - started) * 1000)
    output = (proc.stdout or "") + (proc.stderr or "")
    tail = "\n".join(output.strip().splitlines()[-8:])
    return BuildResult(dproj, proc.returncode == 0, elapsed, tail)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="Debug")
    parser.add_argument("--platform", default="Win64")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--only", action="append", default=[],
                        help="substring filter on .dproj path (repeatable)")
    args = parser.parse_args()

    if not RSVARS.exists():
        print(colour(f"rsvars.bat not found at {RSVARS}", RED), file=sys.stderr)
        print("Set BDS env var to your RAD Studio install root if it lives elsewhere.")
        return 4

    dprojs = sorted(REPO.rglob("*.dproj"))
    if args.only:
        dprojs = [d for d in dprojs if any(s.lower() in str(d).lower() for s in args.only)]
    if not dprojs:
        print(colour("No .dproj files matched.", YELLOW))
        return 1

    print(colour(f"Building {len(dprojs)} project(s) | {args.config} / {args.platform}", BOLD))
    print()

    results: list[BuildResult] = []
    for dproj in dprojs:
        rel = dproj.relative_to(REPO)
        print(f"  building {rel} ...", end="", flush=True)
        r = run_msbuild(dproj, args.config, args.platform, args.verbose)
        results.append(r)
        status = colour("PASS", GREEN) if r.ok else colour("FAIL", RED)
        print(f" {status} ({r.elapsed_ms} ms)")
        if not r.ok or args.verbose:
            for line in r.tail.splitlines():
                print(f"      {line}")

    print()
    ok = sum(1 for r in results if r.ok)
    summary_colour = GREEN if ok == len(results) else RED
    print(colour(f"Summary: {ok}/{len(results)} built", summary_colour))
    return 0 if ok == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
