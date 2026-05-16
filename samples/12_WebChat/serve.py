#!/usr/bin/env python3
"""Serve the web/ folder over plain HTTP on http://localhost:8080.

The pages use ES modules (import statements), which require an HTTP origin
- file:// won't work. Run this script, then open in two browser windows:
    http://localhost:8080/alice.html
    http://localhost:8080/bob.html
"""

from __future__ import annotations

import http.server
import os
import socketserver
import sys
from pathlib import Path

PORT = int(os.environ.get("PORT", 8080))
ROOT = Path(__file__).parent / "web"


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def log_message(self, fmt, *args):
        sys.stderr.write("[serve] " + fmt % args + "\n")


def main() -> int:
    if not ROOT.exists():
        print(f"web/ folder missing at {ROOT}", file=sys.stderr)
        return 1
    with socketserver.ThreadingTCPServer(("", PORT), Handler) as httpd:
        print(f"Serving {ROOT}")
        print(f"  http://localhost:{PORT}/alice.html")
        print(f"  http://localhost:{PORT}/bob.html")
        print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
