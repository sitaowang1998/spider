#!/usr/bin/env python3
"""Progress logging helpers for AWS benchmark scripts."""

from __future__ import annotations

import time


def log(script: str, message: str) -> None:
    print(f"[{script}] {time.strftime('%Y-%m-%d %H:%M:%S')} {message}", flush=True)
