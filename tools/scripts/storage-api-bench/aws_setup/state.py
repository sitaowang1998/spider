#!/usr/bin/env python3
"""State file helpers for AWS benchmark automation."""

from __future__ import annotations

import json
import pathlib
import time


def default_state(run_id: str) -> dict[str, object]:
    return {
        "run_id": run_id,
        "created_at": int(time.time()),
        "resources": {},
    }


def load_state(path: pathlib.Path) -> dict[str, object]:
    if not path.exists():
        return default_state(path.parent.name)
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: pathlib.Path, state: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
