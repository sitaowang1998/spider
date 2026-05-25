#!/usr/bin/env python3
"""AMI state helpers for AWS benchmark automation."""

from __future__ import annotations

import json
import pathlib
import time


ROOT = pathlib.Path(__file__).resolve().parents[4]
DEFAULT_AMI_STATE = ROOT / ".aws-bench/ami/latest.json"


def default_ami_state_path() -> pathlib.Path:
    return DEFAULT_AMI_STATE


def load_ami_state(path: pathlib.Path) -> dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_ami_state(path: pathlib.Path, data: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def timestamp() -> int:
    return int(time.time())
