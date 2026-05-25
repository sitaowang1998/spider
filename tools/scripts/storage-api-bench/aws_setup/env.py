#!/usr/bin/env python3
"""Environment helpers for AWS benchmark automation."""

from __future__ import annotations

import os
import pathlib


AWS_SECRET_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


def load_secret(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key in AWS_SECRET_KEYS:
            values[key] = value.strip().strip('"').strip("'")
    missing = [key for key in AWS_SECRET_KEYS[:2] if key not in values]
    if missing:
        msg = f"missing AWS secret values: {', '.join(missing)}"
        raise ValueError(msg)
    return values


def build_aws_env(
    secret_values: dict[str, str],
    *,
    region: str,
    endpoint_url: str | None,
    base_env: dict[str, str] | None = None,
) -> dict[str, str]:
    env = dict(os.environ if base_env is None else base_env)
    for key in AWS_SECRET_KEYS:
        if key in secret_values:
            env[key] = secret_values[key]
        else:
            env.pop(key, None)
    env["AWS_DEFAULT_REGION"] = region
    env["AWS_REGION"] = region
    if endpoint_url is not None:
        env["AWS_ENDPOINT_URL"] = endpoint_url
    else:
        env.pop("AWS_ENDPOINT_URL", None)
    return env
