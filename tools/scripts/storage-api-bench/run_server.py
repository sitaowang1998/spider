#!/usr/bin/env python3
"""Starts a storage API benchmark server, optionally with local MariaDB."""

import argparse
import pathlib
import signal
import subprocess
import sys
import uuid


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = ROOT / "components/spider-storage-api-bench/config/default.toml"


def main() -> int:
    args = parse_args()
    container_name = f"spider-storage-api-bench-{uuid.uuid4()}"
    mariadb = load_database_config(args.config)

    start_cmd = [
        str(ROOT / "tools/scripts/mariadb/start.py"),
        "--name",
        container_name,
        "--port",
        mariadb["port"],
        "--database",
        mariadb["database"],
        "--username",
        mariadb["username"],
        "--password",
        mariadb["password"],
    ]
    stop_cmd = [
        str(ROOT / "tools/scripts/mariadb/stop.py"),
        "--name",
        container_name,
    ]

    use_local_mariadb = not args.external_database and mariadb["host"] in {
        "127.0.0.1",
        "localhost",
    }
    if use_local_mariadb:
        subprocess.run(start_cmd, cwd=ROOT, check=True)
    server = None
    try:
        cmd = [
            "cargo",
            "run",
            "--release",
            "--package",
            "spider-storage-api-bench",
            "--",
            "server",
            "--protocol",
            args.protocol,
            "--config",
            str(args.config),
        ]
        if args.bind is not None:
            cmd.extend(["--bind", args.bind])
        server = subprocess.Popen(cmd, cwd=ROOT)

        def stop_server(_signum: int, _frame: object) -> None:
            if server is not None:
                server.terminate()

        signal.signal(signal.SIGINT, stop_server)
        signal.signal(signal.SIGTERM, stop_server)
        return server.wait()
    finally:
        if server is not None and server.poll() is None:
            server.terminate()
            server.wait()
        if use_local_mariadb:
            subprocess.run(stop_cmd, cwd=ROOT, check=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", choices=["rest", "grpc"], required=True)
    parser.add_argument("--config", type=pathlib.Path, default=DEFAULT_CONFIG)
    parser.add_argument("--bind")
    parser.add_argument(
        "--external-database",
        action="store_true",
        help="Do not start/stop a local MariaDB container; use the database in the config.",
    )
    return parser.parse_args()


def load_database_config(config: pathlib.Path) -> dict[str, str]:
    database: dict[str, str] = {}
    in_database_section = False
    for line in config.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            in_database_section = line == "[database]"
            continue
        if not in_database_section or "=" not in line:
            continue
        key, value = line.split("=", 1)
        database[key.strip()] = value.strip().strip('"')
    return {
        "host": database["host"],
        "port": str(database["port"]),
        "database": database["name"],
        "username": database["username"],
        "password": database["password"],
    }


if __name__ == "__main__":
    sys.exit(main())
