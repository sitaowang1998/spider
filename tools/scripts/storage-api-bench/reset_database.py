#!/usr/bin/env python3
"""Resets storage benchmark database tables before a benchmark workload."""

from __future__ import annotations

import argparse
import dataclasses
import os
import pathlib
import shutil
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = ROOT / "components/spider-storage-api-bench/config/default.toml"
TABLES = ("jobs", "resource_groups", "execution_managers", "sessions")


@dataclasses.dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    name: str
    username: str
    password: str


def main() -> int:
    args = parse_args()
    database = load_database_config(args.config)
    sql = reset_sql()
    if args.dry_run:
        print(sql)
        return 0
    if not args.yes:
        print("refusing to reset database tables without --yes", file=sys.stderr)
        return 2
    reset_database(database, sql, args.client_bin)
    print(f"reset database `{database.name}` on {database.host}:{database.port}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=pathlib.Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--client-bin",
        help="MariaDB/MySQL command-line client. Defaults to mariadb, then mysql.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm destructive benchmark table cleanup.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print cleanup SQL without connecting to the database.",
    )
    return parser.parse_args()


def load_database_config(config: pathlib.Path) -> DatabaseConfig:
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
    return DatabaseConfig(
        host=database["host"],
        port=int(database["port"]),
        name=database["name"],
        username=database["username"],
        password=database["password"],
    )


def reset_sql() -> str:
    lines = ["SET FOREIGN_KEY_CHECKS = 0;"]
    lines.extend(f"TRUNCATE TABLE `{table}`;" for table in TABLES)
    lines.append("SET FOREIGN_KEY_CHECKS = 1;")
    return "\n".join(lines) + "\n"


def reset_database(database: DatabaseConfig, sql: str, client_bin: str | None) -> None:
    binary = resolve_client_binary(client_bin)
    env = os.environ.copy()
    env["MYSQL_PWD"] = database.password
    subprocess.run(
        [
            binary,
            "--host",
            database.host,
            "--port",
            str(database.port),
            "--user",
            database.username,
            "--database",
            database.name,
            "--batch",
            "--skip-column-names",
        ],
        input=sql,
        text=True,
        env=env,
        cwd=ROOT,
        check=True,
    )


def resolve_client_binary(client_bin: str | None) -> str:
    if client_bin is not None:
        return client_bin
    for candidate in ("mariadb", "mysql"):
        path = shutil.which(candidate)
        if path is not None:
            return path
    raise FileNotFoundError("could not find `mariadb` or `mysql` client in PATH")


if __name__ == "__main__":
    sys.exit(main())
