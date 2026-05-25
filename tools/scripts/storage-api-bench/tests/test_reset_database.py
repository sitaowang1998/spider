#!/usr/bin/env python3

import importlib.util
import pathlib
import sys
import tempfile
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = SCRIPT_DIR / "reset_database.py"


def load_module():
    spec = importlib.util.spec_from_file_location("reset_database", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["reset_database"] = module
    spec.loader.exec_module(module)
    return module


class ResetDatabaseTest(unittest.TestCase):
    def test_load_database_config_reads_database_section(self):
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            config = pathlib.Path(directory) / "config.toml"
            config.write_text(
                """
[server]
rest_target = "http://10.1.0.1:8091"

[database]
host = "db.example.internal"
port = 3307
name = "spider_bench"
username = "bench_user"
password = "bench_password"
max_connections = 512
""",
                encoding="utf-8",
            )

            database = module.load_database_config(config)

        self.assertEqual("db.example.internal", database.host)
        self.assertEqual(3307, database.port)
        self.assertEqual("spider_bench", database.name)
        self.assertEqual("bench_user", database.username)
        self.assertEqual("bench_password", database.password)

    def test_reset_sql_truncates_storage_tables_with_foreign_keys_disabled(self):
        module = load_module()

        sql = module.reset_sql()

        self.assertIn("SET FOREIGN_KEY_CHECKS = 0;", sql)
        self.assertIn("TRUNCATE TABLE `jobs`;", sql)
        self.assertIn("TRUNCATE TABLE `resource_groups`;", sql)
        self.assertIn("TRUNCATE TABLE `execution_managers`;", sql)
        self.assertIn("TRUNCATE TABLE `sessions`;", sql)
        self.assertTrue(sql.strip().endswith("SET FOREIGN_KEY_CHECKS = 1;"))


if __name__ == "__main__":
    unittest.main()
