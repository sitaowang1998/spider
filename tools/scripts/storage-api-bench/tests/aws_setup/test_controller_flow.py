#!/usr/bin/env python3

import importlib.util
import pathlib
import sys
import tempfile
import textwrap
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parents[2] / "aws_setup"


def load_module(name: str):
    sys.path.insert(0, str(SCRIPT_DIR))
    path = SCRIPT_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class ControllerFlowTest(unittest.TestCase):
    def test_bootstrap_commands_write_config_state_and_secret(self):
        bootstrap = load_module("bootstrap_controller")

        commands = bootstrap.build_bootstrap_commands(
            remote_root="~/spider",
            remote_workspace=".aws-bench/run/controller",
            config_text="[aws]\nrun_id = \"run\"\n",
            state_text="{\"run_id\":\"run\"}\n",
            secret_text="AWS_ACCESS_KEY_ID=key\nAWS_SECRET_ACCESS_KEY=secret\n",
        )

        joined = "\n".join(commands)
        self.assertIn("config.toml", joined)
        self.assertIn("state.json", joined)
        self.assertIn(".secret", joined)
        self.assertIn("chmod 600", joined)

    def test_run_controller_command_sources_secret_and_runs_matrix_wrapper(self):
        run_controller = load_module("run_controller")

        commands = run_controller.build_controller_run_commands(
            remote_root="~/spider",
            remote_workspace=".aws-bench/run/controller",
            remote_data_dir="data/aws-run",
        )

        joined = "\n".join(commands)
        self.assertIn("set -a", joined)
        self.assertIn(". .aws-bench/run/controller/.secret", joined)
        self.assertIn("aws_setup/run.py", joined)
        self.assertIn("--data-dir data/aws-run", joined)

    def test_full_run_uses_controller_steps_before_teardown(self):
        full_run = load_module("full_run")

        steps = full_run.full_run_steps(teardown=True)

        self.assertEqual(
            ["provision", "deploy", "bootstrap-controller", "run-controller", "fetch-results", "teardown"],
            steps,
        )

    def test_full_run_forwards_ami_state_to_provision(self):
        full_run = load_module("full_run")

        command = full_run.step_command(
            "provision",
            pathlib.Path("/tmp/config.toml"),
            pathlib.Path("/tmp/.secret"),
            pathlib.Path("/tmp/state.json"),
            pathlib.Path("/tmp/ami.json"),
            pathlib.Path("/tmp/data"),
            False,
        )

        self.assertIn("--ami-state", command)
        self.assertIn("/tmp/ami.json", command)

    def test_full_run_rejects_run_id_that_differs_from_config(self):
        full_run = load_module("full_run")
        with tempfile.TemporaryDirectory() as directory:
            config_path = pathlib.Path(directory) / "config.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [aws]
                    run_id = "config-run"

                    [benchmark]
                    node_counts = [1]

                    [instances]
                    client_count = 1
                    """
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(SystemExit, "does not match"):
                full_run.parse_run_id(config_path, "other-run")


if __name__ == "__main__":
    unittest.main()
