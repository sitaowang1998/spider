#!/usr/bin/env python3

import importlib.util
import json
import pathlib
import sys
import tarfile
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


class FakeAwsCli:
    def __init__(self):
        self.commands = []

    def run(self, args):
        self.commands.append(args)

    def run_allow_failure(self, args):
        self.commands.append(args)
        return 0

    def run_json(self, args):
        self.commands.append(args)
        if args[:2] == ["ec2", "run-instances"]:
            return {"Instances": [{"InstanceId": "i-builder"}]}
        if args[:2] == ["ssm", "send-command"]:
            return {"Command": {"CommandId": "command-1"}}
        if args[:2] == ["ec2", "create-image"]:
            return {"ImageId": "ami-built"}
        if args[:2] == ["ec2", "describe-images"]:
            return {
                "Images": [
                    {
                        "BlockDeviceMappings": [
                            {"Ebs": {"SnapshotId": "snap-1"}},
                            {"Ebs": {"SnapshotId": "snap-2"}},
                        ]
                    }
                ]
            }
        return {}


class AmiTest(unittest.TestCase):
    def test_build_ami_uploads_runtime_archive_and_writes_state(self):
        build_ami = load_module("build_ami")
        config_module = load_module("config")
        build_ami.build_local_binary = lambda _source_root: None
        client = FakeAwsCli()
        with tempfile.TemporaryDirectory() as directory:
            temp_dir = pathlib.Path(directory)
            source = temp_dir / "source"
            create_runtime_tree(source)
            config_path = temp_dir / "config.toml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    [aws]
                    run_id = "ami-test"

                    [benchmark]
                    node_counts = [1]

                    [instances]
                    client_count = 1
                    ami_id = "ami-runtime"

                    [artifact]
                    base_ami_id = "ami-base"
                    s3_uri = "s3://bench-artifacts/runtime.tar.gz"
                    """
                ),
                encoding="utf-8",
            )
            state_path = temp_dir / "latest.json"
            config = config_module.load_config(config_path)

            metadata = build_ami.build_ami(
                config,
                client,
                source_root=source,
                artifact_dir=temp_dir,
                ami_state_path=state_path,
                localstack_smoke=True,
            )
            saved_ami_id = json.loads(state_path.read_text(encoding="utf-8"))["ami_id"]

        self.assertEqual("ami-built", metadata["ami_id"])
        self.assertEqual("ami-built", saved_ami_id)
        flattened = [" ".join(command) for command in client.commands]
        self.assertTrue(any("s3 cp" in command for command in flattened))
        self.assertTrue(any("create-image" in command for command in flattened))
        self.assertTrue(any("terminate-instances" in command for command in flattened))

    def test_artifact_bucket_creation_uses_region_constraint_outside_us_east_1(self):
        build_ami = load_module("build_ami")

        command = build_ami.create_bucket_command("bench-artifacts", "us-east-2")

        self.assertIn("--create-bucket-configuration", command)
        self.assertIn("LocationConstraint=us-east-2", command)

    def test_runtime_archive_contains_only_required_runtime_files(self):
        build_ami = load_module("build_ami")
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / "repo"
            create_runtime_tree(root)
            (root / "data").mkdir()
            (root / "data" / "result.json").write_text("{}", encoding="utf-8")
            archive_path = pathlib.Path(directory) / "runtime.tar.gz"

            build_ami.create_runtime_archive(root, archive_path)

            with tarfile.open(archive_path) as archive:
                names = archive.getnames()
        self.assertIn("spider/target/release/spider-storage-api-bench", names)
        self.assertIn("spider/tools/scripts/storage-api-bench/run_agent.py", names)
        self.assertIn("spider/tools/scripts/storage-api-bench/aws_setup/run.py", names)
        self.assertIn("spider/components/spider-storage-api-bench/config/default.toml", names)
        self.assertNotIn("spider/data/result.json", names)

    def test_cleanup_ami_deregisters_image_and_deletes_snapshots(self):
        cleanup_ami = load_module("cleanup_ami")
        client = FakeAwsCli()

        cleanup_ami.cleanup_ami(client, {"ami_id": "ami-built", "builder_instance_id": "i-builder"})

        flattened = [" ".join(command) for command in client.commands]
        self.assertTrue(any("deregister-image --image-id ami-built" in command for command in flattened))
        self.assertTrue(any("delete-snapshot --snapshot-id snap-1" in command for command in flattened))
        self.assertTrue(any("delete-snapshot --snapshot-id snap-2" in command for command in flattened))

    def test_provision_can_resolve_runtime_ami_from_ami_state(self):
        provision = load_module("provision")
        config_module = load_module("config")
        with tempfile.TemporaryDirectory() as directory:
            temp_dir = pathlib.Path(directory)
            state_path = temp_dir / "latest.json"
            state_path.write_text('{"ami_id":"ami-from-state"}\n', encoding="utf-8")
            config = config_module.AwsBenchConfig()

            ami_id = provision.resolve_runtime_ami_id(config, state_path)

        self.assertEqual("ami-from-state", ami_id)

    def test_result_bucket_creation_uses_region_constraint_outside_us_east_1(self):
        provision = load_module("provision")

        command = provision.create_bucket_command("bench-results", "us-east-2")

        self.assertIn("--create-bucket-configuration", command)
        self.assertIn("LocationConstraint=us-east-2", command)

    def test_local_image_commands_build_smoke_and_tag_for_localstack(self):
        local_image = load_module("build_local_image")

        build_command = local_image.build_command(
            "spider-node:local",
            pathlib.Path("/tmp/context"),
            pathlib.Path("/tmp/context/Dockerfile"),
        )
        smoke_command = local_image.smoke_command("spider-node:local")
        tag_command = local_image.tag_localstack_ami_command("spider-node:local", "ami-000001")

        self.assertEqual("docker", build_command[0])
        self.assertIn("spider-node:local", build_command)
        self.assertIn("target/release/spider-storage-api-bench", smoke_command)
        self.assertEqual(
            ["docker", "tag", "spider-node:local", "localstack-ec2/spider-bench-node:ami-000001"],
            tag_command,
        )


def create_runtime_tree(root: pathlib.Path) -> None:
    build_ami = load_module("build_ami")
    for relative_path in (build_ami.BENCH_BINARY, *build_ami.RUNTIME_FILES):
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/usr/bin/env python3\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
