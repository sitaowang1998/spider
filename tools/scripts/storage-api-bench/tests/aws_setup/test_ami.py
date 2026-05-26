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
        self.dry_run = False

    def run(self, args):
        self.commands.append(args)

    def run_allow_failure(self, args):
        self.commands.append(args)
        return 0

    def run_json(self, args):
        self.commands.append(args)
        if args[:2] == ["ec2", "run-instances"]:
            return {"Instances": [{"InstanceId": "i-builder"}]}
        if args[:2] == ["iam", "get-instance-profile"]:
            return {"InstanceProfile": {"Roles": [{"RoleName": "builder"}]}}
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

    def try_run_json(self, args):
        self.commands.append(args)
        return 1, {}


class FakeProvisionAwsCli:
    def __init__(self):
        self.commands = []
        self.dry_run = False
        self.subnet_count = 0
        self.association_count = 0

    def run(self, args):
        self.commands.append(args)

    def run_json(self, args):
        self.commands.append(args)
        if args[:2] == ["ec2", "create-vpc"]:
            return {"Vpc": {"VpcId": "vpc-created"}}
        if args[:2] == ["ec2", "create-subnet"]:
            self.subnet_count += 1
            return {"Subnet": {"SubnetId": f"subnet-{self.subnet_count}"}}
        if args[:2] == ["ec2", "create-internet-gateway"]:
            return {"InternetGateway": {"InternetGatewayId": "igw-created"}}
        if args[:2] == ["ec2", "create-route-table"]:
            return {"RouteTable": {"RouteTableId": "rtb-created"}}
        if args[:2] == ["ec2", "associate-route-table"]:
            self.association_count += 1
            return {"AssociationId": f"rtbassoc-{self.association_count}"}
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
                    worker_count = 1
                    ami_id = "ami-runtime"

                    [artifact]
                    base_ami_id = "ami-base"
                    s3_uri = "s3://bench-artifacts/runtime.tar.gz"

                    [network]
                    rds_subnet_availability_zones = ["us-east-1a", "us-east-1b"]
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
                state_path=temp_dir / "state.json",
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
        self.assertIn("spider/tools/scripts/storage-api-bench/aws_make_config.py", names)
        self.assertIn("spider/tools/scripts/storage-api-bench/run_agent.py", names)
        self.assertIn("spider/tools/scripts/storage-api-bench/aws_setup/run.py", names)
        self.assertIn("spider/components/spider-storage-api-bench/config/default.toml", names)
        self.assertNotIn("spider/data/result.json", names)

    def test_builder_commands_create_runtime_and_workspace_dirs(self):
        build_ami = load_module("build_ami")
        config_module = load_module("config")
        config = config_module.AwsBenchConfig()
        config.instances.remote_root = "/opt/spider"
        config.instances.remote_workspace_root = "/var/lib/spider-bench"
        config.results.remote_data_dir = "/var/lib/spider-bench/data"

        commands = build_ami.builder_commands(config, "s3://bucket/runtime.tar.gz")

        joined = "\n".join(commands)
        self.assertIn("mkdir -p /opt/spider", joined)
        self.assertIn("mkdir -p /var/lib/spider-bench", joined)
        self.assertIn("mkdir -p /var/lib/spider-bench/data", joined)
        self.assertIn("tar -xzf /tmp/spider-runtime.tar.gz -C /opt/spider", joined)
        self.assertNotIn("/root/spider", joined)

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

    def test_empty_worker_subnets_expand_to_all_configured_availability_zones(self):
        provision = load_module("provision")
        config_module = load_module("config")
        client = FakeProvisionAwsCli()
        config = config_module.AwsBenchConfig()
        config.aws.availability_zone = "us-east-1a"
        config.network.rds_subnet_availability_zones = [
            "us-east-1a",
            "us-east-1b",
            "us-east-1c",
            "us-east-1d",
            "us-east-1e",
            "us-east-1f",
        ]

        resources = {}
        provision.ensure_network(client, config, resources)

        self.assertEqual(
            ["subnet-1", "subnet-2", "subnet-3", "subnet-4", "subnet-5", "subnet-6"],
            config.network.worker_subnet_ids,
        )
        self.assertEqual(config.network.worker_subnet_ids, resources["worker_subnet_ids"])
        self.assertEqual(
            config.network.worker_subnet_ids,
            resources["route_table_associated_subnet_ids"],
        )
        create_subnet_commands = [
            command for command in client.commands if command[:2] == ["ec2", "create-subnet"]
        ]
        created_cidrs = [
            command[command.index("--cidr-block") + 1] for command in create_subnet_commands
        ]
        self.assertEqual(
            [
                "10.42.1.0/24",
                "10.42.2.0/24",
                "10.42.3.0/24",
                "10.42.4.0/24",
                "10.42.5.0/24",
                "10.42.6.0/24",
            ],
            created_cidrs,
        )

    def test_worker_subnet_routing_is_idempotent_for_existing_route_table(self):
        provision = load_module("provision")
        config_module = load_module("config")
        client = FakeProvisionAwsCli()
        config = config_module.AwsBenchConfig()
        config.network.worker_subnet_ids = ["subnet-1", "subnet-2", "subnet-3", "subnet-4"]
        resources = {
            "subnet_id": "subnet-1",
            "rds_subnet_ids": ["subnet-1", "subnet-2"],
            "route_table_id": "rtb-existing",
            "route_table_association_ids": ["rtbassoc-1", "rtbassoc-2"],
        }

        provision.ensure_worker_subnet_routing(client, config, resources)
        associate_commands = [
            command for command in client.commands if command[:2] == ["ec2", "associate-route-table"]
        ]

        self.assertEqual(2, len(associate_commands))
        self.assertEqual(
            ["subnet-1", "subnet-2", "subnet-3", "subnet-4"],
            resources["route_table_associated_subnet_ids"],
        )

        provision.ensure_worker_subnet_routing(client, config, resources)
        associate_commands = [
            command for command in client.commands if command[:2] == ["ec2", "associate-route-table"]
        ]

        self.assertEqual(2, len(associate_commands))

    def test_workers_are_split_across_all_worker_subnets(self):
        provision = load_module("provision")
        config_module = load_module("config")
        client = FakeProvisionAwsCli()
        config = config_module.AwsBenchConfig()
        config.instances.worker_count = 64
        config.instances.ami_id = "ami-test"
        config.network.security_group_id = "sg-test"
        config.network.worker_subnet_ids = [
            "subnet-1",
            "subnet-2",
            "subnet-3",
            "subnet-4",
            "subnet-5",
            "subnet-6",
        ]

        provision.launch_worker_roles(client, config)

        worker_launches = [
            command
            for command in client.commands
            if command[:2] == ["ec2", "run-instances"] and "benchmark-worker" in " ".join(command)
        ]
        counts = [int(command[command.index("--count") + 1]) for command in worker_launches]
        subnets = [command[command.index("--subnet-id") + 1] for command in worker_launches]
        self.assertEqual([11, 11, 11, 11, 10, 10], counts)
        self.assertEqual(config.network.worker_subnet_ids, subnets)


def create_runtime_tree(root: pathlib.Path) -> None:
    build_ami = load_module("build_ami")
    for relative_path in (build_ami.BENCH_BINARY, *build_ami.RUNTIME_FILES):
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/usr/bin/env python3\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
