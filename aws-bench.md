# AWS Storage API Benchmark Runbook

This runbook describes how to run the distributed storage API benchmark on AWS with one storage
server and many client-agent nodes. It assumes one protocol is tested at a time, which keeps REST
and gRPC results isolated.

Do not paste AWS access keys into repo files. Use an AWS CLI profile, environment variables in the
current shell, or an instance role on the controller.

The automated scripts write generated runtime files under `.aws-bench/<run-id>/<node-count>/` by
default. This keeps generated configs, private IP lists, and temporary command state out of the
project root. Use `--workspace` or `--workspace-root` only if you intentionally want a different
hidden or `etc`-style directory.

## Recommended AWS Shape

- One VPC, one subnet, one Availability Zone.
- One cluster placement group for the storage server, controller, and all client agents.
- On-Demand Capacity Reservation for large runs such as 64 or 128 client nodes.
- One storage-server EC2 instance running MariaDB in the local container started by
  `run_server.py`.
- One controller EC2 instance.
- N client-agent EC2 instances.
- Private IP traffic only for benchmark RPCs.

Use small client agents and scale the node count. Start with `c7i.large` or `c7gn.large` client
agents for 64/128-node runs. Move clients to `c7i.xlarge`/`c7gn.xlarge` only if client CPU or
network metrics show the agents are becoming the bottleneck. Use a larger storage server such as
`c7i.12xlarge`, `c7i.16xlarge`, `c7gn.8xlarge`, or larger because all client nodes converge on that
single service. Keep the client instance type fixed within a run series.

## Local AWS CLI Setup

Install and configure the AWS CLI on the machine that will create instances:

```bash
aws configure --profile spider-bench
export AWS_PROFILE=spider-bench
export AWS_REGION=us-east-1
```

When prompted, enter the access key and secret key. Do not commit them.

Check identity and quota before launching a large run:

```bash
aws sts get-caller-identity
aws service-quotas get-service-quota \
  --service-code ec2 \
  --quota-code L-1216C47A
```

For 128 `c7i.large` clients, budget at least 256 client vCPUs plus server/controller overhead.
Request more quota than the exact target so a failed replacement launch does not block the run.

## Automated Run Path

After instances are launched, tagged, and reachable through AWS Systems Manager, the normal path is
one command from the controller machine:

```bash
tools/scripts/storage-api-bench/aws_run_matrix.py \
  --run-id aws-128 \
  --node-counts 1,2,4,8,16,32,64,128 \
  --protocols grpc rest \
  --data-dir data
```

For a single protocol and node count:

```bash
tools/scripts/storage-api-bench/aws_run_protocol.py \
  --run-id aws-128 \
  --node-count 128 \
  --protocol grpc \
  --data-dir data/aws-128/grpc
```

### Automation Scripts

`aws_run_matrix.py` is the normal entry point for scalability experiments. It loops over the
requested node counts and protocols, creates a hidden workspace for each node count, and invokes
`aws_run_protocol.py` for each run. Use this when you want the full 1/2/4/.../128 sequence.

`aws_run_protocol.py` runs one protocol at one node count. It discovers instances, generates the
TOML config, syncs that config to remote nodes through SSM, starts or refreshes client agents,
starts the storage server for the selected protocol, runs `run_distributed_protocol.py`, and stops
the storage server afterward. Use this for a one-off run or debugging one protocol.

`aws_discover.py` only discovers AWS instances from tags and writes the server IP, server instance
ID, client IPs, and client instance IDs into the hidden workspace. Use it when you want to verify
AWS tagging and node selection before running the benchmark.

`aws_make_config.py` converts a server private IP plus a client-IP list into a distributed benchmark
TOML file. The AWS runners call it automatically, but it is also useful when manually debugging a
config.

`aws_common.py` is a shared helper module used by the scripts above. It is not meant to be run
directly.

The runner:

- discovers the storage server and client-agent instances from `RunId`/`Role` tags;
- writes discovery files and generated config under `.aws-bench/<run-id>/<node-count>/`;
- syncs the config to the same hidden path inside the repo on the server and client nodes;
- starts or refreshes client-agent processes through SSM;
- starts the storage server for the selected protocol through SSM;
- runs `run_distributed_protocol.py` locally on the controller;
- stops the storage server after the protocol run.

The automated path assumes:

- every instance has the repo at `~/spider`, unless `--remote-root` is provided;
- AWS SSM agent is running on every instance;
- instances have an IAM role that allows SSM Run Command;
- your controller shell has AWS credentials or an instance role that can call EC2 describe APIs and
  SSM send-command APIs.

To use a different hidden runtime directory on the controller:

```bash
tools/scripts/storage-api-bench/aws_run_matrix.py \
  --run-id aws-128 \
  --node-counts 64,128 \
  --workspace-root etc/aws-bench \
  --data-dir data
```

To use a different hidden runtime directory on remote nodes:

```bash
tools/scripts/storage-api-bench/aws_run_protocol.py \
  --run-id aws-128 \
  --node-count 128 \
  --protocol grpc \
  --remote-workspace .aws-bench/aws-128/128 \
  --data-dir data/aws-128/grpc
```

Use the manual sections below when provisioning infrastructure or debugging a failed automated run.

## Create Network Resources

Pick one AZ and one subnet in that AZ. Use an existing VPC/subnet if you already have one.

```bash
export AZ=us-east-1a
export VPC_ID=vpc-xxxxxxxx
export SUBNET_ID=subnet-xxxxxxxx
export KEY_NAME=spider-bench
export PG_NAME=spider-storage-api-bench

aws ec2 create-placement-group \
  --group-name "$PG_NAME" \
  --strategy cluster
```

Create a security group. Replace `YOUR_PUBLIC_IP/32` with your SSH source address:

```bash
export SG_ID=$(
  aws ec2 create-security-group \
    --group-name spider-storage-api-bench \
    --description "Spider storage API benchmark" \
    --vpc-id "$VPC_ID" \
    --query GroupId \
    --output text
)

aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --protocol tcp \
  --port 22 \
  --cidr YOUR_PUBLIC_IP/32

aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --protocol tcp \
  --port 8091 \
  --source-group "$SG_ID"

aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --protocol tcp \
  --port 50051 \
  --source-group "$SG_ID"

aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --protocol tcp \
  --port 19091 \
  --source-group "$SG_ID"
```

For 64 or 128 nodes, reserve capacity before launching. Use the exact instance type, AZ, and count
you plan to run:

```bash
aws ec2 create-capacity-reservation \
  --instance-type c7i.large \
  --instance-platform Linux/UNIX \
  --availability-zone "$AZ" \
  --instance-count 128 \
  --placement-group-arn "$(aws ec2 describe-placement-groups \
    --group-names "$PG_NAME" \
    --query 'PlacementGroups[0].GroupArn' \
    --output text)"
```

Create separate reservations for the server and controller if they use different instance types.

## Launch Instances

Use a current Ubuntu AMI for the selected region. Set tags so the controller can discover nodes.
Example:

```bash
export AMI_ID=ami-xxxxxxxx
export RUN_ID=aws-128

aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type c7i.16xlarge \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SG_ID" \
  --subnet-id "$SUBNET_ID" \
  --placement "AvailabilityZone=$AZ,GroupName=$PG_NAME" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=RunId,Value=$RUN_ID},{Key=Role,Value=storage-server}]"

aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type c7i.large \
  --count 128 \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SG_ID" \
  --subnet-id "$SUBNET_ID" \
  --placement "AvailabilityZone=$AZ,GroupName=$PG_NAME" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=RunId,Value=$RUN_ID},{Key=Role,Value=benchmark-client}]"

aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type c7i.large \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SG_ID" \
  --subnet-id "$SUBNET_ID" \
  --placement "AvailabilityZone=$AZ,GroupName=$PG_NAME" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=RunId,Value=$RUN_ID},{Key=Role,Value=controller}]"
```

Wait until all instances are running:

```bash
aws ec2 wait instance-running \
  --filters "Name=tag:RunId,Values=$RUN_ID"
```

## Install Benchmark Dependencies

Run this on the server, controller, and every client-agent instance:

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev protobuf-compiler docker.io git python3
sudo usermod -aG docker "$USER"
curl https://sh.rustup.rs -sSf | sh -s -- -y
```

Log out and back in so Docker group membership applies. Then clone or copy this repo on every node,
check out the same branch/commit, and build:

```bash
cd ~/spider
cargo build --release --package spider-storage-api-bench
```

## Discover Private IPs

On the controller or local machine with AWS CLI access:

```bash
export SERVER_IP=$(
  aws ec2 describe-instances \
    --filters "Name=tag:RunId,Values=$RUN_ID" "Name=tag:Role,Values=storage-server" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].PrivateIpAddress' \
    --output text
)

aws ec2 describe-instances \
  --filters "Name=tag:RunId,Values=$RUN_ID" "Name=tag:Role,Values=benchmark-client" "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].PrivateIpAddress' \
  --output text | tr '\t' '\n' | sort -V > data/aws-client-ips.txt

wc -l data/aws-client-ips.txt
echo "$SERVER_IP"
```

The client IP count should equal the node count you intend to test.

## Generate Benchmark Config

Generate a config from the server private IP and client-agent private IP list:

```bash
tools/scripts/storage-api-bench/aws_make_config.py \
  --server-private-ip "$SERVER_IP" \
  --agent-ips data/aws-client-ips.txt \
  --output components/spider-storage-api-bench/config/aws-128.toml
```

Defaults:

- 10 jobs per client-agent, so 128 agents produce 1280 total jobs.
- 1000 tasks per job.
- 8 submitter/monitor clients per agent.
- 16 workers per agent.
- 50% flat jobs for the mixed workload.
- REST target on port 8091, gRPC target on port 50051, agent control on port 19091.

Override only the values you intentionally want to change, for example:

```bash
tools/scripts/storage-api-bench/aws_make_config.py \
  --server-private-ip "$SERVER_IP" \
  --agent-ips data/aws-client-ips.txt \
  --output components/spider-storage-api-bench/config/aws-128.toml \
  --jobs-per-agent 20 \
  --tasks-per-job 1000 \
  --flat-percent 50
```

Copy the generated config to the same path on every node, or keep the repo/config synchronized
before starting processes.

## Start Client Agents

On each client node, run one agent with an ID matching the generated config. The generated ID is
`client-` plus the private IP with dots replaced by dashes:

```bash
cd ~/spider
PRIVATE_IP=$(hostname -I | awk '{print $1}')
AGENT_ID="client-${PRIVATE_IP//./-}"
nohup tools/scripts/storage-api-bench/run_agent.py \
  --config components/spider-storage-api-bench/config/aws-128.toml \
  --agent-id "$AGENT_ID" \
  --bind 0.0.0.0:19091 \
  > "agent-${AGENT_ID}.log" 2>&1 &
```

From the controller, verify agents are reachable:

```bash
while read -r ip; do
  curl -fsS "http://$ip:19091/health" >/dev/null || echo "agent not ready: $ip"
done < data/aws-client-ips.txt
```

If `/health` is not implemented in the current agent, use the controller run as the reachability
test and check agent logs on failure.

## Run One Protocol

Start the storage server on the server instance. Run one protocol at a time:

```bash
cd ~/spider
tools/scripts/storage-api-bench/run_server.py \
  --protocol grpc \
  --config components/spider-storage-api-bench/config/aws-128.toml \
  --bind 0.0.0.0:50051
```

On the controller instance, run all workloads for that protocol:

```bash
cd ~/spider
tools/scripts/storage-api-bench/run_distributed_protocol.py \
  --protocol grpc \
  --config components/spider-storage-api-bench/config/aws-128.toml \
  --data-dir data/aws-128/grpc
```

Stop the storage server, restart it for REST, and run REST:

```bash
tools/scripts/storage-api-bench/run_server.py \
  --protocol rest \
  --config components/spider-storage-api-bench/config/aws-128.toml \
  --bind 0.0.0.0:8091
```

```bash
tools/scripts/storage-api-bench/run_distributed_protocol.py \
  --protocol rest \
  --config components/spider-storage-api-bench/config/aws-128.toml \
  --data-dir data/aws-128/rest
```

The client-agent processes do not need to be restarted when switching between REST and gRPC. The
controller sends the protocol, target URL, workload, and per-agent job allocation in each `/runs`
request, and each agent creates fresh storage API clients for that run. Restart client agents only
if an agent is unhealthy, stuck with a running benchmark, its `agent_id`/bind port changed, or the
repo/config path it was started with is no longer valid.

For scalability studies, repeat the same process with 1, 2, 4, 8, 16, 32, 64, and 128 client
agents. Generate a config per node count so `job_count` scales with the selected client-agent count.

## Collect Results

Each controller run writes merged JSON and per-agent JSON under the selected data directory. Copy
the controller output back to your workstation or upload it to S3:

```bash
aws s3 sync data/aws-128 "s3://YOUR_BUCKET/spider-storage-api-bench/aws-128/"
```

After copying result directories into this repo's `data/` layout, use the report scripts to draw
the same charts used for local/baker runs.

## Cleanup

Terminate instances when finished:

```bash
aws ec2 describe-instances \
  --filters "Name=tag:RunId,Values=$RUN_ID" "Name=instance-state-name,Values=running,pending,stopped" \
  --query 'Reservations[].Instances[].InstanceId' \
  --output text
```

Then terminate those instance IDs:

```bash
aws ec2 terminate-instances --instance-ids INSTANCE_ID_1 INSTANCE_ID_2
```

Cancel capacity reservations that are no longer needed:

```bash
aws ec2 describe-capacity-reservations \
  --filters "Name=state,Values=active"
aws ec2 cancel-capacity-reservation --capacity-reservation-id cr-xxxxxxxx
```

## Checks Before Trusting A Large Run

- Run a 1-client and 2-client smoke test first in the same AWS setup.
- Confirm controller JSON contains server metrics and all expected agent reports.
- Confirm server CPU, DB CPU, network PPS, and disk I/O are not saturated unless that is the
  intended bottleneck.
- Keep REST and gRPC as separate server runs.
- Keep instance type, AZ, placement group, and benchmark config fixed across node counts except for
  the client-agent count and total job count.
