# Running the Spider cluster with docker compose

The compose stack in [`docker-compose.e2e.yml`](docker-compose.e2e.yml) brings up a full Spider
cluster: MariaDB, the storage gRPC server, the scheduler, and four execution-manager workers. It is
the same stack the end-to-end fault-recovery tests run against, and every service is wired into the
`spider_net` user-defined bridge network with static IPs (`mariadb` `172.40.0.20`, `storage`
`172.40.0.30`, `scheduler` `172.40.0.40`).

## Prerequisites

- Docker with the Compose plugin.
- Spider release binaries built first. Each service image is built against
  [`Dockerfile`](Dockerfile) and copies the release binaries into the image, so build the Rust
  workspace before bringing the stack up:

  ```sh
  task build:rust
  ```

## Run via the e2e test task

The compose stack needs per-service configs (`gen-storage.yaml`, `gen-scheduler.yaml`,
`gen-em.yaml`) generated from [`tools/scripts/stack/spider-compose.yaml`](../scripts/stack/spider-compose.yaml),
plus the `nn` TDL package staged for the worker task executor to dlopen. The
`test:e2e-compose-fault` task does all of that — stages packages, generates configs, brings the
stack up with `--build --wait`, runs the `fault_recovery` tests, and tears the stack down when done:

```sh
task test:e2e-compose-fault
```

This is the simplest path: it sets up the cluster and exercises it end to end in one command.

## Manual: bring the stack up yourself

If you want the cluster running without running the fault tests, first stage the config the stack
mounts (the commands below match the internal tasks the e2e task calls):

```sh
# Generate per-service configs into build/spider-compose/.
uv run --script tools/scripts/stack/generate.py \
  --config tools/scripts/stack/spider-compose.yaml \
  --output-dir build/spider-compose

# Stage the nn TDL package the worker task executor dlopens.
mkdir -p build/tdl_packages/nn
cp build/rust-targets/release/libnn.so build/tdl_packages/nn/libnn.so
```

Then drive the stack with the `docker:compose-*` tasks, which wrap `docker compose` with the right
file (`-f tools/docker/docker-compose.e2e.yml`) and project name (`-p spider-e2e`):

```sh
# Build (or rebuild, cached) the service images.
task docker:compose-build

# Bring the stack up and wait for the storage + scheduler healthchecks to pass.
task docker:compose-up

# Tear it down and remove named volumes (fresh DB on next up).
task docker:compose-down
```

`compose-up` runs `docker compose up -d --build --wait`, so the storage and scheduler healthchecks
gate readiness. Once `up` reports done, the storage gRPC server is reachable from the host at
`http://172.40.0.30:50051`.

## Inspecting the running cluster

```sh
# List services and health.
docker compose -f tools/docker/docker-compose.e2e.yml -p spider-e2e ps

# Tail logs for a service (storage, scheduler, worker-1, ...).
docker compose -f tools/docker/docker-compose.e2e.yml -p spider-e2e logs -f scheduler
```