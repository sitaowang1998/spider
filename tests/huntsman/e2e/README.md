# Huntsman end-to-end tests

The end-to-end tests submit jobs to a running Spider cluster. The NN test also requires a worker
image containing the `nn` task package.

## Run the NN test with Docker Compose

Install the repository's Rust toolchain used by the test client:

```shell
task build:toolchains:rust
```

Build the E2E worker image and give it a stable local tag:

```shell
task docker:build-worker-e2e
docker tag "$(cat build/spider-worker-e2e-image.id)" spider-worker-e2e:local
```

Copy the complete Compose environment template, then select the E2E worker image:

```shell
cp tools/deployment/spider-compose/.env.example tools/deployment/spider-compose/.env
```

```dotenv
SPIDER_PULL_POLICY=missing
SPIDER_WORKER_IMAGE_REF=spider-worker-e2e:local
```

Validate and start the cluster. Compose reads the `.env` file automatically because the Task
commands run from its directory.

```shell
task docker:compose:validate
task docker:compose:up
```

Run the test from the repository root:

```shell
. build/toolchains/rust/env
. tools/deployment/spider-compose/.env
SPIDER_ENDPOINT="http://127.0.0.1:${SPIDER_STORAGE_PUBLISHED_PORT}" \
SPIDER_CONCURRENCY=1 \
  cargo test --release --package e2e --test nn -- --nocapture
```

Remove the containers, networks, and database volume after the test:

```shell
task docker:compose:clean
```
