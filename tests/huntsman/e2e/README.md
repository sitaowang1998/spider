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

Create `tools/deployment/spider-compose/.env` from `.env.example`. Set the required database
credentials and override the worker image:

```dotenv
SPIDER_DATABASE_ROOT_PASSWORD=spider-root-password
SPIDER_STORAGE_DB_PASSWORD=spider-password
SPIDER_STORAGE_DB_USERNAME=spider-user

SPIDER_PULL_POLICY=never
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
SPIDER_ENDPOINT=http://127.0.0.1:50051 \
SPIDER_CONCURRENCY=1 \
  cargo test --release --package e2e --test nn -- --nocapture
```

Remove the containers, networks, and database volume after the test:

```shell
task docker:compose:clean
```
