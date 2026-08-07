# Huntsman end-to-end tests

The end-to-end tests submit jobs to a running Spider cluster. The NN test uses the standard Spider
images and mounts the locally built `libnn.so` task package into each worker container.

## Run the tests

Create `tools/deployment/spider-compose/.env` from `.env.example` and set the required database
credentials:

```dotenv
SPIDER_DATABASE_ROOT_PASSWORD=spider-root-password
SPIDER_STORAGE_DB_PASSWORD=spider-password
SPIDER_STORAGE_DB_USERNAME=spider-user
```

Run the E2E task from the repository root:

```shell
task test:e2e
```

The task performs the complete workflow:

1. Builds the Rust workspace, including `build/rust-targets/release/libnn.so`.
2. Validates and starts `tools/deployment/spider-compose/compose.e2e.yaml` with a fresh database.
3. Runs the NN test client against `http://127.0.0.1:50051`.
4. Removes the Compose containers, networks, and database volume, including when the test fails.

The E2E Compose wrapper includes the standard Compose file together with a small override that
bind-mounts `libnn.so` at `/opt/spider/packages/nn/libnn.so` in the worker containers. To inspect or
start this stack without running the test client, use:

```shell
task docker:compose:e2e:validate
task docker:compose:e2e:up
task docker:compose:e2e:clean
```
