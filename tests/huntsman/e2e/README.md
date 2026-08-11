# Huntsman end-to-end tests

The end-to-end tests submit jobs to a running Spider cluster. The NN test uses the standard Spider
images and mounts the locally built `libnn.so` task package into each worker container.

## Run the tests

Copy the complete Compose environment template before running the test. Values in `.env` can be
changed to exercise non-default user-facing configuration.

```shell
cp tools/deployment/spider-compose/.env.example tools/deployment/spider-compose/.env
```

Run the E2E task from the repository root:

```shell
task test:e2e
```

The task performs the complete workflow:

1. Builds the Rust workspace, including `build/rust-targets/release/libnn.so`.
2. Validates and starts `tools/deployment/spider-compose/compose.e2e.yaml` using `.env` with a fresh
   database.
3. Runs the NN test client against the port configured by `SPIDER_STORAGE_PUBLISHED_PORT`.
4. Removes the Compose containers, networks, and database volume, including when the test fails.

The E2E Compose wrapper includes the standard Compose file together with a small override that
bind-mounts `libnn.so` under the directory configured by `SPIDER_WORKER_PACKAGE_DIR` in the worker
containers. To inspect or start this stack without running the test client, use:

```shell
task docker:compose:e2e:validate
task docker:compose:e2e:up
task docker:compose:e2e:clean
```
