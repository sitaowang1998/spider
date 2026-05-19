# Storage API Benchmark Report

This report compares gRPC and REST for storage API benchmark runs. Request averages are weighted by request count. In
each chart, every request type is shown for one workload, with gRPC and REST side by side. Each bar totals
client-observed average request latency. The server-side portion is split into measured phases where available; gray is
server time not covered by a phase probe, and orange is client-side overhead.

## Setup

The storage API server ran on `baker3`; benchmark clients ran on `baker7`. Each run used `10` jobs, `1000` tasks per
job, `128` byte payloads, `4` submit/monitor clients, `8` workers, `64` poll batch size, and `10` ms poll wait. The
mixed workload used `50`% flat jobs and `50`% deep jobs.

## Overall Average Request Latency

| Workload | Protocol | Requests | Server avg (us) | Client overhead (us) | Client observed avg (us) | Client observed avg (ms) |
|----------|---------:|---------:|----------------:|---------------------:|-------------------------:|-------------------------:|
| flat     |     Grpc |   20,621 |           154.4 |                371.3 |                    525.7 |                    0.526 |
| flat     |     Rest |   20,585 |           142.1 |                347.7 |                    489.8 |                    0.490 |
| deep     |     Grpc |   37,897 |         3,584.1 |                444.1 |                  4,028.1 |                    4.028 |
| deep     |     Rest |   39,783 |         3,794.6 |                372.6 |                  4,167.2 |                    4.167 |
| mixed    |     Grpc |   31,319 |         2,937.8 |                418.5 |                  3,356.3 |                    3.356 |
| mixed    |     Rest |   31,230 |         2,911.9 |                361.6 |                  3,273.4 |                    3.273 |

## End-to-End Job Latency

| Workload | Protocol | Jobs | Failed jobs |  p50 (ms) |  p90 (ms) |  p99 (ms) |  max (ms) |
|----------|---------:|-----:|------------:|----------:|----------:|----------:|----------:|
| flat     |     Grpc |   10 |           0 |   335.620 |   408.842 |   408.842 |   408.842 |
| flat     |     Rest |   10 |           0 |   323.697 |   352.337 |   352.337 |   352.337 |
| deep     |     Grpc |   10 |           0 | 6,293.178 | 6,388.677 | 6,388.677 | 6,388.677 |
| deep     |     Rest |   10 |           0 | 6,865.705 | 6,941.146 | 6,941.146 | 6,941.146 |
| mixed    |     Grpc |   10 |           0 | 6,090.572 | 6,401.082 | 6,401.082 | 6,401.082 |
| mixed    |     Rest |   10 |           0 | 5,286.784 | 6,877.996 | 6,877.996 | 6,877.996 |

## Request Latency Detail

| Workload | Request                      | Protocol |  Count | Server avg (us) | Client overhead (us) | Client observed avg (us) |
|----------|------------------------------|---------:|-------:|----------------:|---------------------:|-------------------------:|
| flat     | `add_resource_group`         |     Grpc |      1 |         7,677.0 |                533.0 |                  8,210.0 |
| flat     | `add_resource_group`         |     Rest |      1 |         6,830.0 |                413.0 |                  7,243.0 |
| flat     | `get_job_state`              |     Grpc |    292 |           238.0 |                378.0 |                    616.0 |
| flat     | `get_job_state`              |     Rest |    265 |           257.0 |                325.0 |                    582.0 |
| flat     | `get_session`                |     Grpc |      1 |             0.0 |                517.0 |                    517.0 |
| flat     | `get_session`                |     Rest |      1 |             0.0 |                277.0 |                    277.0 |
| flat     | `poll_ready_tasks`           |     Grpc |    299 |         5,562.0 |                526.0 |                  6,088.0 |
| flat     | `poll_ready_tasks`           |     Rest |    290 |         5,359.0 |                449.0 |                  5,808.0 |
| flat     | `register_execution_manager` |     Grpc |      8 |        23,077.0 |                891.0 |                 23,968.0 |
| flat     | `register_execution_manager` |     Rest |      8 |        19,414.0 |                796.0 |                 20,210.0 |
| flat     | `register_job`               |     Grpc |     10 |        85,726.0 |              2,718.0 |                 88,444.0 |
| flat     | `register_job`               |     Rest |     10 |        77,206.0 |             11,983.0 |                 89,189.0 |
| flat     | `start_job`                  |     Grpc |     10 |        16,086.0 |                421.0 |                 16,507.0 |
| flat     | `start_job`                  |     Rest |     10 |        14,902.0 |                416.0 |                 15,318.0 |
| flat     | `succeed_task_instance`      |     Grpc | 10,000 |            16.0 |                365.0 |                    381.0 |
| flat     | `succeed_task_instance`      |     Rest | 10,000 |            15.0 |                330.0 |                    345.0 |
| flat     | `create_task_instance`       |     Grpc | 10,000 |             8.0 |                370.0 |                    378.0 |
| flat     | `create_task_instance`       |     Rest | 10,000 |             7.0 |                351.0 |                    358.0 |
| deep     | `add_resource_group`         |     Grpc |      1 |         7,563.0 |                461.0 |                  8,024.0 |
| deep     | `add_resource_group`         |     Rest |      1 |         6,625.0 |                276.0 |                  6,901.0 |
| deep     | `get_job_state`              |     Grpc |  5,440 |             9.0 |                493.0 |                    502.0 |
| deep     | `get_job_state`              |     Rest |  5,973 |             8.0 |                374.0 |                    382.0 |
| deep     | `get_session`                |     Grpc |      1 |             0.0 |                397.0 |                    397.0 |
| deep     | `get_session`                |     Rest |      1 |             0.0 |                257.0 |                    257.0 |
| deep     | `poll_ready_tasks`           |     Grpc | 12,427 |        10,873.0 |                472.0 |                 11,345.0 |
| deep     | `poll_ready_tasks`           |     Rest | 13,780 |        10,910.0 |                415.0 |                 11,325.0 |
| deep     | `register_execution_manager` |     Grpc |      8 |        12,373.0 |                857.0 |                 13,230.0 |
| deep     | `register_execution_manager` |     Rest |      8 |         8,820.0 |                970.0 |                  9,790.0 |
| deep     | `register_job`               |     Grpc |     10 |        18,636.0 |              1,902.0 |                 20,538.0 |
| deep     | `register_job`               |     Rest |     10 |        17,158.0 |              3,784.0 |                 20,942.0 |
| deep     | `start_job`                  |     Grpc |     10 |         8,437.0 |                517.0 |                  8,954.0 |
| deep     | `start_job`                  |     Rest |     10 |         7,334.0 |                416.0 |                  7,750.0 |
| deep     | `succeed_task_instance`      |     Grpc | 10,000 |            18.0 |                397.0 |                    415.0 |
| deep     | `succeed_task_instance`      |     Rest | 10,000 |            17.0 |                309.0 |                    326.0 |
| deep     | `create_task_instance`       |     Grpc | 10,000 |            10.0 |                428.0 |                    438.0 |
| deep     | `create_task_instance`       |     Rest | 10,000 |             8.0 |                373.0 |                    381.0 |
| mixed    | `add_resource_group`         |     Grpc |      1 |         6,886.0 |                392.0 |                  7,278.0 |
| mixed    | `add_resource_group`         |     Rest |      1 |         7,713.0 |                351.0 |                  8,064.0 |
| mixed    | `get_job_state`              |     Grpc |  2,843 |            15.0 |                493.0 |                    508.0 |
| mixed    | `get_job_state`              |     Rest |  2,940 |            16.0 |                377.0 |                    393.0 |
| mixed    | `get_session`                |     Grpc |      1 |             0.0 |                218.0 |                    218.0 |
| mixed    | `get_session`                |     Rest |      1 |             0.0 |                233.0 |                    233.0 |
| mixed    | `poll_ready_tasks`           |     Grpc |  8,446 |        10,819.0 |                502.0 |                 11,321.0 |
| mixed    | `poll_ready_tasks`           |     Rest |  8,260 |        10,850.0 |                423.0 |                 11,273.0 |
| mixed    | `register_execution_manager` |     Grpc |      8 |        10,766.0 |                875.0 |                 11,641.0 |
| mixed    | `register_execution_manager` |     Rest |      8 |        17,405.0 |                764.0 |                 18,169.0 |
| mixed    | `register_job`               |     Grpc |     10 |        16,527.0 |              2,325.0 |                 18,852.0 |
| mixed    | `register_job`               |     Rest |     10 |        74,433.0 |              7,900.0 |                 82,333.0 |
| mixed    | `start_job`                  |     Grpc |     10 |         8,116.0 |                335.0 |                  8,451.0 |
| mixed    | `start_job`                  |     Rest |     10 |        14,816.0 |                329.0 |                 15,145.0 |
| mixed    | `succeed_task_instance`      |     Grpc | 10,000 |            16.0 |                361.0 |                    377.0 |
| mixed    | `succeed_task_instance`      |     Rest | 10,000 |            16.0 |                310.0 |                    326.0 |
| mixed    | `create_task_instance`       |     Grpc | 10,000 |             9.0 |                382.0 |                    391.0 |
| mixed    | `create_task_instance`       |     Rest | 10,000 |             7.0 |                350.0 |                    357.0 |

## Server Phase Timing Detail

| Workload | Phase                                    | Protocol | Count | Server avg (us) |
|----------|------------------------------------------|---------:|------:|----------------:|
| flat     | `add_resource_group.db_add`              |     Grpc |     1 |         7,673.0 |
| flat     | `add_resource_group.db_add`              |     Rest |     1 |         6,826.0 |
| flat     | `register_execution_manager.db_register` |     Grpc |     8 |        23,070.0 |
| flat     | `register_execution_manager.db_register` |     Rest |     8 |        19,412.0 |
| flat     | `register_job.cache_insert`              |     Grpc |    10 |             0.0 |
| flat     | `register_job.cache_insert`              |     Rest |    10 |             1.0 |
| flat     | `register_job.create_jcb`                |     Grpc |    10 |         3,193.0 |
| flat     | `register_job.create_jcb`                |     Rest |    10 |         3,174.0 |
| flat     | `register_job.db_register`               |     Grpc |    10 |        78,770.0 |
| flat     | `register_job.db_register`               |     Rest |    10 |        71,690.0 |
| flat     | `register_job.parse_graph`               |     Grpc |    10 |         3,538.0 |
| flat     | `register_job.parse_graph`               |     Rest |    10 |         2,178.0 |
| flat     | `register_job.unframe_inputs`            |     Grpc |    10 |           117.0 |
| flat     | `register_job.unframe_inputs`            |     Rest |    10 |            87.0 |
| flat     | `register_job.validate`                  |     Grpc |    10 |            69.0 |
| flat     | `register_job.validate`                  |     Rest |    10 |            51.0 |
| flat     | `start_job.cache_get`                    |     Grpc |    10 |             1.0 |
| flat     | `start_job.cache_get`                    |     Rest |    10 |             0.0 |
| flat     | `start_job.jcb_start`                    |     Grpc |    10 |        16,079.0 |
| flat     | `start_job.jcb_start`                    |     Rest |    10 |        14,896.0 |
| deep     | `add_resource_group.db_add`              |     Grpc |     1 |         7,558.0 |
| deep     | `add_resource_group.db_add`              |     Rest |     1 |         6,622.0 |
| deep     | `register_execution_manager.db_register` |     Grpc |     8 |        12,369.0 |
| deep     | `register_execution_manager.db_register` |     Rest |     8 |         8,817.0 |
| deep     | `register_job.cache_insert`              |     Grpc |    10 |             0.0 |
| deep     | `register_job.cache_insert`              |     Rest |    10 |             0.0 |
| deep     | `register_job.create_jcb`                |     Grpc |    10 |         2,875.0 |
| deep     | `register_job.create_jcb`                |     Rest |    10 |         2,320.0 |
| deep     | `register_job.db_register`               |     Grpc |    10 |        10,963.0 |
| deep     | `register_job.db_register`               |     Rest |    10 |        10,623.0 |
| deep     | `register_job.parse_graph`               |     Grpc |    10 |         4,770.0 |
| deep     | `register_job.parse_graph`               |     Rest |    10 |         4,171.0 |
| deep     | `register_job.unframe_inputs`            |     Grpc |    10 |             0.0 |
| deep     | `register_job.unframe_inputs`            |     Rest |    10 |             0.0 |
| deep     | `register_job.validate`                  |     Grpc |    10 |             7.0 |
| deep     | `register_job.validate`                  |     Rest |    10 |             8.0 |
| deep     | `start_job.cache_get`                    |     Grpc |    10 |             1.0 |
| deep     | `start_job.cache_get`                    |     Rest |    10 |             0.0 |
| deep     | `start_job.jcb_start`                    |     Grpc |    10 |         8,429.0 |
| deep     | `start_job.jcb_start`                    |     Rest |    10 |         7,327.0 |
| mixed    | `add_resource_group.db_add`              |     Grpc |     1 |         6,882.0 |
| mixed    | `add_resource_group.db_add`              |     Rest |     1 |         7,711.0 |
| mixed    | `register_execution_manager.db_register` |     Grpc |     8 |        10,764.0 |
| mixed    | `register_execution_manager.db_register` |     Rest |     8 |        17,402.0 |
| mixed    | `register_job.cache_insert`              |     Grpc |    10 |             1.0 |
| mixed    | `register_job.cache_insert`              |     Rest |    10 |             0.0 |
| mixed    | `register_job.create_jcb`                |     Grpc |    10 |         2,484.0 |
| mixed    | `register_job.create_jcb`                |     Rest |    10 |         2,405.0 |
| mixed    | `register_job.db_register`               |     Grpc |    10 |        10,502.0 |
| mixed    | `register_job.db_register`               |     Rest |    10 |        69,332.0 |
| mixed    | `register_job.parse_graph`               |     Grpc |    10 |         3,448.0 |
| mixed    | `register_job.parse_graph`               |     Rest |    10 |         2,594.0 |
| mixed    | `register_job.unframe_inputs`            |     Grpc |    10 |            42.0 |
| mixed    | `register_job.unframe_inputs`            |     Rest |    10 |            49.0 |
| mixed    | `register_job.validate`                  |     Grpc |    10 |            34.0 |
| mixed    | `register_job.validate`                  |     Rest |    10 |            29.0 |
| mixed    | `start_job.cache_get`                    |     Grpc |    10 |             0.0 |
| mixed    | `start_job.cache_get`                    |     Rest |    10 |             0.0 |
| mixed    | `start_job.jcb_start`                    |     Grpc |    10 |         8,112.0 |
| mixed    | `start_job.jcb_start`                    |     Rest |    10 |        14,812.0 |

## Detailed Findings

- `flat`: REST has lower weighted average request latency (0.526 ms gRPC vs 0.490 ms REST). REST has lower p50 e2e job
  latency (335.620 ms gRPC vs 323.697 ms REST).
- `deep`: gRPC has lower weighted average request latency (4.028 ms gRPC vs 4.167 ms REST). gRPC has lower p50 e2e job
  latency (6,293.178 ms gRPC vs 6,865.705 ms REST).
- `mixed`: REST has lower weighted average request latency (3.356 ms gRPC vs 3.273 ms REST). REST has lower p50 e2e job
  latency (6,090.572 ms gRPC vs 5,286.784 ms REST).

- For flat `register_job`, most server-side time is in `register_job.db_register` (78.770 ms gRPC, 71.690 ms REST). This
  points at database insert/pool behavior rather than graph parsing, validation, or cache insertion.
- `flat` `start_job` is almost entirely `start_job.jcb_start`; `start_job.cache_get` is near zero. The JCB start phase
  is 16.079 ms for gRPC and 14.896 ms for REST.
- `deep` `start_job` is almost entirely `start_job.jcb_start`; `start_job.cache_get` is near zero. The JCB start phase
  is 8.429 ms for gRPC and 7.327 ms for REST.
- `mixed` `start_job` is almost entirely `start_job.jcb_start`; `start_job.cache_get` is near zero. The JCB start phase
  is 8.112 ms for gRPC and 14.812 ms for REST.
- `register_job.cache_insert`, `register_job.validate`, and `start_job.cache_get` are consistently tiny. They are
  unlikely to explain the protocol differences.
- The main remaining suspects are database insert/pool behavior for DB-backed operations and JCB start scheduling for
  `start_job`. The charts now separate those paths so the next benchmark run can confirm whether the gRPC/REST gap is
  coming from DB registration or non-DB scheduling.

## Protocol Summary

| Workload | Faster by avg request latency | gRPC avg request (ms) | REST avg request (ms) | Faster by e2e p50 | gRPC e2e p50 (ms) | REST e2e p50 (ms) |
|----------|------------------------------:|----------------------:|----------------------:|------------------:|------------------:|------------------:|
| flat     |                          REST |                 0.526 |                 0.490 |              REST |           335.620 |           323.697 |
| deep     |                          gRPC |                 4.028 |                 4.167 |              gRPC |         6,293.178 |         6,865.705 |
| mixed    |                          REST |                 3.356 |                 3.273 |              REST |         6,090.572 |         5,286.784 |
