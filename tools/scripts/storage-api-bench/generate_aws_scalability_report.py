#!/usr/bin/env python3
"""Generates AWS scalability charts and a Markdown report."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
from collections import defaultdict
from dataclasses import dataclass

import cairo


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_INPUT_DIR = ROOT / "data/aws"
DEFAULT_OUTPUT_DIR = ROOT / "data/aws-bench"
PROTOCOLS = ("Grpc", "Rest")
REQUEST_ORDER = (
    "add_resource_group",
    "create_task_instance",
    "get_job_state",
    "get_session",
    "worker_poll_ready_tasks",
    "scheduler_poll_ready_tasks",
    "register_execution_manager",
    "register_job",
    "start_job",
    "succeed_task_instance",
)
WORKER_POLL_OPERATION = "worker_poll_ready_tasks"
SCHEDULER_STORAGE_POLL_OPERATION = "scheduler_poll_ready_tasks"
PHASE_ORDER = (
    "db_add",
    "db_register",
    "db_start",
    "decompress",
    "parse_graph",
    "unframe_inputs",
    "validate",
    "create_jcb",
    "cache_insert",
    "cache_get",
    "jcb_start",
)
COLORS = {
    "Grpc": (0.10, 0.34, 0.74),
    "Rest": (0.10, 0.52, 0.34),
    "grpc_light": (0.62, 0.75, 0.92),
    "rest_light": (0.62, 0.82, 0.70),
    "text": (0.10, 0.12, 0.15),
    "muted": (0.38, 0.42, 0.46),
    "grid": (0.86, 0.88, 0.90),
    "white": (1.0, 1.0, 1.0),
    "server_total": (0.45, 0.50, 0.56),
    "server_other": (0.52, 0.56, 0.61),
    "client_overhead": (0.91, 0.52, 0.14),
    "scheduler_response": (0.28, 0.58, 0.76),
}
REQUEST_TRACE_COLORS = {
    "worker_poll_ready_tasks": (0.10, 0.34, 0.74),
    "scheduler_poll_ready_tasks": (0.78, 0.36, 0.10),
}
TRACE_COLORS = (
    (0.10, 0.34, 0.74),
    (0.10, 0.52, 0.34),
    (0.78, 0.36, 0.10),
    (0.50, 0.33, 0.72),
    (0.77, 0.29, 0.30),
    (0.24, 0.56, 0.70),
    (0.46, 0.46, 0.46),
    (0.64, 0.40, 0.18),
)
PHASE_COLORS = {
    "db_add": (0.09, 0.44, 0.70),
    "db_register": (0.07, 0.50, 0.46),
    "db_start": (0.08, 0.38, 0.74),
    "decompress": (0.50, 0.33, 0.72),
    "parse_graph": (0.58, 0.40, 0.18),
    "unframe_inputs": (0.60, 0.58, 0.15),
    "validate": (0.35, 0.61, 0.25),
    "create_jcb": (0.77, 0.29, 0.30),
    "cache_insert": (0.34, 0.62, 0.78),
    "cache_get": (0.64, 0.40, 0.73),
    "jcb_start": (0.86, 0.25, 0.10),
}


@dataclass(frozen=True)
class Run:
    nodes: int
    protocol: str
    workload: str
    job_count: int
    task_count: int
    task_sleep_ms: int
    flat_percent: int
    submitter_count: int
    worker_count: int
    controller_wall_time_us: int
    job_avg_us: float
    job_latency: dict[str, int]
    server_job_execution_latency: dict[str, int]
    request_count: int
    client_request_avg_us: float
    server_request_avg_us: float
    worker_activity_count: int
    worker_idle_avg_pct: float

    @property
    def runtime_s(self) -> float:
        return self.controller_wall_time_us / 1_000_000

    @property
    def throughput_jobs_per_s(self) -> float:
        if self.controller_wall_time_us <= 0:
            return 0.0
        return self.job_count / self.runtime_s

    @property
    def total_tasks(self) -> int:
        return self.job_count * self.task_count

    @property
    def optimal_job_latency_s(self) -> float:
        worker_slots = self.nodes * self.worker_count
        if worker_slots <= 0:
            return 0.0
        active_jobs = min(self.job_count, self.submitter_count)
        if active_jobs <= 0:
            return 0.0
        if self.workload == "Mixed":
            flat_latency = self.optimal_latency_for_parallelism(worker_slots, active_jobs, self.task_count)
            deep_latency = self.optimal_latency_for_parallelism(worker_slots, active_jobs, 1)
            flat_fraction = self.flat_percent / 100
            return flat_latency * flat_fraction + deep_latency * (1 - flat_fraction)
        per_job_parallelism = self.per_job_parallelism()
        return self.optimal_latency_for_parallelism(worker_slots, active_jobs, per_job_parallelism)

    def optimal_latency_for_parallelism(
        self,
        worker_slots: int,
        active_jobs: int,
        per_job_parallelism: int,
    ) -> float:
        if per_job_parallelism <= 0:
            return 0.0
        slots_per_active_job = max(1, min(per_job_parallelism, worker_slots // active_jobs))
        task_waves = math.ceil(self.task_count / slots_per_active_job)
        return task_waves * self.task_sleep_ms / 1000

    def per_job_parallelism(self) -> int:
        if self.workload == "Deep":
            return 1
        return self.task_count


@dataclass(frozen=True)
class RequestMetric:
    nodes: int
    protocol: str
    workload: str
    category: str
    operation: str
    count: int
    client_avg_us: float
    server_avg_us: float


@dataclass(frozen=True)
class PhaseMetric:
    nodes: int
    protocol: str
    workload: str
    request_operation: str
    phase: str
    count: int
    avg_us: float


@dataclass
class TraceSeries:
    nodes: int
    protocol: str
    workload: str
    events: list[tuple[int, int]]
    requests: list[dict[str, int | str]]
    start_epoch_us: int
    end_epoch_us: int
    max_concurrency: int


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    runs, request_metrics, phase_metrics = load_results(args.input_dir)
    trace_series = load_trace_series(args.input_dir)
    if not runs:
        raise ValueError(f"no benchmark JSON files found under {args.input_dir}")

    chart_paths = [
        args.output_dir / "aws_scalability_throughput.png",
        args.output_dir / "aws_scalability_e2e_latency.png",
        args.output_dir / "aws_scalability_request_latency.png",
    ]
    draw_line_chart(
        runs,
        chart_paths[0],
        "AWS Throughput Scaling",
        "Flat workload. Higher is better.",
        "Jobs / second",
        lambda run: run.throughput_jobs_per_s,
    )
    draw_e2e_latency_chart(
        runs,
        chart_paths[1],
    )
    draw_request_latency_chart(runs, request_metrics, phase_metrics, chart_paths[2])
    trace_chart_paths = draw_trace_concurrency_charts(trace_series, args.output_dir)
    chart_paths.extend(trace_chart_paths)

    for operation in request_operations(request_metrics):
        path = args.output_dir / f"aws_request_{operation}.png"
        draw_request_component_chart(operation, request_metrics, phase_metrics, path)
        chart_paths.append(path)

    report_path = args.output_dir / "aws_scalability_report.md"
    write_report(report_path, runs, request_metrics, phase_metrics)

    for path in [*chart_paths, report_path]:
        print(path)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=pathlib.Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def load_results(input_dir: pathlib.Path) -> tuple[list[Run], list[RequestMetric], list[PhaseMetric]]:
    runs: list[Run] = []
    request_metrics: list[RequestMetric] = []
    phase_metrics: list[PhaseMetric] = []
    for path in sorted(input_dir.glob("aws-*/*/*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if not {"setup", "job_latency", "request_latency", "server_metrics"} <= data.keys():
            continue
        setup = data["setup"]
        nodes = parse_node_count(path)
        workload = setup["workload"]
        protocol = setup["protocol"]
        distributed = data.get("distributed", {})
        wall_time_us = int(distributed.get("controller_wall_time_us", 0))
        if wall_time_us <= 0:
            continue

        server_by_key = {
            (row["category"], row["operation"]): row
            for row in data["server_metrics"].get("request_latency", [])
            if row.get("category") != "phase"
        }
        scheduler_by_key = {
            (row["category"], row["operation"]): row
            for row in data.get("scheduler_metrics", [])
            if row.get("category") != "phase"
        }
        for client_row in data["request_latency"]:
            if client_row.get("category") == "phase":
                continue
            operation = client_row["operation"]
            server_row = None
            if operation == "poll_ready_tasks":
                operation = WORKER_POLL_OPERATION
                server_row = scheduler_by_key.get((client_row["category"], operation))
            elif operation == "scheduler_poll_ready_tasks":
                operation = SCHEDULER_STORAGE_POLL_OPERATION
                server_row = server_by_key.get(("blocking", "poll_ready_tasks"))
            else:
                key = (client_row["category"], operation)
                server_row = server_by_key.get(key)
                if server_row is None:
                    continue
            request_metrics.append(
                RequestMetric(
                    nodes=nodes,
                    protocol=protocol,
                    workload=workload,
                    category=client_row["category"],
                    operation=operation,
                    count=int(client_row["count"]),
                    client_avg_us=float(client_row["avg_us"]),
                    server_avg_us=float(server_row["avg_us"]) if server_row is not None else 0.0,
                )
            )
        for row in data["server_metrics"].get("request_latency", []):
            if row.get("category") != "phase":
                continue
            request_operation, phase = row["operation"].split(".", 1)
            phase_metrics.append(
                PhaseMetric(
                    nodes=nodes,
                    protocol=protocol,
                    workload=workload,
                    request_operation=request_operation,
                    phase=phase,
                    count=int(row["count"]),
                    avg_us=float(row["avg_us"]),
                )
            )
        client_avg, request_count = weighted_average(data["request_latency"])
        server_avg, _ = weighted_average(
            row
            for row in data["server_metrics"].get("request_latency", [])
            if row.get("category") != "phase"
        )
        worker_activity_count, worker_idle_avg_pct = average_worker_idle_pct(data)
        server_metrics = data["server_metrics"]
        runs.append(
            Run(
                nodes=nodes,
                protocol=protocol,
                workload=workload,
                job_count=int(setup["job_count"]),
                task_count=int(setup["task_count"]),
                task_sleep_ms=int(setup.get("task_sleep_ms", 0)),
                flat_percent=int(setup.get("flat_percent", 100)),
                submitter_count=int(setup["client_count"]),
                worker_count=int(setup["worker_count"]),
                controller_wall_time_us=wall_time_us,
                job_avg_us=average_job_latency_us(data),
                job_latency=normalize_latency_summary(data["job_latency"]),
                server_job_execution_latency=normalize_latency_summary(
                    server_metrics.get("job_execution_latency", {})
                ),
                request_count=request_count,
                client_request_avg_us=client_avg,
                server_request_avg_us=server_avg,
                worker_activity_count=worker_activity_count,
                worker_idle_avg_pct=worker_idle_avg_pct,
            )
        )
    return (
        sorted(runs, key=lambda row: (row.workload, row.protocol, row.nodes)),
        sorted(request_metrics, key=lambda row: (row.operation, row.protocol, row.nodes)),
        sorted(phase_metrics, key=lambda row: (row.request_operation, row.phase, row.protocol, row.nodes)),
    )


def load_trace_series(input_dir: pathlib.Path) -> list[TraceSeries]:
    traces_by_key: dict[tuple[int, str, str], list[pathlib.Path]] = defaultdict(list)
    for path in sorted((input_dir / "traces").glob("aws-*/*/*/*/*.jsonl")):
        nodes = int(path.parents[3].name.rsplit("-", 1)[-1])
        protocol = normalize_protocol(path.parents[2].name)
        workload = normalize_workload(path.parents[1].name)
        traces_by_key[(nodes, protocol, workload)].append(path)

    series = []
    for (nodes, protocol, workload), paths in sorted(traces_by_key.items()):
        events: list[tuple[int, int]] = []
        requests: list[dict[str, int | str]] = []
        start_epoch_us: int | None = None
        end_epoch_us: int | None = None
        for path in paths:
            with path.open(encoding="utf-8") as file:
                for line in file:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if row["request"] != WORKER_POLL_OPERATION:
                        continue
                    start_us = int(row["start_epoch_us"])
                    end_us = int(row["end_epoch_us"])
                    start_epoch_us = start_us if start_epoch_us is None else min(start_epoch_us, start_us)
                    end_epoch_us = end_us if end_epoch_us is None else max(end_epoch_us, end_us)
                    events.append((start_us, 1))
                    events.append((end_us, -1))
                    requests.append(
                        {
                            "request": str(row["request"]),
                            "start_epoch_us": start_us,
                            "end_epoch_us": end_us,
                            "latency_us": int(row["latency_us"]),
                        }
                    )
        if start_epoch_us is None or end_epoch_us is None:
            continue
        events.sort(key=lambda event: (event[0], -event[1]))
        requests.sort(key=lambda row: int(row["start_epoch_us"]))
        series.append(
            TraceSeries(
                nodes=nodes,
                protocol=protocol,
                workload=workload,
                events=events,
                requests=requests,
                start_epoch_us=start_epoch_us,
                end_epoch_us=end_epoch_us,
                max_concurrency=max_concurrency(events),
            )
        )
    return series


def normalize_protocol(value: str) -> str:
    return {"grpc": "Grpc", "rest": "Rest"}.get(value, value.title())


def normalize_workload(value: str) -> str:
    return value.title()


def max_concurrency(events: list[tuple[int, int]]) -> int:
    current = 0
    maximum = 0
    for _timestamp, delta in events:
        current += delta
        maximum = max(maximum, current)
    return maximum


def parse_node_count(path: pathlib.Path) -> int:
    return int(path.parents[1].name.rsplit("-", 1)[-1])


def weighted_average(rows: object) -> tuple[float, int]:
    total = 0.0
    count = 0
    for row in rows:
        if row.get("category") == "phase":
            continue
        row_count = int(row["count"])
        total += float(row["avg_us"]) * row_count
        count += row_count
    return (total / count if count else 0.0), count


def average_job_latency_us(data: dict[str, object]) -> float:
    samples = data.get("job_latency_samples", [])
    if isinstance(samples, list):
        latencies = [
            float(sample["latency_micros"])
            for sample in samples
            if isinstance(sample, dict) and sample.get("succeeded") and "latency_micros" in sample
        ]
        if latencies:
            return sum(latencies) / len(latencies)
    latency = data.get("job_latency", {})
    if isinstance(latency, dict) and "avg_us" in latency:
        return float(latency["avg_us"])
    if isinstance(latency, dict) and "p50_us" in latency:
        return float(latency["p50_us"])
    return 0.0


def normalize_latency_summary(summary: object) -> dict[str, int]:
    if not isinstance(summary, dict):
        summary = {}
    normalized = {
        "count": int(summary.get("count", 0)),
        "avg_us": int(float(summary.get("avg_us", 0))),
        "p50_us": int(summary.get("p50_us", 0)),
        "p90_us": int(summary.get("p90_us", 0)),
        "p99_us": int(summary.get("p99_us", 0)),
        "max_us": int(summary.get("max_us", 0)),
    }
    if normalized["avg_us"] == 0 and normalized["p50_us"] > 0:
        normalized["avg_us"] = normalized["p50_us"]
    return normalized


def average_worker_idle_pct(data: dict[str, object]) -> tuple[int, float]:
    samples = data.get("worker_activity_samples", [])
    if not isinstance(samples, list):
        return 0, 0.0
    idle_percentages = []
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        active_window_us = int(sample.get("active_window_us", 0))
        if active_window_us <= 0:
            continue
        idle_us = int(sample.get("idle_us", 0))
        idle_percentages.append(100 * idle_us / active_window_us)
    if not idle_percentages:
        return 0, 0.0
    return len(idle_percentages), sum(idle_percentages) / len(idle_percentages)


def request_operations(metrics: list[RequestMetric]) -> list[str]:
    names = {metric.operation for metric in metrics}
    ordered = [operation for operation in REQUEST_ORDER if operation in names]
    ordered.extend(sorted(names - set(ordered)))
    return ordered


def draw_line_chart(
    runs: list[Run],
    output: pathlib.Path,
    title: str,
    subtitle: str,
    y_label: str,
    value_fn: object,
    baselines: list[tuple[str, tuple[float, float, float], object]] | None = None,
) -> None:
    width = 1400
    height = 820
    margin_left = 110
    margin_right = 260
    margin_top = 90
    margin_bottom = 110
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    nodes = sorted({run.nodes for run in runs})
    values = [float(value_fn(run)) for run in runs]
    for _label, _color, baseline_fn in baselines or []:
        values.extend(float(baseline_fn(run)) for run in runs)
    y_max = nice_upper_bound(max(values))

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_axes(ctx, title, subtitle, y_label, margin_left, margin_top, plot_width, plot_height, nodes, y_max)

    legend = []
    for protocol in PROTOCOLS:
        series = [run for run in runs if run.protocol == protocol]
        if not series:
            continue
        draw_series(ctx, series, nodes, margin_left, margin_top, plot_width, plot_height, y_max, COLORS[protocol], value_fn)
        legend.append((protocol, COLORS[protocol]))
    for label, color, baseline_fn in baselines or []:
        draw_series(
            ctx,
            sorted(unique_runs_by_node(runs), key=lambda run: run.nodes),
            nodes,
            margin_left,
            margin_top,
            plot_width,
            plot_height,
            y_max,
            color,
            baseline_fn,
        )
        legend.append((label, color))
    draw_legend(ctx, legend, width - margin_right + 35, margin_top)
    surface.write_to_png(str(output))


def unique_runs_by_node(runs: list[Run]) -> list[Run]:
    by_node = {}
    for run in runs:
        by_node.setdefault(run.nodes, run)
    return list(by_node.values())


def draw_e2e_latency_chart(runs: list[Run], output: pathlib.Path) -> None:
    width = 1500
    height = 820
    margin_left = 110
    margin_right = 320
    margin_top = 90
    margin_bottom = 110
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    nodes = sorted({run.nodes for run in runs})
    values = [run.job_avg_us / 1_000_000 for run in runs]
    values.extend(run.server_job_execution_latency["avg_us"] / 1_000_000 for run in runs)
    values.extend(run.optimal_job_latency_s for run in runs)
    y_max = nice_upper_bound(max(values))

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_axes(
        ctx,
        "AWS Average End-to-End Latency",
        "Client E2E covers register through result; task execution covers storage start-to-terminal time.",
        "Seconds",
        margin_left,
        margin_top,
        plot_width,
        plot_height,
        nodes,
        y_max,
    )

    legend = []
    for protocol in PROTOCOLS:
        series = [run for run in runs if run.protocol == protocol]
        if not series:
            continue
        draw_series(
            ctx,
            series,
            nodes,
            margin_left,
            margin_top,
            plot_width,
            plot_height,
            y_max,
            COLORS[protocol],
            lambda run: run.job_avg_us / 1_000_000,
        )
        legend.append((f"{protocol} client E2E", COLORS[protocol]))
        execution_color = COLORS["grpc_light"] if protocol == "Grpc" else COLORS["rest_light"]
        draw_series(
            ctx,
            series,
            nodes,
            margin_left,
            margin_top,
            plot_width,
            plot_height,
            y_max,
            execution_color,
            lambda run: run.server_job_execution_latency["avg_us"] / 1_000_000,
        )
        legend.append((f"{protocol} task execution", execution_color))
    draw_series(
        ctx,
        sorted(unique_runs_by_node(runs), key=lambda run: run.nodes),
        nodes,
        margin_left,
        margin_top,
        plot_width,
        plot_height,
        y_max,
        (0.35, 0.35, 0.35),
        lambda run: run.optimal_job_latency_s,
    )
    legend.append(("optimal", (0.35, 0.35, 0.35)))
    draw_legend(ctx, legend, width - margin_right + 35, margin_top)
    surface.write_to_png(str(output))


def draw_request_latency_chart(
    runs: list[Run],
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
    output: pathlib.Path,
) -> None:
    width = 1800
    height = 900
    margin_left = 115
    margin_right = 365
    margin_top = 105
    margin_bottom = 120
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    rows = aggregate_request_component_rows(runs, request_metrics, phase_metrics)
    components = component_order(rows)
    nodes = sorted({int(row["nodes"]) for row in rows})
    y_max = nice_upper_bound(max(float(row["client_avg_ms"]) for row in rows))
    group_width = plot_width / len(nodes)
    bar_width = min(56, group_width / 4.0)

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_axes(
        ctx,
        "AWS Average Request Latency Components",
        "Weighted across all request types. Stacked bars show server work and client overhead.",
        "Milliseconds",
        margin_left,
        margin_top,
        plot_width,
        plot_height,
        nodes,
        y_max,
        x_positions=[margin_left + group_width * index + group_width / 2 for index in range(len(nodes))],
    )
    by_key = {(row["nodes"], row["protocol"]): row for row in rows}
    offsets = {"Grpc": -0.58, "Rest": 0.58}
    for index, nodes_value in enumerate(nodes):
        center = margin_left + group_width * index + group_width / 2
        for protocol in PROTOCOLS:
            row = by_key.get((nodes_value, protocol))
            if row is None:
                continue
            x = center + offsets[protocol] * bar_width
            y_base = margin_top + plot_height
            for component in components:
                value_ms = float(row["components"].get(component, 0.0))
                if value_ms <= 0:
                    continue
                bar_height = plot_height * value_ms / y_max
                y_base -= bar_height
                set_color(ctx, component_color(component))
                ctx.rectangle(x - bar_width / 2, y_base, bar_width, bar_height)
                ctx.fill()
            select_font(ctx, 12)
            set_color(ctx, COLORS["text"])
            label = "g" if protocol == "Grpc" else "r"
            extents = ctx.text_extents(label)
            ctx.move_to(x - extents.width / 2, margin_top + plot_height + 48)
            ctx.show_text(label)
    draw_component_legend(ctx, components, width - margin_right + 30, margin_top)
    draw_note(
        ctx,
        ["g = gRPC, r = REST", "phase components are server-side", "client overhead = client avg - server avg"],
        width - margin_right + 35,
        height - 118,
    )
    surface.write_to_png(str(output))


def aggregate_request_component_rows(
    runs: list[Run],
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
) -> list[dict[str, object]]:
    rows = []
    run_by_key = {(run.nodes, run.protocol): run for run in runs}
    for nodes in sorted({run.nodes for run in runs}):
        for protocol in PROTOCOLS:
            run = run_by_key.get((nodes, protocol))
            if run is None or run.request_count == 0:
                continue
            components = {
                phase: sum(
                    metric.avg_us * metric.count
                    for metric in phase_metrics
                    if metric.nodes == nodes and metric.protocol == protocol and metric.phase == phase
                )
                / run.request_count
                / 1000
                for phase in {
                    metric.phase
                    for metric in phase_metrics
                    if metric.nodes == nodes and metric.protocol == protocol
                }
            }
            server_avg_ms = run.server_request_avg_us / 1000
            phase_sum = sum(components.values())
            components["server other"] = max(server_avg_ms - phase_sum, 0.0)
            components["client overhead"] = max(
                (run.client_request_avg_us - run.server_request_avg_us) / 1000,
                0.0,
            )
            rows.append(
                {
                    "nodes": nodes,
                    "protocol": protocol,
                    "request_count": run.request_count,
                    "client_avg_ms": run.client_request_avg_us / 1000,
                    "server_avg_ms": server_avg_ms,
                    "components": components,
                }
            )
    return rows


def aggregate_request_latency(runs: list[Run]) -> list[dict[str, float | int | str]]:
    rows = []
    for nodes in sorted({run.nodes for run in runs}):
        for protocol in PROTOCOLS:
            selected = [run for run in runs if run.nodes == nodes and run.protocol == protocol]
            request_count = sum(run.request_count for run in selected)
            if request_count == 0:
                continue
            rows.append(
                {
                    "nodes": nodes,
                    "protocol": protocol,
                    "request_count": request_count,
                    "client_avg_ms": sum(run.client_request_avg_us * run.request_count for run in selected) / request_count / 1000,
                    "server_avg_ms": sum(run.server_request_avg_us * run.request_count for run in selected) / request_count / 1000,
                }
            )
    return rows


def draw_request_component_chart(
    operation: str,
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
    output: pathlib.Path,
) -> None:
    rows = request_component_rows(operation, request_metrics, phase_metrics)
    if not rows:
        return
    components = component_order(rows)
    nodes = sorted({int(row["nodes"]) for row in rows})
    width = 1900
    height = 900
    margin_left = 115
    margin_right = 365
    margin_top = 105
    margin_bottom = 120
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    y_max = nice_upper_bound(max(float(row["client_avg_ms"]) for row in rows))
    group_width = plot_width / len(nodes)
    bar_width = min(56, group_width / 4.0)

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_axes(
        ctx,
        f"AWS {operation_label(operation)} Latency Components",
        request_chart_subtitle(operation),
        "Milliseconds",
        margin_left,
        margin_top,
        plot_width,
        plot_height,
        nodes,
        y_max,
        x_positions=[margin_left + group_width * index + group_width / 2 for index in range(len(nodes))],
    )

    by_key = {(row["nodes"], row["protocol"]): row for row in rows}
    offsets = {"Grpc": -0.58, "Rest": 0.58}
    for index, nodes_value in enumerate(nodes):
        center = margin_left + group_width * index + group_width / 2
        for protocol in PROTOCOLS:
            row = by_key.get((nodes_value, protocol))
            if row is None:
                continue
            x = center + offsets[protocol] * bar_width
            y_base = margin_top + plot_height
            for component in components:
                value_ms = float(row["components"].get(component, 0.0))
                if value_ms <= 0:
                    continue
                bar_height = plot_height * value_ms / y_max
                y_base -= bar_height
                set_color(ctx, component_color(component))
                ctx.rectangle(x - bar_width / 2, y_base, bar_width, bar_height)
                ctx.fill()
            select_font(ctx, 12)
            set_color(ctx, COLORS["text"])
            label = "g" if protocol == "Grpc" else "r"
            extents = ctx.text_extents(label)
            ctx.move_to(x - extents.width / 2, margin_top + plot_height + 48)
            ctx.show_text(label)
    draw_component_legend(ctx, components, width - margin_right + 30, margin_top)
    draw_note(
        ctx,
        request_chart_notes(operation),
        width - margin_right + 30,
        height - 118,
    )
    surface.write_to_png(str(output))


def draw_trace_concurrency_charts(trace_series: list[TraceSeries], output_dir: pathlib.Path) -> list[pathlib.Path]:
    paths = []
    keys = sorted({(series.protocol, series.workload) for series in trace_series})
    for protocol, workload in keys:
        selected = [
            series
            for series in trace_series
            if series.protocol == protocol and series.workload == workload
        ]
        if not selected:
            continue
        path = output_dir / f"aws_scheduler_trace_concurrency_{protocol.lower()}_{workload.lower()}.png"
        draw_trace_concurrency_chart(selected, path)
        paths.append(path)
        stale_timeline_path = output_dir / f"aws_scheduler_trace_timeline_{protocol.lower()}_{workload.lower()}.png"
        if stale_timeline_path.exists():
            stale_timeline_path.unlink()
        perfetto_path = output_dir / f"aws_scheduler_trace_perfetto_{protocol.lower()}_{workload.lower()}.json"
        write_perfetto_trace(selected, perfetto_path)
        paths.append(perfetto_path)
        latency_path = output_dir / f"aws_scheduler_trace_latency_over_time_{protocol.lower()}_{workload.lower()}.png"
        draw_trace_latency_over_time_chart(selected, latency_path)
        paths.append(latency_path)
    return paths


def draw_trace_concurrency_chart(series: list[TraceSeries], output: pathlib.Path) -> None:
    ordered_series = sorted(series, key=lambda row: row.nodes)
    width = 1700
    panel_height = 150
    panel_gap = 24
    height = 170 + len(ordered_series) * panel_height + (len(ordered_series) - 1) * panel_gap + 95
    margin_left = 130
    margin_right = 250
    margin_top = 110
    margin_bottom = 100
    plot_width = width - margin_left - margin_right
    x_max = nice_upper_bound(max((item.end_epoch_us - item.start_epoch_us) / 1_000_000 for item in ordered_series))

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_trace_small_multiple_title(
        ctx,
        f"AWS Scheduler Request Concurrency ({ordered_series[0].protocol}, {ordered_series[0].workload})",
        "Each panel shows one node-count run using every traced scheduler request start/end. Time starts at the first traced request.",
        margin_left,
    )

    for index, item in enumerate(ordered_series):
        color = TRACE_COLORS[index % len(TRACE_COLORS)]
        y_top = margin_top + index * (panel_height + panel_gap)
        show_x_labels = index == len(ordered_series) - 1
        draw_trace_panel_axes(
            ctx,
            item,
            margin_left,
            y_top,
            plot_width,
            panel_height,
            x_max,
            show_x_labels,
        )
        draw_trace_series(
            ctx,
            item,
            margin_left,
            y_top,
            plot_width,
            panel_height,
            x_max,
            nice_upper_bound(item.max_concurrency),
            color,
        )
    draw_trace_x_axis_label(ctx, margin_left, height - margin_bottom + 58, plot_width)
    surface.write_to_png(str(output))


def draw_trace_timeline_chart(series: list[TraceSeries], output: pathlib.Path) -> None:
    ordered_series = sorted(series, key=lambda row: row.nodes)
    request_types = trace_request_types(ordered_series)
    width = 1800
    panel_height = max(150, 80 * len(request_types))
    panel_gap = 26
    height = 170 + len(ordered_series) * panel_height + (len(ordered_series) - 1) * panel_gap + 95
    margin_left = 230
    margin_right = 230
    margin_top = 110
    margin_bottom = 100
    plot_width = width - margin_left - margin_right
    x_max = nice_upper_bound(max((item.end_epoch_us - item.start_epoch_us) / 1_000_000 for item in ordered_series))

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_trace_small_multiple_title(
        ctx,
        f"AWS Scheduler Request Timeline ({ordered_series[0].protocol}, {ordered_series[0].workload})",
        "Each horizontal segment is one traced scheduler request. Segment length is request duration.",
        margin_left,
    )
    for index, item in enumerate(ordered_series):
        y_top = margin_top + index * (panel_height + panel_gap)
        show_x_labels = index == len(ordered_series) - 1
        draw_trace_timeline_panel_axes(
            ctx,
            item,
            request_types,
            margin_left,
            y_top,
            plot_width,
            panel_height,
            x_max,
            show_x_labels,
        )
        draw_trace_timeline_panel(
            ctx,
            item,
            request_types,
            margin_left,
            y_top,
            plot_width,
            panel_height,
            x_max,
        )
    draw_trace_x_axis_label(ctx, margin_left, height - margin_bottom + 58, plot_width)
    surface.write_to_png(str(output))


def trace_request_types(series: list[TraceSeries]) -> list[str]:
    names = {str(request["request"]) for item in series for request in item.requests}
    preferred = [SCHEDULER_STORAGE_POLL_OPERATION, WORKER_POLL_OPERATION]
    ordered = [name for name in preferred if name in names]
    ordered.extend(sorted(names - set(ordered)))
    return ordered


def draw_trace_timeline_panel_axes(
    ctx: cairo.Context,
    series: TraceSeries,
    request_types: list[str],
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    x_max: float,
    show_x_labels: bool,
) -> None:
    set_color(ctx, COLORS["grid"])
    ctx.set_line_width(1)
    select_font(ctx, 13)
    band_height = plot_height / len(request_types)
    for index, request_type in enumerate(request_types):
        y = margin_top + index * band_height
        ctx.move_to(margin_left, y)
        ctx.line_to(margin_left + plot_width, y)
        ctx.stroke()
        set_color(ctx, COLORS["muted"])
        label = trace_request_label(request_type)
        extents = ctx.text_extents(label)
        ctx.move_to(margin_left - 18 - extents.width, y + band_height / 2 + 5)
        ctx.show_text(label)
        set_color(ctx, COLORS["grid"])
    ctx.move_to(margin_left, margin_top + plot_height)
    ctx.line_to(margin_left + plot_width, margin_top + plot_height)
    ctx.stroke()
    draw_trace_vertical_grid(ctx, margin_left, margin_top, plot_width, plot_height, x_max, show_x_labels)
    draw_panel_y_axis_label(ctx, "Latency (ms)", margin_left, margin_top, plot_height)
    draw_trace_panel_label(ctx, series, margin_left + plot_width + 18, margin_top)


def draw_trace_timeline_panel(
    ctx: cairo.Context,
    series: TraceSeries,
    request_types: list[str],
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    x_max: float,
) -> None:
    band_height = plot_height / len(request_types)
    lane_count = 18
    ctx.set_line_width(max(1.0, band_height / lane_count * 0.55))
    for request in series.requests:
        request_type = str(request["request"])
        type_index = request_types.index(request_type)
        start_s = (int(request["start_epoch_us"]) - series.start_epoch_us) / 1_000_000
        end_s = (int(request["end_epoch_us"]) - series.start_epoch_us) / 1_000_000
        x1 = margin_left + plot_width * start_s / x_max
        x2 = margin_left + plot_width * end_s / x_max
        lane = int(request["start_epoch_us"]) % lane_count
        y = margin_top + type_index * band_height + (lane + 0.5) * band_height / lane_count
        set_color_alpha(ctx, request_trace_color(request_type), 0.18)
        ctx.move_to(x1, y)
        ctx.line_to(max(x2, x1 + 0.5), y)
        ctx.stroke()


def write_perfetto_trace(series: list[TraceSeries], output: pathlib.Path) -> None:
    ordered_series = sorted(series, key=lambda row: row.nodes)
    with output.open("w", encoding="utf-8") as file:
        file.write('{"traceEvents":[\n')
        first = True

        def write_event(event: dict[str, object]) -> None:
            nonlocal first
            if not first:
                file.write(",\n")
            json.dump(event, file, separators=(",", ":"))
            first = False

        for item in ordered_series:
            write_event(
                {
                    "name": "process_name",
                    "ph": "M",
                    "pid": item.nodes,
                    "tid": 0,
                    "args": {"name": f"{item.nodes} worker nodes"},
                }
            )
            write_event(
                {
                    "name": "thread_name",
                    "ph": "M",
                    "pid": item.nodes,
                    "tid": 1,
                    "args": {"name": "worker -> scheduler poll"},
                }
            )
            for index, request in enumerate(item.requests):
                start_us = int(request["start_epoch_us"]) - item.start_epoch_us
                duration_us = max(1, int(request["end_epoch_us"]) - int(request["start_epoch_us"]))
                write_event(
                    {
                        "name": trace_request_label(str(request["request"])),
                        "cat": "scheduler",
                        "ph": "X",
                        "ts": start_us,
                        "dur": duration_us,
                        "pid": item.nodes,
                        "tid": 1,
                        "args": {
                            "node_count": item.nodes,
                            "request_index": index,
                            "request": str(request["request"]),
                            "latency_us": int(request["latency_us"]),
                        },
                    }
                )
        file.write('\n],"displayTimeUnit":"ms"}\n')


def draw_trace_latency_over_time_chart(series: list[TraceSeries], output: pathlib.Path) -> None:
    ordered_series = sorted(series, key=lambda row: row.nodes)
    width = 1800
    panel_height = 170
    panel_gap = 26
    height = 170 + len(ordered_series) * panel_height + (len(ordered_series) - 1) * panel_gap + 95
    margin_left = 120
    margin_right = 250
    margin_top = 110
    margin_bottom = 100
    plot_width = width - margin_left - margin_right
    x_max = nice_upper_bound(max((item.end_epoch_us - item.start_epoch_us) / 1_000_000 for item in ordered_series))

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_trace_small_multiple_title(
        ctx,
        f"AWS Scheduler Request Latency Over Time ({ordered_series[0].protocol}, {ordered_series[0].workload})",
        "Worker-to-scheduler requests are grouped by start time. Lines show avg, p50, p90, and p99 latency per time bucket.",
        margin_left,
    )
    draw_trace_y_axis_header(ctx, "Latency (ms)", margin_left, margin_top)
    bucket_s = max(0.05, x_max / 120)
    for index, item in enumerate(ordered_series):
        rows = trace_latency_percentile_rows(item, bucket_s)
        y_top = margin_top + index * (panel_height + panel_gap)
        show_x_labels = index == len(ordered_series) - 1
        y_max = nice_upper_bound(max((max(row["avg_ms"], row["p99_ms"]) for row in rows), default=1.0))
        draw_trace_latency_panel_axes(
            ctx,
            item,
            margin_left,
            y_top,
            plot_width,
            panel_height,
            x_max,
            y_max,
            show_x_labels,
        )
        draw_latency_percentile_series(ctx, rows, "avg_ms", margin_left, y_top, plot_width, panel_height, x_max, y_max, (0.10, 0.52, 0.34))
        draw_latency_percentile_series(ctx, rows, "p50_ms", margin_left, y_top, plot_width, panel_height, x_max, y_max, (0.10, 0.34, 0.74))
        draw_latency_percentile_series(ctx, rows, "p90_ms", margin_left, y_top, plot_width, panel_height, x_max, y_max, (0.78, 0.36, 0.10))
        draw_latency_percentile_series(ctx, rows, "p99_ms", margin_left, y_top, plot_width, panel_height, x_max, y_max, (0.77, 0.29, 0.30))
    draw_latency_percentile_legend(ctx, margin_left + plot_width - 120, 44)
    draw_trace_x_axis_label(ctx, margin_left, height - margin_bottom + 58, plot_width)
    surface.write_to_png(str(output))


def trace_latency_percentile_rows(series: TraceSeries, bucket_s: float) -> list[dict[str, float]]:
    buckets: dict[int, list[float]] = defaultdict(list)
    for request in series.requests:
        start_s = (int(request["start_epoch_us"]) - series.start_epoch_us) / 1_000_000
        bucket = int(start_s / bucket_s)
        buckets[bucket].append(int(request["latency_us"]) / 1000)
    rows = []
    for bucket, values in sorted(buckets.items()):
        values.sort()
        rows.append(
            {
                "time_s": (bucket + 0.5) * bucket_s,
                "avg_ms": sum(values) / len(values),
                "p50_ms": percentile(values, 0.50),
                "p90_ms": percentile(values, 0.90),
                "p99_ms": percentile(values, 0.99),
            }
        )
    return rows


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    index = min(len(values) - 1, max(0, math.ceil(len(values) * percentile_value) - 1))
    return values[index]


def draw_trace_small_multiple_title(
    ctx: cairo.Context,
    title: str,
    subtitle: str,
    margin_left: int,
) -> None:
    set_color(ctx, COLORS["text"])
    select_font(ctx, 28, bold=True)
    ctx.move_to(margin_left, 44)
    ctx.show_text(title)
    select_font(ctx, 15)
    set_color(ctx, COLORS["muted"])
    ctx.move_to(margin_left, 70)
    ctx.show_text(subtitle)


def draw_trace_panel_axes(
    ctx: cairo.Context,
    series: TraceSeries,
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    x_max: float,
    show_x_labels: bool,
) -> None:
    y_max = nice_upper_bound(series.max_concurrency)
    set_color(ctx, COLORS["grid"])
    ctx.set_line_width(1)
    select_font(ctx, 14)
    for index in range(3):
        y = margin_top + plot_height - plot_height * index / 2
        ctx.move_to(margin_left, y)
        ctx.line_to(margin_left + plot_width, y)
        ctx.stroke()
        set_color(ctx, COLORS["muted"])
        label = format_axis_value(y_max * index / 2)
        extents = ctx.text_extents(label)
        ctx.move_to(margin_left - 16 - extents.width, y + 5)
        ctx.show_text(label)
        set_color(ctx, COLORS["grid"])

    for index in range(6):
        x = margin_left + plot_width * index / 5
        ctx.move_to(x, margin_top)
        ctx.line_to(x, margin_top + plot_height)
        ctx.stroke()
        if show_x_labels:
            set_color(ctx, COLORS["muted"])
            label = format_axis_value(x_max * index / 5)
            extents = ctx.text_extents(label)
            ctx.move_to(x - extents.width / 2, margin_top + plot_height + 30)
            ctx.show_text(label)
            set_color(ctx, COLORS["grid"])

    set_color(ctx, COLORS["text"])
    ctx.set_line_width(2)
    ctx.move_to(margin_left, margin_top)
    ctx.line_to(margin_left, margin_top + plot_height)
    ctx.line_to(margin_left + plot_width, margin_top + plot_height)
    ctx.stroke()
    select_font(ctx, 15, bold=True)
    ctx.move_to(margin_left + plot_width + 18, margin_top + 22)
    ctx.show_text(f"{series.nodes} nodes")
    select_font(ctx, 13)
    set_color(ctx, COLORS["muted"])
    ctx.move_to(margin_left + plot_width + 18, margin_top + 44)
    ctx.show_text(f"max {series.max_concurrency}")
    ctx.move_to(margin_left + plot_width + 18, margin_top + 64)
    duration_s = (series.end_epoch_us - series.start_epoch_us) / 1_000_000
    ctx.show_text(f"{duration_s:.2f}s")


def draw_trace_vertical_grid(
    ctx: cairo.Context,
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    x_max: float,
    show_x_labels: bool,
) -> None:
    set_color(ctx, COLORS["grid"])
    ctx.set_line_width(1)
    select_font(ctx, 14)
    for index in range(6):
        x = margin_left + plot_width * index / 5
        ctx.move_to(x, margin_top)
        ctx.line_to(x, margin_top + plot_height)
        ctx.stroke()
        if show_x_labels:
            set_color(ctx, COLORS["muted"])
            label = format_axis_value(x_max * index / 5)
            extents = ctx.text_extents(label)
            ctx.move_to(x - extents.width / 2, margin_top + plot_height + 30)
            ctx.show_text(label)
            set_color(ctx, COLORS["grid"])
    set_color(ctx, COLORS["text"])
    ctx.set_line_width(2)
    ctx.move_to(margin_left, margin_top)
    ctx.line_to(margin_left, margin_top + plot_height)
    ctx.line_to(margin_left + plot_width, margin_top + plot_height)
    ctx.stroke()


def draw_trace_panel_label(ctx: cairo.Context, series: TraceSeries, x: float, y: float) -> None:
    select_font(ctx, 15, bold=True)
    set_color(ctx, COLORS["text"])
    ctx.move_to(x, y + 22)
    ctx.show_text(f"{series.nodes} nodes")
    select_font(ctx, 13)
    set_color(ctx, COLORS["muted"])
    ctx.move_to(x, y + 44)
    ctx.show_text(f"{len(series.requests)} requests")
    duration_s = (series.end_epoch_us - series.start_epoch_us) / 1_000_000
    ctx.move_to(x, y + 64)
    ctx.show_text(f"{duration_s:.2f}s")


def draw_trace_latency_panel_axes(
    ctx: cairo.Context,
    series: TraceSeries,
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    x_max: float,
    y_max: float,
    show_x_labels: bool,
) -> None:
    set_color(ctx, COLORS["grid"])
    ctx.set_line_width(1)
    select_font(ctx, 14)
    for index in range(3):
        y = margin_top + plot_height - plot_height * index / 2
        ctx.move_to(margin_left, y)
        ctx.line_to(margin_left + plot_width, y)
        ctx.stroke()
        set_color(ctx, COLORS["muted"])
        label = format_axis_value(y_max * index / 2)
        extents = ctx.text_extents(label)
        ctx.move_to(margin_left - 16 - extents.width, y + 5)
        ctx.show_text(label)
        set_color(ctx, COLORS["grid"])
    draw_trace_vertical_grid(ctx, margin_left, margin_top, plot_width, plot_height, x_max, show_x_labels)
    draw_trace_panel_label(ctx, series, margin_left + plot_width + 18, margin_top)


def draw_latency_percentile_series(
    ctx: cairo.Context,
    rows: list[dict[str, float]],
    key: str,
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    x_max: float,
    y_max: float,
    color: tuple[float, float, float],
) -> None:
    if not rows:
        return
    set_color(ctx, color)
    ctx.set_line_width(2)
    for index, row in enumerate(rows):
        x = margin_left + plot_width * row["time_s"] / x_max
        y = margin_top + plot_height - plot_height * row[key] / y_max
        if index == 0:
            ctx.move_to(x, y)
        else:
            ctx.line_to(x, y)
    ctx.stroke()


def draw_latency_percentile_legend(ctx: cairo.Context, x: int, y: int) -> None:
    draw_legend(
        ctx,
        [
            ("avg", (0.10, 0.52, 0.34)),
            ("p50", (0.10, 0.34, 0.74)),
            ("p90", (0.78, 0.36, 0.10)),
            ("p99", (0.77, 0.29, 0.30)),
        ],
        x,
        y,
    )


def draw_panel_y_axis_label(
    ctx: cairo.Context,
    label: str,
    margin_left: int,
    margin_top: int,
    plot_height: int,
) -> None:
    select_font(ctx, 13)
    set_color(ctx, COLORS["muted"])
    ctx.save()
    ctx.translate(margin_left - 70, margin_top + plot_height / 2)
    ctx.rotate(-math.pi / 2)
    extents = ctx.text_extents(label)
    ctx.move_to(-extents.width / 2, 0)
    ctx.show_text(label)
    ctx.restore()


def draw_trace_y_axis_header(ctx: cairo.Context, label: str, margin_left: int, margin_top: int) -> None:
    select_font(ctx, 14, bold=True)
    set_color(ctx, COLORS["muted"])
    ctx.move_to(margin_left - 92, margin_top - 16)
    ctx.show_text(label)


def request_trace_color(request_type: str) -> tuple[float, float, float]:
    return REQUEST_TRACE_COLORS.get(request_type, COLORS["server_other"])


def trace_request_label(request_type: str) -> str:
    labels = {
        SCHEDULER_STORAGE_POLL_OPERATION: "scheduler -> storage poll",
        WORKER_POLL_OPERATION: "worker -> scheduler poll",
    }
    return labels.get(request_type, request_type.replace("_", " "))


def draw_trace_x_axis_label(ctx: cairo.Context, margin_left: int, y: int, plot_width: int) -> None:
    select_font(ctx, 16)
    set_color(ctx, COLORS["muted"])
    label = "Seconds from first traced request"
    extents = ctx.text_extents(label)
    ctx.move_to(margin_left + plot_width / 2 - extents.width / 2, y)
    ctx.show_text(label)


def draw_trace_series(
    ctx: cairo.Context,
    series: TraceSeries,
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    x_max: float,
    y_max: float,
    color: tuple[float, float, float],
) -> None:
    set_color(ctx, color)
    ctx.set_line_width(2)
    current = 0
    ctx.move_to(margin_left, margin_top + plot_height)
    for timestamp_us, delta in series.events:
        elapsed_s = (timestamp_us - series.start_epoch_us) / 1_000_000
        x = margin_left + plot_width * elapsed_s / x_max
        y = margin_top + plot_height - plot_height * current / y_max
        ctx.line_to(x, y)
        current += delta
        y = margin_top + plot_height - plot_height * current / y_max
        ctx.line_to(x, y)
    duration_s = (series.end_epoch_us - series.start_epoch_us) / 1_000_000
    end_x = margin_left + plot_width * duration_s / x_max
    end_y = margin_top + plot_height - plot_height * current / y_max
    ctx.line_to(end_x, end_y)
    ctx.stroke()


def operation_label(operation: str) -> str:
    labels = {
        WORKER_POLL_OPERATION: "Worker To Scheduler Poll",
        SCHEDULER_STORAGE_POLL_OPERATION: "Scheduler To Storage Poll",
    }
    return labels.get(operation, operation)


def request_chart_subtitle(operation: str) -> str:
    if operation == WORKER_POLL_OPERATION:
        return "Worker calls to the scheduler cache. Stacked bars show scheduler handler time and client overhead."
    if operation == SCHEDULER_STORAGE_POLL_OPERATION:
        return "Scheduler batch polling against the storage server. Lower is better."
    return "Stacked bars show server work and client overhead. Lower is better."


def request_chart_notes(operation: str) -> list[str]:
    if operation == WORKER_POLL_OPERATION:
        return ["g = gRPC, r = REST", "scheduler response = scheduler handler latency"]
    return ["g = gRPC, r = REST", "client overhead = client avg - server avg", "server other = server avg - measured phases"]


def request_component_rows(
    operation: str,
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
) -> list[dict[str, object]]:
    rows = []
    for nodes in sorted({metric.nodes for metric in request_metrics}):
        for protocol in PROTOCOLS:
            selected_requests = [
                metric
                for metric in request_metrics
                if metric.operation == operation and metric.nodes == nodes and metric.protocol == protocol
            ]
            if not selected_requests:
                continue
            client_avg_us = weighted_metric_average((metric.client_avg_us, metric.count) for metric in selected_requests)
            server_avg_us = weighted_metric_average((metric.server_avg_us, metric.count) for metric in selected_requests)
            selected_phases = [
                metric
                for metric in phase_metrics
                if metric.request_operation == operation and metric.nodes == nodes and metric.protocol == protocol
            ]
            components = {
                phase: weighted_metric_average(
                    (metric.avg_us, metric.count) for metric in selected_phases if metric.phase == phase
                )
                / 1000
                for phase in {metric.phase for metric in selected_phases}
            }
            server_avg_ms = server_avg_us / 1000
            phase_sum = sum(components.values())
            if operation == WORKER_POLL_OPERATION:
                components["scheduler response"] = server_avg_ms
                components["client overhead"] = max((client_avg_us - server_avg_us) / 1000, 0.0)
            elif components:
                components["server other"] = max(server_avg_ms - phase_sum, 0.0)
            else:
                components["server total"] = server_avg_ms
            if operation != WORKER_POLL_OPERATION:
                components["client overhead"] = max((client_avg_us - server_avg_us) / 1000, 0.0)
            rows.append(
                {
                    "nodes": nodes,
                    "protocol": protocol,
                    "count": sum(metric.count for metric in selected_requests),
                    "client_avg_ms": client_avg_us / 1000,
                    "server_avg_ms": server_avg_ms,
                    "components": components,
                }
            )
    return rows


def weighted_metric_average(values: object) -> float:
    total = 0.0
    weight = 0
    for value, count in values:
        total += float(value) * int(count)
        weight += int(count)
    return total / weight if weight else 0.0


def component_order(rows: list[dict[str, object]]) -> list[str]:
    names = {
        name
        for row in rows
        for name, value in row["components"].items()
        if float(value) > 0
    }
    ordered = [name for name in PHASE_ORDER if name in names]
    for name in ("server total", "server other", "scheduler response", "client overhead"):
        if name in names:
            ordered.append(name)
    return ordered


def draw_axes(
    ctx: cairo.Context,
    title: str,
    subtitle: str,
    y_label: str,
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    nodes: list[int],
    y_max: float,
    x_positions: list[float] | None = None,
) -> None:
    set_color(ctx, COLORS["text"])
    select_font(ctx, 28, bold=True)
    ctx.move_to(margin_left, 44)
    ctx.show_text(title)
    select_font(ctx, 15)
    set_color(ctx, COLORS["muted"])
    ctx.move_to(margin_left, 70)
    ctx.show_text(subtitle)
    select_font(ctx, 16)
    ctx.move_to(margin_left, margin_top + plot_height + 78)
    ctx.show_text("Worker nodes")
    ctx.save()
    ctx.translate(34, margin_top + plot_height / 2)
    ctx.rotate(-math.pi / 2)
    extents = ctx.text_extents(y_label)
    ctx.move_to(-extents.width / 2, 0)
    ctx.show_text(y_label)
    ctx.restore()

    set_color(ctx, COLORS["grid"])
    ctx.set_line_width(1)
    for index in range(6):
        y = margin_top + plot_height - plot_height * index / 5
        ctx.move_to(margin_left, y)
        ctx.line_to(margin_left + plot_width, y)
        ctx.stroke()
        set_color(ctx, COLORS["muted"])
        select_font(ctx, 14)
        ctx.move_to(margin_left - 86, y + 5)
        ctx.show_text(format_axis_value(y_max * index / 5))
        set_color(ctx, COLORS["grid"])

    set_color(ctx, COLORS["text"])
    ctx.set_line_width(2)
    ctx.move_to(margin_left, margin_top)
    ctx.line_to(margin_left, margin_top + plot_height)
    ctx.line_to(margin_left + plot_width, margin_top + plot_height)
    ctx.stroke()

    select_font(ctx, 15)
    for index, nodes_value in enumerate(nodes):
        x = x_positions[index] if x_positions is not None else x_for_index(index, len(nodes), margin_left, plot_width)
        label = str(nodes_value)
        extents = ctx.text_extents(label)
        ctx.move_to(x - extents.width / 2, margin_top + plot_height + 30)
        ctx.show_text(label)


def draw_series(
    ctx: cairo.Context,
    series: list[Run],
    nodes: list[int],
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    y_max: float,
    color: tuple[float, float, float],
    value_fn: object,
) -> None:
    set_color(ctx, color)
    ctx.set_line_width(3)
    for index, run in enumerate(sorted(series, key=lambda row: row.nodes)):
        x = x_for_index(nodes.index(run.nodes), len(nodes), margin_left, plot_width)
        y = margin_top + plot_height - plot_height * float(value_fn(run)) / y_max
        if index == 0:
            ctx.move_to(x, y)
        else:
            ctx.line_to(x, y)
    ctx.stroke()
    for run in series:
        x = x_for_index(nodes.index(run.nodes), len(nodes), margin_left, plot_width)
        y = margin_top + plot_height - plot_height * float(value_fn(run)) / y_max
        ctx.arc(x, y, 5, 0, 2 * math.pi)
        ctx.fill()


def draw_dict_series(
    ctx: cairo.Context,
    series: list[dict[str, float | int | str]],
    nodes: list[int],
    key: str,
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    y_max: float,
    color: tuple[float, float, float],
    dash: tuple[float, ...],
) -> None:
    set_color(ctx, color)
    ctx.set_line_width(3)
    ctx.set_dash(dash)
    for index, row in enumerate(sorted(series, key=lambda item: int(item["nodes"]))):
        x = x_for_index(nodes.index(int(row["nodes"])), len(nodes), margin_left, plot_width)
        y = margin_top + plot_height - plot_height * float(row[key]) / y_max
        if index == 0:
            ctx.move_to(x, y)
        else:
            ctx.line_to(x, y)
    ctx.stroke()
    ctx.set_dash(())


def draw_legend(ctx: cairo.Context, items: list[tuple[str, tuple[float, float, float]]], x: int, y: int) -> None:
    select_font(ctx, 15)
    for index, (label, color) in enumerate(items):
        row_y = y + index * 28
        set_color(ctx, color)
        ctx.set_line_width(3)
        ctx.move_to(x, row_y)
        ctx.line_to(x + 38, row_y)
        ctx.stroke()
        set_color(ctx, COLORS["text"])
        ctx.move_to(x + 48, row_y + 5)
        ctx.show_text(label)


def draw_component_legend(ctx: cairo.Context, components: list[str], x: int, y: int) -> None:
    select_font(ctx, 14)
    set_color(ctx, COLORS["text"])
    ctx.move_to(x, y - 20)
    ctx.show_text("Components")
    for index, component in enumerate(components):
        row_y = y + index * 24
        set_color(ctx, component_color(component))
        ctx.rectangle(x, row_y - 11, 18, 12)
        ctx.fill()
        set_color(ctx, COLORS["text"])
        ctx.move_to(x + 28, row_y)
        ctx.show_text(component)


def draw_note(ctx: cairo.Context, lines: list[str], x: int, y: int) -> None:
    select_font(ctx, 13)
    set_color(ctx, COLORS["muted"])
    for index, line in enumerate(lines):
        ctx.move_to(x, y + 20 * index)
        ctx.show_text(line)


def component_color(component: str) -> tuple[float, float, float]:
    if component == "server total":
        return COLORS["server_total"]
    if component == "server other":
        return COLORS["server_other"]
    if component == "scheduler response":
        return COLORS["scheduler_response"]
    if component == "client overhead":
        return COLORS["client_overhead"]
    return PHASE_COLORS.get(component, COLORS["server_other"])


def paint_background(ctx: cairo.Context) -> None:
    set_color(ctx, COLORS["white"])
    ctx.paint()


def set_color(ctx: cairo.Context, color: tuple[float, float, float]) -> None:
    ctx.set_source_rgb(*color)


def set_color_alpha(ctx: cairo.Context, color: tuple[float, float, float], alpha: float) -> None:
    ctx.set_source_rgba(color[0], color[1], color[2], alpha)


def select_font(ctx: cairo.Context, size: int, *, bold: bool = False) -> None:
    ctx.select_font_face("Sans", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD if bold else cairo.FONT_WEIGHT_NORMAL)
    ctx.set_font_size(size)


def lighten(color: tuple[float, float, float], amount: float) -> tuple[float, float, float]:
    return tuple(channel + (1.0 - channel) * amount for channel in color)


def x_for_index(index: int, count: int, margin_left: int, plot_width: int) -> float:
    if count == 1:
        return margin_left + plot_width / 2
    return margin_left + plot_width * index / (count - 1)


def nice_upper_bound(value: float) -> float:
    if value <= 0:
        return 1.0
    magnitude = 10 ** math.floor(math.log10(value))
    scaled = value / magnitude
    if scaled <= 2:
        nice = 2
    elif scaled <= 5:
        nice = 5
    else:
        nice = 10
    return nice * magnitude


def format_axis_value(value: float) -> str:
    if value == 0:
        return "0"
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    if value >= 1:
        return f"{value:.2f}"
    return f"{value:.3f}"


def write_report(
    output: pathlib.Path,
    runs: list[Run],
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
) -> None:
    lines = [
        "# AWS Storage API Scalability Report",
        "",
        "## Setup",
        "",
        setup_paragraph(runs),
        "",
    ]
    lines.extend(throughput_table(runs))
    lines.extend(speedup_table(runs))
    lines.extend(e2e_table(runs))
    lines.extend(worker_idle_table(runs))
    lines.extend(request_latency_table(runs))
    lines.extend(per_request_table(request_metrics, phase_metrics))
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def setup_paragraph(runs: list[Run]) -> str:
    node_counts = sorted({run.nodes for run in runs})
    job_counts = sorted({run.job_count for run in runs})
    total_tasks = sorted({run.total_tasks for run in runs})
    task_count = sorted({run.task_count for run in runs})
    task_sleep_ms = sorted({run.task_sleep_ms for run in runs})
    submitters = sorted({run.submitter_count for run in runs})
    workers = sorted({run.worker_count for run in runs})
    workloads = sorted({run.workload for run in runs})
    return (
        f"The AWS benchmark data covers {format_set(node_counts)} worker-node runs for "
        f"{format_set(workloads)} workload. Each job has {format_set(task_count)} tasks with "
        f"{format_set(task_sleep_ms)} ms simulated sleep per task, and the run size scales from {format_set(job_counts)} jobs "
        f"({format_set(total_tasks)} total tasks). Each run uses one dedicated submitter node "
        f"with {format_set(submitters)} submitter clients and {format_set(workers)} worker "
        "processes per worker node. The optimal E2E time is the ideal per-active-job compute "
        "lower bound: `ceil(task_count / min(per_job_parallelism, "
        "floor(worker_nodes * workers_per_node / active_jobs))) * task_sleep_ms`, where "
        "`active_jobs = min(job_count, submitter_clients)`. Flat jobs can expose `task_count` "
        "runnable tasks, and deep jobs can expose one runnable task. This excludes storage, "
        "scheduling, polling, and transport overhead. Results compare gRPC "
        "and REST against the same storage server and RDS-backed storage layer."
    )


def throughput_table(runs: list[Run]) -> list[str]:
    lines = ["", "## Throughput", ""]
    lines.extend(
        [
            "| Nodes | Protocol | Jobs | Tasks | Runtime (s) | Throughput (jobs/s) | Throughput (tasks/s) |",
            "|---:|---|---:|---:|---:|---:|---:|",
        ]
    )
    for run in sorted(runs, key=lambda row: (row.nodes, row.protocol)):
        lines.append(
            f"| {run.nodes} | {run.protocol} | {run.job_count} | {run.total_tasks} | "
            f"{run.runtime_s:.2f} | {run.throughput_jobs_per_s:.2f} | "
            f"{run.total_tasks / run.runtime_s:.0f} |"
        )
    return lines


def speedup_table(runs: list[Run]) -> list[str]:
    lines = ["", "## Speedup And Efficiency", ""]
    lines.extend(
        [
            "| Nodes | Protocol | Speedup vs 1 node | Efficiency |",
            "|---:|---|---:|---:|",
        ]
    )
    baseline = {run.protocol: run.throughput_jobs_per_s for run in runs if run.nodes == 1}
    for run in sorted(runs, key=lambda row: (row.nodes, row.protocol)):
        speedup = run.throughput_jobs_per_s / baseline[run.protocol]
        lines.append(f"| {run.nodes} | {run.protocol} | {speedup:.2f}x | {speedup / run.nodes:.1%} |")
    return lines


def e2e_table(runs: list[Run]) -> list[str]:
    lines = ["", "## End-To-End Job Latency", ""]
    lines.append(
        "Client E2E is measured by the benchmark client from before job registration until "
        "the terminal job result is observed. Task execution is measured by storage from a "
        "successful `start_job` until the job reaches success or failure."
    )
    lines.extend(
        [
            "",
            "| Nodes | Protocol | Metric | Optimal (s) | Avg (s) | P50 (s) | P90 (s) | P99 (s) | Max (s) |",
            "|---:|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for run in sorted(runs, key=lambda row: (row.nodes, row.protocol)):
        lines.append(e2e_latency_row(run, "Client E2E", run.job_latency, run.job_avg_us))
        lines.append(
            e2e_latency_row(
                run,
                "Task execution",
                run.server_job_execution_latency,
                run.server_job_execution_latency["avg_us"],
            )
        )
    return lines


def e2e_latency_row(run: Run, metric: str, latency: dict[str, int], avg_us: float) -> str:
    return (
        f"| {run.nodes} | {run.protocol} | {metric} | {run.optimal_job_latency_s:.3f} | "
        f"{avg_us / 1_000_000:.3f} | "
        f"{latency['p50_us'] / 1_000_000:.3f} | "
        f"{latency['p90_us'] / 1_000_000:.3f} | {latency['p99_us'] / 1_000_000:.3f} | "
        f"{latency['max_us'] / 1_000_000:.3f} |"
    )


def worker_idle_table(runs: list[Run]) -> list[str]:
    lines = ["", "## Worker Idle Time", ""]
    lines.append(
        "Idle percentage is the average across workers with at least one valid request window. "
        "The window starts at the first valid request and ends at the last valid request; "
        "empty polls inside that window count as idle."
    )
    lines.extend(
        [
            "",
            "| Nodes | Protocol | Workers sampled | Avg worker idle |",
            "|---:|---|---:|---:|",
        ]
    )
    for run in sorted(runs, key=lambda row: (row.nodes, row.protocol)):
        lines.append(
            f"| {run.nodes} | {run.protocol} | {run.worker_activity_count} | "
            f"{run.worker_idle_avg_pct:.1f}% |"
        )
    return lines


def request_latency_table(runs: list[Run]) -> list[str]:
    lines = ["", "## Average Request Latency", ""]
    lines.extend(
        [
            "| Nodes | Protocol | Requests | Server avg (ms) | Client avg (ms) | Client overhead (ms) |",
            "|---:|---|---:|---:|---:|---:|",
        ]
    )
    for row in aggregate_request_latency(runs):
        overhead = max(float(row["client_avg_ms"]) - float(row["server_avg_ms"]), 0.0)
        lines.append(
            f"| {row['nodes']} | {row['protocol']} | {row['request_count']} | "
            f"{float(row['server_avg_ms']):.3f} | {float(row['client_avg_ms']):.3f} | {overhead:.3f} |"
        )
    return lines


def per_request_table(
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
) -> list[str]:
    max_node = max(metric.nodes for metric in request_metrics)
    lines = ["", "## Per-Request Scalability", ""]
    lines.append(
        "The table shows each request type at the largest node count and highlights the largest "
        "latency component in the server/client breakdown."
    )
    lines.append("")
    lines.extend(
        [
            "| Protocol | Operation | Count | Server avg (ms) | Client avg (ms) | Dominant component |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for operation in request_operations(request_metrics):
        rows = request_component_rows(operation, request_metrics, phase_metrics)
        for protocol in PROTOCOLS:
            row = next((item for item in rows if item["nodes"] == max_node and item["protocol"] == protocol), None)
            if row is None:
                continue
            dominant, value = dominant_component(row["components"])
            lines.append(
                f"| {protocol} | {operation_label(operation)} | {row['count']} | "
                f"{float(row['server_avg_ms']):.3f} | {float(row['client_avg_ms']):.3f} | "
                f"{dominant} ({value:.3f} ms) |"
            )
    lines.extend(per_request_breakdown_tables(request_metrics, phase_metrics))
    return lines


def per_request_breakdown_tables(
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
) -> list[str]:
    lines = ["", "## Per-Request Detailed Breakdown", ""]
    lines.append(
        "All values are average milliseconds per request. Phase columns are server-side "
        "measurements; `server other` is the unclassified server-side remainder; "
        "`client overhead` is client average minus server average."
    )
    for operation in request_operations(request_metrics):
        rows = request_component_rows(operation, request_metrics, phase_metrics)
        components = component_order(rows)
        lines.extend(["", f"### {operation_label(operation)}", ""])
        header = [
            "Nodes",
            "Protocol",
            "Count",
            "Server avg",
            *components,
            "Client avg",
        ]
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|---:|---|---:|" + "---:|" * (len(header) - 3))
        for row in sorted(rows, key=lambda item: (int(item["nodes"]), str(item["protocol"]))):
            component_values = [
                format_ms(float(row["components"].get(component, 0.0)))
                for component in components
            ]
            values = [
                str(row["nodes"]),
                str(row["protocol"]),
                str(row["count"]),
                format_ms(float(row["server_avg_ms"])),
                *component_values,
                format_ms(float(row["client_avg_ms"])),
            ]
            lines.append("| " + " | ".join(values) + " |")
    return lines


def format_ms(value: float) -> str:
    return f"{value:.3f}"


def dominant_component(components: dict[str, float]) -> tuple[str, float]:
    if not components:
        return "none", 0.0
    return max(components.items(), key=lambda item: item[1])


def format_set(values: list[object]) -> str:
    if len(values) == 1:
        return str(values[0])
    return ", ".join(str(value) for value in values)


if __name__ == "__main__":
    raise SystemExit(main())
