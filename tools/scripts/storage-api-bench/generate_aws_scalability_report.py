#!/usr/bin/env python3
"""Generates AWS scalability charts and a Markdown report."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
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
    "worker_poll_ready_tasks_returned",
    "scheduler_poll_ready_tasks",
    "register_execution_manager",
    "register_job",
    "start_job",
    "succeed_task_instance",
)
WORKER_POLL_OPERATION = "worker_poll_ready_tasks"
WORKER_POLL_RETURNED_OPERATION = "worker_poll_ready_tasks_returned"
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

    def jobs_per_node_distribution(self) -> list[int]:
        base_jobs = self.job_count // self.nodes
        extra_jobs = self.job_count % self.nodes
        return [
            base_jobs + (1 if node_index < extra_jobs else 0)
            for node_index in range(self.nodes)
        ]

    def optimal_latency_for_parallelism(
        self,
        worker_slots: int,
        active_jobs: int,
        per_job_parallelism: int,
    ) -> float:
        if per_job_parallelism <= 0:
            return 0.0
        slots_per_job = max(1, min(per_job_parallelism, worker_slots // active_jobs))
        task_waves = math.ceil(self.task_count / slots_per_job)
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


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    runs, request_metrics, phase_metrics = load_results(args.input_dir)
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

    for operation in request_operations(request_metrics):
        if operation == WORKER_POLL_RETURNED_OPERATION:
            continue
        path = args.output_dir / f"aws_request_{operation}.png"
        draw_request_component_chart(operation, request_metrics, phase_metrics, path)
        chart_paths.append(path)
    stale_returned_chart = args.output_dir / f"aws_request_{WORKER_POLL_RETURNED_OPERATION}.png"
    if stale_returned_chart.exists():
        stale_returned_chart.unlink()

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
            elif operation == "poll_ready_tasks_returned":
                operation = WORKER_POLL_RETURNED_OPERATION
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
        client_avg, request_count = weighted_average(non_derived_request_rows(data["request_latency"]))
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


def non_derived_request_rows(rows: object) -> list[object]:
    return [
        row
        for row in rows
        if row.get("category") != "phase"
        and row.get("operation") != "poll_ready_tasks_returned"
        and row.get("operation") != WORKER_POLL_RETURNED_OPERATION
    ]


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
    if operation == WORKER_POLL_OPERATION:
        draw_worker_poll_component_chart(request_metrics, phase_metrics, output)
        return
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


def draw_worker_poll_component_chart(
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
    output: pathlib.Path,
) -> None:
    rows = []
    for operation, variant in [
        (WORKER_POLL_OPERATION, "all"),
        (WORKER_POLL_RETURNED_OPERATION, "returned task"),
    ]:
        for row in request_component_rows(operation, request_metrics, phase_metrics):
            row = dict(row)
            row["variant"] = variant
            rows.append(row)
    if not rows:
        return
    components = component_order(rows)
    nodes = sorted({int(row["nodes"]) for row in rows})
    width = 2100
    height = 940
    margin_left = 115
    margin_right = 385
    margin_top = 105
    margin_bottom = 150
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    y_max = nice_upper_bound(max(float(row["client_avg_ms"]) for row in rows))
    group_width = plot_width / len(nodes)
    bar_width = min(44, group_width / 5.8)

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_axes(
        ctx,
        "AWS Worker To Scheduler Poll Latency Components",
        "Each node group shows all worker polls and only polls that returned a task.",
        "Milliseconds",
        margin_left,
        margin_top,
        plot_width,
        plot_height,
        nodes,
        y_max,
        x_positions=[margin_left + group_width * index + group_width / 2 for index in range(len(nodes))],
    )

    row_by_key = {
        (int(row["nodes"]), str(row["protocol"]), str(row["variant"])): row
        for row in rows
    }
    bar_specs = [
        ("Grpc", "all", -1.65, "g all"),
        ("Grpc", "returned task", -0.55, "g task"),
        ("Rest", "all", 0.55, "r all"),
        ("Rest", "returned task", 1.65, "r task"),
    ]
    active_specs = [
        spec
        for spec in bar_specs
        if any(row["protocol"] == spec[0] and row["variant"] == spec[1] for row in rows)
    ]
    if len(active_specs) == 2:
        active_specs = [
            (protocol, variant, offset, label)
            for (protocol, variant, _offset, label), offset in zip(active_specs, [-0.6, 0.6])
        ]
    for index, nodes_value in enumerate(nodes):
        center = margin_left + group_width * index + group_width / 2
        for protocol, variant, offset, label in active_specs:
            row = row_by_key.get((nodes_value, protocol, variant))
            if row is None:
                continue
            x = center + offset * bar_width
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
            select_font(ctx, 11)
            set_color(ctx, COLORS["text"])
            extents = ctx.text_extents(label)
            ctx.move_to(x - extents.width / 2, margin_top + plot_height + 48)
            ctx.show_text(label)
    draw_component_legend(ctx, components, width - margin_right + 30, margin_top)
    draw_note(
        ctx,
        [
            "all = every worker poll request",
            "task = only polls that returned a task",
            "scheduler response = scheduler handler latency",
        ],
        width - margin_right + 30,
        height - 132,
    )
    surface.write_to_png(str(output))


def operation_label(operation: str) -> str:
    labels = {
        WORKER_POLL_OPERATION: "Worker To Scheduler Poll",
        WORKER_POLL_RETURNED_OPERATION: "Worker To Scheduler Poll Returned Task",
        SCHEDULER_STORAGE_POLL_OPERATION: "Scheduler To Storage Poll",
    }
    return labels.get(operation, operation)


def request_chart_subtitle(operation: str) -> str:
    if operation in {WORKER_POLL_OPERATION, WORKER_POLL_RETURNED_OPERATION}:
        return "Worker calls to the scheduler cache. Stacked bars show scheduler handler time and client overhead."
    if operation == SCHEDULER_STORAGE_POLL_OPERATION:
        return "Scheduler batch polling against the storage server. Lower is better."
    return "Stacked bars show server work and client overhead. Lower is better."


def request_chart_notes(operation: str) -> list[str]:
    if operation in {WORKER_POLL_OPERATION, WORKER_POLL_RETURNED_OPERATION}:
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
            if operation in {WORKER_POLL_OPERATION, WORKER_POLL_RETURNED_OPERATION}:
                components["scheduler response"] = server_avg_ms
                components["client overhead"] = max((client_avg_us - server_avg_us) / 1000, 0.0)
            elif components:
                components["server other"] = max(server_avg_ms - phase_sum, 0.0)
            else:
                components["server total"] = server_avg_ms
            if operation not in {WORKER_POLL_OPERATION, WORKER_POLL_RETURNED_OPERATION}:
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
    jobs_per_node = sorted({format_jobs_per_node(run) for run in runs})
    submitters = sorted({run.submitter_count for run in runs})
    workers = sorted({run.worker_count for run in runs})
    workloads = sorted({run.workload for run in runs})
    protocols = sorted({run.protocol for run in runs})
    if len(protocols) == 1:
        protocol_sentence = (
            f"Results cover {protocols[0]} against the RDS-backed storage layer."
        )
    else:
        protocol_sentence = (
            f"Results compare {format_set(protocols)} against the same storage server "
            "and RDS-backed storage layer."
        )
    return (
        f"The AWS benchmark data covers {format_set(node_counts)} worker-node runs for "
        f"{format_set(workloads)} workload. Each job has {format_set(task_count)} tasks with "
        f"{format_set(task_sleep_ms)} ms simulated sleep per task, and the run size scales from {format_set(job_counts)} jobs "
        f"({format_set(total_tasks)} total tasks), or {format_set(jobs_per_node)} jobs per worker node. "
        "Each run uses one dedicated submitter node "
        f"with {format_set(submitters)} submitter clients and {format_set(workers)} worker "
        "processes per worker node. The optimal E2E time is the ideal per-active-job compute "
        "lower bound: `ceil(task_count / min(per_job_parallelism, "
        "floor(worker_nodes * workers_per_node / active_jobs))) * task_sleep_ms`, where "
        "`active_jobs = min(job_count, submitter_clients)`. Flat jobs can expose `task_count` "
        "runnable tasks, and deep jobs can expose one runnable task. This excludes storage, "
        f"scheduling, polling, and transport overhead. {protocol_sentence}"
    )

def format_jobs_per_node(run: Run) -> str:
    distribution = run.jobs_per_node_distribution()
    values = sorted(set(distribution))
    if len(values) == 1:
        return str(values[0])
    return f"{values[0]}-{values[-1]}"


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
        if run.protocol not in baseline or baseline[run.protocol] <= 0:
            lines.append(f"| {run.nodes} | {run.protocol} | n/a | n/a |")
            continue
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
