#!/usr/bin/env python3
"""Generates scalability charts and a Markdown report for distributed benchmark runs."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
from dataclasses import dataclass

import cairo


ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = ROOT / "data"
WORKLOADS = ("flat", "deep", "mixed")
PROTOCOLS = ("Grpc", "Rest")
COLORS = {
    "Grpc": (0.10, 0.34, 0.74),
    "Rest": (0.10, 0.52, 0.34),
    "grid": (0.86, 0.88, 0.90),
    "text": (0.10, 0.12, 0.15),
    "muted": (0.38, 0.42, 0.46),
    "white": (1.0, 1.0, 1.0),
    "server": (0.45, 0.50, 0.56),
    "client_overhead": (0.13, 0.16, 0.19),
    "server_other": (0.46, 0.50, 0.56),
}
COMPONENT_COLORS = {
    "client overhead": COLORS["client_overhead"],
    "server total": (0.45, 0.50, 0.56),
    "server other": COLORS["server_other"],
    "db_add": (0.09, 0.44, 0.70),
    "db_register": (0.07, 0.50, 0.46),
    "decompress": (0.50, 0.33, 0.72),
    "parse_graph": (0.58, 0.40, 0.18),
    "unframe_inputs": (0.60, 0.58, 0.15),
    "validate": (0.35, 0.61, 0.25),
    "create_jcb": (0.77, 0.29, 0.30),
    "cache_insert": (0.34, 0.62, 0.78),
    "cache_get": (0.64, 0.40, 0.73),
    "jcb_start": (0.86, 0.25, 0.10),
}
PHASE_ORDER = (
    "db_add",
    "db_register",
    "decompress",
    "parse_graph",
    "unframe_inputs",
    "validate",
    "create_jcb",
    "cache_insert",
    "cache_get",
    "jcb_start",
)
WORKLOAD_DASH = {
    "flat": (),
    "deep": (8.0, 5.0),
    "mixed": (2.0, 4.0),
}


@dataclass(frozen=True)
class Run:
    nodes: int
    protocol: str
    workload: str
    job_count: int
    task_count: int
    task_sleep_ms: int
    client_count: int
    worker_count: int
    flat_percent: int
    controller_wall_time_us: int
    job_latency: dict[str, int]
    client_request_avg_us: float
    server_request_avg_us: float
    request_count: int

    @property
    def throughput_jobs_per_sec(self) -> float:
        return self.job_count / (self.controller_wall_time_us / 1_000_000)


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

    @property
    def name(self) -> str:
        return self.operation


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
    runs, request_metrics, phase_metrics = load_results(args.inputs)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    throughput_path = args.output_dir / "scalability_throughput.png"
    latency_path = args.output_dir / "scalability_e2e_latency.png"
    request_path = args.output_dir / "scalability_request_latency.png"
    report_path = args.output_dir / "scalability_report.md"
    operations = request_operations(request_metrics)
    request_chart_paths = [
        args.output_dir / f"scalability_request_{operation}.png" for operation in operations
    ]

    draw_line_chart(
        runs,
        throughput_path,
        title="Throughput Scaling",
        subtitle="Higher is better. Total jobs scale with client nodes: 10, 20, 40 jobs.",
        y_label="Jobs / second",
        value=lambda run: run.throughput_jobs_per_sec,
    )
    draw_line_chart(
        runs,
        latency_path,
        title="End-to-End P50 Job Latency",
        subtitle="Lower is better. Measures submit-to-completion latency per job.",
        y_label="Seconds",
        value=lambda run: run.job_latency["p50_us"] / 1_000_000,
    )
    draw_request_latency_chart(runs, request_path)
    for operation, path in zip(operations, request_chart_paths, strict=True):
        draw_request_component_chart(operation, request_metrics, phase_metrics, path)
    write_report(
        report_path,
        runs,
        request_metrics,
        phase_metrics,
        [throughput_path, latency_path, request_path, *request_chart_paths],
    )

    for path in [throughput_path, latency_path, request_path, *request_chart_paths, report_path]:
        print(path)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inputs",
        nargs="+",
        type=pathlib.Path,
        default=[
            ROOT / "data/baker-1",
            ROOT / "data/baker-2",
            ROOT / "data/baker-4",
        ],
    )
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def load_results(
    input_dirs: list[pathlib.Path],
) -> tuple[list[Run], list[RequestMetric], list[PhaseMetric]]:
    runs: list[Run] = []
    request_metrics: list[RequestMetric] = []
    phase_metrics: list[PhaseMetric] = []
    for input_dir in input_dirs:
        nodes = parse_node_count(input_dir)
        for path in sorted(input_dir.glob("*.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            if not {"setup", "job_latency", "request_latency", "server_metrics"} <= data.keys():
                continue
            setup = data["setup"]
            distributed = data.get("distributed", {})
            controller_wall_time_us = int(distributed.get("controller_wall_time_us", 0))
            if controller_wall_time_us <= 0:
                continue
            client_avg, request_count = weighted_average(
                row for row in data["request_latency"] if row.get("category") != "phase"
            )
            server_avg, _ = weighted_average(
                row
                for row in data["server_metrics"].get("request_latency", [])
                if row.get("category") != "phase"
            )
            server_by_key = {
                (row["category"], row["operation"]): row
                for row in data["server_metrics"].get("request_latency", [])
                if row.get("category") != "phase"
            }
            for client_row in data["request_latency"]:
                if client_row.get("category") == "phase":
                    continue
                key = (client_row["category"], client_row["operation"])
                server_row = server_by_key.get(key)
                if server_row is None:
                    continue
                request_metrics.append(
                    RequestMetric(
                        nodes=nodes,
                        protocol=setup["protocol"],
                        workload=setup["workload"],
                        category=client_row["category"],
                        operation=client_row["operation"],
                        count=int(client_row["count"]),
                        client_avg_us=float(client_row["avg_us"]),
                        server_avg_us=float(server_row["avg_us"]),
                    )
                )
            for server_row in data["server_metrics"].get("request_latency", []):
                if server_row.get("category") != "phase":
                    continue
                request_operation, phase = split_phase_operation(server_row["operation"])
                phase_metrics.append(
                    PhaseMetric(
                        nodes=nodes,
                        protocol=setup["protocol"],
                        workload=setup["workload"],
                        request_operation=request_operation,
                        phase=phase,
                        count=int(server_row["count"]),
                        avg_us=float(server_row["avg_us"]),
                    )
                )
            runs.append(
                Run(
                    nodes=nodes,
                    protocol=setup["protocol"],
                    workload=setup["workload"],
                    job_count=int(setup["job_count"]),
                    task_count=int(setup["task_count"]),
                    task_sleep_ms=int(setup.get("task_sleep_ms", 0)),
                    client_count=int(setup["client_count"]),
                    worker_count=int(setup["worker_count"]),
                    flat_percent=int(setup["flat_percent"]),
                    controller_wall_time_us=controller_wall_time_us,
                    job_latency=data["job_latency"],
                    client_request_avg_us=client_avg,
                    server_request_avg_us=server_avg,
                    request_count=request_count,
                )
            )
    return (
        sorted(runs, key=lambda run: (run.workload, run.protocol, run.nodes)),
        sorted(
            request_metrics,
            key=lambda row: (row.workload, row.protocol, row.operation, row.nodes),
        ),
        sorted(
            phase_metrics,
            key=lambda row: (
                row.workload,
                row.protocol,
                row.request_operation,
                row.phase,
                row.nodes,
            ),
        ),
    )


def split_phase_operation(operation: str) -> tuple[str, str]:
    request_operation, phase = operation.split(".", 1)
    return request_operation, phase


def parse_node_count(path: pathlib.Path) -> int:
    suffix = path.name.rsplit("-", 1)[-1]
    try:
        return int(suffix)
    except ValueError as error:
        msg = f"input directory name must end with node count: {path}"
        raise ValueError(msg) from error


def weighted_average(rows: object) -> tuple[float, int]:
    total_weight = 0
    total = 0.0
    for row in rows:
        count = int(row["count"])
        total += float(row["avg_us"]) * count
        total_weight += count
    if total_weight == 0:
        return 0.0, 0
    return total / total_weight, total_weight


def draw_line_chart(
    runs: list[Run],
    output: pathlib.Path,
    title: str,
    subtitle: str,
    y_label: str,
    value: object,
) -> None:
    width = 1400
    height = 880
    margin_left = 110
    margin_right = 310
    margin_top = 86
    margin_bottom = 100
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)

    node_values = sorted({run.nodes for run in runs})
    max_y = max(float(value(run)) for run in runs)
    y_max = nice_upper_bound(max_y)

    draw_axes(
        ctx,
        title,
        subtitle,
        y_label,
        margin_left,
        margin_top,
        plot_width,
        plot_height,
        node_values,
        y_max,
    )

    legend_items: list[tuple[str, tuple[float, float, float], tuple[float, ...]]] = []
    for workload in WORKLOADS:
        for protocol in PROTOCOLS:
            series = [run for run in runs if run.workload == workload and run.protocol == protocol]
            if not series:
                continue
            color = COLORS[protocol]
            dash = WORKLOAD_DASH[workload]
            draw_series(
                ctx,
                series,
                node_values,
                margin_left,
                margin_top,
                plot_width,
                plot_height,
                y_max,
                color,
                dash,
                value,
            )
            legend_items.append((f"{protocol} {workload}", color, dash))

    draw_legend(ctx, legend_items, width - margin_right + 30, margin_top)
    draw_workload_style_note(ctx, width - margin_right + 30, margin_top + 190)
    surface.write_to_png(str(output))


def draw_request_latency_chart(runs: list[Run], output: pathlib.Path) -> None:
    width = 1400
    height = 850
    margin_left = 110
    margin_right = 90
    margin_top = 90
    margin_bottom = 120
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    rows = aggregate_request_latency(runs)
    max_y = max(max(row["client_avg_us"], row["server_avg_us"]) / 1000 for row in rows)
    y_max = nice_upper_bound(max_y)
    node_values = sorted({row["nodes"] for row in rows})
    group_width = plot_width / len(node_values)
    x_positions = [
        margin_left + group_width * index + group_width / 2
        for index, _nodes in enumerate(node_values)
    ]

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_axes(
        ctx,
        "Average Request Latency",
        "Weighted average across all workloads and request types. Lower is better.",
        "Milliseconds",
        margin_left,
        margin_top,
        plot_width,
        plot_height,
        node_values,
        y_max,
        x_positions,
    )

    bar_width = group_width / 7
    offsets = {
        ("Grpc", "server"): -2.0,
        ("Grpc", "client"): -1.0,
        ("Rest", "server"): 1.0,
        ("Rest", "client"): 2.0,
    }
    colors = {
        ("Grpc", "server"): lighten(COLORS["Grpc"], 0.55),
        ("Grpc", "client"): COLORS["Grpc"],
        ("Rest", "server"): lighten(COLORS["Rest"], 0.55),
        ("Rest", "client"): COLORS["Rest"],
    }
    by_key = {(row["nodes"], row["protocol"]): row for row in rows}
    for node_index, nodes in enumerate(node_values):
        center = margin_left + group_width * node_index + group_width / 2
        for protocol in PROTOCOLS:
            row = by_key[(nodes, protocol)]
            for side, source_key in [
                ("server", "server_avg_us"),
                ("client", "client_avg_us"),
            ]:
                value_ms = row[source_key] / 1000
                x = center + offsets[(protocol, side)] * bar_width
                bar_height = plot_height * value_ms / y_max
                set_color(ctx, colors[(protocol, side)])
                ctx.rectangle(
                    x - bar_width / 2,
                    margin_top + plot_height - bar_height,
                    bar_width,
                    bar_height,
                )
                ctx.fill()

    legend = [
        ("gRPC server", colors[("Grpc", "server")], ()),
        ("gRPC client", colors[("Grpc", "client")], ()),
        ("REST server", colors[("Rest", "server")], ()),
        ("REST client", colors[("Rest", "client")], ()),
    ]
    draw_legend(ctx, legend, width - 260, margin_top)
    draw_multiline_note(
        ctx,
        [
            "Server: time measured inside storage API",
            "Client: observed request latency",
            "Gap: transport/client overhead",
        ],
        width - 430,
        margin_top + 145,
    )
    surface.write_to_png(str(output))


def aggregate_request_latency(runs: list[Run]) -> list[dict[str, float | int | str]]:
    rows = []
    for nodes in sorted({run.nodes for run in runs}):
        for protocol in PROTOCOLS:
            selected = [run for run in runs if run.nodes == nodes and run.protocol == protocol]
            request_count = sum(run.request_count for run in selected)
            client_total = sum(run.client_request_avg_us * run.request_count for run in selected)
            server_total = sum(run.server_request_avg_us * run.request_count for run in selected)
            rows.append(
                {
                    "nodes": nodes,
                    "protocol": protocol,
                    "request_count": request_count,
                    "client_avg_us": client_total / request_count if request_count else 0,
                    "server_avg_us": server_total / request_count if request_count else 0,
                }
            )
    return rows


def request_operations(metrics: list[RequestMetric]) -> list[str]:
    return sorted({metric.operation for metric in metrics})


def draw_request_component_chart(
    operation: str,
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
    output: pathlib.Path,
) -> None:
    width = 1500
    height = 820
    margin_left = 110
    margin_right = 380
    margin_top = 110
    margin_bottom = 110
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    rows = request_component_rows(operation, request_metrics, phase_metrics)
    components = component_order(rows)
    y_max = nice_upper_bound(max(row["client_avg_ms"] for row in rows))
    node_values = [1, 2, 4]
    group_width = plot_width / len(node_values)
    x_positions = [
        margin_left + group_width * index + group_width / 2
        for index, _nodes in enumerate(node_values)
    ]

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    ctx = cairo.Context(surface)
    paint_background(ctx)
    draw_axes(
        ctx,
        f"{operation} Latency Components",
        "Stacked bars show server work plus client overhead at each node count. Lower is better.",
        "Milliseconds",
        margin_left,
        margin_top,
        plot_width,
        plot_height,
        node_values,
        y_max,
        x_positions,
    )

    bar_width = group_width / 4.8
    bar_offsets = {"Grpc": -0.65, "Rest": 0.65}
    by_key = {(row["nodes"], row["protocol"]): row for row in rows}
    for node_index, nodes in enumerate(node_values):
        center = margin_left + group_width * node_index + group_width / 2
        for protocol in PROTOCOLS:
            row = by_key[(nodes, protocol)]
            x = center + bar_offsets[protocol] * bar_width
            y_base = margin_top + plot_height
            for component in components:
                value_ms = row["components"].get(component, 0.0)
                if value_ms <= 0:
                    continue
                bar_height = plot_height * value_ms / y_max
                y_base -= bar_height
                set_color(ctx, component_color(component))
                ctx.rectangle(x - bar_width / 2, y_base, bar_width, bar_height)
                ctx.fill()
            set_color(ctx, COLORS["text"])
            select_font(ctx, 13)
            label = "gRPC" if protocol == "Grpc" else "REST"
            extents = ctx.text_extents(label)
            ctx.move_to(x - extents.width / 2, margin_top + plot_height + 24)
            ctx.show_text(label)

    draw_component_legend(ctx, components, width - margin_right + 30, margin_top)
    draw_multiline_note(
        ctx,
        [
            "client overhead = client avg - server avg",
            "server other = server avg - measured phases",
            "values are weighted across all workloads",
        ],
        width - margin_right + 30,
        height - 90,
    )
    surface.write_to_png(str(output))


def request_component_rows(
    operation: str,
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
) -> list[dict[str, object]]:
    rows = []
    for nodes in [1, 2, 4]:
        for protocol in PROTOCOLS:
            selected_requests = [
                metric
                for metric in request_metrics
                if metric.operation == operation
                and metric.nodes == nodes
                and metric.protocol == protocol
            ]
            count = sum(metric.count for metric in selected_requests)
            client_avg_us = weighted_metric_average(
                (metric.client_avg_us, metric.count) for metric in selected_requests
            )
            server_avg_us = weighted_metric_average(
                (metric.server_avg_us, metric.count) for metric in selected_requests
            )
            selected_phases = [
                metric
                for metric in phase_metrics
                if metric.request_operation == operation
                and metric.nodes == nodes
                and metric.protocol == protocol
            ]
            components = {
                phase: weighted_metric_average(
                    (metric.avg_us, metric.count)
                    for metric in selected_phases
                    if metric.phase == phase
                )
                / 1000
                for phase in sorted({metric.phase for metric in selected_phases})
            }
            server_avg_ms = server_avg_us / 1000
            phase_sum = sum(components.values())
            if components:
                components["server other"] = max(server_avg_ms - phase_sum, 0.0)
            else:
                components["server total"] = server_avg_ms
            components["client overhead"] = max((client_avg_us - server_avg_us) / 1000, 0.0)
            rows.append(
                {
                    "nodes": nodes,
                    "protocol": protocol,
                    "count": count,
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
    if weight == 0:
        return 0.0
    return total / weight


def component_order(rows: list[dict[str, object]]) -> list[str]:
    names = {
        name
        for row in rows
        for name, value in row["components"].items()
        if float(value) > 0
    }
    ordered = [name for name in PHASE_ORDER if name in names]
    for name in ["server total", "server other", "client overhead"]:
        if name in names:
            ordered.append(name)
    return ordered


def component_color(component: str) -> tuple[float, float, float]:
    return COMPONENT_COLORS.get(component, COLORS["server_other"])


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


def aggregate_request_trends(
    metrics: list[RequestMetric],
    *,
    by_workload: bool,
) -> list[dict[str, float | int | str]]:
    keys = sorted(
        {
            (
                metric.workload if by_workload else "all",
                metric.protocol,
                metric.name,
            )
            for metric in metrics
        }
    )
    rows = []
    for workload, protocol, operation in keys:
        selected = [
            metric
            for metric in metrics
            if (metric.workload == workload or not by_workload)
            and metric.protocol == protocol
            and metric.name == operation
        ]
        by_node = {}
        for nodes in [1, 2, 4]:
            node_rows = [metric for metric in selected if metric.nodes == nodes]
            count = sum(metric.count for metric in node_rows)
            client_total = sum(metric.client_avg_us * metric.count for metric in node_rows)
            server_total = sum(metric.server_avg_us * metric.count for metric in node_rows)
            by_node[nodes] = {
                "count": count,
                "client_avg_us": client_total / count if count else 0.0,
                "server_avg_us": server_total / count if count else 0.0,
            }
        rows.append(
            {
                "workload": workload,
                "protocol": protocol,
                "operation": operation,
                "count_4_node": by_node[4]["count"],
                "client_1_ms": by_node[1]["client_avg_us"] / 1000,
                "client_2_ms": by_node[2]["client_avg_us"] / 1000,
                "client_4_ms": by_node[4]["client_avg_us"] / 1000,
                "server_1_ms": by_node[1]["server_avg_us"] / 1000,
                "server_2_ms": by_node[2]["server_avg_us"] / 1000,
                "server_4_ms": by_node[4]["server_avg_us"] / 1000,
            }
        )
    return rows


def draw_axes(
    ctx: cairo.Context,
    title: str,
    subtitle: str,
    y_label: str,
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    node_values: list[int],
    y_max: float,
    x_positions: list[float] | None = None,
) -> None:
    set_color(ctx, COLORS["text"])
    select_font(ctx, 28, bold=True)
    ctx.move_to(margin_left, 44)
    ctx.show_text(title)
    select_font(ctx, 15)
    set_color(ctx, COLORS["muted"])
    ctx.move_to(margin_left, 68)
    ctx.show_text(subtitle)

    select_font(ctx, 17)
    ctx.move_to(margin_left, margin_top + plot_height + 66)
    ctx.show_text("Client nodes")
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
        label = y_max * index / 5
        set_color(ctx, COLORS["muted"])
        select_font(ctx, 14)
        ctx.move_to(margin_left - 82, y + 5)
        ctx.show_text(format_axis_value(label))
        set_color(ctx, COLORS["grid"])

    set_color(ctx, COLORS["text"])
    ctx.set_line_width(2)
    ctx.move_to(margin_left, margin_top)
    ctx.line_to(margin_left, margin_top + plot_height)
    ctx.line_to(margin_left + plot_width, margin_top + plot_height)
    ctx.stroke()

    select_font(ctx, 16)
    for index, nodes in enumerate(node_values):
        x = (
            x_positions[index]
            if x_positions is not None
            else x_for_node(index, len(node_values), margin_left, plot_width)
        )
        ctx.move_to(x - 8, margin_top + plot_height + 30)
        ctx.show_text(str(nodes))


def draw_series(
    ctx: cairo.Context,
    series: list[Run],
    node_values: list[int],
    margin_left: int,
    margin_top: int,
    plot_width: int,
    plot_height: int,
    y_max: float,
    color: tuple[float, float, float],
    dash: tuple[float, ...],
    value: object,
) -> None:
    series = sorted(series, key=lambda run: run.nodes)
    set_color(ctx, color)
    ctx.set_line_width(3)
    ctx.set_dash(dash)
    for index, run in enumerate(series):
        node_index = node_values.index(run.nodes)
        x = x_for_node(node_index, len(node_values), margin_left, plot_width)
        y = margin_top + plot_height - plot_height * float(value(run)) / y_max
        if index == 0:
            ctx.move_to(x, y)
        else:
            ctx.line_to(x, y)
    ctx.stroke()
    ctx.set_dash(())
    for run in series:
        node_index = node_values.index(run.nodes)
        x = x_for_node(node_index, len(node_values), margin_left, plot_width)
        y = margin_top + plot_height - plot_height * float(value(run)) / y_max
        ctx.arc(x, y, 5, 0, 2 * math.pi)
        ctx.fill()


def draw_legend(
    ctx: cairo.Context,
    items: list[tuple[str, tuple[float, float, float], tuple[float, ...]]],
    x: int,
    y: int,
) -> None:
    select_font(ctx, 15)
    for index, (label, color, dash) in enumerate(items):
        row_y = y + index * 28
        set_color(ctx, color)
        ctx.set_line_width(3)
        ctx.set_dash(dash)
        ctx.move_to(x, row_y)
        ctx.line_to(x + 38, row_y)
        ctx.stroke()
        ctx.set_dash(())
        set_color(ctx, COLORS["text"])
        ctx.move_to(x + 48, row_y + 5)
        ctx.show_text(label)


def draw_workload_style_note(ctx: cairo.Context, x: int, y: int) -> None:
    draw_multiline_note(
        ctx,
        [
            "Line style identifies workload:",
            "solid = flat",
            "dashed = deep",
            "dotted = mixed",
            "Color identifies protocol.",
        ],
        x,
        y,
    )


def draw_multiline_note(ctx: cairo.Context, lines: list[str], x: int, y: int) -> None:
    select_font(ctx, 13)
    set_color(ctx, COLORS["muted"])
    for index, line in enumerate(lines):
        ctx.move_to(x, y + index * 20)
        ctx.show_text(line)


def x_for_node(index: int, count: int, margin_left: int, plot_width: int) -> float:
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
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def write_report(
    output: pathlib.Path,
    runs: list[Run],
    request_metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
    chart_paths: list[pathlib.Path],
) -> None:
    lines = [
        "# Storage API Scalability Report",
        "",
        "## Setup",
        "",
        setup_paragraph(runs),
        "",
    ]
    lines.extend(throughput_table(runs))
    lines.extend(speedup_table(runs))
    lines.extend(e2e_table(runs))
    lines.extend(request_latency_table(runs))
    lines.extend(per_request_summary(request_metrics, phase_metrics))
    lines.extend(per_request_interpretation(request_metrics, phase_metrics))
    lines.extend(per_request_scalability_table(request_metrics))
    lines.extend(conclusion(runs))
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def setup_paragraph(runs: list[Run]) -> str:
    node_counts = ", ".join(str(node) for node in sorted({run.nodes for run in runs}))
    job_counts = sorted({run.job_count for run in runs})
    task_counts = sorted({run.task_count for run in runs})
    task_sleep_ms = sorted({run.task_sleep_ms for run in runs})
    per_agent_jobs = sorted(
        {
            run.job_count // run.nodes
            for run in runs
            if run.nodes > 0 and run.job_count % run.nodes == 0
        }
    )
    clients = sorted({run.client_count for run in runs})
    workers = sorted({run.worker_count for run in runs})
    flat_percent = sorted({run.flat_percent for run in runs})
    return (
        f"The scalability data compares {node_counts} client-node runs. Each run uses the same "
        f"storage server and scales client agents while keeping {format_set(per_agent_jobs)} jobs "
        f"per agent ({format_set(job_counts)} total jobs), a `task_count` setting of "
        f"{format_set(task_counts)}, {format_set(task_sleep_ms)} ms simulated sleep per task, "
        f"{format_set(clients)} submitter clients per agent, and "
        f"{format_set(workers)} workers per agent. The workloads are flat, deep, and mixed task "
        f"graphs; mixed uses {format_set(flat_percent)}% flat jobs and the remaining jobs are "
        "deep."
    )


def throughput_table(runs: list[Run]) -> list[str]:
    lines = ["", "## Throughput", ""]
    lines.extend(
        [
            "| Nodes | Workload | Protocol | Jobs | Tasks | Runtime (s) | Throughput (jobs/s) |",
            "|---:|---|---|---:|---:|---:|---:|",
        ]
    )
    for run in sorted(runs, key=lambda item: (item.workload, item.protocol, item.nodes)):
        lines.append(
            f"| {run.nodes} | {run.workload} | {run.protocol} | {run.job_count} | "
            f"{run.task_count} | "
            f"{run.controller_wall_time_us / 1_000_000:.2f} | "
            f"{run.throughput_jobs_per_sec:.2f} |"
        )
    return lines


def speedup_table(runs: list[Run]) -> list[str]:
    lines = ["", "## Speedup And Efficiency", ""]
    lines.extend(
        [
            "| Nodes | Workload | Protocol | Speedup vs 1 node | Efficiency |",
            "|---:|---|---|---:|---:|",
        ]
    )
    baseline = {
        (run.workload, run.protocol): run.throughput_jobs_per_sec for run in runs if run.nodes == 1
    }
    for run in sorted(runs, key=lambda item: (item.workload, item.protocol, item.nodes)):
        base = baseline[(run.workload, run.protocol)]
        speedup = run.throughput_jobs_per_sec / base
        lines.append(
            f"| {run.nodes} | {run.workload} | {run.protocol} | {speedup:.2f}x | "
            f"{speedup / run.nodes:.2%} |"
        )
    return lines


def e2e_table(runs: list[Run]) -> list[str]:
    lines = ["", "## End-To-End Job Latency", ""]
    lines.extend(
        [
            "| Nodes | Workload | Protocol | P50 (s) | P90 (s) | P99 (s) | Max (s) |",
            "|---:|---|---|---:|---:|---:|---:|",
        ]
    )
    for run in sorted(runs, key=lambda item: (item.workload, item.protocol, item.nodes)):
        lines.append(
            f"| {run.nodes} | {run.workload} | {run.protocol} | "
            f"{run.job_latency['p50_us'] / 1_000_000:.2f} | "
            f"{run.job_latency['p90_us'] / 1_000_000:.2f} | "
            f"{run.job_latency['p99_us'] / 1_000_000:.2f} | "
            f"{run.job_latency['max_us'] / 1_000_000:.2f} |"
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
        overhead_us = max(float(row["client_avg_us"]) - float(row["server_avg_us"]), 0)
        lines.append(
            f"| {row['nodes']} | {row['protocol']} | {row['request_count']} | "
            f"{float(row['server_avg_us']) / 1000:.2f} | "
            f"{float(row['client_avg_us']) / 1000:.2f} | {overhead_us / 1000:.2f} |"
        )
    return lines


def per_request_summary(
    metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
) -> list[str]:
    rows = aggregate_request_trends(metrics, by_workload=False)
    component_rows = {
        (row["protocol"], operation, row["nodes"]): row
        for operation in request_operations(metrics)
        for row in request_component_rows(operation, metrics, phase_metrics)
    }
    lines = ["", "## Per-Request Scalability Summary", ""]
    lines.append(
        "The table below aggregates each request type across all workloads and shows the trend "
        "from 1 to 2 to 4 client nodes. The dominant 4-node component is taken from the stacked "
        "server-phase/client-overhead breakdown used by the per-request charts."
    )
    lines.append("")
    lines.extend(
        [
            "| Protocol | Operation | Client avg 1/2/4 nodes (ms) | "
            "Server avg 1/2/4 nodes (ms) | Dominant 4-node component |",
            "|---|---|---:|---:|---|",
        ]
    )
    for row in sorted(rows, key=lambda item: (item["operation"], item["protocol"])):
        component_row = component_rows[(row["protocol"], row["operation"], 4)]
        lines.append(
            f"| {row['protocol']} | `{row['operation']}` | "
            f"{trend(float(row['client_1_ms']), float(row['client_2_ms']), float(row['client_4_ms']))} | "
            f"{trend(float(row['server_1_ms']), float(row['server_2_ms']), float(row['server_4_ms']))} | "
            f"{dominant_component(component_row)} |"
        )
    return lines


def per_request_scalability_table(metrics: list[RequestMetric]) -> list[str]:
    rows = aggregate_request_trends(metrics, by_workload=True)
    lines = ["", "## Per-Request Scalability By Workload", ""]
    lines.append(
        "This table keeps the workload dimension, so changes caused by flat, deep, and mixed task "
        "graphs do not get hidden by the aggregate request average. Values are average latencies "
        "at 1, 2, and 4 client nodes."
    )
    lines.append("")
    lines.extend(
        [
            "| Workload | Protocol | Operation | Client avg 1/2/4 nodes (ms) | "
            "Server avg 1/2/4 nodes (ms) |",
            "|---|---|---|---:|---:|",
        ]
    )
    for row in sorted(rows, key=lambda item: (item["workload"], item["operation"], item["protocol"])):
        lines.append(
            f"| {row['workload']} | {row['protocol']} | `{row['operation']}` | "
            f"{trend(float(row['client_1_ms']), float(row['client_2_ms']), float(row['client_4_ms']))} | "
            f"{trend(float(row['server_1_ms']), float(row['server_2_ms']), float(row['server_4_ms']))} |"
        )
    return lines


def per_request_interpretation(
    metrics: list[RequestMetric],
    phase_metrics: list[PhaseMetric],
) -> list[str]:
    trend_rows = {
        (row["protocol"], row["operation"]): row
        for row in aggregate_request_trends(metrics, by_workload=False)
    }
    component_rows = {
        (row["protocol"], operation, row["nodes"]): row
        for operation in request_operations(metrics)
        for row in request_component_rows(operation, metrics, phase_metrics)
    }
    start_grpc = trend_rows[("Grpc", "start_job")]
    start_rest = trend_rows[("Rest", "start_job")]
    session_grpc = trend_rows[("Grpc", "get_session")]
    session_rest = trend_rows[("Rest", "get_session")]
    poll_grpc = trend_rows[("Grpc", "poll_ready_tasks")]
    poll_rest = trend_rows[("Rest", "poll_ready_tasks")]
    register_grpc = trend_rows[("Grpc", "register_job")]
    register_rest = trend_rows[("Rest", "register_job")]
    return [
        "",
        "## Per-Request Interpretation",
        "",
        (
            "`start_job` is the clearest scaling-sensitive storage request. Its gRPC client "
            f"trend is {trend(float(start_grpc['client_1_ms']), float(start_grpc['client_2_ms']), float(start_grpc['client_4_ms']))} ms, "
            f"and REST is {trend(float(start_rest['client_1_ms']), float(start_rest['client_2_ms']), float(start_rest['client_4_ms']))} ms. "
            "At 4 nodes, the largest component is "
            f"{dominant_component(component_rows[('Grpc', 'start_job', 4)])} for gRPC and "
            f"{dominant_component(component_rows[('Rest', 'start_job', 4)])} for REST, which "
            "keeps the focus on the server-side work rather than only transport overhead."
        ),
        "",
        (
            "`get_session` has near-zero measured server time, so the visible latency is almost "
            "entirely client/transport overhead or timing granularity. The gRPC client trend is "
            f"{trend(float(session_grpc['client_1_ms']), float(session_grpc['client_2_ms']), float(session_grpc['client_4_ms']))} ms, "
            f"and REST is {trend(float(session_rest['client_1_ms']), float(session_rest['client_2_ms']), float(session_rest['client_4_ms']))} ms."
        ),
        "",
        (
            "`poll_ready_tasks` is the dominant high-count polling request and stays relatively "
            f"stable: gRPC is {trend(float(poll_grpc['client_1_ms']), float(poll_grpc['client_2_ms']), float(poll_grpc['client_4_ms']))} ms, "
            f"and REST is {trend(float(poll_rest['client_1_ms']), float(poll_rest['client_2_ms']), float(poll_rest['client_4_ms']))} ms. "
            "Because this request does not have server phase instrumentation, the chart shows "
            "server total plus client overhead."
        ),
        "",
        (
            "`register_job` has the richest server breakdown. Its gRPC aggregate trend is "
            f"{trend(float(register_grpc['client_1_ms']), float(register_grpc['client_2_ms']), float(register_grpc['client_4_ms']))} ms, "
            f"and REST is {trend(float(register_rest['client_1_ms']), float(register_rest['client_2_ms']), float(register_rest['client_4_ms']))} ms. "
            "The workload table should be used for this request because flat, deep, and mixed "
            "graphs exercise graph parsing, decompression, and database registration differently."
        ),
    ]


def trend(one_node: float, two_node: float, four_node: float) -> str:
    return f"{one_node:.3f} / {two_node:.3f} / {four_node:.3f}"


def dominant_component(row: dict[str, object]) -> str:
    components = {
        name: float(value)
        for name, value in row["components"].items()
        if float(value) > 0
    }
    if not components:
        return "n/a"
    name, value = max(components.items(), key=lambda item: item[1])
    return f"{name} ({value:.3f} ms)"


def conclusion(runs: list[Run]) -> list[str]:
    lines = ["", "## Conclusion", ""]
    best = max(runs, key=lambda run: run.throughput_jobs_per_sec)
    flat_runs = [run for run in runs if run.workload == "flat"]
    deep_runs = [run for run in runs if run.workload == "deep"]
    mixed_runs = [run for run in runs if run.workload == "mixed"]
    lines.append(
        f"The highest throughput is {best.throughput_jobs_per_sec:.2f} jobs/s for "
        f"{best.protocol} {best.workload} at {best.nodes} nodes. Flat workloads complete in "
        f"{min(run.job_latency['p50_us'] for run in flat_runs) / 1_000_000:.2f}-"
        f"{max(run.job_latency['p50_us'] for run in flat_runs) / 1_000_000:.2f}s P50, while "
        f"deep and mixed workloads are dominated by roughly "
        f"{min(run.job_latency['p50_us'] for run in [*deep_runs, *mixed_runs]) / 1_000_000:.1f}-"
        f"{max(run.job_latency['p50_us'] for run in [*deep_runs, *mixed_runs]) / 1_000_000:.1f}s "
        "P50 job runtimes."
    )
    lines.append("")
    lines.append(
        "Scaling is clearest when total job count grows with node count. Throughput generally "
        "improves from 1 to 4 nodes, but efficiency is workload dependent because controller wall "
        "time includes fixed setup/monitoring costs and the deep/mixed runs are constrained by the "
        "long dependency chains in each job."
    )
    return lines


def format_set(values: list[int]) -> str:
    if len(values) == 1:
        return str(values[0])
    return "/".join(str(value) for value in values)


def paint_background(ctx: cairo.Context) -> None:
    set_color(ctx, COLORS["white"])
    ctx.paint()


def set_color(ctx: cairo.Context, color: tuple[float, float, float]) -> None:
    ctx.set_source_rgb(*color)


def lighten(color: tuple[float, float, float], amount: float) -> tuple[float, float, float]:
    return tuple(component + (1.0 - component) * amount for component in color)


def select_font(ctx: cairo.Context, size: int, *, bold: bool = False) -> None:
    ctx.select_font_face(
        "Sans",
        cairo.FONT_SLANT_NORMAL,
        cairo.FONT_WEIGHT_BOLD if bold else cairo.FONT_WEIGHT_NORMAL,
    )
    ctx.set_font_size(size)


if __name__ == "__main__":
    raise SystemExit(main())
