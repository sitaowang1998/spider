#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "matplotlib",
# ]
# ///
"""
Spider Executor Overhead Benchmark Timeline Visualizer

Visualizes timing breakdown of Spider task execution, comparing
C++ and Python task overhead.

Data Sources:
- C++ Worker (spider_worker_*.log): fetch_input, spawn, execution, submit_output
- Task Executor: func_entry, func_exit (for actual function execution time)
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


@dataclass
class TaskTiming:
    """Timing data for a single task."""

    task_id: str
    func_name: str = ""
    # Worker phases (C++)
    fetch_start: Optional[datetime] = None
    fetch_end: Optional[datetime] = None
    spawn_start: Optional[datetime] = None
    spawn_end: Optional[datetime] = None
    execution_start: Optional[datetime] = None
    execution_end: Optional[datetime] = None
    submit_start: Optional[datetime] = None
    submit_end: Optional[datetime] = None
    # Executor phases (func_entry/exit from C++ or Python task executor)
    func_entry: Optional[datetime] = None
    func_exit: Optional[datetime] = None


@dataclass
class WorkerTimeline:
    """Timeline data for a single worker."""

    worker_id: str
    tasks: list[TaskTiming] = field(default_factory=list)


# Log line timestamp pattern: "[2026-01-30 15:17:10.214]"
LOG_TIMESTAMP_PATTERN = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]")

# C++ TIMING patterns for worker phases
CPP_TIMING_PATTERNS = {
    "fetch_input": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"fetch_input_start=(\d+)\s+fetch_input_end=(\d+)\s+fetch_input_duration_ms=\d+"
    ),
    "spawn": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"spawn_start=(\d+)\s+spawn_end=(\d+)\s+spawn_duration_ms=\d+"
    ),
    "execution": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"execution_start=(\d+)\s+execution_end=(\d+)\s+execution_duration_ms=\d+"
    ),
    "submit_output": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"submit_output_start=(\d+)\s+submit_output_end=(\d+)\s+submit_output_duration_ms=\d+"
    ),
}

# Task executor timing pattern (func_entry/func_exit)
FUNC_TIMING_PATTERN = re.compile(
    r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
    r"func_entry=(\d+)\s+func_exit=(\d+)\s+func_duration_ms=\d+"
)


def parse_log_timestamp(line: str) -> Optional[datetime]:
    """Extract timestamp from log line start."""
    match = LOG_TIMESTAMP_PATTERN.match(line)
    if match:
        ts_str = match.group(1)
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
    return None


def epoch_to_datetime(epoch_ms: int, ref_epoch_ms: int, ref_datetime: datetime) -> datetime:
    """Convert internal epoch timestamp (ms) to datetime using a reference point."""
    delta_ms = epoch_ms - ref_epoch_ms
    return ref_datetime + timedelta(milliseconds=delta_ms)


def discover_worker_logs(log_dir: Path) -> list[Path]:
    """Find all spider_worker_*.log files in the directory."""
    worker_logs = list(log_dir.glob("spider_worker_*.log"))
    return sorted(worker_logs)


def discover_task_executor_logs(log_dir: Path) -> list[Path]:
    """Find all task_*.log files (task executor logs) in the directory."""
    task_logs = list(log_dir.glob("task_*.log"))
    return sorted(task_logs)


def parse_task_executor_logs(
    log_dir: Path, tasks: dict[str, TaskTiming]
) -> int:
    """
    Parse task executor logs to get func_entry/func_exit timing.
    Updates the tasks dict in-place with func_entry/func_exit times.
    Returns count of tasks updated.
    """
    task_logs = discover_task_executor_logs(log_dir)
    if not task_logs:
        return 0

    updated = 0
    ref_epoch_ms: Optional[int] = None
    ref_datetime: Optional[datetime] = None

    for log_path in task_logs:
        with open(log_path, "r") as f:
            for line in f:
                if "[TIMING]" not in line:
                    continue

                log_timestamp = parse_log_timestamp(line)
                if not log_timestamp:
                    continue

                match = FUNC_TIMING_PATTERN.search(line)
                if match:
                    task_id, func_name, entry_str, exit_str = match.groups()
                    entry_epoch_ms = int(entry_str)
                    exit_epoch_ms = int(exit_str)

                    if ref_epoch_ms is None:
                        ref_epoch_ms = exit_epoch_ms
                        ref_datetime = log_timestamp

                    if task_id in tasks:
                        task = tasks[task_id]
                        task.func_entry = epoch_to_datetime(
                            entry_epoch_ms, ref_epoch_ms, ref_datetime
                        )
                        task.func_exit = epoch_to_datetime(
                            exit_epoch_ms, ref_epoch_ms, ref_datetime
                        )
                        updated += 1

    return updated


def parse_worker_log(path: Path) -> tuple[str, dict[str, TaskTiming]]:
    """
    Parse a single C++ worker log file for task timings.
    Returns (worker_id, {task_id: TaskTiming})
    """
    filename = path.name
    worker_uuid = filename.replace("spider_worker_", "").replace(".log", "")
    worker_id = worker_uuid[:8]

    tasks: dict[str, TaskTiming] = {}
    ref_epoch_ms: Optional[int] = None
    ref_datetime: Optional[datetime] = None

    with open(path, "r") as f:
        for line in f:
            if "[TIMING]" not in line:
                continue

            log_timestamp = parse_log_timestamp(line)
            if not log_timestamp:
                continue

            # Try each C++ timing pattern
            for phase_name, pattern in CPP_TIMING_PATTERNS.items():
                match = pattern.search(line)
                if not match:
                    continue

                task_id, func_name, start_epoch_str, end_epoch_str = match.groups()
                start_epoch_ms = int(start_epoch_str)
                end_epoch_ms = int(end_epoch_str)

                # Establish reference point
                if ref_epoch_ms is None:
                    ref_epoch_ms = end_epoch_ms
                    ref_datetime = log_timestamp

                phase_start = epoch_to_datetime(start_epoch_ms, ref_epoch_ms, ref_datetime)
                phase_end = epoch_to_datetime(end_epoch_ms, ref_epoch_ms, ref_datetime)

                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id, func_name=func_name)

                task = tasks[task_id]
                task.func_name = func_name

                if phase_name == "fetch_input":
                    task.fetch_start = phase_start
                    task.fetch_end = phase_end
                elif phase_name == "spawn":
                    task.spawn_start = phase_start
                    task.spawn_end = phase_end
                elif phase_name == "execution":
                    task.execution_start = phase_start
                    task.execution_end = phase_end
                elif phase_name == "submit_output":
                    task.submit_start = phase_start
                    task.submit_end = phase_end

                break

            # Try func_entry/func_exit pattern
            match = FUNC_TIMING_PATTERN.search(line)
            if match:
                task_id, func_name, entry_str, exit_str = match.groups()
                entry_epoch_ms = int(entry_str)
                exit_epoch_ms = int(exit_str)

                if ref_epoch_ms is None:
                    ref_epoch_ms = exit_epoch_ms
                    ref_datetime = log_timestamp

                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id, func_name=func_name)

                task = tasks[task_id]
                task.func_entry = epoch_to_datetime(entry_epoch_ms, ref_epoch_ms, ref_datetime)
                task.func_exit = epoch_to_datetime(exit_epoch_ms, ref_epoch_ms, ref_datetime)

    return worker_id, tasks


def get_task_start_time(task: TaskTiming) -> Optional[datetime]:
    """Get the earliest timestamp for a task."""
    times = [t for t in [task.fetch_start, task.spawn_start, task.execution_start] if t]
    return min(times) if times else None


def get_task_end_time(task: TaskTiming) -> Optional[datetime]:
    """Get the latest timestamp for a task."""
    times = [t for t in [task.fetch_end, task.submit_end, task.execution_end] if t]
    return max(times) if times else None


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"


def compute_gap_analysis(
    all_tasks: list[TaskTiming], task_type: str
) -> dict[str, dict]:
    """
    Compute gap analysis statistics for all tasks.
    Returns dict with statistics for each timing gap/phase.
    """
    stats: dict[str, list[float]] = {
        "fetch_duration": [],
        "spawn_duration": [],
        "execution_duration": [],
        "submit_duration": [],
        "func_duration": [],
        "startup_gap": [],  # spawn_end -> func_entry (executor startup)
        "shutdown_gap": [],  # func_exit -> execution_end (cleanup)
    }

    for task in all_tasks:
        # Worker phases
        if task.fetch_start and task.fetch_end:
            stats["fetch_duration"].append(
                (task.fetch_end - task.fetch_start).total_seconds() * 1000
            )
        if task.spawn_start and task.spawn_end:
            stats["spawn_duration"].append(
                (task.spawn_end - task.spawn_start).total_seconds() * 1000
            )
        if task.execution_start and task.execution_end:
            stats["execution_duration"].append(
                (task.execution_end - task.execution_start).total_seconds() * 1000
            )
        if task.submit_start and task.submit_end:
            stats["submit_duration"].append(
                (task.submit_end - task.submit_start).total_seconds() * 1000
            )

        # Function execution
        if task.func_entry and task.func_exit:
            stats["func_duration"].append(
                (task.func_exit - task.func_entry).total_seconds() * 1000
            )

        # Gap analysis
        # Startup gap: from spawn_end to func_entry (interpreter/process startup)
        if task.spawn_end and task.func_entry:
            gap = (task.func_entry - task.spawn_end).total_seconds() * 1000
            if gap >= 0:
                stats["startup_gap"].append(gap)

        # Shutdown gap: from func_exit to execution_end (cleanup)
        if task.func_exit and task.execution_end:
            gap = (task.execution_end - task.func_exit).total_seconds() * 1000
            if gap >= 0:
                stats["shutdown_gap"].append(gap)

    # Compute summary statistics
    results = {}
    for name, values in stats.items():
        if values:
            results[name] = {
                "count": len(values),
                "mean": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
                "total": sum(values),
            }
        else:
            results[name] = {"count": 0, "mean": 0, "min": 0, "max": 0, "total": 0}

    return results


def print_gap_analysis(stats: dict[str, dict], task_type: str) -> None:
    """Print gap analysis statistics to console."""
    print("\n" + "=" * 70)
    print(f"GAP ANALYSIS - {task_type.upper()} TASKS (all values in milliseconds)")
    print("=" * 70)

    sections = [
        (
            "Worker Phases",
            [
                ("Fetch Input", "fetch_duration"),
                ("Spawn Process", "spawn_duration"),
                ("Execution (total)", "execution_duration"),
                ("Submit Output", "submit_duration"),
            ],
        ),
        (
            "Function Execution",
            [
                ("Function Work", "func_duration"),
            ],
        ),
        (
            "Overhead Analysis",
            [
                ("Startup Gap (spawn -> func_entry)", "startup_gap"),
                ("Shutdown Gap (func_exit -> exec_end)", "shutdown_gap"),
            ],
        ),
    ]

    for section_name, items in sections:
        print(f"\n{section_name}:")
        print("-" * 60)
        print(f"{'Phase':<35} {'Mean':>10} {'Min':>10} {'Max':>10}")
        print("-" * 60)
        for label, key in items:
            s = stats[key]
            if s["count"] > 0:
                print(f"{label:<35} {s['mean']:>10.2f} {s['min']:>10.2f} {s['max']:>10.2f}")
            else:
                print(f"{label:<35} {'N/A':>10} {'N/A':>10} {'N/A':>10}")


def render_timeline(
    worker_timelines: list[WorkerTimeline],
    task_type: str,
    output_path: Optional[Path] = None,
    figsize: tuple[int, int] = (16, 12),
) -> None:
    """
    Render timeline showing task execution phases.
    Two rows per task: upper for executor (startup/func/shutdown), lower for worker phases.
    """
    if not worker_timelines:
        print("No timeline data available.")
        return

    # Find global time bounds
    all_times: list[datetime] = []
    for wt in worker_timelines:
        for task in wt.tasks:
            for t in [
                task.fetch_start,
                task.fetch_end,
                task.spawn_start,
                task.spawn_end,
                task.execution_start,
                task.execution_end,
                task.submit_start,
                task.submit_end,
                task.func_entry,
                task.func_exit,
            ]:
                if t:
                    all_times.append(t)

    if not all_times:
        print("No tasks found with timing data.")
        return

    tl_start = min(all_times)
    tl_end = max(all_times)
    total_seconds = (tl_end - tl_start).total_seconds()
    if total_seconds <= 0:
        total_seconds = 1.0

    fig, ax = plt.subplots(figsize=figsize)

    # Color scheme
    colors = {
        "fetch": "#2ecc71",  # Green
        "spawn": "#9b59b6",  # Purple
        "execution": "#3498db",  # Blue (background)
        "submit": "#e74c3c",  # Red
        "startup_gap": "#ff6b6b",  # Coral - executor startup overhead
        "func_work": "#22c55e",  # Bright green - actual function work
        "shutdown_gap": "#fbbf24",  # Amber - cleanup overhead
    }

    total_tasks = sum(len(wt.tasks) for wt in worker_timelines)
    bar_height = 0.35
    row_spacing = 0.1
    task_spacing = 0.3
    current_row = 0
    worker_boundaries: list[tuple[float, float, str]] = []

    for wt in worker_timelines:
        if not wt.tasks:
            continue

        worker_start_row = current_row

        for task in wt.tasks:
            # Bottom row: Worker phases
            worker_row = current_row

            # 1. Fetch
            if task.fetch_start and task.fetch_end:
                start_offset = (task.fetch_start - tl_start).total_seconds()
                width = (task.fetch_end - task.fetch_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, worker_row - bar_height / 2),
                    width,
                    bar_height,
                    facecolor=colors["fetch"],
                    edgecolor="none",
                    alpha=0.9,
                )
                ax.add_patch(rect)

            # 2. Spawn
            if task.spawn_start and task.spawn_end:
                start_offset = (task.spawn_start - tl_start).total_seconds()
                width = (task.spawn_end - task.spawn_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, worker_row - bar_height / 2),
                    width,
                    bar_height,
                    facecolor=colors["spawn"],
                    edgecolor="none",
                    alpha=0.9,
                )
                ax.add_patch(rect)

            # 3. Execution (background)
            if task.execution_start and task.execution_end:
                start_offset = (task.execution_start - tl_start).total_seconds()
                width = (task.execution_end - task.execution_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, worker_row - bar_height / 2),
                    width,
                    bar_height,
                    facecolor=colors["execution"],
                    edgecolor="none",
                    alpha=0.6,
                )
                ax.add_patch(rect)

            # 4. Submit
            if task.submit_start and task.submit_end:
                start_offset = (task.submit_start - tl_start).total_seconds()
                width = (task.submit_end - task.submit_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, worker_row - bar_height / 2),
                    width,
                    bar_height,
                    facecolor=colors["submit"],
                    edgecolor="none",
                    alpha=0.9,
                )
                ax.add_patch(rect)

            # Top row: Executor phases
            exec_row = current_row + bar_height + row_spacing

            # Startup gap (spawn_end -> func_entry)
            if task.spawn_end and task.func_entry:
                start_offset = (task.spawn_end - tl_start).total_seconds()
                width = (task.func_entry - task.spawn_end).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, exec_row - bar_height / 2),
                        width,
                        bar_height,
                        facecolor=colors["startup_gap"],
                        edgecolor="none",
                        alpha=0.9,
                    )
                    ax.add_patch(rect)

            # Function work
            if task.func_entry and task.func_exit:
                start_offset = (task.func_entry - tl_start).total_seconds()
                width = (task.func_exit - task.func_entry).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, exec_row - bar_height / 2),
                    width,
                    bar_height,
                    facecolor=colors["func_work"],
                    edgecolor="none",
                    alpha=0.9,
                )
                ax.add_patch(rect)

            # Shutdown gap (func_exit -> execution_end)
            if task.func_exit and task.execution_end:
                start_offset = (task.func_exit - tl_start).total_seconds()
                width = (task.execution_end - task.func_exit).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, exec_row - bar_height / 2),
                        width,
                        bar_height,
                        facecolor=colors["shutdown_gap"],
                        edgecolor="none",
                        alpha=0.9,
                    )
                    ax.add_patch(rect)

            # Move to next task
            current_row += 2 * bar_height + row_spacing + task_spacing

        worker_boundaries.append((worker_start_row, current_row - task_spacing, wt.worker_id))

    # Draw worker separators
    for i, (start_row, end_row, worker_id) in enumerate(worker_boundaries):
        if i > 0:
            y = start_row - 0.5
            ax.axhline(y=y, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    ax.set_xlim(-0.5, total_seconds + 0.5)
    ax.set_ylim(-1, current_row)

    # Y-axis labels
    y_ticks = []
    y_labels = []
    for start_row, end_row, worker_id in worker_boundaries:
        mid_row = (start_row + end_row) / 2
        y_ticks.append(mid_row)
        y_labels.append(worker_id)

    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels, fontsize=8)

    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Worker")

    # Legend
    legend_handles = [
        mpatches.Patch(color=colors["fetch"], alpha=0.9, label="Fetch Input"),
        mpatches.Patch(color=colors["spawn"], alpha=0.9, label="Spawn"),
        mpatches.Patch(color=colors["execution"], alpha=0.6, label="Execution"),
        mpatches.Patch(color=colors["submit"], alpha=0.9, label="Submit"),
        mpatches.Patch(color=colors["startup_gap"], alpha=0.9, label="Startup Gap"),
        mpatches.Patch(color=colors["func_work"], alpha=0.9, label="Function Work"),
        mpatches.Patch(color=colors["shutdown_gap"], alpha=0.9, label="Shutdown Gap"),
    ]
    ax.legend(handles=legend_handles, loc="upper right", fontsize=8)

    num_workers = len(worker_timelines)
    title = f"Spider Executor Overhead Timeline - {task_type.upper()} Tasks"
    subtitle = f"{num_workers} workers, {total_tasks} tasks, Duration: {format_duration(total_seconds)}"
    ax.set_title(f"{title}\n{subtitle}", fontsize=12)

    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Timeline saved to {output_path}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Visualize Spider executor overhead benchmark results.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/logs/                  Visualize all tasks
  %(prog)s /path/to/logs/ -o timeline.png  Save to file
  %(prog)s /path/to/logs/ --stats-only     Only print statistics
""",
    )
    parser.add_argument("log_dir", type=Path, help="Directory containing log files")
    parser.add_argument("--output", "-o", type=Path, help="Output file (PNG, PDF, etc.)")
    parser.add_argument(
        "--figsize",
        type=str,
        default="16,12",
        help="Figure size as width,height in inches (default: 16,12)",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Only print gap analysis statistics, no visualization",
    )

    args = parser.parse_args()

    if not args.log_dir.is_dir():
        print(f"Error: {args.log_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    try:
        figsize = tuple(map(int, args.figsize.split(",")))
        if len(figsize) != 2:
            raise ValueError()
    except ValueError:
        print(f"Error: Invalid figsize format: {args.figsize}", file=sys.stderr)
        sys.exit(1)

    # Discover worker logs
    worker_logs = discover_worker_logs(args.log_dir)
    if not worker_logs:
        print("No spider_worker_*.log files found.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(worker_logs)} worker log files.")

    # Parse all worker logs
    all_tasks: dict[str, TaskTiming] = {}
    worker_timelines: list[WorkerTimeline] = []

    for log_path in worker_logs:
        worker_id, tasks = parse_worker_log(log_path)

        if not tasks:
            continue

        all_tasks.update(tasks)

        sorted_tasks = sorted(
            tasks.values(),
            key=lambda t: get_task_start_time(t) or datetime.max,
        )

        worker_timelines.append(WorkerTimeline(worker_id=worker_id, tasks=sorted_tasks))

    if not worker_timelines:
        print("No tasks found.", file=sys.stderr)
        sys.exit(1)

    # Parse task executor logs to get func_entry/func_exit timing
    updated_count = parse_task_executor_logs(args.log_dir, all_tasks)
    if updated_count > 0:
        print(f"Updated {updated_count} tasks with executor timing from task logs.")

    # Sort workers by first task start time
    worker_timelines.sort(
        key=lambda wt: get_task_start_time(wt.tasks[0]) if wt.tasks else datetime.max
    )

    total_tasks = sum(len(wt.tasks) for wt in worker_timelines)
    print(f"Found {total_tasks} tasks across {len(worker_timelines)} workers.")

    # Determine task type from function names
    all_task_list = [task for wt in worker_timelines for task in wt.tasks]

    cpp_tasks = [t for t in all_task_list if "cpp_sleep" in t.func_name]
    py_tasks = [t for t in all_task_list if "py_sleep" in t.func_name]

    if cpp_tasks:
        stats = compute_gap_analysis(cpp_tasks, "cpp")
        print_gap_analysis(stats, "cpp")

    if py_tasks:
        stats = compute_gap_analysis(py_tasks, "python")
        print_gap_analysis(stats, "python")

    if not cpp_tasks and not py_tasks:
        # Generic analysis
        stats = compute_gap_analysis(all_task_list, "all")
        print_gap_analysis(stats, "all")

    if not args.stats_only:
        output_base = args.output or args.log_dir / "timeline.png"

        # Create separate visualizations for C++ and Python if both present
        # Determine output naming: if output has a stem, use it as prefix
        output_stem = output_base.stem  # e.g., "executor_overhead" from "executor_overhead.png"
        output_dir = output_base.parent

        if cpp_tasks and py_tasks:
            cpp_timelines = [
                WorkerTimeline(
                    worker_id=wt.worker_id,
                    tasks=[t for t in wt.tasks if "cpp_sleep" in t.func_name],
                )
                for wt in worker_timelines
            ]
            cpp_timelines = [wt for wt in cpp_timelines if wt.tasks]

            py_timelines = [
                WorkerTimeline(
                    worker_id=wt.worker_id,
                    tasks=[t for t in wt.tasks if "py_sleep" in t.func_name],
                )
                for wt in worker_timelines
            ]
            py_timelines = [wt for wt in py_timelines if wt.tasks]

            if cpp_timelines:
                cpp_output = output_dir / f"{output_stem}_cpp.png"
                render_timeline(cpp_timelines, "cpp", cpp_output, figsize)

            if py_timelines:
                py_output = output_dir / f"{output_stem}_python.png"
                render_timeline(py_timelines, "python", py_output, figsize)
        else:
            task_type = "cpp" if cpp_tasks else ("python" if py_tasks else "all")
            # Single type: use the exact output path
            render_timeline(worker_timelines, task_type, output_base, figsize)


if __name__ == "__main__":
    main()
