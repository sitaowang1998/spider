#!/usr/bin/env python3
"""
Process manager for executor overhead benchmark.

Spawns scheduler and worker processes with proper cleanup on exit.
"""

import argparse
import atexit
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

# Global list of spawned processes for cleanup
_processes: list[subprocess.Popen] = []


def _cleanup():
    """Kill all spawned processes."""
    for proc in _processes:
        if proc.poll() is None:  # Process is still running
            try:
                proc.kill()
                proc.wait(timeout=5)
            except Exception:
                pass
    _processes.clear()


def _signal_handler(signum, frame):
    """Handle signals by cleaning up and exiting."""
    _cleanup()
    sys.exit(128 + signum)


def get_free_port() -> int:
    """Get a free port using the project's get_free_port.py script."""
    root_dir = Path(__file__).parent.parent.parent.parent
    script = root_dir / "tools" / "scripts" / "get_free_port.py"
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    return int(result.stdout.strip())


def spawn_scheduler(build_dir: Path, port: int) -> subprocess.Popen:
    """Spawn the scheduler process."""
    scheduler_bin = build_dir / "spider" / "src" / "spider" / "spider_scheduler"
    proc = subprocess.Popen(
        [str(scheduler_bin), "--host", "127.0.0.1", "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _processes.append(proc)
    return proc


def spawn_workers(
    build_dir: Path, task_lib: Path, num_workers: int, log_dir: Path
) -> list[subprocess.Popen]:
    """Spawn worker processes."""
    worker_bin = build_dir / "spider" / "src" / "spider" / "spider_worker"
    env = os.environ.copy()
    env["SPIDER_LOG_DIR"] = str(log_dir)

    # Set up Python environment for the task executor
    root_dir = Path(__file__).parent.parent.parent.parent
    spider_py_dir = root_dir / "python" / "spider-py"
    venv_bin = spider_py_dir / ".venv" / "bin"

    # Add venv bin to PATH so worker finds the correct Python with dependencies
    if venv_bin.exists():
        env["PATH"] = f"{venv_bin}:{env.get('PATH', '')}"

    # Add spider_py source and benchmark tasks to PYTHONPATH
    pythonpath_parts = [
        str(spider_py_dir / "src"),
        str(root_dir / "benchmarks" / "executor_overhead" / "python"),
    ]
    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        pythonpath_parts.append(existing_pythonpath)
    env["PYTHONPATH"] = ":".join(pythonpath_parts)

    workers = []
    for _ in range(num_workers):
        proc = subprocess.Popen(
            [str(worker_bin), "--host", "127.0.0.1", "--libs", str(task_lib)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        _processes.append(proc)
        workers.append(proc)
    return workers


def main():
    parser = argparse.ArgumentParser(description="Spawn benchmark processes")
    parser.add_argument("--build-dir", required=True, help="Build directory path")
    parser.add_argument("--log-dir", required=True, help="Log directory path")
    parser.add_argument("--task-lib", required=True, help="Task library path")
    parser.add_argument(
        "--num-workers", type=int, default=16, help="Number of workers"
    )
    parser.add_argument(
        "--wait", action="store_true", help="Wait for interrupt signal"
    )
    args = parser.parse_args()

    build_dir = Path(args.build_dir)
    log_dir = Path(args.log_dir)
    task_lib = Path(args.task_lib)

    # Register cleanup handlers
    atexit.register(_cleanup)
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # Get a free port for the scheduler
    port = get_free_port()
    print(f"SCHEDULER_PORT={port}")

    # Spawn scheduler
    scheduler = spawn_scheduler(build_dir, port)
    time.sleep(2)

    if scheduler.poll() is not None:
        print(f"ERROR: Scheduler exited with code {scheduler.returncode}", file=sys.stderr)
        sys.exit(1)

    # Spawn workers
    workers = spawn_workers(build_dir, task_lib, args.num_workers, log_dir)
    time.sleep(5)

    # Check all workers are running
    failed = sum(1 for w in workers if w.poll() is not None)
    if failed > 0:
        print(f"ERROR: {failed} workers failed to start", file=sys.stderr)
        sys.exit(1)

    print(f"Started scheduler (PID {scheduler.pid}) and {len(workers)} workers")

    if args.wait:
        # Wait for signal to terminate
        print("Waiting for termination signal...")
        try:
            signal.pause()
        except KeyboardInterrupt:
            pass

    # Cleanup happens via atexit


if __name__ == "__main__":
    main()
