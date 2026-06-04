import importlib.util
import pathlib
import sys
import tempfile
import unittest


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "generate_aws_scalability_report.py"


def load_report_module():
    spec = importlib.util.spec_from_file_location("generate_aws_scalability_report", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class GenerateAwsScalabilityReportTest(unittest.TestCase):
    def test_worker_poll_zero_scheduler_latency_stays_client_overhead(self):
        report = load_report_module()
        rows = report.request_component_rows(
            report.WORKER_POLL_OPERATION,
            [
                report.RequestMetric(
                    nodes=4,
                    protocol="Grpc",
                    workload="flat",
                    category="blocking",
                    operation=report.WORKER_POLL_OPERATION,
                    count=40_000,
                    client_avg_us=263,
                    server_avg_us=0,
                )
            ],
            [],
        )

        self.assertEqual(1, len(rows))
        self.assertEqual(0.0, rows[0]["components"]["scheduler response"])
        self.assertEqual(0.263, rows[0]["components"]["client overhead"])

    def test_setup_paragraph_describes_available_protocols(self):
        report = load_report_module()

        grpc_paragraph = report.setup_paragraph([run_fixture(report, protocol="Grpc")])
        self.assertIn("Results cover Grpc", grpc_paragraph)
        self.assertNotIn("REST", grpc_paragraph)

        both_paragraph = report.setup_paragraph(
            [
                run_fixture(report, protocol="Grpc"),
                run_fixture(report, protocol="Rest"),
            ]
        )
        self.assertIn("Results compare Grpc, Rest", both_paragraph)

    def test_report_does_not_embed_chart_images(self):
        report = load_report_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = pathlib.Path(temp_dir) / "aws_scalability_report.md"
            report.write_report(
                output_path,
                [run_fixture(report, protocol="Grpc")],
                [
                    report.RequestMetric(
                        nodes=1,
                        protocol="Grpc",
                        workload="flat",
                        category="non_blocking",
                        operation="get_session",
                        count=1,
                        client_avg_us=1000,
                        server_avg_us=500,
                    )
                ],
                [],
            )

            report_text = output_path.read_text(encoding="utf-8")

        self.assertIn("| Nodes | Protocol | Jobs | Tasks |", report_text)
        self.assertNotIn("![", report_text)
        self.assertNotIn(".png", report_text)


def run_fixture(report, protocol: str):
    return report.Run(
        nodes=1,
        protocol=protocol,
        workload="flat",
        job_count=16,
        task_count=1000,
        task_sleep_ms=3,
        flat_percent=100,
        submitter_count=16,
        worker_count=16,
        controller_wall_time_us=1_000_000,
        job_avg_us=1_000,
        job_latency={
            "count": 1,
            "avg_us": 1_000,
            "p50_us": 1_000,
            "p90_us": 1_000,
            "p99_us": 1_000,
            "max_us": 1_000,
        },
        server_job_execution_latency={
            "count": 1,
            "avg_us": 1_000,
            "p50_us": 1_000,
            "p90_us": 1_000,
            "p99_us": 1_000,
            "max_us": 1_000,
        },
        request_count=1,
        client_request_avg_us=1_000,
        server_request_avg_us=500,
        worker_activity_count=1,
        worker_idle_avg_pct=0.0,
    )


if __name__ == "__main__":
    unittest.main()
