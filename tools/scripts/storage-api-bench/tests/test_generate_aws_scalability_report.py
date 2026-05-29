import importlib.util
import pathlib
import sys
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


if __name__ == "__main__":
    unittest.main()
