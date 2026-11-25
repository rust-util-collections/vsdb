"""Check partial, mislabeled or mismatched evidence cannot qualify a cache regime."""
import csv
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
FIELDS = ("round", "pair", "arm", "order", "seed", "binary", "regime",
          "samples", "p50_ns", "p99_ns", "digest", "nearest", "blocks",
          "logical_bytes", "storage_bytes", "io_positive")


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        for regime in ("warm", "device", "singleton", "secondary", "device-width8", "landed", "distance-cache", "width8-repeat"):
            if regime == "secondary":
                pairs = [("v4", ("v4", "1")), ("width8", ("1", "8"))]
            elif regime == "width8-repeat":
                pairs = [("AA", ("shipped","shipped")), ("AB", ("shipped","8"))]
            elif regime == "distance-cache":
                pairs = [("AA", ("shipped","shipped")), ("AB", ("shipped","cached"))]
            elif regime == "landed":
                pairs = [("AA", ("1","1")), ("AB", ("1","shipped")),
                         ("v4AA", ("v4","v4")), ("upgrade", ("v4","shipped"))]
            elif regime == "device-width8":
                pairs = [("diagnostic-width8", ("8",))]
            else:
                pairs = [("AA", ("1", "1")), ("AB", ("1", "32"))]
            rows = []
            for round_no in range(1, 6):
                for pair, binaries in pairs:
                    roles = ("observation",) if len(binaries) == 1 else ("reference", "comparison")
                    for order, (role, binary) in enumerate(zip(roles, binaries), 1):
                        samples = 100_000 if regime == "singleton" else 10_000
                        if regime=="width8-repeat":
                            samples = 30_000
                        seed = 261126 + round_no + (100_000 if regime=="width8-repeat" else 0)
                        disk_hits = 9_500 if regime.startswith("device") else 0
                        rows.append(dict(round=round_no, pair=pair, arm=role, order=order,
                                         seed=seed, binary=binary, regime=regime,
                                         samples=samples, p50_ns=100, p99_ns=110, digest="1234",
                                         nearest=samples, blocks=samples, logical_bytes=samples,
                                         storage_bytes=4096 * disk_hits, io_positive=disk_hits))
            self.save(regime, rows)

    def save(self, regime, rows):
        with (self.folder / (regime + ".csv")).open("w") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)

    def mutate(self, regime, transform):
        with (self.folder / (regime + ".csv")).open() as f:
            rows = list(csv.DictReader(f))
        transform(rows)
        self.save(regime, rows)

    def summarize(self):
        return subprocess.run([sys.executable, "-B", str(HERE / "summarize.py"), self.folder],
                              capture_output=True, text=True)

    def test_complete_matrix(self):
        result = self.summarize()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_comparison_rejected(self):
        self.mutate("warm", lambda rows: rows.pop())
        self.assertNotEqual(self.summarize().returncode, 0)

    def test_changed_result_on_same_graph_rejected(self):
        self.mutate("device", lambda rows: rows[-1].update(digest="different"))
        self.assertNotEqual(self.summarize().returncode, 0)

    def test_disk_coverage_is_per_process(self):
        self.mutate("device", lambda rows: rows[0].update(io_positive=9499))
        self.assertNotEqual(self.summarize().returncode, 0)

    def test_timing_denominator_is_completed_queries(self):
        self.mutate("device", lambda rows: rows[0].update(samples=9999))
        self.assertNotEqual(self.summarize().returncode, 0)
