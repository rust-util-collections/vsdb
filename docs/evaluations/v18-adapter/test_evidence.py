import gzip
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from summarize import load_ok, qualify, throughput
from upgrade import digest, summarize

class ThroughputEvidence(unittest.TestCase):
    def test_bulk_gains_cannot_replace_foreground_demand(self):
        def profile(small,bulk):
            return throughput(dict(small=small,bulk=bulk,wall_ns=10**9,
                                   logical_bytes=small*136+bulk*32*520))
        control=profile(100,200)
        candidate=profile(50,300)
        self.assertGreater(candidate["throughput_mib_s"],control["throughput_mib_s"])
        self.assertGreater(candidate["successful_batches_s"],control["successful_batches_s"])
        self.assertEqual(candidate["successful_small_puts_s"],50)
        self.assertEqual(candidate["matched_mix_mib_s"],control["matched_mix_mib_s"]/2)

    def test_late_backlog_cannot_qualify_a_favorable_latency(self):
        good = dict(load="paced", small=10000, bulk=20000, small_half_debt=0,
                    small_end_debt=0, bulk_half_debt=16, bulk_end_debt=16)
        late = dict(good, small_end_debt=101)
        pairs = [[good]*4 for _ in range(4)] + [[good, good, late, good]]
        count = sum(all(load_ok(row) for row in pair) for pair in pairs)
        self.assertEqual(count, 4)
        gains = [21] * 5
        self.assertEqual(qualify(gains,1.0,count,5,[90000,80000]),
                         ("","offered-load-unqualified",True))
        self.assertEqual(qualify(gains,1.0,count,5,[90000,2**20+1]),
                         ("","offered-load-unqualified",False))
        self.assertEqual(qualify(gains,1.0,5,5,[90000,80000]),
                         (21,"guard-met",True))
        self.assertEqual(qualify(gains,1.0,5,5,[90000,80000],False)[1],
                         "separate-reference-required")

    def test_upgrade_noise_uses_the_frozen_v4_executable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            original=root/"original.gz"
            raw=root/"upgrade.gz"
            def write(path, rows):
                with gzip.open(path,"wt") as out:
                    for row in rows:
                        out.write(json.dumps(row)+"\n")
            write(original,[{"sha256":{"v4":"v4-original"}}])
            m=dict(suite="selected-upgrade",original_packet_sha256=digest(original),sha256={
                "reference":"v4-original","comparison":"native"},seed_base=0,selected_buffer_mib=16,
                offers={"hot":1000,"all":1000},ops=10000)
            args=SimpleNamespace(raw=raw,original=original,summary=root/"summary.csv")
            row=dict(round=1,placement="hot",load="saturation",pair="AA",arm="comparison",binary_sha256="native")
            write(raw,[m,row])
            with self.assertRaisesRegex(ValueError,"wrong executable"):
                summarize(args)
            write(raw,[dict(m,sha256={"reference":"another-v4","comparison":"native"})])
            with self.assertRaisesRegex(ValueError,"v4 reference changed"):
                summarize(args)

if __name__=="__main__":
    unittest.main()
