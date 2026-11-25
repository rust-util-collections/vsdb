"""Summarize complete, interleaved publication and foreground latency pairs."""
import csv
import sys
from collections import defaultdict
from statistics import median

cases=defaultdict(lambda:defaultdict(dict))
diag=[]
with open(sys.argv[1]) as fp:
    for r in csv.DictReader(fp):
        if r["case"]=="submissions":
            diag.append(r)
            continue
        assert r["ops"]=="10000" and r["round"] in {"1","2","3","4","5"}
        pair=cases[r["case"]][(r["round"],r["pair"])]
        assert r["arm"] not in pair
        pair[r["arm"]]=r
default_dag = len(sys.argv)==3 and sys.argv[2]=="--default-dag"
expected = ({"default-child","default-v4-child"} if default_dag else
            {"child","dependent","precomputed","v4-child","v4-individual","v4-batch","placement","v4-hot","v4-split"})
assert set(cases)==expected
writer=csv.writer(sys.stdout,lineterminator="\n")
writer.writerow("case,control_p99_us,compared_p99_us,paired_gain_pct,min_gain_pct,max_gain_pct,max_aa_pct,favorable_pairs,qualified_pairs,control_median_ops_s,compared_median_ops_s".split(","))
def stable(r):
    if not r["bulk_rows"]:return True
    return (all(int(r[k])<=100 for k in ("pending_half","pending_end"))
            and all(int(r[k])<=2 for k in ("bulk_half","bulk_end")))
for name, trials in sorted(cases.items()):
    assert set(trials)=={(str(i),p) for i in range(1,6) for p in ("AA","AB")}
    aa=[];ab=[];before=[];after=[];qualified=0;ta=[];tb=[]
    for i in range(1,6):
        block=[]
        for kind in ("AA","AB"):
            pair=trials[str(i),kind]
            assert set(pair)=={"control","comparison"}
            a,b=pair["control"],pair["comparison"]
            assert a["seed"]==b["seed"]==str(20261126+i) and a["order"]!=b["order"]
            if default_dag:
                assert all(int(r["prefix_advance"]) == 16*int(r["ops"]) for r in (a,b))
            if kind=="AA":
                assert (a["binary"],a["algorithm"])==(b["binary"],b["algorithm"])
            x,y=(int(r["p99_ns"])/1000 for r in (a,b))
            gain=100*(1-y/x)
            if kind=="AA":aa.append(abs(gain))
            else:
                ab.append(gain);before.append(x);after.append(y)
                ta.append(10000e9/int(a["elapsed_ns"]));tb.append(10000e9/int(b["elapsed_ns"]))
            block.extend((a,b))
        qualified+=all(stable(r) for r in block)
    writer.writerow([name,*[round(v,3) for v in (median(before),median(after),median(ab),min(ab),max(ab),max(aa))],
                     sum(g>0 for g in ab),qualified,round(median(ta)),round(median(tb))])
counts={(r["binary"],r["algorithm"]):int(r["submissions"])/int(r["ops"]) for r in diag}
assert all(r["counted"]=="true" for r in diag)
if default_dag:
    assert len(diag)==2 and counts=={("diag-previous","dag16"):2,("diag-pinned","dag16"):1}
    sys.exit(0)
assert len(diag)==len(counts)==5
assert counts["diag-previous","dag"]==2 and counts["diag-pinned","dag"]==1
assert counts["diag-pinned","overlay"]==counts["diag-pinned","batch"]<counts["diag-pinned","individual"]
