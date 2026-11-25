"""Build the same standalone caller against a supplied VSDB checkout."""
import argparse
from pathlib import Path
import json
import shutil
import subprocess

p=argparse.ArgumentParser(description=__doc__)
p.add_argument("checkout", type=Path)
p.add_argument("workdir",type=Path)
p.add_argument("output",type=Path)
p.add_argument("--v5",action="store_true")
p.add_argument("--allocations",action="store_true")
a=p.parse_args()
a.workdir.mkdir(parents=True,exist_ok=True)
a.output.parent.mkdir(parents=True,exist_ok=True)
here=Path(__file__).resolve().parent
for source in ("profile.rs","alloc.rs"):
    shutil.copyfile(here/source,a.workdir/source)
(a.workdir/"Cargo.toml").write_text(f"""[package]
name = "shard-adapter-profile"
version = "0.0.0"
edition = "2024"
[workspace]
[dependencies]
vsdb_core = {{path={json.dumps(str(a.checkout.resolve()/'core'))}}}
[features]
v5 = []
allocations = []
[[bin]]
name = "profile"
path = "profile.rs"
""")
features=",".join(key for key in ("v5","allocations") if getattr(a,key))
cmd=["cargo","+stable","build","--manifest-path",str(a.workdir/"Cargo.toml"),"--release","--message-format=json"]
if features:cmd +=["--features",features]
# The standalone build may inherit a scratch target directory from its caller.
result=subprocess.run(cmd,stdout=subprocess.PIPE,text=True,check=True)
artifacts=[json.loads(line) for line in result.stdout.splitlines()]
executable=next(j['executable'] for j in artifacts if j.get('executable') and j['target']['name']=='profile')
shutil.copy2(executable,a.output)
