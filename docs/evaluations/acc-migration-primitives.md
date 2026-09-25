# ACC consumer migration: VSDB primitive validation

Date: 2026-09-25 UTC. Implementation units: `08ec207` (synchronous batches),
`bb1104f` (borrowed views/readers), `282f642` (fault/lifetime coverage).

## Scope

The consumer needs ordinary durable batches and coherent multi-query reads.
VSDB supplies these at the raw collection boundary, and typed ordinary batches
forward the synchronous commit option. The consumer retains its byte/JSON
encoding, writer ownership and service-specific error handling. No persisted
VSDB format, shard routing, default fsync policy, mutable alias contract or
namespace close model changed. No new `unsafe` was introduced.

The ACC-side comparison did not justify collapsing its three engines during
migration. Two rounds with the existing `parking_lot` writer discipline,
three concurrent streams of 300 synchronous commands (4 KiB intake/score,
64 KiB results) measured median command latency around 1.6–1.9 ms with three
engines and 4.1–4.4 ms with one serialized engine. This is a local synthetic
comparison, not a deployment capacity result or a new admission policy.

## Checks

- Workspace Debug and Release integration/unit suites: **695 passed, 5 ignored
  per profile** using MMDB 4.3.1. Subsequent read-view corruption coverage passed
  in both profiles without changing implementation code.
- Workspace formatting, Clippy, test-target checks and benchmark-target checks
  passed. Core doctests include compile-fail cases for mutable access through
  a reader, a view outliving its map, and an iterator outliving its view.
- Reader tests retain old values across concurrent atomic replacements,
  clears and flushes, preserve range bounds/prefix isolation, and verify that
  a live reader prevents namespace close.
- Corrupted-SST point and forward/reverse range reads panic with diagnostics;
  they do not silently turn a failed scan into end-of-stream.
- Synchronous batches cover dropped/wiped/empty batches, read-only rejection,
  closed-engine errors and process termination after acknowledgement.

## Actual fsync pause and failure injection

A Linux `LD_PRELOAD` interposer below blocks one real WAL fsync after the
probe arms it. A second thread queries the same collection while that syscall
is paused. Results were identical on the consumer's pinned MMDB **4.3.0** and
freshly resolved **4.3.5** (the latter is not a full-workspace validation):

| Write path | Visible while fsync is paused | Completion |
|---|---|---|
| `commit_sync()` | Old value; paired new key absent | Success after release |
| `commit(); try_sync_wal()` | New value and paired key already visible | Success after release |
| `commit_sync()` with injected `EIO` | Old value; paired new key absent | Error, no success acknowledgement |

The failure may still leave WAL bytes that recovery can replay. Batch API
comments now distinguish preflight rejection from ambiguous persistence
failure and do not recommend blind retry. This experiment proves ordering
and error propagation, not physical power-loss behavior.

The SAIR side also tests that a fatal read/command fault invalidates its store
or lane and that staged command writes are discarded on panic. It owns that
policy; VSDB does not impose a global service restart mechanism.

## Reproduce the syscall experiment

Run in this checkout on Linux with a dynamically linked Rust toolchain, Cargo,
`cc`, and `awk`. The probe only creates fresh temporary data. Pinning MMDB here
makes this variant reproducible; the 4.3.0 run used the consumer's existing
lockfile because a fresh exact resolution of that version was rejected as yanked.

```bash
probe_dir=$(mktemp -d /tmp/vsdb-fsync-order.XXXXXX)
awk '/^```c$/ {copy=1; next} copy && /^```$/ {exit} copy' \
  docs/evaluations/acc-migration-primitives.md > "$probe_dir/gate.c"
awk '/^```rust$/ {copy=1; next} copy && /^```$/ {exit} copy' \
  docs/evaluations/acc-migration-primitives.md > "$probe_dir/main.rs"
cc -shared -fPIC -o "$probe_dir/gate.so" "$probe_dir/gate.c" -ldl
cat > "$probe_dir/Cargo.toml" <<EOF
[package]
name = "vsdb-fsync-order-probe"
version = "0.0.0"
edition = "2024"
publish = false
[dependencies]
vsdb_core = { path = "$PWD/core" }
mmdb = "=4.3.5"
[[bin]]
name = "fsync-order"
path = "main.rs"
EOF
cargo build --manifest-path "$probe_dir/Cargo.toml"
LD_PRELOAD="$probe_dir/gate.so" VSDB_FSYNC_GATE="$probe_dir/sync" \
  "$probe_dir/target/debug/fsync-order"
LD_PRELOAD="$probe_dir/gate.so" VSDB_FSYNC_GATE="$probe_dir/async" \
  "$probe_dir/target/debug/fsync-order" async
LD_PRELOAD="$probe_dir/gate.so" VSDB_FSYNC_GATE="$probe_dir/fail" VSDB_FSYNC_FAIL=1 \
  "$probe_dir/target/debug/fsync-order"
```

### Interposer source

```c
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
int fsync(int fd) {
    int (*real_fsync)(int) = dlsym(RTLD_NEXT, "fsync");
    const char *root = getenv("VSDB_FSYNC_GATE");
    if (root) {
        char link[64], path[4096], arm[4096], entered[4096], release[4096];
        snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
        ssize_t n = readlink(link, path, sizeof(path)-1);
        if (n > 0) {
            path[n] = 0;
            snprintf(arm,sizeof(arm),"%s/arm",root);
            if (strstr(path,".wal") && access(arm,F_OK)==0) {
                snprintf(entered,sizeof(entered),"%s/entered",root);
                int marker=open(entered,O_CREAT|O_WRONLY,0600);
                if (marker>=0) close(marker);
                snprintf(release,sizeof(release),"%s/release",root);
                struct timespec pause={0,1000000};
                int tries=0;
                while (access(release,F_OK)!=0 && tries++ < 10000) nanosleep(&pause,NULL);
                unlink(arm);
                if (tries>=10000 || getenv("VSDB_FSYNC_FAIL")) {errno=EIO; return -1;}
            }
        }
    }
    return real_fsync(fd);
}
```

### Probe source

```rust
use std::{fs,thread,time::{Duration,Instant},path::PathBuf};
use vsdb_core::{MapxRaw,Namespace,NamespaceOpts,VsdbOptions,vsdb_configure};
fn main() {
    let root=PathBuf::from(std::env::var_os("VSDB_FSYNC_GATE").unwrap());
    fs::create_dir(&root).unwrap();
    vsdb_configure(VsdbOptions::new(root.join("store"))).unwrap();
    let ns=Namespace::create_with(NamespaceOpts{shards:1,..Default::default()}).unwrap();
    let mut map=MapxRaw::new_in(&ns);
    let mut batch=map.batch(); batch.insert(b"key",b"old"); batch.commit_sync().unwrap();
    let reader=map.reader();
    fs::write(root.join("arm"),b"1").unwrap();
    let async_mode=std::env::args().nth(1).as_deref()==Some("async");
    let writer=thread::spawn(move || {
        let mut batch=map.batch(); batch.insert(b"key",b"new");
        batch.insert(b"paired",b"new");
        let result=if async_mode {batch.commit().and_then(|()|map.try_sync_wal())} else {batch.commit_sync()};
        (map,result)
    });
    let deadline=Instant::now()+Duration::from_secs(5);
    while !root.join("entered").exists() {
        assert!(Instant::now()<deadline,"fsync gate not reached");
        thread::sleep(Duration::from_millis(1));
    }
    let seen=reader.get(b"key").unwrap();
    assert_eq!(seen,if async_mode{b"new"}else{b"old"});
    assert_eq!(reader.get(b"paired").is_some(),async_mode);
    fs::write(root.join("release"),b"1").unwrap();
    let (_map,result)=writer.join().unwrap();
    let failure=std::env::var_os("VSDB_FSYNC_FAIL").is_some();
    assert_eq!(result.is_err(),failure);
    if !failure {assert_eq!(reader.get(b"paired").unwrap(),b"new");}
    println!("PASS async={async_mode} fault={failure} visible_while_fsync_blocked={}",String::from_utf8(seen).unwrap());
}
```
