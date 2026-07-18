//! Historical dynamic memory-budget detection, relocated here from the
//! library (which now defaults the engine to a fixed 2 GiB budget).
//!
//! Benchmarks keep the old sizing — the host's available memory
//! min-folded with a ¾-derated cgroup limit — by computing it at
//! startup and exporting it through `VSDB_MEM_BUDGET_MB`, so results
//! stay comparable with releases that auto-sized from the host. An
//! operator-provided `VSDB_MEM_BUDGET_MB` always wins.

const G: usize = 1024 * 1024 * 1024;

/// Compute the legacy dynamic budget and export it via
/// `VSDB_MEM_BUDGET_MB` (no-op when the variable is already set).
///
/// Must run at bench startup, before the first engine touch and
/// before any thread exists (`std::env::set_var` contract).
pub fn apply() {
    if std::env::var_os("VSDB_MEM_BUDGET_MB").is_some() {
        return;
    }
    let host = host_avail_bytes();
    // A detected cgroup limit contributes `limit * 3/4` to a min-fold
    // with the host reading (memory.high is a throttle line, not a
    // quota; memory.max is the OOM-kill line itself).
    let budget = match cgroup_mem_limit_bytes()
        .map(|limit| limit / 4 * 3)
        .filter(|&derated| derated < host)
    {
        Some(derated) => derated,
        None => host,
    };
    let mb = (budget / (1024 * 1024)).max(1);
    // SAFETY: executed at bench startup, before the first engine touch
    // and before any thread exists — the same contract
    // `vsdb_set_base_dir` documents.
    unsafe { std::env::set_var("VSDB_MEM_BUDGET_MB", mb.to_string()) };
}

/// Available physical memory (platform-specific), in bytes.
fn host_avail_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.contains("MemAvailable"))
                    .and_then(|l| {
                        l.replace(|ch: char| !ch.is_numeric(), "")
                            .parse::<usize>()
                            .ok()
                    })
            })
            .unwrap_or(G / 1024)
            * 1024
    }
    #[cfg(any(target_os = "freebsd", target_os = "macos"))]
    {
        // FreeBSD: hw.physmem, macOS: hw.memsize (both return bytes)
        let key = if cfg!(target_os = "freebsd") {
            "hw.physmem"
        } else {
            "hw.memsize"
        };
        std::process::Command::new("sysctl")
            .arg("-n")
            .arg(key)
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(G)
    }
    #[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
    {
        G
    }
}

/// The memory ceiling imposed on THIS process by its cgroup, if any:
/// the tightest limit found walking from this process's cgroup up to
/// the root (limits are hierarchical), or `None` when
/// unlimited/undetectable (non-Linux, no cgroup, or all-`max` paths).
#[cfg(target_os = "linux")]
fn cgroup_mem_limit_bytes() -> Option<usize> {
    use std::fs;

    fn parse_limit(s: &str) -> Option<usize> {
        let t = s.trim();
        if t.is_empty() || t == "max" {
            return None;
        }
        // v1 reports "no limit" as a platform-dependent huge number
        // (PAGE_COUNTER_MAX); treat anything >= 2^60 as unlimited,
        // and a literal `0` as undetectable rather than as a limit.
        t.parse::<usize>()
            .ok()
            .filter(|&v| v > 0 && v < (1usize << 60))
    }

    // /proc/self/cgroup line: `0::/system.slice/foo.service` (v2) or
    // `N:memory:/path` (v1).
    let cg = fs::read_to_string("/proc/self/cgroup").ok()?;
    let mut tightest: Option<usize> = None;
    let mut consider = |v: usize| {
        tightest = Some(tightest.map_or(v, |t| t.min(v)));
    };

    for line in cg.lines() {
        let mut parts = line.splitn(3, ':');
        let (Some(_), Some(controllers), Some(path)) =
            (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let (mount, files): (&str, &[&str]) = if controllers.is_empty() {
            // v2 unified hierarchy: memory.max is the hard (OOM) line;
            // memory.high is the throttle line -- respect the tighter.
            ("/sys/fs/cgroup", &["memory.max", "memory.high"])
        } else if controllers.split(',').any(|c| c == "memory") {
            ("/sys/fs/cgroup/memory", &["memory.limit_in_bytes"])
        } else {
            continue;
        };

        // Walk this cgroup and every ancestor up to the mount root.
        let mut rel = path.trim_start_matches('/');
        loop {
            for f in files {
                let p = if rel.is_empty() {
                    format!("{mount}/{f}")
                } else {
                    format!("{mount}/{rel}/{f}")
                };
                if let Some(v) =
                    fs::read_to_string(p).ok().as_deref().and_then(parse_limit)
                {
                    consider(v);
                }
            }
            match rel.rfind('/') {
                Some(i) => rel = &rel[..i],
                None if !rel.is_empty() => rel = "",
                None => break,
            }
        }
    }
    tightest
}

#[cfg(not(target_os = "linux"))]
fn cgroup_mem_limit_bytes() -> Option<usize> {
    None
}
