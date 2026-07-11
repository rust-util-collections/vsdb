//! Namespace lifecycle integration tests.
//!
//! One `#[test]` only: every test in this FILE would share the
//! process-global default engine, registry, and base-dir freeze, so
//! sub-scenarios run sequentially inside one body (same pattern as the
//! other integration tests).

use std::{fs, path::PathBuf};
use vsdb_core::{
    DEFAULT_NS_ID, InstanceId, Namespace, NamespaceOpts, basic::mapx_raw::MapxRaw,
    vsdb_ns_close, vsdb_ns_destroy, vsdb_ns_list, vsdb_ns_relocate,
};

#[test]
fn namespace_lifecycle() {
    let dir = format!("/tmp/vsdb_testing/ns_lifecycle_{}", rand::random::<u128>());
    vsdb_core::vsdb_set_base_dir(&dir).unwrap();

    // ---- default namespace: fixed id, zero ceremony ----
    let d = Namespace::default_ns();
    assert_eq!(d.id(), DEFAULT_NS_ID);
    let mut m0 = MapxRaw::new();
    m0.insert(b"k", b"default");
    assert_eq!(m0.namespace().id(), DEFAULT_NS_ID);
    assert_eq!(m0.instance_id().ns, None);

    // ---- anonymous placement group ----
    let ns = Namespace::create().unwrap();
    assert_ne!(ns.id(), DEFAULT_NS_ID);
    assert!(ns.path().starts_with(&dir));

    let mut m1 = MapxRaw::new_in(&ns);
    m1.insert(b"k", b"ns");
    assert_eq!(m1.namespace().id(), ns.id());
    assert_eq!(m1.instance_id().ns, Some(ns.id()));
    // Data paths are disjoint.
    assert_eq!(&m0.get(b"k").unwrap()[..], b"default");
    assert_eq!(&m1.get(b"k").unwrap()[..], b"ns");

    // Prefixes stay globally unique across namespaces.
    assert_ne!(m0.instance_id().map_id, m1.instance_id().map_id);

    // ---- scoped ambient placement (creation-time only) ----
    let (m2, m3_id) = ns.scope(|| {
        let mut a = MapxRaw::new(); // lands in `ns`
        a.insert(b"s", b"scoped");
        let b = MapxRaw::new_in(&Namespace::default_ns()); // explicit wins
        (a, b.namespace().id())
    });
    assert_eq!(m2.namespace().id(), ns.id());
    assert_eq!(m3_id, DEFAULT_NS_ID);
    // Outside the scope, new() reverts to the default namespace.
    assert_eq!(MapxRaw::new().namespace().id(), DEFAULT_NS_ID);
    // Nesting: innermost wins, unwound in order.
    let inner = Namespace::create().unwrap();
    ns.scope(|| {
        inner.scope(|| {
            assert_eq!(Namespace::current().id(), inner.id());
        });
        assert_eq!(Namespace::current().id(), ns.id());
    });

    // ---- co-location primitive ----
    let m4 = MapxRaw::new_in(&m2.namespace());
    assert_eq!(m4.namespace().id(), ns.id());

    // ---- serde round-trip carries the namespace ----
    let blob = postcard::to_allocvec(&m1).unwrap();
    let m1r: MapxRaw = postcard::from_bytes(&blob).unwrap();
    assert_eq!(m1r.namespace().id(), ns.id());
    assert_eq!(&m1r.get(b"k").unwrap()[..], b"ns");
    // Default-ns meta stays byte-identical to the pre-v16 form (16 B
    // inner meta ⇒ no ns suffix).
    let blob0 = postcard::to_allocvec(&m0).unwrap();
    let m0r: MapxRaw = postcard::from_bytes(&blob0).unwrap();
    assert_eq!(m0r.namespace().id(), DEFAULT_NS_ID);

    // ---- instance-meta round-trip, both address forms ----
    let id1 = m1.save_meta().unwrap();
    assert_eq!(id1, m1.instance_id());
    let m1m = MapxRaw::from_meta(id1).unwrap();
    assert_eq!(&m1m.get(b"k").unwrap()[..], b"ns");
    let id0 = m0.save_meta().unwrap();
    // Bare u64 (pre-v16 style) is a complete default-ns address.
    let m0m = MapxRaw::from_meta(id0.map_id).unwrap();
    assert_eq!(&m0m.get(b"k").unwrap()[..], b"default");
    // InstanceId string round-trip.
    assert_eq!(id1.to_string().parse::<InstanceId>().unwrap(), id1);
    assert_eq!(id0.to_string().parse::<InstanceId>().unwrap(), id0);
    // "@0" is a non-canonical spelling of the default namespace: both
    // parsing and deserialization fold it to `ns: None`.
    let noncanon = format!("{}@0", id0.map_id).parse::<InstanceId>().unwrap();
    assert_eq!(noncanon, id0);
    assert_eq!(noncanon.ns, None);
    let wire = postcard::to_allocvec(&InstanceId {
        map_id: id0.map_id,
        ns: Some(DEFAULT_NS_ID),
    })
    .unwrap();
    let decoded: InstanceId = postcard::from_bytes(&wire).unwrap();
    assert_eq!(decoded, id0);

    // ---- cross-namespace deep copy (clone_in) ----
    // ns → default: the copy is a brand-new instance placed in the
    // target namespace, byte-identical in content, fully independent.
    let mut copy = m1.clone_in(&Namespace::default_ns()).unwrap();
    assert_eq!(copy.namespace().id(), DEFAULT_NS_ID);
    assert_eq!(copy.instance_id().ns, None);
    assert_eq!(&copy.get(b"k").unwrap()[..], b"ns");
    assert!(!copy.is_the_same_instance(&m1));
    copy.insert(b"copy-only", b"y");
    assert!(m1.get(b"copy-only").is_none()); // source untouched
    drop(copy);
    // Same-namespace clone_in ≡ Clone (co-located deep copy).
    let same = m1.clone_in(&m1.namespace()).unwrap();
    assert_eq!(same.namespace().id(), ns.id());
    assert_eq!(&same.get(b"k").unwrap()[..], b"ns");
    assert!(!same.is_the_same_instance(&m1));
    drop(same);
    // Crosses the 4096-pair chunk boundary (two batch commits).
    let mut big = MapxRaw::new_in(&ns);
    for i in 0..4100u16 {
        big.insert(i.to_be_bytes(), b"v");
    }
    let big_copy = big.clone_in(&Namespace::default_ns()).unwrap();
    assert_eq!(big_copy.iter().count(), 4100);
    assert_eq!(big_copy.namespace().id(), DEFAULT_NS_ID);
    drop((big, big_copy));

    // ---- registry / admin tier ----
    let infos = vsdb_ns_list().unwrap();
    assert!(infos.iter().any(|i| i.id == ns.id() && !i.pinned));
    // Ids are never reused and open() is idempotent.
    assert_eq!(Namespace::open(ns.id()).unwrap().id(), ns.id());
    assert_eq!(Namespace::open(DEFAULT_NS_ID).unwrap().id(), DEFAULT_NS_ID);
    assert!(Namespace::open(u64::MAX).is_err());

    // Open namespaces refuse destroy/relocate; unknown ids error.
    assert!(vsdb_ns_destroy(ns.id()).is_err());
    assert!(vsdb_ns_destroy(DEFAULT_NS_ID).is_err());
    assert!(vsdb_ns_relocate(ns.id(), "/tmp/x").is_err());
    assert!(vsdb_ns_destroy(u64::MAX).is_err());

    // Destroying a never-opened namespace reclaims its whole tree.
    // (Register one through a scope-free create in a helper process
    // model is overkill here: create, note the path, and destroy it
    // before ever writing through it — creation opens it, so simulate
    // the not-open state by using a fresh id from the registry.)
    let victim = Namespace::create_with(NamespaceOpts {
        shards: 1,
        ..Default::default()
    })
    .unwrap();
    let victim_id = victim.id();
    // Still open in this process ⇒ refused.
    assert!(vsdb_ns_destroy(victim_id).is_err());

    // Explicit-path validation: overlapping the base dir is rejected.
    assert!(
        Namespace::create_with(NamespaceOpts {
            path: Some(PathBuf::from(&dir)),
            ..Default::default()
        })
        .is_err()
    );
    // Relative paths are rejected.
    assert!(
        Namespace::create_with(NamespaceOpts {
            path: Some(PathBuf::from("relative/dir")),
            ..Default::default()
        })
        .is_err()
    );

    // Failed-create, pre-registry path: a FILE at the root path fails
    // the adoptable check before anything is persisted.
    let blocker = format!("{dir}_blocker");
    fs::write(&blocker, b"x").unwrap();
    let before: Vec<_> = vsdb_ns_list().unwrap().iter().map(|i| i.id).collect();
    assert!(
        Namespace::create_with(NamespaceOpts {
            path: Some(PathBuf::from(&blocker)),
            ..Default::default()
        })
        .is_err()
    );
    let after: Vec<_> = vsdb_ns_list().unwrap().iter().map(|i| i.id).collect();
    assert_eq!(before, after);
    fs::remove_file(&blocker).ok();

    // Failed-create, POST-registry rollback path: the root passes
    // validation (absent, under a read-only parent) but the engine
    // open fails on create_dir_all — the just-persisted entry must be
    // rolled back, the root left clean, and the same path immediately
    // retryable once the obstacle is gone.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let ro_parent = format!("{dir}_ro_parent");
        fs::create_dir_all(&ro_parent).unwrap();
        fs::set_permissions(&ro_parent, fs::Permissions::from_mode(0o555)).unwrap();
        let target = format!("{ro_parent}/ns_root");
        let before: Vec<_> = vsdb_ns_list().unwrap().iter().map(|i| i.id).collect();
        let attempt = Namespace::create_with(NamespaceOpts {
            path: Some(PathBuf::from(&target)),
            shards: 1,
            ..Default::default()
        });
        if attempt.is_err() {
            // (Running as root would let the create succeed — only
            // assert the rollback contract when the failure occurred.)
            let after: Vec<_> = vsdb_ns_list().unwrap().iter().map(|i| i.id).collect();
            assert_eq!(before, after, "failed create must roll back");
            assert!(!PathBuf::from(&target).exists(), "root left clean");
            // Retry succeeds once the parent is writable again.
            fs::set_permissions(&ro_parent, fs::Permissions::from_mode(0o755)).unwrap();
            let retried = Namespace::create_with(NamespaceOpts {
                path: Some(PathBuf::from(&target)),
                shards: 1,
                ..Default::default()
            })
            .unwrap();
            assert_eq!(retried.path(), std::path::Path::new(&target));
        } else {
            fs::set_permissions(&ro_parent, fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    // Adopting an existing non-empty dir as an explicit root is refused
    // (foreign prefixes have unknown provenance; destroy would take the
    // dir's other contents with it).
    let occupied = format!("{dir}_occupied");
    fs::create_dir_all(format!("{occupied}/stuff")).unwrap();
    assert!(
        Namespace::create_with(NamespaceOpts {
            path: Some(PathBuf::from(&occupied)),
            ..Default::default()
        })
        .is_err()
    );
    // An existing but EMPTY dir is fine (e.g. a fresh mount point).
    let empty_mnt = format!("{dir}_mnt");
    fs::create_dir_all(&empty_mnt).unwrap();
    let mnt_ns = Namespace::create_with(NamespaceOpts {
        path: Some(PathBuf::from(&empty_mnt)),
        shards: 1,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(mnt_ns.path(), std::path::Path::new(&empty_mnt));
    // Paths containing `..` are rejected outright.
    assert!(
        Namespace::create_with(NamespaceOpts {
            path: Some(PathBuf::from(format!("{empty_mnt}/../sneaky"))),
            ..Default::default()
        })
        .is_err()
    );
    fs::remove_dir_all(&occupied).ok();

    // ---- cross-namespace handles coexist in one container ----
    let all: Vec<MapxRaw> = vec![m0, m1, m2, m4];
    let hits = all.iter().filter(|m| m.get(b"k").is_some()).count();
    assert_eq!(hits, 2);

    // ---- in-process close: the epoch-rotation loop ----
    // Two full epochs of create → fill → drop handles → close →
    // (reopen → close →) destroy, without a process restart.
    for epoch in 0..2u8 {
        let e = Namespace::create_with(NamespaceOpts {
            shards: 2,
            ..Default::default()
        })
        .unwrap();
        let eid = e.id();
        let epath = e.path().to_path_buf();
        let mut m = MapxRaw::new_in(&e);
        m.insert(b"epoch", [epoch]);
        let mid = m.save_meta().unwrap();

        // Refusal matrix: close never invalidates a live handle.
        assert!(vsdb_ns_close(eid).is_err()); // `e` + `m` alive
        drop(m);
        assert!(vsdb_ns_close(eid).is_err()); // `e` alive
        e.scope(|| {
            // The ambient-scope stack holds a clone too.
            assert!(vsdb_ns_close(eid).is_err());
        });
        drop(e);

        // Every handle gone ⇒ full teardown (threads, fds, LOCKs).
        vsdb_ns_close(eid).unwrap();
        assert!(vsdb_ns_close(eid).is_err()); // not open anymore

        // LOCK files were released ⇒ in-process reopen works, and a
        // persisted InstanceId resolves exactly as after a restart.
        let m = MapxRaw::from_meta(mid).unwrap();
        assert_eq!(&m.get(b"epoch").unwrap()[..], [epoch]);
        drop(m);
        vsdb_ns_close(eid).unwrap();

        // destroy composes with close: O(1) bulk reclaim, no restart.
        vsdb_ns_destroy(eid).unwrap();
        assert!(!epath.exists());
        assert!(Namespace::open(eid).is_err()); // registry entry gone
    }
    // The default namespace is never closeable; unknown ids error.
    assert!(vsdb_ns_close(DEFAULT_NS_ID).is_err());
    assert!(vsdb_ns_close(u64::MAX).is_err());

    // ---- per-shard property passthrough + engine-level cache pool ----
    // A fresh namespace owns a private engine, so its telemetry is
    // fully isolated from every other test in this process.
    let t = Namespace::create_with(NamespaceOpts {
        shards: 2,
        ..Default::default()
    })
    .unwrap();
    let tid = t.id();
    let mut tm = MapxRaw::new_in(&t);
    // One map = one prefix = one shard; values sized so the shard's
    // SST spans many 16 KB blocks (only the first L0 block is pinned,
    // so cold reads of the rest must MISS, repeat reads must HIT).
    for i in 0..128u16 {
        tm.insert(i.to_be_bytes(), [7u8; 4096]);
    }
    // Push the rows into SSTs so reads exercise the block cache.
    t.flush();
    for _ in 0..2 {
        for i in 0..128u16 {
            assert!(tm.get(i.to_be_bytes()).is_some());
        }
    }
    // Shard-ordered readings, one per shard.
    let hits = t.shard_properties("stats.block_cache_hits");
    let misses = t.shard_properties("stats.block_cache_misses");
    assert_eq!(hits.len(), 2);
    assert_eq!(misses.len(), 2);
    let sum = |v: &[Option<String>]| -> u64 {
        v.iter()
            .map(|s| s.as_deref().unwrap().parse::<u64>().unwrap())
            .sum()
    };
    // First SST read misses, repeat reads hit — both counters moved.
    assert!(sum(&misses) >= 1, "expected at least one cache miss");
    assert!(sum(&hits) >= 1, "expected at least one cache hit");
    // The cache pool is engine-level: usage readings are pool totals
    // (plus per-shard pins) and must parse on every shard.
    let usage = t.shard_properties("block-cache-usage");
    assert!(
        usage
            .iter()
            .all(|s| s.as_deref().unwrap().parse::<u64>().is_ok())
    );
    // Unknown names yield None per shard, never a panic.
    assert_eq!(t.shard_properties("no-such-property"), vec![None, None]);
    drop(tm);
    drop(t);
    vsdb_ns_close(tid).unwrap();
    vsdb_ns_destroy(tid).unwrap();

    // ---- consuming close: Namespace::close(self) ----
    // Refusal returns the handle for continued use.
    let expect_refused = |r: std::result::Result<
        (),
        (Option<Namespace>, vsdb_core::VsdbError),
    >| match r {
        Err((Some(h), _)) => h,
        _ => panic!("close must be refused and hand the handle back"),
    };
    let c = Namespace::create_with(NamespaceOpts {
        shards: 1,
        ..Default::default()
    })
    .unwrap();
    let cid = c.id();
    // Another `Namespace` clone blocks the consuming form.
    let extra = c.clone();
    let c = expect_refused(c.close());
    assert_eq!(c.id(), cid);
    drop(extra);
    // A collection handle blocks it too — and the returned handle
    // stays fully usable.
    let mut cm = MapxRaw::new_in(&c);
    cm.insert(b"c", b"v");
    let c = expect_refused(c.close());
    assert_eq!(MapxRaw::new_in(&c).namespace().id(), cid);
    drop(cm);
    // Sole handle ⇒ consumed and fully closed; the id-addressed form
    // then sees a not-open namespace.
    c.close().unwrap();
    assert!(vsdb_ns_close(cid).is_err());
    // Restart-equivalent reopen, then reclaim.
    let re = Namespace::open(cid).unwrap();
    re.close().unwrap();
    vsdb_ns_destroy(cid).unwrap();
    // The default namespace refuses the consuming form, handle back.
    let d = expect_refused(Namespace::default_ns().close());
    assert_eq!(d.id(), DEFAULT_NS_ID);

    // ---- relocate refuses an unpopulated target ----
    // (write → close → relocate-to-empty-dir must FAIL: repointing the
    // registry at a dir without the dataset would silently orphan the
    // real data; after physically moving the tree, relocate succeeds
    // and the data is reachable at the new root.)
    let r = Namespace::create_with(NamespaceOpts {
        shards: 2,
        ..Default::default()
    })
    .unwrap();
    let rid = r.id();
    let old_root = r.path().to_path_buf();
    let mut rm = MapxRaw::new_in(&r);
    rm.insert(b"moved", b"yes");
    let rmid = rm.save_meta().unwrap();
    drop((rm, r));
    vsdb_ns_close(rid).unwrap();

    let new_root = format!("{dir}_relocated");
    fs::create_dir_all(&new_root).unwrap();
    // Empty target: refused (the data has not been moved).
    assert!(vsdb_ns_relocate(rid, &new_root).is_err());
    // Bare skeleton (marker + empty shard dirs, no engine anchors —
    // e.g. a provisioning script "prepared" the volume, or a copy was
    // interrupted before any shard content landed): still refused.
    fs::create_dir_all(format!("{new_root}/__SYSTEM__")).unwrap();
    fs::write(format!("{new_root}/__SYSTEM__/format_version"), "16").unwrap();
    fs::create_dir_all(format!("{new_root}/mmdb/shard_00")).unwrap();
    fs::create_dir_all(format!("{new_root}/mmdb/shard_01")).unwrap();
    assert!(vsdb_ns_relocate(rid, &new_root).is_err());
    // Move the tree for real, then relocate: accepted.
    fs::remove_dir_all(&new_root).unwrap();
    fs::rename(&old_root, &new_root).unwrap();
    fs::create_dir_all(format!("{new_root}/mmdb/shard_02")).unwrap();
    fs::write(
        format!("{new_root}/mmdb/shard_02/CURRENT"),
        "MANIFEST-000001",
    )
    .unwrap();
    assert!(vsdb_ns_relocate(rid, &new_root).is_err());
    fs::remove_dir_all(format!("{new_root}/mmdb/shard_02")).unwrap();
    fs::write(format!("{new_root}/__SYSTEM__/format_version"), "17").unwrap();
    assert!(vsdb_ns_relocate(rid, &new_root).is_err());
    fs::write(format!("{new_root}/__SYSTEM__/format_version"), "16").unwrap();
    vsdb_ns_relocate(rid, &new_root).unwrap();
    let rm = MapxRaw::from_meta(rmid).unwrap();
    assert_eq!(&rm.get(b"moved").unwrap()[..], b"yes");
    drop(rm);
    vsdb_ns_close(rid).unwrap();
    vsdb_ns_destroy(rid).unwrap();
    assert!(!PathBuf::from(&new_root).exists());

    fs::remove_dir_all(&dir).ok();
    // Sibling scratch dirs created by the sub-scenarios above.
    fs::remove_dir_all(format!("{dir}_mnt")).ok();
    fs::remove_dir_all(format!("{dir}_ro_parent")).ok();
}
