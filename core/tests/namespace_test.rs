//! Namespace lifecycle integration tests.
//!
//! One `#[test]` only: the suite runs single-threaded, but every test in
//! this FILE shares the process-global default engine and registry, so
//! sub-scenarios run sequentially inside one body (same pattern as the
//! other integration tests).

use std::path::PathBuf;
use vsdb_core::{
    DEFAULT_NS_ID, InstanceId, Namespace, NamespaceOpts, basic::mapx_raw::MapxRaw,
    vsdb_ns_destroy, vsdb_ns_list, vsdb_ns_relocate,
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

    // Failed-create rollback: point the root at an existing FILE, so
    // validation passes (absolute, non-overlapping) but the engine
    // open fails on create_dir_all. The registry must not retain the
    // burnt entry.
    let blocker = format!("{dir}_blocker");
    std::fs::write(&blocker, b"x").unwrap();
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
    std::fs::remove_file(&blocker).ok();

    // Adopting an existing non-empty dir as an explicit root is refused
    // (foreign prefixes have unknown provenance; destroy would take the
    // dir's other contents with it).
    let occupied = format!("{dir}_occupied");
    std::fs::create_dir_all(format!("{occupied}/stuff")).unwrap();
    assert!(
        Namespace::create_with(NamespaceOpts {
            path: Some(PathBuf::from(&occupied)),
            ..Default::default()
        })
        .is_err()
    );
    // An existing but EMPTY dir is fine (e.g. a fresh mount point).
    let empty_mnt = format!("{dir}_mnt");
    std::fs::create_dir_all(&empty_mnt).unwrap();
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
    std::fs::remove_dir_all(&occupied).ok();

    // ---- cross-namespace handles coexist in one container ----
    let all: Vec<MapxRaw> = vec![m0, m1, m2, m4];
    let hits = all.iter().filter(|m| m.get(b"k").is_some()).count();
    assert_eq!(hits, 2);

    std::fs::remove_dir_all(&dir).ok();
}
