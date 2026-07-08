//! Composite structures × namespaces integration.
//!
//! Single `#[test]`: sub-scenarios share the process-global registry
//! and base-dir freeze, so they run sequentially inside one body.

use vsdb::{
    DEFAULT_NS_ID, HnswConfig, MapxOrd, MetricKind, Namespace, Orphan, SlotDex,
    VecDexDyn, VerMap, basic::mapx::Mapx, vsdb_ns_close, vsdb_ns_destroy,
    vsdb_set_base_dir,
};

#[test]
fn composites_in_namespaces() {
    let dir = format!("/tmp/vsdb_testing/ns_strata_{}", rand::random::<u128>());
    vsdb_set_base_dir(&dir).unwrap();

    let ns = Namespace::create().unwrap();

    // ---- typed wrappers: explicit placement + identity round-trip ----
    let mut m: Mapx<u64, String> = Mapx::new_in(&ns);
    m.insert(&1, &"one".to_owned());
    assert_eq!(m.namespace().id(), ns.id());
    let id = m.save_meta().unwrap();
    assert_eq!(id.ns, Some(ns.id()));
    let m2: Mapx<u64, String> = Mapx::from_meta(id).unwrap();
    assert_eq!(m2.get(&1).unwrap(), "one");

    // Bare-u64 compatibility path (default namespace).
    let mut d: MapxOrd<u64, u64> = MapxOrd::new();
    d.insert(&7, &70);
    let did = d.save_meta().unwrap();
    assert_eq!(did.ns, None);
    let d2: MapxOrd<u64, u64> = MapxOrd::from_meta(did.map_id).unwrap();
    assert_eq!(d2.get(&7).unwrap(), 70);

    // ---- composite invariant: every internal map lands in one ns ----
    let mut vm: VerMap<u64, u64> = VerMap::new_in(&ns);
    assert_eq!(vm.namespace().id(), ns.id());
    let mut b = vm.main_mut();
    b.insert(&1, &10).unwrap();
    b.commit().unwrap();
    assert_eq!(vm.main().get(&1).unwrap(), Some(10));
    let vmid = vm.save_meta().unwrap();
    assert_eq!(vmid.ns, Some(ns.id()));
    let vm2: VerMap<u64, u64> = VerMap::from_meta(vmid).unwrap();
    assert_eq!(vm2.main().get(&1).unwrap(), Some(10));

    // Scoped ambient placement covers whole subsystems.
    let sd = ns.scope(|| SlotDex::<u64, u64>::new(16, false));
    assert_eq!(sd.namespace().id(), ns.id());

    let o = Orphan::new_in(&ns, 42u64);
    assert_eq!(o.namespace().id(), ns.id());
    assert_eq!(o.get_value(), 42);

    // Serde round-trip carries the namespace through the typed envelope.
    let blob = postcard::to_allocvec(&m).unwrap();
    let m3: Mapx<u64, String> = postcard::from_bytes(&blob).unwrap();
    assert_eq!(m3.namespace().id(), ns.id());
    assert_eq!(m3.get(&1).unwrap(), "one");

    // Default behavior untouched.
    assert_eq!(Mapx::<u8, u8>::new().namespace().id(), DEFAULT_NS_ID);

    // ---- cross-namespace copy helpers (clone_in) ----
    // ns → default, through the typed wrapper chain.
    let mut m_copy = m.clone_in(&Namespace::default_ns()).unwrap();
    assert_eq!(m_copy.namespace().id(), DEFAULT_NS_ID);
    assert_eq!(m_copy.get(&1).unwrap(), "one");
    m_copy.insert(&2, &"copy-only".to_owned());
    assert!(m.get(&2).is_none()); // source untouched
    // default → ns direction.
    let d_copy = d.clone_in(&ns).unwrap();
    assert_eq!(d_copy.namespace().id(), ns.id());
    assert_eq!(d_copy.get(&7).unwrap(), 70);
    // Orphan follows the same pattern.
    let o_copy = o.clone_in(&Namespace::default_ns()).unwrap();
    assert_eq!(o_copy.namespace().id(), DEFAULT_NS_ID);
    assert_eq!(o_copy.get_value(), 42);

    // ---- runtime-metric VecDex placed via new_in ----
    let mut vd: VecDexDyn<String> = VecDexDyn::new_in(
        &ns,
        MetricKind::Cosine,
        HnswConfig {
            dim: 2,
            ..Default::default()
        },
    );
    assert_eq!(vd.namespace().id(), ns.id());
    assert_eq!(vd.metric(), MetricKind::Cosine);
    vd.insert(&"a".to_owned(), &[1.0, 0.0]).unwrap();
    let vdid = vd.save_meta().unwrap();
    assert_eq!(vdid.ns, Some(ns.id()));
    drop(vd);
    let vd: VecDexDyn<String> = VecDexDyn::from_meta(vdid).unwrap();
    assert_eq!(vd.metric(), MetricKind::Cosine);
    assert_eq!(vd.search(&[1.0, 0.0], 1).unwrap()[0].0, "a");
    drop(vd);

    // ---- in-process close through the typed layer ----
    let e = Namespace::create().unwrap();
    let eid = e.id();
    let mut cm: Mapx<u64, String> = Mapx::new_in(&e);
    cm.insert(&1, &"epoch".to_owned());
    let cid = cm.save_meta().unwrap();
    // Typed handles pin the namespace: refused while any is alive.
    assert!(vsdb_ns_close(eid).is_err());
    drop(cm);
    drop(e);
    vsdb_ns_close(eid).unwrap();
    // Reopen through the persisted meta — restart-equivalent recovery.
    let cm: Mapx<u64, String> = Mapx::from_meta(cid).unwrap();
    assert_eq!(cm.get(&1).unwrap(), "epoch");
    drop(cm);
    vsdb_ns_close(eid).unwrap();
    vsdb_ns_destroy(eid).unwrap();

    std::fs::remove_dir_all(&dir).ok();
}
