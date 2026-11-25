//! Run inside VSDB 6ddaed9 to write a physical v18-on-MMDB-v4 universe.
use std::{env, fs, path::PathBuf};
use vsdb::{
    DagMapRaw, HnswConfig, L2, MapxOrd, MptCalc, Namespace, SlotDex64, VecDex, VerMap,
    VerMapWithProof, VsdbOptions, vsdb_configure,
};
use vsdb_core::NamespaceOpts;

fn main() {
    let path = PathBuf::from(env::args_os().nth(1).expect("fresh fixture base"));
    vsdb_configure(VsdbOptions::new(&path)).unwrap();
    let ns = Namespace::create_with(NamespaceOpts {
        shards: 1,
        ..Default::default()
    })
    .unwrap();
    ns.scope(|| {
        let mut inner = MapxOrd::<u64, String>::new();
        inner.insert(&42, &"embedded".to_string());
        let mut commits = VerMap::<u64, u64>::new();
        let branch = commits.main_branch();
        commits.insert(branch, &1, &21).unwrap();
        let history = commits.commit(branch).unwrap();
        let saved = commits.save_meta().unwrap();
        let mut trie = VerMapWithProof::<_, _, MptCalc>::from_map(commits);
        let hash = trie.merkle_root_at_commit(history).unwrap();
        let mut parent = DagMapRaw::new(None);
        parent.insert(b"base", b"ancestor");
        let mut child = DagMapRaw::new(Some(&mut parent));
        child.insert(b"own", b"child");
        let mut slots = SlotDex64::<u64>::new(4, false).unwrap();
        for (s, v) in [(1, 10), (1, 11), (3, 30)] {
            slots.insert(s, v).unwrap();
        }
        let mut graph = VecDex::<u64, L2>::new(HnswConfig {
            dim: 2,
            ..Default::default()
        })
        .unwrap();
        for (i, x) in [(7, [1.0, 0.0]), (8, [0.75, 0.25]), (9, [0.0, 2.0])] {
            graph.insert(&i, &x).unwrap();
        }
        let hits: Vec<_> = graph
            .search(&[1., 0.], 3)
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k, v.to_bits()))
            .collect();
        let token = parent.child_id(&child).unwrap();
        let oracle = postcard::to_allocvec(&(
            inner,
            saved,
            history,
            hash,
            parent.save_meta().unwrap(),
            child.save_meta().unwrap(),
            token,
            slots.save_meta().unwrap(),
            graph.save_meta().unwrap(),
            hits,
        ))
        .unwrap();
        fs::create_dir_all(path.join("__CUSTOM__")).unwrap();
        fs::write(path.join("__CUSTOM__/oracle"), oracle).unwrap();
        ns.flush();
        ns.try_sync_wal().unwrap();
    });
}
