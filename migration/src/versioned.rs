//! Prefix-preserving conversion of an offline v17 version store.
use crate::{LegacyRaw, corrupt, legacy_payload, tag, typed_handle};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use vsdb::{
    BranchId, CommitId, InstanceId, KeyEnDeOrdered, MapxRaw, Result, ValueEnDe, VerMap, VsdbError,
    common::{load_instance_meta, save_instance_meta},
};

#[derive(Deserialize)]
struct BranchState {
    name: String,
    head: CommitId,
    dirty_root: u64,
}
#[derive(Deserialize)]
struct Commit {
    id: CommitId,
    root: u64,
    parents: Vec<CommitId>,
    timestamp_us: u64,
    ref_count: u32,
}
const NO_COMMIT: CommitId = CommitId::from_raw(0);
type NodeId = u64;
const MAX_KEYS: usize = 32;
const LEAF: u64 = 1 << 62;
const BLOB_LEAF: u64 = 2 << 62;
const BLOB: u64 = 3 << 62;
const HEAD: u8 = 1;
const NAME: u8 = 2;
const COMMIT: u8 = 4;
const HEADER: [u8; 1] = [0];

#[derive(Deserialize)]
struct Components<'a>(
    (LegacyRaw, u64),
    &'a [u8],
    &'a [u8],
    &'a [u8],
    &'a [u8],
    &'a [u8],
    &'a [u8],
    &'a [u8],
);
fn component(frame: &[u8], outer: &str, inner: &str) -> Result<MapxRaw> {
    fn checked<'a>(bytes: &'a [u8], name: &str) -> Result<&'a [u8]> {
        let hash = tag(name);
        if bytes.get(..8) != Some(b"VSTYPE03") || bytes.get(8..16) != Some(&hash.to_le_bytes()) {
            return Err(corrupt("unexpected v17 component type"));
        }
        Ok(&bytes[16..])
    }
    let (inner_frame, tail): (&[u8], _) = postcard::take_from_bytes(checked(frame, outer)?)?;
    if !tail.is_empty() {
        return Err(corrupt("trailing v17 component payload"));
    }
    let (raw, tail): (LegacyRaw, _) = postcard::take_from_bytes(checked(inner_frame, inner)?)?;
    if !tail.is_empty() {
        return Err(corrupt("trailing v17 prefix payload"));
    }
    Ok(raw.0)
}

enum Node {
    Leaf {
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    },
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<u64>,
    },
}
struct Header {
    next_id: u64,
    next_commit: u64,
    next_branch: u64,
    main_branch: BranchId,
    generation: u64,
}
struct BranchHead {
    head: CommitId,
    dirty_root: u64,
}
fn by_name(name: &str) -> Vec<u8> {
    [&[3], name.as_bytes()].concat()
}
fn row(tag: u8, id: u64) -> [u8; 9] {
    let mut key = [tag; 9];
    key[1..].copy_from_slice(&id.to_be_bytes());
    key
}
fn object_key(id: u64) -> [u8; 9] {
    row(if id & BLOB == BLOB { 0x12 } else { 0x10 }, id)
}
impl Header {
    fn bytes(&self) -> Vec<u8> {
        let mut out = vec![18];
        for n in [
            self.next_id,
            self.next_commit,
            self.next_branch,
            self.main_branch.raw(),
            self.generation,
        ] {
            out.extend_from_slice(&n.to_le_bytes());
        }
        out
    }
}
impl BranchHead {
    fn bytes(&self) -> Vec<u8> {
        [self.head.raw().to_le_bytes(), self.dirty_root.to_le_bytes()].concat()
    }
}
impl Commit {
    fn bytes(&self) -> Vec<u8> {
        let mut out = [self.root.to_le_bytes(), self.timestamp_us.to_le_bytes()].concat();
        out.extend_from_slice(&self.ref_count.to_le_bytes());
        for p in &self.parents {
            out.extend_from_slice(&p.raw().to_le_bytes());
        }
        out
    }
}
impl Node {
    fn key_count(&self) -> usize {
        match self {
            Self::Leaf { keys, .. } | Self::Internal { keys, .. } => keys.len(),
        }
    }

    fn encode_with(&self, mut blob: impl FnMut(&[u8]) -> NodeId) -> Vec<u8> {
        let n = self.key_count();
        let leaf = matches!(self, Self::Leaf { .. });
        let keys = match self {
            Self::Leaf { keys, .. } | Self::Internal { keys, .. } => keys,
        };
        let mut raw = vec![0; 2 + 12 * n + if leaf { 0 } else { 8 }];
        raw[0] = if leaf { 0x18 } else { 0x19 };
        raw[1] = n.try_into().expect("B+ tree page fanout overflow");
        for (i, key) in keys.iter().enumerate() {
            let p = 2 + 12 * i;
            raw.extend_from_slice(key);
            let end = u32::try_from(raw.len()).expect("B+ tree page offset overflow");
            raw[p..p + 4].copy_from_slice(&end.to_le_bytes());
            let desc = match self {
                Self::Leaf { values, .. } if values[i].len() > 1024 => blob(&values[i]),
                Self::Leaf { values, .. } => {
                    raw.extend_from_slice(&values[i]);
                    raw.len() as u64
                }
                Self::Internal { children, .. } => children[i],
            };
            raw[p + 4..p + 12].copy_from_slice(&desc.to_le_bytes());
        }
        if let Self::Internal { children, .. } = self {
            raw[2 + 12 * n..10 + 12 * n].copy_from_slice(&children[n].to_le_bytes());
        }
        raw
    }
}

/// Upgrades a saved v17 version store in place under exclusive access.
/// Branch IDs, commit IDs, history and the primary instance ID survive.
/// Retries finish cleanup after the new header has been published.
pub fn migrate_v17<K: KeyEnDeOrdered, V: ValueEnDe>(
    id: impl Into<InstanceId>,
) -> Result<VerMap<K, V>> {
    let id = id.into();
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
    let frame: Vec<u8> = load_instance_meta(id)?;
    if frame.starts_with(b"VSTYPE04") {
        return VerMap::<K, V>::from_meta_verified(id);
    }
    let bytes = legacy_payload::<VerMap<K, V>>(&frame)?;
    let (
        Components(
            (LegacyRaw(mut store), _),
            commits,
            branches,
            names,
            next_commit,
            next_branch,
            main,
            dirty,
        ),
        tail,
    ) = postcard::take_from_bytes(bytes)?;
    if !tail.is_empty() || store.instance_id() != id {
        return Err(corrupt("invalid v17 version store handle"));
    }
    if store.namespace().is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "VerMap migration",
        });
    }
    let layer = |f: &[u8], wrapper: &str, generic: &str, val: &str| {
        component(
            f,
            &format!("vsdb::basic::{wrapper}<{generic}>"),
            &format!("vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey<{val}>"),
        )
    };
    let commits = layer(
        commits,
        "mapx_ord::MapxOrd",
        "u64, vsdb::versioned::Commit",
        "vsdb::versioned::Commit",
    )?;
    let branches = layer(
        branches,
        "mapx_ord::MapxOrd",
        "u64, vsdb::versioned::map::BranchState",
        "vsdb::versioned::map::BranchState",
    )?;
    let names = layer(names, "mapx::Mapx", "alloc::string::String, u64", "u64")?;
    let orphan = |f: &[u8], val: &str| layer(f, "orphan::Orphan", val, val);
    let (next_commit, next_branch, main, dirty) = (
        orphan(next_commit, "u64")?,
        orphan(next_branch, "u64")?,
        orphan(main, "u64")?,
        orphan(dirty, "bool")?,
    );
    let mut retired = [
        commits,
        branches,
        names,
        next_commit,
        next_branch,
        main,
        dirty,
    ];
    let mut ids = HashSet::from([id]);
    for component in &retired {
        let component_id = component.instance_id();
        if component_id.ns != id.ns || !ids.insert(component_id) {
            return Err(corrupt("invalid component identity"));
        }
    }
    if store.get(HEADER).is_none() {
        let [commits, branches, _, next_commit, next_branch, main, _] = &retired;
        let scalar = |v: &MapxRaw| -> Result<u64> {
            Ok(postcard::from_bytes(
                &v.get([]).ok_or_else(|| corrupt("missing v17 allocator"))?,
            )?)
        };
        let mut hdr = Header {
            next_id: 1,
            next_commit: scalar(next_commit)?,
            next_branch: scalar(next_branch)?,
            main_branch: BranchId::from_raw(scalar(main)?),
            generation: 0,
        };
        let mut branch_rows = BTreeMap::new();
        let mut unique_names = HashSet::new();
        for (k, v) in branches.iter() {
            let branch = legacy_id(&k)?;
            let state: BranchState = postcard::from_bytes(&v)?;
            if branch == 0 || !unique_names.insert(state.name.clone()) {
                return Err(corrupt("invalid v17 branch table"));
            }
            hdr.next_branch = hdr.next_branch.max(
                branch
                    .checked_add(1)
                    .ok_or_else(|| corrupt("branch IDs exhausted"))?,
            );
            branch_rows.insert(branch, state);
        }
        if !branch_rows.contains_key(&hdr.main_branch.raw()) {
            return Err(corrupt("missing v17 main branch"));
        }
        let mut live = BTreeMap::new();
        let mut refs = HashMap::<CommitId, u32>::new();
        let mut work: Vec<_> = branch_rows
            .values()
            .map(|s| s.head)
            .filter(|id| *id != NO_COMMIT)
            .collect();
        while let Some(id) = work.pop() {
            let count = refs.entry(id).or_default();
            *count = count
                .checked_add(1)
                .ok_or_else(|| corrupt("commit refcount overflow"))?;
            if *count > 1 {
                continue;
            }
            let raw = commits
                .get(id.raw().to_be_bytes())
                .ok_or_else(|| corrupt("missing v17 commit"))?;
            let c: Commit = postcard::from_bytes(&raw)?;
            if c.id != id
                || c.parents.len() > 2
                || c.parents.iter().any(|p| *p == NO_COMMIT || *p >= id)
                || (c.parents.len() == 2 && c.parents[0] == c.parents[1])
            {
                return Err(corrupt("invalid v17 commit graph"));
            }
            work.extend_from_slice(&c.parents);
            live.insert(id, c);
            hdr.next_commit = hdr.next_commit.max(
                id.raw()
                    .checked_add(1)
                    .ok_or_else(|| corrupt("commit IDs exhausted"))?,
            );
        }

        let roots: Vec<_> = branch_rows
            .values()
            .map(|s| s.dirty_root)
            .chain(live.values().map(|c| c.root))
            .collect();
        let converted = convert_objects(&mut store, &roots, &mut hdr.next_id)?;
        let mut batch = store.batch();
        for (id, s) in branch_rows {
            batch.insert(
                row(HEAD, id),
                BranchHead {
                    dirty_root: converted[&s.dirty_root],
                    head: s.head,
                }
                .bytes(),
            );
            batch.insert(row(NAME, id), s.name.as_bytes());
            batch.insert(by_name(&s.name), id.to_le_bytes());
        }
        for (id, mut c) in live {
            c.root = converted[&c.root];
            c.ref_count = refs[&id];
            batch.insert(row(COMMIT, id.raw()), c.bytes());
        }
        batch.insert(HEADER, hdr.bytes());
        batch.commit_sync()?;
    }

    // Old keys are untagged u64s; a v18 branch-name lookup can also be
    // eight bytes. Protect all live reverse-index keys during cleanup.
    let protected: HashSet<_> = store
        .range([NAME].as_slice()..[NAME + 1].as_slice())
        .filter(|(k, _)| k.len() == 9)
        .map(|(_, name)| {
            String::from_utf8(name)
                .map(|s| by_name(&s))
                .map_err(|_| corrupt("invalid migrated branch name"))
        })
        .collect::<Result<_>>()?;
    remove_old_nodes(&mut store, &protected)?;
    let meta = typed_handle::<VerMap<K, V>>(id, &[])?;
    let mut map: VerMap<K, V> = postcard::from_bytes(&meta)?;
    map.verify_and_gc()?;
    for raw in &mut retired {
        raw.batch_wiped().commit_sync()?;
    }
    save_instance_meta(id, &map)?;
    Ok(map)
}

fn convert_objects(
    store: &mut MapxRaw,
    roots: &[u64],
    next_id: &mut u64,
) -> Result<HashMap<u64, u64>> {
    let mut needed = HashSet::new();
    let mut work = roots.to_vec();
    while let Some(id) = work.pop() {
        if id == 0 || !needed.insert(id) {
            continue;
        }
        if let Node::Internal { children, .. } = legacy_node(store, id)? {
            if children.iter().any(|child| *child == 0 || *child >= id) {
                return Err(corrupt("invalid v17 B+ tree edge"));
            }
            work.extend(children);
        }
    }
    let mut needed: Vec<_> = needed.into_iter().collect();
    needed.sort_unstable();
    struct Span {
        first: Vec<u8>,
        last: Vec<u8>,
        height: u32,
        len: usize,
    }
    let mut spans = HashMap::<NodeId, Span>::new();
    let mut converted = HashMap::from([(0, 0)]);
    let mut objects = Vec::new();
    let mut size = 0;
    for old in needed {
        let mut node = legacy_node(store, old)?;
        // Validate routing and occupancy while all old rows are intact.
        let span = match &node {
            Node::Leaf { keys, .. } => Span {
                first: keys
                    .first()
                    .ok_or_else(|| corrupt("empty v17 leaf"))?
                    .clone(),
                last: keys.last().unwrap().clone(),
                height: 0,
                len: keys.len(),
            },
            Node::Internal { children, keys } => {
                if keys.is_empty() {
                    return Err(corrupt("empty v17 internal page"));
                }
                let first = &spans[&children[0]];
                let mut prev = first;
                for (sep, id) in keys.iter().zip(&children[1..]) {
                    let child = &spans[id];
                    if child.height != first.height
                        || prev.len < MAX_KEYS / 2
                        || child.len < MAX_KEYS / 2
                        || &prev.last >= sep
                        || &child.first < sep
                    {
                        return Err(corrupt("invalid v17 child span"));
                    }
                    prev = child;
                }
                Span {
                    first: first.first.clone(),
                    last: prev.last.clone(),
                    height: first.height + 1,
                    len: keys.len(),
                }
            }
        };
        spans.insert(old, span);
        let class = match &mut node {
            Node::Internal { children, .. } => {
                for child in children {
                    *child = converted[child];
                }
                0
            }
            Node::Leaf { values, .. } if values.iter().any(|v| v.len() > 1024) => BLOB_LEAF,
            Node::Leaf { .. } => LEAF,
        };
        let bytes = node.encode_with(|val| {
            let id = *next_id | BLOB;
            *next_id += 1;
            size += val.len() + 9;
            objects.push((object_key(id), val.to_vec()));
            id
        });
        let id = *next_id | class;
        *next_id += 1;
        converted.insert(old, id);
        size += bytes.len() + 9;
        objects.push((object_key(id), bytes));
        if size >= 16 << 20 {
            write_objects(store, &mut objects)?;
            size = 0;
        }
    }
    write_objects(store, &mut objects)?;
    Ok(converted)
}

fn remove_old_nodes(store: &mut MapxRaw, protected: &HashSet<Vec<u8>>) -> Result<()> {
    let mut old = Vec::new();
    let reader = store.reader();
    let view = reader.read_view();
    for (k, _) in view.range(..) {
        if k.len() == 8 && !protected.contains(&k) {
            old.push(k);
        }
        if old.len() >= 8192 {
            let mut batch = store.batch();
            for key in old.drain(..) {
                batch.remove(key);
            }
            batch.commit()?;
        }
    }
    let mut batch = store.batch();
    for key in old {
        batch.remove(key);
    }
    batch.commit_sync()?;

    Ok(())
}

/// Converts a standalone node pool and returns the new IDs corresponding to
/// the caller's complete list of live roots (including repeated owners).
/// Standalone pools have no persisted root table. Retrying with the original
/// root list also works after the v18 metadata has been saved.
pub fn migrate_tree_v17(
    id: impl Into<InstanceId>,
    roots: &[u64],
) -> Result<(vsdb::PersistentBTree, Vec<u64>)> {
    use vsdb::PersistentBTree;

    let id = id.into();
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
    let mut raw = crate::raw_map(id)?;
    if raw.namespace().is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "standalone tree migration",
        });
    }
    let path = raw.namespace().meta_path(id.map_id);
    let remap_file = path.with_extension("v17-roots");
    let frame_bytes = std::fs::read(&path)?;
    let modern = postcard::take_from_bytes::<&[u8]>(&frame_bytes)
        .ok()
        .is_some_and(|(frame, tail)| tail.is_empty() && frame.starts_with(b"VSTYPE04"));
    if !modern {
        let ((LegacyRaw(pool), _next), tail): ((LegacyRaw, u64), _) =
            postcard::take_from_bytes(&frame_bytes)?;
        if !tail.is_empty() || pool.instance_id() != id {
            return Err(corrupt("invalid standalone tree metadata"));
        }
    }
    let saved_mapping = match std::fs::read(&remap_file) {
        Ok(bytes) => Some(bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => return Err(err.into()),
    };
    #[derive(serde::Serialize, Deserialize)]
    struct RootMapping {
        instance: InstanceId,
        next: u64,
        original: Vec<u64>,
        converted: Vec<u64>,
    }
    let mapping = if let Some(data) = saved_mapping {
        use sha3::{Digest, Keccak256};
        let (payload, checksum) = data
            .split_at_checked(data.len().saturating_sub(32))
            .ok_or_else(|| corrupt("invalid root mapping"))?;
        if Keccak256::digest(payload).as_slice() != checksum {
            return Err(corrupt("damaged root mapping"));
        }
        let mapping: RootMapping = crate::exactly(payload)?;
        if mapping.instance != id || mapping.original.len() != mapping.converted.len() {
            return Err(corrupt("root mapping identity mismatch"));
        }
        mapping
    } else {
        if modern {
            return Err(corrupt("v18 tree has no v17 root mapping"));
        }
        let mut next = 1;
        let all = convert_objects(&mut raw, roots, &mut next)?;
        let mapping = RootMapping {
            instance: id,
            next,
            original: roots.to_vec(),
            converted: roots.iter().map(|r| all[r]).collect(),
        };
        raw.try_sync_wal()?;
        use sha3::{Digest, Keccak256};
        let mut payload = postcard::to_allocvec(&mapping)?;
        let checksum = Keccak256::digest(&payload);
        payload.extend_from_slice(&checksum);
        vsdb::common::atomic_write_file(&remap_file, &payload)?;
        mapping
    };
    let known: HashMap<_, _> = mapping
        .original
        .iter()
        .copied()
        .zip(mapping.converted.iter().copied())
        .collect();
    let remapped = roots
        .iter()
        .map(|r| {
            known
                .get(r)
                .copied()
                .ok_or_else(|| corrupt("unknown original root"))
        })
        .collect::<Result<Vec<_>>>()?;
    let next = mapping.next;
    if modern {
        return Ok((PersistentBTree::from_meta(id)?, remapped));
    }
    remove_old_nodes(&mut raw, &HashSet::new())?;
    let mut batch = raw.batch();
    batch.insert([9], next.to_le_bytes());
    batch.commit_sync()?;
    let bytes = typed_handle::<PersistentBTree>(id, &[])?;
    let mut tree: PersistentBTree = postcard::from_bytes(&bytes)?;
    tree.rebuild_ref_counts(&mapping.converted);
    tree.save_meta()?;
    Ok((tree, remapped))
}

fn write_objects(store: &mut MapxRaw, objects: &mut Vec<([u8; 9], Vec<u8>)>) -> Result<()> {
    if objects.is_empty() {
        return Ok(());
    }
    let mut batch = store.batch();
    for (k, v) in objects.drain(..) {
        batch.insert(k, v);
    }
    batch.commit()
}

fn legacy_id(raw: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        raw.try_into()
            .map_err(|_| corrupt("invalid v17 record key"))?,
    ))
}
fn legacy_node(store: &MapxRaw, id: NodeId) -> Result<Node> {
    let raw = store
        .get(id.to_le_bytes())
        .ok_or_else(|| corrupt("missing v17 B+ tree node"))?;
    let mut bytes = raw.as_slice();
    let tag = *bytes
        .split_off_first()
        .ok_or_else(|| corrupt("empty v17 node"))?;
    fn len(bytes: &mut &[u8]) -> Result<usize> {
        Ok(u32::from_le_bytes(
            bytes
                .split_off(..4)
                .ok_or_else(|| corrupt("invalid v17 field length"))?
                .try_into()
                .unwrap(),
        ) as usize)
    }
    fn field(bytes: &mut &[u8]) -> Result<Vec<u8>> {
        let n = len(bytes)?;
        Ok(bytes
            .split_off(..n)
            .ok_or_else(|| corrupt("truncated v17 node field"))?
            .to_vec())
    }
    let count = len(&mut bytes)?;
    if tag > 1 || count > MAX_KEYS {
        return Err(corrupt("invalid v17 B+ tree header"));
    }
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for _ in 0..count {
        keys.push(field(&mut bytes)?);
        if tag == 0 {
            values.push(field(&mut bytes)?);
        }
    }
    if keys.windows(2).any(|ks| ks[0] >= ks[1]) {
        return Err(corrupt("unsorted v17 B+ tree page"));
    }
    let node = if tag == 0 {
        Node::Leaf { keys, values }
    } else {
        let mut children = Vec::new();
        for _ in 0..=count {
            children.push(u64::from_le_bytes(
                bytes
                    .split_off(..8)
                    .ok_or_else(|| corrupt("truncated v17 child ID"))?
                    .try_into()
                    .unwrap(),
            ));
        }
        Node::Internal { keys, children }
    };
    if !bytes.is_empty() {
        return Err(corrupt("trailing v17 node bytes"));
    }
    Ok(node)
}
