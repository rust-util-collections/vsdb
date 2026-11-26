//! Offline, resumable conversion of v17's three-prefix node handles.

use crate::{
    BasicHandle, LegacyRaw, legacy_payload, raw_map, try_get, typed_handle, typed_handle_tag,
};
use serde::{
    Deserialize, Serialize,
    de::{self, DeserializeOwned},
};
use sha3::{Digest, Sha3_256};
use std::{
    any::type_name,
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};
use vsdb::{
    DagMapRaw, DagMapRawKey, InstanceId, MapxOrdRawKey, Namespace, Orphan, Result, ValueEnDe,
    VsdbError,
};
use vsdb_core::{MapxRaw, common::atomic_write_file};

/// Changed child prefix identities. Keys absent from the map retain their IDs.
pub type Remap = BTreeMap<InstanceId, InstanceId>;
fn corrupt(detail: &str) -> VsdbError {
    VsdbError::Corrupt {
        detail: detail.into(),
    }
}
fn open_prefix(ns: &Namespace, id: u64) -> Result<MapxRaw> {
    raw_map(InstanceId::new(id, ns.id()))
}

#[derive(Copy, Clone, Serialize, Deserialize)]
struct Parent {
    map: u64,
    edge: u64,
}
#[derive(Copy, Clone, Serialize, Deserialize)]
enum State {
    Root,
    Child(Parent),
    Clearing { owner: Option<Parent>, genesis: u64 },
    Destroying(Option<Parent>),
    Destroyed,
}
fn edge_key(edge: u64) -> [u8; 9] {
    let mut k = [1; 9];
    k[1..].copy_from_slice(&edge.to_be_bytes());
    k
}
fn tagged(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + bytes.len());
    v.push(tag);
    v.extend_from_slice(bytes);
    v
}

struct Legacy(MapxRaw, MapxRaw, MapxRaw);
impl Legacy {
    fn ids(&self) -> [InstanceId; 3] {
        [
            self.0.instance_id(),
            self.1.instance_id(),
            self.2.instance_id(),
        ]
    }
}
impl<'de> Deserialize<'de> for Legacy {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let (LegacyRaw(data), parent, children) = <(LegacyRaw, Vec<u8>, Vec<u8>)>::deserialize(d)?;
        let read = || -> Result<Self> {
            let parent = raw_map(Orphan::<Option<DagMapRaw>>::v17_id(&parent)?)?;
            let children = raw_map(MapxOrdRawKey::<DagMapRaw>::v17_id(&children)?)?;
            Ok(Self(data, parent, children))
        };
        read().map_err(de::Error::custom)
    }
}
fn decode<'a, T: Deserialize<'a>>(b: &'a [u8]) -> Result<T> {
    let (v, tail) = postcard::take_from_bytes(b)?;
    if !tail.is_empty() {
        return Err(corrupt("trailing DAG migration bytes"));
    }
    Ok(v)
}

#[derive(Serialize, Deserialize)]
struct Conversion {
    old: [u64; 3],
    target: u64,
    scratch: u64,
    tag: u64,
    state: State,
    children: Vec<(u64, u64)>,
}
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq)]
enum Phase {
    Prepare,
    Publish,
    Retire,
}
#[derive(Serialize, Deserialize)]
enum Record {
    Running {
        dag_id: u64,
        phase: Phase,
        nodes: Vec<Conversion>,
    },
    Done(Remap),
    Finished {
        dag_id: u64,
        members: BTreeMap<u64, Member>,
    },
}

#[derive(Serialize, Deserialize)]
struct Member {
    target: u64,
    // Older completed journals retained only relocated primary IDs.
    components: Option<[u64; 2]>,
}

// A running conversion can own empty destination/scratch prefixes before
// any shard row identifies them. A current raw frame in the journal lets
// ceiling reconstruction observe the highest such allocation on restart.
fn highest_reservation(root: InstanceId, record: &Record) -> [u8; 24] {
    let max = match record {
        Record::Running { nodes, .. } => nodes
            .iter()
            .flat_map(|n| n.old.into_iter().chain([n.scratch, n.target]))
            .max(),
        Record::Finished { members, .. } => members
            .iter()
            .flat_map(|(&old, m)| {
                [old, m.target]
                    .into_iter()
                    .chain(m.components.into_iter().flatten())
            })
            .max(),
        Record::Done(map) => map
            .iter()
            .flat_map(|(old, new)| [old.map_id, new.map_id])
            .max(),
    };
    crate::raw_frame(InstanceId {
        map_id: root.map_id.max(max.unwrap_or(root.map_id)),
        ns: root.ns,
    })
}
fn write_record(path: &Path, root: InstanceId, record: &Record) -> Result<()> {
    let mut bytes = postcard::to_allocvec(&(root, record, highest_reservation(root, record)))?;
    let digest = Sha3_256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    atomic_write_file(path, &bytes)?;
    Ok(())
}
fn read_record(path: &Path, root: InstanceId) -> Result<Option<Record>> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let n = bytes
        .len()
        .checked_sub(32)
        .ok_or_else(|| corrupt("truncated DAG migration journal"))?;
    let (payload, hash) = bytes.split_at(n);
    if Sha3_256::digest(payload).as_slice() != hash {
        return Err(corrupt("damaged DAG migration journal"));
    }
    let (saved, record, maximum): (InstanceId, Record, [u8; 24]) = decode(payload)?;
    if saved != root || maximum != highest_reservation(root, &record) {
        return Err(corrupt("DAG migration journal identity mismatch"));
    }
    Ok(Some(record))
}

fn saved_tag<T>(ns: &Namespace, parts: [InstanceId; 3], is_root: bool) -> Result<u64> {
    let default = crate::tag(type_name::<T>());
    let bytes = match fs::read(ns.meta_path(parts[0].map_id)) {
        Ok(v) => v,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(default),
        Err(e) => return Err(e.into()),
    };
    // A v17 raw DAG may have independently persisted typed descendants.
    // Preserve each saved envelope's value type, even when the root has a
    // different type. Authenticate its inner three handles before rewriting.
    let (saved, tag) = if let Ok(node) = decode::<Legacy>(&bytes) {
        (node, crate::tag(type_name::<DagMapRaw>()))
    } else {
        let frame: &[u8] = decode(&bytes)?;
        let Some(magic @ (b"VSTYPE02" | b"VSTYPE03")) = frame.get(..8) else {
            return Err(corrupt("invalid saved child metadata"));
        };
        if frame.len() < 16 {
            return Err(corrupt("invalid saved child metadata"));
        }
        let vstype02 = magic == b"VSTYPE02";
        let ambiguous_v02 =
            vstype02 && crate::legacy_tag_for::<T>(true) != crate::legacy_tag_for::<T>(false);
        let tag = u64::from_le_bytes(frame[8..16].try_into().unwrap());
        let tag = if tag == crate::legacy_tag_for::<T>(vstype02) && (is_root || !ambiguous_v02) {
            default
        } else if !vstype02 && tag == crate::legacy_tag_for::<DagMapRawKey<vsdb::BranchId>>(false) {
            crate::tag(type_name::<DagMapRawKey<vsdb::BranchId>>())
        } else if !vstype02 && tag == crate::legacy_tag_for::<DagMapRawKey<vsdb::CommitId>>(false) {
            crate::tag(type_name::<DagMapRawKey<vsdb::CommitId>>())
        } else {
            tag
        };
        (decode::<Legacy>(&frame[16..])?, tag)
    };
    if saved.ids() != parts {
        return Err(corrupt("saved DAG metadata names different components"));
    }
    Ok(tag)
}

fn plan<T>(ns: &Namespace, root: InstanceId, legacy: Legacy) -> Result<Vec<Conversion>> {
    if legacy.0.instance_id() != root {
        return Err(corrupt("v17 DAG root identity mismatch"));
    }
    let anchor = open_prefix(ns, root.map_id)?;
    struct Source {
        parts: [InstanceId; 3],
        owner: Option<[InstanceId; 3]>,
        clearing: Option<[InstanceId; 3]>,
        destroying: Option<[InstanceId; 3]>,
        children: Vec<(u64, u64)>,
    }
    let root_parts = legacy.ids();
    let mut pending = vec![legacy];
    let mut sources = HashMap::<u64, Source>::new();
    let mut components = HashSet::new();
    let mut reverse = HashMap::<u64, Vec<Parent>>::new();
    while let Some(node) = pending.pop() {
        let parts = node.ids();
        let id = parts[0].map_id;
        if let Some(known) = sources.get(&id) {
            if known.parts != parts {
                return Err(corrupt("inconsistent v17 DAG node handles"));
            }
            continue;
        }
        if parts
            .iter()
            .any(|p| p.ns != root.ns || !components.insert(p.map_id))
        {
            return Err(corrupt("shared or foreign v17 DAG components"));
        }
        let decode_link = |row: &[u8]| -> Result<Option<[InstanceId; 3]>> {
            Ok(decode::<Option<Legacy>>(row)?.map(|link| link.ids()))
        };
        let owner =
            decode_link(&try_get(&node.1, [])?.ok_or_else(|| corrupt("missing v17 parent slot"))?)?;
        let marker = |key| -> Result<Option<[InstanceId; 3]>> {
            match try_get(&node.1, [key])? {
                Some(v) => Ok(Some(
                    decode_link(&v)?.ok_or_else(|| corrupt("empty v17 recovery marker"))?,
                )),
                None => Ok(None),
            }
        };
        let clearing = marker(1)?;
        let destroying = marker(2)?;
        let mut unexpected_marker = false;
        let mut meta_rows = node.1.try_range(..)?;
        while let Some((k, _)) = meta_rows.try_step_next()? {
            if !matches!(k, [] | [1] | [2]) {
                unexpected_marker = true;
                break;
            }
        }
        drop(meta_rows);
        if clearing.is_some_and(|g| g != root_parts) || unexpected_marker {
            return Err(corrupt("invalid v17 DAG recovery metadata"));
        }
        let mut children = Vec::new();
        let mut child_rows = node.2.try_range(..)?;
        while let Some((k, v)) = child_rows.try_step_next()? {
            let e = u128::from_le_bytes(
                k.try_into()
                    .map_err(|_| corrupt("invalid v17 child token"))?,
            );
            let edge = u64::try_from(e)
                .ok()
                .filter(|e| *e < u64::MAX)
                .ok_or_else(|| corrupt("v17 DAG edge exceeds u64"))?;
            let child: Legacy = decode(v)?;
            let target = child.0.instance_id().map_id;
            reverse
                .entry(target)
                .or_default()
                .push(Parent { map: id, edge });
            children.push((edge, target));
            pending.push(child);
        }
        drop(child_rows);
        sources.insert(
            id,
            Source {
                parts,
                owner,
                clearing,
                destroying,
                children,
            },
        );
    }

    // Verify ownership before allocating fresh prefixes. An interrupted
    // reparent can leave duplicate registry entries (or just the old index
    // entry) for the same edge. The child's parent slot selects the owner.
    let mut states = HashMap::new();
    let mut missing_edges = Vec::new();
    let mut parents = HashMap::new();
    for (&id, node) in &sources {
        if id == root.map_id {
            if node.owner.is_some()
                || node.clearing.is_some()
                || node.destroying.is_some()
                || reverse.contains_key(&id)
            {
                return Err(corrupt("requested v17 node is not a graph root"));
            }
            states.insert(id, State::Root);
            continue;
        }
        let indices = reverse
            .get(&id)
            .ok_or_else(|| corrupt("unreachable v17 child"))?;
        let owner = if let Some(parts) = node.destroying.or(node.owner) {
            let recorded = parts[0].map_id;
            if sources.get(&recorded).is_none_or(|n| n.parts != parts) {
                return Err(corrupt("foreign v17 parent"));
            }
            if let Some(link) = indices.iter().find(|p| p.map == recorded) {
                *link
            } else if recorded == root.map_id && indices.len() == 1 {
                // The child flip reached the WAL before its root registry.
                let link = Parent {
                    map: recorded,
                    edge: indices[0].edge,
                };
                missing_edges.push((id, link));
                link
            } else {
                return Err(corrupt("v17 parent registry mismatch"));
            }
        } else if node.clearing.is_some() && indices.len() == 1 {
            indices[0]
        } else {
            return Err(corrupt(
                "ambiguous parentless v17 child; finish the interrupted v17 cleanup",
            ));
        };
        if indices.iter().any(|p| p.edge != owner.edge) {
            return Err(corrupt("conflicting v17 edge tokens"));
        }
        parents.insert(id, owner.map);
        let state = if node.destroying.is_some() {
            State::Destroying(Some(owner))
        } else if node.clearing.is_some() {
            State::Clearing {
                owner: Some(owner),
                genesis: root.map_id,
            }
        } else {
            State::Child(owner)
        };
        states.insert(id, state);
    }
    let mut verified = HashSet::from([root.map_id]);
    for id in sources.keys().copied() {
        let mut path = HashSet::new();
        let mut cur = id;
        while !verified.contains(&cur) {
            if !path.insert(cur) {
                return Err(corrupt("cyclic v17 owner chain"));
            }
            cur = parents[&cur];
        }
        verified.extend(path);
    }
    for (id, link) in missing_edges {
        let children = &mut sources
            .get_mut(&link.map)
            .expect("verified parent")
            .children;
        if children.iter().any(|(edge, _)| *edge == link.edge) {
            return Err(corrupt("v17 reparent edge collision"));
        }
        children.push((link.edge, id));
    }
    let allocate_colocated = || -> Result<u64> {
        loop {
            let candidate = MapxRaw::new_in(ns);
            let id = candidate.try_instance_id()?;
            if candidate.is_colocated_with(&anchor) {
                return Ok(id.map_id);
            }
        }
    };
    let mut targets = HashMap::new();
    for &id in sources.keys() {
        let colocated = open_prefix(ns, id)?.is_colocated_with(&anchor);
        targets.insert(id, if colocated { id } else { allocate_colocated()? });
    }
    let remap = |mut p: Parent| {
        p.map = targets[&p.map];
        p
    };
    let mut nodes = Vec::with_capacity(sources.len());
    for (id, source) in sources {
        let state = match states[&id] {
            State::Root => State::Root,
            State::Destroyed => return Err(corrupt("unexpected destroyed v17 DAG node")),
            State::Child(p) => State::Child(remap(p)),
            State::Clearing { owner, genesis } => State::Clearing {
                owner: owner.map(remap),
                genesis: targets[&genesis],
            },
            State::Destroying(owner) => State::Destroying(owner.map(remap)),
        };
        nodes.push(Conversion {
            old: source.parts.map(|p| p.map_id),
            target: targets[&id],
            scratch: allocate_colocated()?,
            tag: saved_tag::<T>(ns, source.parts, id == root.map_id)?,
            state,
            children: source
                .children
                .into_iter()
                .map(|(edge, child)| (edge, targets[&child]))
                .collect(),
        });
    }
    Ok(nodes)
}

// Each pass can start over from its immutable source after interruption.
// Bound memory by bytes and count even for graphs with payloads larger than
// the engine's single-batch limit.
fn copy_rows(source: &MapxRaw, target: &mut MapxRaw, tag: bool) -> Result<()> {
    target.batch_wiped().commit()?;
    let mut rows = source.try_range(..)?;
    while let Some((first_k, first_v)) = rows.try_step_next()? {
        let mut batch = target.batch();
        let mut size = 0;
        let mut count = 0usize;
        let mut cur = Some((first_k, first_v));
        while let Some((k, v)) = cur {
            size += k.len() + v.len() + 2;
            count += 1;
            if tag {
                let v = if v.is_empty() { vec![0] } else { tagged(1, v) };
                batch.insert(tagged(2, k), v);
            } else {
                batch.insert(k, v);
            }
            if count >= 4096 || size >= 4 << 20 {
                break;
            }
            cur = rows.try_step_next()?;
        }
        let exhausted = cur.is_none();
        batch.commit()?;
        if exhausted {
            break;
        }
    }
    Ok(())
}
// v17 reserved 128-bit edge tokens across the entire universe. All migrated
// parents must start above that ceiling, including parents with no live edges.
fn next_legacy_edge() -> Result<u64> {
    let system = vsdb_core::vsdb_get_system_dir();
    let read = |name| -> Result<Option<Vec<u8>>> {
        match fs::read(system.join(name)) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    };
    let mut ceiling = match read("dag_id_ceiling")? {
        Some(v) => u128::from_le_bytes(
            v.try_into()
                .map_err(|_| corrupt("invalid v17 DAG edge ceiling"))?,
        ),
        None => 0,
    };
    // v17 accepted an older Orphan<u128> counter and folded it into the file
    // on its first allocation. A universe may be upgraded before that happens.
    if let Some(v) = read("id_num")? {
        let prefix: &[u8] = decode(&v)?;
        let id = u64::from_le_bytes(
            prefix
                .try_into()
                .map_err(|_| corrupt("invalid legacy DAG counter handle"))?,
        );
        let counter = try_get(&raw_map(id.into())?, [])?
            .ok_or_else(|| corrupt("missing legacy DAG counter"))?;
        ceiling = ceiling.max(decode(&counter)?);
    }
    u64::try_from(ceiling)
        .ok()
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| corrupt("v17 DAG edge ceiling exhausts v18 tokens"))
}

fn node_meta(store: &MapxRaw) -> Result<(u64, u64, State)> {
    let bytes = try_get(store, [0])?.ok_or_else(|| corrupt("missing converted DAG node"))?;
    let (magic, dag, next, state): ([u8; 8], u64, u64, State) = decode(&bytes)?;
    if magic != *b"VSDAGN01" {
        return Err(corrupt("unexpected converted DAG node layout"));
    }
    Ok((dag, next, state))
}

fn handle<T: DeserializeOwned>(id: InstanceId, dag_id: u64) -> Result<Vec<u8>> {
    let mut extra = [1u8; 9];
    extra[1..].copy_from_slice(&dag_id.to_le_bytes());
    let bytes = typed_handle::<T>(id, &extra)?;
    let _: T = decode(&bytes)?;
    Ok(bytes)
}

fn source_edge_floor(nodes: &[Conversion]) -> Result<u64> {
    let floor = next_legacy_edge()?;
    if floor == 1 && nodes.iter().any(|n| !n.children.is_empty()) {
        return Err(corrupt(
            "v17 DAG with registered children lacks its edge allocator ceiling",
        ));
    }
    Ok(floor)
}

fn prepare(ns: &Namespace, nodes: &[Conversion], dag_id: u64, next: u64) -> Result<()> {
    for n in nodes {
        let source = open_prefix(ns, n.old[0])?;
        let mut scratch = open_prefix(ns, n.scratch)?;
        copy_rows(&source, &mut scratch, true)?;
        let state = n.state;
        let mut next_edge_id = next;
        for chunk in n.children.chunks(4096) {
            let mut batch = scratch.batch();
            for &(edge, child) in chunk {
                batch.insert(edge_key(edge), child.to_le_bytes());
                next_edge_id = next_edge_id.max(edge + 1);
            }
            batch.commit()?;
        }
        let mut batch = scratch.batch();
        batch.insert(
            [0],
            postcard::to_allocvec(&(*b"VSDAGN01", dag_id, next_edge_id, state))?,
        );
        batch.commit()?;
    }
    open_prefix(ns, nodes[0].scratch)?.try_sync_wal()
}

/// Converts a v17 raw DAG under exclusive access, preserving the root prefix.
/// Returns the converted root and all changed descendant identities. Retrying
/// resumes from the journal stored beside the root's instance metadata.
pub fn migrate_v17(id: impl Into<InstanceId>) -> Result<(DagMapRaw, Remap)> {
    let root = id.into();
    let members = migrate_with::<DagMapRaw>(root, None, |b| decode(b))?;
    Ok((
        DagMapRaw::from_meta(root)?,
        members.map_or_else(Remap::new, |m| m.remap()),
    ))
}

/// Converts a postcard-serialized v17 `DagMapRaw` root handle (and its
/// connected DAG) without requiring a pre-existing `__instance_meta__` file.
pub fn migrate_handle_v17(bytes: &[u8]) -> Result<(Vec<u8>, Remap)> {
    migrate_bytes_with::<DagMapRaw>(bytes, |b| decode(b), |d| d.instance_id())
}

/// Converts a v17 typed-value DAG. Previously saved child metadata retains
/// its value type. Unsaved descendants use the root type.
pub fn migrate_rawkey_v17<V: ValueEnDe>(
    id: impl Into<InstanceId>,
) -> Result<(DagMapRawKey<V>, Remap)> {
    let root = id.into();
    let members = migrate_with::<DagMapRawKey<V>>(root, None, typed_legacy::<V>)?;
    Ok((
        DagMapRawKey::from_meta(root)?,
        members.map_or_else(Remap::new, |m| m.remap()),
    ))
}

/// Converts a postcard-serialized v17 `DagMapRawKey<V>` root handle (and its
/// connected DAG) without requiring a pre-existing `__instance_meta__` file.
pub fn migrate_rawkey_handle_v17<V: ValueEnDe>(bytes: &[u8]) -> Result<(Vec<u8>, Remap)> {
    migrate_bytes_with::<DagMapRawKey<V>>(bytes, typed_legacy::<V>, |d| d.instance_id())
}

/// Re-tags a migrated `DagMapRawKey<V>` descendant whose saved metadata had a
/// different value type `V` from the DAG root and contained a renamed library
/// type (`BranchId`, `CommitId`, or `VecDexDyn`). Pass the relocated child ID
/// from `Remap` (or the original child ID if absent from `Remap`).
pub fn migrate_child_v17<V: ValueEnDe>(id: impl Into<InstanceId>) -> Result<DagMapRawKey<V>> {
    let id = id.into();
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
    let ns = Namespace::open(id.ns.unwrap_or_default())?;
    if ns.is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "DAG child metadata migration",
        });
    }
    let path = ns.meta_path(id.map_id);
    let bytes = fs::read(&path)?;
    let frame: &[u8] = decode(&bytes)?;
    if frame.len() != 45
        || &frame[..8] != b"VSTYPE04"
        || u64::from_le_bytes(frame[16..24].try_into().unwrap()) != id.map_id
        || u64::from_le_bytes(frame[24..32].try_into().unwrap()) != ns.id()
        || u32::from_le_bytes(frame[32..36].try_into().unwrap()) != 9
        || frame[36] != 1
    {
        return Err(corrupt("invalid migrated DAG child handle"));
    }
    let found = u64::from_le_bytes(frame[8..16].try_into().unwrap());
    let expected = crate::tag(type_name::<DagMapRawKey<V>>());
    if found == expected {
        crate::sync_parent(&path)?;
        return DagMapRawKey::<V>::from_meta(id);
    }
    if found != crate::legacy_tag_for::<DagMapRawKey<V>>(false)
        && found != crate::legacy_tag_for::<DagMapRawKey<V>>(true)
    {
        return Err(corrupt("saved DAG child type mismatch"));
    }
    let target = typed_handle::<DagMapRawKey<V>>(id, &frame[36..45])?;
    let node: DagMapRawKey<V> = decode(&target)?;
    atomic_write_file(&path, &target)?;
    Ok(node)
}
fn typed_legacy<V: ValueEnDe>(bytes: &[u8]) -> Result<Legacy> {
    let frame: &[u8] = decode(bytes)?;
    decode(legacy_payload::<DagMapRawKey<V>>(frame)?)
}

/// Translates serialized v17 handles for members of a previously converted
/// graph. Open this once for a group of application records. Each translated
/// handle keeps its own authenticated value type, independently of saved metadata.
pub struct DagHandleConverter {
    root: InstanceId,
    dag_id: u64,
    members: BTreeMap<u64, Member>,
}

impl DagHandleConverter {
    /// Loads the durable mapping after the root converter succeeds.
    pub fn open(root: impl Into<InstanceId>) -> Result<Self> {
        let id = root.into();
        let root = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
        let ns = Namespace::open(root.ns.unwrap_or_default())?;
        let path = journal_path(&ns, root)?;
        match read_record(&path, root)? {
            Some(Record::Finished { dag_id, members }) => Ok(Self {
                root,
                dag_id,
                members,
            }),
            _ => Err(corrupt("run the v17 DAG root converter first")),
        }
    }

    /// Translates a postcard-serialized raw root or child handle.
    pub fn raw_handle(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        self.translate::<DagMapRaw>(bytes, |b| decode(b), DagMapRaw::instance_id)
    }

    /// Translates a postcard-serialized typed-value root or child handle.
    pub fn rawkey_handle<V: ValueEnDe>(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        self.translate::<DagMapRawKey<V>>(bytes, typed_legacy::<V>, DagMapRawKey::instance_id)
    }

    fn translate<T: DeserializeOwned>(
        &self,
        bytes: &[u8],
        parse: impl Fn(&[u8]) -> Result<Legacy>,
        instance_of: impl Fn(&T) -> InstanceId,
    ) -> Result<Vec<u8>> {
        if current_handle(bytes) {
            let id = instance_of(&decode(bytes)?);
            if id.ns != self.root.ns || handle::<T>(id, self.dag_id)? != bytes {
                return Err(corrupt("handle belongs to a different DAG"));
            }
            return Ok(bytes.to_vec());
        }
        let [data, parent, edges] = parse(bytes)?.ids();
        if [data, parent, edges].iter().any(|p| p.ns != self.root.ns) {
            return Err(corrupt("v17 DAG node belongs to another namespace"));
        }
        let target = match self.members.get(&data.map_id) {
            Some(m)
                if m.components
                    .is_none_or(|parts| parts == [parent.map_id, edges.map_id]) =>
            {
                m.target
            }
            Some(_) => return Err(corrupt("v17 DAG node components do not match migration")),
            None if self
                .members
                .get(&self.root.map_id)
                .is_some_and(|root| root.components.is_none()) =>
            {
                // A pre-membership journal lists only relocated IDs. A retained,
                // now-detached node can still prove its original graph and shard.
                if !raw_map(self.root)?.is_colocated_with(&raw_map(data)?) {
                    return Err(corrupt("v17 DAG node belongs to another shard"));
                }
                data.map_id
            }
            None => return Err(corrupt("unknown v17 DAG node")),
        };
        handle::<T>(
            InstanceId::new(target, self.root.ns.unwrap_or_default()),
            self.dag_id,
        )
    }

    fn remap(&self) -> Remap {
        let ns = self.root.ns.unwrap_or_default();
        self.members
            .iter()
            .filter(|(old, m)| **old != m.target)
            .map(|(&old, m)| (InstanceId::new(old, ns), InstanceId::new(m.target, ns)))
            .collect()
    }
}

fn current_handle(bytes: &[u8]) -> bool {
    matches!(postcard::take_from_bytes::<&[u8]>(bytes), Ok((frame, b"")) if frame.starts_with(b"VSTYPE04"))
}

fn migrate_bytes_with<T: DeserializeOwned>(
    bytes: &[u8],
    parse: impl Fn(&[u8]) -> Result<Legacy>,
    instance_of: impl Fn(&T) -> InstanceId,
) -> Result<(Vec<u8>, Remap)> {
    let root = if current_handle(bytes) {
        instance_of(&decode(bytes)?)
    } else {
        parse(bytes)?.0.instance_id()
    };
    let mapping = migrate_with::<T>(root, Some(bytes), &parse)?;
    if let Some(mapping) = mapping {
        let translated = mapping.translate::<T>(bytes, parse, instance_of)?;
        Ok((translated, mapping.remap()))
    } else {
        Ok((bytes.to_vec(), Remap::new()))
    }
}

// Older running journals may belong to another serialized flavor of this
// root, even when an interrupted publish has already wiped its data rows.
fn legacy_journals(ns: &Namespace, root: InstanceId) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(ns.meta_dir()) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let prefix = format!("{:016x}.dag-", root.map_id);
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .and_then(|s| s.strip_prefix(&prefix))
            .and_then(|s| s.strip_suffix("-v17"))
            .is_some_and(|tag| tag.len() == 16 && tag.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            paths.push(entry.path());
        }
    }
    Ok(paths)
}

fn journal_path(ns: &Namespace, root: InstanceId) -> Result<PathBuf> {
    let canonical = ns.meta_path(root.map_id).with_extension("dag-v17");
    if canonical.try_exists()? {
        return Ok(canonical);
    }
    match &*legacy_journals(ns, root)? {
        [] => Ok(canonical),
        [old] => Ok(old.clone()),
        _ => Err(corrupt(
            "conflicting legacy migration journals for DAG root",
        )),
    }
}

fn migrate_with<T: DeserializeOwned>(
    root: InstanceId,
    source: Option<&[u8]>,
    parse: impl Fn(&[u8]) -> Result<Legacy>,
) -> Result<Option<DagHandleConverter>> {
    let root = InstanceId::new(root.map_id, root.ns.unwrap_or_default());
    let ns = Namespace::open(root.ns.unwrap_or_default())?;
    if ns.is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "DAG migration",
        });
    }
    let path = ns.meta_path(root.map_id);
    let previous = journal_path(&ns, root)?;
    let journal = path.with_extension("dag-v17");
    let mut record = match read_record(&previous, root)? {
        Some(r) => {
            crate::sync_parent(&previous)?;
            r
        }
        None => {
            let owned;
            let bytes = match source {
                Some(b) => b,
                None => {
                    owned = fs::read(&path)?;
                    &owned
                }
            };
            if current_handle(bytes) {
                let _: T = decode(bytes)?;
                if source.is_none() {
                    crate::sync_parent(&path)?;
                }
                return Ok(None);
            }
            let nodes = plan::<T>(&ns, root, parse(bytes)?)?;
            vsdb::common::create_dir_all_durable(&ns.meta_dir())?;
            let r = Record::Running {
                dag_id: rand::random(),
                phase: Phase::Prepare,
                nodes,
            };
            write_record(&journal, root, &r)?;
            r
        }
    };
    if let Record::Running { nodes, .. } = &record {
        let requested = nodes
            .iter()
            .find(|n| n.old[0] == root.map_id && n.target == root.map_id)
            .ok_or_else(|| corrupt("migration journal lacks its root"))?;
        match source {
            Some(bytes) if !current_handle(bytes) => {
                let ids = parse(bytes)?.ids();
                if ids.iter().any(|id| id.ns != root.ns) || requested.old != ids.map(|id| id.map_id)
                {
                    return Err(corrupt(
                        "root handle differs from the running DAG migration",
                    ));
                }
            }
            None if requested.tag != crate::tag(type_name::<T>()) => {
                return Err(corrupt("saved root type differs from requested DAG handle"));
            }
            _ => {}
        }
    }
    let floor = if let Record::Running { nodes, .. } = &record {
        source_edge_floor(nodes)?
    } else {
        1
    };
    if previous != journal {
        // Keep the full original plan durable throughout an upgrade of the
        // old type-specific journal, including a crash during final cleanup.
        write_record(&journal, root, &record)?;
    }
    loop {
        if let Record::Done(map) = &record {
            if source.is_none() {
                let _: T = decode(&fs::read(&path)?)?;
            }
            record = repair_completed(&ns, root, map)?;
            write_record(&journal, root, &record)?;
        }
        let Record::Running {
            dag_id,
            phase,
            nodes,
        } = &mut record
        else {
            let Record::Finished { dag_id, members } = record else {
                unreachable!()
            };
            return Ok(Some(DagHandleConverter {
                root,
                dag_id,
                members,
            }));
        };
        match phase {
            Phase::Prepare => {
                prepare(&ns, nodes, *dag_id, floor)?;
                *phase = Phase::Publish;
            }
            Phase::Publish => {
                for n in nodes.iter() {
                    copy_rows(
                        &open_prefix(&ns, n.scratch)?,
                        &mut open_prefix(&ns, n.target)?,
                        false,
                    )?;
                }
                open_prefix(&ns, root.map_id)?.try_sync_wal()?;
                for n in nodes.iter() {
                    let id = InstanceId::new(n.target, ns.id());
                    let mut extra = [1; 9];
                    extra[1..].copy_from_slice(&dag_id.to_le_bytes());
                    // Validate the on-disk NodeMeta before publishing any
                    // value-typed envelope over it.
                    let _: DagMapRaw = decode(&typed_handle::<DagMapRaw>(id, &extra)?)?;
                    let handle = typed_handle_tag(n.tag, id, &extra)?;
                    atomic_write_file(&ns.meta_path(n.target), &handle)?;
                }
                *phase = Phase::Retire;
            }
            Phase::Retire => {
                // Old running journals may have been prepared by an earlier converter.
                let mut members = BTreeMap::new();
                for n in nodes.iter() {
                    let mut target = open_prefix(&ns, n.target)?;
                    raise_edge_floor(&mut target, *dag_id, floor)?;
                    members.insert(
                        n.old[0],
                        Member {
                            target: n.target,
                            components: Some([n.old[1], n.old[2]]),
                        },
                    );
                }
                let requested = handle::<T>(root, *dag_id)?;
                if source.is_none() && fs::read(&path)? != requested {
                    return Err(corrupt(
                        "published root sidecar differs from requested DAG handle",
                    ));
                }
                open_prefix(&ns, root.map_id)?.try_sync_wal()?;
                for n in nodes.iter() {
                    for old in n
                        .old
                        .into_iter()
                        .chain([n.scratch])
                        .filter(|id| *id != n.target)
                    {
                        open_prefix(&ns, old)?.batch_wiped().commit_sync()?;
                    }
                }
                // Earlier v18 converters use this record to recover relocated IDs.
                // Write it before the canonical completion record. An interrupted
                // retry still has the full plan in the canonical Retire journal.
                let remap = nodes
                    .iter()
                    .filter(|n| n.old[0] != n.target)
                    .map(|n| {
                        (
                            InstanceId::new(n.old[0], ns.id()),
                            InstanceId::new(n.target, ns.id()),
                        )
                    })
                    .collect();
                let done = Record::Done(remap);
                let saved_tag = nodes
                    .iter()
                    .find(|n| n.old[0] == root.map_id)
                    .ok_or_else(|| corrupt("missing migrated root"))?
                    .tag;
                let mut compat = legacy_journals(&ns, root)?;
                for tag in [saved_tag, crate::tag(type_name::<T>())] {
                    compat.push(path.with_extension(format!("dag-{tag:016x}-v17")));
                }
                compat.sort();
                compat.dedup();
                for old in compat {
                    write_record(&old, root, &done)?;
                }
                record = Record::Finished {
                    dag_id: *dag_id,
                    members,
                };
            }
        }
        write_record(&journal, root, &record)?;
    }
}

fn raise_edge_floor(store: &mut MapxRaw, dag_id: u64, floor: u64) -> Result<()> {
    let (found, next, state) = node_meta(store)?;
    if found != dag_id {
        return Err(corrupt("converted DAG identity mismatch"));
    }
    if next < floor {
        let mut batch = store.batch();
        batch.insert(
            [0],
            postcard::to_allocvec(&(*b"VSDAGN01", dag_id, floor, state))?,
        );
        batch.commit()?;
    }
    Ok(())
}

fn repair_completed(ns: &Namespace, root: InstanceId, map: &Remap) -> Result<Record> {
    let anchor = open_prefix(ns, root.map_id)?;
    let (dag_id, _, _) = node_meta(&anchor)?;
    let floor = next_legacy_edge()?;
    let mut members = BTreeMap::new();
    let reverse: HashMap<_, _> = map
        .iter()
        .map(|(old, new)| (new.map_id, old.map_id))
        .collect();
    let mut seen = HashSet::new();
    let mut pending = vec![root.map_id];
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        let mut store = open_prefix(ns, id)?;
        if !store.is_colocated_with(&anchor) {
            return Err(corrupt("converted DAG routes to another shard"));
        }
        raise_edge_floor(&mut store, dag_id, floor)?;
        members.insert(
            *reverse.get(&id).unwrap_or(&id),
            Member {
                target: id,
                components: None,
            },
        );
        let mut edges = store.try_range([1]..[2])?;
        while let Some((_, value)) = edges.try_step_next()? {
            pending.push(u64::from_le_bytes(
                value
                    .try_into()
                    .map_err(|_| corrupt("invalid migrated edge"))?,
            ));
        }
    }
    anchor.try_sync_wal()?;
    // Moved nodes may have been removed from the graph since migration.
    for (old, new) in map {
        members.entry(old.map_id).or_insert(Member {
            target: new.map_id,
            components: None,
        });
    }
    Ok(Record::Finished { dag_id, members })
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{env, process::Command};
    type Rows = Vec<(Vec<u8>, Vec<u8>)>;
    #[derive(Deserialize)]
    struct Fixture(
        Vec<u8>,
        Vec<Vec<u8>>,
        Vec<Vec<u8>>,
        [u64; 4],
        Vec<(Vec<u8>, Rows)>,
    );

    #[test]
    fn resumes_interrupted_migration() {
        let Ok(cut) = env::var("VSDB_TEST_DAG_MIGRATION_CUT") else {
            for cut in ["prepare", "publish", "publish_other", "retire", "done"] {
                let dir =
                    env::temp_dir().join(format!("vsdb-dag-resume-{}", rand::random::<u128>()));
                for action in ["seed", "resume"] {
                    let out = Command::new(env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "dagmap::test::resumes_interrupted_migration",
                            "--nocapture",
                        ])
                        .env("VSDB_TEST_DAG_MIGRATION_CUT", cut)
                        .env("VSDB_TEST_DAG_MIGRATION_DIR", &dir)
                        .env("VSDB_TEST_DAG_MIGRATION_ACTION", action)
                        .output()
                        .unwrap();
                    assert!(out.status.success(), "{cut} {action}: {out:?}");
                }
                fs::remove_dir_all(dir).unwrap();
            }
            return;
        };
        let dir = std::path::PathBuf::from(env::var_os("VSDB_TEST_DAG_MIGRATION_DIR").unwrap());
        vsdb::vsdb_configure(vsdb::VsdbOptions::new(&dir)).unwrap();
        let Fixture(bytes, root_tokens, a_tokens, ids, components) =
            decode(include_bytes!("../tests/fixtures/dagmap-v17.postcard")).unwrap();

        let ns = Namespace::default_ns();
        let root = InstanceId::new(ids[0], ns.id());
        if env::var("VSDB_TEST_DAG_MIGRATION_ACTION").unwrap() == "seed" {
            let _issued = MapxRaw::new().instance_id();
            for (raw, rows) in components {
                let LegacyRaw(mut store) = decode(&raw).unwrap();
                for (k, v) in rows {
                    store.insert(k, v);
                }
            }
            let path = ns.meta_path(root.map_id);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, &bytes).unwrap();
            let nodes = plan::<DagMapRaw>(&ns, root, decode(&bytes).unwrap()).unwrap();
            let dag_id = rand::random();
            let phase = match cut.as_str() {
                "prepare" => {
                    open_prefix(&ns, nodes[0].scratch)
                        .unwrap()
                        .insert(b"partial", b"scratch");
                    Phase::Prepare
                }
                "publish" | "publish_other" => {
                    prepare(&ns, &nodes, dag_id, 1).unwrap();
                    // Source root already overwritten; its original rows now
                    // survive only in the checkpointed tagged scratch copy.
                    let mut target = open_prefix(&ns, root.map_id).unwrap();
                    target.clear();
                    target.insert(b"partial", b"published");
                    Phase::Publish
                }
                "retire" | "done" => {
                    prepare(&ns, &nodes, dag_id, 1).unwrap();
                    for n in &nodes {
                        copy_rows(
                            &open_prefix(&ns, n.scratch).unwrap(),
                            &mut open_prefix(&ns, n.target).unwrap(),
                            false,
                        )
                        .unwrap();
                        let id = InstanceId::new(n.target, ns.id());
                        let mut extra = [1; 9];
                        extra[1..].copy_from_slice(&dag_id.to_le_bytes());
                        atomic_write_file(
                            &ns.meta_path(n.target),
                            &typed_handle::<DagMapRaw>(id, &extra).unwrap(),
                        )
                        .unwrap();
                    }
                    open_prefix(&ns, nodes[0].old[1]).unwrap().clear();
                    open_prefix(&ns, nodes[0].scratch).unwrap().clear();
                    Phase::Retire
                }
                _ => panic!("unknown cut"),
            };
            let journal = path.with_extension(format!(
                "dag-{:016x}-v17",
                crate::tag(type_name::<DagMapRaw>())
            ));
            let record = if cut == "done" {
                Record::Done(
                    nodes
                        .iter()
                        .filter(|n| n.old[0] != n.target)
                        .map(|n| {
                            (
                                InstanceId::new(n.old[0], ns.id()),
                                InstanceId::new(n.target, ns.id()),
                            )
                        })
                        .collect(),
                )
            } else {
                Record::Running {
                    dag_id,
                    phase,
                    nodes,
                }
            };
            write_record(&journal, root, &record).unwrap();
            // The earlier converter could already have published parents with
            // next_edge_id below the persisted v17 allocator ceiling.
            fs::write(
                vsdb_core::vsdb_get_system_dir().join("dag_id_ceiling"),
                128u128.to_le_bytes(),
            )
            .unwrap();
            ns.try_sync_wal().unwrap();
            let record = read_record(&journal, root).unwrap().unwrap();
            fs::write(
                dir.join("expected-highest"),
                &highest_reservation(root, &record)[8..16],
            )
            .unwrap();
            fs::remove_file(ns.system_dir().join("__prefix_ceiling__")).unwrap();
            return;
        }
        let fresh = MapxRaw::new().instance_id();
        let expected = u64::from_le_bytes(
            fs::read(dir.join("expected-highest"))
                .unwrap()
                .try_into()
                .unwrap(),
        );
        assert!(fresh.map_id > expected);
        if cut == "publish_other" {
            let path = ns.meta_path(root.map_id).with_extension(format!(
                "dag-{:016x}-v17",
                crate::tag(type_name::<DagMapRaw>())
            ));
            assert!(migrate_rawkey_v17::<Vec<u8>>(root).is_err());
            let Some(Record::Running {
                dag_id,
                phase: Phase::Publish,
                ..
            }) = read_record(&path, root).unwrap()
            else {
                panic!("wrong-flavor ID conversion changed the journal")
            };
            let mut frame = b"VSTYPE03".to_vec();
            frame.extend_from_slice(
                &crate::legacy_tag_for::<DagMapRawKey<Vec<u8>>>(false).to_le_bytes(),
            );
            frame.extend_from_slice(&bytes);
            let (converted, _) =
                migrate_rawkey_handle_v17::<Vec<u8>>(&postcard::to_allocvec(&frame).unwrap())
                    .unwrap();
            assert_eq!(
                converted,
                handle::<DagMapRawKey<Vec<u8>>>(root, dag_id).unwrap()
            );
        }
        let (mut tree, map) = migrate_v17(root).unwrap();
        // Older v18 tools can still read their original completion record and obtain the remap.
        let old = ns.meta_path(root.map_id).with_extension(format!(
            "dag-{:016x}-v17",
            crate::tag(type_name::<DagMapRaw>())
        ));
        assert!(
            matches!(read_record(&old,root).unwrap(),Some(Record::Done(legacy)) if legacy==map)
        );
        assert_eq!(tree.child_ids(), root_tokens);
        let old = InstanceId::new(ids[1], ns.id());
        let new = map.get(&old).copied().unwrap_or(old);
        let a = DagMapRaw::from_meta(new).unwrap();
        assert_eq!(a.child_ids(), a_tokens);
        assert_eq!(a.get("local"), Some(b"a".to_vec()));
        assert_eq!(a.get("root"), None);
        let fresh = DagMapRaw::new(Some(&mut tree));
        let edge = tree.child_id(&fresh).unwrap();
        assert!(u128::from_le_bytes(edge.try_into().unwrap()) > 128, "{cut}");
        let mut a = a;
        let grandchild = DagMapRaw::new(Some(&mut a));
        assert!(u128::from_le_bytes(a.child_id(&grandchild).unwrap().try_into().unwrap()) > 128);
    }
}
