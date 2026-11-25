//! Offline, resumable conversion of v17's three-prefix node handles.

use crate::{BasicHandle, LegacyRaw, legacy_payload, raw_map, typed_handle, typed_handle_tag};
use serde::{
    Deserialize, Serialize,
    de::{self, DeserializeOwned},
};
use sha3::{Digest, Sha3_256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    io::ErrorKind,
    path::Path,
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

fn saved_tag<T>(ns: &Namespace, parts: [InstanceId; 3]) -> Result<u64> {
    let default = crate::tag(std::any::type_name::<T>());
    let bytes = match fs::read(ns.meta_path(parts[0].map_id)) {
        Ok(v) => v,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(default),
        Err(e) => return Err(e.into()),
    };
    // A v17 raw DAG may have independently persisted typed descendants.
    // Preserve each saved envelope's value type, even when the root has a
    // different type. Authenticate its inner three handles before rewriting.
    let (saved, tag) = if let Ok(node) = decode::<Legacy>(&bytes) {
        (node, crate::tag(std::any::type_name::<DagMapRaw>()))
    } else {
        let frame: &[u8] = decode(&bytes)?;
        let Some(magic @ (b"VSTYPE02" | b"VSTYPE03")) = frame.get(..8) else {
            return Err(corrupt("invalid saved child metadata"));
        };
        if frame.len() < 16 {
            return Err(corrupt("invalid saved child metadata"));
        }
        let vstype02 = magic == b"VSTYPE02";
        let tag = u64::from_le_bytes(frame[8..16].try_into().unwrap());
        let tag = if tag == crate::legacy_tag_for::<T>(vstype02) {
            default
        } else if !vstype02 && tag == crate::legacy_tag_for::<DagMapRawKey<vsdb::BranchId>>(false) {
            crate::tag(std::any::type_name::<DagMapRawKey<vsdb::BranchId>>())
        } else if !vstype02 && tag == crate::legacy_tag_for::<DagMapRawKey<vsdb::CommitId>>(false) {
            crate::tag(std::any::type_name::<DagMapRawKey<vsdb::CommitId>>())
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
        let owner = decode_link(
            &node
                .1
                .get([])
                .ok_or_else(|| corrupt("missing v17 parent slot"))?,
        )?;
        let marker = |key| -> Result<Option<[InstanceId; 3]>> {
            match node.1.get([key]) {
                Some(v) => Ok(Some(
                    decode_link(&v)?.ok_or_else(|| corrupt("empty v17 recovery marker"))?,
                )),
                None => Ok(None),
            }
        };
        let clearing = marker(1)?;
        let destroying = marker(2)?;
        if clearing.is_some_and(|g| g != root_parts)
            || node
                .1
                .iter()
                .any(|(k, _)| !matches!(k.as_slice(), [] | [1] | [2]))
        {
            return Err(corrupt("invalid v17 DAG recovery metadata"));
        }
        let mut children = Vec::new();
        for (k, v) in node.2.iter() {
            let e = u128::from_le_bytes(
                k.try_into()
                    .map_err(|_| corrupt("invalid v17 child token"))?,
            );
            let edge = u64::try_from(e)
                .ok()
                .filter(|e| *e < u64::MAX)
                .ok_or_else(|| corrupt("v17 DAG edge exceeds u64"))?;
            let child: Legacy = decode(&v)?;
            let target = child.0.instance_id().map_id;
            reverse
                .entry(target)
                .or_default()
                .push(Parent { map: id, edge });
            children.push((edge, target));
            pending.push(child);
        }
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
    let mut targets = HashMap::new();
    for &id in sources.keys() {
        let colocated = open_prefix(ns, id)?.is_colocated_with(&anchor);
        targets.insert(
            id,
            if colocated {
                id
            } else {
                anchor.new_colocated().instance_id().map_id
            },
        );
    }
    let remap = |mut p: Parent| {
        p.map = targets[&p.map];
        p
    };
    let mut nodes = Vec::with_capacity(sources.len());
    for (id, source) in sources {
        let state = match states[&id] {
            State::Root => State::Root,
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
            scratch: anchor.new_colocated().instance_id().map_id,
            tag: saved_tag::<T>(ns, source.parts)?,
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
    let mut rows = source.iter().peekable();
    while rows.peek().is_some() {
        let mut batch = target.batch();
        let mut size = 0;
        for (k, v) in rows.by_ref().take(4096) {
            size += k.len() + v.len() + 2;
            if tag {
                let v = if v.is_empty() { vec![0] } else { tagged(1, &v) };
                batch.insert(tagged(2, &k), v);
            } else {
                batch.insert(k, v);
            }
            if size >= 4 << 20 {
                break;
            }
        }
        batch.commit()?;
    }
    Ok(())
}
fn prepare(ns: &Namespace, nodes: &[Conversion], dag_id: u64) -> Result<()> {
    for n in nodes {
        let source = open_prefix(ns, n.old[0])?;
        let mut scratch = open_prefix(ns, n.scratch)?;
        copy_rows(&source, &mut scratch, true)?;
        let state = n.state;
        let mut next_edge_id = 1;
        for &(edge, child) in &n.children {
            scratch.insert(edge_key(edge), child.to_le_bytes());
            next_edge_id = next_edge_id.max(edge + 1);
        }
        scratch.insert(
            [0],
            postcard::to_allocvec(&(*b"VSDAGN01", dag_id, next_edge_id, state))?,
        );
    }
    open_prefix(ns, nodes[0].scratch)?.try_sync_wal()
}

/// Converts a v17 raw DAG under exclusive access, preserving the root prefix.
/// Returns the converted root and all changed descendant identities. Retrying
/// resumes from the journal stored beside the root's instance metadata.
pub fn migrate_v17(id: impl Into<InstanceId>) -> Result<(DagMapRaw, Remap)> {
    migrate_with::<DagMapRaw>(id.into(), None, |bytes| decode(bytes), DagMapRaw::from_meta)
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
    migrate_with::<DagMapRawKey<V>>(
        id.into(),
        None,
        |bytes| {
            let frame: &[u8] = decode(bytes)?;
            decode(legacy_payload::<DagMapRawKey<V>>(frame)?)
        },
        DagMapRawKey::from_meta,
    )
}

/// Converts a postcard-serialized v17 `DagMapRawKey<V>` root handle (and its
/// connected DAG) without requiring a pre-existing `__instance_meta__` file.
pub fn migrate_rawkey_handle_v17<V: ValueEnDe>(bytes: &[u8]) -> Result<(Vec<u8>, Remap)> {
    migrate_bytes_with::<DagMapRawKey<V>>(
        bytes,
        |b| {
            let frame: &[u8] = decode(b)?;
            decode(legacy_payload::<DagMapRawKey<V>>(frame)?)
        },
        |d| d.instance_id(),
    )
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
    let expected = crate::tag(std::any::type_name::<DagMapRawKey<V>>());
    if found == expected {
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
fn migrate_bytes_with<T: DeserializeOwned>(
    bytes: &[u8],
    parse: impl Fn(&[u8]) -> Result<Legacy>,
    instance_of: impl Fn(&T) -> InstanceId,
) -> Result<(Vec<u8>, Remap)> {
    if let Ok((frame, b"")) = postcard::take_from_bytes::<&[u8]>(bytes)
        && frame.starts_with(b"VSTYPE04")
    {
        let node: T = decode(bytes)?;
        let root = instance_of(&node);
        let root = InstanceId::new(root.map_id, root.ns.unwrap_or_default());
        let ns = Namespace::open(root.ns.unwrap_or_default())?;
        if ns.is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "DAG migration",
            });
        }
        let journal = ns.meta_path(root.map_id).with_extension(format!(
            "dag-{:016x}-v17",
            crate::tag(std::any::type_name::<T>())
        ));
        let map = match read_record(&journal, root)? {
            Some(Record::Done(map)) => map,
            None => Remap::new(),
            Some(Record::Running { .. }) => {
                let (_, map) = migrate_with::<T>(root, None, parse, |id| {
                    decode(&fs::read(ns.meta_path(id.map_id))?)
                })?;
                map
            }
        };
        return Ok((bytes.to_vec(), map));
    }
    let legacy = parse(bytes)?;
    let root = legacy.0.instance_id();
    let ns = Namespace::open(root.ns.unwrap_or_default())?;
    if ns.is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "DAG migration",
        });
    }
    let path = ns.meta_path(root.map_id);
    let (_, map) = migrate_with::<T>(root, Some(bytes), parse, |id| {
        decode(&fs::read(ns.meta_path(id.map_id))?)
    })?;
    Ok((fs::read(&path)?, map))
}

fn migrate_with<T: DeserializeOwned>(
    root: InstanceId,
    source: Option<&[u8]>,
    parse: impl Fn(&[u8]) -> Result<Legacy>,
    restore: impl Fn(InstanceId) -> Result<T>,
) -> Result<(T, Remap)> {
    let root = InstanceId::new(root.map_id, root.ns.unwrap_or_default());
    let ns = Namespace::open(root.ns.unwrap_or_default())?;
    if ns.is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "DAG migration",
        });
    }
    let path = ns.meta_path(root.map_id);
    vsdb::common::create_dir_all_durable(path.parent().unwrap())?;
    let journal = path.with_extension(format!(
        "dag-{:016x}-v17",
        crate::tag(std::any::type_name::<T>())
    ));
    let mut record = match read_record(&journal, root)? {
        Some(r) => r,
        None => {
            if let Ok(node) = restore(root) {
                return Ok((node, Remap::new()));
            }
            let owned;
            let raw = match source {
                Some(b) => b,
                None => {
                    owned = fs::read(&path)?;
                    &owned
                }
            };
            let nodes = plan::<T>(&ns, root, parse(raw)?)?;
            let r = Record::Running {
                dag_id: rand::random(),
                phase: Phase::Prepare,
                nodes,
            };
            write_record(&journal, root, &r)?;
            r
        }
    };
    loop {
        let Record::Running {
            dag_id,
            phase,
            nodes,
        } = &mut record
        else {
            let Record::Done(map) = record else {
                unreachable!()
            };
            return Ok((restore(root)?, map));
        };
        match phase {
            Phase::Prepare => {
                prepare(&ns, nodes, *dag_id)?;
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
                let mut map = Remap::new();
                for n in nodes.iter() {
                    if n.target != n.old[0] {
                        map.insert(
                            InstanceId::new(n.old[0], ns.id()),
                            InstanceId::new(n.target, ns.id()),
                        );
                    }
                    for old in n
                        .old
                        .into_iter()
                        .chain([n.scratch])
                        .filter(|id| *id != n.target)
                    {
                        open_prefix(&ns, old)?.batch_wiped().commit_sync()?;
                    }
                }
                record = Record::Done(map);
            }
        }
        write_record(&journal, root, &record)?;
    }
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
            for cut in ["prepare", "publish", "retire"] {
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
                "publish" => {
                    prepare(&ns, &nodes, dag_id).unwrap();
                    // Source root already overwritten; its original rows now
                    // survive only in the checkpointed tagged scratch copy.
                    let mut target = open_prefix(&ns, root.map_id).unwrap();
                    target.clear();
                    target.insert(b"partial", b"published");
                    Phase::Publish
                }
                "retire" => {
                    prepare(&ns, &nodes, dag_id).unwrap();
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
                crate::tag(std::any::type_name::<DagMapRaw>())
            ));
            write_record(
                &journal,
                root,
                &Record::Running {
                    dag_id,
                    phase,
                    nodes,
                },
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
        let (tree, map) = migrate_v17(root).unwrap();
        assert_eq!(tree.child_ids(), root_tokens);
        let old = InstanceId::new(ids[1], ns.id());
        let new = map.get(&old).copied().unwrap_or(old);
        let a = DagMapRaw::from_meta(new).unwrap();
        assert_eq!(a.child_ids(), a_tokens);
        assert_eq!(a.get("local"), Some(b"a".to_vec()));
        assert_eq!(a.get("root"), None);
    }
}
