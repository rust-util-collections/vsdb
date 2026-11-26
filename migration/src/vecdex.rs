//! Offline conversion of a single v17 HNSW index. The progress row shares
//! the batch and WAL with its vector chunk, so a retry never decodes LE
//! components as a postcard vector, or skips an unconverted row.
use crate::{corrupt, exactly, legacy_payload, raw_id, raw_map, tag, try_get, typed_handle};
use serde::Deserialize;
use std::{
    any::{TypeId, type_name},
    fs,
    ops::Bound,
};
use vsdb::{
    Cosine, DistanceMetric, HnswConfig, InnerProduct, InstanceId, KeyEnDe, L2, MapxRaw, MetricKind,
    Namespace, Result, Scalar, VecDex,
};

const PROGRESS: [u8; 1] = [7];
const CHUNK_BYTES: usize = 4 << 20;
const TAG_VEC: u8 = 0;
const STATE_KEY: [u8; 1] = [5];
const META_KEY: [u8; 1] = [6];

#[derive(Default, Deserialize)]
struct GraphState {
    entry_point: Option<u64>,
    max_layer: u8,
    node_count: u64,
    next_node_id: u64,
    ef_search: usize,
}

impl GraphState {
    fn encode(&self) -> Result<Vec<u8>> {
        if self.max_layer > 32
            || self.entry_point.is_none() && self.max_layer != 0
            || self.node_count > self.next_node_id
            || self.entry_point.is_some() != (self.node_count != 0)
            || self.entry_point.is_some_and(|ep| ep >= self.next_node_id)
        {
            return Err(corrupt("invalid graph state"));
        }
        Ok([
            &[self.entry_point.is_some() as u8, self.max_layer][..],
            &self.entry_point.unwrap_or_default().to_le_bytes(),
            &self.node_count.to_le_bytes(),
            &self.next_node_id.to_le_bytes(),
            &(self.ef_search as u64).to_le_bytes(),
        ]
        .concat())
    }
}

/// Converts vector framing at the saved v17 prefix while preserving the graph.
/// Run with exclusive access. Interrupted conversions resume at the last
/// committed vector chunk.
pub fn migrate_v17<K: KeyEnDe, D: DistanceMetric<S>, S: Scalar>(
    id: InstanceId,
) -> Result<VecDex<K, D, S>> {
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
    let ns = Namespace::open(id.ns.unwrap_or_default())?;
    if ns.is_read_only() {
        return Err(vsdb::VsdbError::ReadOnly {
            operation: "index migration",
        });
    }
    let path = ns.meta_path(id.map_id);
    let bytes = fs::read(&path)?;
    let (index, target) = convert_bytes::<K, D, S>(&bytes, Some(id))?;
    if bytes != target {
        vsdb::common::atomic_write_file(&path, &target)?;
    } else {
        crate::sync_parent(&path)?;
    }
    Ok(index)
}

/// Converts a postcard-serialized v17 `VecDex` or `VecDexDyn` handle (and its
/// underlying vector rows) without requiring an `__instance_meta__` file.
pub fn migrate_handle_v17<K: KeyEnDe, D: DistanceMetric<S>, S: Scalar>(
    bytes: &[u8],
) -> Result<Vec<u8>> {
    convert_bytes::<K, D, S>(bytes, None).map(|(_, h)| h)
}

fn convert_bytes<K: KeyEnDe, D: DistanceMetric<S>, S: Scalar>(
    bytes: &[u8],
    expected: Option<InstanceId>,
) -> Result<(VecDex<K, D, S>, Vec<u8>)> {
    if let Ok((frame, b"")) = postcard::take_from_bytes::<&[u8]>(bytes)
        && frame.starts_with(b"VSTYPE04")
    {
        let index: VecDex<K, D, S> = exactly(bytes)?;
        if index.namespace().is_read_only() {
            return Err(vsdb::VsdbError::ReadOnly {
                operation: "index migration",
            });
        }
        if expected.is_some_and(|e| index.instance_id() != e) {
            return Err(corrupt("VecDex metadata identity mismatch"));
        }
        return Ok((index, bytes.to_vec()));
    }
    let (id, config, metric) = legacy_handle::<K, D, S>(bytes)?;
    if expected.is_some_and(|e| id != e) {
        return Err(corrupt("VecDex metadata identity mismatch"));
    }
    config.validate()?;
    let mut store = raw_map(id)?;
    if store.namespace().is_read_only() {
        return Err(vsdb::VsdbError::ReadOnly {
            operation: "index migration",
        });
    }
    let version = 2u8;
    let schema = postcard::to_allocvec(&(
        version,
        size_of::<S>() as u8,
        tag(type_name::<VecDex<K, D, S>>()),
        config.dim as u32,
        config.m,
        config.m_max0,
        config.ef_construction,
        config.ef_search,
        &metric,
    ))?;
    match try_get(&store, META_KEY)? {
        Some(old) if old != schema => return Err(corrupt("schema mismatch on migration retry")),
        Some(_) => {}
        None => convert_vectors::<K, S>(&mut store, &config, &schema)?,
    }
    use sha3::{Digest, Keccak256};
    let mut extra = [version; 9];
    extra[1..].copy_from_slice(&Keccak256::digest(schema)[..8]);
    let target = typed_handle::<VecDex<K, D, S>>(id, &extra)?;
    let index = exactly(&target)?;
    Ok((index, target))
}

fn legacy_handle<K: KeyEnDe, D: DistanceMetric<S>, S: Scalar>(
    bytes: &[u8],
) -> Result<(InstanceId, HnswConfig, D)> {
    // v17 dynamic handles were a discriminator followed by a complete
    // typed static handle. Check both the tag and the inner type.
    let (inner, metric) = if TypeId::of::<D>() == TypeId::of::<MetricKind>() {
        let (wire, payload) = bytes
            .split_first()
            .ok_or_else(|| corrupt("empty VecDex metadata"))?;
        let frame: &[u8] = exactly(payload)?;
        let inner = match wire {
            0 => legacy_payload::<VecDex<K, L2, S>>(frame)?,
            1 => legacy_payload::<VecDex<K, Cosine, S>>(frame)?,
            2 => legacy_payload::<VecDex<K, InnerProduct, S>>(frame)?,
            _ => return Err(corrupt("unknown v17 metric")),
        };
        (inner, postcard::from_bytes(std::slice::from_ref(wire))?)
    } else {
        let frame: &[u8] = exactly(bytes)?;
        (legacy_payload::<VecDex<K, D, S>>(frame)?, D::default())
    };
    let ((version, store, config), tail): ((u8, &[u8], HnswConfig), _) =
        postcard::take_from_bytes(inner)?;
    if !tail.is_empty() || version != 2 {
        return Err(corrupt("unsupported v17 VecDex layout"));
    }
    Ok((raw_id(store)?, config, metric))
}

fn convert_vectors<K: KeyEnDe, S: Scalar>(
    store: &mut MapxRaw,
    config: &HnswConfig,
    schema: &[u8],
) -> Result<()> {
    let state: GraphState = match try_get(store, STATE_KEY)? {
        Some(bytes) => exactly(&bytes)?,
        None => GraphState {
            ef_search: config.ef_search,
            ..GraphState::default()
        },
    };
    let state_bytes = state.encode()?;
    let progress = try_get(store, PROGRESS)?;
    let cursor = if let Some(bytes) = progress.as_deref() {
        let (saved, cursor): (&[u8], u64) = exactly(bytes)?;
        if saved != schema
            || cursor >= state.next_node_id
            || !store.try_contains_key(node_key(TAG_VEC, cursor))?
        {
            return Err(corrupt("invalid VecDex migration cursor or schema"));
        }
        Some(cursor)
    } else {
        None
    };
    let repair_reverse = validate_source::<K, S>(store, config, &state, cursor)?;
    let start = cursor.map_or_else(
        || Bound::Included(vec![TAG_VEC]),
        |id| Bound::Excluded(node_key(TAG_VEC, id).to_vec()),
    );
    let reader = store.reader();
    let view = reader.read_view();
    let mut rows = view.try_range((start, Bound::Excluded(vec![TAG_VEC + 1])))?;
    while let Some((first_key, first_raw)) = rows.try_step_next()? {
        let mut batch = store.batch();
        let mut bytes_staged = 0;
        let mut cursor = 0;
        let mut cur = Some((first_key, first_raw));
        while let Some((key, raw)) = cur {
            cursor = u64::from_be_bytes(
                key[1..]
                    .try_into()
                    .map_err(|_| corrupt("invalid vector key"))?,
            );
            if cursor >= state.next_node_id {
                return Err(corrupt("vector ID exceeds allocation ceiling"));
            }
            // Postcard's float components already have the v18 IEEE-754 LE
            // representation. Authenticate the old length and copy the tail.
            let (dim, data): (usize, _) = postcard::take_from_bytes(raw)?;
            if dim != config.dim || dim.checked_mul(size_of::<S>()) != Some(data.len()) {
                return Err(corrupt("malformed v17 vector"));
            }
            bytes_staged += raw.len() + key.len() + 16;
            batch.insert(key, data);
            if bytes_staged >= CHUNK_BYTES {
                break;
            }
            cur = rows.try_step_next()?;
        }
        let exhausted = cur.is_none();
        batch.insert(PROGRESS, postcard::to_allocvec(&(schema, cursor))?);
        batch.commit()?;
        if exhausted {
            break;
        }
    }
    drop(rows);
    drop(view);
    if repair_reverse {
        normalize_reverse_keys(store)?;
    }
    let mut batch = store.batch();
    batch.remove(PROGRESS);
    batch.insert(META_KEY, schema);
    batch.insert(STATE_KEY, state_bytes);
    batch.commit_sync()
}

fn node_key(tag: u8, id: u64) -> [u8; 9] {
    let mut key = [tag; 9];
    key[1..].copy_from_slice(&id.to_be_bytes());
    key
}
fn node_id(raw: &[u8], state: &GraphState) -> Result<u64> {
    let id = u64::from_be_bytes(
        raw.try_into()
            .map_err(|_| corrupt("wrong v17 node key width"))?,
    );
    if id >= state.next_node_id {
        return Err(corrupt("v17 node ID exceeds allocation ceiling"));
    }
    Ok(id)
}

/// Checks the complete source before publishing the first rewritten chunk.
/// In particular a damaged state, duplicate user mapping, or cursor must
/// never turn into a successful conversion of a partly framed graph.
fn validate_source<K: KeyEnDe, S: Scalar>(
    store: &MapxRaw,
    config: &HnswConfig,
    state: &GraphState,
    cursor: Option<u64>,
) -> Result<bool> {
    let mut counts = [0u64; 5];
    let width = config
        .dim
        .checked_mul(size_of::<S>())
        .ok_or_else(|| corrupt("vector dimension overflow"))?;
    let mut unusual = std::collections::HashSet::new();
    let mut highest = 0;
    let mut rows = store.try_range(..)?;
    while let Some((key, row)) = rows.try_step_next()? {
        let (&tag, k) = key
            .split_first()
            .ok_or_else(|| corrupt("empty v17 graph key"))?;
        match tag {
            0 => {
                let id = node_id(k, state)?;
                if !store.try_contains_key(node_key(4, id))? {
                    return Err(corrupt("v17 vector lacks node info"));
                }
                if cursor.is_some_and(|c| id <= c) {
                    if row.len() != width {
                        return Err(corrupt("invalid already-converted vector"));
                    }
                } else {
                    let (dim, data): (usize, _) = postcard::take_from_bytes(row)?;
                    if dim != config.dim || data.len() != width {
                        return Err(corrupt("invalid v17 vector"));
                    }
                }
                counts[0] += 1;
            }
            1 => {
                let (&layer, addr) = k
                    .split_first()
                    .ok_or_else(|| corrupt("malformed adjacency key"))?;
                let id = node_id(addr, state)?;
                if layer > state.max_layer
                    || !store.try_contains_key(node_key(4, id))?
                    || !row.len().is_multiple_of(8)
                    || row
                        .as_chunks::<8>()
                        .0
                        .iter()
                        .any(|n| u64::from_le_bytes(*n) >= state.next_node_id)
                {
                    return Err(corrupt("invalid v17 adjacency row"));
                }
            }
            2 => {
                let raw: [u8; 8] = row
                    .try_into()
                    .map_err(|_| corrupt("invalid v17 forward mapping"))?;
                let id = u64::from_le_bytes(raw);
                if K::decode_key(k)?.encode_key() != k {
                    return Err(corrupt("noncanonical or incompatible v17 key"));
                }
                let reverse = try_get(store, node_key(3, id))?
                    .ok_or_else(|| corrupt("missing v17 reverse mapping"))?;
                if reverse != k {
                    // The v17 forward mapping used KeyEnDe while the reverse
                    // used ValueEnDe. Custom codecs can differ legitimately.
                    let mut old_probe = vec![2];
                    old_probe.extend(reverse);
                    if try_get(store, old_probe)?.as_deref() == Some(&raw) || !unusual.insert(id) {
                        return Err(corrupt("duplicate key mapping to one node"));
                    }
                }
                counts[2] += 1;
            }
            3 | 4 => {
                let id = node_id(k, state)?;
                if tag == 3 && !store.try_contains_key(node_key(4, id))? {
                    return Err(corrupt("v17 reverse mapping has no node info"));
                }
                if tag == 4 {
                    let layer: u8 = exactly(row)?;
                    if layer > 32 {
                        return Err(corrupt("invalid v17 layer"));
                    }
                    highest = highest.max(layer);
                }
                counts[tag as usize] += 1;
            }
            5 | 7 if k.is_empty() => {}
            _ => return Err(corrupt("unexpected v17 graph row")),
        }
    }
    let ep_ok = match state.entry_point {
        Some(ep) => try_get(store, node_key(4, ep))?.as_deref() == Some(&[state.max_layer]),
        None => true,
    };
    if [counts[0], counts[2], counts[3], counts[4]] != [state.node_count; 4]
        || highest != state.max_layer
        || !ep_ok
    {
        return Err(corrupt("v17 graph state disagrees with rows"));
    }
    Ok(!unusual.is_empty())
}
// Only custom v17 key codecs normally require reverse-map updates. This pass
// is idempotent if the process stops after its final converted vector chunk.
fn normalize_reverse_keys(store: &mut MapxRaw) -> Result<()> {
    let reader = store.reader();
    let snapshot = reader.read_view();
    let mut rows = snapshot.try_range([2]..[3])?;
    while let Some((first_key, first_id)) = rows.try_step_next()? {
        let mut batch = store.batch();
        let mut bytes = 0;
        let mut cur = Some((first_key, first_id));
        while let Some((key, id)) = cur {
            let id = u64::from_le_bytes(id.try_into().map_err(|_| corrupt("invalid node ID"))?);
            let reverse = node_key(3, id);
            let original = &key[1..];
            if reader.try_get_with(reverse, |v| Ok(v == original))? != Some(true) {
                batch.insert(reverse, original);
                bytes += original.len() + 25;
                if bytes >= CHUNK_BYTES {
                    break;
                }
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
