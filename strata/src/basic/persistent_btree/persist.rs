//! Version-store publication and verified reference checkpoints.

use super::*;
use crate::versioned::store::{Header, RETIRED, STAGE, row};
use std::collections::BTreeMap;

impl PersistentBTree {
    pub(crate) fn version_store(nodes: MapxRaw, next_id: u64) -> Self {
        let runtime = tree_runtime(
            nodes.instance_id(),
            next_id,
            RefState {
                counts: HashMap::new(),
                edges: HashMap::new(),
                zombies: HashSet::new(),
                pins: HashMap::new(),
                deferred: Some(Vec::new()),
                ready: true,
            },
        );
        Self {
            nodes,
            runtime,
            pending: HashMap::new(),
            pending_bytes: 0,
            stage: Some(Vec::new()),
        }
    }

    pub(crate) fn abandon_pending(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
        self.stage.as_mut().expect("version store").clear();
    }

    pub(crate) fn next_id(&self) -> u64 {
        self.runtime.next_id.load(Ordering::Acquire)
    }

    pub(crate) fn check_ceiling(&self, next: u64) -> Result<()> {
        for class in [0, LEAF, BLOB_LEAF, BLOB] {
            let max = class | (!CLASS_MASK);
            if let Some((key, _)) = self.nodes.get_le(object_key(max))
                && let Some(id) = object_id(&key)
                && id & CLASS_MASK == class
                && id & !CLASS_MASK >= next
            {
                return Err(VsdbError::Corrupt {
                    detail: format!("object ID {id} exceeds allocation ceiling {next}"),
                });
            }
        }
        Ok(())
    }

    pub(super) fn flush_stage(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut hdr = Header::get(&self.nodes)?;
        hdr.next_id = self.next_id();
        // Sort by allocation serial: every dependency precedes its page.
        // One unusually large blob occupies a chunk of its own.
        let mut pages: Vec<_> = self.pending.drain().collect();
        pages.sort_unstable_by_key(|(id, _)| id & !CLASS_MASK);
        let mut pages = pages.into_iter().peekable();
        while pages.peek().is_some() {
            let mut batch = self.nodes.batch();
            let mut roots = HashSet::new();
            let mut size = 0;
            while let Some((id, raw)) =
                pages.next_if(|(_, raw)| size == 0 || size + raw.len() + 9 <= 16 << 20)
            {
                size += raw.len() + 9;
                roots.insert(id);
                if id & CLASS_MASK != BLOB {
                    for child in Node::decode_children(&raw) {
                        roots.remove(&child);
                    }
                }
                batch.insert(object_key(id), raw);
            }
            batch.insert([0], hdr.bytes());
            for &id in &roots {
                batch.insert(row(STAGE, id), []);
            }
            batch.commit()?;
            self.stage
                .as_mut()
                .expect("missing stage tracker")
                .extend(roots);
        }
        self.pending_bytes = 0;
        Ok(())
    }

    pub(crate) fn publish(
        &mut self,
        rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        acquire: &[NodeId],
        retire: &[NodeId],
        sync: bool,
    ) -> Result<()> {
        let shared = Arc::strong_count(&self.runtime) > 1;
        for &id in acquire {
            self.acquire_node(id);
        }
        let mut dead = self
            .runtime
            .refs
            .lock()
            .deferred
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        let previously_dead = dead.len();
        // With a unique runtime, no reader can capture the prior head, so
        // compute its cascade before issuing the single publication batch.
        if !shared {
            let mut refs = self.runtime.refs.lock();
            for &id in retire {
                dead.extend(
                    refs.release(id)
                        .into_iter()
                        .map(|id| object_key(id).to_vec()),
                );
            }
        }
        let mut batch = self.nodes.batch();
        for (k, v) in rows {
            match v {
                Some(bytes) => batch.insert(k, bytes),
                None => batch.remove(k),
            }
        }
        for (id, raw) in &self.pending {
            batch.insert(object_key(*id), raw);
        }
        for key in &dead {
            batch.remove(key);
        }
        for &id in self.stage.as_deref().unwrap_or_default() {
            batch.remove(row(STAGE, id));
        }
        if shared {
            for &id in retire.iter().filter(|&&id| id != EMPTY_ROOT) {
                batch.insert(row(RETIRED, id), []);
            }
        }
        let submit = || {
            if sync {
                batch.commit_sync()
            } else {
                batch.commit()
            }
        };
        #[cfg(test)]
        let result =
            crate::versioned::test::before_tree_publication().and_then(|()| submit());
        #[cfg(not(test))]
        let result = submit();
        self.pending.clear();
        self.pending_bytes = 0;
        self.stage.as_mut().unwrap().clear();
        if let Err(err) = result {
            self.runtime
                .refs
                .lock()
                .deferred
                .get_or_insert_with(Vec::new)
                .extend(dead.into_iter().take(previously_dead));
            return Err(err);
        }

        if shared {
            let mut refs = self.runtime.refs.lock();
            for &id in retire.iter().filter(|&&id| id != EMPTY_ROOT) {
                let dead = refs.release(id);
                let deferred = refs.deferred.get_or_insert_with(Vec::new);
                deferred.extend(dead.into_iter().map(|id| object_key(id).to_vec()));
                if refs.pins.contains_key(&id) {
                    refs.zombies.insert(id);
                } else {
                    refs.deferred
                        .get_or_insert_with(Vec::new)
                        .push(row(RETIRED, id).to_vec());
                }
            }
        }
        Ok(())
    }
}

impl PersistentBTree {
    pub(crate) fn ref_checkpoint(&self, generation: u64) -> Result<Vec<u8>> {
        use sha3::{Digest, Keccak256};
        let refs = self.runtime.refs.lock();
        let mut state = RefState {
            counts: refs.counts.clone(),
            edges: refs.edges.clone(),
            zombies: HashSet::new(),
            ready: true,
            pins: HashMap::new(),
            deferred: None,
        };
        // Pin-only roots must not become durable owners.
        for (&root, &pins) in &refs.pins {
            for _ in 0..pins {
                state.release(root);
            }
        }
        drop(refs);
        let mut raw = Vec::with_capacity(32 + 16 + state.counts.len() * 16);
        raw.extend_from_slice(&generation.to_le_bytes());
        raw.extend_from_slice(&self.instance_id().map_id.to_le_bytes());
        for (id, n) in state.counts {
            if n == 0 {
                continue;
            }
            raw.extend_from_slice(&id.to_le_bytes());
            raw.extend_from_slice(&n.to_le_bytes());
            if matches!(id & CLASS_MASK, 0 | BLOB_LEAF) {
                let edges = state.edges.remove(&id).unwrap_or_default();
                raw.push(u8::try_from(edges.len()).expect("B+ tree fanout overflow"));
                for c in edges {
                    raw.extend_from_slice(&c.to_le_bytes());
                }
            }
        }
        let sum = Keccak256::digest(&raw);
        raw.extend_from_slice(&sum);
        Ok(raw)
    }

    pub(crate) fn load_refs(
        &mut self,
        raw: &[u8],
        gen_num: u64,
        roots: &[NodeId],
    ) -> Option<usize> {
        use sha3::{Digest, Keccak256};
        let (mut data, sum) = raw.split_at_checked(raw.len().checked_sub(32)?)?;
        if Keccak256::digest(data).as_slice() != sum {
            return None;
        }
        let generation = u64::from_le_bytes(data.split_off(..8)?.try_into().ok()?);
        let map = u64::from_le_bytes(data.split_off(..8)?.try_into().ok()?);
        if gen_num != generation || map != self.instance_id().map_id {
            return None;
        }
        let mut counts = HashMap::new();
        while !data.is_empty() {
            let id = u64::from_le_bytes(data.split_off(..8)?.try_into().ok()?);
            let ref_count = u32::from_le_bytes(data.split_off(..4)?.try_into().ok()?);
            if id & !CLASS_MASK == 0
                || id & !CLASS_MASK >= self.next_id()
                || ref_count == 0
            {
                return None;
            }
            let mut children = Vec::new();
            if matches!(id & CLASS_MASK, 0 | BLOB_LEAF) {
                let &n = data.split_off_first()?;
                if n == 0 || n as usize > MAX_KEYS + 1 {
                    return None;
                }
                for _ in 0..n {
                    let child =
                        u64::from_le_bytes(data.split_off(..8)?.try_into().ok()?);
                    if child & !CLASS_MASK >= id & !CLASS_MASK
                        || (id & CLASS_MASK == BLOB_LEAF) != (child & CLASS_MASK == BLOB)
                    {
                        return None;
                    }
                    children.push(child);
                }
            }
            if counts
                .insert(
                    id,
                    NodeRef {
                        ref_count,
                        children,
                    },
                )
                .is_some()
            {
                return None;
            }
        }
        let mut expected = HashMap::<NodeId, u32>::new();
        for id in roots
            .iter()
            .filter(|&&id| id != EMPTY_ROOT)
            .chain(counts.values().flat_map(|v| v.children.iter()))
        {
            let count = expected.entry(*id).or_default();
            *count = count.checked_add(1)?;
        }
        if expected.len() != counts.len()
            || counts
                .iter()
                .any(|(id, v)| expected.get(id) != Some(&v.ref_count))
        {
            return None;
        }
        let n = counts.len();
        let mut refs = self.runtime.refs.lock();
        self.count_nodes(
            &mut counts,
            refs.pins.iter().map(|(&id, &pins)| (id, pins)),
            true,
        )
        .ok()?;
        refs.install(counts);
        Some(n)
    }
}

impl PersistentBTree {
    pub(crate) fn flush_refs(&mut self, key: &[u8], bytes: Vec<u8>) -> Result<()> {
        let dead = self
            .runtime
            .refs
            .lock()
            .deferred
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        let mut batch = self.nodes.batch();
        for key in &dead {
            batch.remove(key);
        }
        batch.insert(key, bytes);
        if let Err(error) = batch.commit_sync() {
            self.runtime
                .refs
                .lock()
                .deferred
                .get_or_insert_with(Vec::new)
                .extend(dead);
            return Err(error);
        }
        Ok(())
    }
}

impl PersistentBTree {
    pub(crate) fn keep_pin_marker(&self, id: NodeId) -> bool {
        let mut refs = self.runtime.refs.lock();
        if refs.pins.contains_key(&id) {
            refs.zombies.insert(id);
            true
        } else {
            false
        }
    }

    pub(crate) fn verify_pages(&self, counts: &HashMap<NodeId, NodeRef>) -> Result<()> {
        struct Span {
            height: u32,
            first: Vec<u8>,
            last: Vec<u8>,
            len: usize,
        }
        let mut pages = HashMap::<NodeId, Span>::new();
        let mut ids: Vec<_> = counts
            .keys()
            .copied()
            .filter(|id| id & CLASS_MASK != BLOB)
            .collect();
        ids.sort_unstable_by_key(|id| id & !CLASS_MASK);
        for id in ids {
            let raw = self.object(id);
            let page = NodeView::open(&raw)?;
            let invalid = || VsdbError::Corrupt {
                detail: format!("invalid child ordering or occupancy at page {id}"),
            };
            if page.len == 0 {
                return Err(invalid());
            }
            let span = if page.leaf {
                Span {
                    height: 0,
                    first: page.key(0).to_vec(),
                    last: page.key(page.len - 1).to_vec(),
                    len: page.len,
                }
            } else {
                let first = pages.get(&page.child(0)).ok_or_else(invalid)?;
                let mut left = first;
                for i in 0..page.len {
                    let right = pages.get(&page.child(i + 1)).ok_or_else(invalid)?;
                    let sep = page.key(i);
                    if first.height != right.height
                        || left.len < MIN_KEYS
                        || right.len < MIN_KEYS
                        || left.last.as_slice() >= sep
                        || right.first.as_slice() < sep
                    {
                        return Err(invalid());
                    }
                    left = right;
                }
                Span {
                    height: first.height + 1,
                    first: first.first.clone(),
                    last: left.last.clone(),
                    len: page.len,
                }
            };
            pages.insert(id, span);
        }
        Ok(())
    }
}
