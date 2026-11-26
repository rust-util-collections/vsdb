//! Version-store publication and verified reference checkpoints.

use super::*;
use crate::versioned::{
    BranchId,
    store::{BranchHead, Header, RETIRED, STAGE, row},
};
use std::collections::BTreeMap;

/// Structural writers are serialized by the version store's alias contract.
struct HistoryPublication<'a>(&'a TreeRuntime);

pub(crate) enum FailedPublish {
    Unpublished,
    AmbiguousStaged {
        staged: Vec<NodeId>,
        start: u64,
        end: u64,
    },
}

impl<'a> HistoryPublication<'a> {
    fn begin(runtime: &'a TreeRuntime, target_generation: u64) -> Self {
        // If a reader observes the next writer's target, it must also
        // observe the end of the preceding publication on its final check.
        runtime
            .history_target_generation
            .store(target_generation, Ordering::Release);
        runtime.history_sequence.fetch_add(1, Ordering::AcqRel);
        Self(runtime)
    }
}

impl Drop for HistoryPublication<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.refs.lock().heads.clear();
        }
        self.0.history_sequence.fetch_add(1, Ordering::Release);
    }
}

impl PersistentBTree {
    pub(crate) fn version_store(nodes: MapxRaw, next_id: u64) -> Self {
        let runtime = tree_runtime(
            nodes.instance_id(),
            next_id,
            nodes.namespace().mem_budget_bytes(),
            RefState {
                counts: HashMap::new(),
                edges: HashMap::new(),
                zombies: HashSet::new(),
                pins: HashMap::new(),
                heads: HashMap::new(),
                tip_cache: TipCache::default(),
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
            staged_start: None,
        }
    }

    pub(crate) fn begin_staged_build(&mut self) {
        debug_assert!(self.pending.is_empty());
        debug_assert!(self.stage.as_ref().is_some_and(Vec::is_empty));
        self.staged_start = Some(self.next_id());
    }

    pub(crate) fn abandon_staged(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
        let start = self.staged_start.take();
        let staged = std::mem::take(self.stage.as_mut().expect("version store"));
        if staged.is_empty() {
            return;
        }
        let start = start.expect("staged rows require a recorded build start");
        let end = self.next_id();
        let mut batch = self.nodes.batch();
        for &id in &staged {
            batch.remove(row(STAGE, id));
        }
        if start < end {
            for (class, class_end) in [
                (0, [0x10u8, 0x40, 0, 0, 0, 0, 0, 0, 0].as_slice()),
                (LEAF, [0x10u8, 0x80, 0, 0, 0, 0, 0, 0, 0].as_slice()),
                (BLOB_LEAF, [0x10u8, 0xC0, 0, 0, 0, 0, 0, 0, 0].as_slice()),
                (BLOB, [0x13u8].as_slice()),
            ] {
                let lo = object_key(class | start);
                if end < LEAF {
                    let hi = object_key(class | end);
                    batch.delete_range(&lo, &hi);
                } else {
                    batch.delete_range(&lo, class_end);
                }
            }
        }
        if batch.commit().is_err() {
            self.defer_ambiguous_staged(staged, start, end, false);
        }
    }

    pub(crate) fn defer_ambiguous_staged(
        &self,
        staged: Vec<NodeId>,
        start: u64,
        end: u64,
        check_unowned: bool,
    ) {
        let mut refs = self.runtime.refs.lock();
        let mut deferred: Vec<Vec<u8>> = staged
            .into_iter()
            .map(|id| row(STAGE, id).to_vec())
            .collect();
        if start < end && (!check_unowned || refs.ready) {
            for class in [0, LEAF, BLOB_LEAF, BLOB] {
                let lo = object_key(class | start);
                let hi = object_key(class | end.saturating_sub(1).min(!CLASS_MASK));
                if let Ok(mut cursor) =
                    self.nodes.try_range(lo.as_slice()..=hi.as_slice())
                {
                    while let Ok(Some((k, _))) = cursor.try_step_next() {
                        let unowned = !check_unowned
                            || object_id(k)
                                .is_none_or(|id| !refs.counts.contains_key(&id));
                        if unowned {
                            deferred.push(k.to_vec());
                        }
                    }
                }
            }
        }
        refs.deferred.get_or_insert_with(Vec::new).extend(deferred);
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
        head_updates: &[(BranchId, Option<BranchHead>)],
        acquire: &[NodeId],
        retire: &[NodeId],
        sync_generation: Option<u64>,
    ) -> StdResult<(), (VsdbError, FailedPublish)> {
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
        let mut dead_internal = Vec::new();
        // With a unique runtime, no reader can capture the prior head, so
        // compute its cascade before issuing the single publication batch.
        if !shared {
            let mut refs = self.runtime.refs.lock();
            for &id in retire {
                for dead_id in refs.release(id) {
                    if dead_id & CLASS_MASK == 0 {
                        dead_internal.push(dead_id);
                    }
                    dead.push(object_key(dead_id).to_vec());
                }
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
        // Only synchronous publications create, retire, or edit commit records.
        // Deferred removals in ordinary working batches are tree objects or
        // retired-root markers. Their branch's committed head remains the same.
        let _publication = sync_generation
            .map(|generation| HistoryPublication::begin(&self.runtime, generation));
        let mut submitted = false;
        let submit = || {
            submitted = true;
            if sync_generation.is_some() {
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
        if let Err(err) = result {
            {
                let mut refs = self.runtime.refs.lock();
                refs.heads.clear();
                refs.deferred
                    .get_or_insert_with(Vec::new)
                    .extend(dead.into_iter().take(previously_dead));
            }
            drop(_publication);
            let ambiguous = submitted
                && matches!(
                    err,
                    VsdbError::Storage {
                        kind: vsdb_core::StorageErrorKind::Io,
                        ..
                    } | VsdbError::Io(_)
                );
            let cleanup = if ambiguous {
                self.pending.clear();
                self.pending_bytes = 0;
                let start = self.staged_start.take();
                let staged = std::mem::take(self.stage.as_mut().expect("version store"));
                match start {
                    Some(start) if !staged.is_empty() => {
                        FailedPublish::AmbiguousStaged {
                            staged,
                            start,
                            end: self.next_id(),
                        }
                    }
                    _ => FailedPublish::Unpublished,
                }
            } else {
                self.abandon_staged();
                FailedPublish::Unpublished
            };
            return Err((err, cleanup));
        }
        self.pending.clear();
        self.pending_bytes = 0;
        self.staged_start = None;
        self.stage.as_mut().unwrap().clear();

        if shared || !head_updates.is_empty() {
            let mut refs = self.runtime.refs.lock();
            for &(branch, head) in head_updates {
                refs.set_head(branch, head);
            }
            if shared {
                for &id in retire.iter().filter(|&&id| id != EMPTY_ROOT) {
                    let released = refs.release(id);
                    let deferred = refs.deferred.get_or_insert_with(Vec::new);
                    for dead_id in released {
                        if dead_id & CLASS_MASK == 0 {
                            dead_internal.push(dead_id);
                        }
                        deferred.push(object_key(dead_id).to_vec());
                    }
                    if refs.pins.contains_key(&id) {
                        refs.zombies.insert(id);
                    } else {
                        refs.deferred
                            .get_or_insert_with(Vec::new)
                            .push(row(RETIRED, id).to_vec());
                    }
                }
            }
        }
        for id in dead_internal {
            self.runtime.page_cache.remove(id);
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
            heads: HashMap::new(),
            tip_cache: TipCache::default(),
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
        self.count_nodes(&mut counts, refs.pins.iter().map(|(&id, &pins)| (id, pins)))
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
            let invalid = || VsdbError::Corrupt {
                detail: format!("invalid child ordering or occupancy at page {id}"),
            };
            let raw = self.nodes.get(object_key(id)).ok_or_else(invalid)?;
            let page = NodeView::open(&raw)?;
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
