//! Persistent raw-key and raw-value overlays on a single shard.
#![forbid(unsafe_code)]

#[cfg(test)]
mod test;

use crate::{
    DagMapId, Namespace,
    common::{
        InstanceId, PrefixFrame, UnwindMark, ensure_writable,
        error::{Result, VsdbError},
        staged::StagedRows,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    result::Result as StdResult,
};
use vsdb_core::{MapxRaw, common::RawBytes};

const META: [u8; 1] = [0];
const EDGE: [u8; 1] = [1];
const DATA: [u8; 1] = [2];
const END: [u8; 1] = [3];
const MAGIC: [u8; 8] = *b"VSDAGN01";
const LAYOUT: u8 = 1;

fn corrupt(message: &str) -> VsdbError {
    VsdbError::Corrupt {
        detail: message.into(),
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Parent {
    map: u64,
    edge: u64,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum State {
    Root,
    Child(Parent),
    Clearing { owner: Option<Parent>, genesis: u64 },
    Destroying(Option<Parent>),
    Destroyed,
}

impl State {
    fn owner(self) -> Option<Parent> {
        match self {
            Self::Child(owner) => Some(owner),
            Self::Clearing { owner, .. } | Self::Destroying(owner) => owner,
            Self::Root | Self::Destroyed => None,
        }
    }
    fn live(self) -> bool {
        matches!(self, Self::Root | Self::Child(_))
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
struct NodeMeta {
    magic: [u8; 8],
    dag_id: u64,
    next_edge_id: u64,
    state: State,
}

impl NodeMeta {
    fn new(dag_id: u64, state: State) -> Self {
        Self {
            magic: MAGIC,
            dag_id,
            next_edge_id: 1,
            state,
        }
    }
    fn bytes(self) -> Vec<u8> {
        postcard::to_allocvec(&self).expect("DAG metadata encoding")
    }
}

fn edge_key(edge: u64) -> [u8; 9] {
    let mut key = [1; 9];
    key[1..].copy_from_slice(&edge.to_be_bytes());
    key
}

fn data_key(key: &[u8]) -> Vec<u8> {
    let mut row = Vec::with_capacity(key.len() + 1);
    row.push(DATA[0]);
    row.extend_from_slice(key);
    row
}
fn live_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut tagged = Vec::with_capacity(bytes.len() + 1);
    tagged.push(1);
    tagged.extend_from_slice(bytes);
    tagged
}
fn value(bytes: &[u8]) -> Option<&[u8]> {
    match bytes {
        [0] => None,
        [1, value @ ..] => Some(value),
        _ => panic!("invalid DAG data row"),
    }
}

// External edge tokens are 128-bit little-endian integers.
fn token(edge: u64) -> Vec<u8> {
    u128::from(edge).to_le_bytes().to_vec()
}

fn open_prefix(ns: &Namespace, id: u64) -> Result<MapxRaw> {
    crate::common::open_raw(InstanceId::new(id, ns.id()))
}

/// A node in a persistent overlay tree. Child lookups fall back to their
/// parent. Serialize structural changes to a connected tree externally.
/// Cloning deep-copies its connected component, including recovery links.
#[derive(Debug)]
pub struct DagMapRaw {
    store: MapxRaw,
    dag_id: u64,
}

impl Serialize for DagMapRaw {
    fn serialize<S: serde::Serializer>(&self, s: S) -> StdResult<S::Ok, S::Error> {
        self.serialize_as::<Self, _>(s)
    }
}
impl<'de> Deserialize<'de> for DagMapRaw {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> StdResult<Self, D::Error> {
        crate::common::deserialize_prefix_handle::<Self, _, _>(d, Self::from_frame)
    }
}

impl DagMapRaw {
    pub(super) fn serialize_as<T, S: serde::Serializer>(
        &self,
        s: S,
    ) -> StdResult<S::Ok, S::Error> {
        let mut extra = [0; 9];
        extra[0] = LAYOUT;
        extra[1..].copy_from_slice(&self.dag_id.to_le_bytes());
        crate::common::serialize_prefix_handle::<T, _>(&self.store, &extra, s)
    }
    pub(super) fn from_frame(frame: PrefixFrame<'_>) -> Result<Self> {
        let PrefixFrame(store, [LAYOUT, dag_id @ ..]) = frame else {
            return Err(corrupt("unsupported DAG layout"));
        };
        let dag_id = u64::from_le_bytes(
            dag_id
                .try_into()
                .map_err(|_| corrupt("invalid DAG token"))?,
        );
        let node = Self { store, dag_id };
        node.meta()?;
        Ok(node)
    }
    fn meta(&self) -> Result<NodeMeta> {
        let raw = self
            .store
            .get(META)
            .ok_or_else(|| corrupt("missing DAG node metadata"))?;
        let (meta, rest) = postcard::take_from_bytes::<NodeMeta>(&raw)?;
        if !rest.is_empty() || meta.magic != MAGIC || meta.dag_id != self.dag_id {
            return Err(corrupt("DAG node identity mismatch"));
        }
        Ok(meta)
    }
    fn set_meta(&mut self, meta: NodeMeta) {
        self.store.insert(META, meta.bytes());
    }
    fn open_with_meta(&self, id: u64) -> Result<(Self, NodeMeta)> {
        let store = open_prefix(&self.namespace(), id)?;
        if !self.store.is_colocated_with(&store) {
            return Err(corrupt("DAG node routes to another shard"));
        }
        let node = Self {
            store,
            dag_id: self.dag_id,
        };
        let meta = node.meta()?;
        Ok((node, meta))
    }
    fn open(&self, id: u64) -> Result<Self> {
        self.open_with_meta(id).map(|(node, _)| node)
    }
    fn id(&self) -> u64 {
        self.instance_id().map_id
    }
    fn links(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.store.range(EDGE..DATA).map(|(k, v)| {
            let edge =
                u64::from_be_bytes(k[1..].try_into().expect("invalid DAG edge key"));
            let map = u64::from_le_bytes(v.try_into().expect("invalid DAG child id"));
            (edge, map)
        })
    }
    fn edge_target(&self, edge: u64) -> Option<u64> {
        self.store
            .get(edge_key(edge))
            .map(|v| u64::from_le_bytes(v.try_into().expect("invalid DAG child id")))
    }
    fn owned(&self, edge: u64, id: u64) -> Option<Self> {
        let (child, meta) = self.open_with_meta(id).ok()?;
        (meta.state.owner()
            == Some(Parent {
                map: self.id(),
                edge,
            }))
        .then_some(child)
    }
    fn register(&mut self, mut meta: NodeMeta, child: u64) -> Result<u64> {
        let edge = meta.next_edge_id;
        meta.next_edge_id = edge
            .checked_add(1)
            .ok_or_else(|| corrupt("DAG edge id exhausted"))?;
        let mut batch = self.store.batch();
        batch.insert(META, meta.bytes());
        batch.insert(edge_key(edge), child.to_le_bytes());
        batch.commit()?;
        Ok(edge)
    }
    /// Creates a root or attaches a child on its parent's shard.
    pub fn new(parent: Option<&mut Self>) -> Self {
        match parent {
            Some(p) => {
                let meta = p.meta().expect("invalid DAG parent");
                assert!(meta.state.live(), "DAG parent has been consumed");
                assert_ne!(meta.next_edge_id, u64::MAX, "DAG edge id exhausted");
                let mut node = Self {
                    store: p.store.new_colocated(),
                    dag_id: p.dag_id,
                };
                node.set_meta(NodeMeta::new(
                    p.dag_id,
                    State::Child(Parent {
                        map: p.id(),
                        edge: meta.next_edge_id,
                    }),
                ));
                p.register(meta, node.id()).expect("DAG child registration");
                node
            }
            None => {
                let dag_id = rand::random();
                let mut node = Self {
                    store: MapxRaw::new(),
                    dag_id,
                };
                node.set_meta(NodeMeta::new(dag_id, State::Root));
                node
            }
        }
    }
    /// Creates a root in this namespace, or attaches a child to its parent.
    pub fn new_in(ns: &Namespace, parent: Option<&mut Self>) -> Self {
        if let Some(p) = parent.as_ref() {
            debug_assert_eq!(ns.id(), p.namespace().id());
        }
        ns.scope(|| Self::new(parent))
    }
    /// Owning namespace.
    pub fn namespace(&self) -> Namespace {
        self.store.namespace()
    }
    /// Stable prefix identity.
    pub fn instance_id(&self) -> InstanceId {
        self.store.instance_id()
    }
    /// Persists the typed handle for later recovery.
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }
    /// Restores a saved node and verifies its graph token.
    pub fn from_meta(id: impl Into<InstanceId>) -> Result<Self> {
        crate::common::load_instance_meta_checked(id.into(), Self::instance_id)
    }
    /// Whether these handles identify the same node.
    pub fn is_the_same_instance(&self, other: &Self) -> bool {
        self.instance_id() == other.instance_id() && self.dag_id == other.dag_id
    }
    /// Whether the node has no parent, children, or live local entries.
    pub fn is_dead(&self) -> bool {
        !matches!(self.meta().expect("DAG metadata").state, State::Child(_))
            && self.no_children()
            && self
                .store
                .range(DATA..END)
                .all(|(_, v)| value(&v).is_none())
    }
    /// Whether the child registry is empty.
    pub fn no_children(&self) -> bool {
        self.links().next().is_none()
    }
    /// Registry tokens accepted by the selective prune methods.
    pub fn child_ids(&self) -> Vec<RawBytes> {
        self.links().map(|(e, _)| token(e)).collect()
    }
    /// Returns a direct child's registry token in constant time.
    pub fn child_id(&self, child: &Self) -> Option<RawBytes> {
        if self.dag_id != child.dag_id || !self.store.is_colocated_with(&child.store) {
            return None;
        }
        let owner = child.meta().ok()?.state.owner()?;
        (owner.map == self.id() && self.edge_target(owner.edge) == Some(child.id()))
            .then(|| token(owner.edge))
    }
    /// Reads the closest override in the parent chain.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let key = data_key(key.as_ref());
        if let Some(v) = self.store.get(&key) {
            return value(&v).map(<[u8]>::to_vec);
        }
        let mut seen = HashSet::new();
        let mut id = self.id();
        let mut state = self.meta().ok()?.state;
        while let State::Child(owner) = state {
            if !seen.insert(id) {
                return None;
            }
            let (p, meta) = self.open_with_meta(owner.map).ok()?;
            if p.edge_target(owner.edge) != Some(id) {
                return None;
            }
            if let Some(v) = p.store.get(&key) {
                return value(&v).map(<[u8]>::to_vec);
            }
            id = p.id();
            state = meta.state;
        }
        None
    }
    /// Edits a local value; parent rows and tombstones are excluded.
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_>> {
        let key = data_key(key.as_ref());
        let mut val = self.store.get(&key)?;
        value(&val)?;
        val.remove(0);
        Some(ValueMut {
            value: val,
            store: &mut self.store,
            key,
            dirty: false,
            unwind: UnwindMark::new(),
        })
    }
    /// Stores a local override, including an empty byte string.
    pub fn insert(&mut self, key: impl AsRef<[u8]>, v: impl AsRef<[u8]>) {
        assert!(
            self.meta().expect("DAG metadata").state.live(),
            "DAG node has been consumed"
        );
        self.store
            .insert(data_key(key.as_ref()), live_bytes(v.as_ref()));
    }
    /// Hides the key locally and during fallback.
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        assert!(
            self.meta().expect("DAG metadata").state.live(),
            "DAG node has been consumed"
        );
        self.store.insert(data_key(key.as_ref()), [0]);
    }

    fn destroy_subtree(&mut self) -> Result<()> {
        let owner = self.meta()?.state.owner();
        let mut pending = vec![self.open(self.id())?];
        let mut marked = Vec::new();
        let mut seen = HashSet::new();
        while let Some(mut node) = pending.pop() {
            if !seen.insert(node.id()) {
                continue;
            }
            let mut meta = node.meta()?;
            if meta.state == State::Destroyed {
                continue;
            }
            pending.extend(node.links().filter_map(|(edge, id)| node.owned(edge, id)));
            meta.state = State::Destroying(meta.state.owner());
            let mut batch = node.store.batch();
            batch.insert(META, meta.bytes());
            batch.delete_range(&DATA, &END);
            batch.commit()?;
            marked.push(node);
        }
        // Descendants are finalized before removing their discovery edge.
        // The target retains its owner through unlink, including on retry.
        for mut node in marked.into_iter().rev() {
            if node.id() == self.id() {
                continue;
            }
            node.finish_destroy()?;
        }
        if let Some(link) = owner
            && let Ok(mut p) = self.open(link.map)
            && p.edge_target(link.edge) == Some(self.id())
        {
            p.store.remove(edge_key(link.edge));
        }
        self.finish_destroy()
    }
    fn finish_destroy(&mut self) -> Result<()> {
        let mut meta = self.meta()?;
        meta.state = State::Destroyed;
        let mut batch = self.store.batch();
        batch.delete_range(&EDGE, &DATA);
        batch.insert(META, meta.bytes());
        batch.commit()
    }
    /// Destroys the owned subtree and durably unlinks it from its parent.
    pub fn destroy(&mut self) {
        self.destroy_subtree()
            .and_then(|()| self.store.try_sync_wal())
            .expect("DAG destruction")
    }
    fn remove_branch(&mut self, edge: u64, child: u64) -> Result<()> {
        if let Some(mut node) = self.owned(edge, child) {
            return node.destroy_subtree();
        }
        self.store.remove(edge_key(edge));
        Ok(())
    }
    fn prune_children(
        &mut self,
        targets: &[impl AsRef<DagMapId>],
        include: bool,
    ) -> Result<()> {
        let set: HashSet<_> = targets.iter().map(AsRef::as_ref).collect();
        for (e, id) in self.links().collect::<Vec<_>>() {
            if set.contains(token(e).as_slice()) == include {
                self.remove_branch(e, id)?
            }
        }
        self.store.try_sync_wal()
    }
    /// Removes children with a listed registry token.
    pub fn prune_children_include(&mut self, targets: &[impl AsRef<DagMapId>]) {
        self.prune_children(targets, true).expect("DAG child prune");
    }
    /// Removes children whose tokens are absent from the list.
    pub fn prune_children_exclude(&mut self, targets: &[impl AsRef<DagMapId>]) {
        self.prune_children(targets, false)
            .expect("DAG child prune");
    }

    // Head first; the root is returned separately. Clearing nodes retain
    // their original owner link so an interrupted marker pass can resume.
    fn mainline(self) -> Result<(Vec<Self>, Self)> {
        let mut line = Vec::new();
        let mut seen = HashSet::new();
        let mut node = self;
        loop {
            if !seen.insert(node.id()) {
                return Err(corrupt("DAG parent cycle"));
            }
            let state = node.meta()?.state;
            let owner = match state {
                State::Root => return Ok((line, node)),
                State::Child(p) | State::Clearing { owner: Some(p), .. } => p,
                _ => return Err(corrupt("mainline does not reach a live root")),
            };
            let p = node.open(owner.map)?;
            if p.edge_target(owner.edge) != Some(node.id()) {
                match (state, p.meta()?.state) {
                    (State::Clearing { genesis, .. }, State::Root)
                        if genesis == p.id() => {}
                    (
                        State::Clearing { genesis, .. },
                        State::Clearing { genesis: other, .. },
                    ) if genesis == other => {}
                    (
                        State::Clearing { genesis, .. },
                        State::Destroyed | State::Destroying(_),
                    ) => {
                        let root = node.open(genesis)?;
                        if root.meta()?.state != State::Root {
                            return Err(corrupt("invalid clearing target"));
                        }
                        line.push(node);
                        return Ok((line, root));
                    }
                    _ => return Err(corrupt("mainline owner mismatch")),
                }
            }
            line.push(node);
            node = p;
        }
    }
    fn fold(line: &[Self], root: &mut Self) -> Result<()> {
        let mut rows = StagedRows::new();
        for node in line.iter().rev() {
            for (k, v) in node.store.range(DATA..END) {
                if value(&v).is_none() {
                    rows.del(k)
                } else {
                    rows.put(k, v)
                }
            }
        }
        rows.commit(&mut root.store)
    }
    fn reparent(&self, root: &mut Self) -> Result<()> {
        let registered: HashMap<_, _> = root.links().map(|(e, id)| (id, e)).collect();
        for (e, id) in self.links().collect::<Vec<_>>() {
            let Some(mut child) = self.owned(e, id) else {
                continue;
            };
            let mut meta = child.meta()?;
            if !meta.state.live() {
                if !matches!(meta.state, State::Clearing { genesis, .. } if genesis == root.id())
                {
                    child.destroy_subtree()?;
                }
                continue;
            }
            let edge = match registered.get(&id) {
                Some(&edge) => edge,
                None => root.register(root.meta()?, id)?,
            };
            meta.state = State::Child(Parent {
                map: root.id(),
                edge,
            });
            child.set_meta(meta);
        }
        Ok(())
    }
    fn mark(line: &mut [Self], genesis: u64) -> Result<()> {
        for node in line {
            let mut meta = node.meta()?;
            match meta.state {
                State::Child(p) => {
                    meta.state = State::Clearing {
                        owner: Some(p),
                        genesis,
                    };
                    node.set_meta(meta);
                }
                State::Clearing { genesis: id, .. } if id == genesis => {}
                _ => return Err(corrupt("invalid consumed mainline")),
            }
        }
        Ok(())
    }
    // A retry through an intermediate marker can start below the original
    // head. Follow remaining discovery edges before clearing any registry.
    // An interrupted pass has already retained these edges until every
    // consumed data row was wiped.
    fn collect_marked(line: &mut Vec<Self>, root: &Self) -> Result<()> {
        let mut seen: HashSet<_> = line.iter().map(Self::id).collect();
        let mut i = 0;
        while i <= line.len() {
            let owner = if i == 0 { root } else { &line[i - 1] };
            let mut found = Vec::new();
            for (edge, id) in owner.links() {
                if seen.contains(&id) {
                    continue;
                }
                let Some(node) = owner.owned(edge, id) else {
                    continue;
                };
                if matches!(node.meta()?.state, State::Clearing{genesis,..} if genesis==root.id())
                {
                    seen.insert(id);
                    found.push(node);
                }
            }
            line.extend(found);
            i += 1;
        }
        Ok(())
    }
    fn clear_line(line: &mut [Self], root: &mut Self) -> Result<()> {
        for node in line.iter_mut() {
            let mut batch = node.store.batch();
            batch.delete_range(&DATA, &END);
            batch.commit()?;
            #[cfg(test)]
            test::after_clear_step();
        }
        for node in line.iter_mut() {
            let mut batch = node.store.batch();
            batch.delete_range(&EDGE, &DATA);
            batch.commit()?;
            #[cfg(test)]
            test::after_clear_step();
        }
        let consumed: HashSet<_> = line.iter().map(Self::id).collect();
        let stale: Vec<_> = root
            .links()
            .filter(|(_, id)| consumed.contains(id))
            .map(|(edge, _)| edge)
            .collect();
        if !stale.is_empty() {
            let mut batch = root.store.batch();
            for edge in stale {
                batch.remove(edge_key(edge));
            }
            batch.commit()?;
        }
        Ok(())
    }
    /// Atomically folds the mainline overlay into the original root, reparents
    /// the head's live children, then clears consumed rows. A clearing marker
    /// means the fold and reparenting are already recorded in the shared WAL.
    /// Returns after one shard WAL fence. Retry on the original head.
    pub fn prune(self) -> Result<Self> {
        ensure_writable(&self.namespace(), "DAG pruning")?;
        let (mut line, mut root) = self.mainline()?;
        if let Some((head, ancestors)) = line.split_first_mut() {
            if head.meta()?.state.live() {
                let protected: HashSet<_> = std::iter::once(head.id())
                    .chain(ancestors.iter().map(Self::id))
                    .chain(head.links().map(|(_, id)| id))
                    .collect();
                for node in ancestors.iter_mut().chain(std::iter::once(&mut root)) {
                    let doomed = node
                        .links()
                        .filter(|(_, id)| !protected.contains(id))
                        .collect::<Vec<_>>();
                    for (e, id) in doomed {
                        node.remove_branch(e, id)?;
                    }
                }
                Self::fold(&line, &mut root)?;
                line[0].reparent(&mut root)?;
            }
            Self::collect_marked(&mut line, &root)?;
            Self::mark(&mut line, root.id())?;
            Self::clear_line(&mut line, &mut root)?;
        }
        root.store.try_sync_wal()?;
        Ok(root)
    }

    /// Copies all reachable nodes and links, including pending recovery links,
    /// to fresh prefixes on a single shard in the destination namespace.
    pub fn clone_in(&self, ns: &Namespace) -> Result<Self> {
        ensure_writable(ns, "DAG clone")?;
        let mut pending = vec![self.open(self.id())?];
        let mut sources = HashMap::new();
        while let Some(node) = pending.pop() {
            if sources.contains_key(&node.id()) {
                continue;
            }
            let meta = node.meta()?;
            if let Some(p) = meta.state.owner() {
                let parent = node.open(p.map)?;
                if matches!(meta.state, State::Child(_))
                    && parent.edge_target(p.edge) != Some(node.id())
                {
                    return Err(corrupt("DAG clone owner mismatch"));
                }
                pending.push(parent);
            }
            if let State::Clearing { genesis, .. } = meta.state {
                let root = node.open(genesis)?;
                if root.meta()?.state != State::Root {
                    return Err(corrupt("DAG clone clearing target mismatch"));
                }
                pending.push(root);
            }
            for (e, id) in node.links() {
                if let Some(child) = node.owned(e, id) {
                    pending.push(child)
                }
            }
            sources.insert(node.id(), node);
        }
        let dag_id = rand::random();
        let mut copies = HashMap::<u64, Self>::new();
        let copy_graph = (|| -> Result<()> {
            for (&id, node) in &sources {
                let store = match copies.values().next() {
                    None => node.store.clone_in(ns)?,
                    Some(anchor) => node.store.clone_colocated(&anchor.store)?,
                };
                copies.insert(id, Self { store, dag_id });
            }
            let ids: HashMap<_, _> =
                copies.iter().map(|(old, new)| (*old, new.id())).collect();
            for (id, node) in sources {
                let mut meta = node.meta()?;
                meta.dag_id = dag_id;
                let remap = |mut p: Parent| {
                    p.map = ids[&p.map];
                    p
                };
                meta.state = match meta.state {
                    State::Child(p) => State::Child(remap(p)),
                    State::Clearing { owner, genesis } => State::Clearing {
                        owner: owner.map(remap),
                        genesis: ids[&genesis],
                    },
                    State::Destroying(owner) => State::Destroying(owner.map(remap)),
                    s => s,
                };
                let copy = copies.get_mut(&id).expect("allocated");
                // All user rows have already been copied in bounded batches.
                // Remap structural links in place on the unpublished graph.
                for (edge, child) in node.links() {
                    let mut batch = copy.store.batch();
                    if let Some(target) = ids.get(&child) {
                        batch.insert(edge_key(edge), target.to_le_bytes());
                    } else {
                        batch.remove(edge_key(edge));
                    }
                    batch.commit()?;
                }
                let mut batch = copy.store.batch();
                batch.insert(META, meta.bytes());
                batch.commit()?;
            }
            Ok(())
        })();
        if let Err(err) = copy_graph {
            for node in copies.values_mut() {
                let _ = node.store.batch_wiped().commit();
            }
            return Err(err);
        }
        Ok(copies.remove(&self.id()).expect("allocated"))
    }
}

impl Clone for DagMapRaw {
    fn clone(&self) -> Self {
        self.clone_in(&self.namespace()).expect("DAG deep clone")
    }
}

/// A buffered edit of this node's local data.
#[derive(Debug)]
pub struct ValueMut<'a> {
    value: RawBytes,
    store: &'a mut MapxRaw,
    key: Vec<u8>,
    dirty: bool,
    unwind: UnwindMark,
}
impl ValueMut<'_> {
    fn write(&mut self) -> Result<()> {
        if self.dirty {
            self.dirty = false;
            self.value.insert(0, 1);
            let mut batch = self.store.batch();
            batch.insert(&self.key, &self.value);
            batch.commit()?;
        }
        Ok(())
    }
    /// Publishes a local edit and returns any storage error.
    pub fn commit(mut self) -> Result<()> {
        self.write()
    }
}
impl crate::common::guard::RawValueSlot for ValueMut<'_> {
    fn commit(self) -> Result<()> {
        self.commit()
    }
}
impl Drop for ValueMut<'_> {
    fn drop(&mut self) {
        if !self.unwind.interrupted() {
            self.write().expect("DAG value write-back failed");
        }
    }
}
impl Deref for ValueMut<'_> {
    type Target = RawBytes;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl DerefMut for ValueMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.value
    }
}
