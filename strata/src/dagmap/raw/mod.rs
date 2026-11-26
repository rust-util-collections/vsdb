//! Persistent raw-key and raw-value overlays on a single shard.
#![forbid(unsafe_code)]

#[cfg(test)]
mod test;

use crate::{
    DagMapId, Namespace,
    common::{
        InstanceId, PrefixFrame, UnwindMark, ensure_writable,
        error::{Result, VsdbError},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::{Deref, DerefMut},
    result::Result as StdResult,
};
use vsdb_core::{ColocatedEdits, MapxRaw, common::RawBytes};

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

// What a prune entering at a live node finds among its owned descendants.
enum BelowHead {
    Clean,
    // The deepest marked descendant on its line: an earlier release marked
    // that line only in part.
    Marked(DagMapRaw),
    // A descendant whose line was folded and whose children were reparented
    // by an earlier release before any marker was written.
    Interrupted(DagMapRaw),
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
    try_value(bytes).expect("invalid DAG data row")
}
fn try_value(bytes: &[u8]) -> Result<Option<&[u8]>> {
    match bytes {
        [0] => Ok(None),
        [1, value @ ..] => Ok(Some(value)),
        _ => Err(corrupt("invalid DAG data row")),
    }
}

// A foreign magic, dag id, missing record, or other shard is not this node.
// Undecodable metadata is damage and must not look like absence.
fn authentication_failure(err: &VsdbError) -> bool {
    match err {
        VsdbError::Corrupt { detail } => matches!(
            detail.as_str(),
            "DAG node identity mismatch"
                | "missing DAG node metadata"
                | "DAG node routes to another shard"
        ),
        _ => false,
    }
}

// Infallible reads treat a node that fails authentication as absent.
// Storage failures and metadata that does not decode are fatal.
fn authenticated<T>(read: Result<T>) -> Option<T> {
    match read {
        Ok(value) => Some(value),
        Err(err) if authentication_failure(&err) => None,
        Err(err) => panic!("vsdb: DAG metadata read failed: {err}"),
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
        #[cfg(test)]
        test::before_meta_read(self.id())?;
        self.store
            .try_get_with(META, |raw| {
                let (meta, rest) = postcard::take_from_bytes::<NodeMeta>(raw)?;
                if !rest.is_empty() {
                    return Err(corrupt("trailing DAG metadata"));
                }
                if meta.magic != MAGIC || meta.dag_id != self.dag_id {
                    return Err(corrupt("DAG node identity mismatch"));
                }
                Ok(meta)
            })?
            .ok_or_else(|| corrupt("missing DAG node metadata"))
    }
    fn set_meta(&mut self, meta: NodeMeta) -> Result<()> {
        let mut batch = self.store.batch();
        batch.insert(META, meta.bytes());
        batch.commit()
    }
    fn unlink(&mut self, edge: u64) -> Result<()> {
        let mut batch = self.store.batch();
        batch.remove(edge_key(edge));
        batch.commit()
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
    fn try_links(&self) -> Result<Vec<(u64, u64)>> {
        let mut cursor = self.store.try_range(EDGE..DATA)?;
        let mut out = Vec::new();
        while let Some((k, v)) = cursor.try_step_next()? {
            let edge = k
                .get(1..)
                .and_then(|s| s.try_into().ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| corrupt("invalid DAG edge key"))?;
            let map = v
                .try_into()
                .map(u64::from_le_bytes)
                .map_err(|_| corrupt("invalid DAG child id"))?;
            out.push((edge, map));
        }
        Ok(out)
    }
    fn edge_target(&self, edge: u64) -> Option<u64> {
        self.store
            .get(edge_key(edge))
            .map(|v| u64::from_le_bytes(v.try_into().expect("invalid DAG child id")))
    }
    fn try_edge_target(&self, edge: u64) -> Result<Option<u64>> {
        self.store.try_get_with(edge_key(edge), |v| {
            v.try_into()
                .map(u64::from_le_bytes)
                .map_err(|_| corrupt("invalid DAG child id"))
        })
    }
    #[cfg(test)]
    fn owned(&self, edge: u64, id: u64) -> Option<Self> {
        self.try_owned(edge, id).ok().flatten()
    }
    fn try_owned(&self, edge: u64, id: u64) -> Result<Option<Self>> {
        let (child, meta) = match self.open_with_meta(id) {
            Ok(pair) => pair,
            Err(err) if authentication_failure(&err) => return Ok(None),
            Err(err) => return Err(err),
        };
        Ok((meta.state.owner()
            == Some(Parent {
                map: self.id(),
                edge,
            }))
        .then_some(child))
    }
    #[cfg(test)]
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
                let mut meta = p.meta().expect("invalid DAG parent");
                assert!(meta.state.live(), "DAG parent has been consumed");
                assert_ne!(meta.next_edge_id, u64::MAX, "DAG edge id exhausted");
                let mut node = Self {
                    store: p.store.new_colocated(),
                    dag_id: p.dag_id,
                };
                let edge = meta.next_edge_id;
                meta.next_edge_id += 1;
                let child_meta =
                    NodeMeta::new(p.dag_id, State::Child(Parent { map: p.id(), edge }));
                let child_id = node.id().to_le_bytes();
                p.store
                    .insert_colocated(
                        &mut node.store,
                        &[(&META, &meta.bytes()), (&edge_key(edge), &child_id)],
                        &[(&META, &child_meta.bytes())],
                    )
                    .expect("DAG child registration");
                node
            }
            None => {
                let dag_id = rand::random();
                let mut node = Self {
                    store: MapxRaw::new(),
                    dag_id,
                };
                node.set_meta(NodeMeta::new(dag_id, State::Root))
                    .expect("DAG root metadata");
                node
            }
        }
    }
    /// Creates a root in `ns`. With a parent, the child inherits the parent's
    /// namespace and shard; `ns` only selects placement for new roots.
    pub fn new_in(ns: &Namespace, parent: Option<&mut Self>) -> Self {
        match parent {
            Some(parent) => Self::new(Some(parent)),
            None => ns.scope(|| Self::new(None)),
        }
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
        crate::common::load_instance_meta_checked(
            id.into(),
            Self::from_frame,
            Self::instance_id,
        )
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
    ///
    /// # Panics
    ///
    /// Panics if reading the child's metadata fails in storage or the
    /// metadata row does not decode. A child that fails authentication is
    /// absent.
    pub fn child_id(&self, child: &Self) -> Option<RawBytes> {
        if self.dag_id != child.dag_id || !self.store.is_colocated_with(&child.store) {
            return None;
        }
        let owner = authenticated(child.meta())?.state.owner()?;
        (owner.map == self.id() && self.edge_target(owner.edge) == Some(child.id()))
            .then(|| token(owner.edge))
    }
    /// Reads the closest override in the parent chain.
    ///
    /// # Panics
    ///
    /// Panics on a storage read failure or metadata that does not decode.
    /// A parent that fails authentication ends the lookup instead.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let key = data_key(key.as_ref());
        if let Some(v) = self.store.get(&key) {
            return value(&v).map(<[u8]>::to_vec);
        }
        let mut seen = HashSet::new();
        let mut id = self.id();
        let mut state = authenticated(self.meta())?.state;
        while let State::Child(owner) = state {
            if !seen.insert(id) {
                return None;
            }
            let (p, meta) = authenticated(self.open_with_meta(owner.map))?;
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
        self.try_get_mut(key.as_ref())
            .expect("DAG local value read")
    }
    pub(super) fn try_get_mut(&mut self, key: &[u8]) -> Result<Option<ValueMut<'_>>> {
        assert!(self.meta()?.state.live(), "DAG node has been consumed");
        let key = data_key(key);
        let Some(mut val) = self.store.try_get_with(&key, |bytes| Ok(bytes.to_vec()))?
        else {
            return Ok(None);
        };
        match val.as_slice() {
            [0] => return Ok(None),
            [1, ..] => {
                val.remove(0);
            }
            _ => return Err(corrupt("invalid DAG data row")),
        }
        Ok(Some(ValueMut {
            value: val,
            store: &mut self.store,
            key,
            dirty: false,
            unwind: UnwindMark::new(),
        }))
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
            for (edge, id) in node.try_links()? {
                if let Some(child) = node.try_owned(edge, id)? {
                    pending.push(child);
                }
            }
            let mut batch = node.store.batch();
            if !matches!(meta.state, State::Clearing { .. }) {
                meta.state = State::Destroying(meta.state.owner());
                batch.insert(META, meta.bytes());
            }
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
        if let Some(link) = owner {
            match self.open(link.map) {
                Ok(mut p) => {
                    if p.try_edge_target(link.edge)? == Some(self.id()) {
                        p.unlink(link.edge)?;
                    }
                }
                Err(err) if authentication_failure(&err) => {}
                Err(err) => return Err(err),
            }
        }
        self.finish_destroy()
    }
    fn finish_destroy(&mut self) -> Result<()> {
        let mut meta = self.meta()?;
        let mut batch = self.store.batch();
        batch.delete_range(&EDGE, &DATA);
        if !matches!(meta.state, State::Clearing { .. }) {
            meta.state = State::Destroyed;
            batch.insert(META, meta.bytes());
        }
        batch.commit()
    }
    /// Destroys the owned subtree and durably unlinks it from its parent.
    pub fn destroy(&mut self) {
        self.destroy_subtree()
            .and_then(|()| self.store.try_sync_wal())
            .expect("DAG destruction")
    }
    fn remove_branch(&mut self, edge: u64, child: u64) -> Result<()> {
        if let Some(mut node) = self.try_owned(edge, child)? {
            return node.destroy_subtree();
        }
        self.unlink(edge)
    }
    fn prune_children(
        &mut self,
        targets: &[impl AsRef<DagMapId>],
        include: bool,
    ) -> Result<()> {
        let set: HashSet<_> = targets.iter().map(AsRef::as_ref).collect();
        for (e, id) in self.try_links()? {
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
            if p.try_edge_target(owner.edge)? != Some(node.id()) {
                match (state, p.meta()?.state) {
                    (State::Clearing { genesis, .. }, State::Root)
                        if genesis == p.id() => {}
                    (State::Clearing { .. }, State::Child(_)) => {}
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
    // Earlier releases published the fold and each reparented child in
    // separate batches before writing any marker. Tests rebuild those
    // on-disk states with these steps.
    #[cfg(test)]
    fn fold(line: &[Self], root: &mut Self) -> Result<()> {
        let mut rows = crate::common::staged::StagedRows::new();
        for node in line.iter().rev() {
            let mut cursor = node.store.try_range(DATA..END)?;
            while let Some((k, v)) = cursor.try_step_next()? {
                if try_value(v)?.is_none() {
                    rows.del(k.to_vec())
                } else {
                    rows.put(k.to_vec(), v.to_vec())
                }
            }
        }
        rows.commit(&mut root.store)
    }
    #[cfg(test)]
    fn reparent(&self, root: &mut Self) -> Result<()> {
        let registered: HashMap<_, _> = root
            .try_links()?
            .into_iter()
            .map(|(e, id)| (id, e))
            .collect();
        for (e, id) in self.try_links()? {
            let Some(mut child) = self.try_owned(e, id)? else {
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
            child.set_meta(meta)?;
        }
        Ok(())
    }
    // One co-located batch publishes the folded rows, the surviving children
    // and a clearing marker on every mainline node, so no folded node stays
    // live. A fresh prune has already removed the ancestors' side branches,
    // so only the head has children to keep. When finishing a line that an
    // earlier release marked only in part, children created under its
    // unmarked nodes since then are kept as well. Non-live children are
    // finished beforehand.
    fn fold_and_mark(line: &mut [Self], root: &mut Self) -> Result<()> {
        let genesis = root.id();
        let mut root_rows = BTreeMap::new();
        for node in line.iter().rev() {
            let mut cursor = node.store.try_range(DATA..END)?;
            while let Some((k, v)) = cursor.try_step_next()? {
                let value = try_value(v)?.is_some().then(|| v.to_vec());
                root_rows.insert(k.to_vec(), value);
            }
        }
        let registered: HashMap<_, _> = root
            .try_links()?
            .into_iter()
            .map(|(e, id)| (id, e))
            .collect();
        let mut root_meta = root.meta()?;
        let first_edge = root_meta.next_edge_id;
        let members: HashSet<_> = line.iter().map(Self::id).collect();
        let mut survivors = Vec::new();
        let mut survivor_meta = Vec::new();
        for node in line.iter() {
            for (e, id) in node.try_links()? {
                if members.contains(&id) {
                    continue;
                }
                let Some(mut child) = node.try_owned(e, id)? else {
                    continue;
                };
                let mut meta = child.meta()?;
                if !meta.state.live() {
                    if !matches!(meta.state, State::Clearing { genesis: g, .. } if g == genesis)
                    {
                        child.destroy_subtree()?;
                    }
                    continue;
                }
                let edge = match registered.get(&id) {
                    Some(&edge) => edge,
                    None => {
                        let edge = root_meta.next_edge_id;
                        root_meta.next_edge_id = edge
                            .checked_add(1)
                            .ok_or_else(|| corrupt("DAG edge id exhausted"))?;
                        root_rows.insert(
                            edge_key(edge).to_vec(),
                            Some(id.to_le_bytes().to_vec()),
                        );
                        edge
                    }
                };
                meta.state = State::Child(Parent { map: genesis, edge });
                survivors.push(child);
                survivor_meta.push(meta.bytes());
            }
        }
        if root_meta.next_edge_id != first_edge {
            root_rows.insert(META.to_vec(), Some(root_meta.bytes()));
        }
        let mut markers = Vec::with_capacity(line.len());
        for node in line.iter() {
            let mut meta = node.meta()?;
            markers.push(match meta.state {
                State::Child(p) => {
                    meta.state = State::Clearing {
                        owner: Some(p),
                        genesis,
                    };
                    Some(meta.bytes())
                }
                State::Clearing { genesis: g, .. } if g == genesis => None,
                _ => return Err(corrupt("invalid consumed mainline")),
            });
        }

        type Edit<'a> = (&'a [u8], Option<&'a [u8]>);
        let meta_key: &[u8] = &META;
        let root_edits: Vec<Edit<'_>> = root_rows
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_deref()))
            .collect();
        let survivor_edits: Vec<[Edit<'_>; 1]> = survivor_meta
            .iter()
            .map(|meta| [(meta_key, Some(meta.as_slice()))])
            .collect();
        let marker_edits: Vec<Option<[Edit<'_>; 1]>> = markers
            .iter()
            .map(|meta| {
                meta.as_ref()
                    .map(|meta| [(meta_key, Some(meta.as_slice()))])
            })
            .collect();
        let mut writes: Vec<(&mut MapxRaw, ColocatedEdits<'_>)> =
            vec![(&mut root.store, &root_edits)];
        writes.extend(
            survivors
                .iter_mut()
                .zip(&survivor_edits)
                .map(|(node, edits)| (&mut node.store, &edits[..])),
        );
        writes.extend(
            line.iter_mut()
                .zip(&marker_edits)
                .filter_map(|(node, edits)| {
                    Some((&mut node.store, &edits.as_ref()?[..]))
                }),
        );
        #[cfg(test)]
        test::before_prune_publication()?;
        MapxRaw::write_colocated(&mut writes)
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
                    node.set_meta(meta)?;
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
            for (edge, id) in owner.try_links()? {
                if seen.contains(&id) {
                    continue;
                }
                let Some(node) = owner.try_owned(edge, id)? else {
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
    fn any_live(line: &[Self]) -> Result<bool> {
        for node in line {
            if node.meta()?.state.live() {
                return Ok(true);
            }
        }
        Ok(false)
    }
    // Whether `id`, reached through one of this node's discovery edges, is
    // now a live child of the root.
    fn reparented_to(&self, id: u64, genesis: u64) -> Result<bool> {
        match self.open_with_meta(id) {
            Ok((_, meta)) => Ok(matches!(
                meta.state,
                State::Child(Parent { map, .. }) if map == genesis
            )),
            Err(err) if authentication_failure(&err) => Ok(false),
            Err(err) => Err(err),
        }
    }
    // Looks below a live prune entry for the state an interrupted prune by
    // an earlier release left: a marked descendant, or a live descendant
    // whose children were already moved to the root. The walk uses an
    // explicit stack because a head can own an arbitrarily deep chain.
    fn inspect_below_head(line: &[Self], genesis: u64) -> Result<BelowHead> {
        struct Frame {
            // `None` stands for the head, which stays in `line`.
            node: Option<DagMapRaw>,
            links: std::vec::IntoIter<(u64, u64)>,
            live: bool,
            marked: bool,
        }

        let mut seen: HashSet<_> = line.iter().map(Self::id).collect();
        let mut stack = vec![Frame {
            node: None,
            links: line[0].try_links()?.into_iter(),
            live: true,
            marked: false,
        }];
        while let Some(frame) = stack.last_mut() {
            if let Some((edge, id)) = frame.links.next() {
                if !seen.insert(id) {
                    continue;
                }
                let owner = frame.node.as_ref().unwrap_or(&line[0]);
                let Some(child) = owner.try_owned(edge, id)? else {
                    // Releases that published the fold and the reparented
                    // children before any marker left a live descendant
                    // whose edge now names a child of the root.
                    if frame.node.is_some()
                        && frame.live
                        && owner.reparented_to(id, genesis)?
                    {
                        let node = stack.pop().and_then(|frame| frame.node);
                        return Ok(BelowHead::Interrupted(
                            node.expect("descendant frames own their node"),
                        ));
                    }
                    continue;
                };
                let state = child.meta()?.state;
                let marked =
                    matches!(state, State::Clearing { genesis: g, .. } if g == genesis);
                let links = child.try_links()?.into_iter();
                stack.push(Frame {
                    node: Some(child),
                    links,
                    live: state.live(),
                    marked,
                });
                continue;
            }
            // Children leave the stack before their parents, so the first
            // marked node found is the deepest one on its line.
            if let Frame {
                node: Some(node),
                marked: true,
                ..
            } = stack.pop().expect("nonempty walk stack")
            {
                return Ok(BelowHead::Marked(node));
            }
        }
        Ok(BelowHead::Clean)
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
            .try_links()?
            .into_iter()
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
    /// Folds the mainline overlay into the original root, reparents the
    /// head's live children and marks every consumed node in one batch, then
    /// clears consumed rows. A clearing marker means the fold and reparenting
    /// are already recorded in the shared WAL. Returns after one shard WAL
    /// fence.
    ///
    /// An error consumes this handle. To finish an interrupted prune, prune
    /// the original head again from a saved or serialized handle.
    pub fn prune(self) -> Result<Self> {
        ensure_writable(&self.namespace(), "DAG pruning")?;
        let (mut line, mut root) = self.mainline()?;
        if !line.is_empty() {
            let head_live = line[0].meta()?.state.live();
            if head_live {
                match Self::inspect_below_head(&line, root.id())? {
                    // Finish the interrupted line from its own head.
                    BelowHead::Marked(head) | BelowHead::Interrupted(head) => {
                        return head.prune();
                    }
                    BelowHead::Clean => {}
                }
                let (head, ancestors) = line.split_first_mut().unwrap();
                let head_links = head.try_links()?;
                let protected: HashSet<_> = std::iter::once(head.id())
                    .chain(ancestors.iter().map(Self::id))
                    .chain(head_links.into_iter().map(|(_, id)| id))
                    .collect();
                for node in ancestors.iter_mut().chain(std::iter::once(&mut root)) {
                    let doomed = node
                        .try_links()?
                        .into_iter()
                        .filter(|(_, id)| !protected.contains(id))
                        .collect::<Vec<_>>();
                    for (e, id) in doomed {
                        node.remove_branch(e, id)?;
                    }
                }
                Self::fold_and_mark(&mut line, &mut root)?;
            } else if Self::any_live(&line)? {
                // An earlier release marked only part of this line, so no
                // row has been wiped yet. Fold it again to keep edits and
                // children added to its unmarked nodes since then.
                Self::fold_and_mark(&mut line, &mut root)?;
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
                    && parent.try_edge_target(p.edge)? != Some(node.id())
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
            for (e, id) in node.try_links()? {
                if let Some(child) = node.try_owned(e, id)? {
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
                for (edge, child) in node.try_links()? {
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
