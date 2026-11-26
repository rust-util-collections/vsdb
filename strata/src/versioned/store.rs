//! Wire rows and atomic metadata edits for the single-prefix version store.

use super::{BranchId, Commit, CommitId, NO_COMMIT};
use crate::{
    InstanceId, Namespace, Result, VsdbError,
    basic::persistent_btree::NodeId,
    common::{load_meta_file, type_tag},
};
use std::{
    collections::BTreeMap,
    io::ErrorKind,
    path::{Path, PathBuf},
};
use vsdb_core::MapxRaw;

pub(super) const HEADER: [u8; 1] = [0];
pub(crate) const HEAD: u8 = 1;
pub(super) const NAME: u8 = 2;
pub(super) const BY_NAME: u8 = 3;
pub(crate) const COMMIT: u8 = 4;
pub(crate) const RETIRED: u8 = 5;
pub(crate) const STAGE: u8 = 6;
pub(super) const REFS: [u8; 1] = [7];

pub(crate) fn row(tag: u8, id: u64) -> [u8; 9] {
    let mut key = [tag; 9];
    key[1..].copy_from_slice(&id.to_be_bytes());
    key
}

pub(crate) fn record_id(k: &[u8]) -> Result<u64> {
    let bytes = k
        .get(1..)
        .and_then(|v| v.try_into().ok())
        .ok_or_else(|| corrupt("invalid version store key length"))?;
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn by_name(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + name.len());
    key.push(BY_NAME);
    key.extend_from_slice(name.as_bytes());
    key
}

pub(super) fn typed_meta_path<T>(primary: &Path) -> PathBuf {
    primary.with_extension(format!("view-{:016x}", type_tag::<T>()))
}

pub(crate) fn load_versioned_meta<T>(id: InstanceId) -> Result<Vec<u8>> {
    let ns = Namespace::open(id.ns.unwrap_or_default())?;
    let primary = ns.meta_path(id.map_id);
    match load_meta_file(&typed_meta_path::<T>(&primary)) {
        Err(VsdbError::Io(e)) if e.kind() == ErrorKind::NotFound => {
            load_meta_file(&primary)
        }
        result => result,
    }
}

pub(super) fn corrupt(msg: impl Into<String>) -> VsdbError {
    VsdbError::Corrupt { detail: msg.into() }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Header {
    pub(crate) next_id: u64,
    pub(super) next_commit: u64,
    pub(super) next_branch: u64,
    pub(super) main_branch: BranchId,
    pub(crate) generation: u64,
}

impl Header {
    pub(super) fn initial() -> Self {
        Self {
            next_id: 1,
            next_commit: 1,
            next_branch: 2,
            main_branch: BranchId::from_raw(1),
            generation: 0,
        }
    }
    pub(crate) fn get(store: &MapxRaw) -> Result<Self> {
        store
            .try_get_with(HEADER, Self::decode)?
            .ok_or_else(|| corrupt("missing version store header"))
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 41 || bytes[0] != 18 {
            return Err(corrupt("invalid version store header"));
        }
        let mut nums = bytes[1..]
            .as_chunks::<8>()
            .0
            .iter()
            .map(|chunk| u64::from_le_bytes(*chunk));
        let header = Self {
            next_id: nums.next().unwrap(),
            next_commit: nums.next().unwrap(),
            next_branch: nums.next().unwrap(),
            main_branch: BranchId::from_raw(nums.next().unwrap()),
            generation: nums.next().unwrap(),
        };
        if header.next_id == 0
            || header.next_id > 1 << 62
            || header.next_commit == 0
            || header.main_branch.raw() == 0
            || header.main_branch.raw() >= header.next_branch
        {
            return Err(corrupt("invalid version store ID ceiling"));
        }
        Ok(header)
    }
    pub(crate) fn bytes(self) -> [u8; 41] {
        let mut raw = [18; 41];
        for (dst, n) in raw[1..].as_chunks_mut::<8>().0.iter_mut().zip([
            self.next_id,
            self.next_commit,
            self.next_branch,
            self.main_branch.raw(),
            self.generation,
        ]) {
            dst.copy_from_slice(&n.to_le_bytes());
        }
        raw
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BranchHead {
    pub(crate) head: CommitId,
    pub(crate) dirty_root: NodeId,
}
impl BranchHead {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 16 {
            return Err(corrupt("invalid branch head length"));
        }
        Ok(Self {
            head: CommitId::from_raw(u64::from_le_bytes(bytes[..8].try_into().unwrap())),
            dirty_root: u64::from_le_bytes(bytes[8..].try_into().unwrap()),
        })
    }
    pub(crate) fn bytes(self) -> Vec<u8> {
        [self.head.raw().to_le_bytes(), self.dirty_root.to_le_bytes()].concat()
    }
}

impl Commit {
    pub(super) fn bytes(&self) -> Vec<u8> {
        let mut raw = Vec::with_capacity(20 + 8 * self.parents.len());
        raw.extend_from_slice(&self.root.to_le_bytes());
        raw.extend_from_slice(&self.timestamp_us.to_le_bytes());
        raw.extend_from_slice(&self.ref_count.to_le_bytes());
        for p in &self.parents {
            raw.extend_from_slice(&p.raw().to_le_bytes());
        }
        raw
    }
    pub(crate) fn parse(id: CommitId, raw: &[u8]) -> Result<Self> {
        let commit = Self::decode_row(id, raw)?;
        let parents = commit.parents.as_slice();
        if parents.iter().any(|p| *p == NO_COMMIT || *p >= id)
            || matches!(parents, [a, b] if a == b)
        {
            return Err(corrupt("invalid commit parent list"));
        }
        Ok(commit)
    }

    /// Decodes a row without checking its parent links against the history.
    pub(crate) fn decode_row(id: CommitId, raw: &[u8]) -> Result<Self> {
        if !matches!(raw.len(), 20 | 28 | 36) {
            return Err(corrupt("invalid commit length"));
        }
        let chunks = raw[20..].as_chunks::<8>().0;
        let mut parents = [CommitId::from_raw(0); 2];
        for (slot, chunk) in parents.iter_mut().zip(chunks) {
            *slot = CommitId::from_raw(u64::from_le_bytes(*chunk));
        }
        Ok(Self {
            id,
            root: u64::from_le_bytes(raw[..8].try_into().unwrap()),
            timestamp_us: u64::from_le_bytes(raw[8..16].try_into().unwrap()),
            ref_count: u32::from_le_bytes(raw[16..20].try_into().unwrap()),
            parents: smallvec::SmallVec::from_buf_and_len(parents, chunks.len()),
        })
    }
}

#[derive(Default)]
pub(super) struct Edits {
    pub(super) rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pub(super) heads: Vec<(BranchId, Option<BranchHead>)>,
    pub(super) acquire: Vec<NodeId>,
    pub(super) retire: Vec<NodeId>,
}
impl Edits {
    pub(super) fn write_head(&mut self, branch: BranchId, head: BranchHead) {
        self.rows
            .insert(row(HEAD, branch.raw()).to_vec(), Some(head.bytes()));
        self.heads.push((branch, Some(head)));
    }
    pub(super) fn delete_head(&mut self, branch: BranchId) {
        self.rows.insert(row(HEAD, branch.raw()).to_vec(), None);
        self.heads.push((branch, None));
    }
    pub(super) fn write_commit(&mut self, c: &Commit) {
        self.rows
            .insert(row(COMMIT, c.id.raw()).to_vec(), Some(c.bytes()));
    }
    pub(super) fn replace_root(&mut self, old: NodeId, new: NodeId) {
        if old != new {
            self.acquire.push(new);
            self.retire.push(old);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VerMap;

    #[test]
    fn history_reads_reject_invalid_commit_parents() {
        let mut map = VerMap::<u32, u32>::new();
        let branch = map.main_branch();
        map.insert(branch, &1, &10).unwrap();
        let first = map.commit(branch).unwrap();
        map.insert(branch, &2, &20).unwrap();
        let second = map.commit(branch).unwrap();
        let key = row(COMMIT, second.raw());
        let saved = map.tree.nodes.get(key).unwrap();
        let reader = map.reader();
        for parents in [
            vec![NO_COMMIT],
            vec![second],
            vec![CommitId::from_raw(second.raw() + 1)],
            vec![first, first],
        ] {
            let mut bytes = saved[..20].to_vec();
            for parent in parents {
                bytes.extend_from_slice(&parent.raw().to_le_bytes());
            }
            map.tree.nodes.insert(key, bytes);
            assert!(matches!(reader.at(second), Err(VsdbError::Corrupt { .. })));
        }
        map.tree.nodes.insert(key, saved);
        assert_eq!(reader.at(second).unwrap().get(&1), Some(10));
    }

    #[test]
    fn recovery_reclaims_an_unreachable_commit_with_invalid_parents() {
        let mut map = VerMap::<u32, u32>::new();
        let branch = map.main_branch();
        map.insert(branch, &1, &10).unwrap();
        let id = map.commit(branch).unwrap();
        let orphan_id = CommitId::from_raw(id.raw() + 1);
        let mut orphan = map.get_commit(id).unwrap();
        orphan.ref_count = 0;
        orphan.parents = smallvec::smallvec![orphan_id];
        map.tree
            .nodes
            .insert(row(COMMIT, orphan_id.raw()), orphan.bytes());
        let mut hdr = Header::get(&map.tree.nodes).unwrap();
        hdr.next_commit = orphan_id.raw() + 1;
        map.tree.nodes.insert(HEADER, hdr.bytes());

        // A recount only needs roots, so the stale row cannot block readers.
        map.tree.mark_refs_poisoned();
        assert_eq!(map.reader().at(id).unwrap().get(&1), Some(10));
        assert_eq!(map.verify_and_gc().unwrap().removed_commits, 1);
        assert!(!map.tree.nodes.contains_key(row(COMMIT, orphan_id.raw())));
        assert_eq!(map.get(branch, &1).unwrap(), Some(10));
    }
}
