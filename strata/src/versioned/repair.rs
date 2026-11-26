//! Validate the published graph before repairing any row or reclaiming pages.

use super::{Commit, CommitId, NO_COMMIT, map::VerMap, store::*};
use crate::{
    Result,
    basic::persistent_btree::{BLOB, CLASS_MASK, EMPTY_ROOT, NodeView, object_key},
    common::{InstanceId, PrefixFrame, parse_prefix_handle_checked},
};
use std::collections::{HashMap, HashSet};

/// Counts from a complete version-store verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcStats {
    pub live_objects: usize,
    pub live_commits: usize,
    pub removed_commits: usize,
}

impl<K, V> VerMap<K, V> {
    pub(super) fn open_meta(id: InstanceId, deep: bool) -> Result<Self> {
        let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
        let bytes = load_versioned_meta::<Self>(id)?;
        match parse_prefix_handle_checked::<Self>(&bytes, Some(id))? {
            PrefixFrame(raw, []) => Self::open(raw, deep),
            _ => Err(corrupt("version store metadata identity mismatch")),
        }
    }

    /// Opens a saved handle and verifies every reachable page and blob.
    pub fn from_meta_verified(id: impl Into<InstanceId>) -> Result<Self> {
        Self::open_meta(id.into(), true)
    }

    #[cfg(test)]
    pub(super) fn records(&self, tag: u8) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        self.tree
            .nodes
            .range([tag].as_slice()..[tag + 1].as_slice())
    }

    #[cfg(test)]
    pub(super) fn commit_rows(
        &self,
    ) -> impl Iterator<Item = Result<(CommitId, Commit)>> {
        self.records(COMMIT).map(|(k, v)| {
            let id = CommitId::from_raw(record_id(&k)?);
            Ok((id, Commit::parse(id, &v)?))
        })
    }

    pub(super) fn try_tag_range(
        &self,
        tag: u8,
    ) -> Result<vsdb_core::basic::mapx_raw::MapxRawIter<'_>> {
        self.tree
            .nodes
            .try_range([tag].as_slice()..[tag + 1].as_slice())
    }

    pub(super) fn validate_commit_graph(&self) -> Result<HashMap<CommitId, u32>> {
        let mut refs = HashMap::<CommitId, u32>::new();
        let mut work = Vec::new();
        let mut heads = self.try_tag_range(HEAD)?;
        while let Some((key, v)) = heads.try_step_next()? {
            record_id(key)?;
            let head = BranchHead::decode(v)?.head;
            if head != NO_COMMIT {
                work.push(head);
            }
        }
        drop(heads);
        while let Some(id) = work.pop() {
            let count = refs.entry(id).or_default();
            *count = count
                .checked_add(1)
                .ok_or_else(|| corrupt("commit reference count overflow"))?;
            if *count == 1 {
                let commit = self.get_commit_inner(id)?;
                let mut seen = HashSet::new();
                for p in commit.parents {
                    if p == NO_COMMIT || p >= id || !seen.insert(p) {
                        return Err(corrupt("invalid commit parent list"));
                    }
                    work.push(p);
                }
            }
        }
        Ok(refs)
    }

    pub(super) fn recover(&mut self, deep: bool) -> Result<GcStats> {
        self.tree.clear_cached_heads();
        let hdr = Header::get(&self.tree.nodes)?;
        self.tree.check_ceiling(hdr.next_id)?;
        let counts = match self.validate_commit_graph() {
            Ok(counts) => counts,
            Err(err @ crate::VsdbError::CommitNotFound { .. }) => {
                return Err(corrupt(err.to_string()));
            }
            Err(err) => return Err(err),
        };
        let mut roots = Vec::new();
        let mut names = HashMap::new();
        let mut edits = Edits::default();
        let mut main_found = false;
        let mut heads = self.try_tag_range(HEAD)?;
        while let Some((key, v)) = heads.try_step_next()? {
            let id = record_id(key)?;
            if id == 0 || id >= hdr.next_branch {
                return Err(corrupt("invalid branch ID ceiling"));
            }
            main_found |= id == hdr.main_branch.raw();
            let name = self
                .tree
                .nodes
                .try_get_with(row(NAME, id), |raw| {
                    std::str::from_utf8(raw)
                        .map(str::to_owned)
                        .map_err(|_| corrupt("invalid branch name"))
                })?
                .ok_or_else(|| corrupt("missing branch name"))?;
            if names.insert(by_name(&name), id).is_some() {
                return Err(corrupt("duplicate branch name"));
            }
            roots.push(BranchHead::decode(v)?.dirty_root);
        }
        drop(heads);
        if !main_found {
            return Err(corrupt("missing main branch"));
        }
        let mut name_rows = self.try_tag_range(NAME)?;
        while let Some((key, _)) = name_rows.try_step_next()? {
            let id = record_id(key)?;
            if !self.tree.nodes.try_contains_key(row(HEAD, id))? {
                edits.rows.insert(key.to_vec(), None);
            }
        }
        drop(name_rows);
        let mut by_name_rows = self.try_tag_range(BY_NAME)?;
        while let Some((key, v)) = by_name_rows.try_step_next()? {
            if names
                .get(key)
                .is_none_or(|n| n.to_le_bytes().as_slice() != v)
            {
                edits.rows.insert(key.to_vec(), None);
            }
        }
        drop(by_name_rows);
        for (k, id) in names {
            let raw = id.to_le_bytes();
            let matches = self
                .tree
                .nodes
                .try_get_with(&k, |v| Ok(v == raw.as_slice()))?
                .unwrap_or(false);
            if !matches {
                edits.rows.insert(k, Some(raw.to_vec()));
            }
        }
        if self.namespace().is_read_only() && !edits.rows.is_empty() {
            return Err(corrupt("branch name index requires writable repair"));
        }
        let mut removed_commits = 0;
        let mut commit_rows = self.try_tag_range(COMMIT)?;
        while let Some((k, v)) = commit_rows.try_step_next()? {
            let id = CommitId::from_raw(record_id(k)?);
            let mut commit = Commit::parse(id, v)?;
            if id == NO_COMMIT || id.raw() >= hdr.next_commit {
                return Err(corrupt("invalid commit ID ceiling"));
            }
            if let Some(&owners) = counts.get(&id) {
                roots.push(commit.root);
                if owners != commit.ref_count {
                    commit.ref_count = owners;
                    edits.write_commit(&commit);
                }
            } else {
                edits.rows.insert(row(COMMIT, id.raw()).to_vec(), None);
                removed_commits += 1;
            }
        }
        drop(commit_rows);
        for root in roots
            .iter()
            .copied()
            .filter(|r| *r != EMPTY_ROOT)
            .collect::<HashSet<_>>()
        {
            let leaf = self
                .tree
                .nodes
                .try_get_with(object_key(root), |bytes| Ok(NodeView::open(bytes)?.leaf))?
                .ok_or_else(|| corrupt(format!("missing reachable root {root}")))?;
            if root & CLASS_MASK == BLOB || (root & CLASS_MASK == 0) == leaf {
                return Err(corrupt("invalid root page class"));
            }
        }
        let mut has_anchors = false;
        for tag in [RETIRED, STAGE] {
            let mut anchors = self.try_tag_range(tag)?;
            while let Some((k, _)) = anchors.try_step_next()? {
                let id = record_id(k)?;
                has_anchors = true;
                if tag != RETIRED || !self.tree.keep_pin_marker(id) {
                    edits.rows.insert(k.to_vec(), None);
                }
            }
        }
        let cached = if deep || has_anchors {
            None
        } else if let Some(bytes) =
            self.tree.nodes.try_get_with(REFS, |raw| Ok(raw.to_vec()))?
        {
            self.tree.load_refs(&bytes, hdr.generation, &roots)
        } else {
            None
        };
        let live_objects = match cached {
            Some(n) => n,
            None => {
                let refs = self.tree.required_node_refs(&roots)?;
                let n = refs.len();
                if deep {
                    self.tree.verify_pages(&refs)?;
                }
                if !self.namespace().is_read_only() {
                    self.tree.nodes.try_sync_wal()?;
                }
                self.tree.finish_ref_counts(refs, &[])?;
                n
            }
        };
        if !self.namespace().is_read_only() && !edits.rows.is_empty() {
            self.publish(hdr, edits, true)?;
        }
        Ok(GcStats {
            live_objects,
            live_commits: counts.len(),
            removed_commits,
        })
    }

    /// Verifies every reachable page, repairs reference counts and sweeps
    /// unreachable objects. Read-only opens perform validation in memory.
    pub fn verify_and_gc(&mut self) -> Result<GcStats> {
        self.recover(true)
    }

    /// Checkpoints a writable store and consumes this handle.
    pub fn close(mut self) -> Result<()> {
        if !self.namespace().is_read_only() {
            self.flush()?;
        }
        Ok(())
    }

    /// Durably checkpoints the current reference graph for fast reopening.
    pub fn flush(&mut self) -> Result<()> {
        self.ensure_writable("version store checkpoint")?;
        let hdr = Header::get(&self.tree.nodes)?;
        let bytes = self.tree.ref_checkpoint(hdr.generation)?;
        self.tree.flush_refs(&REFS, bytes)
    }
}

pub(super) fn record_id(k: &[u8]) -> Result<u64> {
    let bytes = k
        .get(1..)
        .and_then(|v| v.try_into().ok())
        .ok_or_else(|| corrupt("invalid version store key length"))?;
    Ok(u64::from_be_bytes(bytes))
}
