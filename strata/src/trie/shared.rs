//! Copy-on-write node storage shared by the two in-memory tries.
use std::{
    mem,
    sync::{Arc, OnceLock},
};

pub(super) trait TreeNode: Default + Clone {
    fn children<'a>(&'a self, visit: impl FnMut(&'a Handle<Self>));
    fn into_children(self, pending: &mut Vec<Handle<Self>>);
    fn digest(&self) -> [u8; 32];
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Handle<N: TreeNode>(Arc<Record<N>>);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Record<N: TreeNode> {
    node: N,
    hash: OnceLock<[u8; 32]>,
}

impl<N: TreeNode> Default for Handle<N> {
    fn default() -> Self {
        Self::new(N::default())
    }
}
impl<N: TreeNode> Handle<N> {
    pub fn new(node: N) -> Self {
        Self(Arc::new(Record {
            node,
            hash: OnceLock::new(),
        }))
    }
    /// Isolates an entire logical update, including unwinding during a later
    /// edit after earlier edits have already dirtied this candidate.
    pub fn try_update(
        &mut self,
        apply: impl FnOnce(&mut Self) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let mut candidate = self.clone();
        apply(&mut candidate)?;
        *self = candidate;
        Ok(())
    }
    pub fn node(&self) -> &N {
        &self.0.node
    }
    pub fn edit(&mut self) -> &mut N {
        let record = Arc::make_mut(&mut self.0);
        record.hash.take();
        &mut record.node
    }
    pub fn hash(&self) -> Option<&[u8; 32]> {
        self.0.hash.get()
    }
    pub fn root_hash(&self) -> [u8; 32] {
        if let Some(hash) = self.hash() {
            return *hash;
        }
        let mut work = vec![(self, false)];
        while let Some((node, visited)) = work.pop() {
            if node.hash().is_some() {
                continue;
            }
            if visited {
                node.0.hash.get_or_init(|| node.node().digest());
            } else {
                work.push((node, true));
                node.node().children(|child| work.push((child, false)));
            }
        }
        *self.hash().expect("post-order traversal completed")
    }
}

impl<N: TreeNode> Drop for Record<N> {
    fn drop(&mut self) {
        let mut pending = Vec::new();
        mem::take(&mut self.node).into_children(&mut pending);
        while let Some(child) = pending.pop() {
            if let Ok(mut child) = Arc::try_unwrap(child.0) {
                mem::take(&mut child.node).into_children(&mut pending);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::{MptCalc, SmtCalc};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn failed_candidates_leave_shared_root_and_commitment_intact() {
        macro_rules! check {
            ($calc:ty) => {{
                let mut trie =
                    <$calc>::from_entries([(b"a", b"one"), (b"b", b"two")]).unwrap();
                let hash = trie.root_hash();
                let shared = trie.clone();
                assert!(Arc::ptr_eq(&trie.root.0, &shared.root.0));
                let result = catch_unwind(AssertUnwindSafe(|| {
                    trie.root.try_update(|candidate| {
                        <$calc>::edit(candidate, b"a", Some(b"changed"))?;
                        panic!("injected failure before publication")
                    })
                }));
                assert!(result.is_err());
                assert!(Arc::ptr_eq(&trie.root.0, &shared.root.0));
                assert_eq!(trie.root_hash(), hash);
                trie.remove(b"a").unwrap();
                assert_ne!(trie.root_hash(), hash);
                assert_eq!(
                    shared.get(b"a").unwrap().as_deref(),
                    Some(b"one".as_slice())
                );
            }};
        }
        check!(MptCalc);
        check!(SmtCalc);
    }
}
