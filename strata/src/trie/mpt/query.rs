use crate::trie::{
    error::Result,
    nibbles::Nibbles,
    node::{Node, NodeHandle},
};

pub struct TrieRo<'a> {
    root: &'a NodeHandle,
}

impl<'a> TrieRo<'a> {
    pub fn new(root: &'a NodeHandle) -> Self {
        Self { root }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if matches!(self.root, NodeHandle::InMemory(n) if **n == Node::Null) {
            return Ok(None);
        }

        let path = Nibbles::from_raw(key);
        let mut remaining = path.as_slice();
        let mut node = self.root.node();
        loop {
            match node {
                Node::Null => return Ok(None),
                Node::Leaf {
                    path: leaf_path,
                    value,
                } => {
                    return Ok(
                        (leaf_path.as_slice() == remaining).then(|| value.clone())
                    );
                }
                Node::Extension {
                    path: ext_path,
                    child,
                } => {
                    let Some(rest) = remaining.strip_prefix(ext_path.as_slice()) else {
                        return Ok(None);
                    };
                    remaining = rest;
                    node = child.node();
                }
                Node::Branch { children, value } => {
                    let Some((&index, rest)) = remaining.split_first() else {
                        return Ok(value.clone());
                    };
                    let Some(child) = &children[index as usize] else {
                        return Ok(None);
                    };
                    remaining = rest;
                    node = child.node();
                }
            }
        }
    }
}
