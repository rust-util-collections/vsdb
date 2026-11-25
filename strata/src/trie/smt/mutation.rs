use super::{BitPath, SmtHandle, SmtNode, TREE_DEPTH};

fn leaf(full: &BitPath, at: usize, key: [u8; 32], value: &[u8]) -> SmtNode {
    SmtNode::Leaf {
        path: full.slice(at, TREE_DEPTH),
        key_hash: key,
        value: value.into(),
    }
}
pub(crate) fn insert(mut root: &mut SmtHandle, key: [u8; 32], value: &[u8]) {
    let full = BitPath::from_hash(&key);
    let mut at = 0;
    loop {
        if matches!(root.node(),SmtNode::Internal{path,..} if full.starts_with_from(at,path))
        {
            let SmtNode::Internal { path, left, right } = root.edit() else {
                unreachable!()
            };
            at += path.len();
            root = if full.bit_at(at) == 0 { left } else { right };
            at += 1;
            continue;
        }
        let node = root.edit();
        match node {
            SmtNode::Empty => {
                *node = leaf(&full, at, key, value);
                return;
            }
            SmtNode::Leaf {
                key_hash,
                value: old,
                ..
            } if *key_hash == key => {
                old.clear();
                old.extend_from_slice(value);
                return;
            }
            terminal => {
                let mut old = std::mem::take(terminal);
                let prefix = match &mut old {
                    SmtNode::Leaf { path, .. } | SmtNode::Internal { path, .. } => path,
                    SmtNode::Empty => unreachable!(),
                };
                let common = full.common_prefix_from(at, prefix);
                let branch_path = prefix.slice(0, common);
                *prefix = prefix.slice(common + 1, prefix.len());
                let new = SmtHandle::new(leaf(&full, at + common + 1, key, value));
                let old = SmtHandle::new(old);
                let (left, right) = if full.bit_at(at + common) == 0 {
                    (new, old)
                } else {
                    (old, new)
                };
                *terminal = SmtNode::Internal {
                    path: branch_path,
                    left,
                    right,
                };
                return;
            }
        }
    }
}
pub(crate) fn remove(root: &SmtHandle, key: &[u8; 32]) -> SmtHandle {
    let full = BitPath::from_hash(key);
    let mut at = 0;
    let mut current = root;
    let mut parents = Vec::new();
    loop {
        match current.node() {
            SmtNode::Leaf { key_hash, .. } if key_hash == key => break,
            SmtNode::Internal { path, left, right }
                if full.starts_with_from(at, path) =>
            {
                at += path.len();
                let bit = full.bit_at(at);
                at += 1;
                let (next, other) = if bit == 0 {
                    (left, right)
                } else {
                    (right, left)
                };
                parents.push((path, bit, other));
                current = next;
            }
            _ => return root.clone(),
        }
    }
    let Some((path, bit, sibling)) = parents.pop() else {
        return SmtHandle::default();
    };
    let mut child = sibling.node().clone();
    let tail = match &mut child {
        SmtNode::Leaf { path, .. } | SmtNode::Internal { path, .. } => path,
        SmtNode::Empty => unreachable!(),
    };
    *tail = path.concat(&BitPath::from_bits(&[1 - bit])).concat(tail);
    let mut next = SmtHandle::new(child);
    for (path, bit, other) in parents.into_iter().rev() {
        let (left, right) = if bit == 0 {
            (next, other.clone())
        } else {
            (other.clone(), next)
        };
        next = SmtHandle::new(SmtNode::Internal {
            path: path.clone(),
            left,
            right,
        })
    }
    next
}
