use crate::trie::{
    Result, invalid_state,
    nibbles::Nibbles,
    node::{Children, Node, NodeHandle},
};

/// Maximum length of a user key in the Merkle Patricia Trie.
pub const MAX_MPT_KEY_LEN: usize = 1024;

pub(crate) fn insert(mut root: &mut NodeHandle, key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() > MAX_MPT_KEY_LEN {
        return Err(invalid_state(format!(
            "key length {} exceeds MAX_MPT_KEY_LEN ({MAX_MPT_KEY_LEN})",
            key.len()
        )));
    }
    let path = Nibbles::from_raw(key);
    let mut at = 0;
    loop {
        let descend = match root.node() {
            Node::Branch { .. } => true,
            Node::Extension { path: prefix, .. } => path.matches_at(at, prefix),
            _ => false,
        };
        if !descend {
            let terminal = root.edit();
            let suffix = path.slice(at, path.len());
            *terminal = split(std::mem::take(terminal), suffix, value);
            break;
        }
        let body = root.edit();
        match body {
            Node::Branch {
                children,
                value: slot,
            } => {
                if at == path.len() {
                    *slot = Some(value.into());
                    break;
                }
                let idx = path.at(at) as usize;
                at += 1;
                if children.get(idx).is_none() {
                    children.set(
                        idx,
                        Some(NodeHandle::new(Node::Leaf {
                            path: path.slice(at, path.len()),
                            value: value.into(),
                        })),
                    );
                    break;
                }
                root = children.get_mut(idx).unwrap();
            }
            Node::Extension {
                path: prefix,
                child,
            } => {
                at += prefix.len();
                root = child;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

fn split(old: Node, key: Nibbles, val: &[u8]) -> Node {
    let mut children = Children::default();
    let mut value = None;
    let (path, old_value, child) = match old {
        Node::Null => {
            return Node::Leaf {
                path: key,
                value: val.into(),
            };
        }
        Node::Leaf { path, .. } if path == key => {
            return Node::Leaf {
                path,
                value: val.into(),
            };
        }
        Node::Leaf { path, value } => (path, Some(value), None),
        Node::Extension { path, child } => (path, None, Some(child)),
        Node::Branch { .. } => unreachable!(),
    };
    let common = path.common_prefix(&key);
    if common == path.len() {
        value = old_value.map(Vec::into_boxed_slice)
    } else {
        let tail = path.slice(common + 1, path.len());
        let old = match child {
            Some(child) if tail.is_empty() => child,
            Some(child) => NodeHandle::new(Node::Extension { path: tail, child }),
            None => NodeHandle::new(Node::Leaf {
                path: tail,
                value: old_value.unwrap(),
            }),
        };
        children.set(path.at(common) as usize, Some(old));
    }
    if common == key.len() {
        value = Some(val.into())
    } else {
        children.set(
            key.at(common) as usize,
            Some(NodeHandle::new(Node::Leaf {
                path: key.slice(common + 1, key.len()),
                value: val.into(),
            })),
        );
    }
    let branch = Node::Branch { children, value };
    if common == 0 {
        branch
    } else {
        Node::Extension {
            path: key.slice(0, common),
            child: NodeHandle::new(branch),
        }
    }
}
pub(crate) fn remove(root: &NodeHandle, key: &[u8]) -> NodeHandle {
    if key.len() > MAX_MPT_KEY_LEN {
        return root.clone();
    }
    let path = Nibbles::from_raw(key);
    let mut at = 0;
    let mut current = root;
    let mut parents = Vec::new();
    let mut replacement = loop {
        match current.node() {
            Node::Branch { children, value } if at == path.len() => {
                if value.is_none() {
                    return root.clone();
                }
                break compact(children.clone(), None);
            }
            Node::Branch { children, .. } => {
                let idx = path.at(at) as usize;
                let Some(child) = children.get(idx) else {
                    return root.clone();
                };
                parents.push((current, idx));
                at += 1;
                current = child;
            }
            Node::Extension {
                path: prefix,
                child,
            } if path.matches_at(at, prefix) => {
                parents.push((current, 0));
                at += prefix.len();
                current = child;
            }
            Node::Leaf { path: tail, .. }
                if path.len() - at == tail.len() && path.matches_at(at, tail) =>
            {
                break None;
            }
            _ => return root.clone(),
        }
    };
    for (parent, i) in parents.into_iter().rev() {
        replacement = match parent.node() {
            Node::Extension { path, .. } => replacement.map(|c| join(path, c)),
            Node::Branch { children, value } => {
                let mut ch = children.clone();
                ch.set(i, replacement);
                compact(ch, value.clone())
            }
            _ => unreachable!(),
        };
    }
    replacement.unwrap_or_default()
}
fn compact(children: Children, value: Option<Box<[u8]>>) -> Option<NodeHandle> {
    match (children.nodes.len(), value) {
        (0, value) => value.map(|v| {
            NodeHandle::new(Node::Leaf {
                path: Nibbles::default(),
                value: v.into_vec(),
            })
        }),
        (1, None) => Some(join(
            &Nibbles::from_nibbles_unsafe(vec![children.bitmap.trailing_zeros() as u8]),
            children.nodes.into_vec().pop().unwrap(),
        )),
        (_, value) => Some(NodeHandle::new(Node::Branch { children, value })),
    }
}
fn join(path: &Nibbles, child: NodeHandle) -> NodeHandle {
    let node = match child.node() {
        Node::Branch { .. } => Node::Extension {
            path: path.clone(),
            child,
        },
        Node::Leaf { path: tail, value } => Node::Leaf {
            path: path.concat(tail),
            value: value.clone(),
        },
        Node::Extension { path: tail, child } => Node::Extension {
            path: path.concat(tail),
            child: child.clone(),
        },
        Node::Null => Node::Null,
    };
    NodeHandle::new(node)
}
