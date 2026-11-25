use crate::trie::{
    nibbles::Nibbles,
    node::{Node, NodeHandle},
};
pub(crate) fn lookup(
    root: &NodeHandle,
    key: &[u8],
    mut visit: impl FnMut(&Node),
) -> Option<Vec<u8>> {
    let path = Nibbles::from_raw(key);
    let mut node = root.node();
    let mut pos = 0;
    loop {
        visit(node);
        match node {
            Node::Null => return None,
            Node::Leaf { path: p, value } => {
                return (p.len() == path.len() - pos && path.matches_at(pos, p))
                    .then(|| value.clone());
            }
            Node::Extension { path: p, child } => {
                if !path.matches_at(pos, p) {
                    return None;
                }
                pos += p.len();
                node = child.node();
            }
            Node::Branch { children, value } => {
                if pos == path.len() {
                    return value.as_deref().map(<[u8]>::to_vec);
                }
                node = children.get(path.at(pos) as usize)?.node();
                pos += 1
            }
        }
    }
}
