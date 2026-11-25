use super::{BitPath, SmtHandle, SmtNode};
pub(crate) fn get(mut root: &SmtHandle, key: &[u8; 32]) -> Option<Vec<u8>> {
    let full = BitPath::from_hash(key);
    let mut at = 0;
    loop {
        match root.node() {
            SmtNode::Empty => return None,
            SmtNode::Leaf {
                key_hash, value, ..
            } => return (key_hash == key).then(|| value.clone()),
            SmtNode::Internal { path, left, right } => {
                if !full.starts_with_from(at, path) {
                    return None;
                }
                at += path.len();
                root = if full.bit_at(at) == 0 { left } else { right };
                at += 1
            }
        }
    }
}
