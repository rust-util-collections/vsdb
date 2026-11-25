use super::Node;
use sha3::{Digest, Keccak256};
pub struct NodeCodec;
impl NodeCodec {
    pub fn encode(node: &Node) -> Vec<u8> {
        let mut out = Vec::new();
        Self::stream(node, |b| out.extend_from_slice(b));
        out
    }
    pub fn hash(node: &Node) -> [u8; 32] {
        if matches!(node, Node::Null) {
            return [0; 32];
        }
        let mut hash = Keccak256::new();
        Self::stream(node, |bytes| hash.update(bytes));
        hash.finalize().into()
    }
    fn stream(node: &Node, mut emit: impl FnMut(&[u8])) {
        fn len(mut n: usize, mut emit: impl FnMut(&[u8])) {
            let mut bytes = [0; 10];
            let mut used = 0;
            while n >= 128 {
                bytes[used] = (n as u8) | 128;
                used += 1;
                n >>= 7
            }
            bytes[used] = n as u8;
            emit(&bytes[..=used]);
        }
        match node {
            Node::Null => emit(&[0]),
            Node::Leaf { path, value } => {
                emit(&[1]);
                len(path.len(), &mut emit);
                emit(path.bytes());
                len(value.len(), &mut emit);
                emit(value);
            }
            Node::Extension { path, child } => {
                emit(&[2]);
                len(path.len(), &mut emit);
                emit(path.bytes());
                emit(child.hash().unwrap());
            }
            Node::Branch { children, value } => {
                emit(&[3]);
                emit(&children.bitmap.to_le_bytes());
                emit(&[u8::from(value.is_some())]);
                if let Some(v) = value {
                    len(v.len(), &mut emit);
                    emit(v)
                }
                for c in &children.nodes {
                    emit(c.hash().unwrap())
                }
            }
        }
    }
}
