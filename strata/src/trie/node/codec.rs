use super::Node;
use crate::trie::nibbles::Nibbles;

pub struct NodeCodec;

impl NodeCodec {
    /// Encodes a node into bytes for hashing.
    ///
    /// All children must have been hashed (via `commit`) before encoding.
    pub fn encode(node: &Node) -> Vec<u8> {
        let mut buf = Vec::new();
        match node {
            Node::Null => {
                buf.push(0x00);
            }
            Node::Leaf { path, value } => {
                buf.push(0x01);
                encode_path(&mut buf, path);
                encode_bytes(&mut buf, value);
            }
            Node::Extension { path, child } => {
                buf.push(0x02);
                encode_path(&mut buf, path);
                let hash = child
                    .hash()
                    .expect("Child must be hashed before encoding extension");
                buf.extend_from_slice(hash);
            }
            Node::Branch { children, value } => {
                buf.push(0x03);

                let mut bitmap: u16 = 0;
                for (i, child) in children.iter().enumerate() {
                    if child.is_some() {
                        bitmap |= 1 << i;
                    }
                }
                buf.extend_from_slice(&bitmap.to_le_bytes());

                if let Some(v) = value {
                    buf.push(1);
                    encode_bytes(&mut buf, v);
                } else {
                    buf.push(0);
                }

                for child in children.iter().flatten() {
                    let hash = child
                        .hash()
                        .expect("Child must be hashed before encoding branch");
                    buf.extend_from_slice(hash);
                }
            }
        }
        buf
    }
}

fn encode_varint(buf: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        buf.push(((n as u8) & 0x7F) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

fn encode_path(buf: &mut Vec<u8>, path: &Nibbles) {
    let len = path.len();
    encode_varint(buf, len);
    let nibbles = path.as_slice();
    for i in (0..len).step_by(2) {
        let n1 = nibbles[i];
        let n2 = if i + 1 < len { nibbles[i + 1] } else { 0 };
        buf.push((n1 << 4) | n2);
    }
}

fn encode_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    encode_varint(buf, bytes.len());
    buf.extend_from_slice(bytes);
}
