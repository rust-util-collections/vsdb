use super::{Node, NodeHandle};
use crate::config::HASH_LEN;
use crate::error::{Result, TrieError};
use crate::nibbles::Nibbles;

pub struct NodeCodec;

impl NodeCodec {
    pub fn encode(node: &Node) -> Vec<u8> {
        let mut buf = Vec::new();
        match node {
            Node::Null => {
                buf.push(0x00);
            }
            Node::Leaf { path, value } => {
                // Tag: 0x01
                buf.push(0x01);
                encode_path(&mut buf, path);
                encode_bytes(&mut buf, value);
            }
            Node::Extension { path, child } => {
                // Tag: 0x02
                buf.push(0x02);
                encode_path(&mut buf, path);
                // Child must be a hash (or cached)
                let hash = child
                    .hash()
                    .expect("Child must be hashed before encoding extension");
                buf.extend_from_slice(hash);
            }
            Node::Branch { children, value } => {
                // Tag: 0x03
                buf.push(0x03);

                // Bitmap
                let mut bitmap: u16 = 0;
                for (i, child) in children.iter().enumerate() {
                    if child.is_some() {
                        bitmap |= 1 << i;
                    }
                }
                buf.extend_from_slice(&bitmap.to_le_bytes());

                // Value
                if let Some(v) = value {
                    buf.push(1); // Has value
                    encode_bytes(&mut buf, v);
                } else {
                    buf.push(0); // No value
                }

                // Children Hashes
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

    pub fn decode(data: &[u8]) -> Result<Node> {
        if data.is_empty() {
            return Err(TrieError::DecodeError("Empty data".into()));
        }
        let mut cursor = 0;
        let tag = data[cursor];
        cursor += 1;

        match tag {
            0x00 => Ok(Node::Null),
            0x01 => {
                let path = decode_path(data, &mut cursor)?;
                let value = decode_bytes(data, &mut cursor)?;
                Ok(Node::Leaf { path, value })
            }
            0x02 => {
                let path = decode_path(data, &mut cursor)?;
                if cursor + HASH_LEN > data.len() {
                    return Err(TrieError::DecodeError(
                        "Not enough bytes for extension child hash".into(),
                    ));
                }
                let hash = data[cursor..cursor + HASH_LEN].to_vec();
                Ok(Node::Extension {
                    path,
                    child: NodeHandle::Hash(hash),
                })
            }
            0x03 => {
                if cursor + 2 > data.len() {
                    return Err(TrieError::DecodeError(
                        "Not enough bytes for branch bitmap".into(),
                    ));
                }
                let bitmap_bytes = &data[cursor..cursor + 2];
                let bitmap = u16::from_le_bytes([bitmap_bytes[0], bitmap_bytes[1]]);
                cursor += 2;

                let has_value = if cursor < data.len() {
                    data[cursor] == 1
                } else {
                    return Err(TrieError::DecodeError("Unexpected EOF".into()));
                };
                cursor += 1;

                let value = if has_value {
                    Some(decode_bytes(data, &mut cursor)?)
                } else {
                    None
                };

                let mut children = Box::new([
                    None, None, None, None, None, None, None, None, None, None, None, None, None,
                    None, None, None,
                ]);

                for i in 0..16 {
                    if (bitmap & (1 << i)) != 0 {
                        if cursor + HASH_LEN > data.len() {
                            return Err(TrieError::DecodeError(
                                "Not enough bytes for branch child hash".into(),
                            ));
                        }
                        let hash = data[cursor..cursor + HASH_LEN].to_vec();
                        children[i] = Some(NodeHandle::Hash(hash));
                        cursor += HASH_LEN;
                    }
                }

                Ok(Node::Branch { children, value })
            }
            _ => Err(TrieError::DecodeError(format!("Unknown tag: {}", tag))),
        }
    }
}

// Helpers

fn encode_varint(buf: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

fn decode_varint(data: &[u8], cursor: &mut usize) -> Result<usize> {
    let mut n: usize = 0;
    let mut shift = 0;
    loop {
        if *cursor >= data.len() {
            return Err(TrieError::DecodeError("Varint unexpected EOF".into()));
        }
        let b = data[*cursor];
        *cursor += 1;
        n |= ((b & 0x7F) as usize) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(n)
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

fn decode_path(data: &[u8], cursor: &mut usize) -> Result<Nibbles> {
    let len = decode_varint(data, cursor)?;
    let mut nibbles = Vec::with_capacity(len);
    let byte_len = len.div_ceil(2);
    if *cursor + byte_len > data.len() {
        return Err(TrieError::DecodeError("Path bytes unexpected EOF".into()));
    }

    for i in 0..byte_len {
        let b = data[*cursor + i];
        nibbles.push(b >> 4);
        if nibbles.len() < len {
            nibbles.push(b & 0x0F);
        }
    }
    *cursor += byte_len;
    Ok(Nibbles::from_nibbles_unsafe(nibbles))
}

fn encode_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    encode_varint(buf, bytes.len());
    buf.extend_from_slice(bytes);
}

fn decode_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = decode_varint(data, cursor)?;
    if *cursor + len > data.len() {
        return Err(TrieError::DecodeError("Bytes unexpected EOF".into()));
    }
    let bytes = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(bytes)
}
