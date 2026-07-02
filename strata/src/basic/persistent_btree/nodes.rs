//! B+ tree node type, codec, and layout constants.
//!
//! The wire format is hand-written (no external codec dependency) so that
//! node serialisation is deterministic and version-independent.

use super::types::NodeId;

// =========================================================================
// Constants
// =========================================================================

/// Half the maximum fan-out. Non-root nodes hold `B..=2B` keys.
pub(crate) const B: usize = 16;
/// Maximum keys per node.
pub(crate) const MAX_KEYS: usize = 2 * B;
/// Minimum keys for a non-root node.
pub(crate) const MIN_KEYS: usize = B;

// =========================================================================
// Wire-format tags
// =========================================================================

// Wire format (all multi-byte integers are little-endian):
//
//   tag: u8          0 = Leaf, 1 = Internal
//   n:   u32         number of keys
//
// Leaf   (tag=0):  for i in 0..n { key_len:u32 key:[u8] val_len:u32 val:[u8] }
// Internal(tag=1): for i in 0..n { key_len:u32 key:[u8] }
//                  for i in 0..=n { child:u64 }

pub(crate) const TAG_LEAF: u8 = 0;
pub(crate) const TAG_INTERNAL: u8 = 1;

// =========================================================================
// Node
// =========================================================================

#[derive(Clone, Debug)]
pub(crate) enum Node {
    Leaf {
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    },
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<NodeId>,
    },
}

impl Node {
    pub(crate) fn key_count(&self) -> usize {
        match self {
            Node::Leaf { keys, .. } | Node::Internal { keys, .. } => keys.len(),
        }
    }

    // ---- encode ----

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        match self {
            Node::Leaf { keys, values } => {
                buf.push(TAG_LEAF);
                buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                for i in 0..keys.len() {
                    buf.extend_from_slice(&(keys[i].len() as u32).to_le_bytes());
                    buf.extend_from_slice(&keys[i]);
                    buf.extend_from_slice(&(values[i].len() as u32).to_le_bytes());
                    buf.extend_from_slice(&values[i]);
                }
            }
            Node::Internal { keys, children } => {
                buf.push(TAG_INTERNAL);
                buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                for k in keys {
                    buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
                    buf.extend_from_slice(k);
                }
                for c in children {
                    buf.extend_from_slice(&c.to_le_bytes());
                }
            }
        }
        buf
    }

    // ---- decode ----

    pub(crate) fn decode(data: &[u8]) -> Self {
        let len = data.len();
        assert!(
            len >= 5,
            "PersistentBTree: node data too short ({len} bytes)"
        );

        let mut pos = 0;

        let tag = data[pos];
        pos += 1;

        let n = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        match tag {
            TAG_LEAF => {
                let mut keys = Vec::with_capacity(n);
                let mut values = Vec::with_capacity(n);
                for _ in 0..n {
                    assert!(
                        pos + 4 <= len,
                        "PersistentBTree: truncated leaf key length at pos {pos}"
                    );
                    let klen = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap())
                        as usize;
                    pos += 4;
                    assert!(
                        pos + klen <= len,
                        "PersistentBTree: truncated leaf key at pos {pos}, klen={klen}"
                    );
                    keys.push(data[pos..pos + klen].to_vec());
                    pos += klen;
                    assert!(
                        pos + 4 <= len,
                        "PersistentBTree: truncated leaf value length at pos {pos}"
                    );
                    let vlen = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap())
                        as usize;
                    pos += 4;
                    assert!(
                        pos + vlen <= len,
                        "PersistentBTree: truncated leaf value at pos {pos}, vlen={vlen}"
                    );
                    values.push(data[pos..pos + vlen].to_vec());
                    pos += vlen;
                }
                Node::Leaf { keys, values }
            }
            TAG_INTERNAL => {
                let mut keys = Vec::with_capacity(n);
                for _ in 0..n {
                    assert!(
                        pos + 4 <= len,
                        "PersistentBTree: truncated internal key length at pos {pos}"
                    );
                    let klen = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap())
                        as usize;
                    pos += 4;
                    assert!(
                        pos + klen <= len,
                        "PersistentBTree: truncated internal key at pos {pos}, klen={klen}"
                    );
                    keys.push(data[pos..pos + klen].to_vec());
                    pos += klen;
                }
                let mut children = Vec::with_capacity(n + 1);
                for _ in 0..=n {
                    assert!(
                        pos + 8 <= len,
                        "PersistentBTree: truncated child id at pos {pos}"
                    );
                    let c = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    children.push(c);
                }
                Node::Internal { keys, children }
            }
            _ => panic!("PersistentBTree: corrupt node tag {tag}"),
        }
    }
}
