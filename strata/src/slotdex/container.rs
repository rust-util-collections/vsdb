//! Slot-level data container with inline/small-to-large promotion.
//!
//! `DataCtner` starts as an in-memory `BTreeSet` and automatically promotes
//! to a disk-backed `MapxOrd` when the entry count exceeds
//! [`INLINE_CAPACITY_THRESHOLD`].

use std::{
    collections::{BTreeSet, btree_set::Iter as SmallIter},
    fmt,
};

use crate::common::error::{Result, VsdbError};

use crate::{
    KeyEnDeOrdered, MapxOrd, ValueEnDe, basic::mapx_ord::MapxOrdIter as LargeIter,
};

// =========================================================================
// Constants
// =========================================================================

/// Number of entries above which a `DataCtner` transitions from inline
/// `BTreeSet` to disk-backed `MapxOrd`.
pub(crate) const INLINE_CAPACITY_THRESHOLD: usize = 8;

// Tag bytes for binary encoding
const TAG_SMALL: u8 = 0;
const TAG_LARGE: u8 = 1;

// =========================================================================
// DataCtner
// =========================================================================

pub(crate) enum DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    Small(BTreeSet<K>),
    Large { map: MapxOrd<K, ()>, len: usize },
}

impl<K> fmt::Debug for DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Small(s) => f.debug_tuple("Small").field(&s.len()).finish(),
            Self::Large { len, .. } => {
                f.debug_struct("Large").field("len", len).finish()
            }
        }
    }
}

impl<K> ValueEnDe for DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn try_encode(&self) -> Result<Vec<u8>> {
        match self {
            Self::Small(set) => {
                let mut buf = vec![TAG_SMALL];
                let count = set.len() as u32;
                buf.extend_from_slice(&count.to_le_bytes());
                for k in set {
                    let kb = k.to_bytes();
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&kb);
                }
                Ok(buf)
            }
            Self::Large { map, len } => {
                // Large variant: only persist the MapxOrd handle + len;
                // the actual entries live on disk inside MapxOrd already.
                let mut buf = vec![TAG_LARGE];
                let handle_bytes = map.encode();
                buf.extend_from_slice(&(handle_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&handle_bytes);
                buf.extend_from_slice(&(*len as u64).to_le_bytes());
                Ok(buf)
            }
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(VsdbError::Decode {
                detail: "empty DataCtner bytes".to_owned(),
            });
        }
        match bytes[0] {
            TAG_SMALL => {
                let mut off = 1;
                if bytes.len() < off + 4 {
                    return Err(VsdbError::Decode {
                        detail: "truncated count".to_owned(),
                    });
                }
                let count =
                    u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let mut set = BTreeSet::new();
                for _ in 0..count {
                    if bytes.len() < off + 4 {
                        return Err(VsdbError::Decode {
                            detail: "truncated key len".to_owned(),
                        });
                    }
                    let klen =
                        u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap())
                            as usize;
                    off += 4;
                    if bytes.len() < off + klen {
                        return Err(VsdbError::Decode {
                            detail: "truncated key data".to_owned(),
                        });
                    }
                    let k = K::from_slice(&bytes[off..off + klen])?;
                    off += klen;
                    set.insert(k);
                }
                Ok(Self::Small(set))
            }
            TAG_LARGE => {
                let mut off = 1;
                if bytes.len() < off + 4 {
                    return Err(VsdbError::Decode {
                        detail: "truncated handle len".to_owned(),
                    });
                }
                let hlen =
                    u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                if bytes.len() < off + hlen {
                    return Err(VsdbError::Decode {
                        detail: "truncated handle data".to_owned(),
                    });
                }
                let map = MapxOrd::decode(&bytes[off..off + hlen])?;
                off += hlen;
                if bytes.len() < off + 8 {
                    return Err(VsdbError::Decode {
                        detail: "truncated len".to_owned(),
                    });
                }
                let len =
                    u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap()) as usize;
                Ok(Self::Large { map, len })
            }
            _ => Err(VsdbError::Decode {
                detail: "unknown DataCtner tag".to_owned(),
            }),
        }
    }
}

impl<K> DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    pub(crate) fn new() -> Self {
        Self::Small(BTreeSet::new())
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Small(i) => i.len(),
            Self::Large { len, .. } => *len,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        0 == self.len()
    }

    pub(crate) fn clear_storage(&mut self) {
        if let Self::Large { map, .. } = self {
            map.clear();
        }
    }

    fn try_upgrade(&mut self) {
        let inner_set = match self {
            Self::Small(set) if set.len() >= INLINE_CAPACITY_THRESHOLD => set,
            _ => return,
        };

        let set_len = inner_set.len();
        let new_map = inner_set.iter().fold(MapxOrd::new(), |mut acc, k| {
            acc.insert(k, &());
            acc
        });

        *self = Self::Large {
            map: new_map,
            len: set_len,
        };
    }

    pub(crate) fn insert(&mut self, k: K) -> bool {
        match self {
            Self::Small(set) => {
                // Only upgrade if we're about to exceed the inline threshold with a new key.
                if set.len() >= INLINE_CAPACITY_THRESHOLD && !set.contains(&k) {
                    // upgrade in-place (reuse existing helper)
                    self.try_upgrade();
                    // self is now Large, fall through by re-calling insert on the new state
                    return self.insert(k);
                }
                set.insert(k)
            }
            Self::Large { map, len } => {
                let existed = map.get(&k).is_some();
                map.insert(&k, &());
                if !existed {
                    *len += 1;
                }
                !existed
            }
        }
    }

    /// Insert many keys at once, returning how many were newly added.
    ///
    /// Semantically identical to calling [`insert`](Self::insert) per key
    /// (same promotion point, same dedup), but the `Large` variant stages
    /// all new keys into a single engine write batch instead of one
    /// engine put per key.
    pub(crate) fn insert_batch(&mut self, ks: Vec<K>) -> Result<usize> {
        let mut added = 0usize;
        let mut idx = 0usize;
        // Small phase: absorb keys inline until promotion triggers.
        while idx < ks.len() {
            let promote = match self {
                Self::Small(set) => {
                    let k = &ks[idx];
                    if set.len() >= INLINE_CAPACITY_THRESHOLD && !set.contains(k) {
                        true
                    } else {
                        if set.insert(k.clone()) {
                            added += 1;
                        }
                        idx += 1;
                        false
                    }
                }
                Self::Large { .. } => break,
            };
            if promote {
                self.try_upgrade();
            }
        }
        if idx >= ks.len() {
            return Ok(added);
        }
        let Self::Large { map, len } = self else {
            unreachable!("promoted to Large above")
        };
        // Two passes: existence checks first (a live batch holds the
        // map's &mut borrow), then one batched write for the new keys.
        let mut new_keys = BTreeSet::new();
        for k in &ks[idx..] {
            if !new_keys.contains(k) && map.get(k).is_none() {
                new_keys.insert(k.clone());
            }
        }
        if !new_keys.is_empty() {
            let mut b = map.batch_entry();
            for k in &new_keys {
                b.insert(k, &());
            }
            b.commit()?;
            *len += new_keys.len();
            added += new_keys.len();
        }
        Ok(added)
    }

    pub(crate) fn remove(&mut self, target: &K) -> bool {
        match self {
            Self::Small(i) => i.remove(target),
            Self::Large { map, len } => {
                let existed = map.get(target).is_some();
                if existed {
                    map.remove(target);
                    *len -= 1;
                }
                existed
            }
        }
    }

    pub(crate) fn iter(&self) -> DataCtnerIter<'_, K> {
        match self {
            Self::Small(i) => DataCtnerIter::Small(i.iter()),
            Self::Large { map, .. } => DataCtnerIter::Large(Box::new(map.iter())),
        }
    }
}

impl<K> Default for DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn default() -> Self {
        Self::new()
    }
}

// =========================================================================
// DataCtnerIter
// =========================================================================

pub(crate) enum DataCtnerIter<'a, K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    Small(SmallIter<'a, K>),
    Large(Box<LargeIter<'a, K, ()>>),
}

impl<K> Iterator for DataCtnerIter<'_, K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    type Item = K;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next().cloned(),
            Self::Large(i) => i.next().map(|j| j.0),
        }
    }
}

impl<K> DoubleEndedIterator for DataCtnerIter<'_, K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next_back().cloned(),
            Self::Large(i) => i.next_back().map(|j| j.0),
        }
    }
}
