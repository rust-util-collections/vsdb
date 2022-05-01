//!
//! A simple 'Merkle-Tree' ported from solana project.
//!

use crate::{
    basic::{mapx_ord_rawkey::MapxOrdRawKey, vecx_raw::VecxRaw},
    common::RawBytes,
};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};

type Hash = RawBytes;

fn hashv(data: &[&[u8]]) -> Hash {
    let mut hasher = Sha3_256::new();
    for bytes in data {
        hasher.update(bytes);
    }
    hasher.finalize().as_slice().to_vec().into_boxed_slice()
}

// We need to discern between leaf and intermediate nodes to prevent trivial second
// pre-image attacks.
// https://flawed.net.nz/2018/02/21/attacking-merkle-trees-with-a-second-preimage-attack
const LEAF_PREFIX: &[u8] = &[0];
const INTERMEDIATE_PREFIX: &[u8] = &[1];

macro_rules! hash_leaf {
    {$d:ident} => {
        hashv(&[LEAF_PREFIX, $d])
    }
}

macro_rules! hash_intermediate {
    {$l:ident, $r:ident} => {
        hashv(&[INTERMEDIATE_PREFIX, $l, $r])
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MerkleTree {
    leaf_count: usize,
    nodes: Vec<Hash>,
    hash_to_idx: MapxOrdRawKey<u64>,
}

#[derive(Debug, PartialEq)]
pub struct ProofEntry<'a>(&'a [u8], Option<&'a [u8]>, Option<&'a [u8]>);

impl<'a> ProofEntry<'a> {
    pub fn new(
        target: &'a Hash,
        left_sibling: Option<&'a [u8]>,
        right_sibling: Option<&'a [u8]>,
    ) -> Self {
        assert!((None == left_sibling) ^ (None == right_sibling));
        Self(target, left_sibling, right_sibling)
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct Proof<'a>(Vec<ProofEntry<'a>>);

impl<'a> Proof<'a> {
    pub fn push(&mut self, entry: ProofEntry<'a>) {
        self.0.push(entry)
    }

    pub fn verify(&self, target: &[u8]) -> bool {
        let hash = hash_leaf!(target);
        self.verify_by_hash(hash)
    }

    pub fn verify_by_hash(&self, target_hash: Hash) -> bool {
        let result = self.0.iter().try_fold(target_hash, |target_hash, pe| {
            let lsib = pe.1.unwrap_or(&target_hash);
            let rsib = pe.2.unwrap_or(&target_hash);
            let hash = hash_intermediate!(lsib, rsib);

            if &hash[..] == pe.0 { Some(hash) } else { None }
        });
        matches!(result, Some(_))
    }
}

impl MerkleTree {
    #[inline(always)]
    fn next_level_len(level_len: usize) -> usize {
        if level_len == 1 {
            0
        } else {
            (level_len + 1) / 2
        }
    }

    #[inline(always)]
    fn calculate_vec_capacity(leaf_count: usize) -> usize {
        // the most nodes consuming case is when n-1 is full balanced binary tree
        // then n will cause the previous tree add a left only path to the root
        // this cause the total nodes number increased by tree height, we use this
        // condition as the max nodes consuming case.
        // n is current leaf nodes number
        // assuming n-1 is a full balanced binary tree, n-1 tree nodes number will be
        // 2(n-1) - 1, n tree height is closed to log2(n) + 1
        // so the max nodes number is 2(n-1) - 1 + log2(n) + 1, finally we can use
        // 2n + log2(n+1) as a safe capacity value.
        // test results:
        // 8192 leaf nodes(full balanced):
        // computed cap is 16398, actually using is 16383
        // 8193 leaf nodes:(full balanced plus 1 leaf):
        // computed cap is 16400, actually using is 16398
        // about performance: current used fast_math log2 code is constant algo time
        if leaf_count > 0 {
            fast_math::log2_raw(leaf_count as f32) as usize + 2 * leaf_count + 1
        } else {
            0
        }
    }

    pub fn new(items: &[&[u8]]) -> Self {
        let cap = MerkleTree::calculate_vec_capacity(items.len());
        let mut mt = MerkleTree {
            leaf_count: items.len(),
            nodes: Vec::with_capacity(cap),
            hash_to_idx: MapxOrdRawKey::new(),
        };

        for (idx, item) in items.iter().enumerate() {
            let hash = hash_leaf!(item);
            mt.hash_to_idx.insert_ref(&hash, &(idx as u64));
            mt.nodes.push(hash);
        }

        let mut level_len = MerkleTree::next_level_len(items.len());
        let mut level_start = items.len();
        let mut prev_level_len = items.len();
        let mut prev_level_start = 0;
        while level_len > 0 {
            for i in 0..level_len {
                let prev_level_idx = 2 * i;
                let lsib = &mt.nodes[prev_level_start + prev_level_idx];
                let rsib = if prev_level_idx + 1 < prev_level_len {
                    &mt.nodes[prev_level_start + prev_level_idx + 1]
                } else {
                    // Duplicate last entry if the level length is odd
                    &mt.nodes[prev_level_start + prev_level_idx]
                };

                let hash = hash_intermediate!(lsib, rsib);
                mt.nodes.push(hash);
            }
            prev_level_start = level_start;
            prev_level_len = level_len;
            level_start += level_len;
            level_len = MerkleTree::next_level_len(level_len);
        }

        mt
    }

    #[inline(always)]
    pub fn get_root(&self) -> Option<&Hash> {
        self.nodes.iter().last()
    }

    #[inline(always)]
    pub fn get_proof_path(&self, target: &[u8]) -> Option<Proof> {
        let hash = hash_leaf!(target);
        self.get_proof_path_by_hash(hash)
    }

    #[inline(always)]
    pub fn get_proof_path_by_hash(&self, target_hash: Hash) -> Option<Proof> {
        let idx = self.hash_to_idx.get(&target_hash)? as usize;
        self.get_proof_path_by_index(idx)
    }

    pub fn get_proof_path_by_index(&self, index: usize) -> Option<Proof> {
        if index >= self.leaf_count {
            return None;
        }

        let mut level_len = self.leaf_count;
        let mut level_start = 0;
        let mut path = Proof::default();
        let mut node_index = index;
        let mut lsib = None;
        let mut rsib = None;
        while level_len > 0 {
            let level = &self.nodes[level_start..(level_start + level_len)];

            let target = &level[node_index];
            if lsib != None || rsib != None {
                path.push(ProofEntry::new(target, lsib, rsib));
            }
            if node_index % 2 == 0 {
                lsib = None;
                rsib = if node_index + 1 < level.len() {
                    Some(&level[node_index + 1])
                } else {
                    Some(&level[node_index])
                };
            } else {
                lsib = Some(&level[node_index - 1]);
                rsib = None;
            }
            node_index /= 2;

            level_start += level_len;
            level_len = MerkleTree::next_level_len(level_len);
        }
        Some(path)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MerkleTreeStore {
    leaf_count: usize,
    nodes: VecxRaw,
    hash_to_idx: MapxOrdRawKey<u64>,
}

impl From<&MerkleTree> for MerkleTreeStore {
    #[inline(always)]
    fn from(mt: &MerkleTree) -> Self {
        let nodes = VecxRaw::new();
        mt.nodes.iter().for_each(|h| {
            nodes.push_ref(h);
        });
        Self {
            leaf_count: mt.leaf_count,
            nodes,
            hash_to_idx: mt.hash_to_idx,
        }
    }
}

impl From<&MerkleTreeStore> for MerkleTree {
    #[inline(always)]
    fn from(mts: &MerkleTreeStore) -> Self {
        Self {
            leaf_count: mts.leaf_count,
            nodes: mts.nodes.iter().collect(),
            hash_to_idx: mts.hash_to_idx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST: &[&[u8]] = &[
        b"my", b"very", b"eager", b"mother", b"just", b"served", b"us", b"nine",
        b"pizzas", b"make", b"prime",
    ];
    const BAD: &[&[u8]] = &[b"bad", b"missing", b"false"];

    #[test]
    fn test_tree_from_empty() {
        let mt = MerkleTree::new(&[]);
        assert_eq!(mt.get_root(), None);
    }

    #[test]
    fn test_tree_from_one() {
        let input = b"test";
        let mt = MerkleTree::new(&[input]);
        let expected = hash_leaf!(input);
        assert_eq!(mt.get_root(), Some(&expected));
    }

    #[test]
    fn test_path_creation() {
        let mt = MerkleTree::new(TEST);
        for (i, _s) in TEST.iter().enumerate() {
            let _path = mt.get_proof_path_by_index(i).unwrap();
        }
    }

    #[test]
    fn test_path_creation_bad_index() {
        let mt = MerkleTree::new(TEST);
        assert_eq!(mt.get_proof_path_by_index(TEST.len()), None);
    }

    #[test]
    fn test_path_verify_by_hash_good() {
        let mt = MerkleTree::new(TEST);

        for s in TEST.iter() {
            let path = mt.get_proof_path(s).unwrap();
            assert!(path.verify(s));
        }

        for (i, s) in TEST.iter().enumerate() {
            let hash = hash_leaf!(s);
            let path = mt.get_proof_path_by_index(i).unwrap();
            assert!(path.verify_by_hash(hash));
        }
    }

    #[test]
    fn test_path_verify_by_hash_bad() {
        let mt = MerkleTree::new(TEST);

        for s in BAD.iter() {
            assert!(mt.get_proof_path(s).is_none());
        }

        for (i, s) in BAD.iter().enumerate() {
            let hash = hash_leaf!(s);
            let path = mt.get_proof_path_by_index(i).unwrap();
            assert!(!path.verify_by_hash(hash));
        }
    }

    #[test]
    fn test_proof_entry_instantiation_lsib_set() {
        ProofEntry::new(&Hash::default(), Some(&Hash::default()), None);
    }

    #[test]
    fn test_proof_entry_instantiation_rsib_set() {
        ProofEntry::new(&Hash::default(), None, Some(&Hash::default()));
    }

    #[test]
    fn test_nodes_capacity_compute() {
        let iteration_count = |mut leaf_count: usize| -> usize {
            let mut capacity = 0;
            while leaf_count > 0 {
                capacity += leaf_count;
                leaf_count = MerkleTree::next_level_len(leaf_count);
            }
            capacity
        };

        // test max 64k leaf nodes compute
        for leaf_count in 0..65536 {
            let math_count = MerkleTree::calculate_vec_capacity(leaf_count);
            let iter_count = iteration_count(leaf_count);
            assert!(math_count >= iter_count);
        }
    }

    #[test]
    #[should_panic]
    fn test_proof_entry_instantiation_both_clear() {
        ProofEntry::new(&Hash::default(), None, None);
    }

    #[test]
    #[should_panic]
    fn test_proof_entry_instantiation_both_set() {
        ProofEntry::new(
            &Hash::default(),
            Some(&Hash::default()),
            Some(&Hash::default()),
        );
    }
}
