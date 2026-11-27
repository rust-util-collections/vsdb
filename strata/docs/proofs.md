# Merkle tries and versioned proofs

Use a standalone trie to compute a root from application-supplied bytes.
Use `VerMapWithProof<K, V, T>` to prove the contents of a branch or a
retained commit in a persistent map. Both backends support membership
and absence proofs.

## Standalone tries

| Backend | How it addresses a key |
| --- | --- |
| `MptCalc` (Merkle Patricia trie) | Follows the key's nibbles. Insert, batch update and construction from entries accept keys up to 1,024 bytes. |
| `SmtCalc` (sparse Merkle trie) | Hashes the key to a 256-bit path. The path length is independent of input key length. |

```rust
use vsdb::SmtCalc;

let mut trie = SmtCalc::new();
trie.insert(b"alice", b"100").unwrap();
let root = trie.root_hash();

let present = trie.prove(b"alice").unwrap();
assert!(SmtCalc::verify_proof(&root, b"alice", &present).unwrap());
assert_eq!(present.value(), Some(&b"100"[..]));

let absent = trie.prove(b"bob").unwrap();
assert!(SmtCalc::verify_proof(&root, b"bob", &absent).unwrap());
assert_eq!(absent.value(), None);
```

Both calculators implement `get`, `insert`, `remove` and `batch_update`.
In a batch, `Some(value)` sets a key and `None` removes it. The candidate root
becomes visible after every edit in the batch succeeds. Trie clones share
immutable subtrees. Root and proof queries take a shared reference.

## Proof transport

Encode an `MptProof` or `SmtProof` with `to_bytes()`. The receiver decodes it
with `from_bytes()` and verifies it against the expected key and a root it
already trusts.

```rust
use vsdb::{MptCalc, MptProof};

let mut trie = MptCalc::new();
trie.insert(b"account", b"10").unwrap();
let expected_root = trie.root_hash();
let bytes = trie.prove(b"account").unwrap().to_bytes().unwrap();
let received = MptProof::from_bytes(&bytes).unwrap();
assert!(MptCalc::verify_proof(&expected_root, b"account", &received).unwrap());
```

`to_bytes()` produces the same postcard representation as
`postcard::to_allocvec`. `from_bytes()` checks the format tag and rejects
trailing bytes. Version 1 uses these fields in wire order.

| Proof | Eight-byte tag | Fields after the tag |
| --- | --- | --- |
| MPT | `VSMPTP01` | Key, optional value, nodes |
| SMT | `VSSMTP01` | Key hash, optional leaf, siblings |

## Versioned proofs

| State to prove | Method |
| --- | --- |
| A branch's working state, including pending edits | `prove_at(branch, &key)` |
| A retained history commit | `prove_at_commit(commit, &key)` |
| More keys against the last synchronized trie | `prove_key(&key)` for typed keys or `prove(encoded_key)` for bytes |

The first two methods select their state and return a serializable
`ProofWithRoot` in one call. Earlier root queries or checkpoint saves do
not affect which state those calls select. The wrapper uses VerMap's
ordered key encoding for both proof generation and verification.

```rust
use vsdb::{SmtCalc, VerMapWithProof};

type State = VerMapWithProof<u64, String, SmtCalc>;
let mut state = State::new();
let main = state.map().main_branch();
state.map_mut().insert(main, &7, &"pending".into()).unwrap();
let working = state.prove_at(main, &7).unwrap();

let commit = state.map_mut().commit(main).unwrap();
let saved = state.prove_at_commit(commit, &7).unwrap();
assert_eq!(working.root_hash, saved.root_hash);
assert!(State::verify_key_proof(&saved.root_hash, &7, &saved.proof).unwrap());
```

`new_in` places the wrapped map in a namespace. `clone_in` deep-copies its
working trees and retained history. The wrapper and inner map share an
`InstanceId`. Call `save_meta()` on each handle type you plan to restore.
The first type saved uses the primary metadata file and additional types
use tagged files. The wrapper's type tag binds the trie backend, key type
and value type. [VerMap branches and snapshots](versioned.md)

## Save and restore a checkpoint

For a standalone calculator, `save_cache(dir, cache_id, sync_tag)` writes
the trie to the supplied directory. `load_cache(dir, cache_id)` returns
the calculator, the saved tag and the root hash.

For a versioned map, `save_cache(commit)` synchronizes the trie to that
commit and writes a checkpoint. To prove pending edits afterward, use
`prove_at(branch, &key)` or synchronize again with `merkle_root(branch)`.
Construction tries to load an available cache and the map can rebuild the
trie from its rows. Calculating a root and dropping the wrapper do not save
a checkpoint. Historical queries return `CommitNotFound` for a reclaimed
commit even if a corresponding cache file exists.

MPTC v2 and SMTC v4 checkpoints store nodes in post-order and omit cached
node hashes. The loader verifies the checksum, canonical tree shape and root.
For a versioned trie it also checks the universe, map, commit and B+ tree
root IDs. It bounds node count, pending subtrees, values and decoded memory.
The encoded file limit is 512 MiB including the checksum. An oversized save
returns an error and preserves the previous file. [Checkpoint format and validation](../src/trie/cache.rs)
