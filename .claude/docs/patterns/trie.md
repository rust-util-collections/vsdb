# Merkle Trie Subsystem Review Patterns

## Files
- `strata/src/trie/mod.rs` — MptCalc, SmtCalc, TrieCalc trait
- `strata/src/trie/mpt/` — Merkle Patricia Trie (16-ary, nibble-based)
- `strata/src/trie/smt/` — Sparse Merkle Tree (binary, 256-bit paths)
- `strata/src/trie/node/` — trie node codecs
- `strata/src/trie/cache.rs` — disk cache for trie snapshots
- `strata/src/trie/proof.rs` — VerMapWithProof integration

## Architecture
- **MPT**: Ethereum-style 16-ary trie. Path = key nibbles (4-bit each). Nodes: Branch (16 children), Extension (shared prefix), Leaf (value).
- **SMT**: Binary trie with 256-bit paths. Empty subtrees hash to a known default. Constant-depth proofs.
- Both are **stateless** computation layers — ephemeral in-memory, backed by disk cache.
- Integrated with VerMap via VerMapWithProof for version-aware Merkle commitments.

## Critical Invariants

### INV-T1: Proof Soundness
A proof generated for key K with root R must verify successfully against R. Conversely, a proof for a different key or root must fail.
**Check**: Verify proof generation and verification use identical: (1) node encoding, (2) hash function, (3) path computation.

### INV-T2: Nibble Path Correctness (MPT)
Key bytes → nibble path: `byte[i]` produces nibbles `byte[i] >> 4` (high) then `byte[i] & 0x0F` (low). Path length = 2 * key_byte_length.
**Check**: Verify nibble extraction is consistent across all MPT operations (insert, get, prove, verify).

### INV-T3: SMT Default Hash Consistency
Empty subtrees at each level hash to a well-known default. The default at level L is `hash(default[L+1] || default[L+1])`.
**Check**: Verify default hashes are computed identically everywhere — build, prove, verify, cache restore. Must be compile-time constants or identically computed.

### INV-T4: Hash Determinism
The same logical trie state must always produce the same root hash, regardless of insertion order or intermediate states.
**Check**: Verify node canonical form — no ambiguous representations. Extension nodes must be maximally compressed.

### INV-T5: Cache Versioning
Trie cache entries must be keyed by (branch, commit_id) to prevent stale reads.
**Check**: Verify cache invalidation or versioning on new commits. A cache miss must trigger recomputation, not return stale data.

### INV-T6: Non-Existence Proof Correctness
For a key NOT in the trie, the proof must demonstrate that the key's path leads to an empty slot or a different key.
**Check**: Verify non-existence proofs handle both cases: (1) path terminates at empty, (2) path diverges at an extension/branch node.

## Common Bug Patterns

### Nibble Inversion (technical-patterns.md 3.2)
High and low nibbles swapped: `byte & 0x0F` used for high nibble instead of `byte >> 4`.
**Impact**: Every key maps to the wrong trie path. Proofs generated with one convention fail with the other.

### Proof Serialization Mismatch (technical-patterns.md 3.1)
Proof prover serializes nodes in one order, verifier expects another.
**Trigger**: Upgrade changes node encoding without migrating existing proofs.

### Cache Stale After Commit (technical-patterns.md 3.4)
Cache returns Merkle root from commit C1 when asked for commit C2.
**Trigger**: Insert keys → commit C1 → cache root → insert more → commit C2 → cache still returns C1 root.

## Review Checklist
- [ ] Proof generation and verification use identical code paths for hashing/encoding
- [ ] Nibble extraction consistent: high = `>> 4`, low = `& 0x0F`
- [ ] SMT default hashes are constants, identical across all usage sites
- [ ] Root hash is deterministic (canonical node representation)
- [ ] Cache keyed by (branch, commit) — stale entries impossible
- [ ] Non-existence proofs handle both empty-slot and divergent-path cases
- [ ] sha3/keccak hash used consistently (no accidental sha256 mix)
- [ ] Extension node compaction — no two consecutive extension nodes
