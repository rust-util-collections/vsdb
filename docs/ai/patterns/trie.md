# Merkle Trie Review Patterns

[MPT, SMT and versioned proofs](../../../strata/src/trie)

**Arch:** MPT 16-ary nibble (POPCNT `u16` bitmap `Children`, inline-64/heap `Nibbles`); SMT binary 256-bit, JMT-compressed, single `EMPTY_HASH`; `Arc`-COW `Handle` (`edit` = `Arc::make_mut` + `OnceLock::take()`, `try_update` candidate-root rollback on `Err`/unwind, iterative `Record::drop`); `VerMapWithProof` integration.

## Invariants

**T1 Proof verification** — Generation and verification use the same key
encoding, Keccak hash and path (MPT nibbles or SMT bits). Exercise a wrong
key and a wrong root as well as a valid proof. [Proof APIs and transport](../../../strata/docs/proofs.md)

**T2 MPT nibbles** — high `>>4`, low `&0x0F`; path len 2×bytes; consistent all ops.

**T3 SMT defaults** — JMT-style compressed tree: every empty subtree is `EMPTY_HASH = [0; 32]`, at every depth; `hash_internal(EMPTY_HASH, EMPTY_HASH)` returns `EMPTY_HASH`. Construction and proof folding must use the same rule.

**T4 Canonical roots** — The same logical state produces the same root.
Compress extension paths fully, including consecutive bare extensions.

**T5 Cache binding** — check universe, map identity, commit and B+ tree root.
Keep independent committed and working candidate roots. Changing map identity
invalidates both cached states. Verify a historical commit is still live before
serving it. Save only when explicitly checkpointing. The working cursor holds
a lease on the last hashed dirty root. Successive changes on the same branch
can stream the diff from that root in chunks. A high-churn rebuild and a
branch or commit switch must produce the same commitment as full construction.
`VerMapWithProof::flush` and `close` drop the cursor lease before delegating
to the version store.

**T6 Non-exist proofs** — MPT: empty slot, leaf-path mismatch, or divergent extension. SMT: empty subtree or a foreign lone leaf sharing the proven prefix.

**T7 Cache trust boundary** — parse post-order records with file, node,
value, decoded memory and pending-subtree limits. Check canonical shapes,
including the side of each SMT branch against its descendant key hashes.
Recompute the commitment and verify the checksum before returning the tree.
Failed mutations preserve the original root. Rebuild a stale checkpoint
from the selected map state.
