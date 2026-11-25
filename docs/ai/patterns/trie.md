# Merkle Trie Review Patterns

**Files:** `trie/{mod,cache,codec_util,nibbles,proof,shared,transport,test}.rs` (`cache` = shared `MPTC\x02`/`SMTC\x04` streaming checkpoints; `proof` = `VerMapWithProof`), `trie/{mpt,smt,node}/**`.

**Arch:** MPT 16-ary nibble (POPCNT `u16` bitmap `Children`, inline-64/heap `Nibbles`); SMT binary 256-bit, JMT-compressed, single `EMPTY_HASH`; `Arc`-COW `Handle` (`edit` = `Arc::make_mut` + `OnceLock::take()`, `try_update` candidate-root rollback on `Err`/unwind, iterative `Record::drop`); `VerMapWithProof` integration.

## Invariants

**T1 Proof sound** — prove/verify same encode, hash, path (MPT nibble / SMT bit); wrong key/root fails.
**T2 MPT nibbles** — high `>>4`, low `&0x0F`; path len 2×bytes; consistent all ops.
**T3 SMT defaults** — JMT-style compressed tree: every empty subtree is `EMPTY_HASH = [0; 32]`, at every depth; `hash_internal(EMPTY_HASH, EMPTY_HASH)` returns `EMPTY_HASH`. Construction and proof folding must use the same rule.
**T4 Hash det** — same logical state → same root; extensions max-compressed.
**T5 Cache binding** — check universe, map identity, commit and B+ tree root.
Keep independent committed and working candidate roots. Changing map identity
invalidates both cached states. Verify a historical commit is still live before
serving it. Save only when explicitly checkpointing.

**T6 Non-exist proofs** — MPT: empty slot, leaf-path mismatch, or divergent extension. SMT: empty subtree or a foreign lone leaf sharing the proven prefix.
**T7 Cache trust boundary** — parse post-order records with file, node,
value, decoded memory and pending-subtree limits. Check canonical shapes,
including the side of each SMT branch against its descendant key hashes.
Recompute the commitment and verify the checksum before returning the tree.
Failed mutations preserve the original root.

## Bugs

**Nibble swap** · prove/verify serialize order drift · cache root for wrong commit.

## Checklist

- [ ] Prove/verify same hash/encode paths
- [ ] Nibble order consistent
- [ ] SMT single `EMPTY_HASH`; empty⊕empty = EMPTY in build and proof fold
- [ ] Deterministic canonical roots
- [ ] Cache stamp = synced commit; mismatch rebuilds
- [ ] Non-exist: every MPT/SMT absence shape
- [ ] Consistent keccak (no accidental sha256 mix)
- [ ] No consecutive bare extensions
