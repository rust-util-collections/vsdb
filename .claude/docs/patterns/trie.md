# Merkle Trie Review Patterns

**Files:** `trie/{mod,cache,codec_util,error,nibbles,proof,test}.rs` (`cache` = MPT
disk cache; `proof` = `VerMapWithProof`), `trie/{mpt,smt,node}/**`.

**Arch:** MPT 16-ary nibble; SMT binary 256-bit, JMT-compressed, single `EMPTY_HASH`;
stateless compute + disk cache; VerMapWithProof integration.

## Invariants

**T1 Proof sound** — prove/verify same encode, hash, path (MPT nibble / SMT bit); wrong key/root fails.
**T2 MPT nibbles** — high `>>4`, low `&0x0F`; path len 2×bytes; consistent all ops.
**T3 SMT defaults** — JMT-style compressed tree: every empty subtree is `EMPTY_HASH = [0; 32]`, at every depth; `hash_internal(EMPTY_HASH, EMPTY_HASH)` returns `EMPTY_HASH`. Construction and proof folding must use the same rule.
**T4 Hash det** — same logical state → same root; extensions max-compressed.
**T5 Cache version** — one file per map (`{mpt,smt}_cache_<map_id>.bin`) stamped with the synced CommitId; in-memory sync point = (owning InstanceId, branch, commit, dirty applied). Map replacement invalidates all cached state before sync shortcuts; Drop never saves across an identity mismatch. Stamp/root mismatch or diff failure → rebuild, never silent stale.
**T6 Non-exist proofs** — MPT: empty slot, leaf-path mismatch, or divergent extension. SMT: empty subtree or a foreign lone leaf sharing the proven prefix.
**T7 Cache trust boundary** — loaders validate whole-tree shape (depth/nibble budget,
leaf placement, hash len), recompute cached hashes, reject trailing/mixed/oversize;
checksum ≠ structure guarantee. Walker assumptions need matching deserialize checks.
Failed sync must not leave take’d root unrestored / false commit bookkeeping.

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
