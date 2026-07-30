# Merkle Trie Review Patterns

**Files:** `trie/{mod,mpt,smt,node,nibbles,error,cache,codec_util,proof}.rs`.

**Arch:** MPT 16-ary nibble; SMT binary 256-bit + empty defaults; stateless compute
+ disk cache; VerMapWithProof integration.

## Invariants

**T1 Proof sound** — prove/verify same encode, hash, path; wrong key/root fails.
**T2 MPT nibbles** — high `>>4`, low `&0x0F`; path len 2×bytes; consistent all ops.
**T3 SMT defaults** — level L = hash(def[L+1]||def[L+1]); identical everywhere.
**T4 Hash det** — same logical state → same root; extensions max-compressed.
**T5 Cache version** — key (branch, commit); miss recomputes, never silent stale.
**T6 Non-exist proofs** — empty slot **or** divergent extension/branch.
**T7 Cache trust boundary** — loaders validate whole-tree shape (depth/nibble budget,
leaf placement, hash len), recompute cached hashes, reject trailing/mixed/oversize;
checksum ≠ structure guarantee. Walker assumptions need matching deserialize checks.
Failed sync must not leave take’d root unrestored / false commit bookkeeping.

## Bugs

**Nibble swap** · prove/verify serialize order drift · cache root for wrong commit.

## Checklist

- [ ] Prove/verify same hash/encode paths
- [ ] Nibble order consistent
- [ ] SMT defaults const/identical
- [ ] Deterministic canonical roots
- [ ] Cache keyed; no stale
- [ ] Non-exist both cases
- [ ] Consistent keccak (no accidental sha256 mix)
- [ ] No consecutive bare extensions
