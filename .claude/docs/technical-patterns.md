# VSDB Technical Bug Patterns

Catalog for VSDB + mmdb integration. Load before review/debug.

Subsystem-only dense INV lists: `patterns/{btree,versioning,trie,engine,slotdex,dagmap,vecdex}.md` — always load when those paths change.

## 1. B+ tree (COW)

### 1.1 In-place mutation
**Pattern:** Mutate existing NodeId → breaks sharing across branches.
**Where:** `persistent_btree/` insert/delete/split/merge.
**Check:** Every mutation allocates new NodeId up the path; old untouched.

### 1.2 Split boundary
**Pattern:** Wrong separator for internal vs leaf (B=16, max 32 keys).
**Check:** Internal: promote+remove separator from both children. Leaf: copy right.min to parent so `left.max < sep == right.min`; no data key in both leaves.

### 1.3 Sharing / GC leak
**Pattern:** Replaced nodes unreachable but never GC’d.
**Check:** Track replaced IDs; GC from **all** live commit roots.

### 1.4 Empty after delete
**Pattern:** Under-min occupancy without merge/rebalance.
**Check:** Merge/rebalance when below min (root exempt).

## 2. Versioning (VerMap)

### 2.1 Ref-count imbalance
**Pattern:** Branch/commit/merge miss inc or dec.
**Check:** Inc every new reference; dec every loss; check zero **after** dec.

### 2.2 Three-way merge error
**Pattern:** Source-wins conflict resolved to base/target.
**Where:** `merge.rs`. Source wins if both modified since common ancestor.

### 2.3 Commit DAG cycle
**Pattern:** Parent list includes self (transitively).
**Check:** Parents are earlier existing commits.

### 2.4 Rollback ref-count
**Pattern:** Rollback decs commits still needed by other branches.
**Check:** Only drop **this** branch’s reference contribution.

### 2.5 Dirty flag
**Pattern:** Non-idempotent cascade crashes without dirty set.
**Check:** Flag before commit/merge/branch create|delete/rollback ref mutations; clear after. `gc()` is idempotent recount — no new flag required.

## 3. Merkle tries

### 3.1 Prove/verify mismatch
Identical node encode, hash, path (nibble vs bit) on both sides.

### 3.2 MPT nibble path
High `byte>>4`, low `byte&0x0F`; path len = 2×key bytes.

### 3.3 SMT default hash
Empty-subtree defaults identical everywhere (const preferred).

### 3.4 Cache staleness
Cache key (branch, commit); no stale root after new commits.

## 4. Prefix / engine

### 4.1 Prefix collision
Only forward floor/ceiling/cursors; durable ceiling before issue; recovered prefixes reserved; never reused.

### 4.2 Shard routing
Read/write/delete/iter use owning engine’s `dbs.len()`, never hardcoded default.

### 4.3 Namespace double-open
Default one-time init; non-default re-check `OPEN_NAMESPACES` under `REGISTRY_LOCK`.

### 4.4 Cross-shard WriteBatch
Multi-prefix ops need all-or-nothing if logical atomicity required.

## 5. Unsafe

### 5.1 shadow() race
Same-key concurrent write or overlapping structural multi-key ops violate alias contract. Disjoint plain-map keys OK.

### 5.2 from_bytes untrusted
Require same type, unique prefix ownership, correct namespace; no external untrusted bytes.

### 5.3 Entry API cast
`*mut` from shared ref only under same single-writer contract as shadow.

## 6. Encoding

### 6.1 Non-deterministic keys
postcard of HashMap order breaks MMDB key order — keys must be deterministic.

### 6.2 Version incompatibility
Pin postcard; explicit tags/envelopes + old fixtures. `#[serde(default)]` alone ≠ safe postcard layout change. See `compatibility-policy.md`.

### 6.3 Node codec asymmetry
Hand codecs encode/decode field order match + round-trip tests.

### 6.4 Encoded-byte equality
Typed wrappers compare **decoded** values (NaN/non-canonical). Not raw bytes.
