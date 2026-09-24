# VSDB Technical Bug Patterns

Cross-cutting catalog for VSDB + mmdb integration: every collection sits on these
engine, unsafe, and encoding contracts. Load before review/debug.

Subsystem invariants live only in `patterns/{btree,versioning,trie,engine,slotdex,dagmap,vecdex}.md`
— always load the mapped guide when those paths change; do not restate them here.

## 1. Prefix / engine

### 1.1 Prefix collision
Only forward floor/ceiling/cursors; durable ceiling before issue; recovered prefixes reserved; never reused.

### 1.2 Shard routing
Read/write/delete/iter use owning engine’s `dbs.len()`, never hardcoded default.

### 1.3 Namespace double-open
Default one-time init; non-default re-check `OPEN_NAMESPACES` under `REGISTRY_LOCK`.

### 1.4 Cross-prefix atomicity
Engine batches are single-prefix. Logical multi-prefix atomicity needs single-handle staging (SlotDex/VecDex) or dirty-flag/root-last recovery (VerMap) — never an assumed shared batch.

## 2. Unsafe

### 2.1 shadow() race
Same-key concurrent write or overlapping structural multi-key ops violate alias contract. Disjoint plain-map keys OK.

### 2.2 MapxRaw::from_bytes_in untrusted
Require same type, unique prefix ownership, correct namespace; no external untrusted bytes.

### 2.3 Entry API cast
`*mut` cast only from the Entry's `&'a mut` handle; the two derefs sit in exclusive match arms and never coexist. A cast from a shared ref is UB.

## 3. Encoding

### 3.1 Non-canonical keys
Postcard key bytes must be canonical — `HashMap`/`HashSet`/floats (±0.0, NaN) make lookups silently miss. Ordered maps need `KeyEnDeOrdered`, never postcard order.

### 3.2 Version incompatibility
Pin postcard; explicit tags/envelopes + old fixtures. `#[serde(default)]` alone ≠ safe postcard layout change. See `compatibility-policy.md`.

### 3.3 Node codec asymmetry
Hand codecs encode/decode field order match + round-trip tests.

### 3.4 Encoded-byte equality
Typed wrappers compare **decoded** values (NaN/non-canonical). Not raw bytes.
