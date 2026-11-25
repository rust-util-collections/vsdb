# VSDB Technical Bug Patterns

Cross-cutting catalog for VSDB + mmdb integration: every collection sits on these
engine, Rust `unsafe`, and encoding contracts. Load before review/debug.

Use this catalog to check supported local database operations and their
documented results. Each pattern needs a concrete call sequence and observable
failure under `review-core.md`; a matching term alone is not a finding.

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
Engine batches are single-prefix. SlotDex, VecDex and VerMap place each atomic mutation under one prefix. DagMap relies on co-location and recoverable ordering between node prefixes.

## 2. Rust `unsafe` and aliasing contracts

### 2.1 shadow() race
Same-key concurrent write or overlapping structural multi-key ops violate alias contract. Disjoint plain-map keys OK.

### 2.2 Entry ownership

Occupied and vacant entries retain exclusive map access. Guard drops discard edits interrupted by unwinding.

## 3. Encoding

### 3.1 Non-canonical keys
Postcard key bytes must be canonical — `HashMap`/`HashSet`/floats (±0.0, NaN) make lookups silently miss. Ordered maps need `KeyEnDeOrdered`, never postcard order.

### 3.2 Version incompatibility
Postcard uses a compatible requirement, currently 1.1.3. Explicit tags/envelopes + old fixtures. `#[serde(default)]` alone ≠ safe postcard layout change. See `compatibility-policy.md`.

### 3.3 Node codec asymmetry
Hand codecs encode/decode field order match + round-trip tests.

### 3.4 Encoded-byte equality
Typed wrappers compare **decoded** values (NaN/non-canonical). Not raw bytes.

### 3.5 Borrowed key forms
The owning type implements `KeyLookup<Q>` or `OrderedKeyLookup<Q>`. Postcard arrays omit the length prefix used by byte vectors. A wrong-length slice misses a fixed-array exact lookup. Verify the same rule on point reads and writes.

### 3.6 Handle type tags

`VSTYPE04` tags hash the full Rust type name. Applications own their value schemas. Old-format handling belongs in the offline migration library.
