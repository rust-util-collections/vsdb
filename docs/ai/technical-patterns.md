# VSDB Technical Bug Patterns

Cross-cutting catalog for VSDB + mmdb integration: every collection sits on these
engine, Rust `unsafe`, and encoding contracts. Load before review/debug.

Use this catalog to check supported local database operations and their
documented results. Each pattern needs a concrete call sequence and observable
failure under `review-core.md`; a matching term alone is not a finding.

Use the [subsystem map](review-core.md#subsystem-map) to find the guide for each affected path.

## 1. Prefix / engine

### 1.1 Prefix collision
Only forward floor/ceiling/cursors; durable ceiling before issue; recovered prefixes reserved; never reused.

### 1.2 Shard routing
Read/write/delete/iter use owning engine’s `dbs.len()`, never hardcoded default.

### 1.3 Namespace double-open
Default one-time init; non-default re-check `OPEN_NAMESPACES` under `REGISTRY_LOCK`.

### 1.4 Cross-prefix atomicity
Ordinary raw batches bind one map prefix. SlotDex, VecDex and VerMap publish on one prefix. DagMap inserts the parent edge, edge allocator and child metadata together through a checked two-prefix operation on one shard. Pruning retains its recoverable phases.

## 2. Rust `unsafe` and aliasing contracts

### 2.1 shadow() race
Same-key concurrent write or overlapping structural multi-key ops violate alias contract. Disjoint plain-map keys OK.

### 2.2 Entry and ValueGuard ownership

Occupied and vacant `enum Entry` variants retain exclusive `&mut` map access without raw pointers. `ValueGuard` (`guard.rs`) writes back only when `DerefMut` was invoked (`dirty == true`) or the entry was vacant (`vacant == true`), checking un-persisted interior mutability via `debug_assert_eq!` in debug builds. `UnwindMark::interrupted()` (`!panicking_at_entry && thread::panicking()`) discards edits interrupted by a mid-guard unwind while preserving guards created inside `Drop`.

## 3. Encoding

### 3.1 Non-canonical keys
Postcard key bytes must be canonical (`Eq + Hash` blanket bound rejects floats and `HashMap`/`HashSet`). Ordered maps need `KeyEnDeOrdered` (explicit integer widths; `usize`/`isize` excluded), never postcard order.

### 3.2 Version incompatibility and postcard traps
Check the workspace manifest for the postcard dependency and use frozen fixtures to exercise explicit format tags. `#[serde(default)]` alone ≠ safe postcard layout change. See `compatibility-policy.md`.
- **EOF / trailing bytes:** `postcard::from_bytes` ignores trailing bytes; use `postcard::take_from_bytes` and reject `!rest.is_empty()` on metadata, frames, canonical keys, and values so truncated/evolved payloads never silently misread or drop fields on write-back.
- **Stack-slice `collect_str` overflow:** `postcard::to_slice` into a fixed stack buffer returns `Err(postcard::Error::CollectStrError)` (not `SerializeBufferFull`) when `Serializer::collect_str` overflows the slice; stack-buffer encoders (`postcard_key`) must fall back to `postcard::to_allocvec` on `CollectStrError` as well.

### 3.3 Node codec asymmetry
Hand codecs encode/decode field order match + round-trip tests.

### 3.4 Encoded-byte equality
Typed wrappers compare **decoded** values (NaN/non-canonical). Not raw bytes.

### 3.5 Borrowed key forms
The owning type implements `KeyLookup<Q>` or `OrderedKeyLookup<Q>`. Postcard arrays omit the length prefix used by byte vectors. A wrong-length slice misses a fixed-array exact lookup. Verify the same rule on point reads and writes.

### 3.6 Handle type tags

`VSTYPE04` tags hash the full Rust type name. Applications own their value schemas. Old-format handling belongs in the offline migration library.
