# VSDB v18 Structural Refactoring Roadmap

Status: proposed major-release design (`v18.0.0`), 2026-11-24. Not yet implemented; current `v17` contracts remain active until v18 development begins.
Baseline: `1a09c25` (`v17.0.21`), reviewed against `mmdb 4.3.14` (`>= 4.3.12` in `Cargo.toml`).

## Purpose and release scope

`v16` and `v17` introduced namespaces, in-process engine teardown, per-engine block-cache pools, single-handle crash-atomic `SlotDex` and `VecDex` layouts, `PersistentBTree` versioning, synchronous batches, and raw read views — while preserving backward compatibility with `v15`/`v16` shims, multi-handle composite payloads, and unconstrained trait bounds.

`v18.0.0` uses a clean major-version boundary (`format_version = 18` with universe/root identity headers, handle envelopes `VSMAPX02` / `VSTYPE04`, namespace registry `VSNREG02`) to pay down that accumulated structural debt across both crates (`vsdb_core` and `vsdb`):

- eliminate the open `[CRITICAL]` `SlotDex` custom-slot encoding defect and close the major structural `Won't Fix` debts in [audit.md](../audit.md);
- collapse multi-prefix composite collections (`VerMap`, `DagMap` nodes) onto single-prefix or co-located single-shard protocols, deleting `gc_dirty` repair scans and cross-shard WAL sync storms;
- eliminate avoidable `unsafe` blocks across `Entry` APIs, `PersistentBTree`, and `DagMap`, and prune legacy `pub unsafe` constructors;
- remove avoidable VSDB temporary heap allocations across key encoding, engine range scans, B+ tree sibling decoding, mutable value guards, Merkle tries, and HNSW search.

### Consolidated feature matrix

| # | Subsystem | Planned v18 refactoring | Audit / legacy debt closed | Primary win | Break scope |
|---|-----------|-------------------------|----------------------------|-------------|-------------|
| 1 | `SlotDex` | Sealed `[u8; N]` `SlotType::Bytes`, `tier_capacity: u32`, bounded physical-reverse `iter`, `[total, generation]` check | Closes `[CRITICAL] slotdex` ([audit.md](../audit.md)) | Variable-width bug unrepresentable at compile time; 0 temp slot allocs; bounded-memory reverse iter | Public API + handle meta |
| 2 | `ende` + `Mapx*` | 2-trait `KeyEnDe`/`ValueEnDe` (`Eq + Hash` key blanket), stack `with_encoded_key`/`with_ordered_bytes`, `(K, Q)` `KeyLookup`, stack `with_full_key` | Removes `Mapx::query_bytes` 4-alloc shim, `usize`/`isize` non-portability, `f32`/`f64`/`HashMap` key footguns | 0 temporary VSDB key allocs on short keys (`<= 120B`); compile-time rejection of float/`HashMap` keys | Public API + `usize` keys |
| 3 | `Engine` (`mmdb`) | Unify `range_at` onto `db.iter_with_prefix`, unbox `MmdbIter`, in-place prefix strip, `RawRangeBounds` / `OrderedRangeBounds` | Fixes bypassed SST prefix Bloom filters + double seek | Restores prefix Bloom pruning on all ranges/`get_le`/`get_ge`; 0 `Box<dyn>` / key reallocs; `map.range(b"a"..b"z")` | Public API |
| 4 | Basic collections | Safe `enum Entry`, `DerefMut`-only `ValueMut` + fallible `update`, typed `Reader` + `ReadView`, dead `pub unsafe` removal | Completes [durable-batches-and-read-views.md](durable-batches-and-read-views.md) §2; removes all `Entry` + `PersistentBTree` `unsafe` | 0 upfront clones/encodes on `get_mut`/`iter_mut`; `#![forbid(unsafe_code)]` on B+ tree & versioning | Public API |
| 5 | `core` / envelopes / errors | Universe/root `format_version = 18` header + `LOCK`, flat `VSMAPX02` (24B) & `VSTYPE04` (32B), fail-closed `VSNREG02`, lazy `DEFAULT_NS`, drop `ruc` | Removes `VSTYPE02`, v15 allocator fold, `__namespace_state__` sidecars, public `RucError` SemVer leak | Eliminates intermediate handle allocs; halves `Namespace::create` fsyncs; prevents wrong-root `relocate`; structured `VsdbError` | Public API + On-disk |
| 6 | `VerMap` + `PersistentBTree` | Single-handle tagged store, 2-bit tagged IDs + sparse `extra_refs` + `[0x05]` snapshot-pin tombstones, slotted `NodeView`, `'static` `Snapshot`, `write_batch` | Removes 8-prefix `VerMapPayload`, `gc_dirty`, startup full-DB scans, >150 MiB `RefState` RAM | **$O(1)$ normal `from_meta()` open**; ~128 → **0 sibling `Vec` allocs per `get()`**; crash-safe blob & snapshot reclamation | Public API + On-disk |
| 7 | `MptCalc` + `SmtCalc` | `Arc`-COW nodes + inline `OnceLock<[u8; 32]>`, POPCNT branches, inline-64 + heap `Nibbles`, map-bound streaming checkpoints, unified `TrieCalc` | Eliminates `VerMapWithProof::poison()`; closes `[LOW] trie` cache amplification ([audit.md](../audit.md)) | $O(1)$ `Clone` & panic-safe batch rollback; 3–5× lower MPT RAM; crash-safe bounded-RAM checkpoints (-40–60% size) | Public API + cache files |
| 8 | `DagMap` | 1 co-located `MapxRaw` per node, `dag_id`-authenticated `NodeMeta` + local `edge_id`s, ordered crash transitions, typed `DagMap<K, V>` | Closes `[MEDIUM] dagmap` serde decomposition ([audit.md](../audit.md)); deletes all **7 `unsafe` blocks** + `DagIdAllocator` | **0 cross-shard WAL syncs** on `prune()`/`destroy()`; preserves `child_id` across `Clone`; supports empty `[]`/`()` values | Public API + On-disk |
| 9 | `VecDex` | Stateless handle over `[TAG_META]` + 34B `[TAG_STATE]` + snapshot-owning reads/iters, `&self` `DistanceMetric` (merging `VecDexDyn`), LE float slices | Closes `[MEDIUM] cached indexes` ([audit.md](../audit.md)) for `VecDex`; deletes 392-line `dynamic.rs` | Coherent restored handles (including `get`/`iter` across `clear()`); 0 per-node `Rc<Vec<S>>` search allocs; drops `K: ValueEnDe` | Public API + On-disk |

---

## 1. `SlotDex`: compile-time fixed-width `SlotType` (`[u8; N]`), `tier_capacity: u32`, and bounded reverse scans

Source: [slot_type.rs](../../strata/src/slotdex/slot_type.rs), [slotdex/mod.rs](../../strata/src/slotdex/mod.rs), [audit.md](../audit.md).

### Current defects and structural friction

1. **Variable-width custom slot corruption (`[CRITICAL]` in `audit.md`):**
   [`SlotType`](../../strata/src/slotdex/slot_type.rs#L13-L51) requires [`KeyEnDeOrdered`](../../strata/src/common/ende.rs#L321-L337) (`to_bytes(&self) -> Vec<u8>`), which allows variable-width encodings. [`entry_key`](../../strata/src/slotdex/mod.rs#L155-L163) concatenates `[0x00 | slot.to_bytes() | k.to_bytes()]` without framing, while [`decode_entry_row`](../../strata/src/slotdex/mod.rs#L248-L255) and [`iter`](../../strata/src/slotdex/mod.rs#L1430-L1465) slice at `S::MIN.to_bytes().len()`, and [`entry_lower_bound`](../../strata/src/slotdex/mod.rs#L203-L226) / [`entry_upper_bound`](../../strata/src/slotdex/mod.rs#L229-L246) use fixed-width `prefix_successor`. Variable-width `SlotType` implementations silently drop colliding `(slot, key)` pairs, misread or panic on decoded keys, and bleed across prefix-related slot bounds.
2. **Why variable-width byte-stuffing is the wrong v18 design:**
   Order-preserving escaping (`0x00 -> 0x00 0xFF` + `0x00 0x00` terminator) inflates big-endian integer slots by `2 + count(0x00)` bytes (+24–59% larger entry keys on `SlotDex64<u64>`, up to +18B on `u128` across WAL, memtable, and uncompressed `BlockCachePool` pages) and forces per-row delimiter scans. Meanwhile, genuinely variable-length types (`String`, `Vec<u8>`) cannot satisfy `SlotType`'s uniform `cap^level` bucket division (`floor_align`) or bitwise complement (`!slot`) anyway — composite string indexing belongs in `MapxOrd`.
3. **Conflated `tier_capacity: S` and dead trait methods:**
   `tier_capacity` ([slotdex/mod.rs](../../strata/src/slotdex/mod.rs#L108)) is a dimensionless tree fan-out (`>= 2`), not a slot coordinate. Typing it as `S` forced `checked_pow` and `as_i128` onto `SlotType` (all 4 `as_i128` callers in `mod.rs` inspect `tier_capacity`, never a slot), while `max_val`, `saturating_add`, and `as_u64` ([slot_type.rs](../../strata/src/slotdex/slot_type.rs#L35-L45)) have **zero callers** in the workspace. Missing `Copy` also forces 25+ `.clone()` calls on slot scalars.
4. **Hot-path `Vec<u8>` churn:**
   Because `KeyEnDeOrdered::to_bytes` allocates a `Vec<u8>`, a single 3-tier `insert` allocates 6 temporary slot `Vec<u8>`s; [`slot_entry_cnt`](../../strata/src/slotdex/mod.rs#L890-L895) allocates 2 `Vec<u8>`s just to read an 8-byte counter; and [`decode_entry_row`](../../strata/src/slotdex/mod.rs#L248-L255) allocates **3 `Vec<u8>`s per paged entry** (`S::MIN.to_bytes()`, `slot` decoded and discarded via `.1`, and `key` `.to_vec()` instead of `K::from_slice`).
5. **100× physical-reverse iterator cliff in `iter` and unbounded bottom-chunk buffering:**
   [`level0_walk_desc`](../../strata/src/slotdex/mod.rs#L897-L900) documents that `mmdb` backward resolution costs ~100× more than forward streaming, yet [`SlotDex::iter`](../../strata/src/slotdex/mod.rs#L1446-L1459) calls `slot_rows.next_back()` on every slot and opens a separate `entry_range(slot..=slot)` snapshot per slot whenever `(order == Order::Desc) ^ swap_order` is true. In addition, [`level0_walk_desc`](../../strata/src/slotdex/mod.rs#L967-L975) buffers an entire level-1 bucket's L0 rows in memory — which for a large `tier_capacity` (e.g. `1_000_000`) with no level 1 yet can buffer hundreds of thousands of count rows on `.take(1)`. Meanwhile [`entry_cnt_within_two_slots`](../../strata/src/slotdex/mod.rs#L1338-L1354) walks from `S::MIN` twice using signed `i128`.

### v18 design

1. **Compile-time fixed-width `SlotType` over sealed `[u8; N]`:**

   ```rust
   mod sealed {
       pub trait SlotBytes: Copy + Eq + Ord + AsRef<[u8]> + AsMut<[u8]> + 'static {
           const LEN: usize;
           fn zeroed() -> Self;
       }
       impl<const N: usize> SlotBytes for [u8; N] {
           const LEN: usize = N;
           #[inline(always)]
           fn zeroed() -> Self { [0u8; N] }
       }
   }

   pub trait SlotType:
       Copy + Ord + fmt::Debug + Not<Output = Self>
       + Serialize + de::DeserializeOwned + Send + Sync + 'static
   {
       /// Fixed-width big-endian (strictly order-preserving, injective) bytes.
       type Bytes: sealed::SlotBytes;

       const MIN: Self;
       const MAX: Self;

       fn to_bytes(self) -> Self::Bytes;
       fn from_bytes(bytes: Self::Bytes) -> Result<Self>;

       /// Bucket width `tier_capacity^level`, or `None` when a single
       /// whole-domain bucket anchored at `Self::MIN` covers all slots.
       fn tier_stride(tier_capacity: u32, level: u8) -> Option<Self>;
       /// Floor-align `self` in unsigned rank space anchored at `Self::MIN`:
       /// `MIN + ((self - MIN) / stride) * stride`.
       fn floor_align(self, stride: Self) -> Self;
       fn checked_succ(self) -> Option<Self>;
       fn checked_pred(self) -> Option<Self>;
   }
   ```

   - **Required trait laws** (tested on both `swap_order` modes and endpoints `MIN`/`MAX`): `to_bytes` is injective and strictly order-preserving (`a < b ⇔ a.to_bytes() < b.to_bytes()`); `!slot` is an order-reversing involution (`a < b ⇔ !b < !a`, `!MIN == MAX`); and `floor_align` aligns in unsigned offset space anchored at `Self::MIN` (never signed `div_euclid` around `0`, which would underflow `i64::MIN` for stride `3` — instead `rank = (self as u64) ^ (1 << 63)`, `aligned = (rank / stride) * stride`, returning `(aligned ^ (1 << 63)) as i64`). When `tier_stride` returns `None` (stride overflow), every slot aligns to `Self::MIN` (one top-level bucket, terminating tier growth cleanly).
   - Variable-width `SlotType` becomes **impossible to express at compile time** (`to_bytes` returns `[u8; N]`).
   - Change `tier_capacity` in `SlotDex::new` / `new_in` / persisted handle metadata (`LAYOUT_VERSION = 3`) to `u32` (`>= 2`). Delete `max_val`, `saturating_add`, `as_u64`, `as_i128`, and `checked_pow`.
2. **Single storage path and zero-allocation slot/key helpers:**
   - Entry rows stay `[0x00 | slot: [u8; N] | key_be]` with zero escaping and zero runtime width checks.
   - Replace `decode_entry_row` across `page` and `iter` with `K::from_slice(&raw_key[1 + S::Bytes::LEN..])`; encode `level_prefix` as `[u8; 2]` on the stack; look up `slot_entry_cnt` via a `[u8; 34]` stack slice when `S::Bytes::LEN <= 32`; and decode `decode_level_row` into `S::Bytes::zeroed()` on the stack.
3. **Bounded-memory physical-reverse `iter` and common-ancestor `count`:**
   - Select the forward vs descending engine path by **physical** storage order (`(order == Order::Desc) ^ self.swap_order`).
   - When physical descending order is needed, cap in-memory L0 count-row buffering (falling back to bounded L0 sub-ranges when a level-1 bucket exceeds 256 populated slots or no level 1 exists yet), and coalesce contiguous slots into a single forward `entry_range` scan **only while both `sum(cnt) <= 256` and buffered raw key bytes `<= 16 KiB`**, streaming larger slots individually so `.take(k)` has a hard memory bound regardless of `tier_capacity` or key size.
   - Compute `count(slots)` from the lowest unshared tier bucket between `lo` and `hi` using unsigned `u64` arithmetic, and inline the 4 legacy `pub(crate)` `get_entries_by_page*` / `total_by_slot` / `entry_cnt_within_two_slots` helpers.
4. **Generation-checked sequential alias detection + snapshot-consistent reads:**
   - Store `[total: u64 LE, generation: u64 LE]` (16 bytes) at `[TAG_TOTAL]`, incremented (`checked_add`, erroring on `u64::MAX`) on every committed mutation batch — **including `clear()`**, which stages `put(TOTAL_KEY, [0, gen + 1])` inside its `batch_wiped()` commit rather than deleting `TOTAL_KEY`.
   - Keep single-active-writer (`&mut SlotDex`) ownership (no hidden global mutable cache, per [performance-roadmap.md](performance-roadmap.md) §四.3): `hydrate`, `page`, `count`, and `iter` each capture one `MapxRawReadView` (for `iter`, stored by value inside `SlotDexIter<'a, S, K>` alongside `MmdbIter` so the `mmdb::Snapshot<'a>` guard stays alive without self-referential borrows). Before staging a write or serving a cached read, verify `[TAG_TOTAL]`'s `generation == self.generation`: a stale sequential writer returns `VsdbError` rather than corrupting counts, while a stale reader re-hydrates from its captured `MapxRawReadView`. Add deep-copy `Clone` (resetting `generation = 0` on the fresh copy) and `clone_in(&ns)`.

---

## 2. Codec and key pipeline: stack key encoding and `(K, Q)` lookup traits

Source: [ende.rs](../../strata/src/common/ende.rs), [mapx/mod.rs](../../strata/src/basic/mapx/mod.rs#L184-L215), [mapx_ord/mod.rs](../../strata/src/basic/mapx_ord/mod.rs#L81-L135), [mmdb.rs](../../core/src/common/engine/mmdb.rs#L1612-L1617).

### Current defects and costs

1. **2–4 temporary VSDB key allocations per point read or write:**
   `KeyEnDeOrdered::to_bytes` ([ende.rs](../../strata/src/common/ende.rs#L323)), `OrderedKeyRef::ordered_key_bytes` ([ende.rs](../../strata/src/common/ende.rs#L285)), and `KeyEn::encode_key` ([ende.rs](../../strata/src/common/ende.rs#L85)) always allocate a `Vec<u8>` — even for `&str`/`&[u8]` (already contiguous ordered bytes) and scalar integers (`u64::to_be_bytes().to_vec()`). `MmDB` then calls `make_full_key(&meta_prefix, key)` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L1612)), allocating another `Vec<u8>` (`8 + key.len()`) just to pass `&[u8]` to `mmdb`.
2. **`Mapx::query_bytes` 4-allocation workaround and ambiguous `MapxOrd` slice probes:**
   Because `KeyRef` / `OrderedKeyRef` ([ende.rs](../../strata/src/common/ende.rs#L240-L307)) are traits on `Q` alone without knowing `K`, `[u8]` cannot tell `Vec<u8>` (varint-prefixed in postcard) apart from `[u8; N]` (unprefixed tuple in postcard). [`Mapx::query_bytes`](../../strata/src/basic/mapx/mod.rs#L194-L215) works around this on every `&[u8]` lookup by running up to **4 heap allocations + 2 postcard decodes + 2 postcard encodes**.
3. **Float/`HashMap` key footguns, non-portable `usize`/`isize` families, and 6-trait collisions:**
   - `impl<T: Serialize> KeyEn for T` ([ende.rs](../../strata/src/common/ende.rs#L176-L180)) lets `Mapx<f64, V>` and `Mapx<HashMap<A, B>, V>` compile cleanly and silently lose entries across restarts (`SipHash` randomization) or `-0.0`/`NaN`.
   - `usize`/`isize` `KeyEnDeOrdered` ([ende.rs](../../strata/src/common/ende.rs#L531-L575)) — across scalars, `Vec<usize>`, `Box<[usize]>`, and `[usize; N]` — writes platform-dependent 4B vs 8B keys; `Box<[$int]>::to_bytes` ([ende.rs](../../strata/src/common/ende.rs#L462)) double-allocates via `self.to_vec()`; `impl_array!(1..=128)` ([ende.rs](../../strata/src/common/ende.rs#L541-L575)) expands 1,536 impl blocks; and splitting `KeyEn`/`KeyDe`/`KeyEnDe` + `ValueEn`/`ValueDe`/`ValueEnDe` causes `encode`/`decode` method name collisions (`E0034`).

### v18 design

1. **Two traits (`KeyEnDe`, `ValueEnDe`) with `Eq + Hash` key bounds and stack/buffer paths:**
   - Collapse the 6 traits into `pub trait KeyEnDe: Eq + Sized` (`encode_key_into`, `with_encoded_key`, `encode_key`, `decode_key`) and `pub trait ValueEnDe: Sized` (`encode_value_into`, `encode_value`, `decode_value`).
   - Tighten the blanket impl to `impl<T: Serialize + DeserializeOwned + Eq + Hash> KeyEnDe for T`. Because `f32`, `f64`, `HashMap`, and `HashSet` do not implement `Eq + Hash`, standard float/hash-collection keys fail at compile time while ergonomic derived key structs keep working out of the box. Document the remaining canonical-byte law (`a == b ⇔ encode_key(a) == encode_key(b)`) for types with custom `PartialEq` or `Serialize` logic.
   - Implement `with_encoded_key` using `postcard::to_slice` into a `[u8; 128]` stack buffer (including varint-prefixed borrowed `str`/`[u8]` lookups up to ~120B), falling back to `postcard::to_allocvec` only on `SerializeBufferFull`.
   - On `KeyEnDeOrdered`, require `encode_ordered_into(&self, dst: &mut Vec<u8>)` and `with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R` (passing `&[u8]` zero-copy for `String`/`Vec<u8>`/`Box<[u8]>` and stack `[u8; size_of::<T>()]` for scalars). Remove `usize` and `isize` across scalars, `Vec`, `Box<[_]>`, and `[_; N]`, fix `Box<[$int]>`, and replace `impl_array!(1..=128)` with const-generic `impl<const N: usize> KeyEnDeOrdered for [$int; N]` blocks.
2. **Separate exact `KeyLookup<Q>` and ordered `OrderedKeyLookup<Q>`:**
   - Replace `(K: Borrow<Q>, Q: KeyRef / OrderedKeyRef)` with codec-paired `KeyLookup<Q: ?Sized>: KeyEnDe` (for exact `get`/`get_mut`/`contains_key`/`remove`) and `OrderedKeyLookup<Q: ?Sized>: KeyEnDeOrdered` (for `get_le`/`get_ge` and range endpoints).
   - Provide blanket `K: KeyLookup<K>` and `K: OrderedKeyLookup<K>`, plus `(String, str)`, `(Box<str>, str)`, `(Vec<u8>, [u8])`, and `(Box<[u8]>, [u8])`.
   - Keep exact vs ordered semantics distinct on `[u8; N]`: for unordered `Mapx<[u8; N], V>` (`where [u8; N]: KeyEnDe`, i.e. serde-supported `N <= 32`) and exact `MapxOrd<[u8; N], V>::get(&[u8])`, a slice with `q.len() != N` returns `None` in $O(1)$ without decoding; for ordered boundary queries (`get_le`, `get_ge`, `range`) on `MapxOrd<[u8; N], V>`, require fixed-width `&[u8; N]` rather than unsized `&[u8]` so short prefix probes cannot be confused with full keys, or pass raw prefix bytes intentionally on `MapxOrdRawKey`. Delete `KeyRef::as_byte_slice` and `Mapx::query_bytes`.
3. **Stack `with_full_key` in `mmdb.rs`:**
   - Replace transient `make_full_key` calls in `MmDB::{get, insert, remove, lazy_delete}`, `MmdbReadView::get`, and `MmdbBatch::{insert, remove}` with a `[u8; 128]` stack buffer (`8 + key.len() <= 128`, heap fallback for longer keys). This eliminates **all temporary VSDB key-encoding and prefix-concatenation heap allocations** for keys `<= 120` bytes (noting that `mmdb::DB::get` still allocates the returned value `Vec<u8>` on hits — including inside `MapxRaw::contains_key` unless `mmdb` exposes a value-free existence check — and `mmdb::DB::delete` copies the key into its internal `WriteBatch`).

---

## 3. Engine scan path: prefix-Bloom range unification, unboxed `MmdbIter`, and ergonomic bounds

Source: [mmdb.rs](../../core/src/common/engine/mmdb.rs#L452-L681), [engine/mod.rs](../../core/src/common/engine/mod.rs#L82-L96), [staged.rs](../../strata/src/common/staged.rs#L72-L185).

### Current defects and costs

1. **SST prefix Bloom filter bypassed on every `range`, `get_le`, `get_ge`, and `MapxReadView::iter`:**
   [`mmdb_open`](../../core/src/common/engine/mmdb.rs#L1821) configures an 8-byte prefix extractor (`prefix_len: PREFIX_SIZE`), which `mmdb::DB::iter_with_prefix` uses to prune L0 SSTs via prefix Bloom filters and pass prefix/range hints to higher levels. Yet [`MmDB::range_at`](../../core/src/common/engine/mmdb.rs#L563-L569) calls `db.iter_with_range(...)` — which skips prefix Bloom filters — and then calls `db_iter.seek(lo)` a **second** time after `iter_with_range` already sought all sources. Even [`MapxReadView::iter`](../../core/src/common/engine/mod.rs#L83) delegates to `self.range(..)`, losing Bloom pruning on shared shards.
2. **Heap-boxed `dyn DoubleEndedIterator` and per-row `Vec<u8>` reallocation:**
   Every `iter`, `range`, `get_le`, and `get_ge` boxes `MmdbIter(Box::new(iter))` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L466)), dispatches every item through a vtable, re-runs `.filter(...)` on top of `DBIterator` bounds, and allocates a fresh `Vec<u8>` via `k[PREFIX_SIZE..].to_vec()` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L577)) even though `k` is already an owned `Vec<u8>`.
3. **`RangeBounds<Cow<'a, [u8]>>` friction and wasted overlay lookahead clones:**
   Callers of `MapxRaw::range` and `MapxOrdRawKey::range` cannot write `map.range(b"a"..b"z")` without `Cow` wrappers; `MapxOrd<String, V>::range` ([mapx_ord/mod.rs](../../strata/src/basic/mapx_ord/mod.rs#L209)) rejects `"a".."z"`; and [`StagedRows::scan_prefix`](../../strata/src/common/staged.rs#L95-L102) maps `(k.clone(), v.clone())` over `self.rows.range(..)` before `MergeScan` filters out staged `None` tombstones.

### v18 design

1. **Unify all prefix scans onto `db.iter_with_prefix(&meta_prefix, &opts)`:**
   Set `ReadOptions::{iterate_lower_bound, iterate_upper_bound}` with `Bound::Excluded(lo)` converted to inclusive `lo || [0x00]` (mirroring `Bound::Included(hi)` → `hi || [0x00]` at [mmdb.rs](../../core/src/common/engine/mmdb.rs#L547-L551)). Verified against `mmdb 4.3.14`, `iter_with_prefix` intersects caller bounds with `[meta_prefix, prefix_end)`, applies **both** range and **prefix Bloom** SST pruning, installs bounds on `DBIterator`, and seeks once. Preserve captured snapshot `ReadOptions` and [`CheckedBidiIter`](../../core/src/common/engine/mmdb.rs#L631-L657)'s error check after **every** `next()` and `next_back()`, including mixed-direction iteration across wiped prefixes.
2. **Concrete `MmdbIter` and in-place prefix stripping:**
   Replace `Box<dyn DoubleEndedIterator<...>>` with a concrete `pub struct MmdbIter { inner: CheckedBidiIter }` and strip the 8-byte prefix in place via `k.drain(..PREFIX_SIZE)` (avoiding a second `Vec<u8>` heap allocation per yielded forward/backward item).
3. **Inference-safe `RawRangeBounds`, `OrderedRangeBounds<K>`, and zero-tombstone-clone `MergeScan`:**
   Define single-parameter traits `RawRangeBounds` and `OrderedRangeBounds<K>` with dedicated `RangeFull` (`..`) impls that carry **no unbound query type parameter `Q`** (so `ord_map.range(..)` never hits Rust type-inference ambiguity when `K` has multiple `OrderedKeyLookup` targets), alongside impls for `Range<&Q>`, `RangeInclusive<&Q>`, `RangeFrom<&Q>`, `RangeTo<&Q>`, `RangeToInclusive<&Q>`, owned `K` ranges, and `(Bound<&Q>, Bound<&Q>)`. In `StagedRows::MergeScan`, borrow `btree_map::Range<'a, Vec<u8>, Option<Vec<u8>>>` directly and clone `(k, v)` only when emitting a live `Some(v)` overlay entry.

---

## 4. Basic collections: safe `Entry` enum, `DerefMut`-only `ValueMut`, typed `Reader`/`ReadView`, and `unsafe` cleanup

Source: [macros.rs](../../strata/src/common/macros.rs#L57-L183), [mapx_ord_rawkey/mod.rs](../../strata/src/basic/mapx_ord_rawkey/mod.rs#L92-L124), [orphan/mod.rs](../../strata/src/basic/orphan/mod.rs#L91-L208), [mapx_raw/mod.rs](../../core/src/basic/mapx_raw/mod.rs#L74-L185).

### Current defects and costs

1. **2 upfront clones/encodes + up to 2 `Drop` re-encodes on every `get_mut` / `iter_mut` / `Orphan +=`:**
   [`MapxOrdRawKey::get_mut`](../../strata/src/basic/mapx_ord_rawkey/mod.rs#L92-L106) and `ValueIterMut` eagerly serialize `snapshot = value.encode()` **and** clone `original = inner.as_slice().to_vec()` (duplicating bytes already held inside `self.inner`), then re-encode up to twice on `Drop` when `!self.dirty` — solely to auto-persist `RefCell<T>` interior mutability through shared `&V` `Deref` ([mapx/test.rs](../../strata/src/basic/mapx/test.rs#L333-L343)). Meanwhile `ValueMut`, `ValueIterMut`, `mapx::ValueIterMut`, `orphan::ValueMut`, and `dagmap::rawkey::ValueMut` duplicate the guard boilerplate.
2. **Raw-pointer `unsafe` in `Entry` APIs + dead `pub unsafe` surface:**
   `entry_or_insert_via_mock!` ([macros.rs](../../strata/src/common/macros.rs#L282-L298), which only exposes `or_insert(V)`) and `MapxRaw::Entry` ([mapx_raw/mod.rs](../../core/src/basic/mapx_raw/mod.rs#L846-L880), which exposes `or_insert` and `or_insert_with`) cast `&mut Hdr` to `*mut Hdr` to work around borrow lifetimes. `MapxOrd::insert_encoded_value` ([mapx_ord/mod.rs](../../strata/src/basic/mapx_ord/mod.rs#L156)) and `MapxOrdRawKey::insert_encoded_value` have **zero external callers**; `MapxRaw::from_bytes_in` and `engine::Mapx::from_prefix_slice_in` have only **one legacy caller** (`DagIdAllocator::read_legacy_counter`); `define_map_wrapper!` ([macros.rs](../../strata/src/common/macros.rs#L176-L183)) decodes `self.as_bytes()` manually instead of calling `self.inner.instance_id()`; and `Orphan::{is_uninitialized, initialize_if_empty}` are dead relics.
3. **Missing typed `Reader` / `ReadView`:**
   While `&Mapx<K, V>` already permits shared reads when no writer holds `&mut Mapx<K, V>`, retaining an independent read handle alongside an active single writer currently forces typed-map callers into `unsafe { map.shadow() }`.

### v18 design

1. **Explicit `DerefMut` write-back on `ValueMut` / `ValueIterMut` + fallible `update`:**
   - **Intentional v18 contract change:** `ValueMut` and `ValueIterMut` persist changes strictly when `DerefMut` (`&mut V`) was called (`dirty == true`) or when initialized by a vacant `Entry` (`is_mock == true`, so `map.entry(&k).or_insert(v);` inserts even if the returned guard is dropped without dereferencing). Interior mutability through shared `&V` (`RefCell`) is no longer auto-persisted.
   - Preserve [`std::thread::panicking()`](../../strata/src/basic/mapx_ord_rawkey/mod.rs#L415) unwind suppression so unwinding never writes half-mutated values. When `!std::thread::panicking() && (self.is_mock || self.dirty)`, encode `self.value` **once** on `Drop` and write back iff `self.is_mock || encoded.as_slice() != self.inner.as_slice()`. Unmodified guards (`!dirty && !is_mock`) perform **0 upfront encodes, 0 `original` clones, and 0 `Drop` encodes** (also preventing accidental read-only panics on untouched guards).
   - Add an explicit `ValueMut::commit(self) -> Result<()>` (and `map.update(&k, |v: &mut V| ...)` closure helper) for callers that want deterministic write-back and fallible error propagation instead of `Drop`. Unify `ValueMut<'a, V>` and `ValueIterMut<'a, V>` over one internal `RawValueSlot` trait and delete `Orphan::{is_uninitialized, initialize_if_empty}`.
2. **100% safe `enum Entry<'a, V> { Occupied, Vacant }` and `pub unsafe` pruning:**
   Read the raw value inside `entry(key)` before constructing `OccupiedEntry` or `VacantEntry` holding `&'a mut Self` — deleting all `Entry` raw-pointer `unsafe` while providing `or_insert`, `or_insert_with`, `or_default`, `and_modify`, `OccupiedEntry::{get, get_mut, insert, remove}`, and `VacantEntry::insert`. Delete `insert_encoded_value`, delete `from_bytes_in` / `from_prefix_slice_in` alongside `DagIdAllocator`, delegate `define_map_wrapper!::instance_id` to `self.inner.instance_id()` and demote `MapxRaw::as_bytes` and `Orphan::shadow` to crate-private.
3. **First-class `Reader` + `ReadView` across all basic collections:**
   Complete `MapxRawReader` (`contains_key`, `iter`, `range`, `get_le`, `get_ge`, `namespace`, `instance_id`) and ship `MapxReader<K, V>` / `MapxReadView<'a, K, V>`, `MapxOrdReader<K, V>` / `MapxOrdReadView<'a, K, V>`, `MapxOrdRawKeyReader<V>` / `MapxOrdRawKeyReadView<'a, V>`, and `OrphanReader<T>` (resolving [durable-batches-and-read-views.md](durable-batches-and-read-views.md) §2). Every `*Reader` clones `Namespace` (`Arc<NsInner>`), keeping the namespace open while alive.

---

## 5. Universe format header, flat handle frames, fail-closed `VSNREG02`, and `ruc`-free `VsdbError`

Source: [strata/src/common/mod.rs](../../strata/src/common/mod.rs), [core/src/common/mod.rs](../../core/src/common/mod.rs), [namespace.rs](../../core/src/common/namespace.rs), [error.rs](../../core/src/common/error.rs), [mmdb.rs](../../core/src/common/engine/mmdb.rs#L871-L1310).

### Current defects and recovery subtleties

1. **Format check accepts missing markers and older versions, and roots carry no universe/namespace identity:**
   [`check_format_version`](../../core/src/common/engine/mmdb.rs#L1700-L1725) treats `NotFound` as pre-v16 OK and only rejects `ver > SUPPORTED_FORMAT_VERSION` — simply bumping `SUPPORTED_FORMAT_VERSION` to `18` would still silently open `v16`/`v17` datasets! In addition, writable single-process exclusion relies only on in-process mutexes ([namespaces.md](namespaces.md#L27-L30)), and [`Namespace::relocate`](../../docs/proposals/namespaces.md#L219-L222) cannot detect an operator pointing namespace `A` at namespace `B`'s directory because engine roots carry no `universe_id` or `ns_id`.
2. **Triple-nested handle envelopes + `note_meta_bytes` prefix-recovery coupling:**
   Serializing a `Mapx<K, V>` wraps `VSMAPX01` → `MapxOrdRawKey` `VSTYPE03` → `Mapx` `VSTYPE03` (intermediate `Vec<u8>` allocations and 3 `postcard` passes on nested-map reads; note outer `save_instance_meta` / `load_instance_meta` still perform file buffer I/O), plus `VSTYPE02` / `legacy_type_name` shims ([strata/src/common/mod.rs](../../strata/src/common/mod.rs#L106-L152)). Crucially, missing-ceiling prefix recovery ([`note_meta_bytes`](../../core/src/common/engine/mmdb.rs#L1165-L1219)) scans shard values, `__instance_meta__`, and `__CUSTOM__` for raw handle magic so even **empty** saved maps do not have their prefix reissued — any new flat typed frame must be recognized by `note_meta_bytes` including across 64 KiB stream chunk boundaries.
3. **`__namespace_state__` sidecars (`~8 fsync`s per `Namespace::create`) and missing-registry reuse protection:**
   [`Namespace::create_with`](../../core/src/common/namespace.rs#L733-L767) writes `__namespace_state__/<id>` sidecars separately from `__namespaces__` (4 durable writes + 4 directory `fsync`s). However, [`load_registry`](../../core/src/common/namespace.rs#L335-L345) currently treats a missing `__namespaces__` file as `RegistryFile::default()` (`next_id = 1`), relying on surviving sidecars to block ID reuse for explicit-path namespaces if `__namespaces__` was lost!
4. **Default-engine initialization on `vsdb_flush()` and v15 allocator fold:**
   `ensure_alloc_init` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L882-L884)) forces `LazyLock::force(&VSDB)` whenever `<base>/mmdb` exists to fold pre-v16 shard-0 counters, and `vsdb_flush()` ([core/src/common/mod.rs](../../core/src/common/mod.rs#L544)) unconditionally initializes `DEFAULT_NS` (2 GiB sizing input = 256 MiB `BlockCachePool` + 16 shards/compaction workers) even when `<base>/mmdb` never existed.
5. **`ruc` SemVer leak and flattened `VsdbError::Other`:**
   `impl From<Box<dyn ruc::err::RucError>> for VsdbError` ([error.rs](../../core/src/common/error.rs#L154)) leaks `ruc` into public SemVer, while `mmdb.rs` flattens `std::io::Error` and `mmdb::ErrorKind` (`Io`, `Corruption`, `ReadOnly`, `InvalidArgument`, `DbClosed`, `Background`) into `VsdbError::Other { detail: String }` (`source() == None`).

### v18 design

1. **Strict `format_version = 18` root headers + `universe_id` / `ns_id` + OS advisory `LOCK`:**
   - Every engine root (`<base>/__SYSTEM__/format_version` for the universe/default root and `<ns_root>/__SYSTEM__/format_version` for non-default namespaces) stores a small structured header (`VSFMT018`, `universe_id: u128`, `ns_id: u64` where `0` = default).
   - Establish and verify the universe root **before** the first prefix allocation, registry load, or engine open (acquiring an OS advisory file lock on `<base>/__SYSTEM__/LOCK` in writable mode so accidental multi-process writers fail fast even across disjoint namespaces):
     - Fresh empty `<base>` atomically writes `<base>/__SYSTEM__/format_version`, initial `VSNREG02` `__namespaces__`, and `__prefix_ceiling__`.
     - Any non-empty `<base>` or namespace root with a missing marker, `version != 18`, or mismatched `(universe_id, ns_id)` (including on `Namespace::relocate`) fails closed with a structured `VsdbError` — **never** panicking in `vsdb_configure` / `Namespace::open` and never overwriting `v16`/`v17` roots.
2. **Fail-closed `VSNREG02` inline lifecycle + lazy `DEFAULT_NS`:**
   - Embed `state: NsLifecycleState` directly in `NsRecord` under `VSNREG02` (`__namespaces__`), deleting `__namespace_state__` sidecars and cutting `Namespace::create()` metadata writes from 4 (~8 `fsync`s) to 2 (~4 `fsync`s). Because `__namespaces__` is created at universe bootstrap alongside `format_version`, a missing `__namespaces__` in an existing universe (`format_version` present) **fails closed (`VsdbError::Corrupt`)** instead of resetting `next_id` to `1` — preserving explicit-volume ID non-reuse without sidecars.
   - Delete v15 `META_KEY_PREFIX_ALLOCATOR` migration, `struct VsDB`, and `vsdb_meta_path(u64)` / `vsdb_get_meta_dir()`; hold `DEFAULT_NS` in a `OnceLock<Namespace>` so `vsdb_flush()` only flushes `DEFAULT_NS.get()` when open; and unify `write_file_durable` onto `atomic_write_file`.
3. **Flat 24B/32B handle frames (`VSMAPX02` / `VSTYPE04`) + updated streaming prefix scanner:**
   - `MapxRaw`: fixed 24-byte stack frame `b"VSMAPX02" | prefix_le:8 | ns_id_le:8` (`0` = default).
   - Simple typed wrappers (`Mapx`, `MapxOrd`, `MapxOrdRawKey`, `Orphan`): flat 32-byte stack frame `b"VSTYPE04" | type_tag_le:8 | prefix_le:8 | ns_id_le:8`, eliminating intermediate `Vec<u8>` allocations during serde while supporting both `visit_bytes` and `visit_seq` (for non-binary serde formats) and retaining exact-length and `load_instance_meta_checked` identity checks. Delete `VSTYPE02` and `legacy_type_name`.
   - Update [`note_meta_bytes`](../../core/src/common/engine/mmdb.rs#L1165) to extract prefixes from **both** `VSMAPX02` (`+8..+16`) and `VSTYPE04` (`+16..+24`), increasing [`note_meta_stream`](../../core/src/common/engine/mmdb.rs#L1216)'s cross-read `carry` window to `23` bytes (`8 + 8 + 8 - 1`) so empty `VSTYPE04` handles split across 64 KiB buffer boundaries always reserve their prefix.
4. **Drop `ruc` without leaking `mmdb::Error` into public SemVer:**
   - Remove `ruc` from both crates and convert `mmdb.rs` to `crate::Result<T>`.
   - Map `mmdb::Error` via a **crate-private** `pub(crate) fn from_mmdb_err(...) -> VsdbError` (NOT a public `impl From<mmdb::Error> for VsdbError`, avoiding a new public dependency leak), handling `Io`, `Corruption`, `ReadOnly`, `InvalidArgument`, `DbClosed`, `Background`, and `#[non_exhaustive]` fallback while preserving `Error::source()` and distinguishing preflight rejection from post-WAL persistence errors. Move `BranchId(u64)` / `CommitId(u64)` to `vsdb_core::common` for typed `VsdbError` variants.

---

## 6. `VerMap` and `PersistentBTree`: single-handle store, 2-bit tagged sparse refcounts, crash-safe snapshot tombstones, and slotted pages

Source: [versioned/map.rs](../../strata/src/versioned/map.rs), [versioned/repair.rs](../../strata/src/versioned/repair.rs), [versioned/read.rs](../../strata/src/versioned/read.rs), [persistent_btree/mod.rs](../../strata/src/basic/persistent_btree/mod.rs), [persistent_btree/nodes.rs](../../strata/src/basic/persistent_btree/nodes.rs), [persistent_btree/merge.rs](../../strata/src/basic/persistent_btree/merge.rs).

### Current defects and costs

1. **8-prefix non-atomic `VerMapPayload` + `gc_dirty` + `BranchState` churn:**
   `VerMap` persists 8 separate handles ([versioned/map.rs](../../strata/src/versioned/map.rs#L244-L253)). Although v17 co-locates newly created maps onto one shard, engine batches remain single-prefix: `commit()` executes 6 separate writes + `gc_dirty` toggles + a final WAL fence, `new_with_main` burns ~112 prefixes to co-locate 8 handles on 16 shards, and every single-key `VerMap::insert`/`remove` decodes and re-encodes `BranchState` (cloning `name: String`) in a separate prefix after flushing B+ tree nodes.
2. **Unbounded `RefState` RAM and full-DB `from_meta()` scans:**
   Every live node's refcount and child list live only in volatile `RefState.counts: HashMap<NodeId, NodeRef>` ([persistent_btree/mod.rs](../../strata/src/basic/persistent_btree/mod.rs#L72)). Every `VerMap::from_meta()` runs **2 full `nodes.iter()` scans + random `nodes.get` across every reachable node** (reading ~96% leaf pages off disk just to discover `children.is_empty()`) — plus a WAL `fsync` in writable mode — and permanently pins >150 MiB of RAM on multi-million-key trees.
3. **~128 sibling `Vec<u8>` allocations per depth-4 `get()` and borrowed `Snapshot<'a>`:**
   Sequential `Node::decode` ([nodes.rs](../../strata/src/basic/persistent_btree/nodes.rs#L114-L138)) allocates `Vec<Vec<u8>>` for all ~24 sibling keys at every internal level and all ~24 keys **and values** at the leaf before returning 1 value. `Snapshot<'a, K, V>` ([versioned/read.rs](../../strata/src/versioned/read.rs#L37-L40)) borrows `&'a VerMap`, blocking concurrent `&mut VerMap` mutations even though `RootLease` pins the root. `PersistentBTree` also carries the subsystem's sole `unsafe` (`self.nodes.shadow()` at [persistent_btree/mod.rs](../../strata/src/basic/persistent_btree/mod.rs#L461)), lacks multi-key `VerMap` batch writes, double-descends on `merge::replay` inserts, and buffers all of `bulk_load` in RAM.

### v18 design

1. **Single-handle tagged `VerMap` store (`1 MapxRaw`, atomic per-prefix batches):**

   ```text
   [0x00]                   -> Header { ver: u8, next_id: u64, next_commit: u64, next_branch: u64, main_branch: u64 }
   [0x01 | branch_id_be:8]  -> BranchHead { head: u64, dirty_root: u64 }   // fixed 16B, separated from String name
   [0x02 | branch_id_be:8]  -> branch_name UTF-8 bytes
   [0x03 | branch_name]     -> branch_id_le:8
   [0x04 | commit_id_be:8]  -> Commit { root: u64, timestamp_us: u64, ref_count: u32, parents: [u64] }
   [0x05 | root_id_le:8]    -> []                  // ephemeral reclamation queue (pinned snapshot roots & chunked GC)
   [0x10 | node_id_le:8]    -> Slotted B+ tree node page
   [0x11 | obj_id_le:8]     -> extra_refs: u32     (stored iff incoming_ptrs >= 2; value = incoming_ptrs - 1)
   [0x12 | blob_id_le:8]    -> Separated large value bytes (> 1 KiB)
   ```

   - Working edits (`insert`, `remove`, `write_batch`, `discard`) commit node/blob pages + `Header` + `BranchHead` in **1 atomic `batch.commit()`** (0 `String` allocations, 0 WAL fences).
   - History operations (`commit`, `merge`, `create_branch`, `delete_branch`, `rollback_to`, **`set_main_branch`**) commit in **1 atomic `batch.commit_sync()`**.
   - **Open vs lazy verification contract:** Normal `VerMap::from_meta()` validates `Header` (plus `main_branch` existence) in **$O(1)$** (and in writable mode drains any residual `[0x05]` queue left by a prior crash), moving deep node/commit corruption checks from eager open-time full-DB walks to read-time `Corrupt` diagnostics and explicit `VerMap::verify_and_gc()` (which completes full graph/tree validation *before* committing any reclamation writes).
2. **2-bit tagged 62-bit IDs + sparse persisted `extra_refs` + exact blob accounting:**
   - Partition the 64-bit ID counter (`next_id < (1 << 62)`, checked on allocation) by top 2 bits so node kinds and value blobs share `[0x11]` without collision and tell cascade delete exactly when a page must be read:
     - `0b00`: Internal B+ tree node (has child `NodeId`s);
     - `0b01`: Inline-only leaf (**0 outgoing pointers — never read off disk on delete**);
     - `0b10`: Blob-bearing leaf (contains `>= 1` separated blob descriptor — read its fixed-offset `val_descs` header on final delete to decrement referenced `0b11` `BlobId`s);
     - `0b11`: Separated value blob at `[0x12 | blob_id_le]` (0 outgoing pointers).
   - Maintain exact incoming-pointer counts via `1 + extra_refs[id]`, staged per batch in `ref_delta: HashMap<u64, i32>`:
     - Allocating an internal node adds `+1` to its `children`; allocating a `0b10` leaf adds `+1` to each `0b11` `BlobId` in its `val_descs`.
     - Releasing an ID whose resulting count stays `>= 1` (e.g. first COW split of a shared committed node/leaf) decrements `extra_refs` and stops — its children/blobs stay owned by the still-live old node while the new copy adds `+1` to shared siblings/blobs in the same batch.
     - Releasing an ID whose resulting count hits `0` deletes its `[0x10]`/`[0x12]` row; `0b01` inline leaves and `0b11` blobs stop immediately with **zero disk reads**, while `0b00` internal nodes and `0b10` blob-bearing leaves subtract `-1` from their outgoing IDs (canceling `-1`/`+1` to `0` in memory on unshared working paths).
3. **Crash-safe `Snapshot` retirement and bounded-batch reclamation via `[0x05]`:**
   - Back `RootLease` with safe `MapxRawReader` (`#![forbid(unsafe_code)]` across `persistent_btree` and `versioned`) and make `Snapshot<K, V>` **owned (`'static + Clone + Send + Sync`)**.
   - Close the process-crash leak window on snapshot-pinned roots: under the `TreeRuntime` pin lock (`parking_lot::Mutex`), if a writer batch retires a root `R` whose durable count reaches `0` while an in-process `Snapshot` still pins `R`, the writer batch inserts `[0x05 | R_le] -> []` in the **same atomic `WriteBatch`** instead of deleting `R`'s pages. When the last `RootLease` for `R` drops, it pushes `R` onto `runtime.zombie_roots` (zero I/O and zero `unsafe` in `Drop`); the next writable operation (or writable `from_meta()` after a crash) deletes `[0x05 | R_le]` and cascade-releases `R`.
   - Use the same `[0x05]` queue + byte-bounded (~16 MiB) staging chunks so massive `bulk_load`s, merges, or `delete_branch` cascades never build an unbounded `mmdb::WriteBatch` in RAM.
4. **Slotted `NodeView<'a>(&'a [u8])`, owned-leaf `BTreeIter`, and range-grouped `write_batch`:**
   - Encode `u32` key end-offsets and `u64` value/blob descriptors at the head of each node page. Each `mmdb` page fetch still returns one owned `RawValue` (`Vec<u8>`) per tree level, but `NodeView` binary-searches `&[u8]` slices inside it — eliminating all **~128 per-sibling key/value `Vec` allocations** on a depth-4 `get()`. `BTreeIter` holds the current leaf's `RawValue` buffer and decodes `(K, V)` from borrowed slices inside `next()` before advancing leaves.
   - Demote `PersistentBTree` to `pub(crate)`, drop `K: Clone, V: Clone` from `VerMap::clone()`, add `VerMap::clone_in(&ns)`, add `VerMap::write_batch` sharing a single-pass sorted range-grouped COW applier with `merge::replay`, and make `bulk_load` and `BTreeDiffIter` $O(\text{height})$ streaming.

---

## 7. Merkle tries (`MptCalc` and `SmtCalc`): `Arc`-COW nodes, map-bound streaming checkpoints, and unified `TrieCalc`

Source: [trie/mod.rs](../../strata/src/trie/mod.rs), [trie/node/mod.rs](../../strata/src/trie/node/mod.rs), [trie/nibbles.rs](../../strata/src/trie/nibbles.rs), [trie/mpt/mutation.rs](../../strata/src/trie/mpt/mutation.rs), [trie/smt/mutation.rs](../../strata/src/trie/smt/mutation.rs), [trie/cache.rs](../../strata/src/trie/cache.rs), [trie/codec_util.rs](../../strata/src/trie/codec_util.rs), [trie/proof.rs](../../strata/src/trie/proof.rs), [audit.md](../audit.md).

### Current defects and costs

1. **`Box` + `mem::take` ownership churn, partial `batch_update`, and `VerMapWithProof::poison()`:**
   While single-op `TrieMut::insert` validates `MAX_MPT_KEY_LEN` (`1024` bytes = 2,048 nibbles) before `mem::take(&mut self.root)` ([mpt/mutation.rs](../../strata/src/trie/mpt/mutation.rs#L28-L36)), `mem::take` still empties `self.root` across any panic/unwind, and `batch_update` ([trie/mod.rs](../../strata/src/trie/mod.rs#L223-L238)) leaves earlier items applied when a later item fails — forcing `VerMapWithProof` to `poison()` and discard the entire cached trie ([trie/proof.rs](../../strata/src/trie/proof.rs#L235)). $O(N)$ `Clone` also forces `VerMapWithProof::sync_to_branch` to recompute B+ tree diffs on repeated `merkle_root` calls and run `reverse_dirty()` undo passes.
2. **Per-node heap bloat and 256-deep SMT recursion:**
   `Cached(Vec<u8>, Box<Node>)` allocates a separate 32B `Vec<u8>` per hashed node; `Node::Branch` allocates `Box<[Option<NodeHandle>; 16]>` (256+ bytes) even for 2 children; `Nibbles(Vec<u8>)` allocates on the heap and allocates 2 `Vec<u8>`s on every `split_at`; `NodeCodec::encode` allocates a `Vec<u8>` per hashed node; and `SmtCalc` uses recursive `Drop` and 256-frame recursive walks.
3. **Non-atomic checkpoint writes, unbound pre-validation memory (`[LOW]` in `audit.md`), and weak map binding:**
   [`write_cache_file`](../../strata/src/trie/codec_util.rs#L104-L111) truncates the live cache in place via `File::create`, buffers up to 512 MiB in RAM, stores redundant 34B per-node hashes, and materializes the full graph before structural validation ([audit.md](../audit.md#L57-L60)). [`VerMapWithProof::try_load_cache`](../../strata/src/trie/proof.rs#L525-L551) also checks only that `CommitId` exists on `self.map`, so a checkpoint copied from another map with the same numerical `CommitId` would be accepted.

### v18 design

1. **Candidate-root `Arc`-COW nodes + inline `OnceLock<[u8; 32]>` (preserving identical Keccak256 preimages & proof wire bytes):**
   - `Clone` becomes $O(1)$ (`Arc::clone`). Single and batch mutations clone `let mut candidate = self.root.clone();`, edit via `Arc::make_mut` + `hash.take()`, and assign `self.root = candidate;` **only on `Ok`** — making every update **100% atomic across both `Err` and panic/unwind**, and deleting `mem::take`, `TrieMut`/`SmtMut`, `reverse_dirty()`, and `poison()`.
   - Keep **iterative last-owner (`Arc::try_unwrap`) drop stacks** on **both** `MptNode::drop` and `SmtNode::drop` (plus iterative SMT walks) so dropping deep uniquely-owned chains never overflows the stack.
   - Compact `MptNode::Branch` to `(bitmap: u16, children: Box<[Arc<MptNode>]>, value: Option<Box<[u8]>>)` via POPCNT indexing (3–5× lower RAM); pack `Nibbles` inline up to 64 nibbles (32-byte keys) **with a `Box<[u8]>` heap fallback up to 2,048 nibbles (`MAX_MPT_KEY_LEN = 1024` bytes)**; and stream `NodeCodec` directly into `Keccak256::update`.
   - `VerMapWithProof` retains `committed_trie: T` and forks `working_trie` in $O(1)$ only when `working.root != synced_working_root`.
2. **Durable, budget-gated, map-bound post-order checkpoints (`MPTC v2` / `SMTC v4`):**
   - Write via `BufWriter` to sibling temp file → `BufWriter::flush` → `File::sync_all` → atomic `fs::rename` → parent-dir `sync_all`, omitting redundant per-node stored hashes (-40–60% file size).
   - Bind the checkpoint header to `(universe_id, map_instance_id, commit_id, btree_root_id)` so foreign checkpoints cannot match by `CommitId` coincidence.
   - Enforce `max_nodes`, `max_pending_stack`, `max_value_len`, and cumulative `decoded_bytes` budgets **before** each allocation during `BufReader` parsing, plus online canonicality and ancestor-consistent SMT bit-length checks (closing [audit.md](../audit.md#L57-L60)).
3. **Unified `TrieCalc` and `VerMapWithProof` lifecycle:**
   - Add `type Proof` + `prove` / `verify_proof` on `TrieCalc`, return `[u8; 32]` from `root_hash` / `merkle_root`, unify `VerMapWithProof` proof methods into one generic `impl<K, V, T: TrieCalc>`, and add `new_in`, `clone_in`, `save_meta`, `from_meta`, `instance_id`, and `namespace`.

---

## 8. `DagMap`: shard-colocated single-handle nodes, ordered crash transitions, and typed `DagMap<K, V>`

Source: [dagmap/mod.rs](../../strata/src/dagmap/mod.rs), [dagmap/raw/mod.rs](../../strata/src/dagmap/raw/mod.rs), [dagmap/rawkey/mod.rs](../../strata/src/dagmap/rawkey/mod.rs), [audit.md](../audit.md).

### Current defects and hazards

1. **Scattered 3-handle nodes and full-namespace WAL syncs:**
   [`DagMapRaw`](../../strata/src/dagmap/raw/mod.rs#L86-L96) holds `{ data: MapxRaw, parent: Orphan<Option<DagMapRaw>>, children: MapxOrdRawKey<DagMapRaw> }`. `build()` leaves `data` un-colocated ([raw/mod.rs](../../strata/src/dagmap/raw/mod.rs#L278)) and `Clone` drops co-location completely, forcing `prune_mainline` to issue **5 full-namespace `try_sync_wal()` fences** (`5 × shard_count`, i.e. 80 shard WAL syncs in the 16-shard default namespace) and `destroy()` to issue 2 fences.
2. **Serde tuple decomposition + root-wipe debt (`[MEDIUM]` in `audit.md`), 7 `unsafe` blocks, and `DagIdAllocator`:**
   Bare 3-tuple serialization allows safe callers to deserialize and mutate `parent` into a cycle or plant a live root (`parent == None`) into `children` where `owned_or_residue` ([raw/mod.rs](../../strata/src/dagmap/raw/mod.rs#L669-L674)) wipes it. Storing `DagMapRaw` inside `Orphan`/`MapxOrdRawKey` requires **7 internal `unsafe` blocks**, a global `DagIdAllocator` (`dag_id_ceiling`), $O(\text{siblings})$ registry scans on `destroy()`, and 3 handle decodes per ancestor hop on `get()`.
3. **Empty-value `[]` panics and broken typed surface:**
   Using `[]` as deletion tombstone causes `insert(k, [])` and `DagMapRawKey<()>::insert` to panic; `DagMapRawKey<V>::new(Option<&mut DagMapRaw>)` ([rawkey/mod.rs](../../strata/src/dagmap/rawkey/mod.rs#L103)) forces callers into `into_inner()` or `unsafe { shadow_inner() }`; and typed `DagMap<K, V>` is missing.

### v18 design

1. **One shard-colocated `MapxRaw` per node with `dag_id` authentication and node-local `edge_id`s:**
   Keeping one `MapxRaw` per node preserves per-node `InstanceId` / `save_meta()` and $O(1)$ `batch_wiped()` range-tombstones, while creating every child via **`parent.store.new_colocated()`** places the entire connected DAG onto **one shard WAL** (**0 cross-shard WAL fences**):

   ```text
   [0x00]                   -> NodeMeta {
                                 dag_id: u64,          // random per-DAG token generated at root creation
                                 next_edge_id: u64,    // node-local monotonic child edge counter
                                 state: Root
                                      | Child { parent_map: u64, parent_edge: u64 }
                                      | Clearing { owner_parent: Option<u64>, parent_edge: Option<u64>, genesis_map: u64 }
                                      | Destroying { parent_map: Option<u64>, parent_edge: Option<u64> }
                                      | Destroyed
                               }
   [0x01 | edge_id_be:8]    -> child_map_id_le:8
   [0x02 | user_key]        -> [0x01 | val] | [0x00]   // live value vs deletion tombstone
   ```

   - **Preserves `child_ids()` across `Clone` without `DagIdAllocator`:** Keying `[0x01]` by the parent's local `edge_id` (and storing `parent_edge` in the child's `NodeMeta`) preserves logical child edge tokens across `Clone` / `clone_in(&ns)` ([dagmap/raw/test.rs](../../strata/src/dagmap/raw/test.rs#L914-L923)) while keeping `child_id(&child)` and parent unlink **$O(1)$** and deleting global `DagIdAllocator` / `dag_id_ceiling`.
2. **Crash-ordered single-shard state transitions (no multi-prefix atomicity assumption):**
   Because co-located prefixes share one WAL (program order = crash replay order, not cross-prefix batch atomicity), multi-node `prune` and `destroy` preserve self-healing invariants without WAL fences:
   - **`destroy()`:** (a) transition target from `Child { p, e }` / `Root` to `Destroying { p, e }` and wipe its `[0x02]` data; (b) pass 1 marks/wipes owned descendants' `[0x02]` data while keeping `[0x01]` discovery edges intact; (c) pass 2 clears descendants' `[0x01]` edges and transitions them to `Destroyed`; (d) remove `[0x01 | e]` from `p`, then write `Destroyed` on target (so a retry from a saved child handle still has `p, e` until unlinked).
   - **`prune()`:** (a) mark mainline nodes `Clearing { owner_parent, parent_edge, genesis_map }`; (b) fold net overlay into `genesis_map` and reparent surviving children to `genesis_map`; (c) wipe consumed nodes' `[0x02]` data **keeping `Clearing { .., genesis_map }`** in `[0x00]` (so repeated `prune()` on a consumed handle still resolves `genesis_map`), then clear consumed `[0x01]` registries.
3. **Crate-private reconstruction, `dag_id` ownership check, and complete typed hierarchy:**
   - Keep `open_colocated_prefix(map_id)` **crate-private** (never `pub` on `MapxRaw`, preserving prefix capability isolation), and serialize `DagMapRaw` opaquely via `VSTYPE04` `(LAYOUT_VERSION, store, dag_id)`. Every traversal/cleanup verifies `[0x00]` `NodeMeta` magic, matching `dag_id`, and `owner_parent == Some(self_id)` (never wiping `Root` or another DAG's `Clearing` node — closing [audit.md](../audit.md#L36-L40)).
   - Delete **all 7 `unsafe` blocks**; check local `self.store.get` before allocating cycle-detection state; support empty `[]` / `()` values natively; fix `DagMapRawKey<V>::new(parent: Option<&mut Self>)`; add `clone_in(&ns)`; and ship **`DagMap<K, V>`** (`K: KeyEnDe, V: ValueEnDe`).

---

## 9. `VecDex`: unified stateless handles, snapshot-consistent reads, and zero-allocation search

Source: [vecdex/mod.rs](../../strata/src/vecdex/mod.rs), [vecdex/dynamic.rs](../../strata/src/vecdex/dynamic.rs), [vecdex/hnsw.rs](../../strata/src/vecdex/hnsw.rs), [vecdex/distance.rs](../../strata/src/vecdex/distance.rs), [audit.md](../audit.md).

### Current defects and costs

1. **Stale 34-byte `GraphState` corruption (`[MEDIUM]` in `audit.md`) and torn `get` / `search` / `iter` reads:**
   [`VecDex`](../../strata/src/vecdex/mod.rs#L364) caches `GraphState` (5 scalars, 34 bytes) by value, so an independently restored handle holds a stale `next_node_id` and overwrites live nodes on sequential writes. On the read path, [`get`](../../strata/src/vecdex/mod.rs#L570-L575) (`k -> node_id`, then `node_id -> vec`), [`iter`](../../strata/src/vecdex/mod.rs#L590-L601), and [`search_internal`](../../strata/src/vecdex/mod.rs#L902-L996) issue separate uncoordinated `self.store.get` calls without a `MapxRawReadView` — a concurrent `clear()` (which resets `next_node_id` to `0` at [vecdex/mod.rs](../../strata/src/vecdex/mod.rs#L630-L635)) followed by an insert can make `get(k1)` return **`k2`'s vector**!
2. **392 lines of duplicated `VecDexDyn` (`dynamic.rs`):**
   [`DistanceMetric::distance(a, b)`](../../strata/src/vecdex/distance.rs#L101) lacks `&self`, preventing `MetricKind` (and parameterized metrics like `Lp(p)`) from implementing `DistanceMetric` and forcing [`dynamic.rs`](../../strata/src/vecdex/dynamic.rs) to duplicate the entire `VecDex` API.
3. **Per-visited-node `Rc<Vec<S>>` + `HashMap` search allocations and redundant `K: ValueEnDe`:**
   `search_internal` allocates `Vec<S>` + `Rc<Vec<S>>` into a `HashMap` for every visited node (up to `max(4096, 64 * ef)` in filtered search; $O(\text{ef} \cdot \log N)$ in unfiltered search) even though `visited: HashSet<u64>` evaluates each layer-0 node at most once. Meanwhile `TAG_NODE2KEY` encodes `K` via `ValueEnDe` instead of `KeyEnDe` (forcing `K: KeyEnDe + ValueEnDe`), and unbounded `Txn::vec_cache` adds a 3rd in-memory copy of all vectors during `insert_batch` and `compact()`.

### v18 design

1. **Persisted `[TAG_META]` + monotonic 34-byte `[TAG_STATE]` + full `MapxRawReadView` coverage:**
   - Persist immutable schema (`dim: u32`, `scalar_tag: u8`, `hnsw_config`, `metric_tag_and_params`) at `[TAG_META]` and validate it on `from_meta` / `Deserialize` so mismatched metrics (e.g. `L2` vs `Cosine` under `VecDexDyn`, `Lp(2)` vs `Lp(3)`, `f32` vs `f64`) fail closed on open.
   - Persist mutable `GraphState` (`entry_point`, `max_layer`, `node_count`, `next_node_id`, default `ef_search` updated by `set_ef_search`) as a fixed 34-byte LE record at `[TAG_STATE]`, **preserving monotonic `next_node_id` across `clear()`** rather than resetting to `0`.
   - Execute **every** multi-row read — **`get`**, **`iter`** (holding `MapxRawReadView<'a>` inside `VecDexIter<'a>` alongside `MmdbIter`), **`search`**, **`search_with_filter`**, and **`compact`**'s input scan — over a single `MapxRawReadView`. Restored handles become coherent for sequential writers and concurrent snapshot readers (closing [audit.md](../audit.md#L22-L26) for `VecDex`; concurrent writers still require `&mut` exclusion), unlocking deep-copy `Clone`, `clone_in(&ns)`, and `VecDexReader`.
2. **Unify `VecDex` and `VecDexDyn` via `&self` on `DistanceMetric<S>` without inner-loop branch regression:**
   - Change `DistanceMetric<S>::distance(&self, a: &[S], b: &[S]) -> S` to take `&self` (plus a default `dispatch_search` hook specialized once per query by `MetricKind` so dynamic metrics hoist the enum match **outside** `search_layer`), delete `dynamic.rs`, and define `pub type VecDexDyn<K, S = f32> = VecDex<K, MetricKind, S>;`.
3. **Sealed IEEE-754 LE vector bytes, zero-alloc distance scratch, `KeyEnDe`-only `TAG_NODE2KEY`, and bounded `insert_batch` cache:**
   - Seal `Scalar`'s wire format to IEEE-754 `f32` and `f64`, storing raw `dim * size_of::<S>()` LE bytes at `[TAG_VEC | node_id_be]` (saving `postcard`'s length-varint framing while **keeping `TAG_VEC` separate from `TAG_ADJ`** so `2 × m_max0 = 64` neighbor updates per `insert` never rewrite multi-KB vectors).
   - Decode unaligned `&[u8]` chunks via `as_chunks::<4>()` / `as_chunks::<8>()` (`from_le_bytes`) into one reusable `dim`-element scratch slice (or direct chunked FMA) in `search_layer`, eliminating all per-visited-node `Vec<S>` / `Rc<Vec<S>>` / `HashMap` allocations (note `mmdb` still returns one owned `RawValue` `Vec<u8>` per fetched vector).
   - Encode `TAG_NODE2KEY` with `KeyEnDe::encode_key(&k)` (dropping the redundant `K: ValueEnDe` bound while keeping 1-byte `TAG_INFO` separate so disconnected-graph ID fallback scans never read large user keys). Bound `Txn::vec_cache` (e.g. 32 MiB) to cap `insert_batch` RAM; document that whole-index `compact()` still stages the rebuilt graph in memory unless paired with a fresh-prefix swap.

---

## Migration and preserved invariants

### Offline v17 → v18 migration contract

Because `SlotDex::iter` omits slots, `VerMap` `Snapshot::iter` omits commit DAG ancestry/branches/dirty roots, `DagMap::get` flattens per-node overlays/tombstones, and nested collection handles embed universe-specific prefixes, **ordinary public collection iterators are not sufficient for lossless v17 → v18 migration**.

Before flipping the default format break, ship a dedicated **`v17` export / `v18` import migration utility** with an explicit fidelity contract:
- **Lossless logical export/import:** `Mapx*` / `Orphan` entries; `SlotDex` raw `(slot, key)` pairs for fixed-width slot types (`u32`/`u64`/`u128`); `VerMap` commit DAG (`CommitId`, parents, timestamps), branches, and working trees; `DagMap` per-node local live entries, deletion tombstones, and child edge tokens; `VecDex` `(key, vector)` + config/metric/ef (rebuilding HNSW layers cleanly); and an `InstanceId` remapping table for nested handles.
- **Documented non-recoverable legacy case:** Any `v17` `SlotDex` populated with a prefix-colliding variable-width custom `SlotType` ([audit.md](../audit.md#L14-L18)) cannot be disambiguated from index rows alone and must be rebuilt from application source records.

### Preserved architectural invariants

- **Shard routing and universe model:** Default namespace stays at 16 shards (`NUM_SHARDS`); writable access remains single-process per universe (now enforced by `__SYSTEM__/LOCK` as well as in-process `parking_lot` locks).
- **`Clone` on mutable collections vs immutable readers/snapshots:** `Clone` on every mutable collection handle (`Mapx*`, `Orphan`, `VerMap`, `DagMap*`, `SlotDex`, `VecDex`) deep-copies storage into a fresh prefix (`MptCalc`/`SmtCalc` `Arc`-COW `Clone` preserves identical isolated value semantics in memory). Cheap reference-counted read sharing uses explicit immutable types (`*Reader`, `Snapshot<K, V>`), each of which retains a `Namespace` clone (blocking `Namespace::close` until dropped; `mmdb` `range_detached` snapshots retain their existing documented source-refcount behavior in [ns-close.md](ns-close.md#L33-L38)).
- **Cryptographic wire parity:** `MptCalc` and `SmtCalc` node encodings, Keccak256 preimages, root hashes, and proof transport bytes remain bit-for-bit identical to v17 (verified against frozen non-empty membership/absence, odd-nibble, and compressed-path v17 fixtures).
- **Non-goals:** No cross-namespace or arbitrary cross-collection interactive transactions; no implicit per-insert `fsync` on ordinary writes.

---

## Recommended v18 implementation sequence

1. **Stage 0 — Golden v17 fixtures & migration exporter:** Capture frozen v17 fixtures (branched/merged/dirty `VerMap`, multi-node `DagMap` with tombstones and cloned `child_ids`, `SlotDex` with multi-slot duplicate keys, nested empty handles, non-empty MPT/SMT membership & absence proofs, and an unrecoverable custom-slot `SlotDex` negative fixture) plus the v17 export reader before altering codecs or layouts.
2. **Stage 1 — Format-neutral `vsdb_core` improvements:** Drop `ruc` (`pub(crate)` `mmdb::Error` mapping + typed `BranchId`/`CommitId` in `VsdbError`), unify `mmdb.rs` onto `iter_with_prefix` + unboxed `MmdbIter` + stack `with_full_key` + `RawRangeBounds`, and complete `MapxRawReader`. Keep an unreleased development format marker (`18-dev`) + per-collection `LAYOUT_VERSION` gates until Stage 7 freezes wire layouts.
3. **Stage 2 — Universe/handle headers, codecs, and basic collections:** Land universe/root `VSFMT018` + `LOCK` + fail-closed `VSNREG02` + lazy `OnceLock` `DEFAULT_NS`, flat `VSMAPX02`/`VSTYPE04` (updating `note_meta_bytes` + 23-byte `note_meta_stream` carry, and porting `define_map_wrapper!::instance_id` off `as_bytes`), 2-trait `KeyEnDe`/`ValueEnDe` + `KeyLookup`/`OrderedKeyLookup`, `DerefMut`-only `ValueMut` + `update`, safe `enum Entry`, `insert_encoded_value` removal, and typed `*Reader` / `*ReadView`.
4. **Stage 3 — `SlotDex`:** Land `[u8; N]` `SlotType::Bytes` (with rank-space `floor_align`), `tier_capacity: u32`, zero-alloc stack helpers, bounded physical-reverse `iter`, common-ancestor `count`, and `[total, generation]` (preserved across `clear()`) + snapshot-owning reads. Close `[CRITICAL] slotdex` in [audit.md](../audit.md) once custom-slot, `swap_order`, stale-alias, and `clear()` regression tests pass.
5. **Stage 4 — `DagMap`:** Land single-handle shard-colocated `DagMapRaw` with `dag_id` + local `edge_id`s and crash-ordered `prune`/`destroy` transitions, delete `DagIdAllocator` together with its last `from_bytes_in` / `from_prefix_slice_in` callers and all 7 `unsafe` blocks, fix `DagMapRawKey::new`, and add `DagMap<K, V>`. Close `[MEDIUM] dagmap` in [audit.md](../audit.md) after mid-prune/mid-destroy crash-retry tests pass.
6. **Stage 5 — `VecDex`:** Land `[TAG_META]` + monotonic 34B `[TAG_STATE]` + full `MapxRawReadView` coverage (`get`/`iter`/`search`/`compact`), `&self` `DistanceMetric` unifying `VecDexDyn`, sealed IEEE-754 LE vector slices + zero-alloc search scratch, `KeyEnDe`-only `TAG_NODE2KEY`, and bounded `Txn::vec_cache`. Update `[MEDIUM] cached indexes` in [audit.md](../audit.md).
7. **Stage 6 — `PersistentBTree` + `VerMap`:** Land 2-bit tagged IDs + sparse `extra_refs` + `[0x05]` crash-safe snapshot-pin/GC queue, slotted `NodeView`, single-handle `VerMap` store, `#![forbid(unsafe_code)]` owned `'static` `Snapshot`, `VerMap::write_batch` + range-grouped `merge::replay`, and streaming `bulk_load` / `BTreeDiffIter`.
8. **Stage 7 — `TrieCalc` + `VerMapWithProof` & final `format_version = 18` freeze:** Land candidate-root `Arc`-COW `MptCalc`/`SmtCalc` + iterative `Drop` + POPCNT branches + inline-64/heap `Nibbles`, map-bound durable streaming checkpoints (`MPTC v2` / `SMTC v4`), and unified `TrieCalc::Proof` + `VerMapWithProof` lifecycle APIs; close `[LOW] trie` in [audit.md](../audit.md), finalize `SUPPORTED_FORMAT_VERSION = 18`, and validate end-to-end v17 → v18 migration fixtures and isolated Criterion benchmarks ([audit.md](../audit.md#L64-L67)).
