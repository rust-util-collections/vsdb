# VSDB v18 Structural Refactoring Roadmap

Status: proposed major-release design (`v18.0.0`), 2026-11-24. Not yet implemented; current `v17` contracts remain active until v18 development begins.
Baseline: `1a09c25` (`v17.0.21`).

## Purpose and release scope

`v16` and `v17` introduced namespaces, in-process engine teardown, per-engine block-cache pools, single-handle crash-atomic `SlotDex` and `VecDex` layouts, `PersistentBTree` versioning, synchronous batches, and raw read views — while preserving backward compatibility with `v15`/`v16` shims, multi-handle composite payloads, and unconstrained trait bounds.

`v18.0.0` uses a clean major-version boundary (`format_version = 18`, handle envelopes `VSMAPX02` / `VSTYPE04`, namespace registry `VSNREG02`) to pay down that accumulated structural debt across both crates (`vsdb_core` and `vsdb`):

- eliminate the open `[CRITICAL]` `SlotDex` custom-slot encoding defect and close the major structural `Won't Fix` debts in [audit.md](../audit.md);
- collapse multi-prefix composite collections (`VerMap`, `DagMap` nodes) onto single-prefix / single-shard atomic batches, deleting crash-repair flags and cross-shard WAL sync storms;
- eliminate avoidable `unsafe` blocks across `Entry` APIs, `PersistentBTree`, and `DagMap`, and prune legacy `pub unsafe` constructors;
- remove per-operation heap-allocation amplification across key encoding, engine range scans, B+ tree node reads, mutable value guards, Merkle tries, and HNSW search.

### Consolidated feature matrix

| # | Subsystem | Planned v18 refactoring | Audit / legacy debt closed | Primary win | Break scope |
|---|-----------|-------------------------|----------------------------|-------------|-------------|
| 1 | `SlotDex` | Sealed `[u8; N]` `SlotType::Bytes`, `tier_capacity: u32`, tier-guided `Order::Desc` iter, `[total, generation]` check | Closes `[CRITICAL] slotdex` ([audit.md](../audit.md)) | Variable-width bug unrepresentable at compile time; 0 temp slot allocs; fixes 100× reverse-iter cliff | Public API + handle meta |
| 2 | `ende` + `Mapx*` | 2-trait `KeyEnDe`/`ValueEnDe` (`Eq + Hash` key blanket), zero-alloc `with_encoded_key`/`with_ordered_bytes`, `(K, Q)` `KeyLookup`, stack `with_full_key` | Removes `Mapx::query_bytes` 4-alloc shim, `usize`/`isize` non-portability, `f64`/`HashMap` key footguns | 0 heap allocs on point reads/`contains_key`/`remove`; compile-time key determinism | Public API + `usize` keys |
| 3 | `Engine` (`mmdb`) | Unify `range_at` onto `db.iter_with_prefix`, unbox `MmdbIter`, in-place prefix strip, `RawRangeBounds` / `OrderedRangeBounds` | Fixes bypassed SST prefix Bloom filters + double seek | Restores prefix Bloom pruning on all ranges/`get_le`/`get_ge`; 0 `Box<dyn>` / key reallocs; `map.range(b"a"..b"z")` | Public API |
| 4 | Basic collections | Safe `enum Entry`, `DerefMut`-only `ValueMut`, typed `Reader` + `ReadView`, dead `pub unsafe` removal | Completes [durable-batches-and-read-views.md](durable-batches-and-read-views.md) §2; removes all `Entry` + `PersistentBTree` `unsafe` | 0 upfront clones/encodes on `get_mut`/`iter_mut`; `#![forbid(unsafe_code)]` on B+ tree & versioning | Public API |
| 5 | `core` / envelopes / errors | Flat stack `VSMAPX02` (24B) & `VSTYPE04` (32B), `VSNREG02` inline lifecycle, lazy `OnceLock` default ns, drop `ruc` | Removes `VSTYPE02`, v15 allocator fold, `__namespace_state__` sidecars, public `RucError` SemVer leak | 0-alloc handle serde; halves `Namespace::create` fsyncs; non-default-ns processes stop opening 16 default shards; structured `VsdbError` | Public API + On-disk |
| 6 | `VerMap` + `PersistentBTree` | Single-handle tagged store, `LEAF_BIT` + sparse on-disk `extra_refs`, slotted zero-copy `NodeView`, `'static` `Snapshot`, `write_batch` | Removes 8-prefix `VerMapPayload`, `gc_dirty`, startup full-DB scans, >150 MiB `RefState` RAM | **O(1) `from_meta()` open**; ~130 → **0 `Vec` allocs per `get()`**; 1 atomic batch per edit/commit; concurrent `Snapshot` + `&mut VerMap` | Public API + On-disk |
| 7 | `MptCalc` + `SmtCalc` | `Arc`-COW nodes + inline `OnceLock<[u8; 32]>`, POPCNT branches, inline `Nibbles`, atomic streaming checkpoints, unified `TrieCalc` | Fixes `mem::take` error tree-wipe; closes `[LOW] trie` cache amplification ([audit.md](../audit.md)) | O(1) `Clone` & atomic rollback; 3–5× lower MPT RAM; crash-safe streaming checkpoints (-40–60% size) | Public API + cache files |
| 8 | `DagMap` | 1 co-located `MapxRaw` per node, tagged rows (`Root`/`Child`/`Clearing`/`Destroyed`), typed `DagMap<K, V>` | Closes `[MEDIUM] dagmap` serde decomposition ([audit.md](../audit.md)); deletes all **7 `unsafe` blocks** + `DagIdAllocator` | **80 → 0 cross-shard WAL syncs** per `prune()`; supports empty `[]`/`()` values; O(1) child unlink | Public API + On-disk |
| 9 | `VecDex` | Stateless handle over 34B `[TAG_STATE]` + `MapxRawReadView`, `&self` `DistanceMetric` (merging `VecDexDyn`), raw LE vectors, zero-alloc search | Closes `[MEDIUM] cached indexes` ([audit.md](../audit.md)) for `VecDex`; deletes 392-line `dynamic.rs` | Coherent restored handles + `Clone`/`clone_in`; 0 per-node `Rc<Vec<S>>` allocs in search; -25% node meta rows | Public API + On-disk |

---

## 1. `SlotDex`: compile-time fixed-width `SlotType` (`[u8; N]`), `tier_capacity: u32`, and tier-guided descending scans

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
5. **100× reverse-iterator cliff in `iter(..., Order::Desc)` and double `S::MIN` walks in `count`:**
   [`level0_walk_desc`](../../strata/src/slotdex/mod.rs#L897-L900) documents that `mmdb` backward resolution costs ~100× more than forward streaming, yet [`SlotDex::iter`](../../strata/src/slotdex/mod.rs#L1446-L1459) calls `slot_rows.next_back()` on every slot and opens a separate `entry_range(slot..=slot)` snapshot per slot. [`entry_cnt_within_two_slots`](../../strata/src/slotdex/mod.rs#L1338-L1354) walks from `S::MIN` twice using signed `i128` even for a 2-slot window inside one bucket.

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
       /// Fixed-width big-endian (order-preserving) representation, e.g. `[u8; 8]`.
       type Bytes: sealed::SlotBytes;

       const MIN: Self;
       const MAX: Self;

       fn to_bytes(self) -> Self::Bytes;
       fn from_bytes(bytes: Self::Bytes) -> Result<Self>;

       /// Bucket width `tier_capacity^level`, returning `None` on overflow.
       fn tier_stride(tier_capacity: u32, level: u8) -> Option<Self>;
       fn floor_align(self, stride: Self) -> Self;
       fn checked_succ(self) -> Option<Self>;
       fn checked_pred(self) -> Option<Self>;
   }
   ```

   - Custom fixed-width coordinates (`BlockHeight(u64)`, `Hlc96([u8; 12])`, signed `i64` with sign-bit flip `^ i64::MIN` and `div_euclid` floor alignment) remain open and trivial to implement.
   - Variable-width `SlotType` becomes **impossible to express at compile time** — even a buggy `impl` cannot return varying byte lengths because `to_bytes` returns `[u8; N]`.
   - Change `tier_capacity` in `SlotDex::new` / `new_in` / persisted handle metadata (`LAYOUT_VERSION = 3`) to `u32` (`>= 2`). Delete `max_val`, `saturating_add`, `as_u64`, `as_i128`, and `checked_pow`.
2. **Single storage path and zero-allocation slot/key helpers:**
   - Entry rows stay `[0x00 | slot: [u8; N] | key_be]` with zero escaping, zero version branching, and zero runtime width checks.
   - Replace `decode_entry_row` across `page` and `iter` with:
     ```rust
     #[inline(always)]
     fn decode_entry_key<S: SlotType, K: KeyEnDeOrdered>(raw_key: &[u8]) -> K {
         K::from_slice(&raw_key[1 + S::Bytes::LEN..])
             .expect("SlotDex: corrupt entry-row key bytes")
     }
     ```
   - Encode `level_prefix(level)` as `[u8; 2]` on the stack, look up `slot_entry_cnt` via a `[u8; 34]` stack slice when `S::Bytes::LEN <= 32` (0 heap allocations), and decode `decode_level_row` into `S::Bytes::zeroed()` on the stack.
3. **Tier-guided forward `iter(..., Order::Desc)` and common-ancestor `count`:**
   - Drive descending `iter` through lazy ~32-slot forward `level0` windows (sharing `level0_walk_desc`'s chunking). Coalesce contiguous small slots (`sum(cnt) <= 256`, known from L0 counts) into one forward `entry_range` scan (reversing slot groups while keeping within-slot keys ascending), and stream large slots (`cnt > 256`) individually so `.take(k)` keeps $O(k)$ bounded memory.
   - Compute `count(slots)` from the lowest unshared tier bucket between `lo` and `hi` using unsigned `u64` arithmetic, and inline the 4 legacy `pub(crate)` `get_entries_by_page*` / `total_by_slot` / `entry_cnt_within_two_slots` helpers.
4. **Generation-checked cache coherence + `MapxRawReadView`:**
   - Store `[total: u64 LE, generation: u64 LE]` (16 bytes) at `[TAG_TOTAL]`, incremented on every committed mutation batch.
   - Rather than introducing a hidden process-global shared mutable cache (ruled out by [performance-roadmap.md](performance-roadmap.md) §四.3), keep single-active-writer ownership on `SlotDex`, execute multi-step reads (`page`, `count`, `iter`) against one `self.store.read_view()`, and check `persisted_gen == self.generation` so a stale restored handle fails fast with `VsdbError` on write (and re-hydrates or uses `SlotDexReader` on read) instead of silently corrupting counters. Add deep-copy `Clone` and `clone_in(&ns)`.

---

## 2. Codec and key pipeline: zero-allocation lookups and compile-time key determinism

Source: [ende.rs](../../strata/src/common/ende.rs), [mapx/mod.rs](../../strata/src/basic/mapx/mod.rs#L184-L215), [mapx_ord/mod.rs](../../strata/src/basic/mapx_ord/mod.rs#L81-L135), [mmdb.rs](../../core/src/common/engine/mmdb.rs#L1612-L1617).

### Current defects and costs

1. **2–4 heap allocations per point read or write:**
   `KeyEnDeOrdered::to_bytes` ([ende.rs](../../strata/src/common/ende.rs#L323)), `OrderedKeyRef::ordered_key_bytes` ([ende.rs](../../strata/src/common/ende.rs#L285)), and `KeyEn::encode_key` ([ende.rs](../../strata/src/common/ende.rs#L85)) always allocate a `Vec<u8>` — even for `&str`/`&[u8]` (already contiguous ordered bytes) and scalar integers (`u64::to_be_bytes().to_vec()`). `MmDB` then calls `make_full_key(&meta_prefix, key)` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L1612)), allocating another `Vec<u8>` (`8 + key.len()`) just to pass `&[u8]` to `mmdb`.
2. **`Mapx::query_bytes` 4-allocation workaround and unchecked `MapxOrd` borrowed lookups:**
   Because `KeyRef` / `OrderedKeyRef` ([ende.rs](../../strata/src/common/ende.rs#L240-L307)) are traits on `Q` alone without knowing `K`, `[u8]` cannot tell `Vec<u8>` (varint-prefixed in postcard) apart from `[u8; N]` (unprefixed tuple in postcard). [`Mapx::query_bytes`](../../strata/src/basic/mapx/mod.rs#L194-L215) works around this on every `&[u8]` lookup by running up to **4 heap allocations + 2 postcard decodes + 2 postcard encodes**. Meanwhile `MapxOrd` ([mapx_ord/mod.rs](../../strata/src/basic/mapx_ord/mod.rs#L114-L135)) lacks any length check when querying `[u8; N]` via `Borrow<[u8]>`.
3. **Silent data-loss blanket `KeyEn`, non-portable `usize`/`isize`, and 6-trait collisions:**
   - `impl<T: Serialize> KeyEn for T` ([ende.rs](../../strata/src/common/ende.rs#L176-L180)) lets `Mapx<f64, V>` and `Mapx<HashMap<A, B>, V>` compile cleanly and silently lose entries across restarts (`SipHash` randomization) or `-0.0`/`NaN`.
   - `usize`/`isize` `KeyEnDeOrdered` ([ende.rs](../../strata/src/common/ende.rs#L532-L555)) writes platform-dependent 4B vs 8B keys; `Box<[$int]>::to_bytes` ([ende.rs](../../strata/src/common/ende.rs#L462)) double-allocates via `self.to_vec()`; `impl_array!(1..=128)` ([ende.rs](../../strata/src/common/ende.rs#L541-L575)) expands 1,536 impl blocks; and splitting `KeyEn`/`KeyDe`/`KeyEnDe` + `ValueEn`/`ValueDe`/`ValueEnDe` causes `encode`/`decode` method name collisions (`E0034`).

### v18 design

1. **Two traits (`KeyEnDe`, `ValueEnDe`) with deterministic key bounds and stack/buffer paths:**
   - Collapse the 6 traits into `pub trait KeyEnDe: Eq + Sized` (`encode_key_into`, `with_encoded_key`, `encode_key`, `decode_key`) and `pub trait ValueEnDe: Sized` (`encode_value_into`, `encode_value`, `decode_value`).
   - Tighten the blanket impl to `impl<T: Serialize + DeserializeOwned + Eq + Hash> KeyEnDe for T`. Because `f32`, `f64`, `HashMap`, and `HashSet` do not implement `Eq + Hash`, non-canonical float/map keys become compile-time errors while valid keys (`u64`, `String`, `Vec`, `BTreeMap`, tuples, arrays, `#[derive(PartialEq, Eq, Hash, Serialize, Deserialize)]` types) compile unchanged.
   - Implement `with_encoded_key` using `postcard::to_slice` into a `[u8; 128]` stack buffer, falling back to `postcard::to_allocvec` only on `SerializeBufferFull`.
   - On `KeyEnDeOrdered`, require `encode_ordered_into(&self, dst: &mut Vec<u8>)` and `with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R` (passing `&[u8]` zero-copy for `String`/`Vec<u8>`/`Box<[u8]>` and stack `[u8; size_of::<T>()]` for scalars). Remove `usize`/`isize` from `KeyEnDeOrdered`, fix `Box<[$int]>`, and replace `impl_array!(1..=128)` with 12 const-generic `impl<const N: usize> KeyEnDeOrdered for [$int; N]` blocks.
2. **Codec-paired `KeyLookup<Q>` and `OrderedKeyLookup<Q>`:**
   - Replace `(K: Borrow<Q>, Q: KeyRef / OrderedKeyRef)` with `pub trait KeyLookup<Q: ?Sized>: KeyEnDe` and `pub trait OrderedKeyLookup<Q: ?Sized>: KeyEnDeOrdered` (`fn with_lookup_bytes<R>(q: &Q, f: impl FnOnce(&[u8]) -> R) -> Option<R>`).
   - Provide blanket `K: KeyLookup<K>` plus exact zero-cost pairs `(String, str)`, `(Box<str>, str)`, `(Vec<u8>, [u8])`, `(Box<[u8]>, [u8])`, and `([u8; N], [u8])` (checking `q.len() == N` in $O(1)$ with zero decodes or re-encodes). Delete `KeyRef::as_byte_slice` and `Mapx::query_bytes`.
3. **Stack `with_full_key` in `mmdb.rs`:**
   - Replace transient `make_full_key` calls in `MmDB::{get, insert, remove, lazy_delete}`, `MmdbReadView::get`, and `MmdbBatch::{insert, remove}` with a `[u8; 128]` stack buffer helper (`8 + key.len() <= 128`, heap fallback for large keys). Combined with `with_encoded_key` / `with_ordered_bytes`, point `get`, `contains_key`, and `remove` perform **0 heap allocations end-to-end**.

---

## 3. Engine scan path: prefix-Bloom range unification, unboxed `MmdbIter`, and ergonomic bounds

Source: [mmdb.rs](../../core/src/common/engine/mmdb.rs#L452-L681), [engine/mod.rs](../../core/src/common/engine/mod.rs#L82-L96), [staged.rs](../../strata/src/common/staged.rs#L70-L103).

### Current defects and costs

1. **SST prefix Bloom filter bypassed on every `range`, `get_le`, `get_ge`, and `MapxReadView::iter`:**
   [`mmdb_open`](../../core/src/common/engine/mmdb.rs#L1821) configures an 8-byte prefix extractor (`prefix_len: PREFIX_SIZE`), which `mmdb::DB::iter_with_prefix` uses to prune SSTs via prefix Bloom filters. Yet [`MmDB::range_at`](../../core/src/common/engine/mmdb.rs#L563-L569) calls `db.iter_with_range(...)` — which in `mmdb` skips prefix Bloom filters altogether — and then calls `db_iter.seek(lo)` a **second** time after `iter_with_range` already sought all sources. Even [`MapxReadView::iter`](../../core/src/common/engine/mod.rs#L83) delegates to `self.range(..)`, losing Bloom pruning on shared shards.
2. **Heap-boxed `dyn DoubleEndedIterator` and per-row `Vec<u8>` reallocation:**
   Every `iter`, `range`, `get_le`, and `get_ge` boxes `MmdbIter(Box::new(iter))` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L466)), dispatches every item through a vtable, re-runs `.filter(...)` on top of `DBIterator` bounds, and allocates a fresh `Vec<u8>` via `k[PREFIX_SIZE..].to_vec()` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L577)) even though `k` is already an owned `Vec<u8>`.
3. **`RangeBounds<Cow<'a, [u8]>>` friction and eager `StagedRows::scan_prefix` clones:**
   Callers of `MapxRaw::range` and `MapxOrdRawKey::range` cannot write `map.range(b"a"..b"z")` without `Cow` wrappers; `MapxOrd<String, V>::range` rejects `"a".."z"`; and [`StagedRows::scan_prefix`](../../strata/src/common/staged.rs#L80-L98) clones all staged `(k, v)` entries (including tombstones) upfront into a `VecDeque`.

### v18 design

1. **Unify all prefix scans onto `db.iter_with_prefix(&meta_prefix, &opts)`:**
   Set `ReadOptions::{iterate_lower_bound, iterate_upper_bound}` with `Bound::Excluded(lo)` converted to inclusive `lo || [0x00]` (mirroring `Bound::Included(hi)` → `hi || [0x00]` at [mmdb.rs](../../core/src/common/engine/mmdb.rs#L547-L551)). `mmdb`'s `iter_with_prefix` intersects bounds with `[meta_prefix, prefix_end)`, applies **both** range and **prefix Bloom** SST pruning, and seeks once without needing `.filter(...)`.
2. **Concrete `MmdbIter` and in-place prefix stripping:**
   Replace `Box<dyn DoubleEndedIterator<...>>` with a concrete `pub struct MmdbIter { inner: CheckedBidiIter }` and strip the 8-byte prefix in place via `k.drain(..PREFIX_SIZE)`.
3. **`RawRangeBounds`, `OrderedRangeBounds<K>`, and lazy `MergeScan`:**
   Accept `..`, `&[u8]`, `&[u8; N]`, and `Vec<u8>` bounds directly on raw maps; accept borrowed `OrderedKeyLookup<Q>` bounds on `MapxOrd::range` (`ord_map.range("a".."z")`); and store `btree_map::Range<'a, ...>` directly in `StagedRows::MergeScan`.

---

## 4. Basic collections: safe `Entry` enum, `DerefMut`-only `ValueMut`, typed `Reader`/`ReadView`, and `unsafe` cleanup

Source: [macros.rs](../../strata/src/common/macros.rs#L57-L65), [mapx_ord_rawkey/mod.rs](../../strata/src/basic/mapx_ord_rawkey/mod.rs#L92-L124), [orphan/mod.rs](../../strata/src/basic/orphan/mod.rs#L91-L208), [mapx_raw/mod.rs](../../core/src/basic/mapx_raw/mod.rs#L74-L185).

### Current defects and costs

1. **2 upfront clones/encodes + up to 2 `Drop` re-encodes on every `get_mut` / `iter_mut` / `Orphan +=`:**
   [`MapxOrdRawKey::get_mut`](../../strata/src/basic/mapx_ord_rawkey/mod.rs#L92-L106) and `ValueIterMut` eagerly serialize `snapshot = value.encode()` **and** clone `original = inner.as_slice().to_vec()` (duplicating bytes already held inside `self.inner`), then re-encode up to twice on `Drop` when `!self.dirty` — solely to auto-persist `RefCell<T>` interior mutability through shared `&V` `Deref`. Meanwhile `ValueMut`, `ValueIterMut`, `mapx::ValueIterMut`, `orphan::ValueMut`, and `dagmap::rawkey::ValueMut` duplicate the guard boilerplate.
2. **Raw-pointer `unsafe` in incomplete `Entry` APIs + dead `pub unsafe` surface:**
   `entry_or_insert_via_mock!` ([macros.rs](../../strata/src/common/macros.rs#L282-L298)) and `MapxRaw::Entry` ([mapx_raw/mod.rs](../../core/src/basic/mapx_raw/mod.rs#L846-L880)) cast `&mut Hdr` to `*mut Hdr` to work around borrow lifetimes and only expose `or_insert(V)`. `MapxOrd::insert_encoded_value` and `MapxOrdRawKey::insert_encoded_value` have **zero callers**; `MapxRaw::from_bytes_in` and `engine::Mapx::from_prefix_slice_in` have only **one legacy caller** (`DagIdAllocator::read_legacy_counter` for pre-v16 `id_num` files); and `Orphan::{is_uninitialized, initialize_if_empty}` are dead relics.
3. **Missing typed `Reader` / `ReadView`:**
   Only `MapxRaw` exposes `reader()` and `read_view()`. Concurrent readers of `Mapx`, `MapxOrd`, `MapxOrdRawKey`, and `Orphan` are forced into `unsafe { map.shadow() }`.

### v18 design

1. **Standard Rust `DerefMut`-triggered write-back:**
   Persist changes through `ValueMut` / `ValueIterMut` strictly when `DerefMut` (`&mut V`) was invoked (`dirty == true`), encoding once on `Drop` and writing back iff `is_mock || encoded != self.inner.as_slice()`. Unmodified guards perform **0 upfront encodes, 0 `original` clones, and 0 `Drop` encodes**. Unify `ValueMut<'a, V>` and `ValueIterMut<'a, V>` over one internal `RawValueSlot` trait and delete `Orphan::{is_uninitialized, initialize_if_empty}`.
2. **100% safe `enum Entry<'a, V> { Occupied, Vacant }` and `pub unsafe` pruning:**
   Read the raw value inside `entry(key)` before constructing `OccupiedEntry` or `VacantEntry` holding `&'a mut Self` — deleting all `Entry` raw-pointer `unsafe` while adding `or_insert_with`, `or_default`, `and_modify`, `OccupiedEntry::{get, get_mut, insert, remove}`, and `VacantEntry::insert`. Delete `insert_encoded_value`, `from_bytes_in`, and `from_prefix_slice_in`; demote `Orphan::shadow` and `MapxRaw::as_bytes` to `pub(crate)`.
3. **First-class `Reader` + `ReadView` across all basic collections:**
   Complete `MapxRawReader` (`contains_key`, `iter`, `range`, `get_le`, `get_ge`, `namespace`, `instance_id`) and ship `MapxReader<K, V>` / `MapxReadView<'a, K, V>`, `MapxOrdReader<K, V>` / `MapxOrdReadView<'a, K, V>`, `MapxOrdRawKeyReader<V>` / `MapxOrdRawKeyReadView<'a, V>`, and `OrphanReader<T>` (resolving [durable-batches-and-read-views.md](durable-batches-and-read-views.md) §2).

---

## 5. Handle envelopes, engine/namespace shims, and `ruc`-free structured `VsdbError`

Source: [strata/src/common/mod.rs](../../strata/src/common/mod.rs), [core/src/common/mod.rs](../../core/src/common/mod.rs), [namespace.rs](../../core/src/common/namespace.rs), [error.rs](../../core/src/common/error.rs), [mmdb.rs](../../core/src/common/engine/mmdb.rs#L871-L899).

### Current defects and costs

1. **Triple-nested handle serialization and `VSTYPE02` shims:**
   Serializing a `Mapx<K, V>` wraps `VSMAPX01` → `MapxOrdRawKey` `VSTYPE03` → `Mapx` `VSTYPE03` (6 heap allocations on write; 3 `postcard` passes + 3 `Vec<u8>` copies on read), plus `VSTYPE02` and `legacy_type_name` string-rewriting shims ([strata/src/common/mod.rs](../../strata/src/common/mod.rs#L106-L152)).
2. **Accidental 16-shard, 2 GiB default-engine initialization:**
   `ensure_alloc_init` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L882-L884)) forces `LazyLock::force(&VSDB)` on the first allocation in *any* namespace solely for pre-v16 shard-0 allocator migration, and `vsdb_flush()` ([core/src/common/mod.rs](../../core/src/common/mod.rs#L544)) unconditionally initializes `DEFAULT_NS` — opening 16 shards, 16 compaction workers, and a 256 MiB cache pool even in applications that only use non-default namespaces.
3. **`__namespace_state__` sidecars (`~8 fsync`s per `Namespace::create`):**
   Kept separate from `__namespaces__` only to avoid changing v16's positional `NsRecord`, requiring 4 atomic file writes + 4 directory `fsync`s per namespace creation ([namespace.rs](../../core/src/common/namespace.rs#L749-L767)).
4. **`ruc` SemVer leak and flattened `VsdbError::Other`:**
   `impl From<Box<dyn ruc::err::RucError>> for VsdbError` ([error.rs](../../core/src/common/error.rs#L154)) leaks `ruc` into both crates' public SemVer surface, while `mmdb.rs` boxes `std::io::Error` and `mmdb::ErrorKind::{Io, Corruption, ReadOnly, InvalidArgument}` into `VsdbError::Other { detail: String }` (`source() == None`). `VsdbError` versioning variants ([error.rs](../../core/src/common/error.rs#L33-L78)) also carry untyped `u64` instead of `BranchId` / `CommitId`.

### v18 design

1. **Flat, zero-allocation stack handle frames (`format_version = 18`):**
   - `MapxRaw`: fixed 24-byte frame `b"VSMAPX02" | prefix_le:8 | ns_id_le:8` (`0` = default namespace).
   - Simple typed wrappers (`Mapx`, `MapxOrd`, `MapxOrdRawKey`, `Orphan`): flat 32-byte frame `b"VSTYPE04" | type_tag_le:8 | prefix_le:8 | ns_id_le:8` decoded directly from `&[u8]` in `Visitor::visit_bytes` (**0 heap allocations on serialize and deserialize**). Delete `VSTYPE02` and `legacy_type_name`.
2. **Purge v15/v16 engine & namespace shims:**
   - Delete `META_KEY_PREFIX_ALLOCATOR`, shard-0 ceiling fold, `struct VsDB`, and `vsdb_meta_path(u64)` / `vsdb_get_meta_dir()`; hold `DEFAULT_NS` in a `OnceLock<Namespace>` so `vsdb_flush()` only flushes it if already initialized; unify `write_file_durable` onto `atomic_write_file`.
   - Add a `VSNREG02` header to `__namespaces__` and embed `state: NsLifecycleState` directly in `NsRecord`, deleting `__namespace_state__` sidecars and cutting `Namespace::create()` metadata writes from 4 (~8 `fsync`s) to 2 (~4 `fsync`s).
3. **Drop `ruc` and preserve structured errors + typed IDs:**
   - Remove `ruc` from workspace dependencies, return `crate::Result<T>` directly in `mmdb.rs`, implement `From<mmdb::Error> for VsdbError` mapping `Io`/`Corruption`/`ReadOnly`/`InvalidArgument` with `Error::source()` intact, and move `BranchId(u64)` / `CommitId(u64)` to `vsdb_core::common` for `VsdbError`.

---

## 6. `VerMap` and `PersistentBTree`: single-handle atomic store, sparse on-disk refcounts, and slotted zero-copy pages

Source: [versioned/map.rs](../../strata/src/versioned/map.rs), [versioned/repair.rs](../../strata/src/versioned/repair.rs), [versioned/read.rs](../../strata/src/versioned/read.rs), [persistent_btree/mod.rs](../../strata/src/basic/persistent_btree/mod.rs), [persistent_btree/nodes.rs](../../strata/src/basic/persistent_btree/nodes.rs), [persistent_btree/merge.rs](../../strata/src/basic/persistent_btree/merge.rs).

### Current defects and costs

1. **8-prefix non-atomic `VerMapPayload` + `gc_dirty` + `BranchState` churn:**
   `VerMap` persists 8 separate `MapxRaw`-backed handles ([versioned/map.rs](../../strata/src/versioned/map.rs#L244-L253)). Because engine batches are single-prefix, `commit()` executes 6 separate writes + `gc_dirty` toggles + WAL fence deletes, `new_with_main` burns ~112 prefixes to co-locate 8 handles on a 16-shard engine, and every single-key `VerMap::insert`/`remove` decodes and re-encodes `BranchState` (allocating `name: String` on every KV write) in a second batch after flushing B+ tree nodes.
2. **Unbounded `RefState` RAM and full-DB `from_meta()` scans:**
   Every live node's refcount and child list live only in volatile `RefState.counts: HashMap<NodeId, NodeRef>` ([persistent_btree/mod.rs](../../strata/src/basic/persistent_btree/mod.rs#L72)). Every `VerMap::from_meta()` — even after a clean shutdown — runs **2 full `nodes.iter()` scans + random `nodes.get` across every reachable node** (reading ~96% leaf pages off disk just to discover `children.is_empty()` because `NodeId` has no leaf tag) **+ 1 WAL `fsync`**, and pins >150 MiB of non-evictable RAM on multi-million-key trees.
3. **~130 `Vec<u8>` allocations per depth-4 `get()` and borrowed `Snapshot<'a>`:**
   Sequential `Node::decode` ([nodes.rs](../../strata/src/basic/persistent_btree/nodes.rs#L114-L138)) allocates `Vec<Vec<u8>>` for all ~24 sibling keys at every internal level and all ~24 keys **and values** at the leaf before returning 1 value. `Snapshot<'a, K, V>` ([versioned/read.rs](../../strata/src/versioned/read.rs#L37-L40)) borrows `&'a VerMap`, preventing concurrent `&mut VerMap` mutations even though `RootLease` already pins the root. `PersistentBTree` also contains the subsystem's sole `unsafe` (`self.nodes.shadow()` at [persistent_btree/mod.rs](../../strata/src/basic/persistent_btree/mod.rs#L461)), lacks multi-key `VerMap` batch writes, double-descends on `merge::replay` inserts, and buffers all of `bulk_load` in RAM.

### v18 design

1. **Single-handle tagged `VerMap` store (`1 MapxRaw`, `0` WAL fences on working edits):**

   ```text
   [0x00]                   -> Header { ver: u8, next_node: u64, next_commit: u64, next_branch: u64, main_branch: u64 }
   [0x01 | branch_id_be:8]  -> BranchHead { head: u64, dirty_root: u64 }   // fixed 16B, separated from String name
   [0x02 | branch_id_be:8]  -> branch_name UTF-8 bytes
   [0x03 | branch_name]     -> branch_id_le:8
   [0x04 | commit_id_be:8]  -> Commit { root: u64, timestamp_us: u64, ref_count: u32, parents: [u64] }
   [0x10 | node_id_le:8]    -> Slotted B+ tree node page
   [0x11 | node_id_le:8]    -> extra_refs: u32   (stored iff incoming_ptrs >= 2; value = incoming_ptrs - 1)
   [0x12 | blob_id_le:8]    -> Separated large value bytes (> 1 KiB, refcounted via [0x11])
   ```

   Working edits (`insert`, `remove`, `write_batch`, `discard`) commit node pages + `Header` + `BranchHead` in **1 atomic `batch.commit()`** (0 `String` allocations, 0 WAL fences). History mutations (`commit`, `merge`, `create_branch`, `delete_branch`, `rollback_to`) commit in **1 atomic `batch.commit_sync()`**. Delete `gc_dirty`, `colocated`, and automatic startup repair walks, keeping explicit `VerMap::verify_and_gc()`.
2. **`LEAF_BIT`-tagged `NodeId`s and sparse persisted refcounts (`O(1)` open, `O(snapshots)` RAM):**
   - Set bit 63 (`LEAF_BIT = 1 << 63`) on leaf `NodeId`s so internal child arrays identify leaves without disk I/O.
   - Maintain exact incoming-pointer counts via `1 + extra_refs[nid]`, staged per batch in an in-memory `ref_delta: HashMap<NodeId, i32>`:
     - Creating a new internal node adds `+1` to each of its children.
     - Releasing a node whose resulting count stays `>= 1` (e.g. the first COW split of a shared committed path) decrements `extra_refs` and stops — its children stay owned by the still-live old node while the new copy adds `+1` to the unchanged sibling `NodeId`s in the same batch.
     - Releasing a node whose resulting count reaches `0` (e.g. subsequent edits on an already-copied working path, or branch/commit deletion) deletes `[0x10 | nid]` and, iff `nid & LEAF_BIT == 0`, subtracts `-1` from its children (**never reading leaf pages off disk**, and canceling `-1`/`+1` to `0` in memory on unshared working paths so `bulk_load` and repeated working edits write **0 refcount rows**).
   - `VerMap::from_meta()` reads **1 `Header` row in $O(1)$**; runtime `TreeRuntime` holds only `parking_lot::Mutex<HashMap<NodeId, u32>>` pins for active in-process `Snapshot`s.
3. **Slotted zero-copy `NodeView<'a>(&'a [u8])` + value-blob separation:**
   - Encode `u32` key end-offsets and `u64` value/blob descriptors (`[is_blob:1b | offset_or_blob_id:63b]`) at the head of each node page so `get` and `contains_key` binary-search `&[u8]` slices directly inside `RawValue` with **0 `Vec` allocations**, `BTreeIter` yields borrowed slices into `K::from_slice` / `V::decode`, and values `> 1 KiB` live once in `[0x12]` blobs rather than amplifying leaf COW splits.
4. **Owned `'static` `Snapshot`, `#![forbid(unsafe_code)]`, and batch/streaming pipelines:**
   - Back `RootLease` with `MapxRawReader` (deleting `self.nodes.shadow()`), make `Snapshot<K, V>` owned (`'static + Clone + Send + Sync`), demote `PersistentBTree` to `pub(crate)`, drop `K: Clone, V: Clone` from `VerMap::clone()`, add `VerMap::clone_in(&ns)`, add `VerMap::write_batch` sharing a single-pass sorted range-grouped COW applier with `merge::replay`, and make `bulk_load` and `BTreeDiffIter` $O(\text{height})$ streaming.

---

## 7. Merkle tries (`MptCalc` and `SmtCalc`): `Arc`-COW nodes, atomic streaming checkpoints, and unified `TrieCalc`

Source: [trie/mod.rs](../../strata/src/trie/mod.rs), [trie/node/mod.rs](../../strata/src/trie/node/mod.rs), [trie/nibbles.rs](../../strata/src/trie/nibbles.rs), [trie/mpt/mutation.rs](../../strata/src/trie/mpt/mutation.rs), [trie/smt/mutation.rs](../../strata/src/trie/smt/mutation.rs), [trie/cache.rs](../../strata/src/trie/cache.rs), [trie/codec_util.rs](../../strata/src/trie/codec_util.rs), [trie/proof.rs](../../strata/src/trie/proof.rs), [audit.md](../audit.md).

### Current defects and costs

1. **`mem::take` error tree-wipe bug and O(N) `Clone` / `poison()`:**
   Both tries own `Box<Node>` trees and start mutations with `mem::take(&mut self.root)` ([mpt/mutation.rs](../../strata/src/trie/mpt/mutation.rs#L36), [smt/mutation.rs](../../strata/src/trie/smt/mutation.rs#L27)) **before `?`** — any `Err` after `take` returns early with `self.root` emptied, contradicting the documented no-data-loss guarantee. Failed `batch_update` calls leave partial edits applied, forcing `VerMapWithProof` to `poison()` the entire trie ([trie/proof.rs](../../strata/src/trie/proof.rs#L235)), while $O(N)$ `Clone` forces `VerMapWithProof::sync_to_branch` to recompute B+ tree diffs on repeated `merkle_root` calls and run `reverse_dirty()` undo passes.
2. **Per-node heap bloat and 256-deep SMT recursion:**
   `Cached(Vec<u8>, Box<Node>)` heap-allocates 32B hashes separately per node; `Node::Branch` allocates `Box<[Option<NodeHandle>; 16]>` (256+ bytes) even for 2 children; `Nibbles(Vec<u8>)` stores 1 nibble/byte on the heap and allocates 2 `Vec<u8>`s on every `split_at` level; `NodeCodec::encode` allocates a `Vec<u8>` per hashed node; and `SmtCalc` uses recursive `Drop` and 256-frame recursive walks.
3. **Non-atomic checkpoint overwrites and memory amplification (`[LOW]` in `audit.md`):**
   [`write_cache_file`](../../strata/src/trie/codec_util.rs#L104-L111) truncates the live cache in place via `File::create` (destroying the prior checkpoint on crash/`ENOSPC`), buffers up to 512 MiB in RAM, writes redundant 34B per-node hashes that `load_cache` recomputes anyway, and materializes the full graph before structural validation ([audit.md](../audit.md#L57-L60)). `VerMapWithProof` also splits `prove_key` vs `prove_mpt`, returns `Result<Vec<u8>>` instead of `[u8; 32]`, and lacks `new_in`/`clone_in`/`save_meta`/`from_meta`.

### v18 design

1. **Immutable-by-sharing `Arc` nodes + inline `OnceLock<[u8; 32]>` (preserving identical Keccak256 preimages & proof wire bytes):**
   - `Clone` becomes $O(1)$ (`Arc::clone`); edits use `Arc::make_mut` + `hash.take()`. `batch_update` clones `prev = self.root.clone()` in $O(1)$ and restores on `Err` — **100% atomic and unwind-safe**, deleting `mem::take`, `TrieMut`/`SmtMut`, `reverse_dirty()`, and `poison()`.
   - `VerMapWithProof` retains `committed_trie: T` and forks `working_trie` in $O(1)$ only when `working.root != synced_working_root`.
   - Compact `MptNode::Branch` to `(bitmap: u16, children: Box<[Arc<MptNode>]>, value: Option<Box<[u8]>>)` indexed via `(bitmap & ((1 << idx) - 1)).count_ones()` (3–5× lower RAM), pack `Nibbles` inline up to 64 nibbles, stream `NodeCodec` directly into `Keccak256::update`, and convert `SmtCalc` walks/`Drop` to iterative stacks.
2. **Atomic post-order streaming checkpoints (`MPTC v2` / `SMTC v4`):**
   - Stream post-order records via `BufWriter` to a sibling temp file + `sync_all` + atomic rename, omitting redundant per-node stored hashes (-40–60% checkpoint size) and validating canonical shape + depth/node budgets **online** during `BufReader` parsing (closing [audit.md](../audit.md#L57-L60) and capping I/O buffer RAM at 64 KiB).
3. **Unified `TrieCalc` and `VerMapWithProof` lifecycle:**
   - Add `type Proof` + `prove` / `verify_proof` on `TrieCalc` (`prove(&self, key)` computing any uninitialized `OnceLock` hashes on demand), return `[u8; 32]` from `root_hash` / `merkle_root`, unify `VerMapWithProof` proof methods into one generic `impl<K, V, T: TrieCalc>`, and add `new_in`, `clone_in`, `save_meta`, `from_meta`, `instance_id`, and `namespace`.

---

## 8. `DagMap`: single-handle shard-colocated nodes and typed `DagMap<K, V>`

Source: [dagmap/mod.rs](../../strata/src/dagmap/mod.rs), [dagmap/raw/mod.rs](../../strata/src/dagmap/raw/mod.rs), [dagmap/rawkey/mod.rs](../../strata/src/dagmap/rawkey/mod.rs), [audit.md](../audit.md).

### Current defects and hazards

1. **Scattered 3-handle nodes and 80 shard WAL syncs per `prune()`:**
   [`DagMapRaw`](../../strata/src/dagmap/raw/mod.rs#L86-L96) holds `{ data: MapxRaw, parent: Orphan<Option<DagMapRaw>>, children: MapxOrdRawKey<DagMapRaw> }`. `build()` leaves `data` un-colocated (`MapxRaw::new()` at [raw/mod.rs](../../strata/src/dagmap/raw/mod.rs#L278)) and `Clone` drops co-location completely, forcing `prune_mainline` to issue **5 full-namespace `try_sync_wal()` fences** (= 80 synchronous WAL fence deletes + `fsync`s across 16 shards).
2. **Serde tuple decomposition + root-wipe debt (`[MEDIUM]` in `audit.md`), 7 `unsafe` blocks, and `DagIdAllocator`:**
   Bare 3-tuple serialization allows safe callers to deserialize and mutate `parent` into a cycle or plant a live root (`parent == None`) into `children` where `owned_or_residue` ([raw/mod.rs](../../strata/src/dagmap/raw/mod.rs#L669-L674)) wipes it. Storing `DagMapRaw` inside `Orphan`/`MapxOrdRawKey` requires **7 internal `unsafe` blocks**, a separate global `DagIdAllocator` (`dag_id_ceiling`), $O(\text{siblings})$ registry scans on `destroy()`, and 3 handle decodes + 3 namespace lookups per ancestor hop on `get()`.
3. **Empty-value `[]` panics and broken typed surface:**
   Using `[]` as deletion tombstone causes `insert(k, [])` and `DagMapRawKey<()>::insert` to panic; `DagMapRawKey<V>::new(Option<&mut DagMapRaw>)` ([rawkey/mod.rs](../../strata/src/dagmap/rawkey/mod.rs#L103)) forces callers into `into_inner()` or `unsafe { shadow_inner() }`; and typed `DagMap<K, V>` is missing.

### v18 design

1. **One shard-colocated `MapxRaw` per node (`pub struct DagMapRaw { store: MapxRaw }`):**
   Keeping one `MapxRaw` per node preserves per-node `InstanceId` / `save_meta()` and $O(1)$ `batch_wiped()` range-tombstones on `destroy()`, while creating every child via **`parent.store.new_colocated()`** places the entire connected DAG onto **one shard WAL** (**0 cross-shard WAL fences** in `prune()` and `destroy()`):

   ```text
   [0x00]                       -> NodeMeta: Root (0x00) | Child(parent_map_id: u64 LE) (0x01)
                                             | Clearing { parent: Option<u64>, genesis: u64 } (0x02)
                                             | Destroyed (0x03)
   [0x01 | child_map_id_be:8]   -> []                  // child's own globally monotonic prefix ID
   [0x02 | user_key]            -> [0x01 | val] | [0x00]  // live value vs deletion tombstone
   ```

2. **Opaque `VSTYPE04` handle, zero `unsafe`, and complete typed hierarchy:**
   - Add a safe `MapxRaw::open_in_same_ns(&self, map_id: u64) -> Result<MapxRaw>` helper (validating `reserve_recovered_prefix` and cloning `self.ns` with zero registry locks and zero `unsafe`).
   - Serialize `DagMapRaw` opaquely via `VSTYPE04`; `owned_or_residue` matches `Child(p) if p == owner` or `Clearing`/`Destroyed` — never `Root` (closing [audit.md](../audit.md#L36-L40)).
   - Delete `DagIdAllocator`, `DagMapId`, and **all 7 `unsafe` blocks**; check local `self.store.get` before allocating cycle-detection state; support empty `[]` / `()` values natively; fix `DagMapRawKey<V>::new(parent: Option<&mut Self>)`; add `clone_in(&ns)`; and ship **`DagMap<K, V>`** (`K: KeyEnDe, V: ValueEnDe`).

---

## 9. `VecDex`: unified stateless handles, raw LE vectors, and zero-allocation search

Source: [vecdex/mod.rs](../../strata/src/vecdex/mod.rs), [vecdex/dynamic.rs](../../strata/src/vecdex/dynamic.rs), [vecdex/hnsw.rs](../../strata/src/vecdex/hnsw.rs), [vecdex/distance.rs](../../strata/src/vecdex/distance.rs), [audit.md](../audit.md).

### Current defects and costs

1. **Stale 34-byte `GraphState` corruption on restored handles (`[MEDIUM]` in `audit.md`) and torn multi-read searches:**
   [`VecDex`](../../strata/src/vecdex/mod.rs#L364) caches `GraphState` (5 scalars, 34 bytes) by value, so an independently restored handle holds a stale `next_node_id` and overwrites live nodes on sequential writes, while [`search_internal`](../../strata/src/vecdex/mod.rs#L902-L996) executes 50–500 separate `self.store.get` calls without a `MapxRawReadView`.
2. **392 lines of duplicated `VecDexDyn` (`dynamic.rs`):**
   [`DistanceMetric::distance(a, b)`](../../strata/src/vecdex/distance.rs#L101) lacks `&self`, preventing `MetricKind` (and parameterized metrics like `Lp(p)` or truncated dims) from implementing `DistanceMetric` and forcing [`dynamic.rs`](../../strata/src/vecdex/dynamic.rs) to duplicate the entire `VecDex` API.
3. **`postcard` vector serialization, per-visited-node `Rc<Vec<S>>` retention, and split node-meta rows:**
   Vectors are encoded via `postcard::to_allocvec::<Vec<S>>`; `search_internal` allocates `Vec<u8>` + `Vec<S>` + `Rc<Vec<S>>` into a `HashMap` for up to 4,096 visited nodes (~24 MiB for `dim = 1536`) even though `visited: HashSet<u64>` visits each layer-0 node once; `compact()` peaks at 3× dataset RAM because `Txn::vec_cache` is unbounded; and splitting `TAG_INFO` + `TAG_NODE2KEY` forces a redundant `K: ValueEnDe` bound.

### v18 design

1. **Stateless handles over fixed 34-byte `[TAG_STATE]` + `MapxRawReadView`:**
   - Persist `GraphState` as a fixed 34-byte LE record at `[TAG_STATE]` (keeping only query `ef_search` override optional on the handle) and execute every `search_internal` / `iter` over a single `let view = self.store.read_view();`.
   - Restored handles become **always coherent** for sequential writes and concurrent snapshot reads (closing [audit.md](../audit.md#L22-L26) for `VecDex` without any cross-handle mutable cache), unlocking deep-copy `Clone`, `clone_in(&ns)`, and `VecDexReader`.
2. **Unify `VecDex` and `VecDexDyn` via `&self` on `DistanceMetric<S>`:**
   - Change `DistanceMetric<S>::distance(&self, a: &[S], b: &[S]) -> S` to take `&self`, store `metric: D` in `VecDex<K, D = L2, S = f32>` (`VecDex::new(config)` where `D: Default`, plus `VecDex::with_metric(metric, config)`), implement `DistanceMetric<S> for MetricKind` with frozen wire tags, delete `dynamic.rs`, and define `pub type VecDexDyn<K, S = f32> = VecDex<K, MetricKind, S>;`.
3. **Raw LE IEEE-754 vectors, zero-alloc search distance, merged `TAG_NODE`, and bounded `Txn::vec_cache`:**
   - Store vectors at `[TAG_VEC | node_id_be]` as raw fixed-width LE bytes (`dim * size_of::<S>()`) — **keeping `TAG_VEC` separate from `TAG_ADJ`** so `2 × m_max0 = 64` neighbor-list updates per `insert` never rewrite multi-KB vectors (avoiding an ~18× WAL write-amplification regression).
   - Compute `D::distance` directly from `&raw_bytes` / a single reusable `dim`-element scratch slice in `search_layer` (**0 heap allocations per visited node**).
   - Merge `TAG_INFO` + `TAG_NODE2KEY` into `[TAG_NODE | node_id_be] -> [max_layer: u8 | KeyEnDe::encode_key(&k)]`, dropping `K: ValueEnDe` (`K: KeyEnDe + Clone + Eq`) and cutting per-node meta rows by 25%. Cap `Txn::vec_cache` (e.g. 32 MiB FIFO/clock budget) during `insert_batch` and `compact()`.

---

## Preserved invariants and non-goals

Every v18 refactoring preserves VSDB's core architectural boundaries ([compatibility-policy.md](../ai/compatibility-policy.md), [namespaces.md](namespaces.md), [performance-roadmap.md](performance-roadmap.md)):

- **Shard routing and universe model:** Default namespace stays at 16 shards (`NUM_SHARDS`); writable access remains single-process per universe; `parking_lot` remains the sole lock implementation.
- **Deep-copy `Clone` vs explicit `.reader()`:** `Clone` on every persistent collection (`Mapx*`, `Orphan`, `VerMap`, `DagMap*`, `SlotDex`, `VecDex`) deep-copies storage into a fresh prefix (`MptCalc`/`SmtCalc` `Arc`-COW `Clone` preserves identical value-isolation semantics in memory). Read-only storage sharing uses explicit `.reader()` / `.read_view()`, never `Clone`.
- **No hidden cross-handle mutable caches:** `VecDex` becomes stateless on disk-authoritative `[TAG_STATE]`; `SlotDex` detects external mutation deterministically via `[TAG_TOTAL]`'s `generation` counter rather than hiding mutable caches behind global registries.
- **Cryptographic wire parity:** `MptCalc` and `SmtCalc` node encodings, Keccak256 preimages, root hashes, and proof transport bytes remain bit-for-bit identical to v17.
- **Clean format break (`format_version = 18`):** Opening a pre-v18 data directory fails fast with a clear `VsdbError::Corrupt` / version-mismatch diagnostic rather than attempting partial in-place shims; cross-major upgrades migrate via offline logical export/import (`iter` → `batch` / `insert_batch` / `bulk_load`).
- **Non-goals:** No cross-namespace or arbitrary cross-collection interactive transactions; no implicit per-insert `fsync` on ordinary writes.

---

## Recommended v18 implementation sequence

Execute in dependency order, landing each unit as an independently tested commit series on the v18 integration branch:

1. **Core foundation (`vsdb_core`):** Drop `ruc`, type `VsdbError` (`BranchId`/`CommitId`, structured `From<mmdb::Error>`), bump `SUPPORTED_FORMAT_VERSION = 18`, land flat `VSMAPX02` + `VSNREG02` + lazy `OnceLock` `DEFAULT_NS` + `MapxRaw::open_in_same_ns`, and unify `mmdb.rs` onto `iter_with_prefix` + unboxed `MmdbIter` + stack `with_full_key` + `RawRangeBounds` + expanded `MapxRawReader`.
2. **Codec & basic collections (`vsdb` common/basic):** Land 2-trait `KeyEnDe`/`ValueEnDe` (`Eq + Hash` blanket, stack/buffer methods, `KeyLookup`/`OrderedKeyLookup`, flat `VSTYPE04`), `DerefMut`-only `ValueMut`/`ValueIterMut`, safe `enum Entry`, dead `pub unsafe` deletions, and typed `Reader` + `ReadView` wrappers.
3. **`SlotDex`:** Land `[u8; N]` `SlotType::Bytes`, `tier_capacity: u32`, zero-alloc helpers, tier-guided `Order::Desc` `iter`, common-ancestor `count`, and `[total, generation]` + `MapxRawReadView` (+ remove the `[CRITICAL] slotdex` entry from [audit.md](../audit.md)).
4. **`DagMap`:** Land single-handle shard-colocated `DagMapRaw`, delete `DagIdAllocator` and all 7 `unsafe` blocks, fix `DagMapRawKey::new`, add `DagMap<K, V>` (+ remove `[MEDIUM] dagmap` from [audit.md](../audit.md)).
5. **`VecDex`:** Land stateless 34B `[TAG_STATE]` + `MapxRawReadView`, `&self` `DistanceMetric` unifying `VecDexDyn`, raw LE vectors, zero-alloc search, merged `TAG_NODE`, and bounded `Txn::vec_cache` (+ update `[MEDIUM] cached indexes` in [audit.md](../audit.md)).
6. **`PersistentBTree` + `VerMap`:** Land slotted `NodeView` + `LEAF_BIT` + sparse `extra_refs`, single-handle tagged `VerMap` store, `#![forbid(unsafe_code)]` owned `'static` `Snapshot`, `VerMap::write_batch` + range-grouped `merge::replay`, and streaming `bulk_load` / `BTreeDiffIter`.
7. **`TrieCalc` + `VerMapWithProof`:** Land `Arc`-COW `MptCalc`/`SmtCalc` + POPCNT branches + inline `Nibbles`, atomic post-order streaming checkpoints (`MPTC v2` / `SMTC v4`), and unified `TrieCalc::Proof` + `VerMapWithProof` lifecycle APIs (+ remove `[LOW] trie` from [audit.md](../audit.md)).
