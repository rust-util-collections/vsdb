# VSDB v18 Structural Refactoring Roadmap

**Implementation amendment.** v18 opens only the current data format. The independent workspace crate `vsdb_migrate` owns all v17 decoding and conversion, including raw and typed envelopes, root metadata, old index state and application-embedded handles. This supersedes the original backward-reader and lazy-upgrade design throughout this proposal. The migration guide documents the callable entry points. [Migration guide](../../migration/README.md)

Baseline: `4fc5b88` (`v17.0.21`), verified against `mmdb 4.3.14`/`4.3.15` (`>= 4.3.12` in `Cargo.toml`).

The subsystem measurements cover [basic maps](../evaluations/v18-basic.md), [SlotDex and DagMap](../evaluations/v18-slotdex-dagmap.md), [VerMap reference persistence](../evaluations/v18-versioned.md), and [tries and VecDex](../evaluations/v18-trie-vecdex.md). Those initial reports validated the primary happy-path wins, but left critical workload regimes — `> 1 KiB` `BLOB_LEAF` mutations/diffs, deep-tree `VerMap` read/reopen scaling, incremental working-state `VerMapWithProof` syncs, `VecDex` construction/selective-filter/compaction paths, `SlotDex` hot-bucket batch staging, and single-shard `mmdb 4.3.15` memtable flush stalls — unevaluated or partially realized; [§10](#10-post-implementation-structural-performance-evaluation-and-follow-up-designs) records the post-implementation audit, empirical measurements on `cc2f41c`, and concrete follow-up designs. The initial v18 design and its rationale follow.

## Purpose, release boundary, and pre-v18 (`v17.x`) split

`v16` and `v17` introduced namespaces, in-process engine teardown, per-engine block-cache pools, single-handle crash-atomic `SlotDex` and `VecDex` layouts, `PersistentBTree` versioning, synchronous batches, and raw read views — while preserving backward compatibility with `v15`/`v16` shims, multi-handle composite payloads, and unconstrained trait bounds.

Following [compatibility-policy.md](../ai/compatibility-policy.md) (*break only when necessary or compatibility cost is disproportionate; keep reading older handle envelopes where data rows are unchanged*) and [performance-roadmap.md](performance-roadmap.md) (*benchmark before freezing format or public API changes*), work is split into two tracks:

### Immediate `v17.x` non-breaking track (do not hold for `v18.0.0`)

1. **`[CRITICAL]` `SlotDex` runtime width guard:** Check `slot.to_bytes().len() == self.slot_wire_len` (`S::MIN.to_bytes().len()`) in `insert`, `insert_batch`, and `remove`, turning silent custom-slot corruption ([audit.md](../audit.md#L14-L18)) into `Err(VsdbError::...)` for one integer comparison before v18 makes variable-width slots unrepresentable at compile time.
2. **`MmDB::range_at` prefix-Bloom & single-seek fix:** Switch `MmDB::range_at` from `db.iter_with_range` + redundant `db_iter.seek` to `db.iter_with_prefix` with `ReadOptions` bounds.
3. **`VecDex` per-operation `[TAG_STATE]` load:** Re-read the existing `[0x05]` state row on operations so sequentially restored `v17` handles stop clobbering `next_node_id`.
4. **Atomic trie checkpoint writes:** Route `MptCalc`/`SmtCalc` checkpoint saves through `atomic_write_file` (temp file + `sync_all` + `rename` + parent-dir `sync_all`) instead of `File::create` in-place truncation.
5. **`DagMapRaw` `data` co-location:** Co-locate `DagMapRaw.data` with `parent` (`new_colocated` / `clone_colocated`) in `build()` and `Clone`.
6. **Format-neutral `VerMap` / `PersistentBTree` additions:** Ship `VerMap::write_batch`, single-descent `merge::replay`, streaming `bulk_load`, and `MapxRawReader`-backed owned `'static` `Snapshot` without altering node or commit wire formats.

### `v18.0.0` major-version scope

`v18.0.0` reserves breaking API and on-disk changes for structural debts that cannot be cleanly solved in `v17.x`, while **preserving existing prefixes (`InstanceId`) and continuing to read `VSMAPX01`/`VSTYPE03` envelopes** wherever underlying KV data rows are byte-identical:

| # | Subsystem | Planned v18 refactoring | Audit / legacy debt closed | Primary win | Break scope |
|---|-----------|-------------------------|----------------------------|-------------|-------------|
| 1 | `SlotDex` | Sealed `[u8; N]` `SlotType::Bytes`, `tier_capacity: u32`, bounded forward-chunked `iter`, `[total, generation]` check | Closes `[CRITICAL] slotdex` ([audit.md](../audit.md)) at compile time | Variable-width bug unrepresentable; 0 temp slot allocs; bounded-memory reverse iter | Public API + meta upgrade |
| 2 | `ende` + `Mapx*` | 2-trait `KeyEnDe`/`ValueEnDe` (`Eq + Hash` key blanket), stack `with_encoded_key`/`with_ordered_bytes`, `(K, Q)` lookup traits, stack `with_full_key` | Removes `Mapx::query_bytes` 4-alloc shim, `usize`/`isize` non-portability, `f32`/`f64`/`HashMap` key footguns | 0 temporary VSDB key allocs on short keys (`<= 120B`); compile-time rejection of float/`HashMap` keys | Public API + `usize` keys |
| 3 | `Engine` (`mmdb`) | Unbox `MmdbIter`, in-place prefix strip, conflict-free `RawRangeBounds` / `OrderedRangeBounds<K>`, `MapxRawBatch::delete_range` | Eliminates `Box<dyn>` vtable + per-row key `Vec` realloc + `Cow` bound friction | 0 `Box<dyn>` / key reallocs; `map.range(1u64..10)`, `map.range("a".."z")`, `map.range(..)` all compile | Public API |
| 4 | Basic collections | Safe `enum Entry`, `DerefMut`-only `ValueMut` (with debug interior-mutability assert) + `update`, minimal `*Reader` + `*ReadView`, dead `pub unsafe` removal | Completes [durable-batches-and-read-views.md](durable-batches-and-read-views.md) §2; removes all `Entry` + `PersistentBTree` `unsafe` | 0 upfront clones/encodes on `get_mut`/`iter_mut`; `#![forbid(unsafe_code)]` on B+ tree & versioning | Public API |
| 5 | `core` / envelopes / errors | Atomic `VSUNI018` universe header + per-engine `.shards_ready` + `LOCK`, unified `VSTYPE04` header (keeping `VSMAPX01`/`VSTYPE03` readers), fail-closed `VSNREG02`, lazy `DEFAULT_NS`, drop `ruc` | Removes `VSTYPE02`, v15 allocator fold, `__namespace_state__` sidecars, public `RucError` SemVer leak | Eliminates intermediate handle allocs; `4 → 2` registry writes on `Namespace::create`; wrong-root `relocate` guard | Public API + root metadata |
| 6 | `VerMap` + `PersistentBTree` | Single-handle big-endian tagged store, slotted `NodeView`, benchmark-gated ref persistence (`[0x11]` sparse `extra_refs` vs verified checkpoint), lock-free-across-fsync snapshot retirement | Removes 8-prefix `VerMapPayload`, `gc_dirty`, ~128 sibling `Vec` allocs per `get()` | **1 batch** on working edits / **1 `commit_sync()`** on commits; **0 sibling `Vec` allocs**; keeps `PersistentBTree` public | Public API + `VerMap` layout |
| 7 | `MptCalc` + `SmtCalc` | `Arc`-COW nodes + inline `OnceLock<[u8; 32]>`, POPCNT branches, inline-64 + heap `Nibbles`, map-bound streaming checkpoints, unified `TrieCalc` | Eliminates `VerMapWithProof::poison()`; closes `[LOW] trie` cache amplification ([audit.md](../audit.md)) | $O(1)$ `Clone` & atomic batch rollback; ~1.2–1.9× lower MPT RAM; ~34B/node smaller crash-safe checkpoints | Public API + cache files |
| 8 | `DagMap` | 1 co-located `MapxRaw` per node, `dag_id`-authenticated `NodeMeta` + local `edge_id`s, fold-before-`Clearing` crash order, typed `DagMap<K, V>` | Closes `[MEDIUM] dagmap` serde decomposition ([audit.md](../audit.md)); deletes all **7 `unsafe` blocks** + `DagIdAllocator` | `80 → 1` single-shard WAL sync on `prune()`; preserves `child_ids` across `Clone`; supports empty `[]`/`()` | Public API + `DagMap` layout |
| 9 | `VecDex` | Stateless handle over `[TAG_META]` + monotonic 34B `[TAG_STATE]`, lock-free `get` + snapshot-consistent `search`/`iter`, `&self` `DistanceMetric` (merging `VecDexDyn`), LE float slices | Closes `[MEDIUM] cached indexes` ([audit.md](../audit.md)) for `VecDex`; deletes 392-line `dynamic.rs` | Coherent restored handles & lock-free `get` across `clear()`; 0 per-node `Rc<Vec<S>>` search allocs; drops `K: ValueEnDe` | Public API + `VecDex` transcode |

---

## 1. `SlotDex`: compile-time fixed-width `SlotType` (`[u8; N]`), `tier_capacity: u32`, and bounded reverse scans

Source: [slot_type.rs](../../strata/src/slotdex/slot_type.rs), [slotdex/mod.rs](../../strata/src/slotdex/mod.rs), [audit.md](../audit.md).

### Current defects and structural friction

1. **Variable-width custom slot corruption (`[CRITICAL]` in `audit.md`, mitigated in `v17.x`, eliminated in `v18`):**
   [`SlotType`](../../strata/src/slotdex/slot_type.rs#L13-L51) requires [`KeyEnDeOrdered`](../../strata/src/common/ende.rs#L321-L337) (`to_bytes(&self) -> Vec<u8>`), allowing variable-width encodings. [`entry_key`](../../strata/src/slotdex/mod.rs#L155-L163) concatenates `[0x00 | slot.to_bytes() | k.to_bytes()]` without framing, while [`decode_entry_row`](../../strata/src/slotdex/mod.rs#L248-L255) and [`iter`](../../strata/src/slotdex/mod.rs#L1462-L1465) slice at `S::MIN.to_bytes().len()`, and [`entry_lower_bound`](../../strata/src/slotdex/mod.rs#L203-L226) / [`entry_upper_bound`](../../strata/src/slotdex/mod.rs#L229-L246) use fixed-width `prefix_successor`. Variable-width `SlotType` implementations silently drop colliding `(slot, key)` pairs, misread or panic on decoded keys, and bleed across prefix-related slot bounds.
2. **Why variable-width byte-stuffing is the wrong v18 design:**
   Order-preserving escaping (`0x00 -> 0x00 0xFF` + `0x00 0x00` terminator) inflates big-endian integer slots by `2 + count(0x00)` bytes (+24–59% larger entry keys on `SlotDex64<u64>`, up to +18B on `u128` across WAL, memtable, and uncompressed `BlockCachePool` pages) and forces per-row delimiter scans. Genuinely variable-length types (`String`, `Vec<u8>`) cannot satisfy `SlotType`'s uniform `cap^level` bucket division (`floor_align`) or bitwise complement (`!slot`) anyway — composite string indexing belongs in `MapxOrd`.
3. **Conflated `tier_capacity: S` and dead trait methods:**
   `tier_capacity` ([slotdex/mod.rs](../../strata/src/slotdex/mod.rs#L108)) is a dimensionless tree fan-out (`>= 2`), not a slot coordinate. Typing it as `S` forced `checked_pow` and `as_i128` onto `SlotType` (all 4 `as_i128` callers in `mod.rs` inspect `tier_capacity`, never a slot), while `max_val`, `saturating_add`, and `as_u64` ([slot_type.rs](../../strata/src/slotdex/slot_type.rs#L35-L45)) have **zero callers** in the workspace. Missing `Copy` also forces 25+ `.clone()` calls on slot scalars.
4. **Hot-path `Vec<u8>` churn:**
   Because `KeyEnDeOrdered::to_bytes` allocates a `Vec<u8>`, a single 3-tier `insert` allocates 6 temporary slot `Vec<u8>`s; [`slot_entry_cnt`](../../strata/src/slotdex/mod.rs#L890-L895) allocates 2 `Vec<u8>`s to read an 8-byte counter; and [`decode_entry_row`](../../strata/src/slotdex/mod.rs#L248-L255) allocates **3 `Vec<u8>`s per paged entry** (`S::MIN.to_bytes()`, `slot` decoded and discarded via `.1`, and `key` `.to_vec()` instead of `K::from_slice`).
5. **Physical-reverse `iter` overhead and unbounded `level0_walk_desc` chunk buffering:**
   As noted at [`level0_walk_desc`](../../strata/src/slotdex/mod.rs#L897-L900), an `mmdb` reverse iterator costs ~two orders of magnitude more per iterator than forward streaming: [`SlotDex::iter`](../../strata/src/slotdex/mod.rs#L1446-L1459) opens one reverse `level0_range` iterator (`next_back()` per populated slot) plus a separate forward `entry_range(slot..=slot)` iterator for every populated slot whenever `(order == Order::Desc) ^ swap_order` is true. Meanwhile, `page()`'s [`level0_walk_desc`](../../strata/src/slotdex/mod.rs#L967-L975) buffers an entire level-1 bucket's L0 rows in memory — which for a large `tier_capacity` (e.g. `1_000_000`) with no level 1 yet can buffer hundreds of thousands of count rows on a small page query — and [`entry_cnt_within_two_slots`](../../strata/src/slotdex/mod.rs#L1338-L1354) walks from `S::MIN` twice using signed `i128`.

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
   - Change `tier_capacity` in `SlotDex::new` / `new_in` / persisted `VSTYPE04` handle metadata (`LAYOUT_VERSION = 3`) to `u32` (`>= 2`). Delete `max_val`, `saturating_add`, `as_u64`, `as_i128`, and `checked_pow`.
2. **Single storage path and zero-allocation slot/key helpers:**
   - Entry rows stay byte-identical to `v17` (`[0x00 | slot: [u8; N] | key_be]`) with zero escaping and zero runtime width checks.
   - Replace `decode_entry_row` across `page` and `iter` with `K::from_slice(&raw_key[1 + S::Bytes::LEN..])`; encode `level_prefix` as `[u8; 2]` on the stack; look up `slot_entry_cnt` via a `[u8; 34]` stack slice when `S::Bytes::LEN <= 32`; and decode `decode_level_row` into `S::Bytes::zeroed()` on the stack.
3. **Bounded-memory physical-reverse `page` / `iter` and common-ancestor `count`:**
   - Select the forward vs descending engine path by **physical** storage order (`(order == Order::Desc) ^ self.swap_order`).
   - Bound `level0_walk_desc` so both descending `page` and descending `iter` split oversized L1 spans into sub-ranges rather than buffering unbounded L0 rows when `tier_capacity` is large or level 1 is absent, and coalesce contiguous slots into a single forward `entry_range` scan **only while both `sum(cnt) <= 256` and buffered raw key bytes `<= 16 KiB`**, streaming larger slots individually so `.take(k)` has a hard memory cap regardless of `tier_capacity` or key size.
   - Compute `count(slots)` from the lowest unshared tier bucket between `lo` and `hi` using unsigned `u64` arithmetic, and inline the 4 legacy `pub(crate)` `get_entries_by_page*` / `total_by_slot` / `entry_cnt_within_two_slots` helpers.
4. **Generation-checked sequential alias detection + snapshot-consistent reads:**
   - Store `[total: u64 LE, generation: u64 LE]` (16 bytes) at `[TAG_TOTAL]` (continuing to accept legacy 8-byte `[total]` rows from in-place `v17` datasets as `generation = 0` until the first `v18` write upgrades the row to 16 bytes). Increment `generation` (`checked_add`, erroring on `u64::MAX`) on every committed mutation batch — **including `clear()`**, which stages `put(TOTAL_KEY, [0, gen + 1])` inside its `batch_wiped()` commit rather than deleting `TOTAL_KEY`.
   - Keep single-active-writer (`&mut SlotDex`) ownership (no hidden global mutable cache, per [performance-roadmap.md](performance-roadmap.md) §四.3): `hydrate`, `page`, `count`, and `iter` each capture one `MapxRawReadView` (stored by value inside `SlotDexIter<'a, S, K>` alongside `MmdbIter`; note under `mmdb 4.x` acquiring `db.snapshot()` briefly locks `DB::inner`, see §9.1). Before staging a write or serving a cached read, verify `[TAG_TOTAL]`'s `generation == self.generation`: a stale sequential writer returns `VsdbError` rather than corrupting counts, while a stale reader re-hydrates from its captured `MapxRawReadView`. Add deep-copy `Clone` (resetting `generation = 0` on the fresh copy) and `clone_in(&ns)`.

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
   - Collapse the 6 traits into `pub trait KeyEnDe: Eq + Sized` (`encode_key_into`, `with_encoded_key`, `encode_key`, `decode_key`) and `pub trait ValueEnDe: Sized` (`encode_value_into`, `encode_value`, `decode_value`). **Explicitly removed public items:** `KeyEn`, `KeyDe`, `ValueEn`, `ValueDe`, `KeyRef`, `OrderedKeyRef`, and `try_encode*` (`KeyEn::try_encode_key`, `ValueEn::try_encode_value`).
   - Tighten the blanket impl to `impl<T: Serialize + DeserializeOwned + Eq + Hash> KeyEnDe for T`. Because `f32`, `f64`, `HashMap`, and `HashSet` do not implement `Eq + Hash`, standard float/hash-collection keys fail at compile time while ergonomic derived key structs keep working out of the box. Document the remaining canonical-byte law (`a == b ⇔ encode_key(a) == encode_key(b)`) for types with custom `PartialEq` or `Serialize` logic.
   - Implement `with_encoded_key` using `postcard::to_slice` into a `[u8; 128]` stack buffer (including varint-prefixed borrowed `str`/`[u8]` lookups up to ~120B), falling back to `postcard::to_allocvec` only on `SerializeBufferFull`.
   - On `KeyEnDeOrdered`, require `encode_ordered_into(&self, dst: &mut Vec<u8>)` and `with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R` (passing `&[u8]` zero-copy for `String`/`Vec<u8>`/`Box<[u8]>` and stack `[u8; size_of::<T>()]` for scalars). Remove `usize` and `isize` across scalars, `Vec`, `Box<[_]>`, and `[_; N]`, fix `Box<[$int]>`, and replace `impl_array!(1..=128)` with const-generic `impl<const N: usize> KeyEnDeOrdered for [$int; N]` blocks.
2. **Separate unordered `KeyLookup<Q>` (`Mapx`) and ordered `OrderedKeyLookup<Q>` (`MapxOrd`):**
   - `Mapx<K, V>` (`K: KeyEnDe`) bounds exact point operations (`get`, `get_mut`, `contains_key`, `remove`, `entry`) on **`K: KeyLookup<Q>`** (`Q: ?Sized`), whereas `MapxOrd<K, V>` (`K: KeyEnDeOrdered` — which intentionally does **not** require `K: KeyEnDe`, e.g. `[u8; 64]`) bounds exact point operations, `get_le`, and `get_ge` on **`K: OrderedKeyLookup<Q>`**.
   - Provide blanket `K: KeyLookup<K>` and `K: OrderedKeyLookup<K>`, plus `(String, str)`, `(Box<str>, str)`, `(Vec<u8>, [u8])`, and `(Box<[u8]>, [u8])`.
   - For fixed-size byte arrays `[u8; N]`: implement `KeyLookup<[u8]>` on `[u8; N]` (`where [u8; N]: KeyEnDe`, i.e. `N <= 32`) and exact-match `OrderedKeyLookup<[u8]>` on `[u8; N]` (returning `None` in $O(1)$ without engine I/O when `q.len() != N` on `get`/`get_mut`/`contains_key`/`remove`), while ordered boundary methods (`get_le`, `get_ge`, `range`) use `OrderedBoundEndpoint<E>` (§3.3) which accepts `[u8; N]` and `&[u8; N]` (rejecting unsized `&[u8]` so short prefix slices cannot be confused with full keys; raw prefix bounds remain explicit on `MapxOrdRawKey`). Delete `KeyRef::as_byte_slice` and `Mapx::query_bytes`.
3. **Stack `with_full_key` in `mmdb.rs`:**
   - Replace transient `make_full_key` calls in `MmDB::{get, insert, remove, lazy_delete}`, `MmdbReadView::get`, and `MmdbBatch::{insert, remove}` with a `[u8; 128]` stack buffer (`8 + key.len() <= 128`, heap fallback for longer keys). This eliminates **all temporary VSDB key-encoding and prefix-concatenation heap allocations** for keys `<= 120` bytes (`mmdb::DB::get` still allocates the returned value `Vec<u8>` on hits, and `mmdb::DB::delete` copies the key into its internal `WriteBatch`).

---

## 3. Engine scan path: prefix-Bloom range unification, unboxed `MmdbIter`, and coherence-safe bounds

Source: [mmdb.rs](../../core/src/common/engine/mmdb.rs#L452-L681), [engine/mod.rs](../../core/src/common/engine/mod.rs#L82-L96), [staged.rs](../../strata/src/common/staged.rs#L72-L185).

### Current defects and costs

1. **SST prefix Bloom filter bypassed on every `range`, `get_le`, `get_ge`, and `MapxReadView::iter` (shippable in `v17.x`):**
   [`mmdb_open`](../../core/src/common/engine/mmdb.rs#L1821) configures an 8-byte prefix extractor (`prefix_len: PREFIX_SIZE`), which `mmdb::DB::iter_with_prefix` uses to prune L0 SSTs via prefix Bloom filters and pass prefix/range hints to higher levels. Yet [`MmDB::range_at`](../../core/src/common/engine/mmdb.rs#L563-L569) calls `db.iter_with_range(...)` — skipping prefix Bloom filters — and calls `db_iter.seek(lo)` a **second** time after `iter_with_range` already sought all sources.
2. **Heap-boxed `dyn DoubleEndedIterator` and per-row `Vec<u8>` reallocation:**
   Every `iter`, `range`, `get_le`, and `get_ge` boxes `MmdbIter(Box::new(iter))` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L466)), dispatches every item through a vtable, re-runs `.filter(...)` on top of `DBIterator` bounds, and allocates a fresh `Vec<u8>` via `k[PREFIX_SIZE..].to_vec()` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L577)) even though `k` is already an owned `Vec<u8>`.
3. **`RangeBounds<Cow<'a, [u8]>>` friction and wasted overlay lookahead clones:**
   Callers of `MapxRaw::range` and `MapxOrdRawKey::range` cannot write `map.range(b"a"..b"z")` without `Cow` wrappers; `MapxOrd<String, V>::range` ([mapx_ord/mod.rs](../../strata/src/basic/mapx_ord/mod.rs#L209)) rejects `"a".."z"`; and [`StagedRows::scan_prefix`](../../strata/src/common/staged.rs#L95-L102) maps `(k.clone(), v.clone())` over `self.rows.range(..)` before `MergeScan` filters out staged `None` tombstones.

### v18 design

1. **Unify all prefix scans onto `db.iter_with_prefix(&meta_prefix, &opts)` + expose sub-prefix `delete_range`:**
   - Set `ReadOptions::{iterate_lower_bound, iterate_upper_bound}` with `Bound::Excluded(lo)` converted to inclusive `lo || [0x00]` (mirroring `Bound::Included(hi)` → `hi || [0x00]` at [mmdb.rs](../../core/src/common/engine/mmdb.rs#L547-L551)). Verified in `mmdb 4.3.14` (`db.rs:2136-2326`), `iter_with_prefix` intersects caller bounds with `[meta_prefix, prefix_end)`, applies **both** range and **prefix Bloom** SST pruning, installs bounds on `DBIterator`, and seeks once. Preserve captured snapshot `ReadOptions` and [`CheckedBidiIter`](../../core/src/common/engine/mmdb.rs#L631-L657)'s error check after **every** `next()` and `next_back()`.
   - Add `pub(crate)` / `MapxRawBatch::delete_range(&mut self, start: &[u8], end: &[u8])` wrapping `mmdb::WriteBatch::delete_range(&full_start, &full_end)` (`db.rs:1691`) so composite single-prefix stores (`DagMapRaw` `[0x02]..[0x03]`) can range-wipe user data without wiping `[0x00]`/`[0x01]` metadata rows.
2. **Concrete `MmdbIter` and in-place prefix stripping:**
   Replace `Box<dyn DoubleEndedIterator<...>>` with a concrete `pub struct MmdbIter { inner: CheckedBidiIter }` and strip the 8-byte prefix in place via `k.drain(..PREFIX_SIZE)` (avoiding a second `Vec<u8>` heap allocation per yielded forward/backward item).
3. **Compiler-verified `RawRangeBounds`, `OrderedRangeBounds<K>`, and zero-tombstone-clone `MergeScan`:**
   Naively implementing `OrderedRangeBounds<K>` for both `Range<K>` and `Range<&Q>` triggers Rust coherence error `E0119` (since `&LocalType` is fundamental and could overlap `K`). Instead, parameterize endpoint encoding **on `K`** via `pub trait OrderedBoundEndpoint<E>: KeyEnDeOrdered`:
   - `impl<K: KeyEnDeOrdered> OrderedBoundEndpoint<K> for K` (owned endpoints, e.g. `1u64..10u64`, `[0u8; 64]..=[1u8; 64]`);
   - `impl<K: KeyEnDeOrdered> OrderedBoundEndpoint<&K> for K` (borrowed `&K` endpoints — provably disjoint from `K` because `K = &K` is an infinite type);
   - concrete unsized-slice impls `OrderedBoundEndpoint<&str>` for `String` / `Box<str>` and `OrderedBoundEndpoint<&[u8]>` for `Vec<u8>` / `Box<[u8]>`;
   - plus a dedicated `impl<K: KeyEnDeOrdered> OrderedRangeBounds<K> for RangeFull` carrying **no endpoint type parameter `E`**, and `impl<K: OrderedBoundEndpoint<E>, E> OrderedRangeBounds<K>` for `Range<E>`, `RangeInclusive<E>`, `RangeFrom<E>`, `RangeTo<E>`, `RangeToInclusive<E>`, and `(Bound<E>, Bound<E>)` (likewise on `get_le<E>` / `get_ge<E>`).
   Verified with `rustc 1.100.0 --edition=2024`, `map.range(..)`, `map.range(1u64..10)`, `map.range(&1u64..&10)`, `str_map.range("a".."z")`, `str_map.range(String..String)`, `arr_map.range([0u8; 64]..=[1u8; 64])`, and `arr_map.range(&[0u8; 64]..=&[1u8; 64])` all compile without turbofish or coherence conflicts. In `StagedRows::MergeScan`, borrow `btree_map::Range<'a, Vec<u8>, Option<Vec<u8>>>` directly and clone `(k, v)` only when emitting a live `Some(v)` overlay entry.

---

## 4. Basic collections: safe `Entry` enum, `DerefMut`-only `ValueMut`, minimal `Reader`/`ReadView`, and `unsafe` cleanup

Source: [macros.rs](../../strata/src/common/macros.rs#L57-L183), [mapx_ord_rawkey/mod.rs](../../strata/src/basic/mapx_ord_rawkey/mod.rs#L92-L124), [orphan/mod.rs](../../strata/src/basic/orphan/mod.rs#L91-L208), [mapx_raw/mod.rs](../../core/src/basic/mapx_raw/mod.rs#L74-L185).

### Current defects and costs

1. **2 upfront clones/encodes + up to 2 `Drop` re-encodes on every `get_mut` / `iter_mut` / `Orphan +=`:**
   [`MapxOrdRawKey::get_mut`](../../strata/src/basic/mapx_ord_rawkey/mod.rs#L92-L106) and `ValueIterMut` eagerly serialize `snapshot = value.encode()` **and** clone `original = inner.as_slice().to_vec()` (duplicating bytes already held inside `self.inner`), then re-encode up to twice on `Drop` when `!self.dirty` — solely to auto-persist `RefCell<T>` interior mutability through shared `&V` `Deref` ([mapx/test.rs](../../strata/src/basic/mapx/test.rs#L333-L343)). Meanwhile `ValueMut`, `ValueIterMut`, `mapx::ValueIterMut`, `orphan::ValueMut`, and `dagmap::rawkey::ValueMut` duplicate the guard boilerplate.
2. **Raw-pointer `unsafe` in `Entry` APIs + dead `pub unsafe` surface:**
   `entry_or_insert_via_mock!` ([macros.rs](../../strata/src/common/macros.rs#L282-L298), which only exposes `or_insert(V)`) and `MapxRaw::Entry` ([mapx_raw/mod.rs](../../core/src/basic/mapx_raw/mod.rs#L846-L880), which exposes `or_insert` and `or_insert_with`) cast `&mut Hdr` to `*mut Hdr` to work around borrow lifetimes. `MapxOrd::insert_encoded_value` ([mapx_ord/mod.rs](../../strata/src/basic/mapx_ord/mod.rs#L156)) and `MapxOrdRawKey::insert_encoded_value` have **zero external callers**; `MapxRaw::from_bytes_in` and `engine::Mapx::from_prefix_slice_in` have only **one legacy caller** (`DagIdAllocator::read_legacy_counter`); `define_map_wrapper!` ([macros.rs](../../strata/src/common/macros.rs#L176-L183)) decodes `self.as_bytes()` manually instead of calling `self.inner.instance_id()`; and `Orphan::{is_uninitialized, initialize_if_empty}` are dead relics.
3. **Missing typed `Reader` / `ReadView`:**
   While `&Mapx<K, V>` already permits shared reads when no writer holds `&mut Mapx<K, V>`, retaining an independent read handle alongside an active single writer currently forces typed-map callers into `unsafe { map.shadow() }`.

### v18 design

1. **Explicit `DerefMut` write-back on `ValueMut` / `ValueIterMut`, debug upgrade check, and fallible `update`:**
   - **Intentional v18 contract change:** `ValueMut` and `ValueIterMut` persist changes strictly when `DerefMut` (`&mut V`) was called (`dirty == true`) or when initialized by a vacant `Entry` (`is_mock == true`, so `map.entry(&k).or_insert(v);` inserts even if the returned guard is dropped without dereferencing). Interior mutability through shared `&V` (`RefCell`) is no longer auto-persisted.
   - **Upgrade safety check (`#[cfg(debug_assertions)]`):** Because dropping `RefCell`-via-`&V` persistence causes no compile error, debug/test builds record `initial_encode = value.encode_value()` on guard creation and `debug_assert_eq!(self.value.encode_value(), initial_encode, ...)` on non-unwinding `Drop` when `!self.dirty && !self.is_mock` — catching un-migrated interior-mutability mutations immediately in tests with **zero release-build cost** (0 upfront encodes, 0 `original` clones, and 0 `Drop` encodes on untouched release guards).
   - Retain [`UnwindMark::interrupted()`](../../strata/src/common/mod.rs#L39-L52) (`!panicking_at_entry && thread::panicking()`, NOT bare `!thread::panicking()`, so a guard created inside a `Drop` during an active unwind still behaves normally while a panic starting mid-guard suppresses half-mutated write-back). When `!self.unwind.interrupted() && (self.is_mock || self.dirty)`, encode `self.value` **once** on `Drop` and write back iff `self.is_mock || encoded.as_slice() != self.inner.as_slice()`.
   - Add `ValueMut::commit(self) -> Result<()>` and `map.update(&k, |v: &mut V| ...)` for deterministic fallible write-back. Unify `ValueMut<'a, V>` and `ValueIterMut<'a, V>` over one internal `RawValueSlot` trait and delete `Orphan::{is_uninitialized, initialize_if_empty}`.
2. **100% safe `enum Entry<'a, V> { Occupied, Vacant }` and `pub unsafe` pruning:**
   Read the raw value inside `entry(key)` before constructing `OccupiedEntry` or `VacantEntry` holding `&'a mut Self` — deleting all `Entry` raw-pointer `unsafe` while providing `or_insert`, `or_insert_with`, `or_default`, `and_modify`, `OccupiedEntry::{get, get_mut, insert, remove}`, and `VacantEntry::insert`. **Explicitly removed public items:** `MapxOrd::insert_encoded_value`, `MapxOrdRawKey::insert_encoded_value`, `MapxRaw::{from_bytes_in, as_bytes}`, `engine::Mapx::from_prefix_slice_in`, and `Orphan::shadow` (demoted to `pub(crate)`).
3. **Minimal view-factory `Reader` + typed `ReadView` (per [durable-batches-and-read-views.md](durable-batches-and-read-views.md#L216-L219)):**
   Rather than duplicating ~15 collection methods across a parallel hierarchy of 9 reader structs, ship **one generic immutable handle `MapReader<M>`** (exposing `read_view(&self)`, `instance_id(&self)`, and `namespace(&self)`, while owning a `Namespace` clone that prevents premature `Namespace::close`) paired with borrowed `MapxReadView<'a, K, V>`, `MapxOrdReadView<'a, K, V>`, `MapxOrdRawKeyReadView<'a, V>`, and `OrphanReader<T>::get(&self) -> T` (plus completing `MapxRawReader`).

---

## 5. Universe header, unified `VSTYPE04` handle frames, fail-closed `VSNREG02`, and `ruc`-free `VsdbError`

Source: [strata/src/common/mod.rs](../../strata/src/common/mod.rs), [core/src/common/mod.rs](../../core/src/common/mod.rs), [namespace.rs](../../core/src/common/namespace.rs), [error.rs](../../core/src/common/error.rs), [mmdb.rs](../../core/src/common/engine/mmdb.rs#L871-L1521).

### Current defects and recovery subtleties

1. **Format marker conflates universe version with default-shard readiness, and roots lack identity:**
   [`check_format_version`](../../core/src/common/engine/mmdb.rs#L1700-L1725) treats `NotFound` as pre-v16 OK and only rejects `ver > SUPPORTED_FORMAT_VERSION`, while [`validate_shard_layout`](../../core/src/common/engine/mmdb.rs#L1488-L1499) treats `format_version` present + `0` shard dirs as a damaged default engine. In addition, writable single-process exclusion relies only on in-process mutexes ([namespaces.md](namespaces.md#L27-L30)), and [`Namespace::relocate`](../../docs/proposals/namespaces.md#L219-L222) cannot detect an operator pointing namespace `A` at namespace `B`'s directory because engine roots carry no `(universe_id, ns_id)`.
2. **Triple-nested handle envelopes + `note_meta_bytes` prefix-recovery coupling:**
   Serializing a `Mapx<K, V>` wraps `VSMAPX01` → `MapxOrdRawKey` `VSTYPE03` → `Mapx` `VSTYPE03` (intermediate `Vec<u8>` allocations and 3 `postcard` passes on nested-map reads), plus `VSTYPE02` / `legacy_type_name` shims ([strata/src/common/mod.rs](../../strata/src/common/mod.rs#L106-L152)). Missing-ceiling prefix recovery ([`note_meta_bytes`](../../core/src/common/engine/mmdb.rs#L1165-L1219)) scans shard values, `__instance_meta__`, and `__CUSTOM__` for raw handle magic so even **empty** saved maps do not have their prefix reissued — any new typed frame must place `prefix_le` at a single unambiguous offset across **all** typed collections.
3. **`__namespace_state__` sidecars (4 durable registry writes per `Namespace::create`) and missing-registry reuse protection:**
   [`Namespace::create_with`](../../core/src/common/namespace.rs#L733-L767) writes `__namespace_state__/<id>` sidecars separately from `__namespaces__` (4 atomic file writes across creation, alongside initial directory `fsync`s in `create_dir_all_durable`). [`load_registry`](../../core/src/common/namespace.rs#L335-L345) treats a missing `__namespaces__` file as `RegistryFile::default()` (`next_id = 1`), relying on surviving sidecars to block ID reuse for explicit-path namespaces if `__namespaces__` was lost.
4. **Default-engine initialization on `vsdb_flush()` and v15 allocator fold:**
   `ensure_alloc_init` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L882-L884)) forces `LazyLock::force(&VSDB)` whenever `<base>/mmdb` exists to fold pre-v16 shard-0 counters, and `vsdb_flush()` ([core/src/common/mod.rs](../../core/src/common/mod.rs#L544)) unconditionally initializes `DEFAULT_NS` (2 GiB sizing input = 256 MiB `BlockCachePool` + 16 shards/compaction workers) even when `<base>/mmdb` never existed.
5. **`ruc` SemVer leak and flattened `VsdbError::Other`:**
   `impl From<Box<dyn ruc::err::RucError>> for VsdbError` ([error.rs](../../core/src/common/error.rs#L154)) leaks `ruc` into public SemVer, while `mmdb.rs` flattens `std::io::Error` and `mmdb::ErrorKind` into `VsdbError::Other { detail: String }` (`source() == None`).

### v18 design

1. **Decoupled atomic `VSUNI018` universe commit point + per-engine `.shards_ready` + OS advisory `LOCK`:**
   - Separate **universe identity** from **per-engine shard readiness**:
     - `<base>/__SYSTEM__/universe_header` (`VSUNI018`, `format_version = 18`, `universe_id: u128`) is the **single atomic bootstrap commit point** of a fresh `<base>`: bootstrap acquires `<base>/__SYSTEM__/LOCK` (OS advisory lock in writable mode), writes initial `VSNREG02` `__namespaces__` and `__prefix_ceiling__` via `atomic_write_file`, and writes `universe_header` **last** via `atomic_write_file`. If a crash occurs before `universe_header` lands, an uncommitted `<base>` (containing only `__SYSTEM__` pre-commit files and no `mmdb/` or `namespaces/`) resumes bootstrap idempotently instead of bricking.
     - Each engine root (`<base>/mmdb` when `DEFAULT_NS` is first opened, and `<ns_root>` for non-default namespaces) writes its own `<engine_root>/__SYSTEM__/engine_header` (`VSENG018`, `universe_id: u128`, `ns_id: u64`, `shards: u32`) **only after** all `shard_00..shard_{N-1}` directories exist (preserving `.initializing` resumability on mid-create crashes). Lazy `DEFAULT_NS` therefore has `universe_header` present and `<base>/mmdb` absent (`0` shards, no `engine_header`) without triggering `validate_shard_layout`'s damaged-shard check, while deleting shard directories after `engine_header` exists still fails closed.
   - **Error channels:** `vsdb_configure` and `Namespace::{open, create, relocate}` validate headers and return `Result<_, VsdbError>` (never panicking on `v16`/`v17` or mismatched `(universe_id, ns_id)` roots). Calling infallible constructors like `Mapx::new()` without prior `vsdb_configure` on an unmigrated `v17` or damaged `<base>` panics with the underlying `VsdbError` message (matching `LazyLock::force(&VSDB)` today).
2. **Fail-closed `VSNREG02` inline lifecycle + lazy `DEFAULT_NS`:**
   - Embed `state: NsLifecycleState` directly in `NsRecord` under `VSNREG02` (`__namespaces__`), deleting `__namespace_state__` sidecars and reducing `Namespace::create()` registry saves from **4 atomic file writes to 2** (initial directory creation `fsync`s remain unchanged). Once `universe_header` exists, a missing `__namespaces__` **fails closed (`VsdbError::Corrupt`)** instead of resetting `next_id` to `1`.
   - Delete v15 `META_KEY_PREFIX_ALLOCATOR` migration, `struct VsDB`, and `vsdb_meta_path(u64)` / `vsdb_get_meta_dir()`; hold `DEFAULT_NS` in a `OnceLock<Namespace>` so `vsdb_flush()` only flushes `DEFAULT_NS.get()` when open; and unify `write_file_durable` onto `atomic_write_file`.
3. **Unified single-prefix handle frames (`VSMAPX02` & `VSTYPE04`) + backward-read of `VSMAPX01` / `VSTYPE03`:**
   - `MapxRaw`: fixed 24-byte stack frame `b"VSMAPX02" | prefix_le:8 | ns_id_le:8` (`0` = default), **continuing to read 16-byte `VSMAPX01`** (`ns_id = 0` or ambient scope) so existing raw handles open in place.
   - **All** single-prefix typed collections (`Mapx`, `MapxOrd`, `MapxOrdRawKey`, `Orphan`, `PersistentBTree`, `SlotDex`, `VecDex`, `VerMap`, `VerMapWithProof`, `DagMapRaw`, `DagMapRawKey`, `DagMap`) share **one** canonical `VSTYPE04` header layout:

     ```text
     b"VSTYPE04" (8B) | type_tag_le (8B) | prefix_le (8B) | ns_id_le (8B) | meta_len_le (4B) | extra_meta (meta_len B)
     ```

     - For `Mapx*`, `Orphan`, `PersistentBTree`, and `VerMap`, `meta_len_le == 0` (exact **36-byte stack frame `[u8; 36]`**, zero heap allocations during serde).
     - For `SlotDex` (`ver=3, tier_capacity: u32, swap_order: bool`), `VecDex` (`ver=2`), `DagMap*` (`ver=1, dag_id: u64`), and `VerMapWithProof`, `extra_meta` carries the compact collection header **after** fixed offset `36` — so **`prefix_le` is unconditionally at bytes `16..24` of every `VSTYPE04` frame** and never collides with `extra_meta`.
     - `type_tag_le` is `fnv1a64(std::any::type_name::<Self>().as_bytes()).to_le_bytes()` (matching `VSTYPE03` at [strata/src/common/mod.rs](../../strata/src/common/mod.rs#L98-L104)), documented explicitly as a best-effort local nominal guard against opening a handle under the wrong Rust wrapper/type — **not** a cross-module-rename or struct-field schema hash.
     - Keep `VSTYPE03` reader support on `Mapx*`, `Orphan`, and `SlotDex` (deleting `VSTYPE02` and `legacy_type_name`), and update [`note_meta_bytes`](../../core/src/common/engine/mmdb.rs#L1165) to scan `VSMAPX01` (`+8..+16`), `VSMAPX02` (`+8..+16`), and `VSTYPE04` (`+16..+24`) with a 23-byte `note_meta_stream` carry window so both in-place `v17` and `v18` handles always reserve their prefixes on missing-ceiling recovery.
4. **Drop `ruc` without leaking `mmdb::Error` into public SemVer:**
   - Remove `ruc` (and public `From<Box<dyn RucError>>`) from both crates and convert `mmdb.rs` to `crate::Result<T>`.
   - Map `mmdb::Error` via a **crate-private** `pub(crate) fn from_mmdb_err(...) -> VsdbError` (never a public `From<mmdb::Error>`), preserving `Error::source()` across `Io`, `Corruption`, `ReadOnly`, `InvalidArgument`, `DbClosed`, `Background`, and `#[non_exhaustive]` fallback. Move `BranchId(u64)` / `CommitId(u64)` to `vsdb_core::common` for typed `VsdbError` variants.

---

## 6. `VerMap` and `PersistentBTree`: single-handle big-endian store, slotted `NodeView`, benchmark-gated ref persistence, and lock-free-across-fsync snapshots

Source: [versioned/map.rs](../../strata/src/versioned/map.rs), [versioned/repair.rs](../../strata/src/versioned/repair.rs), [versioned/read.rs](../../strata/src/versioned/read.rs), [persistent_btree/mod.rs](../../strata/src/basic/persistent_btree/mod.rs), [persistent_btree/nodes.rs](../../strata/src/basic/persistent_btree/nodes.rs), [persistent_btree/merge.rs](../../strata/src/basic/persistent_btree/merge.rs).

### Current defects and costs

1. **8-prefix non-atomic `VerMapPayload` + `gc_dirty` + `BranchState` churn:**
   `VerMap` persists 8 separate handles ([versioned/map.rs](../../strata/src/versioned/map.rs#L244-L253)). Although v17 co-locates newly created maps onto one shard (avoiding inter-step WAL fences on working edits), engine batches remain single-prefix: every working `VerMap::insert`/`remove` commits 2 separate prefixes (`tree.nodes` + `branches`, cloning `BranchState.name: String`), `commit()` executes **6 separate prefix writes + `gc_dirty` toggles** before its final WAL sync, and `new_with_main` burns ~112 prefixes to co-locate 8 handles on 16 shards.
2. **`RefState` RAM growth under retained history and full-DB `from_meta()` scans:**
   Every live node's refcount and child list live only in volatile `RefState.counts: HashMap<NodeId, NodeRef>` ([persistent_btree/mod.rs](../../strata/src/basic/persistent_btree/mod.rs#L72)). Every `VerMap::from_meta()` runs **2 full `nodes.iter()` scans + random `nodes.get` across every reachable node** (reading ~96% leaf pages off disk just to discover `children.is_empty()`) — plus a WAL `fsync` in writable mode. At ~70–90 B per `RefState` entry (`B = 16`, ~24 keys/leaf), a single-version 5M-key tree uses ~17 MiB, while workloads retaining thousands of historical block commits (2M+ distinct COW nodes) permanently pin **>150 MiB** of RAM.
3. **~128 sibling `Vec<u8>` allocations per depth-4 `get()` and borrowed `Snapshot<'a>`:**
   Sequential `Node::decode` ([nodes.rs](../../strata/src/basic/persistent_btree/nodes.rs#L114-L138)) allocates `Vec<Vec<u8>>` for all ~24 sibling keys at every internal level and all ~24 keys **and values** at the leaf before returning 1 value. `Snapshot<'a, K, V>` ([versioned/read.rs](../../strata/src/versioned/read.rs#L37-L40)) borrows `&'a VerMap`, blocking concurrent `&mut VerMap` mutations even though `RootLease` pins the root, and `PersistentBTree` carries the subsystem's sole `unsafe` (`self.nodes.shadow()` at [persistent_btree/mod.rs](../../strata/src/basic/persistent_btree/mod.rs#L461)).

### v18 design

1. **Single-handle big-endian tagged `VerMap` store (`1 MapxRaw`, preserving `PersistentBTree` as `pub`):**
   Keep standalone `PersistentBTree` **`pub`** (avoiding API whiplash after `v17.0.0` moved callers onto `PersistentBTree::{diff, merge}`; [CHANGELOG.md](../../CHANGELOG.md#L513-L517)) while consolidating `VerMap` onto one `MapxRaw` with **big-endian** object keys so top-2-bit tag classes (`0b00` internal, `0b01` inline leaf, `0b10` blob leaf, `0b11` blob) cluster physically in SST blocks and expose each class's maximum allocated ID via one $O(1)$ reverse seek:

   ```text
   [0x00]                   -> Header { ver: u8, next_id: u64, next_commit: u64, next_branch: u64, main_branch: u64 }
   [0x01 | branch_id_be:8]  -> BranchHead { head: u64, dirty_root: u64 }   // fixed 16B, separated from String name
   [0x02 | branch_id_be:8]  -> branch_name UTF-8 bytes
   [0x03 | branch_name]     -> branch_id_le:8
   [0x04 | commit_id_be:8]  -> Commit { root: u64, timestamp_us: u64, ref_count: u32, parents: [u64] }
   [0x05 | root_id_be:8]    -> []                  // retired roots awaiting deferred/pinned reclamation
   [0x06 | stage_root_be:8] -> []                  // unpublished chunked bulk_load / large-merge subtrees
   [0x10 | node_id_be:8]    -> Slotted B+ tree node page (0b00 internal | 0b01 inline leaf | 0b10 blob leaf)
   [0x11 | obj_id_be:8]     -> extra_refs: u32     (Mode A)  OR  [0x07] Verified RefState checkpoint (Mode B)
   [0x12 | blob_id_be:8]    -> Separated large value bytes (> 1 KiB, tag 0b11)
   ```

   - Working edits (`insert`, `remove`, `write_batch`, `discard`) commit node/blob pages + `Header` + `BranchHead` in **1 atomic `batch.commit()`** (collapsing `2 → 1` batches and `0` `String` clones).
   - Ordinary history operations (`commit`, `create_branch`, `delete_branch`, `rollback_to`, `set_main_branch`, and merges within the ~16 MiB batch budget) commit in **1 atomic `batch.commit_sync()`** (`6 writes + gc_dirty → 1 WAL record`).
   - **Multi-chunk `bulk_load` and oversized `merge` (>16 MiB):** Flush newly allocated unpublished subtrees in ~16 MiB batches anchored by **`[0x06 | stage_root_be] -> []`**, then publish the final `BranchHead`/`Commit` and delete the `[0x06]` anchors in **1 final atomic `commit_sync()`**. If a crash interrupts staging before final publication, writable `from_meta()` cascade-reclaims `[0x06]` orphan roots without touching live branches.
2. **Benchmark-gated reference persistence (Stage 0 gate — acknowledging `mmdb 4.x` read-modify-write cost):**
   Because `mmdb 4.x` has no merge operator, persisted sparse `[0x11]` `extra_refs` (**Mode A**) turns every non-canceling refcount change on an already-persisted node into a point `get` + batch `put/delete` (~72 `[0x11]` updates on the first edit after `commit()` on a depth-4 `B = 16` tree, and ~20 per updated key in a 1,000-key block over a 1M-key tree with retained history, vs. 0 I/O in RAM today). Stage 0 benchmarks Mode A head-to-head against **Mode B (Verified `RefState` Checkpoint)**:
   - **Mode A (Sparse `[0x11]` `extra_refs`):** Chosen iff Stage 0 block-commit throughput stays within 10% of `v17` (or if paired with optional `mmdb v5` delta-merge operands). `0b01` inline leaves and `0b11` blobs have 0 outgoing pointers and are never read on delete.
   - **Mode B (Compact `u32` internal-only `RefState` + verified `[0x07]` checkpoint):** Keeps write-path ref updates 100% in memory (0 extra read/write I/O), omits `0b01` inline-leaf child lists from RAM (shrinking per-node RAM ~3×), and writes a checksummed `[0x07]` refcount snapshot bound to `Header` generation on clean `flush`/`close`/checkpoint so normal `from_meta()` loads sequentially without scanning `[0x10]` pages, falling back to full tree scan only after an uncheckpointed crash.
3. **Open-time verification contract (`strata/docs/versioned.md` alignment):**
   - Preserve fail-closed metadata & ceiling validation on every `VerMap::from_meta()` **before** any reclamation: validate `Header`, verify every `BranchHead` (`dirty_root` and `head`) and reachable `Commit` DAG row exists with valid root page headers, and verify via **4 $O(1)$ big-endian reverse seeks** on `[0x10]`/`[0x12]` that the highest persisted ID in each 2-bit tag class is `< Header.next_id`. Missing commits, dangling branch roots, or truncated `next_id` still return `Err(VsdbError::Corrupt)` at open time ([versioned.md](../../strata/docs/versioned.md#L502-L507)).
   - Provide `VerMap::from_meta_verified()` and `VerMap::verify_and_gc() -> Result<GcStats>` for full leaf-level scan + refcount rebuild/self-healing prior to reclamation, and update `versioned.md` explicitly to distinguish default metadata/root open verification from deep leaf verification.
4. **Shared `TREE_RUNTIMES` and lock-free-across-`fsync` `Snapshot` retirement:**
   - Retain `TREE_RUNTIMES: Mutex<HashMap<InstanceId, Weak<TreeRuntime>>>` so restored in-process aliases and `VerMapReader`s share `next_id`, `pins`, and `zombie_roots`; back `RootLease` with safe `MapxRawReader` (`#![forbid(unsafe_code)]` across `persistent_btree` and `versioned`) and make `Snapshot<K, V>` **owned (`'static + Clone + Send + Sync`)**.
   - **Never hold the pin lock across WAL `fsync`:**
     - When `Arc::strong_count(&self.runtime) == 1` (single handle, no live `Snapshot`/`VerMapReader`/alias), Rust's `&mut self` guarantees zero concurrent `snapshot()` calls, so retired roots reclaim inline.
     - Whenever aliases, `VerMapReader`s, or `Snapshot`s share `TreeRuntime` (`strong_count > 1`), retiring root `R` stages `[0x05 | R_be] -> []` in the `commit_sync()` batch **without deleting `R`'s pages yet** — so concurrent `snapshot()` readers acquire `TreeRuntime`'s in-memory `Mutex` in nanoseconds (never waiting on writer `fsync`), keep reading `R` safely throughout `fsync` (and if `commit_sync()` fails, `R` was never deleted), and `R` is cascade-reclaimed **after** commit once `pin_count(R) == 0` (or on the next writable op / writable `from_meta()` after a crash).
5. **Slotted `NodeView<'a>(&'a [u8])`, owned-leaf `BTreeIter`, and range-grouped `write_batch`:**
   - Encode `u32` key end-offsets and `u64` value/blob descriptors at the head of each node page so `NodeView` binary-searches `&[u8]` slices inside the single owned `RawValue` (`Vec<u8>`) returned per level by `mmdb` — eliminating all **~128 per-sibling key/value `Vec` allocations** on a depth-4 `get()`. `BTreeIter` holds the current leaf's `RawValue` buffer and decodes `(K, V)` on demand.
   - Drop `K: Clone, V: Clone` from `VerMap::clone()`, add `VerMap::clone_in(&ns)`, share a single-pass sorted range-grouped COW applier across `VerMap::write_batch` and `merge::replay`, and make `bulk_load` and `BTreeDiffIter` $O(\text{height})$ streaming.

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
   - **Acknowledged COW update tradeoff:** When a snapshot or `committed_trie` retains a second reference (`Arc::strong_count >= 2`), the first mutation along a shared branch path-copies $O(\text{depth})$ compact nodes via `Arc::make_mut` (~6–8 nodes on typical MPTs; subsequent edits in the same `batch_update` mutate unshared `strong_count == 1` nodes in place), trading small path clones on the first dirty touch against eliminating `v17`'s `reverse_dirty()` double-mutation + re-hashing on every `merkle_root()` call.
   - Keep **iterative last-owner (`Arc::try_unwrap`) drop stacks** on **both** `MptNode::drop` and `SmtNode::drop` (plus iterative SMT walks) so dropping deep uniquely-owned chains never overflows the stack.
   - Compact `MptNode::Branch` to `(bitmap: u16, children: Box<[Arc<MptNode>]>, value: Option<Box<[u8]>>)` via POPCNT indexing (~3–4× smaller branch nodes, **~1.2–1.9× lower whole-trie RAM** depending on branching density and value size); pack `Nibbles` inline up to 64 nibbles (32-byte keys) **with a `Box<[u8]>` heap fallback up to 2,048 nibbles (`MAX_MPT_KEY_LEN = 1024` bytes)**; and stream `NodeCodec` directly into `Keccak256::update`.
   - `VerMapWithProof` retains `committed_trie: T` and forks `working_trie` in $O(1)$ only when `working.root != synced_working_root`.
2. **Durable, budget-gated, map-bound post-order checkpoints (`MPTC v2` / `SMTC v4`):**
   - Stream via `BufWriter` to sibling temp file → `BufWriter::flush` → `File::sync_all` → atomic `fs::rename` → parent-dir `sync_all`, omitting redundant per-node cached hashes (**saving ~34 bytes per serialized node** — ~45% on 32B values, ranging from ~3–5% on multi-KB values to ~85% on empty/tiny values). Old disposable `v17` caches simply miss version check and rebuild cleanly.
   - Bind the checkpoint header to `(universe_id, map_instance_id, commit_id, btree_root_id)` so foreign checkpoints cannot match by `CommitId` coincidence.
   - Enforce `max_nodes`, `max_pending_stack`, `max_value_len`, and cumulative `decoded_bytes` budgets **before** each allocation during `BufReader` parsing, plus online canonicality and ancestor-consistent SMT bit-length checks (closing [audit.md](../audit.md#L57-L60)).
3. **Unified `TrieCalc` and `VerMapWithProof` lifecycle:**
   - Add `type Proof` + `prove` / `verify_proof` on `TrieCalc`, return infallible `[u8; 32]` from `TrieCalc::root_hash(&self)` while **keeping `VerMapWithProof::merkle_root(&mut self, branch: BranchId) -> Result<[u8; 32]>` fallible** (as branch lookup or lazy `> MAX_MPT_KEY_LEN` sync can error), unify `VerMapWithProof` proof methods into one generic `impl<K, V, T: TrieCalc>`, and add `new_in`, `clone_in`, `save_meta`, `from_meta`, `instance_id`, and `namespace`.

---

## 8. `DagMap`: shard-colocated single-handle nodes, fold-before-`Clearing` crash transitions, and typed `DagMap<K, V>`

Source: [dagmap/mod.rs](../../strata/src/dagmap/mod.rs), [dagmap/raw/mod.rs](../../strata/src/dagmap/raw/mod.rs), [dagmap/rawkey/mod.rs](../../strata/src/dagmap/rawkey/mod.rs), [audit.md](../audit.md).

### Current defects and hazards

1. **Scattered 3-handle nodes and full-namespace WAL syncs:**
   [`DagMapRaw`](../../strata/src/dagmap/raw/mod.rs#L86-L96) holds `{ data: MapxRaw, parent: Orphan<Option<DagMapRaw>>, children: MapxOrdRawKey<DagMapRaw> }`. `build()` leaves `data` un-colocated ([raw/mod.rs](../../strata/src/dagmap/raw/mod.rs#L278)) and `Clone` drops co-location completely, forcing `prune_mainline` to issue **5 full-namespace `try_sync_wal()` fences** (`5 × shard_count` = 80 shard WAL syncs in the 16-shard default namespace) and `destroy()` to issue 2 fences (32 shard syncs).
2. **Serde tuple decomposition + root-wipe debt (`[MEDIUM]` in `audit.md`), 7 `unsafe` blocks, and `DagIdAllocator`:**
   Bare 3-tuple serialization allows safe callers to deserialize and mutate `parent` into a cycle or plant a live root (`parent == None`) into `children` where `owned_or_residue` ([raw/mod.rs](../../strata/src/dagmap/raw/mod.rs#L669-L674)) wipes it. Storing `DagMapRaw` inside `Orphan`/`MapxOrdRawKey` requires **7 internal `unsafe` blocks**, a global `DagIdAllocator` (`dag_id_ceiling`), $O(\text{siblings})$ registry scans on `destroy()`, and 3 handle decodes per ancestor hop on `get()`.
3. **Empty-value `[]` panics and broken typed surface:**
   Using `[]` as deletion tombstone causes `insert(k, [])` and `DagMapRawKey<()>::insert` to panic; `DagMapRawKey<V>::new(Option<&mut DagMapRaw>)` ([rawkey/mod.rs](../../strata/src/dagmap/rawkey/mod.rs#L103)) forces callers into `into_inner()` or `unsafe { shadow_inner() }`; and typed `DagMap<K, V>` is missing.

### v18 design

1. **One shard-colocated `MapxRaw` per node with `dag_id` authentication and node-local `edge_id`s:**
   Keeping one `MapxRaw` per node preserves per-node `InstanceId` / `save_meta()`, while creating every child via **`parent.store.new_colocated()`** places the entire connected DAG onto **one shard WAL**:

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
   - **Acknowledged single-shard tradeoffs:** Placing a connected DAG on one shard concentrates all nodes of that DAG onto 1 shard's write buffer (16 MiB in the default 16-shard namespace) and 1 background compaction worker, and `new_colocated()` skips `shard_count - 1` prefixes (~16 prefix IDs per child in default 16-shard space — zero extra I/O inside the 8,192 pre-reserved window and negligible in $2^{64}$ prefix space). Write-heavy DAGs should use a dedicated `shards = 1` namespace with an appropriate `mem_budget_mb` ([performance-roadmap.md](performance-roadmap.md#L17-L31)).
2. **Crash-ordered single-shard state transitions + sub-prefix `[0x02]..[0x03]` range wipe + 1-shard final sync:**
   Because co-located prefixes share one WAL (program order = crash replay order), inter-phase cross-shard WAL fences disappear, while `MapxRawBatch::delete_range(&[0x02], &[0x03])` (§3.1) wipes user data in $O(1)$ without touching `[0x00]` `NodeMeta` or `[0x01]` discovery edges:
   - **`prune()` (fold & reparent *before* `Clearing`, matching [dagmap/raw/mod.rs](../../strata/src/dagmap/raw/mod.rs#L510-L573)):**
     1. Destroy non-mainline side branches while the mainline is intact;
     2. Fold net mainline overlay into `genesis_map` (single atomic batch on `genesis_map`) and reparent surviving head children to `genesis_map` (idempotent and value-exact on crash);
     3. **Only after fold + reparent have been written**, transition consumed mainline nodes (head + intermediates, never `genesis_map`) to `Clearing { owner_parent, parent_edge, genesis_map }` in `[0x00]` — so a retry that observes `Clearing` jumps straight to finishing teardown without ever re-applying a half-wiped ancestor over `genesis_map`;
     4. Range-delete `[0x02]..[0x03]` on consumed nodes while keeping `[0x00]` `Clearing` (so repeated `prune()` on a consumed handle still resolves `genesis_map`) and `[0x01]` discovery edges, then delete consumed `[0x01]` edges, prune non-survivor entries on `genesis_map`, and issue **1 single-shard `sync_wal()`** (preserving `prune()`'s return-time durability for **1 shard sync instead of 80**).
   - **`destroy()`:** (a) transition target from `Child { p, e }` / `Root` to `Destroying { p, e }` and range-delete its `[0x02]..[0x03]` data; (b) pass 1 marks/wipes owned descendants' `[0x02]..[0x03]` data while keeping `[0x01]` discovery edges intact; (c) pass 2 deletes `[0x01]..[0x02]` edges on descendants and marks them `Destroyed`; (d) delete `[0x01 | e]` from `p`, write `Destroyed` on target, and issue **1 single-shard `sync_wal()`** (1 shard sync instead of 32).
3. **Crate-private reconstruction, `dag_id` ownership check, and complete typed hierarchy:**
   - Keep `open_colocated_prefix(map_id)` **crate-private** (preserving prefix capability isolation), and serialize `DagMapRaw` via the unified `VSTYPE04` frame (`extra_meta = (LAYOUT_VERSION, dag_id)`). Every traversal/cleanup verifies `[0x00]` `NodeMeta` magic, matching `dag_id`, and `owner_parent == Some(self_id)` (never wiping `Root` or another DAG's `Clearing` node — closing [audit.md](../audit.md#L36-L40)).
   - Delete **all 7 `unsafe` blocks**; check local `self.store.get` before allocating cycle-detection state; support empty `[]` / `()` values natively; fix `DagMapRawKey<V>::new(parent: Option<&mut Self>)`; add `clone_in(&ns)`; and ship **`DagMap<K, V>`** (`K: KeyEnDe, V: ValueEnDe`).

---

## 9. `VecDex`: unified stateless handles, lock-free `get` + snapshot-consistent `search`/`iter`, and zero-allocation search

Source: [vecdex/mod.rs](../../strata/src/vecdex/mod.rs), [vecdex/dynamic.rs](../../strata/src/vecdex/dynamic.rs), [vecdex/hnsw.rs](../../strata/src/vecdex/hnsw.rs), [vecdex/distance.rs](../../strata/src/vecdex/distance.rs), [audit.md](../audit.md).

### Current defects and costs

1. **Stale 34-byte `GraphState` corruption (`[MEDIUM]` in `audit.md`) and torn `get` / `search` / `iter` reads:**
   [`VecDex`](../../strata/src/vecdex/mod.rs#L364) caches `GraphState` (5 scalars, 34 bytes) by value, so an independently restored handle holds a stale `next_node_id` and overwrites live nodes on sequential writes. On the read path, [`get`](../../strata/src/vecdex/mod.rs#L570-L575) (`k -> node_id`, then `node_id -> vec`), [`iter`](../../strata/src/vecdex/mod.rs#L590-L601), and [`search_internal`](../../strata/src/vecdex/mod.rs#L902-L996) issue separate uncoordinated `self.store.get` calls without a `MapxRawReadView` — a concurrent `clear()` (which resets `next_node_id` to `0` at [vecdex/mod.rs](../../strata/src/vecdex/mod.rs#L630-L635)) followed by an insert can make `get(k1)` return **`k2`'s vector**!
2. **392 lines of duplicated `VecDexDyn` (`dynamic.rs`):**
   [`DistanceMetric::distance(a, b)`](../../strata/src/vecdex/distance.rs#L101) lacks `&self`, preventing `MetricKind` (and parameterized metrics like `Lp(p)`) from implementing `DistanceMetric` and forcing [`dynamic.rs`](../../strata/src/vecdex/dynamic.rs) to duplicate the entire `VecDex` API.
3. **Per-visited-node `Rc<Vec<S>>` + `HashMap` search allocations and redundant `K: ValueEnDe`:**
   `search_internal` allocates `Vec<S>` + `Rc<Vec<S>>` into a `HashMap` for every visited node (up to `max(4096, 64 * ef)` in filtered search; $O(\text{ef} \cdot \log N)$ in unfiltered search) even though `visited: HashSet<u64>` evaluates each layer-0 node at most once. Meanwhile `TAG_NODE2KEY` encodes `K` via `ValueEnDe` instead of `KeyEnDe` (forcing `K: KeyEnDe + ValueEnDe`), and unbounded `Txn::vec_cache` adds a 3rd in-memory copy of all vectors during `insert_batch` and `compact()`.

### v18 design

1. **Persisted `[TAG_META]` + monotonic 34-byte `[TAG_STATE]` + lock-free `get` / snapshot `search` & `iter`:**
   - Persist immutable schema (`dim: u32`, `scalar_tag: u8`, `hnsw_config`, `metric_tag_and_params`) at `[TAG_META]` and validate it on `from_meta` / `Deserialize` so mismatched metrics (e.g. `L2` vs `Cosine` under `VecDexDyn`, `Lp(2)` vs `Lp(3)`, `f32` vs `f64`) fail closed on open.
   - Persist mutable `GraphState` (`entry_point`, `max_layer`, `node_count`, `next_node_id`, default `ef_search` updated by `set_ef_search`) as a fixed 34-byte LE record at `[TAG_STATE]`, **preserving strictly monotonic `next_node_id` across `clear()`** rather than resetting to `0`.
   - **Read isolation vs `mmdb 4.x` snapshot lock cost:**
     - **`VecDex::get` stays 100% lock-free (no `db.snapshot()`):** Because `next_node_id` is strictly monotonic across both `remove()` and `clear()`, and `[TAG_VEC | node_id_be]` is immutable once written for a `node_id`, the two lock-free `DB::get` calls (`k -> node_id`, `node_id -> vec`) can **never** observe another key's vector (if a concurrent `remove`/`clear` lands in between, step 2 returns `None`, linearizing right after the delete without ever acquiring `DB::inner`).
     - **`search`, `search_with_filter`, `iter`, and `compact` capture one `MapxRawReadView` per call:** Multi-hop graph traversal and full iteration pin one snapshot so `[TAG_STATE]`, `[TAG_ADJ]`, and `[TAG_VEC]` come from one consistent LSM sequence. **Acknowledged `mmdb 4.x` cost:** `db.snapshot()` (`mmdb/src/db.rs:2444`) briefly locks `DB::inner`, which queues behind an in-flight `commit_sync()` WAL `fsync` on the same shard (amortized once per whole HNSW query rather than per visited node, and eliminated automatically if `mmdb` decouples `snapshot_seq` or WAL `fsync` from `DB::inner`).
   - Restored handles become coherent for sequential writers and concurrent readers (closing [audit.md](../audit.md#L22-L26) for `VecDex`; concurrent writers still require `&mut` exclusion), unlocking deep-copy `Clone`, `clone_in(&ns)`, and `MapReader<VecDex<...>>`.
2. **Unify `VecDex` and `VecDexDyn` via `&self` on `DistanceMetric<S>` without inner-loop branch regression:**
   - Change `DistanceMetric<S>::distance(&self, a: &[S], b: &[S]) -> S` to take `&self` (plus a default `dispatch_search` hook specialized once per query by `MetricKind` so dynamic metrics hoist the enum match **outside** `search_layer`), delete `dynamic.rs`, and define `pub type VecDexDyn<K, S = f32> = VecDex<K, MetricKind, S>;`.
3. **Sealed IEEE-754 LE vector bytes, zero-alloc distance scratch, `KeyEnDe`-only `TAG_NODE2KEY`, and bounded `insert_batch` cache:**
   - Seal `Scalar`'s wire format to IEEE-754 `f32` and `f64`, storing raw `dim * size_of::<S>()` LE bytes at `[TAG_VEC | node_id_be]` (saving `postcard`'s length-varint framing while **keeping `TAG_VEC` separate from `TAG_ADJ`** so `2 × m_max0 = 64` neighbor updates per `insert` never rewrite multi-KB vectors).
   - Decode unaligned `&[u8]` chunks via `as_chunks::<4>()` / `as_chunks::<8>()` (`from_le_bytes`) into one reusable `dim`-element scratch slice in `search_layer`, eliminating all per-visited-node `Vec<S>` / `Rc<Vec<S>>` / `HashMap` allocations (`mmdb` still returns one owned `RawValue` `Vec<u8>` per fetched vector).
   - Encode `TAG_NODE2KEY` with `KeyEnDe::encode_key(&k)` (dropping the redundant `K: ValueEnDe` bound while keeping 1-byte `TAG_INFO` separate so disconnected-graph ID fallback scans never read large user keys). Bound `Txn::vec_cache` (e.g. 32 MiB) to cap `insert_batch` RAM; document that whole-index `compact()` still stages the rebuilt graph in memory unless paired with a fresh-prefix swap.

---

## Migration and preserved invariants

### Offline v17 to v18 conversion

The v18 runtime requires the current headers and row layouts. The independent `vsdb_migrate` crate converts source universes, saved raw and typed handles, single-handle indexes, version stores, standalone B+ trees and DAGs. See the migration guide for the functions, retry behavior, external root mappings and application-owned data. [Migration guide](../../migration/README.md)

### Preserved architectural invariants

- **Shard routing and universe model:** Default namespace stays at 16 shards (`NUM_SHARDS`); writable access remains single-process per universe (enforced by `__SYSTEM__/LOCK` as well as in-process `parking_lot` locks).
- **`Clone` on mutable collections vs immutable readers/snapshots:** `Clone` on every mutable collection handle (`Mapx*`, `Orphan`, `VerMap`, `DagMap*`, `SlotDex`, `VecDex`) deep-copies storage into a fresh prefix (`MptCalc`/`SmtCalc` `Arc`-COW `Clone` preserves identical isolated value semantics in memory). Cheap reference-counted read sharing uses explicit immutable types (`MapReader<M>`, `Snapshot<K, V>`), each retaining a `Namespace` clone (blocking `Namespace::close` until dropped; `mmdb` `range_detached` snapshots retain their documented source-refcount behavior in [ns-close.md](ns-close.md#L33-L38)).
- **Cryptographic wire parity:** `MptCalc` and `SmtCalc` node encodings, Keccak256 preimages, root hashes, and proof transport bytes remain bit-for-bit identical to v17 (verified against frozen non-empty membership/absence, odd-nibble, and compressed-path v17 fixtures).
- **Non-goals:** No cross-namespace or arbitrary cross-collection interactive transactions; no implicit per-insert `fsync` on ordinary writes.

---

## Recommended implementation sequence

1. **Pre-v18 (`v17.x` patch series — ship immediately, zero breaks):** Land the `[CRITICAL]` `SlotDex` runtime slot-width guard, `MmDB::range_at` `iter_with_prefix` + single-seek fix, `VecDex` per-op `[TAG_STATE]` reload, `atomic_write_file` trie checkpoints, `DagMapRaw` `data` co-location, and format-neutral `VerMap::write_batch` / single-descent `merge::replay` / streaming `bulk_load` / `'static` `Snapshot`.
2. **Stage 0 — Baseline & A/B benchmarks + golden `v17` fixtures (before format/API freeze):** Capture isolated Criterion baselines ([performance-roadmap.md](performance-roadmap.md#L151-L159), [audit.md](../audit.md#L64-L67)) and run an A/B prototype benchmark of §6 Mode A (persisted `[0x11]` `extra_refs` RMW) vs Mode B (compact internal-only `RefState` + verified `[0x07]` checkpoint) on cold/warm `from_meta()` latency, RSS, and 1,000-key block commit throughput over retained history. Freeze golden `v17` datasets for in-place/converter migration tests.
3. **Stage 1 — `vsdb_core` engine & error foundations:** Drop `ruc` (`pub(crate)` `mmdb::Error` mapping + typed `BranchId`/`CommitId` in `VsdbError`), unbox `MmdbIter` + in-place prefix strip + stack `with_full_key` + `RawRangeBounds` + `MapxRawBatch::delete_range`, and complete `MapxRawReader`.
4. **Stage 2 — Universe/engine headers, `VSTYPE04` envelopes, codecs, and basic collections:** Land decoupled atomic `VSUNI018` + `VSENG018` + `LOCK` + fail-closed `VSNREG02` + lazy `OnceLock` `DEFAULT_NS`, unified `VSTYPE04` + `VSMAPX02` (preserving `VSMAPX01`/`VSTYPE03` readers and updating `note_meta_bytes` + 23-byte `note_meta_stream` carry), 2-trait `KeyEnDe`/`ValueEnDe` + `KeyLookup`/`OrderedKeyLookup` + coherence-verified `OrderedRangeBounds`, `DerefMut`-only `ValueMut` (with `#[cfg(debug_assertions)]` interior-mutability detector) + `update`, safe `enum Entry`, dead `pub unsafe` pruning, and minimal `MapReader<M>` + `*ReadView`.
5. **Stage 3 — `SlotDex`:** Land `[u8; N]` `SlotType::Bytes` (with rank-space `floor_align`), `tier_capacity: u32`, zero-alloc stack helpers, bounded forward-chunked descending `page`/`iter`, common-ancestor `count`, and 16B `[total, generation]` (accepting 8B `v17` rows on read and preserving `generation` across `clear()`). Close `[CRITICAL] slotdex` in [audit.md](../audit.md).
6. **Stage 4 — `DagMap`:** Land single-handle shard-colocated `DagMapRaw` with `dag_id` + local `edge_id`s, fold-before-`Clearing` crash transitions using `[0x02]..[0x03]` `delete_range` + 1-shard final `sync_wal()`, `DagMapRaw::migrate_v17`, removal of `DagIdAllocator` / `from_bytes_in` / all 7 `unsafe` blocks, fixed `DagMapRawKey::new`, and typed `DagMap<K, V>`. Close `[MEDIUM] dagmap` in [audit.md](../audit.md).
7. **Stage 5 — `VecDex`:** Land `[TAG_META]` + monotonic 34B `[TAG_STATE]`, lock-free `get` + `MapxRawReadView` `search`/`iter`/`compact`, `&self` `DistanceMetric` unifying `VecDexDyn`, IEEE-754 LE vector slices + zero-alloc search scratch, `KeyEnDe`-only `TAG_NODE2KEY`, bounded `Txn::vec_cache`, and graph-preserving `VecDex::migrate_v17`. Update `[MEDIUM] cached indexes` in [audit.md](../audit.md).
8. **Stage 6 — `PersistentBTree` + `VerMap`:** Keep `PersistentBTree` `pub` with slotted `NodeView` + `#![forbid(unsafe_code)]`, land the single-handle big-endian `VerMap` store with the Stage 0 winner for ref persistence, `[0x05]` lock-free-across-`fsync` snapshot retirement + `[0x06]` multi-chunk staging anchors, open-time header/commit/ceiling verification (`versioned.md`), and prefix-preserving `VerMap::migrate_v17`.
9. **Stage 7 — `TrieCalc` + `VerMapWithProof` & release validation:** Land candidate-root `Arc`-COW `MptCalc`/`SmtCalc` + iterative `Drop` + POPCNT branches + inline-64/heap `Nibbles`, map-bound durable streaming checkpoints (`MPTC v2` / `SMTC v4`), unified `TrieCalc::Proof` + fallible `VerMapWithProof::merkle_root`, close `[LOW] trie` in [audit.md](../audit.md), and validate all `vsdb::migrate` paths against frozen `v17` fixtures and Criterion regression gates.

---

## 10. Post-Implementation Structural Performance Evaluation and Follow-Up Designs

Auditing the shipped `v18.0.0` (`cc2f41c`) implementation against the four validation reports ([v18-basic.md](../evaluations/v18-basic.md), [v18-slotdex-dagmap.md](../evaluations/v18-slotdex-dagmap.md), [v18-versioned.md](../evaluations/v18-versioned.md), [v18-trie-vecdex.md](../evaluations/v18-trie-vecdex.md)) shows that **the structural performance improvements were only partially evaluated** prior to code freeze. Several high-leverage structural wins were confirmed, while six major workload regimes were either untested in the initial benchmarks or stopped halfway in implementation — leaving severe write/read amplification or tail-latency stalls on realistic workloads.

All supplementary measurements below were collected in release mode (`rustc 1.100.0`, `mmdb 4.3.15`) on `cc2f41c` using isolated universes.

### 10.1 Coverage audit of v18 structural performance goals

| Subsystem & mechanism | Evaluated in `evaluations/v18-*.md` | Uncovered gap or regression found on `cc2f41c` | Follow-up track |
|---|---|---|---|
| **Basic maps & codecs** (§2–4): stack `with_encoded_key` / `with_full_key`, unboxed `MmdbIter`, `DerefMut` `ValueGuard` | Point hits (`Mapx` `-6.5%`, `MapxOrd` `-13.7%`), short-key allocs (`6 → 1`), untouched 64 KiB guard allocs (`35 → 3`, `-82%` bytes) | Range scan throughput unchanged (`77.6 µs` vs `78.1 µs` on 1k keys: `mmdb` still allocates `(RawKey, RawValue)` per row) | Upstream `mmdb` borrowed-slice cursor (`v19+`) |
| **`PersistentBTree` slotted `NodeView` & `[0x12]` `BLOB` split** (§6.1, §6.5) | Small-value (`<= 128 B`) `VerMap` point hit (`2.33 µs → 1.10 µs`, `-53%`) and 1M-key `u64` block commit (`24.54 ms`) | **`> 1 KiB` values (`BLOB_LEAF`) never benchmarked**; `self.node()` + `self.alloc()` re-reads and re-allocates all up to 32 sibling blobs on every leaf edit (`32×` blob I/O & versioned space amplification); write path still allocates `Vec<Vec<u8>>` per level | **§10.2** (format-preserving `v18.x` fix) |
| **`VerMap` single-prefix store & Mode B `[0x07]` `REFS`** (§6.1–6.4) | Mode B (`22.5 ms`) vs Mode A (`60.4 ms`) 1k-key block; warm reopen `30.19 ms` vs Mode A `0.30 ms`; branch lifecycle regressed `11.72 ms → 16.02 ms` (`+36.7%`) | `VerMap::get` re-reads `[HEAD \| branch]` from `mmdb` on every lookup (`+176–480 ns` over `Snapshot::get`); `create_branch` runs a redundant $O(\text{branches})$ `records(NAME)` scan on every non-duplicate call; `[0x07]` stores `counts` for all unique `0b01`/`0b11` leaf/blob objects; internal pages re-copied from `mmdb` on every descent | **§10.3** (`v18.x` caches + `Mode B+` shared-only refs) |
| **Tries & `VerMapWithProof`** (§7) | 1k-key `MPT`/`SMT` cold/warm hash (`-96%` warm), `batch_update` (`-32%` / `-18%`), 100k-entry `MPT` RSS (`-43%`) & checkpoint size (`-51%` / `-38%`) | **`VerMapWithProof` sync paths not benchmarked**: working-state `merkle_root(branch)` re-diffs all $K$ dirty keys from `committed_root` on every single edit ($O(K^2)$ cumulative work); `sync_committed` materializes full `Vec<TreeDiff>` even when `100%` of keys changed | **§10.4** (format-preserving `v18.x` fix) |
| **`VecDex` LE vectors, scratch buffer & unified `MetricKind`** (§9) | Unfiltered L2 `search` at `N = 1,000, dim = 128` (`503.55 µs → 304.47 µs`, `-39%`) | **Construction, selective filters, and `compact()` unmeasured**: `search_with_filter` slows **32.8×** at `1%` selectivity due to per-node `TAG_NODE2KEY` reads + `K::decode_key`; `insert_batch` drops `Txn::vec_cache` every 64 vectors and recomputes $O(M^2)$ pruning distances per neighbor; `compact()` holds the entire rebuilt graph in RAM | **§10.5** (`v18.x` + `v19` shadow-prefix compact) |
| **`SlotDex` fixed-width slots & forward-chunked `iter`** (§1) | Descending `iter` (`6.0–6.3×` faster) and descending `page` (`1.5–3.2×` faster); `wide_then_hot` batch shifted `65.15 ms → 79.15 ms` | `insert_batch` calls `staged.put(level_key(i + 1, &floor), ...)` inside the per-item loop (`> 30,000` redundant `Vec` allocs + `BTreeMap` probes on 12k hot-bucket items) even though `stage_level_growth_over` reads `bumps` | **§10.6** (format-preserving `v18.x` fix) |
| **`DagMap` single-shard co-location & engine memtable sizing** (§8, `mmdb_open`) | `prune` (`669.28 ms → 6.00 ms`, `111×` faster) and `destroy` (`34.33 ms → 7.33 ms`) under concurrent unrelated-map traffic | Concentrating an entire `VerMap` or `DagMap` onto **1 shard** hits `mmdb_open`'s `16 MiB` per-shard write buffer (`mem_budget / 8 / shards`, sized for an obsolete `1 + 4` immutable-memtable model unused in `mmdb 4.3.15`), causing **26.6 ms synchronous flush stalls** (`634×` median) | **§10.7** (`mmdb_open` calibration + placement guidance) |

---

### 10.2 `PersistentBTree`: descriptor-preserving `BLOB_LEAF` COW, $O(1)$-blob diff pruning, and slotted splice writes

#### Empirical diagnosis (`cc2f41c`, 1,024-key tree, `B = 16`)

While `NodeView` eliminated sibling `Vec` allocations on `get` and `BTreeIter`, every mutator (`insert_rec`, `remove_rec`, `fix_underflow`, `apply_leaf_run`) and `BTreeDiffIter` (`Cursor::expand`) still calls `self.node(id)` → `NodeView::materialize`, which eagerly reads **every** `[0x12]` `BLOB` referenced by a `BLOB_LEAF` into `Node::Leaf { values: Vec<Vec<u8>> }`. `self.alloc` then allocates **brand-new `[0x12]` `BLOB` IDs** for all `<= 32` entries in the leaf — even when 31 of the 32 entries were untouched. Furthermore, `fix_underflow` (`remove.rs:174, 180`) calls `self.node(sibling).key_count()`, reading all 32 blobs of a sibling leaf off disk solely to inspect `raw[1]`, and reads them a second time inside `borrow_left` / `borrow_right`.

| Value size | Leaf class | `VerMap::insert` median (p99) | 40-edit `diff_commits` | `BTree` 1-key overwrite | `BTree` 1-key `diff` | Underflow `remove` |
|---:|---|---:|---:|---:|---:|---:|
| `1,024 B` | Inline `LEAF` (`0b01`) | `66.1 µs` (`95.5 µs`) | `440.1 µs` | `48.8 µs` | `19.9 µs` | `30.3 µs` |
| `1,025 B` | `BLOB_LEAF` (`0b10`) | `101.8 µs` (`131.1 µs`) | `742.3 µs` | `85.7 µs` | `51.9 µs` | `28.2 µs` |
| `16,384 B` | `BLOB_LEAF` (`0b10`) | `583.9 µs` (`16,641.4 µs`) | `4,467.1 µs` | `982.7 µs` | `209.1 µs` | `229.2 µs` |
| `65,536 B` | `BLOB_LEAF` (`0b10`) | `2,049.4 µs` (`53,275.9 µs`) | `55,968.7 µs` | `2,028.7 µs` | `439.7 µs` | `891.8 µs` |

Crossing `1,024 B → 1,025 B` (+1 byte) adds +54% write latency and 2.6× diff latency; at `64 KiB`, 40 single-key working edits write `~80 MiB` of duplicate `[0x12]` rows instead of `~2.5 MiB`, triggering five synchronous `16 MiB` memtable flushes (`53.3 ms` p99 on an unsynced working `insert`) and forfeiting cross-commit structural sharing of unchanged sibling blobs.

#### Design specification (zero on-disk format change; `v18.x`-compatible)

1. **Descriptor-preserving leaf entries (`LeafVal`):**
   Replace `values: Vec<Vec<u8>>` in internal COW paths with:
   ```rust
   pub(crate) enum LeafVal {
       Inline(Vec<u8>),
       Blob(NodeId), // existing [0x12 | id_be] object; immutable by COW contract
   }
   ```
   - `NodeView::materialize_shallow(&self)` decodes `[0x10]` slot descriptors without reading `[0x12]` blobs (`desc & CLASS_MASK == BLOB` becomes `LeafVal::Blob(desc)`).
   - On `insert_leaf` / `apply_leaf_run` (`Ok(i)` overwrite): if `new_val.len() <= 1024` and old slot is `LeafVal::Blob(_)`, their lengths differ so they cannot be equal — replace with `LeafVal::Inline(new_val)` without reading the old blob! Only read `self.object(blob_id)` when `new_val.len() > 1024` to check byte equality (`Unchanged`).
   - In `PersistentBTree::alloc`: `LeafVal::Blob(existing_id)` writes `existing_id` directly into the new leaf's 8-byte slot descriptor and pushes `existing_id` into `edges` (incrementing `refs.counts[existing_id]` under `refs.ready`), allocating a fresh `[0x12]` object **only** for `LeafVal::Inline(v)` where `v.len() > 1024` (or `LeafVal::NewBlob(Vec<u8>)`). Single-key `BLOB_LEAF` edits drop from `32` blob reads + `32` blob writes to **`<= 1` blob read + `1` blob write**, and retained `VerMap` commits share all 31 untouched `[0x12]` blobs.
2. **Header-only sibling occupancy check in `fix_underflow`:**
   Add `PersistentBTree::key_count(&self, id: NodeId) -> usize` reading `NodeView::open(&self.object(id)).expect(...).len` (`O(1)` slice header read, 0 `Vec` allocations, 0 blob reads).
3. **Descriptor-equality pruning in `BTreeDiffIter`:**
   Carry `LazyVal { Inline(Vec<u8>), Blob(NodeId) }` in `diff::Item::Entry`. When `Step::Compare(Ordering::Equal)` compares two `Blob(a)` and `Blob(b)` entries with `a == b`, skip both in $O(1)$ with **zero `[0x12]` reads**; materialize blob bytes via `read_object` only when emitting `Added`, `Removed`, or `Modified` (`a != b`).
4. **Slotted page splice (`NodeView::splice`, optional Stage 2):**
   For single-key `insert` / `remove` without split/merge, serialize the new `[0x10]` page directly from `(old_page: NodeView<'_>, idx, op)` into one pre-sized `Vec<u8>` (`raw.len() ± delta`), copying the prefix and suffix payload slices in two `extend_from_slice` calls instead of allocating up to `32` temporary `Vec<u8>` key/value buffers at every tree level.

---

### 10.3 `VerMap`: `BranchHead` / `InternalPageCache` acceleration and `Mode B+` shared-only reference state

#### Empirical diagnosis (`cc2f41c`, `VerMap<u64, u64>`)

| Tree keys (`N`) | Height | `Snapshot::get` | `VerMap::get` | Per-lookup `[HEAD]` overhead | `flush()` (`[0x07]`) | Warm `from_meta` |
|---:|---:|---:|---:|---:|---:|---:|
| `5,000` | 2 | `452.4 ns` | `628.5 ns` | `+176.1 ns` (`+39%`) | `1.95 ms` | `0.20 ms` |
| `100,000` | 3 | `885.2 ns` | `1,313.2 ns` | `+428.0 ns` (`+48%`) | `2.18 ms` | `0.70 ms` |
| `500,000` | 4 | `1,178.5 ns` | `1,657.8 ns` | `+479.3 ns` (`+41%`) | `4.04 ms` | `3.20 ms` |

1. **Per-read `[HEAD | branch]` lookup & redundant `create_branch` scan:**
   `VerMap::get` and `contains_key` call `self.get_branch(branch)` (`mmdb::DB::get`) on every call, adding `176–480 ns` (+39–48%) over `Snapshot::get`. Separately, `VerMap::create_branch` (`map.rs:495-499`) evaluates `self.branch_id(name).is_some() || self.records(NAME).any(...)` — opening a full `[0x02..0x03)` range iterator over every branch whenever `name` is new, even though `VerMap::recover` already enforces 1-to-1 `NAME` ↔ `BY_NAME` parity on open and every writable branch op updates both in one atomic batch. (Combined with 4 synchronous `commit_sync()` WAL `fsync`s at `~2.0 ms` each, this accounts for the `11.72 ms → 16.02 ms` branch-lifecycle timing in [v18-versioned.md](../evaluations/v18-versioned.md#L44) where the preceding benchmark populated ~1,500 branches on the same map.)
2. **Internal-page re-reads and `Vec<u8>` page copies:**
   Each B+ tree level above the leaf costs `~300–400 ns` (`mmdb::DB::get` + fresh `Vec<u8>` copy of the 400–1,200 B internal page), even though internal pages (`id & CLASS_MASK == 0`) are **immutable forever** and all internal pages of a 1,000,000-key tree (`~1,050` pages) total **`~450 KiB`**.
3. **`Mode B` `RefState` footprint under single-tip and deep-history workloads:**
   `RefState.counts` currently inserts a `HashMap<NodeId, u32>` entry for **every** live object — including unique `0b01` inline leaves and `0b11` blobs (`ref_count == 1`), which make up `> 96%` of objects on single-branch or recent-window trees. At 1M keys (`~32k` leaves), warm `from_meta()` spends `30.19 ms` (`24.7–34.0 MiB` RSS) deserializing and degree-checking `[0x07]`; at 10M+ keys or thousands of retained commits, `counts` + `edges` grows past `150 MiB` and warm reopen scales linearly into hundreds of milliseconds.

#### Design specification

1. **Generation-stamped `BranchHead` table & $O(1)$ `create_branch` check (`v18.x`, no format change):**
   - Remove the redundant `|| self.records(NAME).any(...)` fallback from writable `create_branch` (relying on the verified `[0x03 | name]` `BY_NAME` index).
   - Cache `(generation: u64, heads: HashMap<BranchId, BranchHead>)` under `TreeRuntime` (updated on `publish` / invalidated if `Header.generation` advances via an alias), eliminating the extra `DB::get` on `VerMap::get` / `contains_key`.
2. **Byte-bounded immutable `InternalPageCache` on `TreeRuntime` (`v18.x`, no format change):**
   - Add an 8-way lock-sharded clock cache `NodeId -> Arc<[u8]>` on `TreeRuntime` for committed internal pages (`id & CLASS_MASK == 0`), bounded by bytes (default `min(ns_budget / 64, 16 MiB)`).
   - **Correctness & lifecycle:** Because `NodeId` serials are strictly monotonic and COW pages never mutate in place, cached `Arc<[u8]>` slices are valid for all branches, commits, rollbacks, and `Snapshot`s with **zero invalidation races**; `TreeRuntime` lives behind `Weak` in `TREE_RUNTIMES`, so dropping the map and its snapshots frees the cache automatically, and `release_node` / `gc` may opportunistically evict dead internal IDs. `lookup` borrows `&[u8]` from the cache hit without calling `mmdb::DB::get` or allocating a `Vec<u8>`, reducing warm tree lookups to **1 leaf read**.
3. **`Mode B+`: Shared-only (`extra_refs`) `RefState` + optional `[0x08]` ref-delta log (`v18.x` / `v19`):**
   - **Shared-only RAM representation (`v18.x`-compatible if `[0x07]` codec bumped or translated):** Every reachable object in a live tree has `ref_count >= 1`. Store only `extra_refs: HashMap<NodeId, NonZeroU32>` (`ref_count - 1` for nodes with `ref_count >= 2`) plus `edges: HashMap<NodeId, Box<[NodeId]>>` for internal/`BLOB_LEAF` nodes (or read `NodeView::edges` on disk only when a `ref_count == 1` internal/`BLOB_LEAF` page is retired). On a 1M-key single-branch tree, `extra_refs` shrinks from `~33,000` entries to **`0` entries** (and after 16 retained 1,000-key blocks, only shared interior/boundary nodes carry `extra_refs > 0`), cutting `RefState` RAM and `[0x07]` checkpoint size by **5–15×**.
   - **Bounded `[0x08 | gen_be]` ref-delta log (`v19` format extension):** Between periodic `[0x07]` base checkpoints (e.g. every 64 history commits or `flush()`), append compact `(NodeId, i32)` net ref-delta records at `[0x08 | generation_be]` inside the **existing** `publish` batch (**0 point `get`s** on commit, avoiding Mode A's 23,000-read penalty while bounding post-crash recount work to replaying `<= 64` delta frames).

---

### 10.4 `VerMapWithProof`: incremental working-branch cursor and adaptive streaming rebuild

#### Empirical diagnosis (`cc2f41c`, `N = 20,000` keys, 32 B values)

| Workload | Measured time (`cc2f41c`) | Behavior & bottleneck |
|---|---:|---|
| Full streaming `MptCalc::from_entries` + `root_hash` (`20,000` keys) | `15.49 ms` | $O(\text{height})$ B+ tree cursor memory; single bottom-up hash pass |
| `merkle_root_at_commit` incremental diff: `0.5%` (`100` keys) | `1.01 ms` | `15.3×` faster than full rebuild |
| `merkle_root_at_commit` incremental diff: `20%` (`4,000` keys) | `11.37 ms` | Approaches full-rebuild cost while allocating `Vec<TreeDiff>` |
| `merkle_root_at_commit` incremental diff: `100%` (`20,000` keys) | `16.17 ms` | Slower than `from_entries` **and** materializes all `20,000` `TreeDiff` + `ops` slices in RAM |
| Working-state `1 insert + merkle_root(br)` with `2,000` prior dirty keys | `1.69 ms / step` | Clones `committed_trie` and **re-diffs all `2,001` dirty keys on every call** ($O(K^2)$ across a block) |

#### Design specification (`v18.x`, zero format change)

1. **Incremental working-state cursor `(working_branch, working_base_commit, working_root)`:**
   In `VerMapWithProof`, store `working_branch: Option<BranchId>` and `working_base: Option<CommitId>` alongside `working_root: Option<u64>`:
   - When `merkle_root(branch)` observes `self.working_branch == Some(branch)`, `self.working_base == roots.head`, and `let Some(prev_working) = self.working_root` (provided `prev_working` is still leased via a retained `Arc<RootLease>`!), diff **`prev_working -> roots.working`** and apply only the newly edited keys directly onto a candidate clone of `self.working_trie` ($O(\Delta)$ per call instead of $O(K)$, turning 2,000 sequential `insert + merkle_root` steps from `~1.7 s` into `~15 ms`).
   - On `discard`, `rollback_to`, branch switch, or `commit`, fall back cleanly to `committed_trie.clone()`.
2. **Chunked streaming diff & adaptive full-rebuild fallback:**
   - Replace `let diff: Vec<TreeDiff> = self.map.diff_roots(...)` (`proof.rs:214, 262`) with streaming consumption of `BTreeDiffIter` in bounded chunks (e.g. `4,096` entries per `batch_update`), eliminating unbounded `Vec<TreeDiff>` + `Vec<(&[u8], Option<&[u8]>)>` peak memory on multi-million-key `bulk_load` commits.
   - When `from.root == EMPTY_ROOT` (initial `bulk_load` commit), immediately run `T::from_entries(self.map.tree.iter(root.root))` instead of diffing against an empty root.

---

### 10.5 `VecDex`: cross-chunk build cache, raw-key filter fast path, and bounded-memory compaction

#### Empirical diagnosis (`cc2f41c`, `N = 2,000`, `dim = 128`, `f32`, `L2`, `ef_search = 50`)

- **Construction & compaction:** Serial `insert`: `409.2 µs/vec` (`818.5 ms` total); `insert_batch` (`CHUNK = 64`): `283.2 µs/vec` (`566.4 ms` total); `compact()` (`1,500` live vectors): `341.6 ms`.
- **Filtered search degradation (`k = 10`):** Unfiltered `140.7 µs` → `50%` filter `277.1 µs` (`2.0×`) → `10%` filter `1,131.4 µs` (`8.0×`) → `1%` filter `4,610.0 µs` (**`32.8×`**).

Three structural bottlenecks explain these numbers:
1. `insert_batch` (`vecdex/mod.rs:865-874`) instantiates a fresh `Txn` every 64 vectors, dropping `Txn::vec_cache` (up to 32 MiB) at every chunk boundary so upper-layer navigation hubs are repeatedly fetched and decoded from `mmdb`. Meanwhile, `link_node` runs `prune_selection` (`select_neighbors_heuristic`, up to $33 \times 32 / 2 = 528$ pairwise distances) for every neighbor that overflows `m_max`.
2. Under `<= 1%` filter selectivity (`< ef` total matching nodes in the graph), `result.len() < ef` holds throughout traversal, so `search_layer` evaluates `passes(n_id)` — issuing a `MapxRawReadView::get(TAG_NODE2KEY)` **and** `K::decode_key` — on every visited node up to `filter_visit_cap(ef)`.
3. `compact()` rebuilds the entire index inside one `Txn` + one `StagedRows` batch (`~3 × N × dim × sizeof(S)` peak RAM).

#### Design specification

1. **Cross-chunk `vec_cache` retention & lazy degree check in `insert_batch` (`v18.x`, no format change):**
   Lift the 32 MiB `HashMap<u64, Rc<Vec<S>>>` cache above the `for chunk in dedup.chunks(64)` loop (evicting only on byte-cap overflow; `node_id`s are monotonic and immutable!). In `link_node`, read neighbor count (`raw.len() / 8`) before allocating `Vec<u64>` when `< m_max`, and skip `prune_selection` whenever `n_neighbors.len() <= m_max`.
2. **Zero-decode `search_with_raw_filter` & predicate ordering (`v18.x` additive API):**
   Add `search_with_raw_filter(&self, query: &[S], k: usize, pred: impl Fn(&[u8]) -> bool)` (and `VecDexReadView` equivalent) inspecting raw `TAG_NODE2KEY` bytes without `K::decode_key` heap allocation for prefix/tenant/range filters, while keeping `K::decode_key` only for the final `<= k` winners.
3. **Bounded-memory shadow-prefix `compact()` (`v19` handle indirection or documented operational pattern):**
   Single-prefix crash-atomic `compact()` cannot split its `batch_wiped()` commit across multiple batches without leaving a half-built index on crash. For `v18.x`, document the `~3 × raw_vector_bytes` RAM ceiling on in-place `compact()` and recommend streaming `iter()` into a fresh `VecDex::insert_batch` + atomic `save_meta` swap for multi-million-vector indexes; for `v19`, an optional 1-byte active-generation prefix tag (`[0x00 | gen | ...]` vs `[0x05]` active `gen`) allows `compact()` to stream 64-node chunks into `gen ^ 1`, flip `[STATE_KEY]` atomically, and `delete_range` old `gen` afterward with $O(\text{chunk})$ RAM.

---

### 10.6 `SlotDex`: coalescing tier-bucket updates in `insert_batch`

#### Diagnosis & design (`v18.x`, zero format change)

In `SlotDex::insert_batch` (`slotdex/mod.rs:607-624`), every input item loops over all `i in 0..num_tiers` and executes `staged.put(level_key(i as u8 + 1, &floor), encode_cnt(*v).to_vec())` — performing `N × num_tiers` `Vec<u8>` allocations and $O(\log N)$ `StagedRows` `BTreeMap` updates (`> 30,000` redundant updates on the 12,049-item `wide_then_hot` benchmark, `11.6 ms` warm / `79.15 ms` Criterion with `clear`), even though `stage_level_growth_over` (`mod.rs:818-830`) reads **`bumps`**, never `staged`, for `level >= 1`!

- **Fix:** Remove `staged.put(level_key(i as u8 + 1, &floor), ...)` from the per-item loop and emit one `staged.put` per distinct `((i, floor), v)` in `bumps` after the loop (lines 641–643). Similarly, track pending `level 0` slot counts in a typed `BTreeMap<S, EntryCnt>` (`stack`-sized `S` keys, 0 `Vec<u8>` allocs) and flush `level 0` rows into `staged` once at `tier 0 → 1` promotion (if any) and once at batch end. All persisted bytes and tier-promotion boundaries remain bit-for-bit identical.

---

### 10.7 Engine (`mmdb 4.3.15`): recalibrating `wr_buffer_size` for single-shard `VerMap` and `DagMap`

#### Empirical diagnosis (`cc2f41c`, 60 MiB sustained write = 1,500 × 40 KiB batches into one prefix)

| Placement | Per-shard `write_buffer_size` | Total time | Median batch | p99 batch | Max batch stall |
|---|---:|---:|---:|---:|---:|
| Default namespace (`16` shards, `2 GiB` budget) | `16 MiB` | `127.4 ms` | `42.0 µs` | `110.8 µs` | **`26,624.9 µs` (`26.6 ms`)** |
| Dedicated namespace (`1` shard, `2 GiB` budget) | `256 MiB` | `87.4 ms` (`-31%`) | `53.9 µs` | `216.9 µs` | **`306.0 µs` (`0.31 ms`, `87×` lower)** |

In `mmdb_open` ([mmdb.rs](../../core/src/common/engine/mmdb.rs#L1425-L1451)), `wr_buffer_size` is clamped to `mem_budget / 8 / shards` (`16 MiB` at default `2 GiB / 16` shards, and `16 MiB` at non-default `512 MiB / 4` shards) based on the comment that each shard holds `1 + max_immutable_memtables (4) = 5×` memtables in RAM. In `mmdb 4.3.15`, `max_immutable_memtables` is unused: when the active memtable fills, the write leader synchronously runs `freeze_memtable_sync` + `flush_frozen_memtable` (`<= 2×` memtable residency during flush). Because v18 consolidated all 8 `VerMap` components onto **1 prefix** and all connected `DagMap` nodes onto **1 co-located shard**, a single hot `VerMap` or `DagMap` in the default namespace hits synchronous `16 MiB` flush stalls (`26.6 ms` max latency spike) 16× earlier than its `2 GiB` budget suggests.

#### Design & operational guidance

1. **Immediate operational pattern (zero code change):** Place write-intensive `VerMap`, `DagMap`, `SlotDex`, or `VecDex` instances in a dedicated `Namespace::create_with(NamespaceOpts { path: None, shards: 1, mem_budget_mb: Some(B) })` so the collection receives the full namespace write buffer and compaction worker without altering `DEFAULT_NS`'s pinned 16-shard routing.
2. **Engine sizing recalibration (`v18.x` tuning gate):** Update the obsolete `5×` memtable comment in `mmdb_open` and run a multi-shard peak-RSS + L0-stall validation comparing `mem_budget / 8 / shards` (`16 MiB` default) against `mem_budget / 4 / shards` (`32 MiB` default, `<= 50%` worst-case active+flushing memtable footprint across all 16 shards simultaneously, leaving `12.5%` for `BlockCachePool` and `37.5%` for compaction/allocator headroom). Do not alter `NUM_SHARDS = 16`.
