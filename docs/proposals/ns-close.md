# RFC: In-Process Namespace Close — Ownership-Inverted Engine Lifecycle

- **Status**: implemented (v16.1.0)
- **Prerequisite**: [namespaces.md](./namespaces.md) (implemented, v16.0.0+)
- **Supersedes**: the P3 `close()` sketch in namespaces.md §6, which assumed a
  `&'static DB` → `Arc<DB>` migration. That migration is **not needed**.

---

## 1. Motivation

v16 namespaces are leak-forever: every `Namespace::open`/`create` calls
`Box::leak` on the engine, so an open namespace can never release its
resources within the process lifetime. Consequences:

1. **No in-process rotation.** The natural lifecycle for time-partitioned
   data — `create → fill → close → destroy` — requires a process restart
   between *close* and *destroy*, because `vsdb_ns_destroy` (correctly)
   refuses to remove an open namespace.
2. **Unbounded idle cost at scale.** Each open namespace parks `shards`
   condvar-waiting compaction threads plus fds. Dozens-to-hundreds of
   namespaces need nothing; *thousands* of sequentially-used namespaces
   accumulate threads and fds that can never be reclaimed.

Goal: a `close()` that fully reclaims an idle namespace — memory, threads,
fds, mmdb `LOCK` files — without restart, without weakening any v16
correctness guarantee, and without poisoning live handles.

## 2. Design evolution — three candidate schemes

Three schemes were evaluated in sequence; each round removed a layer of
mechanism. Facts about mmdb verified during evaluation (mmdb source,
current master):

- `DB::close(&self)` already exists (`src/db.rs:2287`): CAS on a
  `closed: AtomicBool`, flush of the active memtable, WAL sync with error
  propagation. Idempotent.
- Every DB operation already fail-fasts through `check_usable()`, which
  returns `Error::db_closed()` once the flag is set — the per-op atomic
  load is *already paid*.
- `DB::drop` performs the full teardown: WAL sync (skipped if `closed`
  already set), compaction-thread shutdown + join (lost-wakeup safe), and
  flock release via the `_lock_file` field drop.

### Scheme A — status flag on a leaked shell

Keep `Box::leak`; add an explicit shutdown that joins threads and releases
the LOCK while the leaked `DB` shell stays allocated forever. Stale handles
hit `db_closed` errors (vsdb layer: panic).

- Pros: smallest diff; mmdb needs only a `&self` shutdown variant (~20
  lines — thread join + flock release currently live only in `Drop` and the
  test-only `simulate_crash`).
- Cons: leaks a shell per close/reopen cycle; introduces *poisoned-handle*
  semantics (live handles keep compiling but panic at runtime); close
  under live handles is a silent behavioral cliff.

### Scheme B — `Box::from_raw` reclamation

`Box::from_raw` is the std-documented inverse of `Box::leak`. Reclaim the
shell after proving exclusivity: every public path to an engine
(collections, iterators, `shadow()` — which goes through `ns.clone()`,
`engine/mod.rs:119`) transitively holds the `Arc<NsInner>`, so
`Arc::strong_count == 1` under the registry lock proves no alias exists.

- Pros: zero mmdb changes; no leaked shells; refuse-don't-poison semantics.
- Cons: raw-pointer bookkeeping (must retain the original leak pointer for
  provenance); an unsafe reconstruction ritual at close; the
  `&'static Engine` handout remains a type-system lie that only review
  discipline keeps sound.

### Scheme C (accepted) — don't leak at all

If exclusivity must be proven before reclamation anyway (Scheme B), the
leak buys nothing: **invert ownership so the `Arc` itself owns the engine**,
and let `Drop` do what `from_raw` was hand-rolling. The `'static` handout
then disappears entirely — the whole engine-reference surface becomes
lifetime-honest.

| | A: status flag | B: leak + `from_raw` | **C: ownership + RAII** |
|---|---|---|---|
| unsafe | ±0 | +2 audited sites | **net removal** (both leak sites deleted) |
| shell leak per cycle | ~KB | none | none |
| stale handles | panic (poisoned) | impossible (refused) | impossible (refused) |
| invariant held by | convention | convention + count check | **compiler (lifetimes)** |
| mmdb changes | ~20 lines | zero | zero |
| diff width | smallest | small | moderate (lifetime plumbing) |

Scheme C costs a moderately wider diff but is the only one where the
soundness invariant is *type-enforced* rather than *review-enforced* — the
right trade for a database. Accepted.

## 3. Final design

### 3.1 Ownership inversion

The complete production `'static` surface is six sites, all in `vsdb_core`
(strata has zero engine references — it is untouched):

| # | Site | Today | After |
|---|------|-------|-------|
| 1 | `NsInner.engine` (namespace.rs:456) | `&'static Engine` + `Box::leak` (namespace.rs:688) | `engine: Engine` (owned inline; `Arc` heap allocation pins the address) |
| 2 | `Namespace::engine()` (namespace.rs:663) | `-> &'static Engine` | `-> &Engine` (borrow of `self`) |
| 3 | `ValueIterMut.engine` (engine/mod.rs:633) | `&'static Engine` | `ns: Namespace` (`Arc` clone — a streaming iterator's `&mut self` cannot reborrow its handle for `'a`, so the stored reference rides with its anchor instead) |
| 4 | `MmDB.dbs` (mmdb.rs:141) | `Box<[&'static DB]>` + per-shard `Box::leak` (mmdb.rs:236) | `Box<[DB]>` (owned) |
| 5 | `MmDB::shard()` (mmdb.rs:244) | `-> &'static DB` | `-> &DB` |
| 6 | `MmdbIter` (mmdb.rs:422) | `Box<dyn …>` (implicit `+ 'static`) | **unchanged** — mmdb's `DBIterator` is fully-owning (no lifetime parameter; it pins its SST/memtable sources via internal refcounts), so the boxed iterator never borrows the `DB` and `range_detached` keeps its detached semantics |

Both `Box::leak` sites are deleted. Engine teardown becomes a plain drop
cascade: last `Arc<NsInner>` → `NsInner` → `Engine` → each `DB` (whose
`Drop` joins compaction threads and releases the flock).

No lifetime plumbing was needed beyond two borrowck-friendly
`materialize()` hoists: public iterators already carry lifetimes
(`MapxRawIter<'_>`, mapx_raw/mod.rs:264) and the engine-level iterators
(`MapxIter<'a>`/`MapxIterMut<'a>`, engine/mod.rs:561, 585) bind them via
`PhantomData`. The test-only leak (mmdb.rs:1509) became a plain owned
binding — tests improve for free.

A consequence of #6 worth stating: a detached snapshot iterator holds its
engine sources via mmdb-internal refcounts, not through the `Namespace`
handle, so one may outlive a `close()` and keep yielding its (consistent,
stale) snapshot. Memory-safe by construction; documented on
[`vsdb_ns_close`].

### 3.2 Default namespace uniformity

Today the default engine is owned by the `VSDB` static (`VsDB { db: Engine }`,
common/mod.rs:194) and `DEFAULT_NS` borrows a true `'static` from it
(namespace.rs:258). With `NsInner` owning its engine, ownership moves into
`DEFAULT_NS` itself:

- `DEFAULT_NS: LazyLock<Namespace>` constructs and owns the default engine
  inside its `NsInner` — one uniform shape for all namespaces.
- The `VSDB` static keeps its public surface by delegating to `DEFAULT_NS`
  (or dissolves into it if nothing external needs it — implementation
  detail).
- A static never drops its value ⇒ the default engine still lives for the
  process lifetime, with **zero** special-casing in the lifecycle code.
  `vsdb_ns_close(DEFAULT_NS_ID)` is additionally refused up front, matching
  `destroy`/`relocate`.

### 3.3 `close()` protocol

New public API, joining the existing admin family (`vsdb_ns_list`,
`vsdb_ns_destroy`, `vsdb_ns_relocate`):

```rust
/// Close an open namespace, releasing all of its resources
/// (memory, compaction threads, fds, mmdb LOCK files).
///
/// Fails unless the caller has dropped every handle first:
/// all collections, iterators, and `Namespace` clones.
/// Refused for the default namespace.
pub fn vsdb_ns_close(id: NsId) -> Result<()>;
```

Algorithm (all under `REGISTRY_LOCK`, serializing against
`open`/`destroy`/`relocate`):

```text
1. id == DEFAULT_NS_ID            → Err (never closeable)
2. OPEN_NAMESPACES entry missing  → Err (not open)
3. Arc::strong_count(entry) > 1   → Err("N live handles") — entry stays
4. remove entry                   → we hold the sole Arc
5. for each shard: DB::close()    → flush memtable + WAL sync;
                                    collect the first error, keep going
6. drop the Namespace             → cascade: threads joined, flocks
                                    released, memory freed
7. return the collected result
```

**Check-before-remove** (step 3 before step 4) eliminates any
reinsert-on-failure dance. The count check is TOCTOU-free: cloning an `Arc`
requires an existing reference, and `count == 1` proves the registry holds
the only one; concurrent `open(id)` is excluded by `REGISTRY_LOCK`.

Step 5 runs before the drop so WAL-sync errors surface to the caller
(`DB::drop` cannot report them; it also skips the redundant sync because
`close()` already set the `closed` flag). Even when step 5 errors, step 6
still runs — the on-disk state is whatever was durably synced, the error
tells the operator, and a subsequent `open(id)` recovers from disk exactly
like a restart.

### 3.4 Rotation loop (the payoff)

```rust
let ns = Namespace::create()?;                    // epoch begins
let log = ns.scope(|| MapxOrd::<u64, Event>::new());
// ... fill ...
drop(log);
let id = ns.id();
drop(ns);
vsdb_ns_close(id)?;                               // full reclaim, in-process
vsdb_ns_destroy(id)?;                             // O(1) bulk delete — no restart
```

`destroy`'s existing not-open check composes with `close` unchanged: after
a successful close the namespace *is* not-open, and no zombie engine can
exist (close is deterministic-or-refused). Reopen after close
(`Namespace::open(id)`) constructs a fresh `NsInner`; persisted
`InstanceId` metas resolve exactly as after a restart.

## 4. Invariants

- **INV-NSC1 (anchored borrows)**: every engine reference reachable from
  user code is a borrow whose lifetime is bounded by a live `Arc<NsInner>`
  (handle field or iterator borrow chain). *Enforced by the compiler* —
  there is no `'static` engine reference left to escape.
- **INV-NSC2 (exclusivity proof)**: `strong_count == 1` observed under
  `REGISTRY_LOCK` is stable — no thread can clone an `Arc` it holds no
  reference to, and `open` is excluded by the lock.
- **INV-NSC3 (refuse, don't poison)**: `close` never invalidates a live
  handle. It either reclaims a provably-unreferenced namespace or returns
  an error naming the live-handle count. No poisoned-handle state exists.
- **INV-NSC4 (default ns immortal)**: the default namespace is never
  closeable; its engine is owned by a static and drops never.
- **INV-NSC5 (durable close)**: a successful `close` implies the active
  memtable was flushed and the WAL synced (`DB::close` semantics); a failed
  close still tears down, reports the first error, and leaves the
  namespace re-openable with restart-equivalent recovery.

## 5. Non-goals

- **Force-close / poisoned handles** — rejected with Scheme A. Live handles
  block close by design.
- **Runtime relocation** — rejected outright in namespaces.md §6 P3;
  unchanged here. `relocate` remains an offline registry-pointer repair.
- **Handle-leak rescue** — `mem::forget`-ed handles pin the count above 1
  forever; such a namespace can never be closed (only destroyed after
  restart). This is the fail-safe direction and is documented, not worked
  around.
- **Downgrade support** — unaffected; this RFC changes no on-disk format.

## 6. Concurrency notes

- **Spurious refusal**: `vsdb_flush_all_open` clones handles out of the
  registry lock before flushing, transiently raising counts; a concurrent
  `close` may be refused. Rare, loud, and retryable — documented, not
  serialized away.
- **Lock hold during join**: step 6 joins condvar-parked compaction threads
  while `REGISTRY_LOCK` is held, briefly blocking other admin/open calls.
  Joining parked threads is fast; `close` is an admin-path operation, and
  holding the lock is what makes `open(id)`-during-close impossible (the
  alternative — dropping the lock first — would surface as a confusing
  transient flock failure inside `open`).
- **mmdb flock as backstop**: unchanged; a hypothetical double-open across
  processes is still refused by the per-shard `LOCK` file.

## 7. Compatibility & versioning

- No on-disk change of any kind (format marker stays 16).
- No public-API breakage: `Namespace::engine()` is `pub(crate)`;
  `MmdbIter` is crate-internal; public iterator types already carry
  lifetimes. The only visible change is the *added* `vsdb_ns_close`.
- Minor bump: **v16.1.0**.
- mmdb: **zero changes required** (verified against `DB::close`,
  `check_usable`, `Drop`).

## 8. Implementation plan

1. **Ownership inversion** (core/engine): `MmDB { dbs: Box<[DB]> }`,
   `shard() -> &DB`, delete the per-shard leak (`MmdbIter` needs no
   change — see §3.1 #6).
2. **Namespace ownership** (core/namespace): `NsInner { engine: Engine }`,
   `engine() -> &Engine`, delete the leak; move default-engine ownership
   into `DEFAULT_NS`, delegate `VSDB`.
3. **`vsdb_ns_close`** (core/namespace): protocol of §3.3; re-export via
   `vsdb::common` like the other admin fns.
4. **Docs**: rustdoc for the drop-all-handles contract; CHANGELOG;
   namespaces.md P3 bullet updated to point here; `.claude/docs/patterns/
   engine.md` close-lifecycle notes.
5. **Tests** (integration, parallel-safe):
   - rotation loop: create → fill → drop handles → close → destroy →
     recreate, twice, asserting full data visibility each epoch;
   - refusal matrix: live collection handle / live `Namespace` clone /
     inside own `scope` / default ns / not-open id;
   - reopen-after-close resolves persisted `InstanceId`s;
   - close-error surfacing is covered by mmdb's own suite (`DB::close`
     error propagation); vsdb asserts the happy path plus refusals.

Estimated scope: ~150–250 lines changed in `vsdb_core`, zero in `strata`,
zero in mmdb.

## 9. Open questions

None blocking. Deferred niceties: a consuming `Namespace::close(self)`
sugar (needs an ergonomic story for returning the handle on refusal), and
whole-ns `merge` (unrelated, stays P3 in namespaces.md).
