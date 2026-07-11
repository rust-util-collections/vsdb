# RFC: Namespaces — Multiple Engine Instances per Process

- **Status**: implemented (v16.0.0–v16.0.2); in-process `close()` shipped in
  v16.1.0 ([ns-close.md](./ns-close.md)); cross-ns copy helpers (`clone_in`)
  and consuming `Namespace::close(self)` shipped in v16.2.0; remaining P3:
  whole-ns `merge`. Final design: rev 10 (original bullets evaluated in §5; rev 2: one
  **global** prefix allocator, not per-namespace — §4.6; rev 3: downgrade
  support dropped — §7; rev 4: single-process contract — §4.7; rev 5: meta =
  optional `ns_id` suffix — §4.3; rev 6: ns path optional, derived — §4.1;
  rev 7: scoped ambient placement — §4.1; rev 8: namespaces are anonymous
  placement groups, co-location via `handle.namespace()` — §4.1; rev 9:
  public identity enriched to `InstanceId { map_id, ns: Option<NsId> }`,
  mirroring the meta wire format; `From<u64>` keeps bare ids working — §4.1;
  rev 10: default ns id fixed at 0, registry holds non-default only, no
  probing anywhere — `from_meta` fully deterministic — §4.5)
- **Target**: v16.0.0 (major bump — see §7)
- **Scope**: `vsdb_core` engine layer + mechanical plumbing through every `strata` collection

---

## 1. Motivation

Today a process owns exactly one VSDB universe: one `VSDB` global static, one base
dir, one prefix allocator, 16 mmdb shards. This proposal adds **namespaces**:
independently-rooted engine instances that coexist in one process.

What this buys:

1. **Physical placement** — each namespace has its own base dir, so hot/cold data
   can live on different disks (NVMe vs ZFS volume, etc.).
2. **Hard isolation** — collections in different namespaces share no data-path
   state: no WAL, no compaction queues, no memtable budget contention. (The
   only shared components are cold-path metadata: the prefix-ID allocator
   §4.6, the registry §3, and the DagMap ID ceiling §4.5.)
3. **Instant bulk reclaim** — `destroy(ns)` is `rm -rf` of a directory tree.
   For time-partitioned data (rotate a namespace per epoch, destroy expired
   ones) this is far cheaper than LSM range-tombstone + compaction reclaim.
4. **Smaller per-tree working sets** — splitting a monotonically-growing dataset
   across namespaces keeps each LSM tree shallower (fewer levels → lower read
   and compaction amplification per tree). Honest caveat: total work on the
   same disk is similar; the real wins are placement, isolation, and reclaim.
5. **Test isolation** — each test can own a throwaway namespace (see §8).

Default behavior stays byte-for-byte identical to v15: one implicit default
namespace, zero new ceremony for existing code.

**Constraint zero — the founding philosophy.** VSDB's global-singleton design
was a deliberate trade: the API must feel like an in-memory data structure
(`Mapx::new()`, done), or users may as well use rocksdb/mmdb directly.
Namespaces must not tax that. The consequence (rev 8): a namespace is an
**anonymous placement group** — users never name one, never persist an id for
one, never pass a path. They express the only thing they actually mean —
*"put this data together with that data"* — via `existing.namespace()` +
`new_in`/`scope`, and recovery rides the identifiers users already persist
today (serialized handles / `instance_id`s). Paths/shards/budgets exist only
as opt-in advanced knobs. Any API where the simple case requires threading
parameters through call sites is rejected by construction.

---

## 2. Current-Architecture Constraints (verified)

These facts shape the design; each was checked against the code.

| # | Fact | Where | Consequence |
|---|------|-------|-------------|
| C1 | One global engine: `pub static VSDB: LazyLock<VsDB>`; shards are `[&'static DB; 16]` via `Box::leak` | `core/src/common/mod.rs:135`, `engine/mmdb.rs:100` | Engines are leak-forever; iterators (`MmdbIter(Box<dyn DoubleEndedIterator>)`, no lifetime) silently depend on `'static`. In-process `close()` was deferred (§9) on the assumption it required an `Arc` migration — later disproved: [ns-close.md](./ns-close.md) (v16.1.0) removed the leak entirely with plain `Arc`-owned engines. |
| C2 | Prefix allocator state is **module-global** (`GLOBAL_COUNTER/CEILING/FLOOR`, `PENDING_WINDOWS`, `RECOVERED_PREFIXES`, `thread_local LOCAL_NEXT/LOCAL_CEIL`); the ceiling persists in default shard 0 | `engine/mmdb.rs:50-82, 136-146` | **Stays global by design** (§4.6): one allocator serves all namespaces, so prefixes are unique across the whole registry and none of the in-memory machinery changes. Only the ceiling's *persistence* moves out of shard 0, to decouple allocation from the default engine. |
| C3 | Handle meta is hand-encoded, not a serde struct: `"VSMAPX01" ‖ prefix_le(8)` = 16 bytes | `engine/mod.rs:30, 308-333` | The `Option<NamespaceId>` idea cannot be a serde field (there is no struct), but it applies at the **byte level**: an optional trailing `ns_id`, absent ⇔ `None` ⇔ default ns (§4.3). Strata's `VSTYPE02` typed wrapper transports the inner bytes opaquely — unaffected. |
| C4 | Shard routing is `prefix % NUM_SHARDS`, `NUM_SHARDS = 16` hard-coded | `engine/mmdb.rs:84-86, 165-167` | Shard count must become a **per-namespace, creation-time-persisted** property: changing it under an existing dir silently re-routes every prefix. |
| C5 | Instance metas, trie caches, and the DagMap ID ceiling are keyed by bare `u64` under **global** dirs (`__SYSTEM__/__instance_meta__/{prefix:016x}`, `mpt_cache_{id}.bin`, …) | `core/src/common/mod.rs:87-110`, `strata/src/trie/mod.rs:256,443`, `strata/src/dagmap/mod.rs:103` | With per-ns allocators (every ns restarting from `4096_0000`) these bare-`u64` keys would collide — the global allocator (§4.6) removes the collision class outright. Per-ns `__SYSTEM__` trees are adopted anyway, for destroy-hygiene, locality, and structural robustness (§4.5). |
| C6 | Memory budget (`VSDB_MEM_BUDGET_MB` / detected) is computed per-process and divided by 16 shards at open | `engine/mmdb.rs:740-900` | Naively applying the formula per namespace multiplies memory by the number of open namespaces → OOM hazard. Needs explicit per-ns budgets (§4.4). |
| C7 | mmdb takes an exclusive `flock` (LOCK file) per shard dir | `mmdb/src/db.rs:198-326` | An **inherited tripwire**, not a coordination mechanism: double-opening the same shards (an app/ops bug) fails cleanly at open instead of corrupting via dueling WAL writers/compactions. The RFC itself adds **no locks of any kind** (§4.7). |
| C8 | Atomic write batches are per-prefix (→ per-shard); SlotDex/VecDex are single-handle for exactly this reason | `engine/mod.rs:243-256`, `strata/src/common/staged.rs` | Unchanged: one map = one prefix = one shard of one namespace. Namespaces add no new atomicity constraint — but **cross-namespace atomicity does not exist** and must be documented loudly. |
| C9 | The word "namespace" is already used for per-prefix key ranges ("prefix namespace") | `engine/mmdb.rs:480,537` | Rename those doc comments to "prefix range" to free the term. |

---

## 3. Design Overview

A **`Namespace`** is a cheap, cloneable handle (`Arc` inside) to an engine
instance: its own base dir, mmdb shards, and `__SYSTEM__` tree. Prefix IDs
come from one **registry-wide allocator** (§4.6). The default namespace
(`DEFAULT_NS_ID = 0`, a fixed constant — never allocated, never registered,
never looked up) wraps today's global engine and directory logic unchanged.

Identity is split deliberately (this resolves the ID-vs-path question, §5.6):

- **`NsId` (`u64`)** — small, stable, allocated once at creation, never
  reused. **Not a user-facing name** (rev 8): it is the routing token
  embedded in persisted handle metadata, surfacing in user code only at the
  admin tier (`list`/`destroy`/`relocate`, epoch-rotation bookkeeping). A
  namespace is otherwise reached the way any in-memory structure is — through
  the object graph: `existing_map.namespace()`.
- **Path** — *configuration*, not identity, and **optional** (rev 6): omitted,
  it derives from the id — `{default_base}/__NAMESPACES__/{ns_id:016x}/` —
  and is recorded base-relative, so the whole universe stays movable as one
  tree. An explicit path (e.g. a dir on another volume) is stored absolute
  and pinned; moving that volume = one registry update (`relocate`), and
  every persisted handle keeps working. Embedding paths in handles would
  duplicate environment-specific strings into every meta and make relocation
  impossible.

**Registry** — `{default_base}/__SYSTEM__/__namespaces__`: the single mapping
`NsId → (path: Option<PathBuf>, shards, created_at)` (`None` = derived),
written via the existing
`atomic_write_file`, serialized by an in-process mutex (registry mutations are
rare cold-path ops; §4.7). Deserializing a handle
that references a not-yet-open namespace auto-opens it through the registry;
a missing/dead entry surfaces as a `Decode`-class error with an instructive
message, never a panic.

---

## 4. Detailed Design

### 4.1 Public API surface

```rust
// ---- vsdb_core::common ----
pub type NsId = u64;
/// Fixed forever; never allocated, never in the registry. The default
/// namespace needs no lookup of any kind: `default_ns()` is infallible,
/// zero-I/O, and independent of the registry (which stores non-default
/// namespaces only; allocation starts at 1).
pub const DEFAULT_NS_ID: NsId = 0;

/// The complete public identity of a collection instance — the same shape
/// at every layer: in-memory comparison, the persisted meta bytes (§4.3),
/// and this token. `ns: None` ⇔ default namespace ⇔ the 16-byte meta form.
/// Display/FromStr: "42" (default ns) / "42@7" (ns 7) — config/log friendly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstanceId {
    pub map_id: u64,        // the prefix — what v15 called `instance_id`
    pub ns: Option<NsId>,
}
impl From<u64> for InstanceId { /* {map_id, ns: None} — v15 tokens work as-is */ }

#[derive(Clone)]
pub struct Namespace(/* Arc<NsInner { id, path, engine, opts }> */);

impl Namespace {
    /// The implicit default namespace (today's global engine).
    pub fn default_ns() -> Namespace;

    /// Start a NEW placement group: fresh NsId on every call; root
    /// derived from the id — {default_base}/__NAMESPACES__/{ns_id:016x}/
    /// — collision-free (ids are never reused). Zero parameters.
    pub fn create() -> Result<Namespace>;
    pub fn create_with(opts: NamespaceOpts) -> Result<Namespace>;

    /// Admin/advanced tier only: open by the stable id (obtained from
    /// `Namespace::id()` / `vsdb_ns_list()`, e.g. epoch-rotation
    /// bookkeeping). `open(DEFAULT_NS_ID)` short-circuits to
    /// `default_ns()` without touching the registry. Normal flows never
    /// call this — deserialization and `from_meta` auto-open namespaces
    /// via the ids embedded in metas.
    pub fn open(id: NsId) -> Result<Namespace>;

    /// Scoped ambient placement: inside `f`, plain `MapxXXX::new()`
    /// creates its storage in `self`. Creation-time only, this thread
    /// only, nestable; see the invariant below.
    pub fn scope<R>(&self, f: impl FnOnce() -> R) -> R;

    /// Top of this thread's scope stack; `default_ns()` when empty.
    /// (`MapxXXX::new()` ≡ `new_in(&Namespace::current())`.)
    pub fn current() -> Namespace;

    pub fn id(&self) -> NsId; // getter — an input only on the admin tier (`open`)
    pub fn path(&self) -> &Path;
    pub fn flush(&self);
}

/// Creation-time options; persisted in the registry (C4).
/// Everything is defaulted — this struct exists for the advanced 10%.
pub struct NamespaceOpts {
    /// None ⇒ derived under {default_base}/__NAMESPACES__/, recorded
    /// base-relative (the whole universe stays movable as one tree).
    /// Some ⇒ explicit root (e.g. another volume), stored absolute and
    /// pinned (`relocate` to move). Rejected if it nests inside another
    /// registered namespace or the default-ns reserved subtrees.
    pub path: Option<PathBuf>,
    pub shards: usize,               // default: see §10; clamped to 1..=64
    pub mem_budget_mb: Option<usize> // None => conservative fixed default (§4.4)
}

pub fn vsdb_ns_list() -> Result<Vec<NsInfo>>; // NsInfo = {id, path, shards, created_at}
pub fn vsdb_ns_relocate(id: NsId, new_path: impl AsRef<Path>) -> Result<()>; // not-open-only, §4.7
pub fn vsdb_ns_destroy(id: NsId) -> Result<()>;                             // not-open-only, §4.7

// ---- every collection (core Mapx up through strata wrappers) ----
impl MapxXXX {
    pub fn new() -> Self;                  // current ns (ambient scope; default = default ns)
    pub fn new_in(ns: &Namespace) -> Self; // explicit ns (naming mirrors Box::new_in)
    pub fn namespace(&self) -> Namespace;  // THE co-location primitive: "put with this"

    pub fn instance_id(&self) -> InstanceId;      // complete address (rev 9)
    pub fn save_meta(&self) -> Result<InstanceId>;

    /// `Into` accepts bare u64 (⇒ ns: None) — every existing call site
    /// compiles unchanged. Resolution is deterministic, never a search:
    /// `ns: Some(id)` ⇒ that namespace's meta dir; `ns: None` ⇒ the
    /// default namespace's meta dir (its id is a fixed constant — there
    /// is nothing to find). One lookup either way; miss = clean error.
    pub fn from_meta(id: impl Into<InstanceId>) -> Result<Self>;
}
```

Notes:

- **Three usage tiers**, each complete on its own:
  1. *Unaware* (99% of existing code): `Mapx::new()` — default ns, nothing
     changes.
  2. *Co-location* (the everyday namespace tier): start a group once —
     `Namespace::create()?.scope(|| Subsystem::init())` — then put related
     data together via `existing.namespace()` + `new_in`/`scope`. No names,
     no ids, no paths in user code.
  3. *Explicit/admin* (advanced): `create_with(opts)` (volumes, shards,
     budgets), `open(id)`/`vsdb_ns_*` for rotation bookkeeping and ops.
- **Zero new persistent identifiers — a bare u64 stays a complete address**
  for everything it can legitimately reference (default-ns instances, §4.5).
  Recovery rides what users already persist today: serialized handles (metas
  embed `ns_id`, §4.3) or bare `instance_id`s (accepted via `From<u64>`).
  The richer `InstanceId` is what new code *receives* from
  `save_meta()`/`instance_id()` — a complete, direct-addressing token —
  but it is never a required input shape; `NsId` alone is readable (`id()`)
  and never demanded on any normal path.
- **Scope invariant — placement only, never routing.** The ambient scope is
  consulted at exactly one instant: collection *creation* (`new()` resolves
  "current ns", default = default ns, and embeds it in the handle). It never
  affects reads, writes, deserialization, or `from_meta` — those take the
  ns from the handle/meta itself. Scope state is a thread-local stack:
  nestable, popped on unwind (guard-based), **not inherited by spawned
  threads** (documented loudly; explicit `new_in`/handle-passing is the tool
  for cross-thread construction).
- `new_in` replaces the draft's `new_with_ns(..., ns)`; `new()` is literally
  `new_in(&Namespace::current())`.
- `Namespace` itself is **not** serializable — it is a runtime handle. Handles
  persist only the `NsId`.
- `vsdb_flush()` keeps its "flush everything" intent: flushes **all open
  namespaces**.
- Composite structures (VerMap, SlotDex, VecDex, DagMap, tries, B+ tree) get
  the same `new_in`/`from_meta` pair and honor the ambient scope in their
  `new()`; **invariant: a composite and all its internal maps live in exactly
  one namespace**, captured at construction.

### 4.2 Handle plumbing

Core `Mapx` gains a `ns: Namespace` field (one `Arc` clone, 8 bytes),
captured **eagerly at `new()`** — the ambient scope must be read at creation
time, even though the prefix itself stays lazily allocated
(`Prefix::Created(LazyLock)`); resolving the ns inside the LazyLock closure
would leak scope state into first-use time. All data ops route
`self.ns.engine` instead of `VSDB.db`. `shadow()` / `from_prefix_slice` copy
the ns handle; `is_the_same_instance` compares `(ns_id, prefix)`. Wide-touch
but mechanical: all strata collections construct through core `Mapx`, and no
strata code references `VSDB` directly (verified — only `core` does).

### 4.3 Persisted metadata: optional `ns_id` suffix

```text
default ns  (= v15, byte-identical):  "VSMAPX01" ‖ prefix_le(8)               — 16 B
non-default namespace:                "VSMAPX01" ‖ prefix_le(8) ‖ ns_id_le(8) — 24 B
```

The meta *is* an `Option<NsId>` — encoded by presence, not by a serde tag
(the meta is a hand-encoded byte string, C3, and a postcard `Option` field
would change the existing bytes): suffix absent ⇔ `None` ⇔ default ns. One
magic; no format versions.

Implementation-wise this is **codec-only**: a few-line widening of
`decode_prefix_meta` (16 ⇒ `None`, 24 ⇒ `Some`), a conditional append in
`encode_prefix_meta`, and ns-handle resolution in `from_prefix_meta`. No
stored byte is ever rewritten — v15 metas are valid input forever, and the
whole proposal's only write into an existing v15 dir is P0's additive
ceiling-file copy (§4.6).

- **Read**: length 16 ⇒ `None` (v15 data decodes to the default ns exactly
  as-is); length 24 ⇒ `Some(ns_id)`; any other length ⇒ `Decode` error.
  Symmetrically, v15's strict `len == 16` check rejects 24 B metas cleanly —
  unsupported-downgrade failures stay loud (§7).
- **Write**: default ns ⇒ suffix omitted — byte-identical to v15, so existing
  serialized handles (including ones nested inside stored values) re-save
  without churn.
- Deserialize resolves `Some(ns_id)` → open namespace (auto-open via
  registry) for routing; prefix recovery calls `reserve_recovered_prefix` on
  the one global allocator, exactly as today.
- Strata's `VSTYPE02` typed-handle envelope and the postcard instance-meta
  files carry the core meta opaquely — no change.
- Cost of forgoing a second magic, stated honestly: a 24 B meta truncated to
  exactly its first 16 bytes would decode as a *valid* default-ns handle
  (a distinct magic would catch that structurally). Every layer around these
  bytes is length-delimited (postcard byte-bufs) or whole-file atomic
  (instance metas), and v15 metas carry no checksum today either — judged
  not worth a second wire format.

### 4.4 Resource model

- **Shards**: per-namespace count, fixed at creation, persisted in the registry
  and validated against the on-disk `shard_NN` layout at open (C4). Default
  namespace stays at 16 forever.
- **Memory (C6)**: the existing env/cgroup formula applies to the **default
  namespace only** (exact v15 parity). Non-default namespaces size from
  `NamespaceOpts.mem_budget_mb`, defaulting to a small fixed budget (e.g.
  512 MB-equivalent sizing) so that opening N namespaces cannot silently
  multiply the process footprint. Documented rule: *the process owner is
  responsible for the sum across namespaces*; VSDB never auto-rebalances
  budgets at runtime (write buffers are sized at open and cannot resize).
- **Threads/FDs**: each namespace costs `shards ×` (compaction thread + WAL +
  memtable set + file handles). This is exactly why "one namespace per Mapx"
  is rejected (§5.4) — but a user who truly needs a standalone map can still
  do it manually with `shards: 1`.

### 4.5 Per-namespace `__SYSTEM__` tree

Every namespace gets its own base-dir subtree, same layout as today
(`ns_path` = the explicit root given at creation, or the derived
`{default_base}/__NAMESPACES__/{ns_id:016x}`, §4.1):

```text
{ns_path}/mmdb/shard_NN/...
{ns_path}/__SYSTEM__/format_version           (ASCII "16"; see §7 tripwire)
{ns_path}/__SYSTEM__/__instance_meta__/{prefix:016x}
{ns_path}/__SYSTEM__/mpt_cache_{id}.bin, smt_cache_{id}.bin
```

Deliberately absent: a per-ns `__CUSTOM__`. The custom dir is the
**app-level bootstrap anchor** — the place users keep their serialized
app state (top-level handles), which may reference *any* namespace.
Namespaces are reached through handles and handles are bootstrapped from
the custom dir, so a per-ns location would be circular — and
`destroy(ns)` would silently delete the app's root pointer. There is
exactly one custom dir per universe: `{default_base}/__CUSTOM__`
(`vsdb_get_custom_dir()`); users who want ns-local scratch files can use
`ns.path()` directly.

`vsdb_get_system_dir()`/`vsdb_get_meta_dir()` keep returning default-ns
paths; ns-aware variants (`ns.system_dir()`, `ns.meta_dir()`) are added
and strata call sites for instance metas and trie caches route through the
owning handle's namespace. `vsdb_get_custom_dir()` stays universe-global
(see above). The DagMap ID allocator (`dag_id_ceiling`) is a
pure ID space like the prefix allocator and stays global in the default tree,
untouched.

With the global allocator (§4.6), per-ns trees are **not** needed to avoid key
collisions — `map_id`s are unique registry-wide. They are adopted for
three other reasons: `destroy(ns)` reclaims metas and caches together with the
data (no orphaned files under a shared tree); metas/caches live on the same
volume as their data (§1.1); and structural robustness — nothing outside a
namespace's own tree is keyed by its prefixes, so no cross-ns file state can
ever dangle.

`map_id`s are globally unique, yet each meta file lives under its owning
namespace's tree. `from_meta` never searches (rev 10): an `InstanceId` is a
*complete address* in both forms —

- `ns: Some(id)` — computed location, one lookup in that namespace's meta
  dir (registry gives the path).
- `ns: None` (bare u64, v15-style) — the default namespace's meta dir, whose
  id is the fixed `DEFAULT_NS_ID` constant: nothing to look up, nothing to
  probe. A bare u64 cannot legitimately reference a non-default instance
  (v15 tokens predate namespaces; v16 mints ns-resident tokens with `Some`),
  so a miss here is a clean "not found in the default namespace" error —
  the honest answer to a stripped/corrupted address, never a global hunt
  that would mask the user's bug.

### 4.6 One global prefix allocator (all namespaces)

Namespaces do **not** get private allocators. One allocator — the existing
one — serves every namespace in the registry, so prefixes (= `map_id`s)
are unique across all namespaces *by construction*: no coordination, no
reconciliation, uniqueness falls out of "there is only one counter".

Why this is strictly simpler than per-ns allocators:

- **Zero in-memory change.** All of C2's machinery (thread-local windows,
  pending-window registry, recovered-prefix set, `PendingWindowGuard`) stays
  byte-identical — the per-`(thread, ns)` window split, the riskiest refactor
  of rev 1, disappears.
- The C5 collision class is gone by construction, not by directory layout.
- `reserve_recovered_prefix` remains one global call — no "reservation must
  land in the owning allocator" footgun at meta-decode time.
- **u64 headroom** confirms the premise: allocation starts at
  `4096_0000 ≈ 4.1e7` out of `≈ 1.8e19`; worst-case waste is one 8192-window
  per thread per process lifetime. Even burning 1,000 windows/second
  non-stop exhausts the space in ~70,000 years.

The one thing that must move is **persistence**. Today the ceiling is a
sync-WAL key in default shard 0 (C2). Left there, creating a map in *any*
namespace would force the full default engine (16 shards, threads, memtables)
open even in programs that only ever use a custom namespace. So:

- The ceiling moves to `{default_base}/__SYSTEM__/__prefix_ceiling__`
  (`atomic_write_file` semantics). Window persists are serialized by the
  existing in-process `PREFIX_ALLOC_LOCK` — no file locking (§4.7: one
  process per universe).
- **v15 upgrade migration** at first open: `file = max(file, shard0)` —
  idempotent and crash-safe. The legacy shard-0 key is left in place and
  never written again (downgrade is out of contract, §7).

Honest caveats:

- Creating a *new* map in any namespace touches the default-base allocator
  file (one fsync per 8192 allocations per thread). Opening, reading, and
  writing *existing* maps never does.
- Global uniqueness holds unconditionally within the contract. Still,
  correctness is deliberately never staked on cross-ns uniqueness (§4.5):
  data dirs are disjoint, metas/caches are per-ns, identity is
  `(ns_id, prefix)`. Uniqueness is defense-in-depth, not load-bearing.

### 4.7 Lifecycle & process model

**One universe = one process.** A registry and all its namespaces belong to
at most one process at a time — the same embedded, in-process model VSDB has
always had; namespaces do not widen it. Two applications = two base dirs (two
registries), which already works today. Consequences:

- **No cross-process coordination exists anywhere.** Registry mutations and
  allocator-window persists are serialized by ordinary in-process mutexes;
  no file locks, no shared-state protocols. Namespaces cannot interfere with
  each other by construction (disjoint dirs), so there is nothing to lock
  *between* them either.
- **Violations fail loudly, they are not "supported".** Running two processes
  on the same universe is an application/ops bug. VSDB does not try to make
  it work — but the inherited per-shard mmdb LOCK (C7) makes the worst case
  (two engines on the same shard dir → dueling WALs and compactions,
  silent corruption) fail cleanly at `open` instead. One `flock` syscall at
  open, held for the process lifetime; standard embedded-DB practice
  (RocksDB/LevelDB/SQLite do the same). Correctness-first: the DB won't fix
  an app bug, but it must not *silently corrupt* under one. Registry-level
  races between out-of-contract processes are not defended beyond
  `atomic_write_file`'s all-or-nothing rename (a torn registry can never be
  observed; a lost entry leaves an orphaned-but-intact dir).
- **Open** = held for the process lifetime by default; in-process
  `close()` (v16.1.0) fully reclaims an idle namespace — see
  [ns-close.md](./ns-close.md) (engines are owned by their `Arc<NsInner>`,
  dropped after an exclusivity proof; no leak anywhere).
- **Destroy / relocate** require the target namespace to be *not open in this
  process* (checked against the in-process open-namespace table): update the
  registry, then act (destroy: remove registry entry → delete tree; a crash
  in between leaves an orphaned-but-harmless dir, manually removable —
  re-attachment is excluded along with all foreign-root adoption, §9).
- A byte-sized lifecycle sidecar under `__SYSTEM__/__namespace_state__/`
  distinguishes a pending create from an established registry record without
  changing the positional postcard registry. Only pending records may
  initialize an absent root; established (and legacy sidecar-less) records
  require a complete marked dataset. Valid legacy roots are promoted to
  established on first open.
- Opening any non-default namespace materializes the registry under the
  default base dir and therefore freezes it (same rule as today's derived
  dirs); programs that want a custom default base must call
  `vsdb_set_base_dir` first — unchanged contract.

---

## 5. Evaluation of the Original Notes

| # | Original point | Verdict |
|---|----------------|---------|
| 5.1 | Embed the ns in the Map, self-contained, set once at creation | **Accept.** Realized as `ns: Namespace` field + optional `ns_id` meta suffix (§4.3). |
| 5.2 | `Option<NamespaceId>` field + serde attributes for compat | **Accept — at the byte level** (rev 5). Not literally a serde field: metas are hand-encoded bytes (C3), and a postcard `Option` tag would perturb existing bytes. But the shape is exactly the note's: optional trailing `ns_id`, absent = `None` = default ns; v15 bytes decode as `None` verbatim; default-ns writes stay byte-identical (§4.3). |
| 5.3 | Keep `new()`, add `new_with_ns`, add `new_inner(Option<ns>)` | **Accept, simplified.** `new()` + `new_in(&ns)`; no `new_inner` layer needed. |
| 5.4 | Auto-create ns on first use; mgmt APIs (`merge`/`destroy`); `get_ns()` | **Mostly accept.** `create()` makes cheap creation literally parameterless (rev 6); `namespace()` getter; `destroy`/`relocate` not-open-only (§4.7). **`merge` deferred** (§9): global prefix uniqueness (§4.6) removes the ID-reconciliation blocker, but the resumable crash-consistent copy protocol it needs is P3 scope; per-map cross-ns copy is first-class since v16.2.0 (`clone_in(&ns)`, Clone-style chunking). |
| 5.5 | Each ns has its own basedir; default ns needs no id; default behavior identical to current VSDB | **Accept.** Core of the design. |
| 5.6 | First `new_with_ns(ns_id)` … later: "the param should be a path, not an id" | **Both, split by role.** Path = configuration (registry-only, relocatable, and since rev 6 *optional* — omitted, it derives from the id, §4.1); `NsId` = persisted identity (in metas). Callers pass neither raw ids nor raw paths at map creation — they pass a `Namespace` handle. This dissolves the id-vs-path dilemma. |
| 5.7 | Mixing namespaces in one structure, e.g. `Vec<Mapx…>` | **Accept.** Every handle self-routes, so heterogeneous containers work naturally. One *composite* (VerMap etc.) stays wholly inside one ns (§4.1 invariant). |
| 5.8 | Drop 16-way sharding? Solo mmdb per instance? | **Keep sharding, make it per-ns configurable.** Sharding solves write/compaction concurrency *within* one namespace; namespaces solve placement/isolation *across* datasets — orthogonal. A hot single namespace still needs shard fan-out. `shards: 1` is available for lightweight namespaces. Default ns pinned to 16 (on-disk routing stability, C4). |
| 5.9 | Auto-create one instance per MapxXXX | **Reject** (as the notes already concluded): per-ns cost is `shards ×` threads/WALs/memtables/FDs (§4.4). Manual opt-in suffices. |
| 5.10 | Automatic namespace split for performance; reserve ID ranges (1..100M) for the old header | **Reject** (as the notes already suspected). Auto-split means migrating live maps between engines — handle invalidation, non-atomic cross-engine moves, allocator reconciliation; it violates the correctness-first principle for a speculative win. Reserved-ID carve-outs are moot under §5.6's identity model. |
| 5.11 | Major version bump necessary | **Accept.** v16.0.0 (§7). |
| 5.12 | Tests get isolated spaces; single-thread mode becomes unnecessary | **Accept direction, phase the claim** (§8). Isolation is immediate; *parallel* tests need the per-ns budget/shard knobs plus a suite migration — flip `--test-threads=1` only after that audit, not on day one. |
| 5.13 | Endless data growth degrades perf; multi-ns on one disk still helps ("less data to scan"?) | **Refine.** Per-tree depth (read amp) and per-tree compaction amp shrink; aggregate disk work for the same total data does not. The dependable wins are placement, isolation, and O(1) whole-ns reclaim (§1.3-1.4). Bloom filters already spare point-gets most "scanning"; range scans within one map are prefix-bounded regardless. |

---

## 6. Implementation Plan

- **P0 — allocator persistence relocation** (shippable alone, no API change):
  ceiling moves shard-0 → the `__prefix_ceiling__` file, with the
  idempotent take-max upgrade migration (§4.6); the legacy key is never
  written again. The `format_version` marker (`16`) is written durably
  before the file-based allocator issues anything (see §7 — the marker
  ships its v15-side check as a v15.0.2 pre-release). Every piece of
  in-memory allocator machinery stays byte-identical.
- **P1 — namespaces**: `Namespace`/`NsId`/registry; optional-`ns_id` meta
  codec; core `Mapx` ns field + routing; per-ns `__SYSTEM__` tree and
  strata call-site rerouting (instance metas, trie caches; the DagMap ID
  allocator stays global, §4.5);
  `new_in`/`namespace()`/universal `from_meta` across all collections;
  scoped ambient placement (`scope`, thread-local stack);
  mgmt APIs; C9 doc rename; README/CHANGELOG.
- **P2 — test-suite migration** *(done in v16.0.2, simpler than
  planned)*: per-test namespace scopes proved unnecessary — globally
  unique prefixes already make test data disjoint by construction. The
  actual blockers were env mutation (`vsdb_set_base_dir` per lib test —
  removed; integration binaries serialize theirs behind a `Once`) and a
  few exact-value global-allocator assertions (made race-tolerant).
  `--test-threads=1` dropped.
- **P3 (remaining work)**: whole-ns `merge`. Cross-ns map-copy convenience
  helpers shipped in v16.2.0: `clone_in(&ns)` on `MapxRaw` and the typed
  wrappers (`Mapx`/`MapxOrd`/`MapxOrdRawKey`/`Orphan`) — the
  cross-namespace form of `Clone` (chunked, never whole-map in memory),
  mirroring `new` vs `new_in`. In-process `close()` originally targeted
  P3 but shipped in
  v16.1.0 ([ns-close.md](./ns-close.md)) — the ownership inversion
  removed both `Box::leak` sites, engines are owned by their `Arc<NsInner>`,
  and `vsdb_ns_close` provides the in-process epoch-rotation loop
  (`create → fill → close → destroy`) without restart. Runtime
  relocation ("close → move files → reopen without restart") is
  rejected — moving live database files is operationally unacceptable;
  `relocate` stays what it is: an offline registry-pointer repair after
  an operator has moved the data. `destroy` remains the O(1) bulk-reclaim
  primitive for time-partitioned data (tombstone + compaction reclaim is
  hours and write-amplification). No further namespace self-management
  APIs will be added.

---

## 7. Compatibility

- **Major bump to v16.0.0.** Even though `new()` is signature-identical,
  every default-ns call site keeps compiling (`from_meta` *widens* to
  `impl Into<InstanceId>`), and existing data files are never rewritten —
  the meta *read* grammar widens, core `Mapx` layout changes, and the
  resource model gains new knobs; semver-major is the honest choice
  (matching the notes' instinct). The only source-breaking change on the
  unaware tier: `instance_id()`/`save_meta()` now return `InstanceId`
  instead of `u64` (compile-time, mechanical migration; stored u64 tokens
  keep working — no data impact).
- **Upgrade (v15 → v16)**: data dirs open as the default namespace after one
  idempotent, crash-safe metadata migration (allocator ceiling copied
  shard-0 → file, take-max; §4.6).
- **Downgrade: unsupported, by decision.** Once v16 has written, pointing a
  v15 binary at the dataset is out of contract — the storage layer carries
  zero rollback machinery (no dual-writes, no legacy-format guarantees).
  Failure is still clean, not silent, via two tripwires: 24-byte metas hit
  v15's strict 16-byte length check ⇒ `Decode` error; and — covering
  default-ns-only datasets, whose metas stay 16 B — v15.0.2+ checks the
  `__SYSTEM__/format_version` marker (ASCII decimal; v16 writes `16` into
  every namespace tree it opens, durably *before* any divergent state such
  as file-based allocator windows) and refuses anything newer at open.
  The last v15 release is therefore the designated safe landing point for
  an out-of-contract rollback attempt; pre-marker v15 binaries are
  unprotected — unavoidable, they validate nothing. Going back =
  application-level export (iterate the maps, write into a fresh v15
  dataset); the engine does not carry that burden.

---

## 8. Test-Suite Impact

Globally-unique prefixes (§4.6) make test data disjoint by construction —
no per-test namespace ceremony needed; test bodies keep their plain
`new()` calls and the prefix allocator guarantees zero cross-test bleed.
The actual blockers to parallel execution were env mutation
(`vsdb_set_base_dir` per lib test — removed; integration binaries
serialize theirs behind a `Once`) and a few exact-value global-allocator
assertions (made race-tolerant). `--test-threads=1` was dropped in
v16.0.2. Tests must not assert on cross-test global state (exact
allocator values, registry sizes) and must serialize any
`vsdb_set_base_dir` behind a `Once` (env mutation is unsound to race).

---

## 9. Non-Goals (this RFC)

- **Cross-namespace atomic transactions** — separate WALs; physically
  impossible without a coordination layer. Document prominently. (Per-map
  atomicity is unaffected, C8.)
- **In-process namespace close/reopen** — shipped in v16.1.0
  ([ns-close.md](./ns-close.md)); the ownership inversion removed both
  `Box::leak` sites, engines are owned by their `Arc<NsInner>`, and
  `vsdb_ns_close` provides the in-process epoch-rotation loop.
- **Whole-namespace merge** — the ID-reconciliation blocker is gone (§4.6:
  same-registry prefixes never collide), but the resumable crash-consistent
  bulk-copy protocol it needs is P3 scope; per-map copy is covered since
  v16.2.0 by the `clone_in(&ns)` helpers.
- **Importing/attaching a namespace dir created under a different registry** —
  foreign prefixes came from a foreign allocator; attach must first raise the
  local ceiling above the import's maximum used prefix. P3, with merge.
- **Automatic split/rebalance across namespaces** — rejected (§5.10).
- **Multi-process anything** — one universe belongs to one process (§4.7);
  there is no cross-process locking, sharing, or coordination to design,
  document, or maintain. The only lock in the system is mmdb's pre-existing
  per-shard LOCK tripwire, which exists to *reject* double-open, not to
  support it.

## 10. Open Questions

All three were settled during implementation:

1. ~~Default `shards` for new namespaces~~ — **4** (`DEFAULT_NS_SHARDS`),
   favoring the lightweight many-namespaces pattern per constraint zero;
   the default namespace stays pinned at 16.
2. ~~Default `mem_budget_mb`~~ — **fixed 512 MB** (`DEFAULT_NS_BUDGET_MB`),
   always treated as a binding limit so opening N namespaces cannot
   silently multiply the process footprint. Tunable per-namespace via
   `NamespaceOpts`; revisit the constant if benchmarks ever show it
   binding badly for common secondary datasets.
3. ~~`VSDB_MEM_BUDGET_MB` sum-cap across namespaces~~ — **default-ns-only**,
   documented (§4.4): the process owner is responsible for the sum;
   VSDB never auto-rebalances budgets at runtime.
