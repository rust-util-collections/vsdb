# Namespaces — current design

Namespaces shipped in v16.0.0; in-process close in v16.1.0, cross-namespace
copy in v16.2.0, and per-engine cache pooling in v16.3.0. This document
describes the current implementation, including v17 configuration and handle
APIs. Earlier RFC revisions and implementation plans remain in Git history.

Source of truth: [namespace.rs](../../core/src/common/namespace.rs),
[engine metadata](../../core/src/common/engine/mod.rs), and
[MMDB integration](../../core/src/common/engine/mmdb.rs).

## Purpose and boundaries

A namespace is an anonymous placement group with its own root directory,
engine shards, WALs, compaction workers, and memory sizing. It supports
separate volumes and epoch rotation: create, fill, close, then remove a whole
directory tree without traversing individual database keys. Directory deletion
still costs filesystem work proportional to the files/directories removed.

Namespace placement and shard placement are different. `new_in(&ns)` selects
an engine; a collection prefix routes to one shard via `prefix % shard_count`.
Two collections in that namespace need not share a shard or WAL. New VerMap
instances explicitly co-locate their component prefixes on one shard; SlotDex
and VecDex stage related rows under one prefix. There is no cross-namespace
atomic transaction, and VSDB engine batches are single-prefix.

The registry and prefix allocator are shared across the universe. Writable
access requires one process per universe, including when processes intend to
use disjoint namespaces: these metadata locks are process-local. Multiple
read-only processes may inspect a complete immutable universe; see the
[read-only guide](../../core/docs/read-only.md) for shard locking and snapshots.

## Creation and placement

```rust
use vsdb::{Mapx, Namespace};

let ns = Namespace::create().unwrap();
let map = Mapx::<u64, String>::new_in(&ns);
let scoped: Mapx<u64, u64> = ns.scope(Mapx::new);
let same_namespace = Mapx::<u64, u64>::new_in(&map.namespace());
assert_eq!(scoped.namespace().id(), same_namespace.namespace().id());

let token = map.save_meta().unwrap();
drop(map);
let restored = Mapx::<u64, String>::from_meta(token).unwrap();
assert_eq!(restored.namespace().id(), ns.id());
```

- `Namespace::create()` opens a fresh anonymous namespace with a derived path.
- `Namespace::create_with(NamespaceOpts { path, shards, mem_budget_mb })`
  selects an explicit absolute root, shard count, or memory sizing.
- `Namespace::open(id)` resolves the registry and reuses a cached engine when
  already open. `Namespace::default_ns()` obtains the process-lifetime default
  engine; its first call can initialize/open storage and perform I/O.
- Ordinary `new()` calls use the current thread's ambient scope, otherwise the
  default namespace. Scopes nest and restore their predecessor on unwind;
  they do not propagate to other threads. Explicit `new_in` wins over scope.
- Deserialization, reads, and writes always use the handle's namespace.
  DagMap children inherit their parent's namespace, even inside another scope.
- A composite collection remains within one namespace. In-memory `MptCalc`
  and `SmtCalc` have no namespace constructor; use
  `VerMapWithProof::from_map(VerMap::new_in(&ns))` for a placed proof wrapper.

`clone_in(&ns)` deep-copies MapxRaw, Mapx, MapxOrd, MapxOrdRawKey, or Orphan
into a fresh instance in the requested namespace. Map copies use bounded
chunks; the operation is not a cross-engine transaction. Ordinary `Clone`
deep-copies into the original namespace; unsafe `shadow()` aliases storage
under its documented write-exclusion contract.

## Identity and metadata

`NsId` is a monotonic `u64`. The default namespace is ID `0`, is not allocated,
and has no registry entry. Non-default IDs start at `1` and are never reused.
Paths are configuration, stored in the registry rather than collection handles.

```rust
use vsdb::InstanceId;

let default = InstanceId::from(42_u64);
let placed: InstanceId = "42@7".parse().unwrap();
assert_eq!(default.ns, None);
assert_eq!(placed.ns, Some(7));
assert_eq!("42@0".parse::<InstanceId>().unwrap(), default);
```

`InstanceId { map_id, ns }` carries the prefix and owning namespace. A bare
`u64` always means the default namespace; recovery never searches every
namespace. Constructors/parsing/deserialization canonicalize default routing
to `ns: None`; manually constructing `Some(0)` routes to default but should be
avoided when comparing tokens.

Raw handle metadata has two forms:

| Bytes | Meaning |
|-------|---------|
| `VSMAPX01` + prefix LE (8 bytes) | 16-byte default-namespace handle |
| `VSMAPX01` + prefix LE + namespace LE (8 bytes) | 24-byte non-default handle |

Typed wrappers write `VSTYPE03`, including the fully qualified Rust type name
and generic arguments; they still read `VSTYPE02` with its compatibility
rules. Type tags are not schema fingerprints: changing a persisted type's
fields can require an application migration even when its name stays the same.
Serialized handles identify local storage; they do not export collection data.

## Roots and registry

```text
<base>/
  __SYSTEM__/
    __namespaces__              registry: next_id + records
    __namespace_state__/<id>    P (pending) or E (established)
    __prefix_ceiling__          global durable prefix reservation ceiling
    format_version             engine format marker
    __instance_meta__/         default-namespace saved handles
    ...                        other universe/default metadata
  __NAMESPACES__/<id in hex>/   derived non-default root
    __SYSTEM__/                own marker, saved handles, trie caches
    ...                        that namespace's engine shards
  ...                          default engine shards
```

Explicit roots may be on other volumes. `NsRecord` persists, in postcard field
order: `id`, optional UTF-8 `path`, `shards`, optional `mem_budget_mb`, and
`created_at`. The registry is positional; adding serde defaults alone does not
make layout changes compatible. Lifecycle sidecars avoid changing that format.

Explicit creation roots must be absent or empty and must not overlap the
default base or another registered root. Validation checks lexical components
and resolves existing ancestors to catch symlink aliases. Replacing symlinks
after registration is filesystem administration outside this contract. Existing
foreign data cannot be adopted: its prefix provenance is unknown to this
universe's allocator.

Derived roots move with the default base; explicit paths stay pinned until
`relocate`. Backups must preserve the default base and all explicitly rooted
namespaces referenced by saved handles. Per-namespace instance metadata and
trie checkpoints live with the owning engine; the namespace registry, prefix
allocator, and DagMap ID allocation remain universe-wide.

## Shards and memory

The default namespace has 16 shards for persisted routing compatibility.
Non-default creation defaults to 4 shards and clamps requests into `1..=64`.
The chosen count is persisted; opening an out-of-range registry value or an
incomplete established shard layout fails rather than silently re-routing keys.

Default-engine sizing is 2 GiB unless configured by `VsdbOptions` or
`VSDB_MEM_BUDGET_MB`. Non-default engines default to 512 MiB each or their
persisted option. No host-RAM/cgroup detection is used. These are cache/buffer
sizing inputs with floors and caps, not hard process memory limits.

Each engine shares one block-cache pool across its own shards. Write buffers
remain per shard, and pools are not shared between namespaces. See
[shared-mem-pool.md](shared-mem-pool.md) for formulas and deferred designs.

## Global prefix allocation

One allocator serves all namespaces, keeping prefixes unique across the whole
universe. It reserves windows of 8,192 prefixes per thread. The global ceiling
is persisted under the default base before any prefix in a new window is
issued. Recovered prefixes advance the floor; IDs are never reused. Unused
reservations and shard co-location can leave gaps.

The v15-to-v16 transition takes the maximum of the old default-shard allocator
and the new ceiling file before further allocation. That legacy upgrade can
open the default engine; normal file-backed allocation does not require its
data path. The engine format marker is currently `16`, independently of the
crate's v17 API version. Newer unsupported or malformed markers fail open.

## Initialization and recovery

`vsdb_configure(VsdbOptions)` is process-wide and one-shot. Call it before
database, namespace, or derived-directory APIs; derived path use freezes the
base. Configuration does not mutate environment variables or configure child
processes. Pass `VSDB_BASE_DIR` explicitly to a child when needed.

Creation records a pending lifecycle before establishing a namespace. Writable
recovery may complete a pending create. An established (or legacy unmarked
lifecycle) record must point to a complete marked engine; missing storage is
an error, never permission to manufacture an empty replacement. Initialization
uses `__SYSTEM__/__initializing__`; a complete marked dataset can retain a
stale sentinel after interruption. Read-only open accepts that completed state
without removing the sentinel, but refuses a `Pending` namespace lifecycle.

Read-only mode is inherited by every namespace opened in that process. It
replays valid WAL records in memory and does not create storage, rewrite
markers, migrate allocator metadata, or repair lifecycle state.

## Administration and lifetime

| API | Contract |
|-----|----------|
| `Namespace::list()` | Registered non-default namespaces and resolved paths |
| `Namespace::close_by_id(id)` | Refuse while namespace-owning client handles remain; otherwise flush/sync and tear down the engine |
| `ns.close()` | Account for the consumed handle; return it on refusal |
| `Namespace::destroy(id)` | Require a non-default, not-open target; remove registry entry, then directory tree |
| `Namespace::relocate(id, path)` | Require a not-open target and completed destination dataset; update registry only |

Engines are owned by `Arc<NsInner>`; there are no leaked engine shells. The
open table pins non-default engines until explicit close. The default handle
is static and cannot be closed or destroyed. Details, detached iterator
semantics, and teardown errors: [ns-close.md](ns-close.md).

Destroy persists registry removal first. A crash or filesystem error afterward
can leave an orphan directory; the namespace is already unregistered and a
repeated destroy is not an automatic filesystem-cleanup retry.

Relocation does not move files. Close the namespace, move its data, then call
`relocate` and reopen; a process restart is not required. The destination is
checked for a completed engine with the recorded shard count, but roots do not
carry namespace IDs, so operators must select the correct dataset.

## Compatibility and deferred work

v16 introduced `InstanceId` and namespace routing; default raw handle bytes
remain compatible with the old 16-byte form. v17 changed public configuration
and namespace administration names and strengthened typed-handle tags. Use
the [changelog](../../CHANGELOG.md) for migration details, not the original RFC
API sketches.

Foreign-root adoption, whole-namespace merge, live data relocation, automatic
splitting, and cross-namespace transactions are not implemented. Per-map
`clone_in` and close/move/relocate cover the current explicit migration paths.

Tests run in parallel with globally unique collection prefixes. Configure once
per binary; avoid assertions on exact global allocator/registry state. For
isolated runs, launch bare Cargo with a fresh `VSDB_BASE_DIR`; do not change
process environment variables from concurrent tests.
