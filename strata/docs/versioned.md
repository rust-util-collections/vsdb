# Versioned Module — Architecture & Internals

This document explains the design, data flow, and internal mechanisms of `VerMap`,
the Git-model versioned storage engine in `vsdb`.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Layer Architecture](#layer-architecture)
- [Core Data Structures](#core-data-structures)
- [Lifecycle: Create → Write → Commit → Branch → Merge](#lifecycle)
- [Copy-on-Write & Structural Sharing](#copy-on-write--structural-sharing)
- [Commit DAG](#commit-dag)
- [Three-Way Merge Algorithm](#three-way-merge-algorithm)
- [Durability and Recovery](#durability-and-recovery)
- [Garbage Collection](#garbage-collection)
- [Fork Point & Commit Distance](#fork-point--commit-distance)
- [Rollback & Discard](#rollback--discard)
- [Summary](#summary)

---

## Architecture Overview

```mermaid
graph TB
    subgraph "User API"
        VM["VerMap&lt;K, V&gt;"]
    end

    subgraph "Version Control"
        BR["BranchHead<br/>(head, dirty_root)"]
        CM["Commit<br/>(id, root, parents, timestamp_us, ref_count)"]
    end

    subgraph "Storage Engine"
        BT["PersistentBTree<br/>(COW B+ Tree)"]
        ND["Nodes<br/>(Leaf / Internal)"]
    end

    subgraph "Persistence"
        MR["MapxRaw<br/>(MMDB)"]
    end

    VM -->|"branch/commit/merge"| BR
    VM -->|"commit metadata"| CM
    VM -->|"insert/remove/get"| BT
    BT -->|"allocate/read nodes"| ND
    ND -->|"persisted as bytes"| MR
    BR -->|"stored in"| MR
    CM -->|"stored in"| MR
```

---

## Layer Architecture

```mermaid
block-beta
    columns 1
    block:L1["Layer 1 — User-Facing API"]
        A["VerMap&lt;K, V&gt;<br/>branch / commit / merge / rollback_to / gc"]
    end
    block:L2["Layer 2 — Persistent B+ Tree"]
        B["PersistentBTree<br/>insert / remove / iter / range / bulk_load / gc<br/>Copy-on-Write, structural sharing"]
    end
    block:L3["Layer 3 — Raw KV Storage"]
        C["MapxRaw<br/>Single MMDB prefix"]
    end
```

Each `VerMap` persists its state in a single `MapxRaw` prefix.

| Key tag | Contents |
|---|---|
| `00` | Format, monotonic ID ceilings, main branch, generation |
| `01`, `02`, `03` | Branch head and working root, branch name by ID, reverse lookup by name |
| `04` | Immutable commit state plus mutable reference count |
| `05`, `06` | Retired roots and unpublished staged subtrees |
| `07` | Checksummed reference-count checkpoint |
| `10`, `12` | Slotted B+ tree pages and value blobs |

---

## Core Data Structures

`BranchId` and `CommitId` are distinct serde-transparent `u64` newtypes.
Both implement `KeyEnDeOrdered`, so they can key `MapxOrd`, `VerMap`, and
`SlotDex` with the same big-endian key encoding as their former `u64` aliases.
Branch and commit keys contain their big-endian numeric IDs.

### Commit

```
Commit {
    id:           CommitId  (transparent u64 newtype),
    root:         NodeId    (B+ tree root snapshot),
    parents:      Vec<CommitId>,
    timestamp_us: u64,
    ref_count:    u32,       // branch HEADs + child parent-links
}
```

- `parents.len() == 0` → initial commit
- `parents.len() == 1` → normal linear commit
- `parents.len() == 2` → merge commit `[target_head, source_head]`

### BranchHead

```
BranchHead {
    head:       CommitId,   // latest committed snapshot (0 = no commits yet)
    dirty_root: NodeId,     // uncommitted working-tree root
}
```

### B+ Tree Node (B = 16, max 32 keys per node)

```mermaid
graph TB
    subgraph "Internal Node"
        I["keys: [k1, k2, ..., kN]<br/>children: [c0, c1, c2, ..., cN]"]
    end
    subgraph "Leaf Node"
        L["keys:   [k1, k2, ..., kN]<br/>values: [v1, v2, ..., vN]"]
    end

    I -->|"child pointers"| L
```

A `NodeId` combines a monotonically allocated 62-bit serial with a two-bit class.
The classes distinguish internal nodes, inline leaves, leaves with blobs and blobs.
Leaf values over 1024 encoded bytes use separate blobs. A point lookup
binary-searches borrowed keys inside each fetched page. The forward iterator
retains the current leaf page and reads only the selected values.
`EMPTY_ROOT = 0` is the sentinel for an empty tree.

---

## Lifecycle

### Overview

```mermaid
graph LR
    A(["Create"]) --> B(["Write"])
    B --> C(["Commit"])
    C --> B
    C --> D(["Branch"])
    D --> B
    C --> E(["Merge"])
    E --> C
```

Typical flow: **create → write → commit → branch → merge**
(GC is automatic — no explicit step needed)

### Detailed Step-by-Step

```mermaid
sequenceDiagram
    participant U as User
    participant V as VerMap
    participant T as PersistentBTree
    participant D as Disk (MMDB)

    Note over V: 1. CREATE
    U->>V: VerMap::new()
    V->>D: allocate main branch (head=0, dirty_root=0)

    Note over V: 2. WRITE (uncommitted)
    U->>V: insert(main, key, val)
    V->>T: tree.insert(dirty_root, key, val)
    T-->>V: new_root (COW)
    V->>V: dirty_root = new_root

    Note over V: 3. COMMIT
    U->>V: commit(main)
    V->>V: alloc CommitId
    V->>D: store Commit{root: dirty_root, parents: [] or [head], ref_count: 1}
    V->>V: head = new CommitId

    Note over V: 4. BRANCH
    U->>V: create_branch("feat", main)
    V->>D: store BranchHead{head: main.head, dirty_root: main.dirty_root}
    Note right of V: No data copied! Both share same tree root.

    Note over V: 5. MERGE
    U->>V: merge(feat, main)
    V->>V: find all lowest common ancestors
    V->>T: replay union of source diffs from merge bases
    T-->>V: merged_root
    V->>D: store merge Commit{parents: [main.head, feat.head]}

    Note over V: 6. GC (automatic)
    Note right of V: Dead commits are hard-deleted<br/>by ref-count cascade during<br/>delete_branch / rollback_to.
    Note right of T: Retire dead roots after publication.<br/>Readers retain pinned roots.
```

---

## Copy-on-Write & Structural Sharing

Branching copies only branch metadata and retains existing roots; it does not
copy entries. The operation still performs writes and a durability sync.
Both branches point to the **same B+ tree root**. The first mutation
triggers copy-on-write, allocating only the modified path (~O(log n) nodes).

```mermaid
graph TB
    subgraph "Before mutation (shared)"
        R1["Root (r1)"]
        I1["Internal A"]
        I2["Internal B"]
        L1["Leaf 1"]
        L2["Leaf 2"]
        L3["Leaf 3"]
        L4["Leaf 4"]

        R1 --> I1
        R1 --> I2
        I1 --> L1
        I1 --> L2
        I2 --> L3
        I2 --> L4
    end

    MH["main.dirty_root"] -.-> R1
    FH["feat.dirty_root"] -.-> R1

    style MH fill:#4a9,color:#fff
    style FH fill:#49a,color:#fff
```

After `feat.insert(key_in_leaf3, new_val)`:

```mermaid
graph TB
    subgraph "Shared nodes (unchanged)"
        I1["Internal A"]
        L1["Leaf 1"]
        L2["Leaf 2"]
        L4["Leaf 4"]
        I1 --> L1
        I1 --> L2
    end

    subgraph "main's view"
        R1["Root r1"]
        I2["Internal B"]
        L3["Leaf 3"]
        R1 --> I1
        R1 --> I2
        I2 --> L3
        I2 --> L4
    end

    subgraph "feat's new nodes (COW)"
        R2["Root r2 (new)"]
        I3["Internal B' (new)"]
        L5["Leaf 3' (new)"]
        R2 --> I1
        R2 --> I3
        I3 --> L5
        I3 --> L4
    end

    MH["main.dirty_root"] -.-> R1
    FH["feat.dirty_root"] -.-> R2

    style R2 fill:#c62,color:#fff
    style I3 fill:#c62,color:#fff
    style L5 fill:#c62,color:#fff
    style MH fill:#4a9,color:#fff
    style FH fill:#49a,color:#fff
```

> Only 3 new nodes allocated (red). The 4 shared nodes (Internal A, Leaf 1, Leaf 2, Leaf 4) are referenced by both versions simultaneously.

---

## Commit DAG

Commits form a **Directed Acyclic Graph** via parent pointers.
Linear commits have one parent; merge commits have two.

```mermaid
gitGraph
    commit id: "c1"
    commit id: "c2"
    branch feature
    commit id: "c3"
    commit id: "c4"
    checkout main
    commit id: "c5"
    merge feature id: "c6 (merge)"
    commit id: "c7"
```

### Commit Parent Relationships

```mermaid
graph RL
    c1["c1<br/>parents: []"]
    c2["c2<br/>parents: [c1]"]
    c5["c5<br/>parents: [c2]"]
    c3["c3<br/>parents: [c2]"]
    c4["c4<br/>parents: [c3]"]
    c6["c6 (merge)<br/>parents: [c5, c4]"]
    c7["c7<br/>parents: [c6]"]

    c2 --> c1
    c3 --> c2
    c4 --> c3
    c5 --> c2
    c6 --> c5
    c6 --> c4
    c7 --> c6

    style c6 fill:#c62,color:#fff
```

Each commit's `root` field is a **snapshot** — an immutable B+ tree root that
captures the full state of the map at that point in time.

`at(commit)` and `snapshot(branch)` return a read-only view that retains its
captured tree root. Later changes through a restored alias, including rollback
and branch deletion, do not change the view. Its iterators share that retention
and can outlive the view itself. Capturing a view updates only runtime ownership;
it does not write persistent state or copy entries. Iteration remains streaming.

A view keeps its tree nodes alive until it and all its iterators are dropped. It
does not keep the commit record or branch alive, so a later `at(commit)` can fail
for a deleted commit even while an existing view still reads its contents.

---

## Three-Way Merge Algorithm

### Overview

`merge(&mut self, source: BranchId, target: BranchId) -> Result<CommitId>`
finds all lowest common ancestors (merge bases) and replays source changes
onto the target. It rejects self-merge, uncommitted source or target branches,
and a source branch with no commits. Equal heads return the existing commit.
An empty target fast-forwards to the source head; every other successful merge
creates a two-parent commit, even when it can reuse an existing tree root.

```mermaid
graph TB
    A["Ancestor<br/>(common base)"]
    S["Source branch<br/>(incoming changes)"]
    T["Target branch<br/>(receiving changes)"]
    M["Merged result"]

    A -->|"what changed<br/>in source?"| S
    A -->|"what changed<br/>in target?"| T
    S -->|"source wins<br/>on conflict"| M
    T -->|"target-only changes<br/>preserved"| M
```

### Tree-root fast paths

For a single merge base, the B+ tree layer can reuse roots as follows. These
are tree-state shortcuts, not branch-history fast-forwards: the public
`VerMap::merge` still follows the commit rules above. Multiple merge bases
use the union of their source diffs.

```mermaid
flowchart TD
    Start(["Tree merge with one base"]) --> FP1{"base_root == source_root?"}
    FP1 -->|Yes| KeepT["Reuse target_root: source unchanged"]
    FP1 -->|No| FP2{"base_root == target_root?"}
    FP2 -->|Yes| KeepS["Reuse source_root: target unchanged"]
    FP2 -->|No| FP3{"source_root == target_root?"}
    FP3 -->|Yes| Same["Reuse common root"]
    FP3 -->|No| Full["Diff and replay source changes"]
```

### Full Three-Way Merge Process

Every row of the conflict matrix below reduces to one rule: a key whose
source state differs from the ancestor takes the source state; every other
key keeps the target state. The merge therefore replays the source-side
delta onto the target tree instead of rebuilding it:

```mermaid
flowchart TD
    Start(["Full three-way merge"]) --> Diff["diff(ancestor → source)<br/>(one per merge base; union)<br/>skips shared subtrees"]
    Diff --> Loop{"More changed keys?"}
    Loop -->|Yes| Same{"target already<br/>has source state?"}
    Same -->|Yes| Loop
    Same -->|No| Apply["COW insert / remove<br/>on the running root"]
    Apply --> Loop
    Loop -->|No| MergeCommit["Create merge commit<br/>parents: [target.head, source.head]"]

    style MergeCommit fill:#c62,color:#fff
```

Diff skips subtrees with identical NodeIds. With structural sharing, this
limits work to changed paths; independently built trees can require a full
walk even when their contents match. Replay updates only the source delta
(roughly changed keys × tree depth), retaining unaffected subtrees. Finding
merge bases also walks commit history. All replay steps share one write
buffer, discarding intermediate versions before they reach the engine.
`diff_commits` / `diff_uncommitted` use the same shared-subtree skipping walk.

### Conflict Resolution Matrix

> **Rule: Source wins on conflict.**

```mermaid
graph LR
    subgraph "No conflict"
        NC1["Only source changed → use source"]
        NC2["Only target changed → use target"]
        NC3["Neither changed → keep as-is"]
        NC4["Both changed to same value → keep"]
        NC5["Both deleted → delete"]
    end

    subgraph "Conflict (source wins)"
        C1["Source=S, Target=T → S"]
        C2["Source=deleted, Target=T → deleted"]
        C3["Source=S, Target=deleted → S"]
        C4["Both added different values → source"]
    end

    style C1 fill:#c62,color:#fff
    style C2 fill:#c62,color:#fff
    style C3 fill:#c62,color:#fff
    style C4 fill:#c62,color:#fff
```

Full table:

| Ancestor | Source | Target | Result | Type |
|:---------|:-------|:-------|:-------|:-----|
| A | A | A | A | no change |
| A | **S** | A | **S** | source-only |
| A | A | **T** | **T** | target-only |
| A | **S** | **S** | **S** | both same |
| A | **S** | **T** | **S** | conflict → source wins |
| A | _deleted_ | A | _deleted_ | source-only delete |
| A | A | _deleted_ | _deleted_ | target-only delete |
| A | _deleted_ | **T** | _deleted_ | conflict → source wins |
| A | **S** | _deleted_ | **S** | conflict → source wins |
| A | _deleted_ | _deleted_ | _deleted_ | both deleted |
| _absent_ | **S** | _absent_ | **S** | source-only add |
| _absent_ | _absent_ | **T** | **T** | target-only add |
| _absent_ | **S** | **S** | **S** | both added same |
| _absent_ | **S** | **T** | **S** | conflict → source wins |

The caller controls priority by choosing which branch to pass as `source` vs `target`.

---

## Durability and Recovery

Working edits publish the new pages, allocator ceiling and 16-byte branch head in one atomic write batch. `write_batch(branch, edits)` sorts edits by encoded key, retains the last edit for duplicates and applies runs that share a leaf together. An ordinary insert or removal does not force a WAL sync.

History changes, including commits, branch management, rollback and merge, publish one synchronous batch. Readers observe the complete result after the WAL sync succeeds. An oversized merge or sorted `bulk_load` writes unpublished pages in approximately 16 MiB chunks. Each intermediate batch records the subtree roots at tag `06`. The final synchronous batch publishes the new root and removes its staging markers. An interrupted build leaves the previous branch state available.

An owned `Snapshot<K, V>` supports cloning and can move to another thread. `VerMap::reader()` shares the same namespace and pin table. The reader can take snapshots independently of the writer. Snapshot iterators retain their leases after the view or source map is dropped. Mutable `VerMap::clone()` and `clone_in(&namespace)` create independent stores, including all retained commits and working roots.

`flush()` and consuming `close()` checkpoint reference counts. A checkpoint includes the instance ID, the header generation and a Keccak256 digest. Open verifies branch heads and reachable parent links, checks root page headers and probes the ceiling of each object class. It accepts a matching checkpoint after validating its reference graph. A missing, stale or damaged checkpoint, or an unpublished or retired root marker, triggers a full recount. Read-only opens validate without writing repairs. Writable recovery mutates the store and requires the same writer exclusion as other structural changes. Use `reader()` alongside an active writer.

`from_meta_verified(id)` also checks every reachable slotted page, blob reference, separator, child height and occupancy. `verify_and_gc()` runs that verification on an open map and returns live object and commit counts and the number of removed orphan commits. Verification rejects missing reachable history and truncated allocator ceilings before reclamation.

Standalone `PersistentBTree` stores its allocator ceiling at tag `09` in the same batch as the pages it allocates. Both the versioned and standalone handles carry a fixed 36-byte typed frame. For offline conversion of external standalone root IDs, supply every root to `vsdb_migrate::versioned::migrate_tree_v17` and retain the returned ID mapping. [Migration guide](../../migration/README.md)

## Garbage Collection

Commits count branch-head and commit-parent references. Deleting a branch or rolling it back cascades through commits that lose their last owner. B+ tree objects count incoming edges and root owners. The runtime stores counts as `u32` and retains child lists only for objects with outgoing edges.

A unique writer includes the dead object keys in its publication batch. With readers or restored aliases present, the writer records retired roots at tag `05`. A successful publication withdraws ownership and queues unpinned objects for the next batch or checkpoint. Readers can pin the old root during a synchronous publication. The last snapshot lease releases its pin and queues any objects that have become unreachable. Engine compaction reclaims physical disk space.

### Example

```mermaid
graph RL
    subgraph "Before delete_branch(feat)"
        c1["c1 (ref=2)"] --> c0["c0 (ref=1)"]
        c2["c2 (ref=1)"] --> c1
        c3["c3 (ref=1)"] --> c1
        c4["c4 (ref=1)"] --> c3
    end

    MAIN["main.head"] -.-> c2
    FEAT["feat.head"] -.-> c4
```

After `delete_branch(feat)`:
- Ref-count cascade immediately deletes **c4** (ref 1→0) and
  **c3** (ref 1→0).
- **c1** drops from ref=2 to ref=1 (still alive via c2).
- B+ tree nodes from c3/c4 with no remaining owner (including live views)
  are deleted with publication or queued for reclamation after the last reader releases them.
- MMDB can reclaim their disk space when compaction rewrites the affected SSTs.

---

## Fork Point & Commit Distance

These APIs support divergent-branch detection.

### Fork Point

`fork_point(a, b)` returns one **lowest common ancestor**. If criss-cross
history has several merge bases, it selects the greatest CommitId among them;
`merge` itself considers all bases. It returns `None` when no common ancestor
is found.

```mermaid
graph RL
    c0["c0"]
    c1["c1"] --> c0
    c2["c2"] --> c1
    c3["c3"] --> c2
    f1["f1"] --> c1
    f2["f2"] --> f1
    f3["f3"] --> f2

    LCA["fork_point(c3, f3) = c1"]

    style c1 fill:#f90,color:#fff
    style LCA fill:#f90,color:#fff
```

### Commit Distance

`commit_distance(from, ancestor)` counts hops on the **first-parent chain** only.

```mermaid
graph RL
    c0["c0"]
    c1["c1<br/>(ancestor)"] --> c0
    c2["c2"] --> c1
    c3["c3<br/>(from)"] --> c2

    style c1 fill:#f90,color:#fff
    style c3 fill:#49a,color:#fff
```

```
commit_distance(c3, c1) = 2    (c3 → c2 → c1, two hops)
```

Combined example:

```
Main:    c0 → c1 → c2 → c3
Fork:    c0 → c1 → f1 → f2 → f3 → f4

fork_point(c3, f4)       = c1
commit_distance(c3, c1)  = 2
commit_distance(f4, c1)  = 4
```

---

## Rollback & Discard

```mermaid
flowchart LR
    subgraph "rollback_to(branch, target_commit)"
        R1["Verify target is ancestor of head"] --> R2["head = target_commit"]
        R2 --> R3["dirty_root = target_commit.root"]
        R3 --> R4["Abandoned commits auto-deleted<br/>via ref-count cascade"]
    end

    subgraph "discard(branch)"
        D1["dirty_root = head.root"] --> D2["All uncommitted<br/>changes lost"]
    end
```

---

## Summary

| Concept | Mechanism |
|:--------|:----------|
| **Versioning** | Each commit snapshots a B+ tree root |
| **Branching** | Copies only a 16-byte `BranchHead` |
| **Isolation** | Each branch has independent `head` + `dirty_root` |
| **Structural sharing** | COW B+ tree — mutations allocate ~O(log n) nodes |
| **Merge** | All merge bases, shared-subtree diff, and source-delta replay; source wins on conflict |
| **GC** | Automatic ref-count cleanup and fenced node retirement; physical compaction reclaim is best-effort. `gc()` supports repair/full node sweeps |
| **Persistence** | All state stored in MMDB via `MapxRaw` |
