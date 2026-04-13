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
- [Garbage Collection](#garbage-collection)
- [Fork Point & Commit Distance](#fork-point--commit-distance)

---

## Architecture Overview

```mermaid
graph TB
    subgraph "User API"
        VM["VerMap&lt;K, V&gt;"]
    end

    subgraph "Version Control"
        BR["BranchState<br/>(name, head, dirty_root)"]
        CM["Commit<br/>(id, root, parents, timestamp)"]
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
        A["VerMap&lt;K, V&gt;<br/>branch / commit / merge / rollback / gc"]
    end
    block:L2["Layer 2 — Persistent B+ Tree"]
        B["PersistentBTree<br/>insert / remove / iter / range / bulk_load / gc<br/>Copy-on-Write, structural sharing"]
    end
    block:L3["Layer 3 — Raw KV Storage"]
        C["MapxRaw / MapxOrd / Mapx<br/>MMDB-backed byte storage"]
    end
```

**VerMap** holds the following persistent state:

| Field | Type | Purpose |
|:------|:-----|:--------|
| `tree` | `PersistentBTree` | Shared node pool for all versions |
| `commits` | `MapxOrd<u64, Commit>` | CommitId → Commit metadata |
| `branches` | `MapxOrd<u64, BranchState>` | BranchId → branch state |
| `branch_names` | `Mapx<String, u64>` | name → BranchId lookup |
| `next_commit` | `Orphan<u64>` | monotonic CommitId allocator |
| `next_branch` | `Orphan<u64>` | monotonic BranchId allocator |
| `main_branch` | `Orphan<u64>` | protected main branch ID |

---

## Core Data Structures

### Commit

```
Commit {
    id:           CommitId  (u64),
    root:         NodeId    (B+ tree root snapshot),
    parents:      Vec<CommitId>,
    timestamp_us: u64,
    ref_count:    u32,       // branch HEADs + child parent-links
}
```

- `parents.len() == 0` → initial commit
- `parents.len() == 1` → normal linear commit
- `parents.len() == 2` → merge commit `[target_head, source_head]`

### BranchState

```
BranchState {
    name:       String,
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

Each `NodeId` is a `u64`, monotonically allocated and never reused.
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
    V->>D: store Commit{root: dirty_root, parents: [head]}
    V->>V: head = new CommitId

    Note over V: 4. BRANCH
    U->>V: create_branch("feat", main)
    V->>D: store BranchState{head: main.head, dirty_root: main.dirty_root}
    Note right of V: No data copied! Both share same tree root.

    Note over V: 5. MERGE
    U->>V: merge(feat, main)
    V->>V: find common ancestor (BFS)
    V->>T: three-way merge (ancestor, source, target)
    T-->>V: merged_root
    V->>D: store merge Commit{parents: [main.head, feat.head]}

    Note over V: 6. GC (automatic)
    Note right of V: Dead commits are already hard-deleted<br/>by ref-count cascade in step 5.
    Note right of T: Dead B+ tree nodes are registered<br/>for deferred deletion (lazy_delete).<br/>MMDB reclaims disk space during<br/>background compaction.
```

---

## Copy-on-Write & Structural Sharing

Branching is **instantaneous** — no data is physically copied.
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

---

## Three-Way Merge Algorithm

### Overview

`merge(source, target)` finds the common ancestor, then resolves every key
across all three versions:

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

### Fast Paths

Before running the full algorithm, three fast paths are checked:

```mermaid
flowchart TD
    Start(["merge(source, target)"]) --> ChkDirty{"Both branches<br/>committed?<br/>(no dirty state)"}
    ChkDirty -->|No| Err["Error: uncommitted changes"]
    ChkDirty -->|Yes| FindAnc["Find common ancestor<br/>(BFS from both heads)"]
    FindAnc --> FP1{"ancestor_root<br/>== source_root?"}
    FP1 -->|Yes| KeepT["Return target_root<br/>(source unchanged)"]
    FP1 -->|No| FP2{"ancestor_root<br/>== target_root?"}
    FP2 -->|Yes| KeepS["Return source_root<br/>(target unchanged, fast-forward)"]
    FP2 -->|No| FP3{"source_root<br/>== target_root?"}
    FP3 -->|Yes| Same["Return source_root<br/>(both converged)"]
    FP3 -->|No| Full["Full three-way merge"]

    style KeepT fill:#4a9,color:#fff
    style KeepS fill:#4a9,color:#fff
    style Same fill:#4a9,color:#fff
    style Full fill:#c62,color:#fff
```

### Full Three-Way Merge Process

```mermaid
flowchart TD
    Start(["Full three-way merge"]) --> Iters["Create 3 sorted iterators:<br/>ancestor, source, target"]
    Iters --> Loop{"More keys?"}
    Loop -->|No| Build["bulk_load(merged_entries)<br/>→ new B+ tree root"]
    Loop -->|Yes| Peek["Peek smallest key<br/>across all 3 iterators"]
    Peek --> Extract["Extract values:<br/>a_val, s_val, t_val<br/>(None if key absent)"]
    Extract --> Decide["Apply decision matrix"]
    Decide --> Emit{"Result?"}
    Emit -->|"Some(v)"| Add["Add (key, v) to merged"]
    Emit -->|"None"| Skip["Key deleted"]
    Add --> Loop
    Skip --> Loop
    Build --> MergeCommit["Create merge commit<br/>parents: [target.head, source.head]"]

    style MergeCommit fill:#c62,color:#fff
```

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

## Garbage Collection

**Users do not need to call `gc()` in normal operation.**  Both commit
cleanup and B+ tree disk reclamation happen automatically.

### How It Works

Lifecycle management is split into two layers, both fully automatic:

1. **Commit ref counting** — each `Commit` tracks a `ref_count`
   (branch HEADs + child parent-links).  When a branch is deleted or
   rolled back, commits whose `ref_count` drops to zero are
   **immediately hard-deleted** via cascading decrement.

2. **B+ tree node ref counting + lazy deletion** — `PersistentBTree`
   maintains an in-memory `HashMap<NodeId, NodeRef>` that tracks
   per-node reference counts.  When a commit root is released and a
   node's count reaches zero, it is:
   - cascade-removed from the in-memory ref map, **and**
   - registered for deferred disk deletion via the MMDB storage
     engine's compaction filter (`lazy_delete`).

   The underlying MMDB engine reclaims disk space during background
   compaction — no user action required.

```mermaid
flowchart TD
    Start(["delete_branch / rollback"]) --> RC["Decrement commit ref_count"]
    RC --> Dead{"ref_count == 0?"}
    Dead -->|yes| Del["Hard-delete commit"]
    Del --> Rel["release_node(commit.root)"]
    Rel --> NodeRC["Cascade-decrement<br/>B+ tree node ref_counts"]
    NodeRC --> NodeDead{"node ref_count == 0?"}
    NodeDead -->|yes| Lazy["lazy_delete(node key)<br/>→ MMDB dead_keys set"]
    Lazy --> Compact["MMDB background compaction<br/>→ disk space reclaimed"]
    Dead -->|no| Done(["Done"])
    NodeDead -->|no| Done

    style Del fill:#4a9,color:#fff
    style Lazy fill:#4a9,color:#fff
    style Compact fill:#4a9,color:#fff
```

### When to Call `gc()` Explicitly

`gc()` is still available for two edge cases:

- **Crash recovery** — if a ref-count cascade was interrupted
  (`gc_dirty` flag set), `gc()` rebuilds all commit ref counts from
  scratch and removes orphaned commits.
- **Forced full sweep** — guarantees every unreachable node is
  registered for compaction, even if a prior cascade was incomplete.

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
- B+ tree nodes from c3/c4 are released from the in-memory ref map
  **and** registered for deferred disk deletion via `lazy_delete`.
- MMDB background compaction reclaims disk space automatically.

---

## Fork Point & Commit Distance

These APIs support divergent-branch detection.

### Fork Point

`fork_point(a, b)` finds the **lowest common ancestor** of two commits.

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
| **Branching** | Copies only a lightweight `BranchState` struct |
| **Isolation** | Each branch has independent `head` + `dirty_root` |
| **Structural sharing** | COW B+ tree — mutations allocate ~O(log n) nodes |
| **Merge** | Three-way with sorted iterators; source wins on conflict |
| **GC** | Fully automatic — commit ref counting + B+ tree lazy deletion via MMDB compaction; `gc()` only for crash recovery |
| **Persistence** | All state stored in MMDB via `MapxRaw` |
