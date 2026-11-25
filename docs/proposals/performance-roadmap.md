# 性能优化路线与结构性改造候选（v18 基线更新）

本文基于 [performance-fixes.md](../evaluations/performance-fixes.md)、`v18.0.0`（`cc2f41c`）四份子系统评测报告（[v18-basic.md](../evaluations/v18-basic.md)、[v18-slotdex-dagmap.md](../evaluations/v18-slotdex-dagmap.md)、[v18-versioned.md](../evaluations/v18-versioned.md)、[v18-trie-vecdex.md](../evaluations/v18-trie-vecdex.md)）以及针对 `cc2f41c` 新增的补充实测整理，同步当前 `v18.0.0` 已落地的结构性优化与**尚未完成评估/落地的后续候选**。完整英文设计细则与补充实测数据见 [v18-roadmap.md §10](v18-roadmap.md#10-post-implementation-structural-performance-evaluation-and-follow-up-designs)。

> 文中涉及盘上格式、公开接口或引擎默认参数的条目，均需按 [compatibility-policy.md](../ai/compatibility-policy.md) 评审后落地。

---

## 一、`v18.0.0` 已落地的结构性优化与实测边界

1. **键编码与基础集合热路径（已落地）**
   - 栈上 128B `with_encoded_key` / `with_ordered_bytes` / `with_full_key`、`(K, Q)` 借用查找 trait、去 `Box<dyn>` 的 `MmdbIter`（`iter_with_prefix` 单次 seek + 原地 `drain` 去前缀）以及仅在 `DerefMut` 触发写回的 `ValueGuard`。
   - 实测（[v18-basic.md](../evaluations/v18-basic.md)）：`Mapx` 点查 `-6.5%`、`MapxOrd` 点查 `-13.7%`；短键点查临时堆分配降至 `1` 次，未修改 64 KiB guard 分配由 `35 次 / 720 KiB` 降至 `3 次 / 131 KiB`。
2. **`SlotDex` 与 `DagMap`（已落地）**
   - `SlotDex` 采用编译期定长 `[u8; N]` `SlotType`、有界前向分块逆序遍历（`<= 256` 条且 `<= 16 KiB` 原始键）及 16B `[total, generation]`；逆序 `iter` 提速约 `6.0–6.3×`（[v18-slotdex-dagmap.md](../evaluations/v18-slotdex-dagmap.md)）。
   - `DagMap` 收敛为单分片同址（co-located）单前缀节点 + 尾部单分片 `try_sync_wal`；并发无关负载下 `prune` 由 `669.28 ms` 降至 `6.00 ms`（`111×`）。
3. **单前缀 `VerMap`、槽式 `NodeView` 与 `Mode B` 检查点（部分完成）**
   - `VerMap` 8 个句柄收敛为单前缀大端序表，`NodeView` 消除点查与 `BTreeIter` 的兄弟键值 `Vec` 分配，引入 `write_batch`、范围聚合 `apply_leaf_run`、`'static` `Snapshot` 与 `[0x07]` `REFS` 检查点（[v18-versioned.md](../evaluations/v18-versioned.md)：点查 `2.33 µs → 1.10 µs`，百万键块提交 `24.54 ms`）。
   - **未充分评估/未闭环点**：`> 1 KiB` `BLOB_LEAF` 写与 diff 放大、`VerMap::get` 每查读 `[HEAD]`、内部页重复拷贝、`Mode B` 热重开 `30.19 ms` 与深历史内存占用（见第二节）。
4. **`Arc`-COW 默克尔树与流式检查点、`VecDex` 小端切片与零分配搜索缓冲（部分完成）**
   - `MptCalc`/`SmtCalc` 改为 `Arc`-COW + 内联 `OnceLock<[u8; 32]>` + POPCNT 分支 + 原子流式带预算检查点（[v18-trie-vecdex.md](../evaluations/v18-trie-vecdex.md)：10 万键 MPT 常驻内存 `-43%`，检查点体积 `-51%`）；`VecDex` 改为 IEEE-754 LE 向量切片 + 搜索复用缓冲 + 32 MiB `Txn::vec_cache`（`N=1000` L2 搜索 `-39%`）。
   - **未充分评估/未闭环点**：`VerMapWithProof` 工作态连续 `merkle_root` 重复全量 diff、`VecDex` 构图跨批缓存丢失与低选择性过滤退化、大索引 `compact()` 全内存峰值（见第二节）。

---

## 二、补充实测揭示的热路径瓶颈与下一阶段优化设计（`v18.x` / `v19`）

### 1. `PersistentBTree`：`BLOB_LEAF` 描述符保留 COW 与零 Blob 读 Diff（`v18.x` 高优，零格式变更）

- **补充实测（1,024 键树）**：`self.node(id)` 与 `self.alloc` 在每次叶更新与 `BTreeDiffIter` 展开时都会读取并重新分配整片 `BLOB_LEAF` 内最多 32 个 `[0x12]` 大值对象；`fix_underflow` 为读取兄弟叶 `key_count()` 也会全量读取兄弟叶的 32 个 Blob。
  - 值从 `1,024 B` 增至 `1,025 B`（+1B）：`VerMap::insert` 中位延迟 `66.1 µs → 101.8 µs`（`+54%`），单键 `diff` `19.9 µs → 51.9 µs`（`2.6×`）。
  - `64 KiB` 值：40 次单键更新实际写入约 `80 MiB` 重复 Blob（理论仅需 `~2.5 MiB`），无 `fsync` 的单次 `insert` 中位 `2.05 ms`、p99 飙升至 **`53.28 ms`**（触发 16 MiB memtable 同步刷盘），40 键提交 `diff` 耗时 **`55.97 ms`**，且跨版本无法共享未修改兄弟 Blob。
- **落地设计**：
  1. 内部叶值改为 `LeafVal::Inline(Vec<u8>) | LeafVal::Blob(NodeId)`：未改动槽位直接复用原 `Blob(NodeId)` 描述符并递增 `refs.counts`，仅对新增/修改的 `> 1024 B` 值分配新 `[0x12]` 对象；
  2. `fix_underflow` 通过 `NodeView::open(&self.object(sib))?.len` 只读页头槽数（0 次 Blob 读取）；
  3. `BTreeDiffIter` 对同键两侧均为 `Blob(id)` 且 `old_id == new_id` 的槽位直接 $O(1)$ 跳过，不读 `[0x12]`；
  4. 进一步可为无分裂/合并的单键修改提供 `NodeView::splice` 原位切片拼页，省去每层最多 32 个临时 `Vec<u8>` 分配。

### 2. `VerMap`：分支头缓存、有界不可变内部页缓存与 `Mode B+` 仅共享引用表

- **补充实测**：
  - `VerMap::get` 每次点查额外执行一次 `mmdb::DB::get([HEAD | branch])`，在 5k / 100k / 500k 键下比 `Snapshot::get` 慢 `176–480 ns`（`+39%–48%`）；每下降一层内部页额外消耗 `~350–400 ns`（`DB::get` + 页 `Vec<u8>` 拷贝）；
  - `create_branch` 在 `branch_id(name)` 未命中后仍执行 `self.records(NAME).any(...)` 全分支范围扫描；
  - `Mode B` 对唯一归属（`ref_count == 1`）的 `0b01` 内联叶与 `0b11` Blob 也全部写入 `RefState.counts` 与 `[0x07]` 检查点（100 万键热重开 `30.19 ms`，RSS `24.7–34.0 MiB`）。
- **落地设计**：
  1. **去除写路径冗余 `records(NAME)` 全扫 + `TreeRuntime` 代际 `BranchHead` 缓存**（`v18.x`，零格式变更）；
  2. **`TreeRuntime` 字节有界不可变内部页缓存 `InternalPageCache`（`NodeId -> Arc<[u8]>`）**（`v18.x`，默认上限 `min(budget / 64, 16 MiB)`）：利用 COW 内部页 `NodeId` 永不复用、内容永不更改的性质，零失效竞态地缓存上层内部页（100 万键全树内部页仅约 `450 KiB`），将热树点查收敛为单次叶子 `DB::get`；
  3. **`Mode B+` 稀疏共享引用表（`extra_refs = count - 1 > 0`）+ 可选 `[0x08 | gen_be]` 增量引用日志（`v19`）**：单分支树与近期历史窗口中 `> 95%` 节点 `ref_count == 1`，省略 `0` 值项可将 `RefState` 内存与 `[0x07]` 体积压缩 `5–15×`；若配合同批盲写的 `[0x08]` 净引用增量帧（0 次点读），既避开 `Mode A` 的随机点读退化，又将热重开恢复至亚毫秒级。

### 3. `VerMapWithProof`：工作态增量游标与流式自适应重建（`v18.x`，零格式变更）

- **补充实测（`N = 20,000` 键）**：
  - 当分支存在 `2,000` 个未提交脏键时，后续每次 `1 insert + merkle_root(branch)` 耗时 **`1.69 ms`**——因当前实现未记录前一次 `working_branch` 游标，每次都克隆 `committed_trie` 并重算全部 `2,000+` 个脏键 diff（区块内逐笔求根退化为 $O(K^2)$）；
  - `sync_committed` 在 `100%` 键变更时耗时 `16.17 ms` 且一次性分配全量 `Vec<TreeDiff>`，慢于 $O(\text{height})$ 内存的流式 `from_entries`（`15.49 ms`）。
- **落地设计**：保存 `(working_branch, working_base, working_root_lease)`，同分支连续工作态求根直接 diff `prev_working -> new_working`（$O(\Delta)$）；将 `apply_diff` 改为按 `4,096` 条分块流式消费 `BTreeDiffIter`，并在空基根（如首次 `bulk_load`）时直接走 `T::from_entries`。

### 4. `VecDex` 与 `SlotDex` 热路径收敛

- **`VecDex`（`N = 2,000, dim = 128` 补充实测）**：
  - `insert_batch` 为 `283.2 µs/vec`（每 64 条重建 `Txn` 丢弃 32 MiB `vec_cache`，且反向 `prune_selection` 重复解码邻居并执行 $O(M^2)$ 多样性距离计算）；低选择性过滤 `search_with_filter` 在 `1%` 选择性下从 `140.7 µs` 退化至 **`4,610 µs`（`32.8×`）**（未满 `ef` 前对每个访问节点执行 `TAG_NODE2KEY` 点查 + `K::decode_key`）；`compact()` 仍在单批内存中驻留全图。
  - **落地设计**：`insert_batch` 跨 64 条 chunk 复用 32 MiB `vec_cache` 并在度数 `<= m_max` 时跳过拷贝/裁剪；新增零解码 `search_with_raw_filter(&[u8])` 供前缀/租户过滤跳过 `K::decode_key`；大索引 `compact()` 文档化 `~3×` 原始向量内存上限，并在 `v19` 评估基于世代前缀位切换的分块流式压缩。
- **`SlotDex::insert_batch`（`v18.x`，零格式变更）**：
  - 移出循环内每条记录对所有层级的冗余 `staged.put(level_key(i + 1, &floor), ...)`（`wide_then_hot` 12,049 条输入产生 `> 30,000` 次冗余 `Vec` 分配与 `BTreeMap` 更新），统一在循环结束后由 `bumps` 一次性写入 `staged`，并用定长键 `BTreeMap<S, EntryCnt>` 聚合 L0 槽计数。

### 5. 引擎写缓冲校准与单分片热集合放置（`mmdb 4.3.15`）

- **补充实测（单集合连续写入 60 MiB = 1,500 × 40 KiB 批）**：
  - 默认 16 分片命名空间（`2 GiB` 预算，单分片 `write_buffer_size = 16 MiB`）：总耗时 `127.4 ms`，单批最大停顿 **`26.62 ms`**（中位 `42.0 µs` 的 `634×`）；
  - 独立 `shards = 1, mem_budget_mb = 2048` 命名空间（`write_buffer_size = 256 MiB`）：总耗时 `87.4 ms`（`-31%`），单批最大延迟仅 **`0.31 ms`**（峰值尾延迟降低 `87×`）。
- **根因与路线**：
  1. **现即可用运维准则**：v18 将 `VerMap` 与连通 `DagMap` 全部收敛到单个分片，吞吐或尾延迟敏感实例应放置到 `NamespaceOpts { shards: 1, mem_budget_mb: Some(...) }` 独立命名空间；**禁止**修改默认命名空间 16 分片常量。
  2. **`mmdb_open` 参数重测候选**：`mmdb 4.3` 已废弃异步多不可变 memtable 队列（`max_immutable_memtables` 未使用，写满即由 leader 同步冻结刷盘，单分片内存峰值至多 `2×` 而非旧注释假设的 `5×`）。后续可在 16 分片满载压测验证峰值 RSS 与 L0 写减速后，评估将 `mem_budget / 8 / shards` 上调至 `mem_budget / 4 / shards`（默认单分片 `32 MiB`）。

---

## 三、不可逾越的正确性与兼容性边界

1. **路由与格式不变量**：默认命名空间固定 16 分片（`NUM_SHARDS`）；`v18.x` 补丁不得改变任何已发布盘上键值、页格式、`VSTYPE04` / `VSMAPX02` 帧或枚举判别值。
2. **崩溃原子性与恢复顺序**：`SlotDex` 单批、`DagMap` 单分片阶段顺序（先 `fold` + `reparent`、后 `Clearing`、删完数据再删 `[0x01]` 发现边 + 尾部 `try_sync_wal`）及 `VerMap` 的 `[0x05]` `RETIRED` / `[0x06]` `STAGE` 锚点不可省去。
3. **缓存安全与句柄隔离**：任何新增进程内缓存（如 `InternalPageCache`）必须严格建立在不可变对象（单调不复用 `NodeId`）之上并带硬字节上限；可变集合 `Clone` 保持深拷贝语义，MPT/SMT 优化不得改变任何 Keccak256 原像或证明编码。
