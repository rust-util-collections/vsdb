# 性能优化路线与结构性改造候选

日期：2026-11-17。本文基于
[performance-fixes.md](../evaluations/performance-fixes.md) 核验后的有效结论整理，
汇总当前代码基线（`2996b1d` / `v17.0.19`）上仍成立但**尚未施行**的性能调优方向、
运维用法与结构性改造候选。

> 本文不是已采纳的设计定稿，也未新增实测结论。文中涉及盘上格式、公开接口或
> 引擎默认参数的条目，均需先补齐对应压测数据，再按兼容性策略单独设计与落地。

---

## 一、现即可用的配置与用法建议（无需改码）

以下约束来自当前引擎与集合实现，不改动 VSDB 代码即可用于缓解热路径瓶颈：

1. **热集合单独放置到 `shards = 1` 命名空间**
   - 默认命名空间固定为 16 分片（`NUM_SHARDS`），路由规则 `prefix % shard_count`
     使单集合的全部键只落入 1 个分片，其余 15 个分片的写缓冲、WAL 与压缩
     worker 不参与该集合。
   - 在默认 2 GiB 预算下，[`mmdb_open`](../../core/src/common/engine/mmdb.rs)
     给单分片的写缓冲为 `16 MiB`（`block cache` 池 `16 × 16 MiB = 256 MiB`）。
     而在 `mmdb 4.3`（当前 `Cargo.toml` 要求 `4.3.12`，已在本地 `4.3.7` /
     `4.3.11` 源码确认）中，`DbOptions::max_immutable_memtables` 已标注未使用，
     memtable 满后由写批 leader 在写线程同步执行 `freeze_memtable_sync` 与
     `flush_frozen_memtable`（读路径不阻塞）。因此单集合持续写入会周期性在约
     `16 MiB` 处触发同步刷盘；L0 达 8 个文件进入写减速，且单分片默认只有 1 个
     后台压缩线程（L2 起使用 Zstd）。
   - 对吞吐敏感的热集合，可将其置于独立命名空间（`shards = 1`）并按需配置
     `mem_budget_mb`（例如 1 分片、2 GiB 预算下写缓冲约 `256 MiB`，同步刷盘
     次数约降一个数量级）；**不要**为吞吐而修改默认命名空间的 16 分片常量。
2. **应用侧拆分超大逻辑表**
   - 前缀分配器递增分配，连续创建的 16 个 `Mapx` 会均匀分布到默认命名空间的
     16 个分片。应用侧可按键哈希分桶至多份 `Mapx` 并在调用侧合并范围扫描。
3. **沿用既有的批量与检查点用法**
   - 批量导入使用 `Mapx` / `SlotDex` / `VecDex` 现有 `batch()` /
     `insert_batch()`；需要同时落盘持久性时使用 `commit_sync()`（若需覆盖该批
     之前的异步写，再调用 `sync_wal`）。
   - `DagMap` 保持主链尽量短并定期 `prune`，减少沿父链逐层点查与墓碑堆积。
   - `VerMapWithProof` 按检查点显式调用 `save_cache`。
   - `VecDex` 面对高选择性过滤谓词时，适当调低 `ef`（过滤 visit cap 为
     `max(64 * ef, 4096)`）比调整图参数更直接。
   - `mmdb` 的 `lazy_delete_compaction_threshold` 保持默认关闭（0）：开启后每层
     重写会长时间持有 DB 锁阻塞读写；空间回收优先依赖正常 compaction 或在维护
     窗口显式调用 `compact()`。

---

## 二、局部优化候选（需先有基准或剖析数据再推进）

### 1. 引擎 Sizing 与压缩参数重测

- **重新评估写缓冲尺寸公式**：[`mmdb_open`](../../core/src/common/engine/mmdb.rs)
  当前按「1 个活跃 + 最多 4 个不可变 memtable」将单分片写缓冲钳位在
  `mem_budget / 8 / shards`。基于 `mmdb 4.3` 已改为写线程同步刷盘的事实，可
  在充分压测峰值 RSS 与写停顿后，重新校准默认写缓冲上限（向 `mmdb` 默认 64 MiB
  靠近），并将新预算公式写入变更说明。
- **热分片压缩与块大小实验**：
  - 对比 `max_background_compactions = 2` 或 L2 暂时保持 LZ4，确认慢压缩是否由
    L2+ Zstd 引起。
  - 当前块大小为 `16 KiB`（`mmdb` 默认 `4 KiB`）。16 KiB 适合 B+ 树大节点，但
    会让小值点查在同等 block cache 下容纳更少块。应先借助
    `Namespace::shard_properties` 观察 `stats.block_cache_hits` /
    `stats.block_cache_misses` 与 RSS，再决定是否做成可选项。

### 2. Core / Mapx 热路径分配与编码开销

- **栈 / 小缓冲构造 `full_key`**：引擎读写每次都在 `make_full_key` 为
  `8 字节前缀 + 用户键` 分配 `Vec`。`mmdb` 仅借用 `&[u8]`，可在热路径使用栈上
  定长数组、`SmallVec` 或线程局部缓冲，仅超长键退化到堆分配。
- **`Mapx` 字节/字符串借用查询单次编码快路径**：
  [`Mapx::query_bytes`](../../strata/src/basic/mapx/mod.rs) 当前会对 `&[u8]`
  先尝试按 `K` 解码、校验后再重编码，以区分定长数组（按元组编码）与带长度前缀的
  `Vec<u8>`/`Box<[u8]>`。可在严格保留该类型区分及非规范编码安全性的前提下，
  为明确同编码的字节/字符串查询提供快路径，去掉冗余的 decode + encode。
- **`MapxOrdRawKey::get_mut` 更新专用接口**：当前 `get_mut` 为兼容不稳定编码与
  内部可变性，在构造 guard 时会额外编码 `snapshot` 并拷贝 `original`（`DerefMut`
  已避免 `Drop` 时第三次编码）。若剖析显示常规标量更新开销显著，可评估增设显式
  `get_mut_for_update` / `entry` 接口。

### 3. VerMap / PersistentBTree 增量改进

- **`VerMap` 多键批量写入接口**：[`PersistentBTree`](../../strata/src/basic/persistent_btree/insert.rs)
  已具备 `insert_buffered`，但公开 `VerMap::insert` 每次均立即 `flush_pending` 并
  写入一条 branch 行。可增加多键写入接口：循环 `insert_buffered`，末尾统一
  `flush_pending` 并只更新一次 branch 行（单键 `insert` 的崩溃顺序不变）。
- **merge replay 插入路径去重查与批降**：
  [`merge::replay`](../../strata/src/basic/persistent_btree/merge.rs) 中删除路径
  已免去重复查找，但 `Some(value)` 路径仍先 `tree.get(root, &key)` 再执行
  `insert_buffered`。可将「与 target 比较」和叶编辑合并为单次下降，或按已排序
  edits 批量对同一叶子聚合修改，减少重复路径下降与路径复制。

### 4. Trie 缓存写入原子化与同步策略

- **cache 写入改为临时文件 + 原子重命名**：当前
  [`strata/src/trie/cache.rs`](../../strata/src/trie/cache.rs) 与
  [`strata/src/trie/smt/cache.rs`](../../strata/src/trie/smt/cache.rs) 仍通过
  `File::create` 直接覆写缓存文件，并发写入或中途退出可能留下半截文件。应改为
  写入临时文件并重命名替换，并在 `load` 侧实现流式读取与边解析边校验哈希，避免
  文件字节与整棵节点多份内存并存。
- **`VerMapWithProof` working-state revision 与自适应同步**：记录工作态变更版本，
  避免连续 `merkle_root` 查询重复 diff；当预估 diff 节点成本高于全量重建时，
  自动切换为流式全量重建。

### 5. DagMap 与 VecDex 局部维护成本

- **DagMap 仅对脏 shard 执行 WAL fence**：当前 `prune` / `destroy` 已把全引擎
  memtable flush 改为命名空间级 WAL fence（`try_sync_wal`），但仍会对该命名空间
  所有分片执行同步操作。可在完整保留多阶段发布顺序与恢复 marker 的前提下，收敛
  为仅收集并同步本 DAG 实际写入的分片。
- **VecDex 构图期缓存上限与增量层高维护**：限制 `Txn` 构图阶段 `vec_cache` 的
  总字节/条目预算，避免大批量插入把所有触达向量驻留到 chunk 结束；增量维护节点
  最大层高，替代 `repair_entry_point` 对候选节点重复扫描 base edge。

---

## 三、暂不施行的结构性改造（长期候选）

下列改造涉及缓存所有权、节点内存/磁盘布局、恢复协议或引擎上游接口，实现复杂度高
且需要配套迁移或兼容性评审，当前**暂不施行**，仅作后续架构演进的备忘：

| 子系统 | 候选改造 | 解决的问题 | 暂不施行的原因与前置门槛 |
| --- | --- | --- | --- |
| `PersistentBTree` | 进程内有界 `NodeId -> Arc<Node>` 解码缓存 | 避免每次路径读取重复 `get` + 解码全部兄弟 KV；大 value 时可仅缓存内部节点 | 需绑定引擎生命周期并在 `lazy_delete` 后同步失效，且必须带字节上限与写入穿透一致性 |
| `PersistentBTree` | 内存态改共享字节缓冲 + 偏移切片 `NodeRef` | 消除解码与迭代时每条 key/value 各自 `Vec` 分配（可不改磁盘格式） | 需要重构 `Node` 内部表示及 COW 物化路径 |
| `PersistentBTree` | 大 value 与叶子分离存储；按需削减叶子 refcount 内存 | 叶子仅存 value locator，减小页大小并避免节点跨多 16 KiB 块 | 改变节点字节布局，必须走版本号与迁移策略；在 value 分离之前不加大 `B = 16` |
| `VerMap` | 统一单次 recovery walk、可信 `next_id` / 图摘要持久化与惰性 ref-state | 避免 clean 打开时仍全量扫描历史 commit 与存活节点池；物理 GC 时流式删除替代整池 `dead_keys: Vec` | 不可跳过校验或削弱 `refs` 锁对 snapshot pins 的保护；需重新设计恢复信任边界与损坏降级 |
| `VerMap` | `WriteTxn` / `BranchTxn` 批量工作态叠加层 | 在内存态合并多键连续读写、一次发布 branch | 需明确事务接口与每键崩溃恢复边界；保持 `Clone` 深拷贝语义，如有需要另提供显式共享接口 |
| `VecDex` | 分块/分段 `compact()` + 原子根发布协议 | 避免大索引 `compact()` 在内存和单个 `WriteBatch` 内聚集全量向量与全图 | 必须通过新 prefix/新图分块写完再小事务切换，保持崩溃后旧图完整可用，严禁无协议拆批 |
| `VecDex` | 向量字节与 layer-0 邻接合行（或内联节点信息行）+ 定长小端 `f32`/`f64` | 搜索随机点查减半，省去 `postcard` 通用反序列化 | 改变底层 KV 行格式，需要版本化与旧数据迁移；顺带可评估可选的句柄级有界常驻向量缓存 |
| `VecDex` | 查询执行统计与显式 `FallbackPolicy` | 观测访问节点数、磁盘向量/键读次数与距离计算次数；为调用侧提供带预算的非穷尽近似模式 | 默认无过滤搜索仍需满足既有的断连图召回语义，不可静默截断默认 fallback |
| `MPT` / `SMT` | `Arc` / COW 不可变子树（或节点 arena）+ 紧凑 `Nibbles` / 切片 `BitPath` | 将 `Clone` 从整树深拷贝降至 O(path)；消除逐层路径 split/复制开销 | 需保持不可变语义与所有 Merkle hash preimage / proof transport 完全一致（注：`SmtMut::insert_rec` 已有 256-bit 深度护栏） |
| `MPT` / `SMT` | 磁盘版 Merkle 节点库 | 支持超出内存上限的超大状态树 | 仅在真实负载突破内存容量时单独立项设计 |
| `SlotDex` | 持久元数据与按需分页 bucket 缓存分离；深分页游标 `cursor` | 避免 `hydrate` 将全部 tier buckets 驻留进 `BTreeMap`；缓解深页细 slot 扫描 | 需设计页淘汰策略并保持单一活跃句柄一致性；游标分页属于公开 API 变更 |
| `DagMap` | 有界内存分阶段 `prune` fold 或单 shard co-located DagMap 新布局 | 控制长主链 `prune` 的 `StagedRows` 内存峰值；彻底消除跨 shard 同步屏障 | 分阶段发布需严密保留恢复 marker 与未重定向 child 回退；co-located 新布局需要格式迁移 |
| `Engine` / `Namespace` | 上游 `mmdb` 原生 `sync_wal`、多 range 共享 `ReadView`、零拷贝借用键切片迭代器 | 去除 `META_KEY_WAL_FENCE` 同步 delete 写放大；避免 `SlotDex`/`DagMap` 每 range 重新 snapshot 与整键复制 | 依赖上游 `mmdb` 接口演进，且 `ReadView` 需约束生命周期以防阻滞 SST/WAL 回收 |
| `Engine` / `Namespace` | 跨命名空间写缓冲共享、tiny namespace profile、安全可恢复的分 shard prefix 预发队列与 SST `max_issued_prefix` 检查点 | 降低多小命名空间常驻内存与 co-located prefix 空洞/缺 ceiling 全扫代价 | 前者延后记录见 [shared-mem-pool.md](shared-mem-pool.md)；prefix 预发队列必须先有防 ID 碰撞的恢复协议 |

---

## 四、不可逾越的正确性与兼容性边界

后续无论推进哪一项优化，均不得突破以下边界：

1. **路由与持久化格式不变量**：不得为吞吐修改默认命名空间 16 分片（`NUM_SHARDS`）；任何盘上节点/行布局变更都必须引入格式版本号、兼容读取或显式拒绝策略。
2. **崩溃原子性**：不得在无多阶段发布协议的前提下拆散 `SlotDex` 原子批、`VecDex::compact` 批或 `DagMap` 阶段屏障；`DagMap` 尾部的多别名清理与 `VerMap` 恢复校验不可简省。
3. **缓存隔离与深拷贝语义**：`SlotDex` / `VecDex` 不可用跨句柄共享可变运行时缓存掩盖单活跃句柄约束；集合 `Clone` 保持深拷贝语义；MPT/SMT 优化不可改变任何哈希域或 proof 编码。
4. **生命周期与有界内存**：长驻解码缓存必须有字节上限；复用 `ReadView` 时必须明确其对底层 SST/WAL 回收的阻塞窗口。

---

## 五、立项前应补齐的基准与观测清单

在将第二、三节中的任何候选项投入实现前，建议先补齐对应基线（避免仅凭静态推断或热缓存微基准调整默认值）：

- **Engine**：单 `Mapx` 连续写入跨越 `16 MiB` 的周期性刷盘延迟与停顿分布，同时采集 `Namespace::shard_properties` 的 cache 命中计数、L0 数量与进程 RSS；`Mapx` 小键/大值/嵌套集合的分配与编码字节计数。
- **VerMap / PersistentBTree**：冷/热 `from_meta` 打开耗时、百万节点恢复点查次数与峰值内存、长链与 criss-cross `merge-base`、不同扇出 `B`（16/32/64/128）与聚合 edits merge replay。
- **SlotDex / VecDex / DagMap**：`SlotDex` 同桶热写入与不同槽密度下的 `iter(Desc)`；`VecDex` 断连图、`ef = N`、高选择性过滤（`1e-4`）下的读键/读向量计数以及大维度 `compact` 峰值 RSS；`DagMap` 同命名空间存在大量无关读写时 `prune` / `destroy` 的前台 p99 与分片刷盘次数。
- **可选引擎遥测**：按需以条件编译或零开销开关统计 `point_gets`、`range_creates`、`snapshot_creates`、`rows_read`、`decode_calls`、`flush_calls`、`wal_syncs`、`bytes_staged`、`bytes_encoded`，确保默认热路径零额外开销。
