# 两份性能报告的代码核验与修复

日期：2026-11-12。审查输入是工作区本地的
`docs/proposals/performance-bottlenecks.md`（性能瓶颈建议稿）和
`docs/proposals/performance-bottlenecks-independent-review.md`（独立审查）。
这两份原始报告仅保留在本地，不随本次修复提交。
两份输入都是静态分析，不能把其中的预期收益视为实测结论。本次针对
`1978ca4` 上可确认的重复工作做局部修复；不改变公共 API、磁盘格式、
默认分片路由、深拷贝语义或查询结果保障。

## 采纳的改动

| 路径 | 确认的问题与修复 | 保留的正确性保障 |
| --- | --- | --- |
| VerMap merge-base | 每轮遍历 heap 检查非 stale 节点；改为入队、变 stale、出队时维护 live 数量 | 原有 paint-down、全部最低共同祖先、stale 提前停止与 source-wins 语义 |
| VerMap 恢复 | 需要修复引用计数时重复验证可达提交图；复用首次验证得到的计数 | clean/read-only 恢复仍验证图；ID 修复、dirty marker、持久化和回收顺序不变 |
| B+ 树引用重建 | 为了读取 children 而复制整页键和值；复用同一解析器，只保留 ownership edges | 所有长度、tag、child 检查仍执行；快照 pin 的引用锁仍覆盖播种、遍历与替换 |
| B+ 树迭代 | 解码后复制整片叶子，返回时再复制单项；直接接管并逐项移出缓冲 | 独立迭代器、range 边界、快照与原树内容不变 |
| B+ 树删除与 merge | 删除节点只为分配后判断长度而 clone keys；删除 delta 先 get 再下降删除 | 提前保存 underflow 判断并移动 keys；删除直接复用 absent 时不分配、返回原 root 的实现；插入仍保留相等检查 |
| SlotDex batch | 每条唯一键都重新统计顶层 pending floors；改为增量维护每层新增 floor 数 | 保持与串行插入一致的升层时机；已有 floor、重复键不计新增；仍是一次原子批 |
| VecDex filter | 输出前重新读取已通过谓词的结果键；查询内至多缓存 k 个通过键，命中后移入输出 | 大键缓存不随 visited 集增长；miss 正常补读；谓词调用、断连分量回退、ef=N 与排序不变 |
| DagMap 阶段屏障 | 强制刷出命名空间全部 memtable；原位使用现有 all-shard WAL fence | 八处阶段屏障全部保留，失败即停止；range tombstone、marker、重挂与清理发布顺序不变 |
| DagMap 元数据遍历 | 主链成员用线性查找；恢复清理为每个标记节点重复扫描根 registry | HashSet 判断成员；一次扫描收集所有匹配 registry IDs，仍保留所有权检查和重复 alias 清理 |
| Namespace 恢复扫描 | 持全局 open 表锁扫描全部引擎；先复制 Namespace handles 再扫描 | Arc 在扫描期间保活引擎；缺 ceiling 的恢复证据不减少 |
| MPT/SMT cache | 树编码再复制到 payload，SMT 每个子树另建缓冲；直接追加到同一 payload | 字节顺序、版本、checksum、哈希和加载校验不变 |
| Merkle 全量重建 | 先 collect 全部持久化 KV 再建 trie；直接消费 snapshot iterator | 仍在完整构建成功后替换 trie，失败保留原 trie；读取期间 snapshot lease 有效 |

## 未直接采纳的建议及纠偏

- **SlotDex Desc 不存在报告所述的逐槽重建计数快照。** `iter` 只创建一次
  `level0_range`，mmdb 4.3.7 已提供反向 seek/prev。每槽 entry range 仍有成本，
  但段内整槽缓冲可能破坏现有流式读取的内存特性，不能据错误前提改写。
- **串行同桶写入已有 clone 前的容量门槛。** 本次解决的是 batch 中重复
  遍历 pending floors，不是再增加一个同桶 clone gate。
- **VecDex 缓存无法消除每个首次过滤节点的键读取。** 键与向量是不同的行；
  本次省去的是输出阶段缓存命中的重复读取，至多 k 次。缓存只保留前 k 个
  通过键，未命中的结果仍正常补读，拒绝的键不缓存；避免大键、小 k、宽 ef
  查询因为这项优化额外保留所有通过节点的键。
- 不给 VecDex 默认回退加 cap：现有断连图、选择性过滤及 exhaustive 查询
  测试要求补全。`compact`、SlotDex batch 与 prune fold 也不能无发布协议地拆批。
- 不跳过恢复校验，不把引用重建的 I/O 直接移出 refs 锁，不将叶子读取简化为
  只看 tag。前者保护存活数据与 ID，后两者分别保护 snapshot pins 和结构校验。
- 不直接建立 prefix 分 shard 待发队列：当前 safe restore 的 reservation
  协议依赖单调游标与 pending windows；提前发行再缓存会让恢复绕过待发 ID，
  必须先设计完整的防碰撞协议。缺 ceiling 的全扫继续作为恢复兜底。
- `destroy` 尾部不能只删一个已知 child ID：同一实例可能存在多个 registry
  aliases。原有扫描清理全部别名的行为继续保留。
- 不调大默认 memtable、不改压缩参数和 block size：同步 flush 的尺寸模型
  值得重新测量，但本次没有足以支持新默认值的吞吐/RSS 对照。
- decoded-node cache、共享 Trie/COW、工作态事务、惰性恢复、向量行合并和
  value 分离涉及额外内存策略、生命周期或兼容协议，本次未作为局部优化实现。
- `Mapx` 字节借用查询与 mutable guard 的检查还承担数组/custom codec、
  非稳定编码和内部可变性保障；不能仅为减少编码次数删除。明确 dirty 的 guard
  已有只编码一次的路径。mmdb 仍无独立 WAL-sync API，现有同步保留键 delete
  fence 也不能简单删除。

## 验证与测量

使用更新后的官方 stable（本机为 rustc 1.99.1，x86_64 Linux）。测试用父进程
独立的临时 `VSDB_BASE_DIR`，不操作用户数据目录；保留默认并行测试。

新增回归覆盖宽 DAG 与参考全图算法比较、包含/排除边界四种组合的跨叶大值迭代、
metadata/full 节点解析在每个截断位置均拒绝、批量升层与串行持久化行相等、
恢复后继续写入、过滤键的解码次数，以及 DagMap 的重复 registry aliases。
MPT 沿用已有历史 root/cache 摘要 fixture；SMT 新增从修改前 HEAD 和同一
Cargo.lock 独立生成的三键 cache 摘要，包含多字节长度字段，验证编码兼容、
加载及后续增删。删除与 merge 复用现有随机删除、决策矩阵及引用计数回归。
DagMap 子进程用例在阶段 fence 后直接退出再恢复，覆盖 range tombstone，
并在独立命名空间断言真实 prune/destroy 不增加 SST flush 数。这些是进程退出
恢复测试，不是物理断电模拟。

基准使用相同机器、数据集和 Criterion 配置，依次运行，比较修改前后的
`perf-before`。B+ 树使用现有 `versioned` 的 5000 条完整迭代；新建
`slotdex_batch` 避免为无关分页基准构造数据。仅计时插入，clear 不计时。
这组测量针对热缓存与指定输入，不代表全库负载、冷启动或尾延迟。

| Criterion 用例 | 修改前估计 | 修改后估计 | 观察 |
| --- | --- | --- | --- |
| `versioned`: `iter full (5k keys)` | 988.06 µs [980.90, 1000.3] | 696.99 µs [691.28, 703.60] | 显著改善，Criterion change 区间约 -31% 至 -29% |
| `slotdex_batch`: `wide_then_hot/1024`，12049 条 | 141.92 ms [138.65, 144.82] | 77.104 ms [64.961, 83.564] | 显著改善；消除了触达 1024 个 pending floors 后每条 hot key 的重复扫描 |
| `slotdex_batch`: `wide_floors/16`，10000 条 | 167.66 ms [128.53, 221.12] | 163.67 ms [123.75, 216.60] | 波动较大，p=0.88，无显著变化，不宣称这类输入提速 |

方括号是 Criterion 给出的估计区间。基准命令：

```bash
cargo +stable bench -p vsdb --bench versioned -- 'iter full' --save-baseline perf-before
cargo +stable bench -p vsdb --bench slotdex_batch -- --save-baseline perf-before
# 应用修复后，使用相同输入比较：
cargo +stable bench -p vsdb --bench versioned -- 'iter full' --baseline perf-before
cargo +stable bench -p vsdb --bench slotdex_batch -- --baseline perf-before
```

最终验证全部通过：

```bash
cargo +stable fmt --all -- --check
cargo +stable clippy --workspace --all-targets -- -D warnings
cargo +stable test --workspace --release --tests --quiet
cargo +stable test --workspace --tests --quiet
cargo +stable test --workspace --doc --quiet
git diff --check
```

release 与 debug 均包含 `vsdb` 的 646 个库测试、`vsdb_core` 的 44 个库测试，
以及工作区全部集成测试。原有作为子进程入口的 ignored helpers 保持原设置。
缺 ceiling 的四个恢复用例、namespace lifecycle、快照 pin 与回收竞争、
criss-cross merge、断连图 exhaustive/filter、proof transport 和只读恢复均通过。
最终的 VecDex 回归同时验证缓存命中复用，以及超过 k 个通过键后，断连图
fallback 找到的未缓存最近邻仍正确补读，且不重复调用谓词。
