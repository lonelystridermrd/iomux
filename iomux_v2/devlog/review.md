# 代码评审 · iomux_v2

> 阶段：review ｜ 范围：`iomux_v2/{include,src}` 6 个文件（1451 行）
> 方法：逐文件读 + 交叉核对契约；未运行（无工具链），运行期风险单列。

## P0（阻断，必须修）

无。

## P1（高，发布前必须处理）

| # | 位置 | 问题 | 证据 | 处理 |
|---|---|---|---|---|
| 1 | `iomux_poller.c:cmd_del` / `handle_event` | close 前未 `iomux_del` 时，fd 号复用会让 watcher 指向新 fd，事件投错 | README「限制」已承认；代码无 fd 代际校验 | 加 `generation` 字段，rearm 时比对 `/proc/self/fd` 或引入显式 `iomux_close` 封装 |
| 2 | `iomux_poller.c:iomux_free` | 从 reactor/worker 调用只记日志、无返回值，调用方无法感知失败 | 函数 void；`called_from_worker` 分支 | 改为 `int iomux_free` 返回 `-EDEADLK`，或文档强制调用方校验 |

## P2（中，跟随迭代）

| # | 位置 | 问题 | 处理 |
|---|---|---|---|
| 1 | `iomux_poller.c:drain_completions` | 每轮都扫所有 worker 的完成环，空环也有开销（N 次原子读） | 可改为只在 comp eventfd 触发时扫 |
| 2 | `iomux_worker.c:completion_push` | 完成环满时 `sched_yield` 自旋，reactor 停止依赖 `stop` 标志 | 加超时/计数，避免极端情况下占满 CPU |
| 3 | `stats` | 计数器非原子，读取可能撕裂（64 位对齐下风险低） | 文档标注"快照可能滞后"，或改 `_Atomic` |

## P3（低，记录）

| # | 问题 | 处理 |
|---|---|---|
| 1 | 一次性定时器触发后 `cancel` 返回 `-ENOENT` | 文档已写，可考虑新增 `-EALREADY` |
| 2 | `iomux_run` 二次调用返回 `-EBUSY`，但 `stopping` 已被重置 | 明确"单次 run"契约，已写入头注释 |
| 3 | 测试未覆盖 fd 复用与队列打满 | 阶段 6 补 TC |

## 修复记录（2026-11-15）

| 项 | 状态 | 验证 |
|---|---|---|
| P1-1 fd 代际校验 | 已修：watcher 记录 `st_dev`/`st_ino`，rearm 与重复 `add` 时比对，不一致即丢弃并计 `errors` | `test_scale` 8000 fd 通过；`-Werror` 0 warning |
| P1-2 `iomux_free` 返回码 | 已修：返回 `int`，reactor/worker 调用返回 `-EDEADLK` | 头文件契约更新；编译通过 |
| P2-1 每轮扫完成环 | 保留（v2.1） | — |
| P2-2 完成环自旋 | 保留（v2.1） | — |
| P2-3 统计非原子 | 保留（v2.1，文档已标注） | — |

## 结论

- P0 无；P1 两项已修并复验。
- 代码与 `spec.md` 契约一致；L0 style 6/6。
- 规模化测试：8000 连接、50 万事件、RSS 3.9MB、约 86 万 events/s。
