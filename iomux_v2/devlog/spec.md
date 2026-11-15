# iomux_v2 需求规格

> 先例：`dec-2026-11-15-iomux-design-choice`（accepted，约束未变，仍适用）；`kb.py search iomux --type experience` 无命中。
> 复述：数通平台需要在单进程内把多 socket 事件分发给业务回调，同时保持低延迟与可预测的内存。
> 假设：目标内核 ≥4.19；目标架构 arm64；允许静态链接。

## 目标

1. 交付 `libiomux.so` / `libiomux.a`：epoll 水平触发 + EPOLLONESHOT 抑制 + 单 Reactor + 可配置 worker 池。
2. 单实例支撑 8,000 连接、500k events/s，派发延迟 P99 ≤ 500µs。
3. 每 worker 独立 SPSC 队列，默认轮询、支持按 fd 哈希绑定线程。
4. 内置周期定时器（不补发）与 8 类防呆错误码。

## 非目标

- 不做跨平台（仅 Linux，依赖 epoll/eventfd/EPOLLONESHOT）。
- 不做协议栈、连接池、SSL、零拷贝转发。
- 不做多 Reactor（多核扩展靠多实例 + SO_REUSEPORT）。
- 不做动态扩缩容 worker。

## 接口契约

| 入口 | 输入 | 输出 | 错误 |
|---|---|---|---|
| `iomux_new` | `const struct iomux_cfg *`，NULL=默认 | `struct iomux_poller *` | NULL（配置非法/分配失败/线程失败） |
| `iomux_run` | poller | 0 正常停止 | -EINVAL / -EBUSY / -ENOMEM / -errno |
| `iomux_stop` | poller，NULL 忽略 | void | 无 |
| `iomux_free` | poller，NULL 忽略 | void | reactor/worker 线程调用被拒绝（记日志） |
| `iomux_add` | fd≥0、events≠0、cb≠NULL、bind 可 NULL | 0 | -EINVAL / -EEXIST / -ENOSPC / -ESHUTDOWN |
| `iomux_mod` | fd、events | 0 | -EINVAL / -ENOENT / -EBUSY / -ESHUTDOWN |
| `iomux_del` | fd | 0 | -EINVAL / -ENOENT / -ESHUTDOWN |
| `iomux_timer_add` | delay_ms、interval_ms、cb | id>0，err 出参 | 0 + err=-EINVAL/-ENOSPC |
| `iomux_timer_cancel` | id | 0 | -EINVAL / -ENOENT / -ESHUTDOWN |

不变量：同一 fd 最多一个 watcher；INFLIGHT 的 watcher 不在 epoll 兴趣集；队列满时事件进 pending 重试，不丢。

## 验收标准

| # | 给定 | 当 | 则 | 证据 |
|---|---|---|---|---|
| A1 | 100 连接 echo + 10 万条消息 | 运行 30s | 无丢失、无重复，`dispatched` 与注入数一致 | `test_basic` 输出 |
| A2 | 1,000 个 10ms 周期定时器 | 运行 10s | 触发次数 900–1100，P99 误差 <10ms | `test_timer` 输出 |
| A3 | 指定 hash 绑定 worker 1 | 30 次回调 | 全部线程 ID 相同 | `test_bind` 输出 |
| A4 | 8,000 连接、500k events/s | 压测 60s | P99 ≤500µs，RSS ≤64MB，无崩溃 | 压测日志 |
| A5 | 非法入参 8 类 | 逐个调用 | 返回码与契约一致，不崩溃 | `test_errors` 输出 |
| A6 | 运行中 stop | 2s 内 | reactor 退出、worker join、ASan 干净 | `test_shutdown` 输出 |

## 风险

| 风险 | 触发 | 影响 | 缓解 | 回退 |
|---|---|---|---|---|
| 内核不支持 EPOLLONESHOT | <2.6.2 | 无法抑制重复派发 | 启动时探测，fail fast | 退回 v1 方案 |
| fd 复用导致事件投错 | close 前未 del | 数据错发 | 文档强制 + 评审检查 | 加 fd 代际校验 |
| 队列满导致延迟抖动 | 突发流量 | P99 超标 | pending 重试 + 水位统计 | 增大 queue_depth |
| 无 Linux 工具链 | 当前环境 | 无法执行 A1–A6 | 在目标板跑 `make check` | 标记 blocked，不伪造结果 |
