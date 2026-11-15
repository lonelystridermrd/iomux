# 调研报告：iomux_v2 多路分发方案

> 任务：iomux-v2 ｜ 阶段：research ｜ 产出技能：tech-evaluation
> 结论仅作决策材料，不含最终选型。

## 1. 约束

- Linux 用户态，内核假设 ≥4.19（4.14/4.19/5.4/5.10 LTS 均需可用）
- 单实例 8,000 fd、500k events/s、派发 P99 ≤500µs、RSS ≤64MB

## 2. 候选（广度，11 个）

| # | 候选 | 定位 | 维护状态 | 许可 |
|---|---|---|---|---|
| 1 | 自研 epoll reactor | 薄封装、零依赖、可精确控制 | 自己维护 | 自有 |
| 2 | libevent | 通用事件通知 + 缓冲 IO | 2.1.13-stable（2026-07） | BSD-3 |
| 3 | libuv | 跨平台异步 IO（Node 底层） | 活跃 | MIT |
| 4 | libev | 极简事件循环 | 更新慢 | BSD-2/GPL |
| 5 | io_uring + liburing | 共享 SQ/CQ、批量提交 | 内核主线 | MIT |
| 6 | nginx 事件核心 | ET + 连接池 + accept 分离 | 活跃 | BSD-2 |
| 7 | FRR thread 调度器 | 数通控制面实践 | 活跃 | GPL-2.0 |
| 8 | DPDK | 轮询模式数据面 | 活跃 | BSD-3 |
| 9 | VPP | 用户态协议栈 | 活跃 | Apache-2.0 |
| 10 | AF_XDP | 内核旁路通道 | 4.18+ | GPL-2.0/MIT |
| 11 | 维持现状 | 现有自研实现 | — | — |

## 3. 证据表

| # | 结论 | 等级 | 来源 |
|---|---|---|---|
| 1 | libevent 2.1.13-stable / BSD-3 | A | https://libevent.org/ |
| 2 | epoll 兴趣表 + 就绪链表；每 fd 约 90B/160B；ET 必须读到 EAGAIN | A | https://man7.org/linux/man-pages/man7/epoll.7.html |
| 3 | EPOLLEXCLUSIVE 4.5 引入，仅 ADD、one-or-more | A | https://man7.org/linux/man-pages/man2/epoll_ctl.2.html |
| 4 | io_uring 共享 SQ/CQ、SQPOLL、单 socket 串行 send/recv | A | https://man7.org/linux/man-pages/man7/io_uring.7.html |
| 5 | nginx 默认 ET、1.11.3 起 accept_mutex 默认 off | A | https://nginx.org/en/docs/ngx_core_module.html |
| 6 | libuv 单线程 loop、epoll 后端、fs/DNS 走线程池 | A | https://docs.libuv.org/en/v1.x/design.html |
| 7 | 各方案产物体积/RSS | 未实测 | 需目标板自测 |

## 4. 版本能力矩阵

| 内核 | EPOLLEXCLUSIVE | io_uring | AF_XDP |
|---|---|---|---|
| 4.14 LTS | ✅ | ❌ | ❌ |
| 4.19 LTS | ✅ | ❌ | ✅ |
| 5.4 LTS | ✅ | 基础 | ✅ |
| 5.10/5.15 | ✅ | 较全 | ✅ |

## 5. 结论（材料）

- 在 4.19 LTS 上 io_uring 不可用；EPOLLONESHOT 与 eventfd 均可用。
- 成熟库的抽象层与"每 fd 绑线程 + 回调进池"的模型不匹配，包装后仍付其事件模型成本。
- 体积/RSS 未实测，任何数值结论都不可采信。

> 仲裁点留给阶段 3（tech-selection-decision）。
