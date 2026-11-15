# iomux

嵌入式数通平台用的多路分发库：epoll 水平触发 + 单 Reactor + 可配置 worker 池。
设计决策见 `.dsh/knowledge/decisions/dec-2026-11-15-iomux-design-choice.md`。

## 架构

```
reactor 线程
  epoll_wait(LT) → 选 worker → SPSC 入队 → eventfd 唤醒
  定时器最小堆 → 到点派发（周期到期若上次回调未完则丢弃该 tick）
  完成环出队 → EPOLL_CTL_MOD 重新武装（EPOLLONESHOT）

worker 线程 × nworkers
  出队 → 执行回调 → 写完成环 → eventfd 唤醒 reactor
```

- **LT + EPOLLONESHOT**：派发时该 fd 自动从兴趣集摘除，回调完成后由 reactor 用
  `EPOLL_CTL_MOD` 重挂。数据没读完时重挂后会立即再触发，语义仍是水平触发。
- **单 Reactor**：epoll fd、fd 表、定时器堆只由 reactor 线程访问。
- **每 worker 独立队列**：reactor 是唯一生产者，SPSC 无锁。
- **公平调度**：默认轮询；注册时可挂 `iomux_hash_fn` 绑定到固定 worker。
- **跨线程注册**：命令队列 + eventfd，API 返回真实错误码；在 `iomux_run` 之前
  调用则直接在本线程执行。

## 类型约定

- 结构体不 typedef：`struct iomux_cfg`、`struct iomux_poller *`。
- 函数指针后缀 `_cb` / `_fn`，不用 `_t`。
- 错误码：`0` 成功，负 `errno` 失败。

## 构建

```sh
make            # libiomux.so + libiomux.a
make check      # 4 个测试
```

## 最小用法

```c
#include "iomux.h"
#include <sys/epoll.h>

static void on_readable(int fd, uint32_t events, void *arg)
{
	char buf[256];
	ssize_t n = read(fd, buf, sizeof(buf));
	(void)events; (void)arg;
	if (n <= 0)
		return;
	/* ... */
}

int main(void)
{
	struct iomux_cfg cfg;
	struct iomux_poller *p;

	iomux_cfg_default(&cfg);
	cfg.nworkers = 4;

	p = iomux_new(&cfg);
	if (p == NULL)
		return 1;
	if (iomux_add(p, fd, EPOLLIN, on_readable, NULL, NULL) != 0)
		return 1;
	iomux_run(p);          /* 阻塞；另一线程 iomux_stop() */
	iomux_free(p);
	return 0;
}
```

## 线程与生命周期

| 调用 | 线程 | 说明 |
|---|---|---|
| `iomux_new` | 任意 | 创建 epoll、eventfd、worker 线程 |
| `iomux_run` | 任意（独占） | 阻塞跑 reactor；返回后不能再 run |
| `iomux_stop` | 任意 | 置停止位并唤醒 reactor |
| `iomux_free` | 非 reactor/worker | 先 stop，等 reactor 退出，再 join |
| `iomux_add/mod/del` | 任意 | 跨线程走命令队列；回调内可用 |
| `iomux_timer_*` | 任意 | 同上 |

`iomux_free` 从 reactor 或 worker 线程调用会被拒绝（只记日志），避免自 join 死锁。

## 定时器语义

- `interval_ms == 0`：一次性，触发后回调执行完即释放。
- `interval_ms > 0`：周期；到点先重挂下一次，再派发。若上一次回调还在执行，
  **丢弃该 tick**（`timer_skipped++`），不排队、不补发。
- 一次性定时器一旦触发即不可取消（`iomux_timer_cancel` 返回 `-ENOENT`）。

## 防呆

- `NULL` / `fd < 0` / `events == 0` / `cb == NULL` → `-EINVAL`
- 重复注册 → `-EEXIST`；未注册就删 → `-ENOENT`
- 回调在飞时 `iomux_mod` → `-EBUSY`
- 队列满 → 派发进入 pending 列表，下轮重试，不丢事件
- `epoll_wait` 被信号打断 → 继续
- 重新武装失败（fd 已被外部 close）→ 记日志并回收 watcher
- 运行结束后的 API 调用 → `-ESHUTDOWN`

## 统计

`iomux_stats()` 返回只读快照指针：`dispatched / rearmed / queue_full /
timer_fired / timer_skipped / epoll_events / epoll_batches / errors`。

## 限制

- 仅 Linux；依赖 epoll / eventfd / EPOLLONESHOT（内核 2.6.x+）。
- 单实例单 reactor；多核扩展靠多个 poller 实例 + `SO_REUSEPORT`。
- **close 前必须先 `iomux_del`**：fd 号被复用后，watcher 会指向新的 fd，事件将投错对象；库无法识别这种复用。
- `iomux_mod` 对 in-flight 的 fd 返回 `-EBUSY`（回调结束后再改）。
- 未做本平台实测：`make check` 需要在目标环境跑，体积/RSS/延迟数据待补。
