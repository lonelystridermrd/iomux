# 测试设计 · iomux_v2

> 阶段：tests ｜ 覆盖 `spec.md` A1–A6 ｜ 每条含判定方式

| 用例 | 对应验收 | 步骤 | 通过判定 | 环境 |
|---|---|---|---|---|
| TC1 | A1 | 100 连接 echo，注入 100k 条消息，运行 30s | 收发计数相等；`iomux_stats()->dispatched` 与注入数一致；无重复 | Linux x86_64 |
| TC2 | A2 | 注册 1000 个 10ms 周期定时器，运行 10s | 触发 900–1100 次；相邻触发间隔 P99 误差 <10ms | Linux |
| TC3 | A3 | 同一 fd 注册 `hash_to_one`，触发 30 次 | 30 次回调的 `pthread_self()` 全相同 | Linux |
| TC4 | A4 | 8000 fd + 500k events/s 压测 60s | P99 ≤500µs；RSS ≤64MB；无崩溃；`queue_full` <5% | 目标板 |
| TC5 | A5 | 8 类非法入参逐个调用 | 返回码符合契约；进程不崩溃 | Linux |
| TC6 | A6 | 运行中 `iomux_stop`，2s 内退出 | reactor 退出、worker join；ASan 无泄漏 | Linux + ASan |
| TC7 | P1-1 | 注册 fd 后 close、再 open 复用同号 | 库检测到并记录错误，不向新 fd 投递事件 | Linux（v2.1） |
| TC8 | P2-2 | 队列满 + 慢回调 10s | `queue_full` 增长但无丢事件；回调完成后恢复 | Linux |

## 判定方式

- TC1/TC5/TC6：测试程序退出码 0 + 断言输出。
- TC2：测试内计算 P99 并断言。
- TC3：测试内缓存首个线程 ID 并比对。
- TC4：外部压测脚本输出 P99/RSS，人读判定。
- TC7/TC8：v2.1 补，当前标 `待实现`。

## 执行命令

```sh
cd iomux_v2 && make check
```
