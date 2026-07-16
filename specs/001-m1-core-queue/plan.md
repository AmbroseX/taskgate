# Implementation Plan: taskgate M1 — 核心排队、限流、重试与依赖

**Branch**: `001-m1-core-queue` | **Date**: 2026-07-14 | **Spec**: [spec.md](./spec.md)

## Summary

从零实现 taskgate 库的 M1:先定型公共类型与 Broker 接口,再写 brokertest 合规套件,然后按 memory → sqlite 顺序实现后端,最后实现 limiter、scheduler(worker 池+续租+reaper+重试编排+Shutdown)与 client 门面,配三级流水线示例。全程测试先行:契约进 brokertest,纯逻辑进 L1,链路进 L3,专项(崩溃恢复/唤醒中途崩)单列。

## Technical Context

**Language/Version**: Go 1.25(本机 go1.25.0,go.mod 尚未初始化,`go mod init github.com/AmbroseX/taskgate`)
**Primary Dependencies**: modernc.org/sqlite(纯 Go)、golang.org/x/time/rate、github.com/oklog/ulid/v2;仅此三个,M1 不引 redis
**Affected Backends**: memory + sqlite(M1 全部);Broker 接口一次定型,M2 的 redis 不改签名
**Testing**: L1(limiter/退避/校验/状态机/依赖计数)+ L2(brokertest,memory/sqlite 双后端)+ L3(integration_test.go 全场景)+ 专项(kill -9 崩溃恢复、唤醒中途崩)
**Concurrency Semantics**: 认领互斥(sqlite 事务内子查询 UPDATE;memory 加锁)、终态+唤醒同事务、租约令牌校验、自动续租 LeaseTTL/3、reaper 定期回收
**Performance Goals**: 无新增性能指标;基准留 M2
**Constraints**: 零 cgo;时间全部走可注入 clock;Payload/Result 只用 json.RawMessage;库不读 env/配置文件

## Constitution Check

| 原则 | 本计划遵循方式 | 结果 |
|------|---------------|------|
| I 库的边界 | 只做 M1 清单,不碰第 10 节禁区;仅 3 个轻量依赖 | ✅ |
| II Broker 最小公倍数 | 接口 14 个方法一次定型,全部是 redis 也能同语义实现的;时间窗口统计不进接口 | ✅ |
| II brokertest 合规 | 先写 brokertest 14 条契约,memory/sqlite 后实现、同套跑 | ✅ |
| III 终态+唤醒原子性 | sqlite 单事务;memory 单锁临界区;连锁取消链式一层一层 | ✅ |
| III 三计数分工 | Attempts/LeaseLost/Throttled 各自封顶;Requeue 不占 | ✅ |
| IV 数据模型 | json.RawMessage、ulid、导出哨兵错误 | ✅ |
| V 测试纪律 | 全部离线、注入 clock、-race、覆盖率 85%/80% | ✅ |
| VI 文档纪律 | spec-kit 产物在 specs/001-m1-core-queue/,完成后归档 docs/plans/ | ✅ |

**Constitution Check Result**: ✅ 通过

## Project Structure

### Documentation (this feature)

```text
specs/001-m1-core-queue/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
└── contracts/broker-contract.md
```

### Source Code (repository root)

```text
taskgate/
├── go.mod                    # module github.com/AmbroseX/taskgate
├── taskgate.go               # Task/Status/哨兵错误/提交选项/Config/QueueConfig/Duration
├── errors.go                 # ErrTaskExists/ErrLeaseLost/ErrThrottled/ErrSkipRetry/ErrTaskNotFound...
├── broker.go                 # Broker 接口 + Filter
├── clock.go                  # Clock 接口(Now/After/NewTimer)+ 真实现;测试用 fakeclock
├── limiter.go                # 每队列 {Workers信号量 + x/time/rate 令牌桶}
├── backoff.go                # 指数退避+抖动(注入 rand 源)
├── deps.go                   # 依赖唤醒/连锁取消的公共纯逻辑(供后端复用)
├── scheduler.go              # worker 池+认领循环+自动续租+reaper+重试编排+Shutdown
├── client.go                 # New/Handle/Run/Submit/Get/Cancel/List/Stats/Overview/Wait/Shutdown
├── memorybroker/broker.go    # 参考实现(单锁+条件变量)
├── sqlitebroker/broker.go    # modernc.org/sqlite,WAL,事务认领
├── sqlitebroker/schema.sql   # DDL(嵌入)
├── brokertest/suite.go       # 14 条契约,Run(t, factory)
├── internal/fakeclock/       # 测试时钟
├── *_test.go                 # L1 跟随源码;integration_test.go = L3
├── crash_test.go             # 专项:子进程 kill -9 / 唤醒中途崩(sqlite)
└── examples/llm/main.go      # 三级流水线示例
```

**Structure Decision**: 与设计方案第 12 节一致;deps.go 只放"计算哪些子任务该唤醒/取消"的纯函数,原子写入仍在各后端事务内完成,避免上层拆两步。

## Complexity Tracking

无(零新依赖超出宪法清单;无宪法违规项)。
