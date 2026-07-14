# Implementation Plan: taskgate M2 — Redis 后端、分布式限流与多进程能力

**Branch**: `002-m2-redis` | **Date**: 2026-07-14 | **Spec**: [spec.md](./spec.md)

## Summary

新增 redisbroker(全部流转走服务端 Lua,时间由 Go 侧注入保 fakeclock 可用),通过既有 brokertest 17 条契约(miniredis 进 CI + 真 Redis env 门控);认领改为**单段 Lua 原子完成**(消灭方案 v5 的"BLMOVE 两步"崩溃窗口);分布式限流经新增的 QueueLimiter 能力接口接入 scheduler(Workers=带过期的计数信号量,RPS=GCRA),单机后端零回归;补跨进程专项与基准基线。

## Technical Context

**Language/Version**: Go 1.25
**Primary Dependencies**: 新增 github.com/redis/go-redis/v9、github.com/go-redis/redis_rate/v10、github.com/alicebob/miniredis/v2(仅测试);既有依赖不变
**Affected Backends**: 新增 redisbroker;Broker 接口零改动;memory/sqlite 零改动;scheduler 加限流器装配点(行为兼容)
**Testing**: L2(brokertest×miniredis / ×真 Redis 门控)+ L1(分布式限流器)+ L3(集成测试加 redis 档)+ 专项(双进程恰好一次、kill -9、跨进程流水线/取消、断连恢复)+ 基准(测试方案第 6 节)
**Concurrency Semantics**: 认领互斥与终态+唤醒原子性由单段 Lua 保证(Redis 单线程执行脚本);租约令牌语义不变;分布式并发槽带过期自动回收
**Performance Goals**: 无硬指标;基准建基线入档,后续 ±20% 防退化
**Constraints**: Broker 签名禁改;brokertest 契约语义禁改;上层禁止对具体后端特判(能力接口除外,见 research 第 5 节);Lua 内禁用 TIME/随机(时间全由 ARGV 注入)

## Constitution Check

| 原则 | 本计划遵循方式 | 结果 |
|------|---------------|------|
| I 库的边界 | 仅加 redis 后端与限流实现,不碰第 10 节禁区;新依赖 3 个全在宪法技术栈表内 | ✅ |
| II.1 接口最小公倍数 | Broker 接口零改动 | ✅ |
| II.2 上层不特判后端 | scheduler 只断言 taskgate.LimiterProvider **能力接口**,不 import 也不断言具体后端类型(research 第 5 节论证) | ✅ |
| II.3 brokertest 合规 | redisbroker 作为第三个 factory 接入,17 条契约一字不改全过 | ✅ |
| II.4 租约令牌 | Lua 内逐一校验令牌,语义与 M1 合同一致 | ✅ |
| III.1 终态+唤醒原子性 | 同一段 Lua 收敛(宪法 v1.1.0 认可的单调用收敛形态) | ✅ |
| III.3 三计数分工 | Lua 按 FailKind/Reap/Requeue 维护,语义照 broker-contract.md | ✅ |
| IV 数据模型 | 无类型变更;新导出符号仅 QueueLimiter/LimiterProvider 与 redisbroker.New | ✅ |
| V 测试纪律 | miniredis 离线可跑;时间 ARGV 注入使 fakeclock 有效;真 Redis/双进程档 env 门控或 -short 跳过;覆盖率 redisbroker ≥80% | ✅ |
| VI 文档纪律 | spec-kit 产物在 specs/002-m2-redis/,完成后归档 docs/plans/ | ✅ |

**Constitution Check Result**: ✅ 通过

## Project Structure

### Documentation (this feature)

```text
specs/002-m2-redis/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
└── quickstart.md        # contracts/ 不需要:接口零改动,沿用 001 的 broker-contract.md
```

### Source Code (repository root)

```text
taskgate/
├── limiter.go                 # 改:抽出 QueueLimiter 接口,现有实现改名 localLimiter 实现之
├── broker.go                  # 改:新增 LimiterProvider 能力接口(仅接口定义,非 Broker 方法)
├── scheduler.go               # 改:限流器装配点(Broker 实现 LimiterProvider 则用之,否则 local)
├── redisbroker/
│   ├── broker.go              # New(addr, password, db) / Options 构造 + Init/Close + 公共件
│   ├── lua/*.lua              # go:embed:enqueue/claim/finish/heartbeat/requeue/reap
│   ├── enqueue.go dequeue.go lifecycle.go query.go   # 按 sqlitebroker 的文件划分惯例
│   ├── limiter.go             # QueueLimiter 实现:zset 信号量 + redis_rate GCRA
│   └── broker_test.go         # miniredis 档 + TASKGATE_REDIS_ADDR 真 Redis 档接入 brokertest
├── integration_test.go        # 改:后端参数化加 redis(miniredis)档
├── multiproc_test.go          # 新:双进程恰好一次/kill -9/跨进程流水线与取消/断连恢复(-short 跳过)
├── bench_test.go              # 新:Enqueue/DequeueAck/Pipeline 基准(sqlite vs redis)
└── docs/plans/2026-07-14-测试方案.md  # 改:第 6 节回写基线数字
```

**Structure Decision**: redisbroker 沿用 sqlitebroker 的文件划分,Lua 独立成 .lua 文件 go:embed(可读、可在 redis-cli 调试);限流能力接口放根包,实现放 redisbroker(依赖方向:redisbroker → taskgate,与现有一致)。

## Complexity Tracking

| 超标项 | 原因 | 补救措施 |
|--------|------|----------|
| 与方案 v5 §7.2 的偏差:认领不用"BLMOVE 中转 list+Lua 挪 zset"两步,改单段 Lua 原子认领+clock 驱动轮询 | 两步方案存在崩溃窗口,要靠 reaper 扫中转区兜底;单段 Lua 直接消灭窗口,且轮询等待才能挂在注入 clock 上(fakeclock/契约套件的前提) | research 第 1 节完整论证;跨进程唤醒延迟 ≤100ms 与 sqlite 后端同级,写进文档;M3 若要更低延迟再补 BLMOVE 快路径 |
