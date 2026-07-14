# Tasks: taskgate M2 — Redis 后端、分布式限流与多进程能力

**Input**: Design documents from `/specs/002-m2-redis/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, quickstart.md(接口合同沿用 001 的 contracts/broker-contract.md,零改动)

**Tests**: L2(brokertest×miniredis+真 Redis 门控)、L1(分布式限流)、L3(集成加 redis 档)、专项(多进程/-short 跳过)、基准。

**Organization**: Phase 1 是能力接口地基(动根包,必须先行且零回归);Phase 2 起按故事推进,redisbroker 内部文件互不冲突处标 [P]。

## Format: `[ID] [P?] [Story] Description`

## Path Conventions

- 根包:limiter.go / broker.go / scheduler.go / integration_test.go / multiproc_test.go / bench_test.go
- 新后端:redisbroker/(broker.go、enqueue.go、dequeue.go、lifecycle.go、query.go、limiter.go、lua/*.lua、broker_test.go)

---

## Phase 1: Setup(能力接口,唯一的根包改动)

**Purpose**: QueueLimiter/LimiterProvider 定型 + scheduler 装配点,单机后端零回归。

- [x] T101 根包:broker.go 加 `LimiterProvider` 接口、limiter.go 抽 `QueueLimiter` 接口并把 `limiter` 改名 `localLimiter` 实现它(research 第 5 节签名);godoc 写明能力接口的语义与"上层不特判后端"的关系
- [x] T102 scheduler.go 装配点:构造限流器时先断言 `LimiterProvider`,无则 `newLocalLimiter`;**全量 M1 测试回归零失败**(`go test ./... -race -count=1`)

**Checkpoint**: 单机行为与 M1 完全一致,接口就绪。

---

## Phase 2: User Story 1 - Redis 后端过全部契约 (Priority: P1) 🎯 MVP

**Goal**: redisbroker 通过 brokertest 17 条(miniredis+真 Redis 两档)。

**Independent Test**: `go test ./redisbroker/... -race` 全绿;设 TASKGATE_REDIS_ADDR 后同样全绿。

- [x] T103 [US1] redisbroker 骨架:`redisbroker/broker.go`——Options{Addr,Password,DB,KeyPrefix}/New/Init/Close、键名工具、hash↔Task 编解码、Lua 加载(go:embed+redis.NewScript)、错误码映射(哨兵 vs 网络错误,research 第 9 节)、换代式同进程唤醒信号;新依赖 go-redis/v9 与 miniredis/v2
- [x] T104 [US1] `redisbroker/lua/enqueue.lua` + `redisbroker/enqueue.go`:查重/父校验/初始状态判定/落 hash/入 pending 或 delayed 或直接 canceled(含传播复用 finish 的收敛逻辑,可先内联)/索引/计数/types;ID 由 Go 生成注入,失败不回填
- [x] T105 [US1] `redisbroker/lua/claim.lua` + `redisbroker/dequeue.go`:搬到期 delayed→pending → LPOP 校验循环 → 写租约/令牌/inflight/计数 → 返回全字段;Dequeue Go 循环挂 clock/唤醒信号/ctx 三源,ctx 取消统一返回 ctx.Err();此步过 brokertest 契约 1~5(RoundTrip/IdempotentID/ClaimMutex/BlockingDequeue/DelayedTask)
- [x] T106 [US1] `redisbroker/lua/finish.lua`(op=ack/fail/finish_canceled/cancel/requeue 共用:令牌+canTransition 校验、整棵子树工作队列收敛、计数/索引/inflight/delayed 维护、返回流转快照)+ `redisbroker/lua/heartbeat.lua` + `redisbroker/lua/reap.lua`(含 cancel_requested→canceled、LeaseLost 封顶、blocked 防御修复)+ `redisbroker/lifecycle.go`、`redisbroker/query.go`(Get/List/QueueLen/Counts,List 走索引 set);Notify 在 Lua 返回快照后由 Go 异步发(recover 包住)
- [x] T107 [US1] `redisbroker/broker_test.go`:miniredis 档一行接入 brokertest,**17 条全绿**;真 Redis 档(TASKGATE_REDIS_ADDR 门控,随机 KeyPrefix 隔离+测后清理)同一 factory;补 TestUseBeforeInit(与另两后端对齐)

**Checkpoint**: MVP——第三个后端契约全绿,`go test ./... -race` 全量绿。

---

## Phase 3: User Story 2 - 多进程恰好执行一次 (Priority: P1)

**Goal**: 双进程不丢不重;kill -9 回收;断连恢复。

- [ ] T108 [US2] `multiproc_test.go`(根包,-short 跳过):miniredis 起在父进程,子进程 exec 自身二进制(沿用 sqlitebroker/crash_test.go 哨兵模式)——① 双进程灌 1000 任务,handler 往 Redis 写执行记录,断言恰好 1000 条无重复;② 子进程认领后 kill -9,存活进程经 reaper 回收重跑,LeaseLost=1 最终 completed
- [ ] T109 [US2] multiproc_test.go 增加断连恢复:miniredis 重启(Close/Restart 或新起同地址),worker 报错后重连继续消费,任务最终全 completed 不丢失

**Checkpoint**: SC-002 成立。

---

## Phase 4: User Story 3 - 分布式限流 (Priority: P2)

**Goal**: 多进程共享 Workers/RPS 配额;崩溃槽自动回收。

- [ ] T110 [US3] `redisbroker/lua/sem_acquire.lua` + `redisbroker/limiter.go`:zset 信号量(清过期→ZCARD<limit→ZADD;续期 goroutine LeaseTTL/3;Release=ZREM+停续期)+ redis_rate GCRA 的 WaitToken(被拒按 RetryAfter 挂 clock 等待;RPS=0 直通);实现 taskgate.QueueLimiter;Broker 实现 LimiterProvider;新依赖 redis_rate/v10
- [ ] T111 [US3] `redisbroker/limiter_test.go` L1:同一 miniredis 上两个限流器实例 {Workers:2} 全局并发 ≤2;槽过期自动回收(fakeclock 推进);{RPS:10} 1 秒全局 10±2(真时钟短窗口);RPS=0 直通
- [ ] T112 [US3] integration_test.go:后端参数化加 redis(miniredis)档跑全部既有 L3 场景;multiproc_test.go 增加双进程 {Workers:2} 全局并发 ≤2 的观测断言(handler 上报水位)

**Checkpoint**: SC-003 成立;单机后端零回归。

---

## Phase 5: User Story 4 - 跨进程流水线与取消 (Priority: P2)

- [ ] T113 [US4] multiproc_test.go 增加:① 进程 A 只消费 ocr 队列、进程 B 只消费 extract 队列,依赖链跨进程唤醒全 completed;② 任务在子进程 running,父进程发 Cancel,断言一个心跳周期(LeaseTTL/3+余量)内落 canceled 且传播生效

**Checkpoint**: SC-004 成立。

---

## Phase 6: User Story 5 - Stats O(1) 与基准 (Priority: P3)

- [ ] T114 [US5] 验证 Counts/QueueLen 实现为计数器/长度读取(代码审视+契约 13 已盖行为);README 补 redis 键名速查(运维 LLEN 直查,FR-012)
- [ ] T115 [US5] `bench_test.go`:BenchmarkEnqueue(单/32 并发×sqlite/redis)、BenchmarkDequeueAck(W=8,RPS=0)、BenchmarkPipeline(三级链);跑一轮把 ns/op 回写 docs/plans/2026-07-14-测试方案.md 第 6 节(注明 redis=miniredis 含其开销)

---

## Phase 7: Polish & Cross-Cutting

- [ ] T116 [P] 导出符号 godoc 补全(redisbroker 全包+根包新接口);README 更新:Redis 后端快速开始、分布式限流说明、跨进程延迟(Dequeue≤100ms/Cancel≤心跳周期)与已知限制
- [ ] T117 运行 quickstart.md 验收检查点全表(含覆盖率 redisbroker ≥80%、连跑 3 遍、gofmt/vet);宪法取证 grep
- [ ] T118 完成记录写 docs/plans/2026-07-14-M2完成记录.md(交付/验收/裁决/M3 待办)——git 提交由主控做

---

## Dependencies & Execution Order

- Phase 1 → 2 → 3 → 4 → 5 → 6 → 7 严格串行(2 之后各阶段都依赖 redisbroker 存在;multiproc_test.go 多阶段追加不可并行)
- Phase 2 内 T103→T104→T105→T106→T107 串行(同包层层叠);Phase 6 的 T114/T115 可并行于 Phase 5 之后

### Parallel Opportunities

```
Phase 6~7: T114, T115, T116 互不同文件可并行
```

---

## Implementation Strategy

1. Phase 1 必须零回归再动 redisbroker
2. Phase 2 完成即 MVP(第三后端契约全绿),STOP and VALIDATE
3. 之后每 Phase 结束 `go test ./... -race` 回归 + git 提交

## Task Summary

| 阶段 | 任务数 | 可并行 |
|------|--------|--------|
| P1 Setup | 2 | 0 |
| P2 US1(MVP) | 5 | 0 |
| P3~P6(US2~5) | 8 | 2 |
| P7 Polish | 3 | 1 |
| **Total** | **18** | **3** |

**MVP Scope**: Phase 1 + Phase 2 = 7 tasks
