# Tasks: taskgate M1 — 核心排队、限流、重试与依赖

**Input**: Design documents from `/specs/001-m1-core-queue/`
**Prerequisites**: plan.md, spec.md, data-model.md, quickstart.md, contracts/broker-contract.md

**Tests**: L1(单元,跟随源码)+ L2(brokertest,memory/sqlite 双后端)+ L3(integration_test.go)+ 专项(crash_test.go);L4 mock 网关留 M2 前置。

**Organization**: Phase 1/2 是公共地基(类型、接口、brokertest、双后端——后端行为被 16 条契约整体约束,放 Foundational 一次做对);Phase 3 起按用户故事接线 scheduler/client 并补 L1/L3。

## Format: `[ID] [P?] [Story] Description`

## Path Conventions

- 核心包:仓库根(taskgate.go / errors.go / clock.go / broker.go / limiter.go / backoff.go / deps.go / scheduler.go / client.go)
- 后端:memorybroker/、sqlitebroker/;合规套件:brokertest/
- 测试:`*_test.go` 跟随源码(L1)、integration_test.go(L3)、crash_test.go(专项)

---

## Phase 1: Setup(类型与接口地基)

**Purpose**: 全部公共类型一次定型,后续任务只消费不改动。

- [X] T001 初始化 `go.mod`(module github.com/ambrose/taskgate,go 1.25)并写 `taskgate.go`:Task/Status 七态/canTransition 表/ParentFailurePolicy/Config/QueueConfig/Duration(UnmarshalText)/SubmitOption(WithID/Delay/RunAt/MaxRetry/DependsOn/IgnoreParentFailure)/Handler 类型,字段与默认值照 data-model.md 第 1 节
- [X] T002 [P] 写 `errors.go`:ErrTaskExists/ErrTaskNotFound/ErrLeaseLost/ErrTaskCanceled/ErrAlreadyFinal/ErrShutdown 哨兵 + ErrThrottled{RetryAfter}/ErrSkipRetry{Err} 错误类型(实现 error/Unwrap)
- [X] T003 [P] 写 `clock.go`(Clock 接口:Now/After/Sleep(ctx)/NewTicker,realClock 实现)与 `internal/fakeclock/fakeclock.go`(手动推进、唤醒 waiter,并发安全)
- [X] T004 写 `broker.go`:Broker 接口 + BrokerOptions + FailKind + Filter,签名一字不差照 contracts/broker-contract.md
- [X] T005 [P] 写 `taskgate_test.go`:L1 状态机表驱动全枚举(canTransition)、选项组合合法性、Config 校验规则单测(校验函数本体在 T001 的 validate 中)、Duration 解析

**Checkpoint**: `go build ./...` 通过;L1 类型层测试绿。

---

## Phase 2: Foundational(brokertest + 双后端 + 依赖纯逻辑)

**Purpose**: 契约先行,双后端一次实现全部 16 条行为契约(涵盖 US1~US6 的存储侧语义)。

- [ ] T006 写 `deps.go`:依赖决策纯函数(计算唤醒/传播动作、pending_parents 递减不为负、提交时父已终态的初始状态判定)+ `deps_test.go` L1 覆盖
- [ ] T007 写 `brokertest/suite.go`:`Run(t, factory)` + contracts 清单 16 条契约用例(RoundTrip/IdempotentID/ClaimMutex/BlockingDequeue/DelayedTask/AckFail/LeaseReap/StaleToken/RetryingReclaim/DepWake/CascadeCancel/CancelStates/CountsConsistency/ListFilter/RequeueNoCount/IllegalTransition),统一注入 fakeclock;此阶段只需编译通过
- [ ] T008 写 `memorybroker/broker.go`(单 Mutex+Cond,语义参考实现)+ `memorybroker/broker_test.go` 一行接入 brokertest,**16 条全绿**
- [ ] T009 写 `sqlitebroker/broker.go` + `sqlitebroker/schema.sql`(go:embed,DDL 照 data-model.md 第 3 节,WAL+busy_timeout,BEGIN IMMEDIATE 子查询认领,终态+唤醒同事务)+ `sqlitebroker/broker_test.go` 接入 brokertest,**16 条全绿**(依赖 T008 先定语义基准,但文件独立可并行开工)

**Checkpoint**: `go test ./brokertest/... ./memorybroker/... ./sqlitebroker/... -race` 全绿——存储层全部行为契约成立,这是整个 M1 的地基。

---

## Phase 3: User Story 1 - 提交任务并追踪状态与结果 (Priority: P1) 🎯 MVP

**Goal**: Submit→执行→Get/Wait 拿结果;List/Stats/Overview 可查。

**Independent Test**: memory 后端 Submit→Wait 拿 Result;时间戳链完整;同 ID 二次提交 ErrTaskExists。

- [ ] T010 [US1] 写 `client.go`:Gate 结构 + New(cfg 校验+Broker.Init 装配)+ Handle + Submit(Routes 路由,选项应用)+ Get/List/Stats/Overview/Wait(50ms 轮询走 clock)
- [ ] T011 [US1] 写 `scheduler.go` 最小版:每队列 worker 池(仅并发槽,限流下一 Story)、Dequeue→handler→Ack/Fail(先只处理成功路径与 FailBusiness)、Run(ctx) 生命周期
- [ ] T012 [US1] `integration_test.go` L3 场景:提交→执行→取结果(时间戳链)、Wait 超时(handler 睡 1s/Wait 500ms)、统计一致(Overview/Stats/List 对得上)、幂等 ID

**Checkpoint**: MVP 可用——memory/sqlite 双后端上 Submit→completed 全链路绿。

---

## Phase 4: User Story 2 - 类型隔离限流与队列路由 (Priority: P1)

**Goal**: 每队列 {Workers,RPS,Burst} 独立生效;Routes 共享队列;纯生产者进队列正确。

**Independent Test**: RPS=10 → 1s 放行 10±1;慢队列不拖快队列;纯生产者(不 Run)提交进对队列。

- [ ] T013 [US2] 写 `limiter.go`:每队列信号量(带缓冲 channel)+ x/time/rate 令牌桶(RPS=0 不限速;Burst 缺省 max(1,int(RPS)));`limiter_test.go` L1:RPS 精度 10±1、Burst 生效、Workers=2 第 3 个等槽
- [ ] T014 [US2] scheduler.go 接入 limiter:认领前"先拿并发槽再等令牌"顺序写死;handler 结束归还槽
- [ ] T015 [US2] integration_test.go 增加:限流隔离(两队列 {W:1,RPS:1} vs {W:8})、Routes 路由(纯生产者 Gate 不 Run 提交 review→xunfei 队列,消费者 Gate 认领执行)

**Checkpoint**: SC-001(RPS 10±1、并发≤Workers)在 L1+L3 都有断言。

---

## Phase 5: User Story 3 - 失败重试、死信与限流特化 (Priority: P1)

**Goal**: 指数退避重试、MaxRetry 死信、ErrThrottled 不占 Attempts、ErrSkipRetry 直接 failed。

**Independent Test**: 前 2 次失败第 3 次成功 → Attempts=3 最终 completed;ErrThrottled{1s}×3 → Attempts 不涨 RunAt 递推。

- [ ] T016 [US3] 写 `backoff.go`:`min(2^n×1s,10min)±20%`(注入 rand 源)+ `backoff_test.go` L1:曲线与抖动范围断言
- [ ] T017 [US3] scheduler.go 重试编排:handler 错误分类(errors.As ErrThrottled→FailThrottled+RetryAfter;ErrSkipRetry→FailSkip;其余→FailBusiness+退避),调 Broker.Fail
- [ ] T018 [US3] integration_test.go 增加:重试链路(间隔递增)、ErrThrottled 链路、ErrSkipRetry、Throttled 封顶进 failed(fakeclock 或调小上限)

**Checkpoint**: P1 三个故事全绿 = 核心价值可交付。

---

## Phase 6: User Story 4 - 租约、reaper 与自动续租 (Priority: P2)

**Goal**: 崩溃回收、慢任务不误杀、旧令牌被拒。

**Independent Test**: 慢任务(3×LeaseTTL)零误回收;kill -9 后 LeaseLost=1 最终 completed。

- [ ] T019 [US4] scheduler.go:每在跑任务心跳 goroutine(LeaseTTL/3 调 Heartbeat,收 ErrTaskCanceled 则 cancel 任务 ctx;收 ErrLeaseLost 则放弃结果)+ 全局 reaper goroutine(周期 min(各队列 LeaseTTL)/2 调 ReapExpired)
- [ ] T020 [US4] integration_test.go 增加:毒任务 LeaseLost 封顶进 failed、慢任务自动续租不被误回收(fakeclock 推进)
- [ ] T021 [US4] 写 `crash_test.go` 专项(`-short` 跳过):子进程模式跑 sqlite worker 处理中 kill -9 → 主进程重开同一 db,reaper 回收,LeaseLost=1,重跑 completed;"唤醒中途崩"(sqlitebroker 测试注入点:父 Ack 事务提交前 panic)→ 重启后子正常唤醒不丢

**Checkpoint**: SC-004 全部成立。

---

## Phase 7: User Story 5 - 任务依赖与流水线 (Priority: P2)

**Goal**: DependsOn 声明、原子唤醒、FailFast/IgnoreParentFailure、链式传播(存储侧已在 Phase 2 达成,本阶段接线+全链路验证)。

**Independent Test**: A→B→C 流水线;fan-in;A 失败连锁取消。

- [ ] T022 [US5] client.go Submit 接线 DependsOn/IgnoreParentFailure 选项(Enqueue 侧校验已在后端);Get 返回 DependsOn 供 handler 取父结果
- [ ] T023 [US5] integration_test.go 增加:三级流水线(C handler 里 Get 父 Result)、fan-in(两父都完成才跑;IgnoreParentFailure 时父失败照跑)、提交时父已完成直接 pending、A 失败 B/C 连锁 canceled、DependsOn 不存在的父 → 拒收

**Checkpoint**: SC-005 流水线语义全链路成立。

---

## Phase 8: User Story 6 - 取消 (Priority: P2)

**Goal**: 各状态 Cancel 语义 + running 的 ctx 取消 + FinishCanceled 落库 + 传播。

- [ ] T024 [US6] scheduler.go/client.go:Cancel 接线——本进程 running 任务保存 cancel func 即时 cancel;handler 退出后调 FinishCanceled;Wait 对 canceled 终态即时返回
- [ ] T025 [US6] integration_test.go 增加:取消 running(handler ctx 被 cancel、终态 canceled)、取消 blocked 向下传播、终态 Cancel 报错

**Checkpoint**: US6 验收场景全绿。

---

## Phase 9: User Story 7 - Shutdown 优雅停止 (Priority: P3)

- [ ] T026 [US7] scheduler.go/client.go:Shutdown(ctx)——停认领与限流放行 → 等 worker → 超时 cancel 各任务 ctx → handler 退出后 Requeue(不占计数)→ 关 reaper;Shutdown 后 Submit 返回 ErrShutdown
- [ ] T027 [US7] integration_test.go 增加:Shutdown 正常(3 任务干完才返回)、Shutdown 超时(500ms,任务回 pending 三计数不变)

## Phase 10: User Story 8 - OnStateChange 注册口 (Priority: P3)

- [ ] T028 [US8] 双后端 Notify 接线(事务/锁外异步、recover 包住)+ integration_test.go:回调收到流转快照、回调 panic 不影响主流程

---

## Phase 11: Polish & Cross-Cutting

- [ ] T029 [P] 写 `examples/llm/main.go` 三级流水线示例(memory 后端,可 go run)
- [ ] T030 [P] 导出符号 godoc 注释补全 + README.md(简介/快速开始/与方案文档链接)
- [ ] T031 运行 quickstart.md 验收检查点全表:`go build ./...`、`go vet ./...`、`go test ./... -race`、覆盖率(核心 ≥85%/broker ≥80%,不足则补测试)、宪法取证 grep
- [ ] T032 将完成情况回写 docs/plans/(YYYY-MM-DD-M1完成记录.md)并 git 提交

---

## Dependencies & Execution Order

- Phase 1 → Phase 2 → Phase 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11(scheduler 逐阶段叠功能,同文件顺序执行)
- Phase 1 内:T001 先行,T002/T003/T005 可并行,T004 依赖 T001~T003
- Phase 2 内:T006/T007 可并行(都依赖 Phase 1);T008 先于 T009 定语义基准(文件独立,允许并行开工、串行收敛)
- integration_test.go 各阶段追加场景,不可并行写同一文件

### Parallel Opportunities

```
Phase 1: T002, T003, T005
Phase 2: T006, T007;随后 T008, T009
Phase 11: T029, T030
```

---

## Implementation Strategy

1. Phase 1+2 是地基,必须一次做对(brokertest 是后续一切的安全网)
2. Phase 3 完成即 MVP,STOP and VALIDATE
3. 之后每个 Phase 结束跑 `go test ./... -race` 回归 + git 提交一次

## Task Summary

| 阶段 | 任务数 | 可并行 |
|------|--------|--------|
| P1 Setup | 5 | 3 |
| P2 Foundational | 4 | 2+2 |
| P3~P5(US1~3,P1 级) | 9 | 0 |
| P6~P10(US4~8) | 10 | 0 |
| P11 Polish | 4 | 2 |
| **Total** | **32** | **9** |

**MVP Scope**: Phase 1+2+3 = 12 tasks
