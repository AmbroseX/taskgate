# Tasks: Identity 身份模型(BusinessKey / ExecutionID / Replay)

**Input**: Design documents from `/specs/005-identity-replay/`
**Prerequisites**: plan.md, spec.md, data-model.md, quickstart.md, contracts/broker-contract-delta.md

**Tests**: L1(根包类型/选项/错误)+ L2(brokertest 契约 2 重写 + 新增 19~22)+ L3(Gate 接线与并发竞态 in integration_test.go)+ L4(e2e cron 配方)。pg/mysql 走 DSN 门控(宪法 V.5)。

**Organization**: US1(键幂等+Replay 写路径)是核心;US2(不可变与溯源)主要落在契约断言;US3(查询)可后置。

## Format: `[ID] [P?] [Story] Description`

## Path Conventions

- 核心包:`taskgate.go` / `errors.go` / `broker.go` / `client.go`
- 后端:`memorybroker/` `sqlitebroker/` `redisbroker/` `internal/sqlbroker/`(pg/mysql 薄壳:`pgbroker/` `mysqlbroker/`)
- 测试:`brokertest/`(L2)、`*_test.go` 跟随源码(L1)、`integration_test.go`(L3)、`e2e/`(L4)

---

## Phase 1: Setup(类型与接口地基)

- [X] T001 Task 加 `BusinessKey`/`ReplayOf` 字段;`WithBusinessKey`;`WithID` 改 Deprecated 别名并删 `submitOptions.id`;`ReplayOption`(AllowCompleted/WithPayload)in `taskgate.go`
- [X] T002 [P] 新增 `ErrReplayNotFinal`/`ErrAlreadyReplayed`/`ErrCompletedNotAllowed` 哨兵与 `TaskExistsError` 类型(Unwrap→ErrTaskExists)in `errors.go`
- [X] T003 [P] Broker 接口加 `Replay(ctx, ReplayRequest) (*Task, error)`;`ReplayRequest` 定义;`Filter` 加 `BusinessKey` in `broker.go`
- [X] T004 L1 单测:选项应用、错误 Is/As 链、WithID 别名等价 in `taskgate_test.go`

## Phase 2: Foundational(契约先行,全后端此时编译红属预期)

- [X] T005 契约 2 重写 `caseBusinessKeyIdempotent`(含预置 ID 防御保留)in `brokertest/cases_basic.go`
- [X] T006 新契约 19~22(ReplayBasic/ReplayChain/BusinessKeyQuery/IdentityRace)in `brokertest/cases_identity.go`(新文件),`brokertest/suite.go` 注册;溯源断言(US2)并入 ReplayBasic

---

## Phase 3: User Story 1 - 键幂等 + Replay 写路径 (Priority: P1) 🎯 MVP

**Goal**: 同键二次提交拒绝且错误可解构;终态链尾可 Replay,链不分叉,五后端原子。

**Independent Test**: brokertest 契约 2/19/20/22 在 memory+sqlite+miniredis 全绿。

- [X] T007 [US1] memory:sidecar 索引(chains/replayed)+ Enqueue 键校验 + Replay 临界区实现(语义参考)in `memorybroker/broker.go`
- [X] T008 [P] [US1] sqlite:schema 加列/三索引 + 存量库幂等 ALTER in `sqlitebroker/schema.sql`、`sqlitebroker/broker.go`(Init)
- [X] T009 [US1] sqlite:Enqueue 键校验(事务内查链尾 + 撞索引翻译)in `sqlitebroker/enqueue.go`;Replay 事务实现 in `sqlitebroker/replay.go`(新文件)
- [X] T010 [P] [US1] redis:common.lua 加 kBk 键工具;enqueue.lua 键检查+RPUSH;新 `redisbroker/lua/replay.lua`;BusinessKey 校验(控制字符/长度)in `redisbroker/enqueue.go`
- [X] T011 [US1] redis:Replay Go 侧接线(脚本注册、TGERR 翻译、快照回填)in `redisbroker/replay.go`(新文件)、`redisbroker/broker.go`
- [X] T012 [P] [US1] sqlbroker:方言 DDL(pg 部分索引/mysql 生成列)与约束名翻译钩子 in `internal/sqlbroker/dialect.go`、`pgbroker/`、`mysqlbroker/`
- [X] T013 [US1] sqlbroker:Enqueue 键校验 + Replay(FOR UPDATE 串行化)in `internal/sqlbroker/enqueue.go`、`internal/sqlbroker/replay.go`(新文件)
- [X] T014 [US1] Gate 接线:Submit 传 BusinessKey;`Gate.Replay`/`Gate.ReplayByKey`;Enqueue 拒非空 ReplayOf in `client.go`
- [X] T015 [US1] L3 集成:Gate 层同键并发 Submit / 并发 Replay 竞态(-race)in `integration_test.go`

**Checkpoint**: 契约 2/19/20/22 五后端过(pg/mysql 至少本地 DSN 或 CI);`go test ./... -race` 绿。

---

## Phase 4: User Story 2 - 历史不可变与依赖溯源 (Priority: P1)

**Goal**: Replay 后旧执行逐字段不变;DependsOn 视角永远指向旧执行。

**Independent Test**: ReplayBasic 契约中的逐字段断言与 C.DependsOn[0] 断言。

- [X] T016 [US2] 契约 19 补强:E1(completed,r1)←C 场景,Replay 后 Get(E1) 逐字段比对、C 视角断言、新子任务引用 E1 仍见 r1(在 `brokertest/cases_identity.go` 内完成,若 T006 已含则本任务为核对项)
- [X] T017 [US2] L3:真调度下(Gate.Run + handler)完整重放一条 failed 链并验证旧记录字节级不变 in `integration_test.go`

**Checkpoint**: 原型五断言(SC-001)全部契约化且五后端绿。

---

## Phase 5: User Story 3 - 按键查询 (Priority: P2)

**Goal**: Filter.BusinessKey 过滤 + Gate.History 链序枚举。

**Independent Test**: 契约 21(BusinessKeyQuery)三离线后端绿。

- [X] T018 [P] [US3] memory:List 支持 BusinessKey 过滤 in `memorybroker/broker.go`
- [X] T019 [P] [US3] sqlite:query 加键过滤 in `sqlitebroker/query.go`
- [X] T020 [P] [US3] redis:kBk LIST 做候选集 in `redisbroker/query.go`
- [X] T021 [P] [US3] sqlbroker:query 加键过滤 in `internal/sqlbroker/query.go`
- [X] T022 [US3] `Gate.History`(List 封装)+ L1 测试 in `client.go`、`taskgate_test.go`

**Checkpoint**: 契约 21 全绿;History 空键返回空切片。

---

## Phase 6: Polish & Cross-Cutting

- [X] T023 [P] e2e cron 配方场景:同键被拒 → errors.As 拿链尾 → Replay → 跑完 in `e2e/`
- [X] T024 [P] sqlite 存量库升级用例(旧 schema 文件打开→新版本可读可调度,SC-006)in `sqlitebroker/broker_test.go` 或 `sqlitebroker/internal_test.go`
- [X] T025 [P] README.md / README.en.md:cron 配方重写(BusinessKey+Replay)、WithID 迁移说明;examples/ 如涉及 WithID 同步
- [X] T026 [P] `docs/plans/2026-07-15-MySQL-PG后端适配方案.md` 补"Broker 接口 +Replay"修订注记
- [X] T027 全量验收:`go build ./...`、`go vet ./...`、`go test ./... -race`、覆盖率(核心 ≥85%/broker ≥80%)、quickstart 检查点逐项勾
- [X] T028 归档:完成记录写 `docs/plans/2026-07-16-spec005-Identity完成记录.md`;宪法 IV.2 措辞对齐建议记录在内

---

## Dependencies & Execution Order

- Phase 1 → Phase 2 → Phase 3(T007 参考实现先行,T008~T013 后端间 [P])→ Phase 4 → Phase 5 → Phase 6
- T014 依赖 T007(至少 memory 可跑);T015 依赖 T014;US3 依赖 Phase 1 的 Filter 字段,不依赖 US1/US2 的 Replay(可提前)
- 并行组:`T002,T003`;`T008,T010,T012`;`T018~T021`;`T023~T026`

## Implementation Strategy

MVP = Phase 1+2+3(T001~T015)。先 memory 全绿再铺其他后端;每个 Story 完成跑 `go test ./... -race` 再进下一个。

## Task Summary

| 阶段 | 任务数 | 可并行 |
|------|--------|--------|
| Phase 1 Setup | 4 | 2 |
| Phase 2 Foundational | 2 | 0 |
| Phase 3 US1 | 9 | 3 |
| Phase 4 US2 | 2 | 0 |
| Phase 5 US3 | 5 | 4 |
| Phase 6 Polish | 6 | 4 |
| **Total** | **28** | **13** |

**MVP Scope**: T001~T015(15 tasks)
