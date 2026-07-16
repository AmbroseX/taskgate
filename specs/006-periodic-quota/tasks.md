# Tasks: 周期配额 Periodic Quota(硬配额)

**Input**: Design documents from `/specs/006-periodic-quota/`
**Prerequisites**: plan.md, spec.md, data-model.md, quickstart.md, contracts/quota-capability-contract.md

**Tests**: L1(配置校验/类型)+ L2(新 `brokertest.RunQuota` 五后端)+ L3(scheduler 集成:耗尽/失联/Stats/双 Gate 共享)+ L4(e2e 三维组合)+ 基准(FR-013)。

**Organization**: US1(硬配额不超发)与 US2(耗尽调度衔接)是 P1;US3(fail-closed 与装配校验)P2。

## Format: `[ID] [P?] [Story] Description`

## Path Conventions

- 核心:`taskgate.go` / `broker.go` / `client.go` / `scheduler.go`
- 后端:`memorybroker/` `sqlitebroker/` `redisbroker/` `internal/sqlbroker/`(薄壳 `pgbroker/` `mysqlbroker/`)
- 测试:`brokertest/quota.go`(L2)、`integration_test.go`(L3)、`e2e/`(L4)、`bench_test.go`

---

## Phase 1: Setup

- [X] T001 QueueConfig 加 QuotaLimit/QuotaPeriod/QuotaKey + validate 三条规则(负值/启用必带 Period/同 key 参数一致)in `taskgate.go`
- [X] T002 [P] QuotaProvider/QuotaGate/QuotaReservation 能力接口 in `broker.go`
- [X] T003 newGate 能力断言 fail-fast(启用配额 ⇒ broker 必须实现 QuotaProvider);QueueStats 加 QuotaExhausted/QuotaStalled in `client.go`
- [X] T004 L1:配置校验用例(合法/非法矩阵、同 key 冲突、能力断言报错文案)in `taskgate_test.go`

## Phase 2: Foundational(套件先行)

- [X] T005 `brokertest.RunQuota` 套件:QuotaFactory(带 advance 介质时间回调)+ Q1~Q5 用例 in `brokertest/quota.go`(新文件)

---

## Phase 3: User Story 1 - 硬配额:窗口内绝不超发 (Priority: P1) 🎯 MVP

**Goal**: 五后端各自介质内原子预留,Q1~Q5 全过。

**Independent Test**: RunQuota 在 memory/sqlite/miniredis 全绿 + MySQL 真机。

- [X] T006 [US1] memory:quota map + Reserve/Release(单锁,注入钟即介质钟,含旧窗清理)in `memorybroker/broker.go`;RunQuota 接入 in `memorybroker/broker_test.go`
- [X] T007 [P] [US1] sqlite:schema.sql 加 quota 表;`sqlitebroker/quota.go`(单语句 Reserve/尽力 Release/轮换清理/测试时间钩子 in `sqlitebroker/export_test.go`);RunQuota 接入 in `sqlitebroker/broker_test.go`
- [X] T008 [P] [US1] redis:`redisbroker/lua/quota_reserve.lua` + `quota_release.lua`(TIME 豁免注释)+ `redisbroker/quota.go`;RunQuota(miniredis SetTime)in `redisbroker/broker_test.go`
- [X] T009 [P] [US1] sqlbroker:quota 表 DDL 进两方言 SchemaSQL;`internal/sqlbroker/quota.go`(PG 单语句/MySQL 两步 affected 判定,方言差异钩子)in `pgbroker/dialect.go`、`mysqlbroker/dialect.go`;RunQuota 门控接入 in `sql_backends_test.go`
- [X] T010 [US1] MySQL 真机跑 RunQuota + 并发竞态(Q5)验证

**Checkpoint**: RunQuota 离线三后端 + MySQL 真机全绿;`QuotaLimit=0` 时零行为变化。

---

## Phase 4: User Story 2 - 耗尽行为与调度衔接 (Priority: P1)

**Goal**: claimLoop 四环;耗尽不占槽、非错误、下窗恢复;三维正交。

**Independent Test**: memory 后端(fakeclock)集成场景。

- [X] T011 [US2] scheduler:quotaState、claimLoop 插入(启发式→Reserve→限时 Dequeue→扑空 Release)、退避常量、run() 构造配额闸 in `scheduler.go`
- [X] T012 [US2] Stats 接线(QuotaExhausted/QuotaStalled 读 quotaState)in `client.go`
- [X] T013 [US2] L3:耗尽暂停(计数零污染/不占槽/在跑不受影响/下窗恢复/Stats 位)in `integration_test.go`
- [X] T014 [US2] L3:双 Gate 共享同一 sqlite 文件每窗恰好 N(时间钩子驱动,SC-001/002)in `integration_test.go` 或 `multiproc_test.go`
- [X] T015 [US2] L4:e2e `{Workers:2,RPS:3,Quota:10/1s}` 组合打 mockgw(SC-005)in `e2e/pipeline_test.go`

**Checkpoint**: `go test ./... -race` 全绿;SC-001/002/005 断言过。

---

## Phase 5: User Story 3 - fail-closed 与装配校验 (Priority: P2)

- [X] T016 [US3] L3:stub 配额闸注入故障 → 零放行、退避重试、QuotaStalled 可见、恢复续上(SC-003)in `integration_test.go`
- [X] T017 [P] [US3] L1:后端不支持能力/同 key 冲突/Period 缺失的 New() 报错文案断言(若 T004 已含则核对项)in `taskgate_test.go`

**Checkpoint**: SC-003 断言过;所有非法配置 fail-fast。

---

## Phase 6: Polish & Cross-Cutting

- [X] T018 [P] 基准:有/无配额认领吞吐对比 + sqlite 热点行争用(可选 MySQL DSN 档)in `bench_test.go`
- [X] T019 [P] README.md / README.en.md:特性表加周期配额;"频率 ≠ 配额"一节 + LLM 组合配置例;能力声明收窄口径(FR-014)
- [X] T020 全量验收:build/vet/`go test ./... -race`/覆盖率;quickstart 检查点逐项勾
- [X] T021 归档:完成记录写 `docs/plans/2026-07-16-spec006-Quota完成记录.md`(含基准数据与 >30% 判定;宪法 V.3 措辞细化建议)

---

## Dependencies & Execution Order

- Phase 1 → 2 → 3(T006 参考实现先行,T007~T009 [P])→ 4 → 5 → 6;T010 依赖 T009;T012 依赖 T011
- 并行组:`T002 与 T001 后半`;`T007,T008,T009`;`T017,T018,T019`

## Implementation Strategy

MVP = T001~T010。memory 先绿再铺其余后端;每 Story 完成跑 `go test ./... -race`。

## Task Summary

| 阶段 | 任务数 | 可并行 |
|------|--------|--------|
| Phase 1 Setup | 4 | 1 |
| Phase 2 Foundational | 1 | 0 |
| Phase 3 US1 | 5 | 3 |
| Phase 4 US2 | 5 | 0 |
| Phase 5 US3 | 2 | 1 |
| Phase 6 Polish | 4 | 2 |
| **Total** | **21** | **7** |

**MVP Scope**: T001~T010(10 tasks)
