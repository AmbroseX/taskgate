# Tasks: taskgate M3 — 仿真 E2E、手动续租与 List 分页

**Input**: Design documents from `/specs/003-m3-polish/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, quickstart.md(List 合同修订进 001 的 broker-contract.md)

**Tests**: L2(brokertest 第 18 条 ListPagination)、L3(手动续租场景)、L4(e2e/ 五用例)、L5(realgw 手动档)。

**Organization**: 三个功能块相互独立(分页动三后端、续租动 scheduler/client、L4 全是新增测试目录),按故事推进。

## Format: `[ID] [P?] [Story] Description`

## Path Conventions

- 核心包:taskgate.go / broker.go / errors.go / client.go / scheduler.go / integration_test.go
- 后端:memorybroker/broker.go、sqlitebroker/query.go、redisbroker/query.go;契约:brokertest/cases_query.go
- L4:e2e/mockgw/mockgw.go、e2e/pipeline_test.go、e2e/realgw_test.go

---

## Phase 1: User Story 3 - List 分页 (Priority: P2,先做——契约先行的地基改动)

**Goal**: Filter+Offset,(CreatedAt,ID) 升序,三后端一致。

**Independent Test**: brokertest ListPagination 在三后端全绿。

- [x] T201 [US3] broker.go Filter 加 Offset 字段+排序合同注释;修订 specs/001-m1-core-queue/contracts/broker-contract.md(Get/List 段写排序/Offset/越界语义,用例表加第 18 条 ListPagination)
- [x] T202 [US3] brokertest/cases_query.go 加 caseListPagination(25 任务 fakeclock 逐个 +1ms、3 页并集无重无漏、页内跨页升序、越界空、Offset+Limit=0 组合、过滤+分页组合)并挂入 suite.go——先让三后端红
- [x] T203 [US3] 三后端实现:memorybroker/broker.go(过滤后 sort+切片)、sqlitebroker/query.go(ORDER BY created_at,id + LIMIT/OFFSET,Limit=0 用 -1)、redisbroker/query.go(候选集排序+切片)——契约全绿

**Checkpoint**: brokertest 18 条三后端全绿,既有用例零回归。

---

## Phase 2: User Story 2 - handler 手动续租 (Priority: P2)

**Goal**: RenewLease(ctx) + QueueConfig.ManualHeartbeat。

**Independent Test**: 手动档保活 3×TTL 零回收;不续则 LeaseLost+1;非任务 ctx 返回 ErrNoTask。

- [x] T204 [US2] errors.go 加 ErrNoTask;taskgate.go QueueConfig 加 ManualHeartbeat(yaml/json tag);client.go 加 RenewLease(ctx)+非导出 ctx key(骨架照 quickstart)
- [x] T205 [US2] scheduler.go:execute 把续租闭包注入 handler ctx(语义照 research 第 2 节:ErrTaskCanceled 先 cancel 再返回、ErrLeaseLost 置标记+cancel、网络错误原样);ManualHeartbeat=true 时不起心跳 goroutine(收尾通道短路,零泄漏)
- [x] T206 [US2] integration_test.go 加场景(双后端 memory/sqlite 即可,redis 走同一 scheduler 路径):自动档 RenewLease 与心跳共存、手动档定期续租跑 3×TTL 零回收(LeaseLost=0)、手动档不续租被回收(LeaseLost=1)、回收后旧 ctx 续租返回 ErrLeaseLost、非任务 ctx 返回 ErrNoTask、手动档 Shutdown 正常 Requeue

**Checkpoint**: US2 场景全绿;ManualHeartbeat=false 全量零回归。

---

## Phase 3: User Story 1 - L4 仿真 E2E (Priority: P1) 🎯 本里程碑核心

**Goal**: mockgw + 测试方案第 4 节五用例,离线确定。

**Independent Test**: `go test ./e2e/... -race` 全绿。

- [x] T207 [US1] 写 e2e/mockgw/mockgw.go:New(opts)/URL/Close/MaxConcurrency/BusyCount/Requests + Latency/BusyAfterConcurrency(200+SSE busy 事件)/FailRate(固定种子)/CrashAfterConcurrency(断连) 四开关,并发观测原子;响应体约定照 data-model 第 5 节;附 mockgw_test.go 最小自测(开关行为+并发观测正确性)
- [x] T208 [US1] 写 e2e/pipeline_test.go 五用例(memory 后端):①限流挡 busy({W:2} MaxConcurrency≤2 且 BusyCount=0;{W:5} busy 走 ErrThrottled 零 failed)②OCR 灌库({W:2} 不崩全完;{W:6} 断连走普通重试补完——断连条件并发 >4,{W:4} 打不破红线)③三队列流水线 30/30 且 score 读到 extract 的 Result ④中途取消(ocr 完成、Cancel extract → score 连锁 canceled、ocr 保持 completed)⑤SSE 藏错误(200 体内错误事件 → handler 判定 ErrThrottled → 重排后成功)

**Checkpoint**: L4 全绿,M3 核心交付达成(测试方案第 9 节"M3 试点前=L4 全绿")。

---

## Phase 4: User Story 4 - realgw 冒烟档 (Priority: P3)

- [x] T209 [US4] 写 e2e/realgw_test.go(//go:build realgw):读 LLM_GATEWAY_URL/KEY 缺失即 Skip;10 任务 {Workers:2,RPS:1} sqlite 后端全 completed;注释写明 max_tokens≥600 与 NO_PROXY 坑;取证常规构建零引入(不带 tag 的 go vet/go test 不编译)

---

## Phase 5: Polish & Cross-Cutting

- [x] T210 [P] README 更新(RenewLease/ManualHeartbeat 用法与跨进程取消在手动档的语义、List 分页合同与 redis 代价、e2e 目录说明、realgw 跑法);godoc 补全新导出符号
- [x] T211 quickstart.md 验收检查点全表执行(brokertest 18 条、L4 五用例、续租三场景、零回归、覆盖率、连跑 3 遍、realgw 零引入取证)
- [x] T212 完成记录写 docs/plans/2026-07-15-M3完成记录.md(交付/验收/裁决/遗留:优先级与 webhook 维持不做、游标分页 M4 再议、hr-matching 试点属外部项目)——git 提交由主控做

---

## Dependencies & Execution Order

- Phase 1 → 2 → 3 → 4 → 5 顺序执行(1/2/3 理论上互不依赖,但 integration_test.go 与根包文件有共改,串行最稳;3 依赖的都是既有 API)
- Phase 1 内 T201→T202→T203 严格串行(契约先行);Phase 5 的 T210 可与 T209 并行

### Parallel Opportunities

```
Phase 4~5: T209, T210 互不同文件可并行
```

---

## Implementation Strategy

1. Phase 1(分页)是唯一动三后端的地基改动,先做完保住契约
2. Phase 3(L4)是里程碑核心交付,完成即 M3 主目标达成
3. 每 Phase 结束 `go test ./... -race` 回归 + git 提交

## Task Summary

| 阶段 | 任务数 | 可并行 |
|------|--------|--------|
| P1 US3 分页 | 3 | 0 |
| P2 US2 续租 | 3 | 0 |
| P3 US1 L4 | 2 | 0 |
| P4 US4 realgw | 1 | 1 |
| P5 Polish | 3 | 1 |
| **Total** | **12** | **2** |

**MVP Scope**: Phase 1~3 = 8 tasks(US1 是核心,US3/US2 是其地基与并列交付)
