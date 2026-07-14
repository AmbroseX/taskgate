# Implementation Plan: taskgate M3 — 仿真 E2E、手动续租与 List 分页

**Branch**: `003-m3-polish` | **Date**: 2026-07-15 | **Spec**: [spec.md](./spec.md)

## Summary

四件事:① `e2e/` 目录落地 mock LLM/OCR 网关(故障注入开关+原子并发观测)与测试方案第 4 节的五个 L4 用例;② handler ctx 注入手动续租入口 `RenewLease(ctx)`,QueueConfig 加 `ManualHeartbeat` 关自动心跳;③ Filter 加 Offset,List 排序合同定为"(CreatedAt,ID) 升序",先加 brokertest 契约再改三后端;④ `//go:build realgw` 真实网关冒烟档。公开 API 只增不改,Broker 接口签名零改动。

## Technical Context

**Language/Version**: Go 1.25
**Primary Dependencies**: 零新增(mock 网关用标准库 net/http/httptest;随机用 math/rand 固定种子)
**Affected Backends**: 三后端都改 List(排序+Offset);Broker 接口签名不变(Filter 是结构体加字段,兼容)
**Testing**: L4(e2e/pipeline_test.go,五用例)+ L2(brokertest 新增 ListPagination 契约)+ L3(手动续租集成场景)+ L5(realgw 手动档)
**Concurrency Semantics**: 手动续租与自动心跳共用租约令牌,Heartbeat 幂等延长;mock 网关并发观测用原子计数
**Performance Goals**: 无新增指标;redis List 分页为候选集内存排序,O(N) 写进已知限制
**Constraints**: 公开 API 只增不改;库本体不读 env(realgw 测试读 env 属测试豁免);L4 离线确定(固定种子)

## Constitution Check

| 原则 | 本计划遵循方式 | 结果 |
|------|---------------|------|
| I 库的边界 | 零新依赖;mockgw 在 e2e/ 测试目录,不进库 API;优先级/webhook 维持不做 | ✅ |
| II.1 接口最小公倍数 | Broker 签名不变;List 排序+Offset 三后端都能同语义实现 | ✅ |
| II.3 brokertest 合规 | 先加 ListPagination 契约(第 18 条),再改三后端 | ✅ |
| II.4 租约令牌 | RenewLease 带当前令牌走既有 Heartbeat,ErrLeaseLost 语义不变 | ✅ |
| III 原子性/计数 | 无状态机与计数变更;手动模式下 reaper/LeaseLost 语义原样 | ✅ |
| V 测试纪律 | L4 离线+固定种子;realgw 构建标签隔离不进 CI;-race 全量 | ✅ |
| VI 文档纪律 | spec-kit 产物在 specs/003-m3-polish/,完成后归档 docs/plans/ | ✅ |

**Constitution Check Result**: ✅ 通过

## Project Structure

### Documentation (this feature)

```text
specs/003-m3-polish/
├── spec.md / plan.md / research.md / data-model.md / quickstart.md
└── contracts/ 不新建 —— List 分页合同直接修订 001 的 broker-contract.md(Filter 段+用例表)
```

### Source Code (repository root)

```text
taskgate/
├── taskgate.go                # QueueConfig 加 ManualHeartbeat;Filter 在 broker.go
├── broker.go                  # Filter 加 Offset 字段(注释写排序合同)
├── client.go                  # RenewLease(ctx) 导出函数 + ctx 注入键(或放 taskgate.go,plan 定在 client.go)
├── scheduler.go               # execute 注入续租闭包进 handler ctx;ManualHeartbeat 时不起心跳 goroutine
├── brokertest/cases_query.go  # ListPagination 契约(第 18 条)+ ListFilter 扩展排序断言
├── memorybroker/broker.go     # List 排序+Offset
├── sqlitebroker/query.go      # List SQL 加 ORDER BY created_at,id 与 OFFSET
├── redisbroker/query.go       # List 候选集排序+切片
├── e2e/
│   ├── mockgw/mockgw.go       # mock 网关(故障注入开关+并发观测)
│   ├── pipeline_test.go       # L4 五用例
│   └── realgw_test.go         # //go:build realgw
└── integration_test.go        # 手动续租 L3 场景(自动档互不干扰/手动档保活/不续被回收)
```

**Structure Decision**: e2e/ 目录与测试方案第 8 节的目录规划一致;mockgw 做成可导入的测试组件包(非 _test.go)以便 realgw/pipeline 共用,但不在库根包、不进公开 API 文档。

## Complexity Tracking

无(零新依赖;无宪法违规)。
