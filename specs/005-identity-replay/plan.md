# Implementation Plan: Identity 身份模型(BusinessKey / ExecutionID / Replay)

**Branch**: `005-identity-replay` | **Date**: 2026-07-16 | **Spec**: [spec.md](./spec.md)

## Summary

把 `Task.ID` 的双重职责拆开:`ID` 收紧为系统生成、永不复用的 ExecutionID;新增 `BusinessKey`(业务幂等键)与 `ReplayOf`(重放来源指针)两个不可变字段。Submit 带键时"键下存在任何执行即拒";新增 `Replay` 操作对终态链尾创建新执行。两条模型不变式("键下 ≤1 非终态"、"链不分叉")落成**两个存储级唯一约束**:链头唯一(同键下 `replay_of` 为空的行至多一条)、重放来源唯一(`replay_of` 指向同一执行的行至多一条),并发竞态由约束兜底而不是靠应用层检查。Broker 接口新增 `Replay` 方法(15 → 16),brokertest 契约 2 重写 + 新增 Replay/BusinessKey 契约,五后端同批落地。

## Technical Context

**Language/Version**: Go 1.25(go.mod)
**Primary Dependencies**: 现有依赖零新增——modernc.org/sqlite、go-redis/v9、pgx/v5(stdlib)、go-sql-driver/mysql、oklog/ulid/v2
**Affected Backends**: 全部五个(memory / sqlite / redis / pg / mysql,后两者共享 `internal/sqlbroker`);进 Broker 接口(新增 `Replay`,修改 `Enqueue` 与 `List` 的行为合同)
**Testing**: L1 单元(选项/错误类型)→ L2 brokertest(契约 2 重写 + 新增 4 条)→ L3 集成(Gate.Replay 接线、并发竞态)→ L4 e2e(cron 配方走通);pg/mysql 走环境变量门控(宪法 V.5)
**Concurrency Semantics**: 键幂等与链不分叉靠存储级唯一约束(sqlite/pg 部分唯一索引、mysql 生成列唯一索引、redis 单段 Lua、memory 单锁);Replay 的"创建新执行 + 校验前置条件"必须同事务/同 Lua(宪法 III)
**Performance Goals**: 无新增性能要求;Enqueue 多一次键查(带索引),Replay 是低频操作
**Constraints**: 零新依赖;存量数据零迁移脚本(新列可空/默认空串);状态机与租约/唤醒机制零改动;`WithID` 编译兼容(Deprecated 别名)

## Constitution Check

| 原则 | 本计划遵循方式 | 结果 |
|------|---------------|------|
| I 库的边界 | 不新增服务型能力;Replay 是库 API;不做 TTL/清理(本功能恰是"不删数据"的方案) | ✅ |
| II.1 接口最小公倍数 | `Replay` 五后端都能同语义实现(锁/事务/Lua 三种原子手段都够用) | ✅ |
| II.2 上层不特判后端 | Gate.Replay 只调 Broker 接口;无类型断言 | ✅ |
| II.3 brokertest 先行 | 新契约先写进 brokertest,再改各后端(tasks.md 强排此序) | ✅ |
| III 原子性 | Replay 的校验+创建同事务/同 Lua;终态更新与唤醒逻辑不动 | ✅ |
| III.4 依赖无环 | "父必须已存在"校验不动,只是明确校验对象是 ExecutionID | ✅ |
| IV.1 RawMessage | 新字段是 string,Payload 复制沿用 RawMessage | ✅ |
| IV.2 任务 ID | ulid 自动生成保留;"自定义 ID 幂等去重"语义**迁移**到 BusinessKey——契约 2 重写属宪法 II.3 允许的"修改行为契约"路径,ErrTaskExists 语义保留 | ✅(见下方说明) |
| IV.3 导出哨兵错误 | 新增 4 个导出错误 + 1 个可 errors.As 的类型,`errors.Is(err, ErrTaskExists)` 兼容 | ✅ |
| V 测试纪律 | 分层落位见 Testing;fakeclock 照旧;pg/mysql 门控例外(V.5)照旧 | ✅ |
| VI 文档流程 | spec-kit 产物在 specs/005-…;完成后归档 docs/plans/ | ✅ |

**宪法 IV.2 说明**:该条写着"支持自定义 ID 做幂等去重"。本功能把"用户可指定的幂等键"从 ID 字段挪到 BusinessKey 字段,幂等去重能力**不减反强**(错误携带链尾信息、键下历史可查),`Enqueue 遇到已存在 ID 必须返回 ErrTaskExists` 在存储层照样成立(主键防御)。属"重大扩展"而非违背;落地后建议随归档提一次宪法 MINOR 修订,把 IV.2 的措辞对齐新模型。

**Constitution Check Result**: ✅ 通过(附 IV.2 措辞对齐的后续动作)

## Project Structure

### Documentation (this feature)

```text
specs/005-identity-replay/
├── spec.md
├── plan.md
├── research.md          # 5 个关键决策
├── data-model.md        # 字段表 + 各介质约束落法 + DDL/Lua 要点
├── quickstart.md        # 开发顺序 + 骨架代码 + 验收清单
└── contracts/
    └── broker-contract-delta.md   # Broker 接口增量合同 + 新契约用例清单
```

### Source Code (repository root)

```text
taskgate/
├── taskgate.go            # Task 加 BusinessKey/ReplayOf;WithBusinessKey;WithID 打 Deprecated;ReplayOptions
├── errors.go              # ErrReplayNotFinal/ErrInFlight/ErrAlreadyReplayed/ErrCompletedNotAllowed;TaskExistsError 类型
├── broker.go              # Broker 接口 +Replay;Filter 加 BusinessKey 字段
├── client.go              # Gate.Replay / Gate.ReplayByKey / Gate.History;Submit 接线 BusinessKey
├── memorybroker/broker.go # 键索引 sidecar + Replay(单锁原子)
├── sqlitebroker/          # schema.sql 加列加部分唯一索引;enqueue.go 键校验;新 replay.go;query.go 过滤
├── redisbroker/           # key 设计加 bk 链索引;enqueue/replay Lua;query.go 过滤
├── internal/sqlbroker/    # 方言化 DDL(pg 部分索引 / mysql 生成列);enqueue/replay/query
├── brokertest/            # cases_basic.go 契约 2 重写;新 cases_identity.go;suite.go 注册
├── integration_test.go    # Gate 层接线与并发竞态
└── e2e/                   # cron 配方场景(被拒→读链尾→Replay→跑完)
```

**Structure Decision**: 沿用既有"根包公共类型 + 每后端一包 + brokertest 合规套件"分层;Replay 在每个后端新开一个文件(sqlite/sqlbroker)或并入 enqueue 同文件(redis/memory),对齐各包现有文件粒度。

## Complexity Tracking

| 超标项 | 原因 | 补救措施 |
|--------|------|----------|
| Broker 接口 15 → 16 方法 | Replay 的前置校验+创建必须在共享介质内原子完成,client 层组合无法满足并发不变式(FR-010) | 一次只加一个方法;`docs/plans/2026-07-15-MySQL-PG后端适配方案.md`"接口一行不改"的结论同步修订(调研方案第 12 节已列为既成风险) |
| brokertest 契约 2 语义变更 | 幂等判定从 ID 挪到 BusinessKey 是本功能的核心 | 存储层 ID 唯一性(主键)保留并单独保留用例,防止防御性语义丢失 |
