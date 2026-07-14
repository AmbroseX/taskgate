# 技术实施计划 — 基于 spec.md 生成 plan.md 与配套设计文档

你负责把 `specs/NNN-<slug>/spec.md` 翻译成**技术实施计划**，包括技术选型、包结构、数据模型、研究决策、快速开始指南。这是 `/speckit-tasks` 的输入。

## 用户输入

$ARGUMENTS

支持的形式：
- `（空）` — 对**当前分支**对应的 spec 目录生成 plan
- `<spec 目录名>` — 如 `001-priority-scheduling`，显式指定目标
- `--force` — 覆盖已存在的 plan.md / research.md / data-model.md / quickstart.md

## 执行流程

### 1. 定位目标 spec 目录

- 如参数为空：用 `git branch --show-current` 取当前分支，匹配 `specs/<branch>/`
- 如 `specs/` 下只有一个目录：直接用它
- 否则要求用户显式指定
- 校验目标目录下存在 `spec.md`，否则报错并提示先运行 `/speckit-specify`

### 2. 读取并消化上下文

必读：
- `.specify/memory/constitution.md` — 吸收所有 MUST 约束
- `specs/NNN-<slug>/spec.md` — 功能需求与用户故事
- `CLAUDE.md` — 项目层级约定
- `docs/plans/` 下的 taskgate 设计方案（v5）与测试方案 — 既有架构决策与测试分层
- `go.mod` — 现有 Go 版本与依赖（若尚不存在，以设计方案为准）

按需读：
- 已有源码的包结构（`broker.go`、`scheduler.go`、各 `*broker/` 目录），理解现有分层
- 相近功能的现有实现（用于模式参考）
- `brokertest/` 已有契约用例（新行为要不要进合规套件的判断依据）

### 3. 生成 plan.md

文件：`specs/NNN-<slug>/plan.md`

必含段落：

```markdown
# Implementation Plan: <功能名>

**Branch**: `NNN-<slug>` | **Date**: YYYY-MM-DD | **Spec**: [spec.md](./spec.md)

## Summary

<2-4 句话概括要做什么、怎么做>

## Technical Context

**Language/Version**: <从 go.mod 读，如 Go 1.2x>
**Primary Dependencies**: <会用到的现有依赖，如 modernc.org/sqlite、go-redis、x/time/rate>
**Affected Backends**: <memory / sqlite / redis 中哪些受影响；是否进 Broker 接口>
**Testing**: <落在测试方案的哪几层：L1 单元 / L2 brokertest / L3 集成 / L4 mock 网关 E2E>
**Concurrency Semantics**: <涉及的并发保证：认领互斥、原子唤醒、租约令牌等>
**Performance Goals**: <吞吐 / 延迟目标，或"无新增性能要求">
**Constraints**: <向后兼容、零新依赖等约束>

## Constitution Check

逐条对照 `.specify/memory/constitution.md` 中的 MUST 条款，给出本计划的遵循情况：

| 原则 | 本计划遵循方式 | 结果 |
|------|---------------|------|
| Broker 接口最小公倍数 | 三后端都能同语义实现才进接口 | ✅ |
| brokertest 合规 | 新行为契约先写进 brokertest | ✅ |
| 终态+唤醒原子性 | 同事务 / 同 Lua | ✅ |
| YAGNI 边界 | 不触碰第 10 节不做清单 | ✅ |
| ... | ... | ... |

**Constitution Check Result**: ✅ 通过 / ❌ 存在违规（列出并说明补救）

## Project Structure

### Documentation (this feature)

\`\`\`text
specs/NNN-<slug>/
├── plan.md
├── spec.md
├── research.md
├── data-model.md
├── quickstart.md
└── contracts/       # 可选，如涉及 Broker 接口变更
\`\`\`

### Source Code (repository root)

列出**将要新增或修改的文件**，按现有包结构组织。例：

\`\`\`text
taskgate/
├── taskgate.go            # 新增选项 / 类型
├── scheduler.go           # 调度逻辑改动
├── broker.go              # Broker 接口变更（如有，须三后端同步）
├── memorybroker/broker.go
├── sqlitebroker/broker.go
├── redisbroker/broker.go
├── brokertest/suite.go    # 新增契约用例
└── examples/<feature>/    # 如需示例
\`\`\`

**Structure Decision**: <一句话说明为什么这么组织>

## Complexity Tracking

> 如无违规或复杂度超标项，写"无"

| 超标项 | 原因 | 补救措施 |
|--------|------|----------|

```

### 4. 生成 research.md

文件：`specs/NNN-<slug>/research.md`

每个**需要技术决策的点**用以下结构记录：

```markdown
# Research: <功能名>

## <决策项 1>

**问题**: <需要决策什么>

**研究结果**: <调研发现>

**Decision**: <最终选择>

**Rationale**: <为什么选这个>

**Alternatives considered**:
- 方案 A：<排除原因>
- 方案 B：<排除原因>
```

重点决策项通常包含：
- 是否进 Broker 接口（三后端能否同语义实现）
- sqlite 的 SQL 写法 vs Redis 的数据结构 + Lua 脚本设计
- 并发控制与竞态处理（租约、令牌、原子性边界）
- 公开 API 形态（新方法 / 新 Option / 新 Config 字段）
- 状态机变更（新状态？新流转？）
- 是否引入新依赖（默认不引入，引入要在 Complexity Tracking 说明）

### 5. 生成 data-model.md

文件：`specs/NNN-<slug>/data-model.md`

必含（按涉及面取舍，不涉及的段落写"无变更"）：
- 受影响的 Go 类型定义（Task / Config / QueueConfig / 新类型），字段表（字段名、类型、默认值、语义）
- 状态机变更图（用 ASCII 或 Mermaid，标出新增流转与非法流转）
- sqlite：表结构 / 索引 / 关键 SQL（DDL + 认领类语句）
- redis：key 设计（名称、类型、用途）+ Lua 脚本要点（原子性边界）
- 配置校验规则（New 时 fail fast 的条件）
- 计数语义（动了 Attempts / LeaseLost / Throttled 中的哪个，为什么）

### 6. 生成 quickstart.md

文件：`specs/NNN-<slug>/quickstart.md`

必含：
- 前置条件（依赖版本、是否需要本地 Redis / miniredis）
- 开发顺序（类型定义 → Broker 接口 → brokertest 契约 → memory 实现 → sqlite → redis → scheduler/client 接线）
- 关键代码参考（复制即可用的骨架，3-5 段：接口签名、契约用例、后端实现要点）
- 测试步骤（`go test ./... -race`、`TASKGATE_REDIS_ADDR` 真 Redis 档）
- 验收检查点（checklist，对齐测试方案的层级定义）

### 7. 生成 contracts/（可选）

如果功能涉及 Broker 接口变更或新的行为契约：
- 生成 `contracts/broker-contract.md`：新增 / 修改的接口方法签名、每个方法的语义合同（阻塞行为、错误返回、幂等性、令牌校验）
- 列出要加进 `brokertest` 的契约用例清单（用例名 + Given/When/Then 一句话）

## 写入要求

- **中文**
- 所有代码示例使用项目真实存在的包路径与类型名
- 包结构要与设计方案第 12 节 / 宪法一致
- 禁止引入宪法未批准的新依赖；如必须引入，在 Complexity Tracking 中说明
- **分批写入**长文件，避免 API 中断

## 启动

1. 一句话汇报：定位到的 spec 目录 + 将生成的文件清单
2. 先扫描代码库建立技术上下文（依赖、包结构、已有模式）
3. 按顺序生成：plan.md → research.md → data-model.md → quickstart.md → contracts/（如需）
4. 写完后打印：所有新建文件路径 + 下一步提示 `运行 /speckit-tasks 生成任务清单`
