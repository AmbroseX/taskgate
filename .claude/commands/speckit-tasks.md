# 任务清单 — 基于 plan.md / data-model.md 生成 tasks.md

你负责把技术实施计划拆成**可并行、可独立验收**的任务清单，作为 `/speckit-implement` 的输入。

## 用户输入

$ARGUMENTS

- `（空）` — 对当前分支 / 唯一的 spec 目录生成 tasks
- `<spec 目录名>` — 显式指定
- `--no-parallel` — 禁止标记 `[P]` 并行任务（某些团队习惯顺序执行）

## 执行流程

### 1. 定位目标目录并读取前置文档

必读：
- `specs/NNN-<slug>/plan.md`
- `specs/NNN-<slug>/spec.md`（拿到 User Story 的优先级与独立性要求）
- `specs/NNN-<slug>/data-model.md`
- `specs/NNN-<slug>/quickstart.md`
- `specs/NNN-<slug>/contracts/`（如存在）
- `.specify/memory/constitution.md`

缺失任何一个（除 contracts）都应提示用户先运行 `/speckit-plan`。

### 2. 组织原则

- **按 User Story 分组**：每个 Story 是一组可独立交付的任务集
- **标注优先级**：沿用 spec 中的 P1/P2/P3
- **标注并行**：不同文件、无依赖的任务标 `[P]`
- **标注归属**：每条任务标 `[US1]` / `[US2]` 等映射回用户故事
- **精确路径**：每条任务必须包含具体文件路径（可直接执行，不需要猜）
- **契约先行**：涉及 Broker 行为变更的 Story，`brokertest` 契约用例任务排在各后端实现之前
- **可检查点**：每个 Story 结束放一个 `**Checkpoint**` 说明此时系统应达到什么状态
- **MVP 范围**：明确 Phase 1（Setup）+ P1 Story = MVP

### 3. 生成 tasks.md

文件：`specs/NNN-<slug>/tasks.md`

结构模板：

```markdown
# Tasks: <功能名>

**Input**: Design documents from `/specs/NNN-<slug>/`
**Prerequisites**: plan.md, spec.md, data-model.md, quickstart.md

**Tests**: <根据 plan 的 Testing 段落填写覆盖层级（L1/L2/L3/L4）；taskgate 是库项目，行为变更默认要有测试任务>

**Organization**: 任务按用户故事分组，每个故事可独立实现和测试。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可并行执行（不同文件，无依赖）
- **[Story]**: 所属用户故事（如 US1, US2）
- 包含准确的文件路径

## Path Conventions

- 核心包：<从 plan.md 的 Project Structure 抄过来，如 taskgate.go / scheduler.go / broker.go>
- 后端实现：memorybroker/ sqlitebroker/ redisbroker/
- 测试：brokertest/（L2）、*_test.go 跟随源码（L1）、integration_test.go（L3）、e2e/（L4）

---

## Phase 1: Setup

**Purpose**: 类型定义与接口变更（后续任务的公共地基）

- [ ] T001 <新增类型 / 选项 / Config 字段 in taskgate.go>
- [ ] T002 [P] <Broker 接口签名变更 in broker.go>
- ...

## Phase 2: Foundational

**Purpose**: 跨 Story 的基础设施（如 brokertest 新契约用例；如无，说明"无需额外基础设施开发"）

---

## Phase 3: User Story 1 - <标题> (Priority: P1) 🎯 MVP

**Goal**: <一句话目标>

**Independent Test**: <如何独立验证，如"brokertest 新契约在 memory+sqlite 全绿">

### 契约与实现 - User Story 1

- [ ] T00X [US1] brokertest 契约用例 in brokertest/suite.go
- [ ] T00X [P] [US1] memory 实现 in memorybroker/broker.go
- [ ] T00X [P] [US1] sqlite 实现 in sqlitebroker/broker.go
- [ ] T00X [P] [US1] redis 实现 in redisbroker/broker.go
- [ ] T00X [US1] 调度器 / 客户端接线 in scheduler.go / client.go

### 测试 - User Story 1

- [ ] T0XX [P] [US1] L1 单元测试 in <完整路径>
- [ ] T0XX [US1] L3 集成测试场景 in integration_test.go

**Checkpoint**: <Story 1 验收状态>

---

## Phase 4: User Story 2 - ...

（同上结构）

---

## Phase N: Polish & Cross-Cutting Concerns

- [ ] TXXX [P] 导出符号的 godoc 注释补全
- [ ] TXXX [P] README / examples/ 示例更新（如公开 API 有变化）
- [ ] TXXX 验证 quickstart.md 中所有验收检查点
- [ ] TXXX 运行 `go build ./...` && `go vet ./...` && `go test ./... -race` 确认全绿

---

## Dependencies & Execution Order

### Phase Dependencies

- Setup → Foundational → User Stories → Polish
- User Story 间的依赖（如 US2 依赖 US1 新增的接口方法）

### Within Each User Story

- 类型定义 → Broker 接口 → brokertest 契约 → 各后端实现（互相 [P]）→ scheduler/client 接线 → 集成测试
- 并行标记 [P] 的任务可同时启动

### Parallel Opportunities

列出每个 Phase 下可并行的任务组：

\`\`\`bash
# Phase 3 可并行（三后端实现互不冲突）：
T004, T005, T006
\`\`\`

---

## Implementation Strategy

### MVP First

1. Phase 1 完成 → 类型与接口就绪
2. Phase 3（US1）完成 → MVP 可验证
3. **STOP and VALIDATE**：测试 US1 独立功能

### Incremental Delivery

1. Setup → US1（MVP）→ 全量测试
2. 追加 US2 → 回归测试
3. 追加 US3 → ...

---

## Task Summary

| 阶段 | 任务数 | 可并行任务数 |
|------|--------|-------------|
| Phase 1: Setup | X | Y |
| Phase 3: US1 | X | Y |
| ... | | |
| **Total** | **N** | **M** |

**MVP Scope**: Phase 1 + Phase 3 = <数量> tasks

**Parallel Efficiency**: <并行任务数> / <总任务数> = <百分比>

---

## Notes

- [P] 标记的任务操作不同文件，无依赖，可并行
- [Story] 标签便于追踪到具体需求
- 每完成一组相关任务后提交 git（遵循 CLAUDE.md 的提交约定）
- Go 代码改完运行 `go build ./...` 验证；Story 完成跑 `go test ./... -race`
- 避免：模糊任务、同文件冲突、破坏故事独立性
```

### 4. 自检清单

写入前核对：
- [ ] 任务编号连续（T001, T002, ...）
- [ ] 每条任务都有完整文件路径
- [ ] 并行任务 [P] 操作的文件确实互不相同
- [ ] Broker 行为变更的 Story 里，brokertest 契约任务排在后端实现之前
- [ ] 每个 User Story 的任务能在不做其他 Story 的前提下完整交付
- [ ] MVP 范围清晰（Phase 1 + 最高优先级 Story）
- [ ] 任务里没有引入 plan.md 未定义的新文件或新依赖

## 写入要求

- **中文**
- 分批写入，避免 API 中断
- 写完后打印：任务总数、MVP 任务数、下一步提示 `运行 /speckit-implement 开始实现`

## 启动

1. 一句话汇报：将要读取的 plan / data-model / spec 路径
2. 扫描 plan 的 Project Structure，列出所有将涉及的新文件清单
3. 按阶段分组生成任务
4. 自检通过后落盘
