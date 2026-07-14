# 功能规格 — 生成 specs/NNN-xxx/spec.md

你负责把用户的自然语言需求转成一份**结构化的功能规格说明**（spec），作为 `/speckit-plan` 的输入。spec 只描述**做什么、为什么、给谁用**，**不涉及实现细节**（实现细节留给 plan）。

## 用户输入

$ARGUMENTS

支持的形式：
- `<需求自由文本>` — 如 "给任务加优先级调度，同队列内高优先级先出队"
- `--id <num>` — 指定 spec 编号（默认自动递增）
- `--branch` — 自动创建 git 分支 `NNN-<slug>` 并切换过去

## 输出位置

- 目录：`specs/NNN-<slug>/`（项目根目录，与 spec-kit 惯例一致）
  - `NNN` 为 3 位零填充编号（001, 002, ...）
  - `<slug>` 是英文短名，从需求中提取，连字符分隔
- 主文件：`specs/NNN-<slug>/spec.md`

**注意**：与项目 `docs/plans/` 的关系——`specs/` 存放进行中的结构化规格；完成后的重要 spec 归档到 `docs/plans/YYYY-MM-DD-<名称>.md`。两者并不冲突。

## 执行流程

### 1. 读取宪法与既有设计

- 读 `.specify/memory/constitution.md`；若不存在，提示用户先运行 `/speckit-constitution`
- 读 `docs/plans/` 下的 taskgate 设计方案与测试方案，判断新需求：
  - 是否已被方案覆盖（指出对应章节，不要重复造 spec）
  - 是否与方案第 10 节"明确不做的（YAGNI）"冲突（冲突就直说，让用户决定是否推翻原决策）
- 吸收其中的强制约束作为后续 spec 的隐含前提

### 2. 确定编号与 slug

- `ls specs/` 找最大编号 N，新编号为 N+1
- 从用户描述抽取 2-4 个英文关键词作 slug（全小写，连字符）
- 创建目录 `specs/NNN-<slug>/`

### 3. 需求澄清（最多 5 个问题）

在写 spec 前，先用**带编号清单**问用户，问题优先围绕库设计的关键分歧点：

- 公开 API 的形态与语义（新方法？新选项？改已有签名？）
- 并发与限流语义（阻塞还是报错？哪个后端范围内保证？）
- 三个后端是否都要支持，还是允许某后端不实现（进不进 Broker 接口的分水岭）
- 失败 / 重试 / 取消语义（占不占 Attempts？终态怎么定？）
- 向后兼容边界（允不允许破坏已有公开 API）

**规则**：
- 最多 5 个问题，问最影响公开 API 或状态机的
- 等用户答完再继续写
- 把 Q/A 原文嵌入 spec 的 `## Clarifications` 段落

### 4. 起草 spec.md

文件结构（所有段落均为**强制**）：

```markdown
# Feature Specification: <功能名>

**Feature Branch**: `NNN-<slug>`
**Created**: YYYY-MM-DD
**Status**: Draft
**Input**: <用户原始需求原文>

## Clarifications

- Q: <问题> → A: <用户回答>
- ...

## User Scenarios & Testing *(mandatory)*

### User Story 1 - <标题> (Priority: P1)

<故事描述：作为使用 taskgate 的开发者，我需要 Y，以便 Z>

**Why this priority**: <...>

**Independent Test**: <如何独立测试这个故事>

**Acceptance Scenarios**:

1. **Given** <前置>, **When** <动作>, **Then** <预期>
2. ...

### User Story 2 - <标题> (Priority: P2)

...（同上结构）

### Edge Cases

- <边界 1，如：进程崩溃、租约过期、并发认领、ctx 取消>
- <边界 2>

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: 库必须 ...
- **FR-002**: ...

### Key Entities

- **<类型 / 接口名>**: <职责>，包含：
  - 字段 / 方法 1（语义与约束）
  - 字段 / 方法 2（语义与约束）
  - 与已有类型（Task / Broker / QueueConfig 等）的关系

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: <可度量的结果，含数字，如"RPS=10 时 1 秒放行 10±1 个">
- **SC-002**: ...
```

### 5. 校验清单（写完自检）

- [ ] User Story 按优先级（P1/P2/P3）排序，P1 即 MVP
- [ ] 每个 Story 都能**独立交付并测试**
- [ ] FR 编号连续，无实现细节
- [ ] Key Entities 的语义约束明确（并发保证、状态流转、默认值）
- [ ] Success Criteria 可度量（有数字或明确阈值）
- [ ] 所有 Clarifications 都有答案（没有 `[TODO]`）
- [ ] 不与设计方案第 10 节的 YAGNI 清单冲突（或已获用户确认推翻）
- [ ] 文件内不出现具体依赖库名、SQL 语句、Redis 数据结构等实现细节（taskgate 已有的公开 API 名可以出现）

### 6. 分支（可选）

如 `$ARGUMENTS` 含 `--branch`：
- `git checkout -b NNN-<slug>`
- 初次创建，从当前分支拉出

## 写入要求

- **中文**输出
- 分批写入长文件，避免 API 中断（先写前 3 段，再续写其余）
- 写完后打印文件路径和下一步提示：`运行 /speckit-plan 生成技术实施计划`

## 启动

1. 先汇报：准备创建 `specs/NNN-<slug>/spec.md`
2. 提出最多 5 个澄清问题（如需）
3. 用户回答后起草 spec
4. 自检清单通过后落盘
