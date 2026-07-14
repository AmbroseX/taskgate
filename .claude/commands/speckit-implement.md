# 实现 — 按 tasks.md 执行任务并同步勾选

你负责**按 tasks.md 的顺序落地代码**。每完成一个任务，更新 tasks.md 的勾选状态；每完成一个 User Story，跑编译与测试验证并在 checkpoint 处停下汇报。

## 用户输入

$ARGUMENTS

- `（空）` — 执行当前 spec 下**尚未完成**的下一批任务（默认做完下一个 User Story 就停）
- `mvp` — 只做 Phase 1 + Priority=P1 的所有任务（MVP）
- `phase <n>` — 只做 Phase N
- `us <n>` — 只做 User Story N（如 `us 1`）
- `task <id>` — 只做指定任务（如 `task T007`）
- `continue` — 继续做直到全部完成（不要 checkpoint 停顿；慎用）
- `--dry-run` — 不写代码，只列出将要执行的任务

## 前置检查

1. 定位 `specs/NNN-<slug>/` 目录
2. 必须存在 `tasks.md`，否则提示先运行 `/speckit-tasks`
3. 读取：
   - `.specify/memory/constitution.md`（所有 MUST 约束）
   - `specs/NNN-<slug>/plan.md`（技术上下文、Project Structure）
   - `specs/NNN-<slug>/data-model.md`（类型与约束）
   - `specs/NNN-<slug>/quickstart.md`（关键代码参考）
   - `specs/NNN-<slug>/contracts/`（Broker 接口合同，如存在）
   - `CLAUDE.md`（项目级约定）

## 执行流程

### 1. 确定任务范围

根据 `$ARGUMENTS` 选择要做的任务子集。打印出：
- 即将执行的任务 ID 列表
- 预计修改 / 新建的文件清单
- 停止条件（下一个 checkpoint 或全部完成）

### 2. 按顺序落地

对每个任务：

1. 读取 tasks.md 中该任务的描述和文件路径
2. **并行任务**（标 `[P]`）如确认彼此无依赖，可在一个响应内同时发多个 Write / Edit 调用
3. **顺序任务**必须等前一个完成再做下一个
4. 落地时严格遵守：
   - 宪法的所有 MUST 条款
   - CLAUDE.md 的约定（中文注释、简洁易懂、分批写长文件）
   - plan.md 的 Project Structure（不要创建未规划的新文件）
   - data-model.md 的类型与约束（字段语义、状态流转、计数分工）
   - contracts/ 的接口合同（阻塞语义、错误返回、令牌校验）
   - **Broker 行为变更**：先让 brokertest 契约用例成立，再逐个后端实现，三后端语义必须一致
5. 每个文件写完立即在脑内过一遍宪法清单；发现违规立即修正
6. **完成一个任务后**：用 Edit 将 tasks.md 中对应的 `[ ]` 改成 `[X]`

### 3. Checkpoint 验证

每完成一个 **User Story** 的所有任务，执行：

1. `go build ./...`（必做）
2. `go test ./... -race` 跑该 Story 涉及的包（至少 brokertest 相关后端 + 改动的核心包；慢用例可先 `-short`）
3. 对照 tasks.md 中该 Story 的 `**Checkpoint**` 描述，检查当前状态是否符合
4. 在响应中汇报：
   - 已完成任务 ID 清单
   - `go build` / `go test` 结果
   - 下一步建议（继续下一个 Story / 先让用户验收 / 修复失败）
5. **停下来等用户确认**，除非 `$ARGUMENTS` 是 `continue`

### 4. 失败处理

任何任务失败时：

- **编译或测试失败**：立即停止，汇报错误位置。不要推进下一个任务
- **需要不在 plan 中的文件 / 依赖**：停下，向用户请示（不要擅自扩大范围）
- **发现 data-model 与现有代码冲突**：停下，汇报冲突，让用户决定是改 data-model 还是改方案
- **遇到宪法违规的诱惑**（比如"只在 sqlite 实现、redis 先跳过 brokertest"能省事）：选遵守宪法的那条路，不要走捷径
- **竞态类测试偶发失败**：不要靠重跑糊弄过去，先分析是不是真竞态（`-race` 输出、时序假设），修根因

### 5. 最终收尾

当本次范围内所有任务完成：

1. 最后一次 `go build ./...` && `go test ./... -race` 确认整体通过
2. 汇总：
   - 新增文件清单
   - 修改文件清单
   - 已完成任务数 / 总任务数
   - 剩余未完成任务（如有）
3. 提示下一步：
   - 如还有剩余 Story：`继续运行 /speckit-implement us N 实现下一个 Story`
   - 如全部完成：`运行 /code-review 做代码审查，再运行 /security-review 做安全审查，然后提交`
   - 如重要功能：建议把 spec 目录的关键内容归档到 `docs/plans/YYYY-MM-DD-<名称>.md`

## 执行纪律

- **一次只做一个任务**（除非是 [P] 并行组）；不要跳号
- **禁止扩大范围**：不要顺手做相邻任务、重构无关代码、加"顺便优化"
- **禁止跳过验证**：编译不过、测试不绿不能说"完成"
- **写代码前先看现有模式**：如果项目里已有 `memorybroker/` 的写法，新后端就照这个写；brokertest 已有的用例风格照着延续
- **不要创建 plan.md 未列出的测试文件**（除非 plan 明确要求测试）
- **中文**输出所有汇报和注释

## 启动

1. 一句话汇报：
   - 目标 spec 目录
   - 即将执行的任务子集（列 ID）
   - 停止条件
2. 列出未完成任务清单让用户确认范围
3. 开始执行
