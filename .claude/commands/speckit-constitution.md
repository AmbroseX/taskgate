# 项目宪法 — 生成或更新 .specify/memory/constitution.md

你负责为本仓库**沉淀或迭代"项目宪法"**（constitution）——一份**整个项目生命周期都不可违背**的根本原则集合。宪法是后续 `/speckit-specify`、`/speckit-plan`、`/speckit-tasks`、`/speckit-implement` 的评判依据。

## 用户输入

$ARGUMENTS

支持的参数：
- `（空）` — 基于当前代码库自动生成首版宪法（如 `.specify/memory/constitution.md` 已存在，则走"增量修订"流程）
- `<自由文本>` — 要补充 / 修改的原则说明（例如："新增：所有对外错误必须是导出的哨兵错误或错误类型"）
- `bump <major|minor|patch>` — 版本号升级（默认 patch）
- `reset` — 重新生成（危险，会覆盖现有宪法；需二次确认）

## 宪法应包含什么

1. **核心原则**（Core Principles）：3-7 条，每条需有名字、描述、强制等级（MUST / SHOULD）
2. **技术栈与依赖**：明确使用的语言、依赖库、工具链版本
3. **禁止事项**：明确的"不要做什么"
4. **治理规则**：怎么修订本宪法、版本号规则
5. **元信息**：Version（semver）、Ratified 日期、Last Amended 日期

## 本仓库已有的硬约定（宪法必须吸收）

从 `CLAUDE.md` 和 `docs/plans/` 两份方案文档（taskgate 设计方案 v5 + 测试方案）抽取的既有硬规则：

- 所有输出使用中文，解释用大白话
- taskgate 是**轻量、可嵌入的 Go 库**，不是独立服务；禁止加 Web UI、server 模式、DAG 工作流引擎等设计方案第 10 节明确不做的东西
- **Broker 接口是最小公倍数**：只收"所有后端（memory / sqlite / redis）都能以相同语义实现"的方法；后端私有能力不进接口
- **三后端必须通过同一套 `brokertest` 合规套件**：新增行为契约先写进 brokertest，再改各后端实现，防止"签名一样、行为漂移"
- Payload / Result 用 `json.RawMessage`，禁止 `interface{}`
- 三种计数各管一段：`Attempts` 管业务失败、`LeaseLost` 管 worker 崩溃、`Throttled` 管网关限流，互不占用；Shutdown 的 Requeue 一个都不占
- 任务终态更新与子任务唤醒必须在**同一事务 / 同一段 Lua** 里完成（丢唤醒是最大的坑）
- 依赖靠"父任务必须已存在"的提交校验保证无环，不做环检测
- 修改 Go 代码后必须 `go build ./...` 验证；提交前跑 `go test ./... -race`
- 测试离线可跑、结果确定：LLM/OCR 用 httptest mock，真实网关只留 `//go:build realgw` 手动冒烟档
- 时间相关逻辑（租约、退避）走可注入的 clock 接口，测试不真 sleep
- 计划文档写到 `docs/plans/`，命名 `YYYY-MM-DD-功能描述.md`
- 写长文件分批写入，避免 API 中断
- 调试先验证假设再改代码；被用户纠正后彻底放弃旧假设

## 执行流程

### 情况 A：`.specify/memory/constitution.md` 不存在（首次生成）

1. **扫描代码库建立事实基础**
   - `ls` / `find` 列出顶层目录，识别模块
   - 读取 `go.mod` 确定 Go 版本与依赖（若尚未 `go mod init`，以设计方案第 12 节的目录结构为准）
   - 读取 `CLAUDE.md`、`README.md`、`docs/plans/` 中已有的约定与设计决策
   - 浏览已有源码目录（如存在），印证包结构与分层

2. **起草宪法**，至少覆盖：
   - **I. 库的边界**：轻量可嵌入、零强制依赖（sqlite 纯 Go 免 cgo）、设计方案第 10 节的"明确不做"清单
   - **II. Broker 接口纪律**：最小公倍数、brokertest 合规、租约令牌防竞态
   - **III. 一致性与原子性**：终态+唤醒同事务、认领互斥、计数分工
   - **IV. 测试纪律**：L1~L4 分层（单元 → brokertest → 集成 → mock 网关 E2E）、`-race` 全量、覆盖率目标（核心包 ≥85%、broker 实现 ≥80%）
   - **V. 文档纪律**：计划文档路径与命名、spec 归档
   - **VI. 调试与修复**：先验证假设再改代码
   - 技术栈段落：Go 版本、modernc.org/sqlite、go-redis + redis_rate、x/time/rate、ulid 等
   - 治理段落：修改流程、版本规则（MAJOR/MINOR/PATCH）

3. **写入文件**：`.specify/memory/constitution.md`
   - 如 `.specify/` 目录不存在，先创建
   - 版本从 `1.0.0` 开始
   - Ratified / Last Amended 使用今天日期

### 情况 B：`.specify/memory/constitution.md` 已存在（增量修订）

1. 读取现有宪法，理解当前版本号和已有原则
2. 根据 `$ARGUMENTS` 描述的诉求，判断是：
   - **PATCH**：措辞修正、非语义性调整
   - **MINOR**：新增原则、重大扩展
   - **MAJOR**：删除 / 重新定义核心原则
3. 修改对应章节，更新 Version 和 Last Amended
4. 在文件末尾的 `## 修订记录` 追加一行：`vX.Y.Z (YYYY-MM-DD): <变更摘要>`

### 情况 C：`reset`

- 向用户明确确认后，按"情况 A"重新生成（保留原版本号 +1 MAJOR）

## 写入要求

- 文件标题：`# taskgate 宪法`
- 每条原则：`### <编号>. <原则名>`，接一段描述，**必须**标明强制等级（MUST / SHOULD）
- 所有 MUST 条款必须可在代码层面取证验证（用 grep / go vet / go test 能查到违规）
- 禁止空洞原则（"代码要优雅"、"多写注释"不算原则）
- 保持中文

## 启动

1. 先用一句话告知用户"即将生成首版宪法 / 增量修订（PATCH|MINOR|MAJOR）"
2. 列出打算新增 / 修改的原则要点（清单式），让用户确认
3. 用户确认后再写入文件
4. 写完后打印最终版本号与文件路径
