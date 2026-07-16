# Feature Specification: Identity 身份模型(BusinessKey / ExecutionID / Replay)

**Feature Branch**: `005-identity-replay`
**Created**: 2026-07-16
**Status**: Draft
**Input**: Identity 身份模型:把 Task.ID 的双重职责拆开——引入 BusinessKey(业务幂等键,可重复提交)与 ExecutionID(不可复用的执行身份,系统 ulid 生成、用户不可指定),新增 ReplayOf 指针与 Replay 操作(对终态执行创建新 execution 并指回旧的,历史链不分叉),DependsOn 永远引用 ExecutionID,旧 execution 终态后记录不可变。依据《2026-07-16-Identity领域模型.md》(裁决采纳 f 模型)与《2026-07-16-领域模型原型验证记录.md》(原型已通过)。

> 上游依据:`docs/plans/2026-07-16-Identity领域模型.md`(领域模型,已过评审)、`docs/plans/2026-07-16-外部项目调研改进方案.md` 第 6/13 节(裁决 #1)、`prototype/identity/`(五断言 + 规则性拒绝全部验证通过)。本 spec 把模型落成可实现、可验收的功能规格。

## Clarifications

模型文档第 7 节问题清单逐条落定(来源标注:「评审确认」= 2026-07-16 人工评审拍板;「模型倾向」= 模型文档记录的倾向,本 spec 采纳并给出理由;「本 spec 决定」= spec 起草时按宪法与评审提示收敛,属审查重点):

- Q1: Submit 对"键下已存在 execution"是一律拒绝,还是"最新为终态时自动 Replay"? → A: **一律拒绝**(评审确认)。自动重放会让 Submit 语义变成"有时新建、有时重放",重复触发直接变重复执行。
- Q2: Replay 是否允许对 `completed` 执行? → A: **允许,且必须显式参数**(评审确认)。调用方必须显式声明允许重放已完成的执行,挡住误触发重复计费。
- Q3: Replay 的 Payload 复制旧的还是允许新的? → A: **默认复制旧 Payload,允许显式覆盖**(模型倾向,采纳)。"再跑一次同一份作业"是主场景;覆盖入口留给"参数修正后重跑"。
- Q4: 无 BusinessKey 的 execution 是否允许 Replay? → A: **允许**(模型倾向,采纳)。ReplayOf 不依赖业务键;"链不分叉 + 目标必须终态"两条不变式已保证无键链的安全(模型第 4/5 节论证)。
- Q5: `WithID` 的兼容路径? → A: **废弃 `WithID`,新增 `WithBusinessKey`;过渡期 `WithID` 保留为 `WithBusinessKey` 的 Deprecated 别名,行为完全等同**(本 spec 决定)。理由:评审已指出"别名方案"和"废弃方案"破坏面相同(`Get("my-key")`/`DependsOn("my-key")` 都会断),既然破坏面一样,选语义诚实的那个——新代码只见 `WithBusinessKey`,老代码编译不断但 godoc 明说语义已变(键不再是任务 ID,不能拿去 Get/DependsOn)。库尚未对外试点,是改名的最后窗口。
- Q6: 两条不变式在各介质的实现与并发竞态? → A: **实现方式是 plan 的事,spec 只定行为合同**:并发同键 Submit 恰好一个成功;并发 Replay 同目标恰好一个成功;任何介质不得出现"键下两个非终态"或"一个 execution 被重放两次"的可观测状态。
- Q7: Submit 拒绝时错误要不要携带已存在执行的信息? → A: **要**(本 spec 决定)。保留 `errors.Is(err, ErrTaskExists)` 成立,同时错误可经 `errors.As` 取出已存在链尾的 ExecutionID 与状态——否则调用方每次都得按 BusinessKey 再查一圈才能决定要不要 Replay,而"Submit 被拒 → 查状态 → Replay"是 cron 配方的主干路径。

## User Scenarios & Testing *(mandatory)*

### User Story 1 - cron 配方不再死锁:业务键幂等 + 失败后重放 (Priority: P1)

作为用 taskgate 跑每日报表的开发者,我用固定业务键(如 `daily-report:2026-07-16`)防止 cron 多实例重复提交;当这一天的任务重试耗尽进入 `failed` 后,我需要能**再跑一次这份报告**,而不是眼睁睁看着这个键被永久占死、只能去数据库手工改数据。

**Why this priority**: 这是整个功能的起点——调研方案第 6 节判定的"设计遗漏"(README cron 配方在失败后死锁,且全库没有任何补救 API)。不解决它,README 推荐的配方就是个陷阱。

**Independent Test**: 不依赖其他故事,单用 memory 后端即可:同键提交 → 拒绝;失败后 Replay → 新执行入队并正常跑完。

**Acceptance Scenarios**:

1. **Given** 业务键 K 下不存在任何执行, **When** `Submit(..., WithBusinessKey(K))`, **Then** 创建新 execution(系统生成 ExecutionID),任务正常进入调度。
2. **Given** 键 K 下已存在 execution(**不论其状态**), **When** 再次 `Submit(..., WithBusinessKey(K))`, **Then** 拒绝并返回满足 `errors.Is(err, ErrTaskExists)` 的错误,且错误中可取出已存在链尾的 ExecutionID 与状态;原有执行原封不动。
3. **Given** 键 K 的链尾执行 E1 已 `failed`, **When** `Replay(E1)`(或按键 K 指定), **Then** 创建新 execution E2:新 ExecutionID、`ReplayOf=E1`、Attempts/LeaseLost/Throttled 清零、默认复制 E1 的 Payload,进入正常调度;E1 的全部字段逐字不变。
4. **Given** 键 K 的链尾执行 E2 尚未终态(pending/running/…), **When** 对该键或 E2 发起 Replay, **Then** 拒绝——一个业务键同一时刻最多一次在途执行。
5. **Given** E1 已被重放出 E2, **When** 再对 E1 发起 Replay, **Then** 拒绝(链不分叉:每个 execution 至多被重放一次,重放只能打在链尾)。
6. **Given** 链尾执行已 `completed`, **When** 不带"允许重放已完成"显式参数发起 Replay, **Then** 拒绝;**When** 带上该显式参数, **Then** 成功创建新 execution。

### User Story 2 - 执行历史不可变,依赖溯源永远指向当年那次执行 (Priority: P1)

作为依赖任务状态可查、可追溯的使用者,我需要:一次执行终态之后,它的结果、错误、计数永远不被后来的重放改写;子任务 `DependsOn` 引用的父执行,无论父被重放多少次,子任务看到的永远是它当年真正消费的那一次。

**Why this priority**: 这是模型的地基(模型第 3 节规则 5)。没有它,Replay 就退化成被否决的"删除+复用"——静默篡改历史依赖指向。它和 Story 1 是同一次改动的两面,必须同批交付。

**Independent Test**: 构造 E1(completed, 结果 r1)← C(DependsOn=[E1]),对 E1 Replay 得 E2(结果 r2),验证 Get(E1) 与 C 的视角。

**Acceptance Scenarios**:

1. **Given** E1 已 `completed` 且结果为 r1, **When** Replay 产生 E2 并以结果 r2 完成, **Then** `Get(E1)` 返回的 Result/LastError/Attempts/Status/FinishedAt 与重放前逐字段相同。
2. **Given** 子任务 C 的 `DependsOn=[E1]`, **When** E2 存在之后按 `C.DependsOn[0]` 查询, **Then** 得到 E1 与 r1,不是 E2/r2。
3. **Given** E2 存在, **When** 新提交子任务 `DependsOn(E1)`, **Then** 允许(父存在且终态语义照旧),子任务消费的是 r1。
4. **Given** 任何 execution, **Then** 其 ExecutionID 由系统生成(ulid),用户无法指定,永不复用、永不释放;`DependsOn` 只接受 ExecutionID,不接受 BusinessKey。

### User Story 3 - 按业务键回答"这个键现在什么状态" (Priority: P2)

作为 cron 使用方,Submit 被拒后我需要能查询:这个业务键下最新执行是谁、什么状态、历史链有哪些——才能决定要不要 Replay、要不要报警。

**Why this priority**: 没有它,Story 1 的"被拒 → 决定 Replay"闭环走不通(错误携带信息只覆盖 Submit 路径,运维巡检还需要主动查询)。但它是查询能力,晚于写路径交付也不阻塞核心语义。

**Independent Test**: 提交并重放出一条 E1←E2←E3 链,按键枚举历史。

**Acceptance Scenarios**:

1. **Given** 键 K 下有链 E1←E2←E3, **When** 按键查询历史, **Then** 返回 [E1, E2, E3],可枚举且按链序有序,链尾即最新执行。
2. **Given** 键 K 不存在, **When** 按键查询, **Then** 明确的"不存在"结果(不是空链与错误混淆)。
3. **Given** 列表查询(List/Filter), **When** 以 BusinessKey 过滤, **Then** 能筛出该键下全部执行。

### Edge Cases

- **并发同键 Submit**(cron 双触发、网络重试):N 个并发只有一个成功,其余全部 `ErrTaskExists`;不得出现同键两条链头。
- **并发 Replay 同一目标**:恰好一个成功,其余被拒;不得产生两个 `ReplayOf` 指向同一 execution(链分叉)。
- **Replay 与 Submit 赛跑**(同键):任何交错下,键下非终态 execution ≤ 1 恒成立。
- **进程在 Replay 中途崩溃**:新 execution 的创建与 ReplayOf 写入必须原子——不得出现"新执行存在但 ReplayOf 丢失"或"链尾标记更新而新执行不存在"的中间态(宪法 III 同事务/同 Lua 纪律)。
- **无 BusinessKey 的执行**:可以 Replay;链不分叉与"目标必须终态"照常生效;不参与任何键幂等判定。
- **旧数据迁移**:已存在的任务(用 `WithID` 写入的自定义 ID)在升级后语义如何解释——ID 原样保留为 ExecutionID(历史上它确实标识那次执行),无 BusinessKey、无 ReplayOf;不做数据迁移改写。
- **`canceled` 终态**:与 `failed` 同样可无条件 Replay(显式参数仅 `completed` 需要)。
- **Replay 覆盖 Payload 为空 JSON**:显式传了覆盖就用覆盖值(包括空对象),不传才复制旧值——"没传"和"传了空"必须可区分。

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: `Task` 必须新增 `BusinessKey`(可空)与 `ReplayOf`(可空)两个只读字段;`ID` 的语义收紧为 ExecutionID——系统以 ulid 生成,用户不可指定,创建后不可变、永不复用。
- **FR-002**: 库必须提供 `WithBusinessKey(key string)` 提交选项;`WithID` 保留为其 Deprecated 别名(行为完全等同),godoc 写明"键不再是任务 ID,不能用于 Get/DependsOn"。
- **FR-003**: Submit 幂等:带 BusinessKey 提交时,键下存在**任何** execution(不论状态)必须拒绝;不带 BusinessKey 的提交无业务级幂等,每次都是新 execution。
- **FR-004**: Submit 因键冲突被拒时,返回的错误必须同时满足:`errors.Is(err, ErrTaskExists)` 为真;可经 `errors.As` 取出已存在链尾的 ExecutionID 与其状态。
- **FR-005**: 库必须提供 Replay 操作:目标可按 ExecutionID 或 BusinessKey 指定(按键时作用于链尾);成功时创建新 execution——新 ExecutionID、`ReplayOf=旧ExecutionID`、三计数(Attempts/LeaseLost/Throttled)清零、默认复制旧 Payload(允许显式覆盖)、进入正常调度(含 Delay/RunAt 等提交期选项按新提交对待,MaxRetry/Queue/Type 沿用旧执行)。
- **FR-006**: Replay 前置条件三条,任一不满足即拒绝并返回可区分的导出错误:①目标 execution 已终态(completed/failed/canceled);②目标所在键下(或无键时目标自身的链上)不存在非终态 execution;③目标是链尾(不存在 ReplayOf 指向它的 execution)。
- **FR-007**: 重放 `completed` 的执行必须要求显式参数;`failed`/`canceled` 无需。
- **FR-008**: 终态 execution 的记录不可变:Result、LastError、三计数、时间戳、依赖关系不得被任何后续操作(含 Replay)改写。
- **FR-009**: `DependsOn` 只接受 ExecutionID;校验规则维持"父任务必须已存在";子任务不自动跟随重放——引用 E1 的子任务在 E2 出现后看到的仍是 E1 及其结果。
- **FR-010**: 两条不变式在所有后端恒成立且并发安全:①同一 BusinessKey 下非终态 execution ≤ 1(由 FR-003"存在即拒"保证,是其推论);②每个 execution 至多被重放一次(链不分叉)。并发同键 Submit / 并发同目标 Replay 恰好一个成功。
- **FR-011**: 库必须提供按 BusinessKey 的查询能力:枚举该键下的执行历史链(有序,链尾为最新);List/Filter 支持按 BusinessKey 过滤。
- **FR-012**: 新增/修改的全部行为必须先写进 `brokertest` 合规套件再改后端实现(宪法 II.3);现有契约 2(IdempotentID)按新模型重写:幂等判定从"任务 ID"改到"BusinessKey";五后端(memory/sqlite/redis/pg/mysql)过同一套契约。
- **FR-013**: 状态机不变:七状态与既有流转完全沿用,不新增状态;Retry 一词继续专指同一 execution 内的失败重试(Attempts/退避),Replay 是唯一的跨 execution 重跑入口。
- **FR-014**: 升级兼容:既有存量任务的 ID 原样解释为 ExecutionID,BusinessKey/ReplayOf 为空;各后端存储结构的演进不得丢失或改写存量任务数据。

### Key Entities

- **Task(扩展)**: 一次执行(execution)的持久化记录。
  - `ID`(ExecutionID):系统生成 ulid,不可指定、不可复用、不可释放;依赖引用与结果溯源的唯一锚点。
  - `BusinessKey`:用户语义的"这件事";可空;同键下可先后存在多次执行,构成历史链;创建后不可变。
  - `ReplayOf`:本执行由哪次执行重放而来;可空;创建时写入,不可变。
  - 与既有字段的关系:状态机、三计数、租约字段全部不变。
- **Submit 幂等合同**: BusinessKey 存在任何 execution → 拒绝(`ErrTaskExists` + 链尾信息);无键 → 无幂等。
- **Replay 操作**: 终态链尾 → 新 execution(计数清零、Payload 默认复制可覆盖、ReplayOf 回指);`completed` 需显式参数;前置条件三条(终态/无在途/链尾)。
- **历史链**: 同一起点经 ReplayOf 串成的有序链;链不分叉(每 execution 至多被重放一次);链尾至多一个非终态。
- **导出错误**: `ErrTaskExists`(语义扩展:键冲突,携带链尾信息)、Replay 前置条件对应的可区分错误(非终态/非链尾/completed 未显式允许;具体命名 plan 定)。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 原型五断言在正式实现上全部成立(brokertest 化):Replay 后 `Get(E1)` 逐字段不变;`C.DependsOn[0]` 视角不变;`ReplayOf` 正确;历史链可枚举有序;链不分叉拒绝。
- **SC-002**: 并发竞态:同键 100 并发 Submit 恰好 1 个成功、99 个 `ErrTaskExists`;同目标 100 并发 Replay 恰好 1 个成功——五后端(memory/sqlite/redis 离线,pg/mysql 门控)一致,`-race` 下全绿。
- **SC-003**: `brokertest` 重写后的合规套件五后端全部通过;既有 18 条契约中除契约 2 重写外,其余语义不回归。
- **SC-004**: `go build ./...` 与 `go test ./... -race` 全绿;核心包覆盖率 ≥85%、broker 实现 ≥80% 不回落(宪法 V.4)。
- **SC-005**: cron 配方端到端走通:同键二次提交被拒 → 从错误中直接拿到链尾状态(无需额外查询)→ failed 后 Replay → 新执行跑完;README 配方章节据此重写后无"死锁"路径。
- **SC-006**: 升级兼容:用旧结构写入的任务(自定义 ID、无新字段)在新版本可 Get/List/正常调度,零数据迁移脚本。
