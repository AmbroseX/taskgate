# Feature Specification: taskgate M1 — 核心排队、限流、重试与依赖

**Feature Branch**: `001-m1-core-queue`
**Created**: 2026-07-14
**Status**: Draft
**Input**: taskgate M1:核心接口 + memory/sqlite 后端 + 单机限流 + 重试/死信 + Cancel + reaper + 租约自动续租 + 依赖唤醒 + Shutdown + OnStateChange,依据 docs/plans/2026-07-14-任务排队限流库taskgate方案.md v5

## Clarifications

> 本 spec 是设计方案 v5 的 M1 切片,澄清点全部以方案文档为准,不另行提问:

- Q: 公开 API 形态? → A: 按方案第 5 节:`New(Config)` / `Handle` / `Run` / `Submit` / `Get` / `Cancel` / `List` / `Stats` / `Overview` / `Wait` / `Shutdown`;选项 `WithID/Delay/RunAt/MaxRetry/DependsOn/FailFast/IgnoreParentFailure`。
- Q: M1 支持哪些后端? → A: memory 与 sqlite 两个后端,都必须通过同一套 brokertest;redis 留 M2,但 Broker 接口一次定型(含 redis 也能实现的语义)。
- Q: Dequeue 阻塞语义? → A: 合同统一为"阻塞到有任务或 ctx 取消,实现内部允许轮询";同进程 Submit 条件变量唤醒零延迟,跨进程共用 sqlite 文件靠 100ms 轮询兜底。
- Q: 失败/重试语义? → A: 三计数分工:Attempts(业务失败,超 MaxRetry→failed)、LeaseLost(租约过期回收,默认 3 次封顶)、Throttled(ErrThrottled 重排,默认 100 次封顶);Shutdown 的 Requeue 一个都不占。普通失败指数退避 `min(2^n×1s,10min)±20%`。
- Q: 向后兼容边界? → A: 全新库,无兼容包袱;但 Broker 接口按"三后端最小公倍数"一次定型,M2 加 redis 不允许改接口签名。

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 提交任务并追踪状态与结果 (Priority: P1)

作为使用 taskgate 的开发者,我要异步提交带类型的任务,随时查询"排到没有、跑完没有、结果是啥、错在哪",也能阻塞等待结果,以便把 LLM 调用等耗时工作交给库管理。

**Why this priority**: 这是库存在的意义本身,没有它其余一切无从谈起。

**Independent Test**: 只用 memory 后端,Submit→Wait 拿到 Result,Get 看到完整状态与时间戳链。

**Acceptance Scenarios**:

1. **Given** 已注册 "scoring" handler 并 Run,**When** Submit(ctx,"scoring",payload),**Then** 返回 ulid 任务 ID,任务最终 completed,Get 能读到 Result 且 CreatedAt≤StartedAt≤FinishedAt。
2. **Given** 已提交任务,**When** Wait(ctx,id) 且 ctx 500ms 超时而 handler 睡 1s,**Then** Wait 返回 ctx 错误,任务照常跑完。
3. **Given** 用 WithID 提交了自定义 ID 的任务,**When** 再次用同 ID 提交,**Then** 返回 ErrTaskExists,原任务不被覆盖。
4. **Given** 一批不同 Type/Status 的任务,**When** List(Filter)/Stats(queue)/Overview(),**Then** 过滤结果、单队列水位、Type×Status 矩阵三者相互一致。

### User Story 2 - 按类型隔离限流与队列路由 (Priority: P1)

作为开发者,我要每个任务类型默认独享一条队列和一套 {Workers,RPS,Burst} 限流,也能通过 Config.Routes 把多个类型路由到同一条队列共享网关限额,以便打分、总结、评审互不拖垮,同网关的类型合并占额。

**Why this priority**: 限流是 taskgate 与现成队列库的核心差异点。

**Independent Test**: 两个队列 {Workers:1,RPS:1} 与 {Workers:8},灌满慢队列不影响快队列吞吐;Routes 指到共享队列后纯生产者进程(不注册 handler、不 Run)提交的任务也进对队列。

**Acceptance Scenarios**:

1. **Given** 队列 RPS=10,**When** 一次性灌 100 个任务,**Then** 1 秒内新启动的任务数为 10±1。
2. **Given** 队列 Workers=2,**When** 3 个任务就绪,**Then** 第 3 个必须等前两个之一归还并发槽才启动。
3. **Given** Routes{"review":"xunfei"},**When** 纯生产者进程 Submit "review" 任务,**Then** 任务落入 xunfei 队列,消费者进程能认领执行。
4. **Given** RPS=0 的队列,**Then** 不限速,只受 Workers 并发槽约束。
5. **Given** 非法配置(Workers=0、RPS<0、Routes 指向未配置队列且无 DefaultQueue),**When** New(cfg),**Then** 返回 error 而非 panic。

### User Story 3 - 失败重试、死信与限流特化错误 (Priority: P1)

作为开发者,我要业务失败自动指数退避重试、次数用尽进 failed 死信可查;handler 返回 ErrThrottled 时按 RetryAfter 延后重排不占重试次数;ErrSkipRetry 直接失败,以便网关抽风不把任务打成死信、参数错误不浪费重试。

**Why this priority**: LLM 场景"网关 busy 等重试"与"真失败"必须区分,这是方案的核心特化。

**Independent Test**: handler 前 2 次返回 error 第 3 次成功 → Attempts=3 最终 completed;返回 ErrThrottled{1s} 3 次 → Attempts 不涨、RunAt 每次 +1s、最终成功。

**Acceptance Scenarios**:

1. **Given** MaxRetry=3 的任务,**When** handler 持续失败,**Then** 重试间隔按 `min(2^n×1s,10min)±20%` 递增,第 4 次失败后状态 failed,LastError 保留。
2. **Given** handler 返回 ErrThrottled{RetryAfter:3s},**Then** 任务延后 3s 重排,Attempts 不变,Throttled+1;Throttled 超上限(默认 100)进 failed,LastError 记 "throttled N times"。
3. **Given** handler 返回 ErrSkipRetry,**Then** 任务直接 failed,不再重试。
4. **Given** 任务处于 retrying 且 RunAt 已到,**Then** 可被重新认领(sqlite 认领条件必须包含 retrying)。

### User Story 4 - 崩溃恢复:租约、reaper 与自动续租 (Priority: P2)

作为开发者,我要 worker 进程崩溃后任务不永久卡在 running:租约过期由 reaper 捞回 pending;慢任务运行期间调度器自动续租不被误回收;旧 worker 复活后带过期令牌的 Ack/Fail 被拒绝,以便"至少执行一次"且结果不被旧进程覆盖。

**Why this priority**: 队列库的生命线,但依赖 US1 先存在。

**Independent Test**: 认领后不 Ack,租约过期 ReapExpired 捞回 pending 且 LeaseLost+1;handler 跑 3×LeaseTTL 时长的任务不被回收;过期令牌 Ack 返回 ErrLeaseLost。

**Acceptance Scenarios**:

1. **Given** 任务被认领后 worker 不再心跳,**When** 租约过期且 reaper 运行,**Then** 任务回到 pending,LeaseLost+1;LeaseLost 到上限(默认 3)→ failed,LastError 记 "lease expired N times"。
2. **Given** LeaseTTL=60s、handler 要跑 5 分钟,**Then** 调度器每 LeaseTTL/3 自动 Heartbeat,任务不被 reaper 回收,对 handler 完全透明。
3. **Given** worker A 卡死、任务被回收后 worker B 认领,**When** A 复活调 Ack(旧令牌),**Then** 返回 ErrLeaseLost,B 的执行状态不被覆盖。
4. **Given** 子进程处理任务到一半被 kill -9,**When** 主进程重开同一 sqlite 文件,**Then** 任务被回收重跑最终 completed,LeaseLost=1。

### User Story 5 - 任务依赖:声明父任务,完成自动唤醒 (Priority: P2)

作为开发者,我要 Submit 时用 DependsOn 声明父任务列表,父任务全部结束前子任务 Blocked 不进队列,全部结束后自动翻 pending 并照常受限流;父失败/取消时按 FailFast(默认,连锁取消并逐层传播)或 IgnoreParentFailure(照常唤醒)处理,以便自然串起 LLM 流水线。

**Why this priority**: LLM 流水线刚需,但建立在 US1/US3 之上。

**Independent Test**: A→B→C 三级流水线,A 完成唤醒 B、B 完成唤醒 C;A 失败时 FailFast 的 B、C 连锁 canceled。

**Acceptance Scenarios**:

1. **Given** B DependsOn(A),**When** 提交 B,**Then** B 状态 Blocked 不进队列;A completed 后 B 翻 pending 入队,受所属队列限流。
2. **Given** C 依赖 A+B 两个父(fan-in),**Then** 两个都结束才唤醒;IgnoreParentFailure 时 A 失败 C 照跑。
3. **Given** A failed 且 B 为默认 FailFast,**Then** B canceled 且 LastError 记 "parent <A-id> failed",B 的子任务 C 也连锁 canceled(链式逐层,最终一致)。
4. **Given** DependsOn 引用不存在的任务 ID,**When** Submit,**Then** 拒收报错(天然无环,不做环检测)。
5. **Given** 提交那一刻父任务已全部完成,**Then** 子任务直接 pending 不卡 Blocked;父已 failed/canceled 且策略 FailFast,则子直接 canceled 落库。
6. **Given** 父任务 Ack 与子任务唤醒,**Then** 必须原子完成(同一事务),"父完成但子永远 Blocked"不可出现;reaper 防御性扫描兜底修复。

### User Story 6 - 取消任务 (Priority: P2)

作为开发者,我要 Cancel 任意未终态任务:blocked/pending/retrying 直接 canceled;running 的 handler ctx 被 cancel、退出后以 canceled 落库;取消同样触发依赖传播,以便流水线中途可以叫停。

**Why this priority**: 追踪的配套能力,依赖 US1/US5。

**Independent Test**: Cancel running 任务后 handler ctx 收到取消,任务终态 canceled;Cancel blocked 任务其 FailFast 子任务连锁 canceled。

**Acceptance Scenarios**:

1. **Given** blocked/pending/retrying 任务,**When** Cancel,**Then** 状态直接 canceled,不再被认领;blocked 的取消同样向下传播。
2. **Given** running 任务,**When** Cancel,**Then** 本进程 handler 的 ctx 被 cancel;handler 退出后经 FinishCanceled(带令牌)落库 canceled 并触发依赖传播。
3. **Given** 已到终态(completed/failed/canceled)的任务,**When** Cancel,**Then** 返回错误。

### User Story 7 - 优雅停止 Shutdown (Priority: P3)

作为开发者,我要 Shutdown(ctx):先停止出队与限流放行,等在跑任务干完;ctx 超时未完的 cancel 其 context,handler 退出后任务经 Requeue 放回 pending 且不占任何计数,以便部署重启不消耗任务配额。

**Why this priority**: 生产必需但不阻塞核心链路验证。

**Acceptance Scenarios**:

1. **Given** 3 个任务在跑,**When** Shutdown(5s),**Then** 3 个都 completed 后返回,期间不再认领新任务。
2. **Given** handler 睡 10s,**When** Shutdown(500ms),**Then** 返回超时错误,handler ctx 被 cancel,任务回 pending,Attempts/LeaseLost/Throttled 均不变。

### User Story 8 - 状态变更回调注册口 OnStateChange (Priority: P3)

作为开发者,我要注册 `OnStateChange func(Task)` 回调(默认空实现,同进程内回调),以便埋点监控;webhook 等重语义留 M3。

**Acceptance Scenarios**:

1. **Given** 注册了 OnStateChange,**When** 任务发生状态流转,**Then** 回调收到流转后的任务快照;回调 panic 或阻塞不得影响调度主流程。

### Edge Cases

- 同一任务 100 次并发 Dequeue,只被认领一次(认领互斥)。
- 空队列 Dequeue 阻塞,ctx 取消立即退出。
- 延迟任务 RunAt 未到不出队,到点才出队。
- "父 Ack 事务提交前"进程崩溃:重启后父租约过期→回收重跑→子正常唤醒,不丢唤醒。
- 非法状态流转(completed→running 等)一律拒绝,表驱动全枚举。
- 依赖计数重复递减不得变负数。
- 还被子任务引用的父任务不能清理(文档约束,提交校验依赖父存在)。
- Wait 轮询期间任务被 Cancel → Wait 返回任务终态而非死等。
- sqlite 文件被同机多进程共用(WAL),认领互斥与唤醒仍然成立。

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: 库必须提供统一入口 `New(Config)`,Config 含 Broker、Queues(每队列 {Workers,RPS,Burst,LeaseTTL})、Routes(Type→Queue)、DefaultQueue;New 时校验配置 fail fast。
- **FR-002**: 可序列化配置字段必须带 json/yaml tag;Broker/回调等注入字段打 `yaml:"-"`;提供带 UnmarshalText 的 Duration 类型;库自身禁止读 env 或配置文件。
- **FR-003**: 任务必带 Type;队列名默认等于 Type,Routes 可显式改写;Queue 在入队那一刻定死。
- **FR-004**: 库必须提供 Submit/Get/Cancel/List/Stats/Overview/Wait/Handle/Run/Shutdown 公开 API 及 WithID/Delay/RunAt/MaxRetry/DependsOn/FailFast/IgnoreParentFailure 提交选项。
- **FR-005**: Task 模型含 ID(ulid,可自定义)、Type、Queue、Payload、Status、Result、LastError、Attempts、MaxRetry、LeaseLost、Throttled、RunAt、DependsOn、CreatedAt/StartedAt/FinishedAt;Payload/Result 为 json.RawMessage。
- **FR-006**: 状态机固定为 blocked/pending/running/retrying/completed/failed/canceled 七态,非法流转拒绝。
- **FR-007**: Broker 接口一次定型:Enqueue/Dequeue/Ack/Fail/Cancel/FinishCanceled/Requeue/Heartbeat/Get/List/QueueLen/Counts/ReapExpired/Close;Ack/Fail/FinishCanceled/Requeue/Heartbeat 必须带租约令牌,令牌不符返回 ErrLeaseLost。
- **FR-008**: M1 交付 memory 与 sqlite 两个 Broker 实现;memory 兼任语义参考实现;两者通过同一套 brokertest 合规套件。
- **FR-009**: brokertest 必须覆盖:往返一致、幂等 ID、认领互斥、阻塞语义、延迟任务、Ack/Fail 流转、租约回收与续租、依赖唤醒原子性、连锁取消、Cancel 各状态、Counts 一致性、List 过滤、过期令牌拒绝、retrying 到点可认领。
- **FR-010**: 每队列独立限流:Workers 并发槽 + RPS 令牌桶(Burst 可配),两者独立生效;RPS=0 表示不限速。
- **FR-011**: 失败处理:普通失败指数退避 `min(2^n×1s,10min)±20%` 直至 MaxRetry 进 failed;ErrThrottled 按 RetryAfter 重排不占 Attempts、Throttled 计数封顶(默认 100);ErrSkipRetry 直接 failed。
- **FR-012**: 租约机制:认领即加租约(LeaseTTL 默认 60s,可按队列配置)与新令牌;调度器每 LeaseTTL/3 自动 Heartbeat;reaper 定期 ReapExpired 捞回过期任务,LeaseLost 封顶(默认 3)。
- **FR-013**: 依赖:Submit 校验父任务必须已存在(与落库同事务);终态更新与子任务唤醒同事务原子完成;连锁取消链式逐层触发,禁止一次性递归;reaper 防御性扫描兜底。
- **FR-014**: Cancel 语义按状态区分;running 取消经 FinishCanceled 落库;所有终态(含 canceled)都触发同一段"处理直接子任务"逻辑。
- **FR-015**: Shutdown:停止出队→等待在跑任务→超时 cancel ctx→Requeue 归还不占计数。
- **FR-016**: OnStateChange 注册口 M1 暴露,同进程回调,默认空实现,回调异常不影响主流程。
- **FR-017**: 对外错误必须是导出的哨兵错误/错误类型:ErrTaskExists、ErrLeaseLost、ErrThrottled{RetryAfter}、ErrSkipRetry、ErrTaskNotFound 等。
- **FR-018**: 时间相关逻辑(租约、退避、限流)必须走可注入的 clock,测试不真 sleep。

### Key Entities

- **Task**: 任务记录,字段见 FR-005;三计数分工见 Clarifications;终态后记录保留可查可手动重放。
- **Config / QueueConfig**: 统一配置入口;QueueConfig{Workers≥1, RPS≥0, Burst, LeaseTTL>0};Routes 是生产者消费者共同契约。
- **Broker**: 可插拔存储+排队接口,最小公倍数纪律,见 FR-007;上层只依赖接口。
- **Scheduler**: 每队列 worker 池+限流器,负责认领、执行、心跳续租、重试编排、reaper、Shutdown 编排、Cancel 的 ctx 管理。
- **Client**: Submit/Get/Cancel/List/Stats/Overview/Wait 门面,与 Scheduler 共享 Broker。
- **brokertest**: 行为契约合规套件,`brokertest.Run(t, func() Broker)` 一行接入。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: RPS=10 的队列 1 秒放行 10±1 个任务;Workers=2 时观测最大并发 ≤2。
- **SC-002**: 同一任务 100 次并发 Dequeue 恰好 1 次认领成功;1000 任务双 goroutine 组抢占每任务恰好执行一次。
- **SC-003**: brokertest 全部契约在 memory 与 sqlite 后端全绿;`go test ./... -race` 零 data race。
- **SC-004**: 慢任务(3×LeaseTTL)零误回收;kill -9 崩溃恢复用例 LeaseLost=1 且最终 completed;"唤醒中途崩"用例零丢唤醒。
- **SC-005**: 三级流水线示例(examples)跑通:A→B→C 全 completed,C 的 handler 能 Get 到父 Result;FailFast 连锁取消生效。
- **SC-006**: 语句覆盖率:核心包(scheduler/limiter/deps)≥85%,broker 实现 ≥80%。
- **SC-007**: M1 完成定义达成:L1 全绿 + brokertest 在 memory/sqlite 全绿 + L3 全场景 + 崩溃恢复/唤醒中途崩两个专项。
