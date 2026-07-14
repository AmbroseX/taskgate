# Feature Specification: taskgate M2 — Redis 后端、分布式限流与多进程能力

**Feature Branch**: `002-m2-redis`
**Created**: 2026-07-14
**Status**: Draft
**Input**: taskgate M2:Redis 后端(BLMOVE 中转+Lua 认领/流转/唤醒)+ 分布式限流(redis_rate GCRA + Redis 计数信号量)+ Stats,依据方案 v5 第 7.2/6/11 节与测试方案 M2 完成定义

## Clarifications

> 依据方案 v5 第 6/7.2/8/11 节与测试方案第 2/5/6/9 节自答,不另行提问:

- Q: Broker 接口允许改吗? → A: 不允许(宪法 II + M1 定型)。redis 后端必须在现有 14 个方法签名内实现;M1 的 17 条 brokertest 契约一条不少全部要过。
- Q: 契约测试用什么 Redis? → A: 双档:miniredis 保 CI(离线、可控时间),真 Redis 档由环境变量 `TASKGATE_REDIS_ADDR` 门控(保 Lua 脚本兼容性),没有该变量自动跳过。
- Q: 分布式限流的语义范围? → A: 同一队列的 Workers(并发槽)与 RPS 在**所有接同一 Redis 的进程间共享**;单机后端(memory/sqlite)维持进程内限流不变。公开 Config 不变,限流器实现按后端能力切换。
- Q: 跨进程取消怎么生效? → A: 沿用 M1 合同:Cancel 打标记,持有任务的进程在下一次 Heartbeat(LeaseTTL/3)发现 ErrTaskCanceled 后 cancel handler ctx——M1 的机制天然跨进程,M2 只需验证,不改语义。
- Q: 时间窗口统计做不做? → A: 不做(方案 7.2 诚实的限制)。redis 后端只有累计计数+当前水位;Counts/QueueLen 必须 O(1) 读取。
- Q: 基准测试? → A: 按测试方案第 6 节建基线(Enqueue/DequeueAck/Pipeline 三项,sqlite vs redis),首跑数字写回测试方案该节,不定死目标。

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Redis 后端一行切换,契约不打折 (Priority: P1)

作为使用 taskgate 的开发者,我要把 Broker 构造从文件后端换成 Redis 后端一行完成,其余代码零改动,且所有行为契约(认领互斥、租约令牌、依赖唤醒、连锁取消、计数)与单机后端完全一致,以便多实例部署时无痛切换。

**Why this priority**: M2 的存在意义;契约一致性是宪法生命线。

**Independent Test**: brokertest 全部 17 条契约在 miniredis 上全绿;设 `TASKGATE_REDIS_ADDR` 后在真 Redis 上同样全绿。

**Acceptance Scenarios**:

1. **Given** M1 的任何使用代码,**When** 只把 Broker 构造换成 Redis 后端,**Then** 编译通过、行为不变(L3 集成测试参数化加 redis 档全绿)。
2. **Given** miniredis 环境,**When** 跑 brokertest,**Then** 17/17 契约通过,含过期令牌拒绝、retrying 到点认领、Notify。
3. **Given** 真 Redis(env 门控),**When** 跑同一套契约,**Then** 全绿(Lua 脚本在真实服务器上兼容)。
4. **Given** 未设 `TASKGATE_REDIS_ADDR`,**Then** 真 Redis 档测试自动 skip,CI 不依赖外部服务。

### User Story 2 - 多进程抢任务恰好执行一次 (Priority: P1)

作为开发者,我要多个 worker 进程接同一个 Redis 抢同一批任务时,每个任务恰好被执行一次;任一进程 kill -9 后其在跑任务(含认领中途滞留的)被回收重跑,以便水平扩容不丢不重。

**Why this priority**: 分布式正确性,方案 M2 验收原文。

**Independent Test**: 双进程灌 1000 任务,执行记录恰好 1000 条且无重复;kill -9 专项含"认领两步之间崩溃"窗口。

**Acceptance Scenarios**:

1. **Given** 两个 worker 进程共用真 Redis/miniredis,**When** 灌 1000 个任务,**Then** 每个任务恰好执行一次(handler 写执行记录,总数 1000、无重复 ID)。
2. **Given** 进程 A 认领任务后 kill -9(不 Ack 不心跳),**Then** 租约过期后任务被回收,LeaseLost+1,由存活进程重跑至 completed。
3. **Given** 认领过程中任意时刻进程崩溃,**Then** 任务要么仍可被认领、要么已带租约(由 reaper 按租约回收),不存在"取走了但没记租约"的第三种状态(实现可用原子认领消灭该窗口,或用中转区+reaper 扫描覆盖)。
4. **Given** Redis 短暂断连后恢复,**Then** worker 报错重连后继续消费,任务不丢(最终全部 completed)。

### User Story 3 - 分布式限流:多进程共享配额 (Priority: P2)

作为开发者,我要同一队列的 Workers 并发槽与 RPS 在所有进程间共享(网关限的是总量,不是单进程量),以便加机器不等于把网关打爆。

**Why this priority**: 类型级限流是 taskgate 核心卖点,多进程下不共享就是假限流。

**Independent Test**: 两进程同队列 {Workers:2},观测到的全局最大并发 ≤2;{RPS:10} 双进程 1 秒总放行 10±2。

**Acceptance Scenarios**:

1. **Given** 两个进程消费同一队列 {Workers:2},**When** 灌 20 个慢任务,**Then** 任意时刻全局在跑数 ≤2(handler 上报并发水位验证)。
2. **Given** 两个进程 {RPS:10},**When** 灌 100 个任务,**Then** 1 秒窗口内全局新启动数 10±2。
3. **Given** 持有并发槽的进程崩溃,**Then** 槽在有限时间内自动释放(不永久泄漏,后续任务能继续跑)。
4. **Given** 单机后端(memory/sqlite),**Then** 限流行为与 M1 完全一致(进程内),零回归。

### User Story 4 - 跨进程流水线与跨进程取消 (Priority: P2)

作为开发者,我要依赖唤醒跨进程生效(进程 A 只跑 ocr 队列、进程 B 只跑 llm 队列,A 完成的父任务能唤醒 B 消费的子任务),取消请求对别的进程持有的 running 任务也能生效,以便流水线按队列分工部署。

**Independent Test**: 跨进程三级流水线全 completed;进程 A 持有的任务被进程 B 发起 Cancel,在心跳周期内退出且落 canceled。

**Acceptance Scenarios**:

1. **Given** 进程 A 消费 "ocr" 队列、进程 B 消费 "extract" 队列,**When** 提交 ocr→extract 依赖链,**Then** A 完成父任务后 B 的子任务被唤醒执行,最终全 completed。
2. **Given** 任务在进程 A 上 running,**When** 进程 B 调 Cancel(id),**Then** A 的 handler ctx 在一个心跳周期(LeaseTTL/3)内被 cancel,任务终态 canceled 并触发传播。
3. **Given** 唤醒发生时子任务所属队列的消费者正阻塞在 Dequeue,**Then** 唤醒后子任务能被及时认领(阻塞出队对"别的进程写入"也生效,允许轮询级延迟)。

### User Story 5 - Stats O(1) 与性能基线 (Priority: P3)

作为开发者,我要 Redis 后端的 Overview/Stats/QueueLen 是 O(1) 读取(计数器随状态流转原子维护,不全量扫描),并要一份 sqlite vs redis 的性能基线数字防退化。

**Acceptance Scenarios**:

1. **Given** 任意一串任务操作,**When** Counts,**Then** 与逐个 Get 汇总一致(契约 13 在 redis 上同样成立),且实现为计数器读取而非全量扫描。
2. **Given** 基准测试跑完,**Then** Enqueue/DequeueAck/Pipeline 三项基线数字写回测试方案第 6 节,后续 PR 对比 ±20%。

### Edge Cases

- 认领过程中崩溃:不得出现"离队却无租约"的悬空态(US2-3)。
- 令牌桶/信号量所在的 Redis 与任务存储是同一实例:限流键与任务键共存,flushdb 级故障两者同生共死,可接受。
- 多队列 Dequeue:一个 worker 进程监听多条队列时,阻塞出队对任一队列的新任务都要响应。
- Lua 脚本在 miniredis 与真 Redis 的行为差异:契约双档跑,差异即 bug。
- 依赖唤醒的原子性在 redis 上同样成立:终态+唤醒同一段 Lua,"父完成子未唤醒"不可观测(防御修复兜底)。
- fakeclock 与 Redis:契约套件的时间推进机制在 redis 后端也要可用(实现层决定怎么注入,套件语义不变)。
- 断连中的 Heartbeat 失败不得误判为 ErrLeaseLost(网络错误与令牌拒绝要区分,M1 调度器已区分,redis 后端错误返回要配合)。

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: 提供 Redis 后端构造函数(地址/密码/db 入参),返回值实现 Broker 接口;接口签名零改动。
- **FR-002**: Redis 后端通过 brokertest 全部 17 条契约,miniredis 档进 CI,真 Redis 档由 `TASKGATE_REDIS_ADDR` 门控。
- **FR-003**: 认领路径不得存在"任务已离开队列但尚未持有租约"的崩溃可达状态;若实现存在中转区,滞留超时任务必须由 ReapExpired 回收。
- **FR-004**: 所有状态流转(含终态+依赖唤醒、连锁取消、计数维护)在服务端原子执行;"父完成但子未唤醒"不可被外部观测到。
- **FR-005**: 每次状态流转原子维护 Type×Status 计数器;Counts/QueueLen 为 O(1) 读取,与逐个 Get 汇总一致。
- **FR-006**: 多进程共享限流:同一队列的 Workers 并发槽与 RPS 配额为全局语义;进程崩溃持有的并发槽在有限时间内自动回收。
- **FR-007**: 单机后端(memory/sqlite)的限流、调度行为零回归;限流实现按后端能力选择,公开 Config 不变。
- **FR-008**: 跨进程依赖唤醒、跨进程 Cancel(心跳周期内生效)、多队列阻塞出队均正确工作。
- **FR-009**: Redis 断连:Dequeue/回执/心跳报错后可重试恢复,不丢任务、不重复执行(令牌机制兜底)。
- **FR-010**: 基准测试:Enqueue(单/32 并发)、DequeueAck 空转、三级 Pipeline 吞吐,sqlite vs redis,基线数字回写测试方案第 6 节。
- **FR-011**: 新增依赖限定:Redis 客户端、Redis 限流库、miniredis(仅测试);不得引入其他依赖。
- **FR-012**: 运维可观测:队列积压可用 Redis 原生命令直接查看(键名设计写进文档)。

### Key Entities

- **redisbroker.Broker**: Broker 接口的 Redis 实现;认领=阻塞取任务+原子记租约两步,崩溃窗口由 reaper 覆盖;所有流转走服务端原子脚本。
- **分布式限流器**: 与 M1 进程内限流器同一抽象下的第二个实现;Workers=全局计数信号量(带过期自动回收),RPS=全局速率算法;scheduler 按后端能力装配,对 handler 透明。
- **计数器**: Type×Status 累计计数,随流转原子增减,供 Overview;队列积压计数供 Stats/QueueLen。
- **brokertest**: 不变;新增后端只是多一个 factory 接入(契约语义一字不改)。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: brokertest 17/17 在 miniredis 与真 Redis(env 门控)双档全绿;`go test ./... -race` 全绿且不依赖外部服务。
- **SC-002**: 双进程 1000 任务恰好各执行一次(0 丢失、0 重复);kill -9(含认领中途)后任务 100% 被回收重跑。
- **SC-003**: 双进程 {Workers:2} 全局并发峰值 ≤2;双进程 {RPS:10} 1 秒全局放行 10±2;进程崩溃后并发槽在 ≤2×LeaseTTL 内可再用。
- **SC-004**: 跨进程流水线(分队列部署)全 completed;跨进程 Cancel 在一个心跳周期内生效。
- **SC-005**: 单机后端全部 M1 测试零回归;覆盖率:redisbroker ≥80%,核心包维持 ≥85%。
- **SC-006**: 三项基准基线数字入档测试方案第 6 节。
