# Feature Specification: 周期配额 Periodic Quota(硬配额)

**Feature Branch**: `006-periodic-quota`
**Created**: 2026-07-16
**Status**: Draft
**Input**: 周期配额 Periodic Quota:队列级"每个固定时长窗口最多启动 N 次 handler"的硬配额。依据《2026-07-16-Quota领域模型.md》(六项裁决已定)与《2026-07-16-领域模型原型验证记录.md》(sqlite 双进程原型已通过)。核心对象 QuotaReservation;耗尽=停止认领不占 worker 槽、不是错误;介质不可达 fail-closed 零放行。

> 上游依据:`docs/plans/2026-07-16-Quota领域模型.md`(领域模型,已过评审)、`docs/plans/2026-07-16-外部项目调研改进方案.md` 第 5/13 节(裁决 #2~#5)、`prototype/quota/`(四断言全过:双进程不超发/窗口恢复/kill -9 只少不多/介质断连零放行)。
>
> **能力声明(收窄,评审 #7)**:本功能给的是——**在固定时长窗口内,最多启动 N 次 handler**。不是自然日/自然月控制(窗口对齐 epoch,`24h` 即 UTC 零点),不是 token 计量(那需要"执行后上报用量"模型,另一个功能)。两个"配额被消耗但没真正调网关"的漏口写进合同:handler 在真正调网关前失败/被取消、任务类型未注册 handler 被判 FailSkip,都已消耗一次配额。

## Clarifications

模型文档第 8 节问题清单逐条落定(「模型/评审」= 上游文档已给的结论或缓解候选,本 spec 采纳;「本 spec 决定」= 起草时收敛,属审查重点):

- Q1: 退还(released)的原子语句形态;退还与窗口切换赛跑? → A: 语句形态是 plan 的事;spec 只定行为合同——**退还必须只作用于预留时的那个窗口**;窗口已切走时退还落空,落空无害(原型已确认:旧窗口计数随窗口作废)。(模型/评审)
- Q2: 空队列长轮询的白烧节奏? → A: **预留前先查 `QueueLen(queue) > 0` 再预留**(启发式,有竞态不作保证)+ **空扑后退避**;偶尔白烧照走尽力退还,退还失败当 leaked(保守方向)。先例:认领循环本就接受空闲期预烧至多 1 个令牌(M1 SC-001 ±1 容差)。(评审缓解候选,采纳)
- Q3: quota key 配置面? → A: `QueueConfig` 加三字段:`QuotaLimit int`(0=不启用)、`QuotaPeriod Duration`、`QuotaKey string`(默认=队列名)。校验(New 时 fail-fast):启用时 Period 必须 >0;**同一 QuotaKey 出现在多个队列时,其 (QuotaLimit, QuotaPeriod) 必须完全一致,否则报错**。(本 spec 决定;配置放 QueueConfig 是因为配额与 Workers/RPS 同属队列限流参数,反面教材 8.5 禁的是"通用钩子",不是队列配置面)
- Q4: 测试时间缝的形态? → A: spec 只定两条合同:**生产路径的窗口计算用共享介质的服务端时间,不信任应用节点钟**;**测试不真 sleep,通过介质侧注入覆盖时间**(redis 有 ARGV 注时先例;SQL 侧允许语句内"覆盖参数优先、缺省取服务端时间"的形态;memory 的介质就是本进程,注入 Clock 即服务端钟)。具体缝的形态 plan 定。(裁决 #5 + 模型第 4 节)
- Q5: 观测口? → A: **v1 不进 Overview**(YAGNI);`QueueStats` 加两个只读位:`QuotaExhausted`(本窗口额度已尽,等下窗)、`QuotaStalled`(介质不可达,fail-closed 暂停中)。"队列莫名不动"必须能靠 Stats 区分出这两种原因。(本 spec 决定)
- Q6: 基准不过关的回退条件? → A: 基准产出"有配额 vs 无配额"的认领吞吐对比数,写进完成记录;**吞吐损耗 > 30% 时触发对"备选路线"(后端原子 admission/claim)的重新评估**,本期不实现备选路线。(本 spec 决定,阈值供人工复核)
- Q7: 预留插在认领顺序链哪一环? → A: **占并发槽 → 等 RPS 令牌 → 预留配额 → Dequeue**。预留放最后一环使"预留到认领"的窗口最短(白烧/泄漏暴露面最小);耗尽或介质故障时**先释放并发槽**再暂停认领——瞬时持有不算占,与"不占 worker 槽"语义不矛盾(模型问题 #7 的预判,采纳)。(本 spec 决定)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 硬配额:窗口内绝不超发,跨进程共享 (Priority: P1)

作为用 taskgate 调用按量计费 LLM 网关的开发者,我给队列配 `QuotaLimit=5000, QuotaPeriod=24h`,要的是**接同一共享介质的所有进程合计**每窗口最多启动 5000 次 handler——多开几个 pod 不等于配额翻倍,任何故障(崩溃/断连/竞态)只会少放行、不会多放行。

**Why this priority**: "绝不超发"是裁决 #2 定义的产品本体;做不到这一条,这个功能不叫 quota(命名后备:改名 AdmissionBudget)。

**Independent Test**: 两个 Gate 实例共享同一介质(sqlite 同文件),同 quota key,短窗口高压提交,统计每窗口 handler 启动次数。

**Acceptance Scenarios**:

1. **Given** 队列饱和(积压充足)、`QuotaLimit=N, QuotaPeriod=1s`、两个 Gate 共享同一介质与同一 quota key, **When** 连续运行多个窗口, **Then** 每个窗口两实例合计启动 handler 恰好 ≤ N(饱和时 = N),无一窗口超发。
2. **Given** 某实例在"预留成功、认领未完成"之间崩溃/停摆, **Then** 该份额度视同已消耗(leaked),该窗口总放行 ≤ N(只少不多);窗口切换后额度自动恢复,无需任何回收机制。
3. **Given** 多个队列配置同一 `QuotaKey`, **Then** 它们共享同一份窗口预算(合计 ≤ N)。
4. **Given** `QuotaLimit=0`(不启用), **Then** 行为与本功能引入前完全一致,零开销路径。
5. **Given** 窗口边界前后(含多实例), **Then** 窗口起点由共享介质的服务端时间统一计算,不出现"两个节点各持一窗同时放量"。

### User Story 2 - 耗尽行为:停止认领而不是报错,三维度正交 (Priority: P1)

作为使用者,配额用完后我期望:队列安静地停止认领、任务老实待在 pending 等下个窗口;worker 槽不被占着;已在跑的任务不受影响;这**不是错误**——不进 Throttled 计数、不进 failed;RPS 和 Workers 照常各管各的。

**Why this priority**: 耗尽是配额的常态路径(不是异常),行为不对会直接污染任务状态或泄漏并发额度。

**Independent Test**: 单 Gate、`{Workers:2, RPS:0, QuotaLimit:3, QuotaPeriod:1s}`,提交 10 个任务,观察每窗口完成 3 个、其余滞留 pending、计数零污染。

**Acceptance Scenarios**:

1. **Given** 本窗口额度已尽, **When** 认领循环继续运转, **Then** 不再 Dequeue、不把任务认领出来再拒绝;pending/retrying 任务的三计数与状态零变化。
2. **Given** 额度耗尽且队列仍有积压, **Then** 不占用任何 worker 槽等待(瞬时持有后释放);同 Gate 其他队列的认领与执行不受影响。
3. **Given** 耗尽时已有任务在跑, **Then** 在跑任务照常跑完并 Ack/Fail(配额管"放行",不管"中断")。
4. **Given** 下一个窗口开始, **Then** 认领自动恢复,无需人工干预;恢复延迟 ≤ 一次退避间隔。
5. **Given** `{Workers, RPS, Quota}` 同时配置, **Then** 三维度正交生效:并发不超 Workers、速率不超 RPS、窗口累计不超 Quota。
6. **Given** 耗尽状态, **When** 查询 `Stats(queue)`, **Then** `QuotaExhausted=true` 可见;窗口恢复后翻回 false。

### User Story 3 - fail-closed 与装配期强校验:宁可停,不可假 (Priority: P2)

作为运维,共享介质断连时我要的是队列**暂停认领并可观测**,绝不允许退回进程内计数继续放行(那等于配额假保护);配置错误(后端不支持/同 key 参数打架)要在 `New()` 就报错,不要跑起来才发现。

**Why this priority**: "不存在静默降级"是裁决 #3;这是本功能与既有 LimiterProvider 模式的本质区别。

**Independent Test**: sqlite 介质上长持写锁模拟不可达(原型验证过的手法);构造非法配置断言 New 报错。

**Acceptance Scenarios**:

1. **Given** 共享介质不可达(断连/超时/锁死), **When** 认领循环尝试预留, **Then** 该 quota key 下暂停认领、按退避重试;**故障期间放行数为 0**;恢复后自动续上。
2. **Given** 介质故障期间, **When** 查询 `Stats(queue)`, **Then** `QuotaStalled=true` 可见。
3. **Given** 后端不支持共享计数(未实现配额能力)却配置了 `QuotaLimit>0`, **Then** `New()` 直接报错,错误信息写明哪个队列哪项配置不被该后端支持。
4. **Given** 同一 `QuotaKey` 在两个队列上配了不同的 `QuotaLimit` 或 `QuotaPeriod`, **Then** `New()` 报错。
5. **Given** `QuotaLimit>0` 但 `QuotaPeriod<=0`, **Then** `New()` 报错。

### Edge Cases

- **认领扑空的白烧**:预留成功但 Dequeue 扑空(队列被别的实例清空)→ 尽力退还;退还失败当 leaked(保守)。`QueueLen>0` 启发式把常态空轮询的白烧消掉,竞态窗口内的偶发白烧可接受(先例:令牌预烧 ±1 容差)。
- **退还与窗口切换赛跑**:退还只作用于预留时的窗口;窗口已切走 → 落空无害。
- **崩溃在预留后**:leaked,方向保守(US1-2);无 TTL、无后台回收机制。
- **时钟偏差**:应用节点之间时钟差多大都不影响窗口一致性(服务端时间);节点钟只影响退避节奏。
- **配额与 Shutdown**:Shutdown 打断时已预留未认领的份额尽力退还,退不掉当 leaked;Requeue 归还的任务不新增消耗(它没有第二次"放行")。
- **消耗漏口(合同写明)**:handler 启动前的最后一步是认领成功——认领成功即消耗;此后 handler 失败/被取消、类型未注册被判 FailSkip,配额都已消耗,不退。
- **重试与配额**:retrying 任务被重新认领同样要过配额(每次"启动 handler"都是一次消耗)——配额单位是启动次数,不是任务数。
- **memory 后端**:介质=本进程内存,单进程语义下配额照常成立(跨进程本来就不共享任务,谈不上共享配额);fakeclock 即其服务端钟。
- **sqlite 后端**:介质=库文件,能打开它的进程同机同钟,"本机钟"就是服务端钟,跨进程共享成立(原型已双进程验证)。

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: `QueueConfig` 必须新增 `QuotaLimit int`(每窗口最多启动的 handler 次数,0=不启用)、`QuotaPeriod Duration`(窗口时长)、`QuotaKey string`(配额键,空=队列名);字段带 yaml/json tag。
- **FR-002**: 硬配额保证:同一 quota key 在同一窗口内,接同一共享介质的**所有进程合计** handler 启动次数 ≤ `QuotaLimit`;任何故障(崩溃/断连/竞态/退还失败)只能造成少放行,绝不多放行。
- **FR-003**: 窗口为对齐 epoch 的固定时长(`windowStart = now / period × period`),`now` 取共享介质的服务端时间;生产路径不得依赖应用进程本地时钟;不支持自然日/自然月(文档明写)。
- **FR-004**: 预留(QuotaReservation)生命周期:认领前由后端在共享介质内**一个原子操作**完成"检查当前窗口余额 + 扣减";认领成功 → consumed;认领扑空/出错 → 尽力 released(退还只作用于预留窗口,失败即 leaked);进程崩溃 → leaked。无 TTL、无后台回收。
- **FR-005**: 认领顺序:占并发槽 → 等 RPS 令牌 → 预留配额 → Dequeue;预留前先做 `QueueLen>0` 启发式检查;耗尽/故障时释放并发槽后暂停该队列认领,空扑后退避。
- **FR-006**: 耗尽行为:停止认领、不占 worker 槽、不把任务认领出来再拒绝;pending/retrying 任务状态与三计数零变化;不进 Throttled/failed;下窗自动恢复。
- **FR-007**: fail-closed:介质不可达时该 quota key 暂停认领并按退避重试预留;故障期间放行数为 0;绝不退回进程内计数。
- **FR-008**: 配额能力是独立于 `LimiterProvider` 的**可选能力接口**,不复用其"整包替换 + 静默退化"模式;五后端(memory/sqlite/redis/pg/mysql)各自在其介质范围内实现共享计数;`New()` 时后端未实现该能力却配了配额 → 报错(FR-010)。
- **FR-009**: 多队列共享:同一 `QuotaKey` 的多个队列共享同一份窗口预算;三维度(Workers/RPS/Quota)正交并存,互不替代。
- **FR-010**: 装配期校验(`New()` fail-fast):`QuotaLimit>0` 时 `QuotaPeriod` 必须 >0;同一 QuotaKey 的 (QuotaLimit, QuotaPeriod) 跨队列必须一致;后端不支持配额能力时拒绝启动,错误信息含队列名与配置项。
- **FR-011**: 观测:`QueueStats` 新增 `QuotaExhausted bool`(本窗口耗尽)与 `QuotaStalled bool`(介质不可达暂停);两者可经 `Stats(queue)` 查询。
- **FR-012**: 测试合同:所有配额行为测试离线可跑、不真 sleep;窗口时间经介质侧注入覆盖(测试专用缝),生产路径零改动。
- **FR-013**: 基准:提供"有配额 vs 无配额"的认领吞吐对比基准(至少覆盖 sqlite 单写者与同 key 热点争用),结果写进完成记录;损耗 >30% 触发备选路线(后端原子 admission/claim)重评。
- **FR-014**: 文档:README 特性表加"周期配额";"类型级限流"一节讲清**频率 ≠ 配额**(附 LLM 网关组合配置例);能力声明按收窄口径写(固定窗口、非自然日、两个消耗漏口、单位是启动次数不是任务数)。

### Key Entities

- **QueueConfig(扩展)**: `QuotaLimit`(0=不启用,零开销)、`QuotaPeriod`(启用时必填 >0)、`QuotaKey`(默认=队列名;跨队列同 key = 共享预算,参数必须一致)。
- **QuotaReservation(领域对象)**: 一次额度预留;状态 reserved → consumed(认领成功)/ released(尽力退还,只退预留窗口)/ leaked(崩溃或退还失败,视同 consumed);创建必须是介质内单原子操作("检查+扣减"无窗口)。
- **配额能力接口(名称 plan 定)**: 后端可选实现;按 (queue, QueueConfig) 构造配额闸;未实现 + 配了配额 = New() 报错——**没有静默退化路径**。与 `LimiterProvider` 平行且互不影响。
- **QueueStats(扩展)**: `QuotaExhausted` / `QuotaStalled` 两个只读位。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 双 Gate 共享同一 sqlite 库文件、同 quota key、`QuotaLimit=5, QuotaPeriod=1s`、队列饱和:连续 ≥4 个被完整覆盖的窗口,每窗两实例合计启动 handler **恰好 5 次**,无一窗口 >5(原型断言①②的正式化)。
- **SC-002**: "预留后不认领"的故障注入:总放行只少不多(≤N),且下一窗口恢复满额 N(原型断言③的正式化)。
- **SC-003**: 介质不可达期间(锁死/断连注入)放行数为 0;恢复后自动续上;期间 `Stats.QuotaStalled=true`(原型断言④的正式化)。
- **SC-004**: `QuotaLimit=0` 时全量既有测试(22 条契约 + L1~L4)零回归,认领热路径无新增介质往返。
- **SC-005**: `{Workers:2, RPS:3, QuotaLimit:10, QuotaPeriod:1s}` 组合下,e2e 观测:每秒新启动 ≤3、同时在跑 ≤2、每窗累计 ≤10,第 11 个任务在下个窗口才跑。
- **SC-006**: `go test ./... -race` 全绿;核心包覆盖率 ≥85%、broker ≥80% 不回落。
- **SC-007**: 基准数据产出:sqlite 与至少一个服务器型后端(有 DSN 时)的"有/无配额"认领吞吐对比,写入完成记录;若损耗 >30%,完成记录中必须含备选路线重评结论。
