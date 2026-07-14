# Feature Specification: taskgate M3 — 仿真 E2E、手动续租与 List 分页

**Feature Branch**: `003-m3-polish`
**Created**: 2026-07-15
**Status**: Draft
**Input**: taskgate M3 打磨:L4 mock LLM/OCR 网关仿真 E2E(测试方案第 4 节)+ //go:build realgw 真实网关手动冒烟档(第 7 节)+ handler 手动续租 API + List 分页,依据方案 v5 第 11 节 M3 行;优先级调度与 webhook 维持不做(YAGNI)

## Clarifications

> 依据方案 v5 第 10/11 节与测试方案第 4/7 节自答:

- Q: 优先级调度和 OnStateChange webhook 做不做? → A: 不做。方案第 11 节标"(可选)优先级"、第 10 节 webhook 是"M3 再说"——按 YAGNI 维持现状(进程内回调已有),本 spec 明确排除,不算砍需求。
- Q: 手动续租的 API 形态? → A: 不改 Handler 签名、不改 Broker 接口。handler 在自己的 ctx 里拿到续租口子(具体形态 plan 定);自动续租默认保持开启,新增队列级开关允许关掉自动续租改为完全手动(方案第 6 节"handler 想自己控制租约节奏的场景")。
- Q: List 分页的排序与游标? → A: 三后端统一"按创建顺序升序"(任务 ID 是 ulid,天然可排序),Filter 加 Offset(Limit 已有);不做游标分页(YAGNI,单机/中小规模够用,redis 侧代价写进已知限制)。这是 Broker 行为变更,必须先加 brokertest 契约再改三后端。
- Q: mock 网关的随机故障(FailRate)怎么保证 CI 确定性? → A: 注入固定种子的随机源,同种子同序列;所有 L4 用例离线可跑、结果确定(宪法 V.3)。
- Q: 真实网关冒烟进不进 CI? → A: 不进。`//go:build realgw` 隔离,读 LLM_GATEWAY_URL/KEY 环境变量(这是测试读 env,不是库读,宪法允许),手动执行。

## User Scenarios & Testing *(mandatory)*

### User Story 1 - L4 仿真 E2E:mock 网关 + 故障注入 (Priority: P1)

作为 taskgate 的维护者,我要一对可注入故障的 mock LLM/OCR 网关(延迟、并发超限返 busy、随机 500、并发超限断连、200 里藏错误事件),把生产踩过的坑做成开关,验证"限流真的挡住了 busy、ErrThrottled 真的救回了任务、流水线真的端到端跑通",以便每次改动都能在离线 CI 里复演真实故障场景。

**Why this priority**: M3 试点前的最后一道安全网(测试方案第 9 节:"M3 试点前 = L4 全绿");没有它,接真实网关就是裸奔。

**Independent Test**: `go test ./e2e/... -race` 离线全绿,五个核心用例逐一对应测试方案第 4 节。

**Acceptance Scenarios**:

1. **Given** mock LLM(并发>2 返 busy)与队列 {Workers:2},**When** 100 个任务灌入,**Then** mock 端观测最大并发 ≤2 且零 busy 触发;改成 {Workers:5} 后 busy 全部走 ErrThrottled 延后重排,最终零任务 failed。
2. **Given** mock OCR(延迟 2s、并发>4 断连)与队列 {Workers:2},**When** 20 个任务灌入,**Then** mock 不崩、全部 completed;{Workers:4} 时断连错误走普通重试,退避后补完。
3. **Given** ocr→llm-extract→llm-score 三类型三队列各自限流,**When** 10 份文档并行灌入,**Then** 30 个任务全 completed,score 的 handler 能 Get 到 extract 的 Result。
4. **Given** 流水线跑到 ocr 完成、extract 排队,**When** Cancel extract,**Then** score 连锁 canceled,ocr 保持 completed。
5. **Given** mock LLM 返回 200 但 body 是错误事件(SSE 藏错误),**When** handler 判定后返回 ErrThrottled,**Then** 任务按重排路径最终成功——"HTTP 状态码骗人"场景被覆盖。

### User Story 2 - handler 手动续租 (Priority: P2)

作为开发者,我的 handler 在长任务的检查点之间想自己控制租约节奏(比如"每处理完一页就续一次,卡死就让任务尽快被回收"),我要在 handler 的 ctx 里拿到续租口子,并能按队列关掉自动续租,以便毒任务检测更灵敏而正常慢任务不受影响。

**Why this priority**: 方案 M3 行明确列出;自动续租(M1)对"handler 卡死"不敏感——心跳是调度器发的,handler 卡死心跳照跳,任务永不被回收,手动模式补上这个洞。

**Independent Test**: 关自动续租的队列里,handler 定期手动续租则跑完 3×TTL 的任务不被回收;handler 不续租则租约到期被 reaper 回收。

**Acceptance Scenarios**:

1. **Given** 默认队列(自动续租开),**When** handler 内调手动续租,**Then** 调用成功且与自动心跳互不干扰(幂等地延长租约)。
2. **Given** 关掉自动续租的队列、LeaseTTL=短,**When** handler 每 TTL/3 手动续租并跑 3×TTL,**Then** 任务不被回收、最终 completed、LeaseLost=0。
3. **Given** 同一队列,**When** handler 卡死不续租,**Then** 租约到期被 reaper 回收,LeaseLost+1(毒任务防护重新生效)。
4. **Given** 任务已被回收(令牌过期),**When** 旧 handler 调手动续租,**Then** 返回 ErrLeaseLost,handler 可据此尽快退出。
5. **Given** 非任务上下文(普通 ctx),**When** 调手动续租,**Then** 返回明确错误,不 panic。

### User Story 3 - List 分页 (Priority: P2)

作为开发者,我要 List 支持 Offset+Limit 且结果按创建顺序稳定排序,以便在管理界面翻页查看大量任务而不把全库拉回来。

**Why this priority**: 方案 M3 行明确列出;终态任务清理前可能积累大量记录,无分页的 List 会越用越重。

**Independent Test**: 造 25 个任务,Limit=10 翻三页,并集恰好 25 个无重复无遗漏,顺序=创建顺序;三后端行为一致(brokertest 契约)。

**Acceptance Scenarios**:

1. **Given** 25 个任务,**When** List{Limit:10,Offset:0/10/20},**Then** 依次返回 10/10/5 条,按创建顺序升序,三页并集=全集且无重复。
2. **Given** Offset 超出总数,**Then** 返回空列表不报错。
3. **Given** Offset>0 且 Limit=0(不限),**Then** 返回从 Offset 起的全部剩余。
4. **Given** 组合过滤(Type/Status/Queue)+分页,**Then** 先过滤后分页,计数正确。

### User Story 4 - 真实网关手动冒烟档 (Priority: P3)

作为维护者,我要一个不进 CI 的真实网关冒烟测试(构建标签隔离,读环境变量拿网关地址/密钥),以便试点前对真实 LLM 网关跑通一轮:10 个真实抽取任务全部 completed,观察 ErrThrottled 在真实 429 下的行为。

**Acceptance Scenarios**:

1. **Given** 未带构建标签,**Then** 冒烟测试完全不编译进常规 `go test`,CI 零依赖。
2. **Given** 带标签但环境变量缺失,**Then** 测试 skip 并给出提示,不误报失败。
3. **Given** 环境齐备,**When** 10 个真实任务 {Workers:2,RPS:1} 灌入,**Then** 全部 completed(注意 reasoning 模型 max_tokens≥600 的已知坑写进测试注释)。

### Edge Cases

- mock 网关的并发观测必须原子(竞态下的最大并发统计不能少算)。
- FailRate 随机源固定种子,CI 上同序列可复现;失败重试后最终全部成功的断言要给足重试预算。
- 手动续租与自动心跳并发调用:两者都带同一租约令牌,互不冲突(续租是幂等延长)。
- 关自动续租的队列里 Shutdown:在跑任务照常被打断并 Requeue,不因手动模式漏收尾。
- 分页遍历期间有新任务入队/状态流转:不要求快照一致性,只要求"已存在且未变动的任务不丢不重"(写进文档)。
- List 分页在 redis 后端的排序代价(候选集内存排序)写进已知限制。

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: 提供可注入故障的 mock 网关测试组件(延迟、并发上限返 busy、固定种子随机 500、并发上限断连、200 藏错误事件),含原子的并发水位观测;仅用于测试,不进库的公开 API。
- **FR-002**: L4 仿真 E2E 覆盖测试方案第 4 节全部五个核心用例,离线、确定、进 CI(`-race`)。
- **FR-003**: 提供 handler 内可用的手动续租入口:在任务 ctx 上可获取,续租带当前租约令牌,令牌失效返回 ErrLeaseLost;非任务 ctx 调用返回明确错误。
- **FR-004**: QueueConfig 新增队列级开关关闭自动续租(默认关闭=保持自动续租,向后兼容);手动模式下 reaper/毒任务防护语义不变。
- **FR-005**: Filter 新增 Offset;List 结果三后端统一按创建顺序(任务 ID)升序;先过滤后分页;Offset 越界返回空。
- **FR-006**: List 分页作为 Broker 行为变更,先加 brokertest 契约(新用例或扩展 ListFilter),三后端同套全绿。
- **FR-007**: 真实网关冒烟测试以构建标签隔离,不进 CI;环境变量缺失时 skip;库本体仍不读任何 env。
- **FR-008**: 公开 API 只增不改:已有函数签名与 Broker 接口签名零改动(手动续租走 ctx 与新导出函数/选项,Filter 加字段属兼容扩展)。
- **FR-009**: 明确不做:优先级调度、OnStateChange webhook、游标分页(本 spec 记录为有意排除)。

### Key Entities

- **mock 网关(测试组件)**: HTTP 测试服务器 + 故障开关(Latency/BusyAfterConcurrency/FailRate/CrashAfterConcurrency/SSE 错误模式)+ 原子并发观测;放测试目录,不属库 API。
- **手动续租入口**: handler ctx 携带的续租能力;与调度器自动心跳共用租约令牌与 Heartbeat 通道;QueueConfig 开关控制自动心跳是否启动。
- **Filter**: 既有 {Type,Queue,Status,Limit} + 新增 Offset;排序合同=创建顺序升序。
- **brokertest**: 新增/扩展分页契约;三后端同套。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: L4 五用例全绿且离线:{Workers:2} 档 mock 观测最大并发 ≤2、busy=0;{Workers:5} 档零 failed;三队列流水线 30/30 completed。
- **SC-002**: 手动续租:关自动续租+手动续租跑 3×TTL 零误回收;不续租则被回收(LeaseLost+1);过期令牌续租返回 ErrLeaseLost。
- **SC-003**: 分页契约在 memory/sqlite/redis 三后端全绿:25 任务 3 页并集无重无漏、顺序=创建序。
- **SC-004**: `go test ./... -race -count=1` 全量(含 L4)全绿连跑 3 遍;覆盖率不低于 M2 水平(核心 ≥85%、各后端 ≥80%)。
- **SC-005**: realgw 档:常规构建零引入(grep 构建标签取证);带标签+环境齐备时 10/10 completed(手动执行,不计 CI)。
