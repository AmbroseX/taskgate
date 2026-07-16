# Feature Specification: SQL 后端适配(MySQL / PostgreSQL)

**Feature Branch**: `004-sql-brokers`
**Created**: 2026-07-16
**Status**: Draft
**Input**: 为 taskgate 新增 pgbroker 与 mysqlbroker 两个 Broker 后端,共享 internal/sqlbroker 核心 + 薄壳公开包,通过现成的 brokertest 18 条契约(契约零新增)。Broker 接口一行不改,scheduler/client 零改动。验收:brokertest 在 PG/MySQL 全绿(env 门控)、`go test ./... -race` 本地(无 DSN skip)与 CI(有 DSN 全跑)双绿、新后端覆盖率 ≥80%。已知限制照方案第 10 节。不做:分布式限流(LimiterProvider 不实现)、LISTEN/NOTIFY、迁移框架。

> 事实来源:`docs/plans/2026-07-15-MySQL-PG后端适配方案.md`(下称"方案")。本 spec 只描述"做什么、为什么、给谁用",实现细节留给 plan.md。

## Clarifications

方案文档(经两轮外部审查)已把所有关键分歧点拍定,以下为直接采纳的决策,不再另行追问:

- Q: 两个新后端进不进 Broker 接口?接口要不要改? → A: 全部 15 个方法(含 Init/Close)MySQL/PG 都能以与 sqlite 完全对齐的语义实现,**Broker 接口一行不改**;scheduler/client 零改动(方案第 1、2 节)。
- Q: 要不要新增行为契约? → A: **零新增**。新后端过现成的 brokertest 18 条契约即算合格(方案第 1、9 节)。
- Q: 分布式限流(LimiterProvider)做不做? → A: 第一期**不做**。SQL 后端不实现 LimiterProvider,scheduler 自动退回进程内限流,多进程各限各的(写进已知限制;方案第 8 节)。
- Q: 跨进程即时唤醒(PG LISTEN/NOTIFY)做不做? → A: **不做**(YAGNI)。跨进程新任务感知靠兜底轮询,默认间隔 200ms 可调(方案第 5、10 节)。
- Q: 建表用迁移框架吗? → A: **不引入迁移框架**。沿用 `CREATE TABLE IF NOT EXISTS` 自动建表,但要处理冷启动并发建表竞态(方案第 4.5 节)。
- Q: 两后端各写一份 SQL,还是共享? → A: 共享核心 `internal/sqlbroker` + 两个薄壳公开包 `pgbroker`/`mysqlbroker`,差异用最小 Dialect 接口收口(方案第 3 节方案 A)。
- Q: 本地测试跑不了真库怎么办? → A: 契约测试用环境变量门控(`TASKGATE_PG_DSN`/`TASKGATE_MYSQL_DSN`),缺失即 skip;CI 用 service container 提供真库必跑。已随宪法 v1.2.0 第 V.5 条门控例外批准(方案第 6 节)。
- Q: 先做哪个后端? → A: **PG 先做**(有 `UPDATE ... RETURNING`,形态最接近 sqlite,风险最小),MySQL 跟上(方案第 1、9 节)。
- Q: 有没有对现有公开 API 的破坏? → A: **无**。memory/redis/sqlite 后端零改动,scheduler/client/brokertest 零改动(只加测试接线);纯增量(方案第 3 节"明确不动的")。

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 用 PostgreSQL 做任务后端 (Priority: P1)

作为一个已经在用 PostgreSQL 的服务开发者,我希望 `pgbroker.Open(dsn)` 拿到一个 Broker,把它交给 taskgate 的 scheduler/client,就能像用 sqlite 后端一样入队、认领、完成、失败重试、连锁取消任务,并且**多个进程连同一个 PG 库时,任务不会被两个 worker 同时认领**。

**Why this priority**: PG 支持 `UPDATE ... RETURNING`,代码形态与 sqlite 蓝本最接近,风险最小,是共享核心 `internal/sqlbroker` 的第一个落地验证。PG 通了,MySQL 只是换方言。这是本 spec 的 MVP。

**Independent Test**: 起一个 PG 库,`TASKGATE_PG_DSN` 指向它,`brokertest.Run(t, factory)` 挂上 pgbroker 工厂函数,18 条契约全绿即证明这个故事独立可用——不依赖 MySQL 部分。

**Acceptance Scenarios**:

1. **Given** 一个空 PG 库和一个 pgbroker,**When** Enqueue 一个任务再 Dequeue,**Then** 拿到该任务且状态为 running、带新租约令牌。
2. **Given** 两个进程共享同一 PG 库、同一队列里只有一个可认领任务,**When** 两个进程同时 Dequeue,**Then** 只有一个拿到任务,另一个阻塞等待或超时,绝不出现两个都拿到(认领互斥,靠 `FOR UPDATE SKIP LOCKED`)。
3. **Given** 一个已存在自定义 ID 的任务,**When** 用同一 ID 再 Enqueue,**Then** 返回 `taskgate.ErrTaskExists`(PG 重复键翻译)。
4. **Given** 一棵父子依赖树,**When** 取消根任务,**Then** 触发调用返回前整条传播链收敛,所有后代任务终态一致(同事务连锁取消)。
5. **Given** 一个租约已过期的 running 任务,**When** ReapExpired 运行,**Then** 该任务被回收、计一次 LeaseLost;多个进程各自跑 reaper 时不重复计。

### User Story 2 - 用 MySQL 做任务后端 (Priority: P2)

作为一个用 MySQL 8.0+ 的服务开发者,我希望 `mysqlbroker.Open(dsn)` 拿到一个语义与 PG 后端完全一致的 Broker,过同一套 18 条契约;同时对 MySQL 独有的两个限制(ID/type/queue 长度上限、排序规则大小写敏感)有清晰的入口报错而不是驱动脏错。

**Why this priority**: MySQL 没有 `UPDATE ... RETURNING`,认领要拆两步;且有 VARCHAR(255) 长度上限、必须 utf8mb4_bin 排序规则两个"击穿点",需要额外入口校验和 DDL 保证。它复用 PG 已验证的共享核心,只换方言,故排 P2。

**Independent Test**: `TASKGATE_MYSQL_DSN` 指向一个 MySQL 8.0+ 库,同一套 `brokertest.Run` 挂 mysqlbroker 工厂,18 条契约全绿;另加长度校验用例:Enqueue 一个 256 字符的自定义 ID 返回清晰错误。

**Acceptance Scenarios**:

1. **Given** 一个 MySQL 8.0+ 库和一个 mysqlbroker,**When** 跑完整 18 条契约,**Then** 与 PG 后端表现完全一致(认领拆两步对调用方不可见)。
2. **Given** 一个 mysqlbroker,**When** Enqueue 一个 ID(或 type/queue)超过 255 字符的任务,**Then** 入口返回清晰的长度超限错误,不把请求发给驱动。
3. **Given** MySQL 表以 utf8mb4_bin 排序规则建成,**When** Enqueue 自定义 ID "abc" 和 "ABC" 两个任务,**Then** 二者被视为不同任务,均入队成功(默认 _ci 会误判重复,本后端 DDL 必须避免)。

### User Story 3 - 高并发下依赖传播不丢错、不脏 flaky (Priority: P3)

作为一个在高并发下依赖连锁取消/失败传播的用户,我希望即使多个 goroutine/进程同时往同一片子树打 Cancel/Fail/ReapExpired,数据库层面产生的死锁被自动重试吸收,而不是把 40P01/1213 这类死锁错误漏给我。

**Why this priority**: sqlite 单连接串行天生不死锁,是照抄的最大陷阱;PG/MySQL 是真并发行锁,多行事务必然有死锁窗口。不处理就是"CI 跑 100 次挂 3 次"的脏 flaky。这是方案自认的头号机制,但契约用例基本单 goroutine 顺序跑压不出来,需专项验证,故单列 P3。

**Independent Test**: L3 新增并发 propagate 压测专项——构造一片共享子树,多个 goroutine 同时打 Cancel/Fail/ReapExpired,断言零错误抛出、全部任务终态一致(死锁被重试环吃掉而非漏给调用方)。env 门控,与契约档同一开关。

**Acceptance Scenarios**:

1. **Given** 一片被多个 goroutine 同时操作的共享子树,**When** 并发打 Cancel/Fail/ReapExpired,**Then** 无死锁错误抛给调用方,所有任务终态一致。
2. **Given** 一个正常传播事务,**When** 事务内部经历数据库死锁被重试成功,**Then** 只在整个事务最终成功后才对外发通知,绝不出现"回滚掉的事务已经发过通知"的幻通知。

### Edge Cases

- **进程崩溃(kill -9)**:running 任务的租约到期后被 ReapExpired 回收,计 LeaseLost;复用现有 crash 专项(tcpProxy)。
- **ctx 取消**:database/sql 中断在跑的语句,驱动私有错误漏出时,统一先查 `ctx.Err()`,非空返回 ctx 错误(契约要求)。
- **冷启动并发建表**:多个进程同时首启并发跑 DDL,PG 报 `tuple concurrently updated`/`duplicate key pg_type`——建表外套库级互斥锁 + 容忍重试。
- **MySQL lock wait timeout(1205)**:默认要等 50s 才报,不能和死锁(1213)同等指数重试;会话超时调低 + 1205 单独限重试。
- **连接池打爆**:一堆 worker 阻塞轮询会打爆 `max_connections`,须给保守的最大连接数默认(10),可调。
- **payload/result 超包大小**:MySQL LONGBLOB 受 `max_allowed_packet` 限制(默认 64M),超了驱动报错——不做入口校验(上限是服务器配置,客户端不可知),写进已知限制。
- **断连/重连**:数据库连接中断,复用现有断连专项验证 requeue 与恰好一次语义。

## Requirements *(mandatory)*

### Functional Requirements

**后端形态与接口**

- **FR-001**: 库必须提供 `pgbroker.Open(dsn, opts...)` 与 `mysqlbroker.Open(dsn, opts...)` 两个公开构造函数,各自返回一个满足现有 `Broker` 接口的实例;两个薄壳包各自 blank import 自己的驱动,用户只用 PG 就不把 MySQL 驱动链进二进制。
- **FR-002**: 库必须**不改动** `Broker` 接口的任何方法签名,scheduler/client/brokertest/memory/sqlite/redis 后端零改动(纯增量)。取证:`git diff` 上述文件无改动(仅测试接线除外)。
- **FR-003**: 两个新后端必须共享一份核心实现,后端间差异只通过一个最小的 Dialect 抽象收口;禁止把两份几乎逐字相同的 SQL 复制成两套(违背 DRY,漂移风险)。

**契约合规**

- **FR-004**: 两个新后端必须以与 sqlite 后端完全对齐的语义通过现成的 brokertest 18 条契约,不新增契约。
- **FR-005**: 认领必须互斥——同一任务同一时刻只能被一个 worker 持有;多进程/多连接并发认领时,一个成功另一个不得拿到同一行,且不互相长时间等锁(要求 MySQL 8.0+ / PG 9.5+ 的 SKIP LOCKED)。
- **FR-006**: 认领即加租约,每次认领生成新令牌;Ack/Fail/FinishCanceled/Requeue/Heartbeat 带令牌校验,令牌不符返回 `ErrLeaseLost`(沿用现有契约,与 sqlite 一致)。
- **FR-007**: 任务终态更新(completed/failed/canceled)与子任务唤醒必须在同一个事务内完成;连锁取消/失败传播的对外合同是"触发调用返回前整条传播链收敛"(宪法 III;单事务整树收敛形态)。
- **FR-008**: 三种计数各管一段互不占用:`Attempts` 管业务失败、`LeaseLost` 管 worker 崩溃(默认 3 次封顶)、`Throttled` 管网关限流;Shutdown 的 Requeue 一个都不占(沿用现有契约)。

**并发正确性(SQL 后端新机制)**

- **FR-009**: 写事务必须内置死锁/序列化失败自动重试:事务内报错时判定是否可重试(PG 40P01/40001、MySQL 1213/1205),可重试则回滚后指数退避重跑整个事务函数,上限默认 5 次;超限或不可重试原样返回原始错误。退避走注入 clock,测试可确定。
- **FR-010**: 事务函数必须无副作用可重跑——每轮重新取行再判状态改状态;对外通知(Notify)严禁在重试环内触发,只在事务整体成功返回后才发,防止回滚掉的事务发出"幻通知"。
- **FR-011**: MySQL 的 lock wait timeout(1205)不得与死锁(1213)同等指数重试:连接初始化时把会话级 `innodb_lock_wait_timeout` 调低(如 5s),1205 单独限重试且不长退避,避免单次调用最坏挂死几百秒。
- **FR-012**: 只读路径(Get/List 等)必须走无事务无锁的连接池查询,不得给只读查询加行锁;只有写事务用"取行加行锁(FOR UPDATE)→判状态→改状态"骨架。
- **FR-013**: 数据库错误判定必须用 `errors.As` 到驱动的具体错误类型再看 SQLSTATE/errno,禁止字符串匹配错误文案。

**入口校验与错误(击穿点防护)**

- **FR-014**: Enqueue 撞主键必须翻译成 `taskgate.ErrTaskExists`(PG SQLSTATE 23505 / MySQL errno 1062),供 `errors.Is` 判断。
- **FR-015**: mysqlbroker 的 Enqueue 入口必须显式校验 ID/type/queue 长度,超过 255 字符返回清晰的长度超限错误,不把请求发给驱动(参照 redisbroker validateID 挡控制字符的先例)。PG/sqlite 后端无此限制,不加此校验。
- **FR-016**: MySQL 建表 DDL 必须内置 utf8mb4_bin 排序规则,保证自定义 ID 大小写敏感(默认 _ci 会把 "abc"/"ABC" 判成重复、且排序契约漂移)。

**建表、连接池与配置**

- **FR-017**: 库必须用 `CREATE TABLE IF NOT EXISTS` 自动建表,不引入迁移框架;并处理冷启动并发建表竞态——建表全过程套一把库级互斥锁(锁 + DDL + 解锁钉在同一独占连接上),并对并发 DDL 脏错兜一层容忍重试。
- **FR-018**: 表名与索引名/约束名都必须带可配前缀(默认如 `taskgate_`),供多应用共享同一服务器库时隔离,避免索引裸名撞车(对齐 redisbroker 的 KeyPrefix 思路)。
- **FR-019**: 库必须给最大连接数一个保守默认(10)并可通过 Options 调,防止一堆 worker 的阻塞轮询打爆服务器 `max_connections`。
- **FR-020**: 阻塞 Dequeue 必须复用现有三唤醒源结构(同进程 wake channel / 注入 clock 到点等待 / ctx);跨进程写入靠兜底轮询发现,默认间隔 200ms 且可通过 Options 调。
- **FR-021**: 出错后必须先查 `ctx.Err()`,非空统一返回 ctx 错误(与 sqlite 处理一致),不把驱动私有错误漏给调用方。

**明确不做(YAGNI 边界)**

- **FR-022**: 库**不实现** LimiterProvider——SQL 后端无分布式限流能力,scheduler 对无此能力的后端自动退回进程内限流,零改动;写进已知限制。
- **FR-023**: 库**不实现** PG LISTEN/NOTIFY 即时跨进程唤醒(留作后续可选项),不实现迁移框架。

### Key Entities

- **Broker(现有接口,不改)**:pgbroker/mysqlbroker 实例都实现它;15 个方法语义与 sqlite 完全对齐。与 Task/QueueConfig 的关系维持现状。
- **internal/sqlbroker(新,共享核心)**:基于 database/sql 的通用实现,持有 Dialect、`*sql.DB`、注入 clock、唤醒结构。职责:Init/Close、withTx(含死锁重试环)、wake、scanRec、enqueue/dequeue/lifecycle/propagate/query 的标准 SQL 骨架。不对外公开(internal)。
- **Dialect(新,最小差异抽象)**:只收"两库真正不同"的点——方言名、`?` 占位符改写、各自 DDL、认领语句、重复键判定、可重试错误判定、建表期库级锁/解锁。收口在此接口内,枚举可数。
- **pgbroker(新,薄壳公开包)**:`Open(dsn, opts...)` blank import PG 驱动 + 注入 PG 方言;约 100 行。对外只暴露 Open 和 Options。
- **mysqlbroker(新,薄壳公开包)**:`Open(dsn, opts...)` blank import MySQL 驱动 + 注入 MySQL 方言 + Enqueue 入口长度校验;约 100 行。
- **Options(新,每后端配置)**:TablePrefix、MaxOpenConns(默认 10)、PollInterval(默认 200ms)等;字段带 tag 供应用自己 unmarshal(不在库内读环境变量,宪法 I)。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: brokertest 18 条契约在 PG 后端(`TASKGATE_PG_DSN` 存在时)100% 通过。
- **SC-002**: brokertest 18 条契约在 MySQL 8.0+ 后端(`TASKGATE_MYSQL_DSN` 存在时)100% 通过。
- **SC-003**: 本地无 DSN 时,`go test ./... -race` 全绿(两后端契约档 skip,不算红);CI 注入 DSN 后同一命令全跑全绿。
- **SC-004**: pgbroker、mysqlbroker、internal/sqlbroker 三包各自测试覆盖率 ≥80%(取证 `go test -cover`)。
- **SC-005**: 并发 propagate 压测专项:N 个 goroutine(N≥8)同时对一片共享子树打 Cancel/Fail/ReapExpired,运行结束零错误抛给调用方,全部任务终态一致——重复跑 100 次不出现 flaky。
- **SC-006**: 两进程共享同一库、同一队列单个可认领任务,1000 次并发认领中被同时认领两次的次数 = 0。
- **SC-007**: mysqlbroker Enqueue 一个 256 字符 ID,返回可 `errors.Is`/清晰文案的长度超限错误,且未产生任何数据库写入。
- **SC-008**: `git diff` 显示 broker.go、scheduler.go、client.go、memory/sqlite/redis 后端实现文件零改动(仅 brokertest 测试接线与 go.mod 依赖新增)。
- **SC-009**: `grep -rn` 上层代码对具体后端类型断言:pgbroker/mysqlbroker 不出现在 scheduler/client 的类型特判里(宪法 II.2)。

---

**下一步**:运行 `/speckit-plan` 生成技术实施计划(research 头号问题按序:withTx 死锁重试环结构、占位符不复用纪律、Dialect 边界、认领 SQL、建表互斥、测试门控)。
