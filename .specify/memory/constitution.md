# taskgate 宪法

> Version: 1.1.0 | Ratified: 2026-07-14 | Last Amended: 2026-07-14
>
> 本宪法是 taskgate 项目全生命周期不可违背的根本原则,是 /speckit-specify、/speckit-plan、/speckit-tasks、/speckit-implement 各环节的评判依据。设计事实来源:`docs/plans/2026-07-14-任务排队限流库taskgate方案.md`(v5)与 `docs/plans/2026-07-14-测试方案.md`。

## 核心原则

### I. 库的边界:轻量可嵌入,不是服务(MUST)

taskgate 是一个 `go get` 即用的 Go 库,不是独立部署的服务。以下事项**禁止实现**(方案第 10 节,MUST NOT):

- Web UI / 管理后台
- cron 周期调度(只支持一次性延迟任务 Delay/RunAt)
- DAG 工作流引擎(静态图、条件分支、循环、父结果自动注入)
- 独立 server 模式
- 库内读取环境变量或配置文件(只收 Config struct;字段带 yaml/json tag 供应用自己 unmarshal)
- 结果长期存储 / TTL 管理(清理交给使用方;被子任务引用的父任务不能清)

依赖必须保持轻量:文件后端用纯 Go 的 modernc.org/sqlite(免 cgo),不得引入需要 cgo 或重型框架的依赖。取证:`grep cgo`、审查 go.mod 新增依赖。

### II. Broker 接口纪律:最小公倍数 + brokertest 合规(MUST)

1. Broker 接口只收"memory / sqlite / redis 三后端都能以相同语义实现"的方法;只有单一后端做得动的能力(如时间窗口统计)不进接口,算后端私有能力。
2. 上层(client/scheduler)只依赖 Broker 接口,禁止对具体后端做类型断言或特判。取证:`grep -rn '\.(\*sqlitebroker\|\*redisbroker\|\*memorybroker' --include='*.go'` 应无命中(broker 实现包自身除外)。
3. **新增或修改任何行为契约,必须先写进 `brokertest` 合规套件,再改各后端实现**;三后端跑同一套题,memory 实现兼任语义参考实现,统计类(Counts/QueueLen)不得偷懒。
4. 认领即加租约,每次认领生成新租约令牌;Ack/Fail/FinishCanceled/Requeue/Heartbeat 必须带令牌校验,令牌不符返回 ErrLeaseLost。

### III. 一致性与原子性:同事务/同 Lua 是生命线(MUST)

1. 任务终态更新(completed/failed/canceled)与子任务唤醒必须在**同一个事务 / 同一段 Lua 脚本**内完成,禁止拆成两步——丢唤醒是本项目最大的坑。
2. 认领互斥:同一任务同一时刻只能被一个 worker 持有(sqlite 靠事务内子查询 UPDATE,redis 靠 BLMOVE 中转 list + Lua 挪 zset)。
3. 三种计数各管一段,互不占用:`Attempts` 管业务失败(超 MaxRetry → failed)、`LeaseLost` 管 worker 崩溃(默认 3 次封顶)、`Throttled` 管网关限流(默认 100 次封顶);Shutdown 的 Requeue 一个都不占。
4. 依赖无环靠提交校验:"所有父任务 ID 必须已存在,否则拒收",禁止实现环检测算法。连锁取消的对外合同是**触发调用返回前整条传播链收敛**;实现允许两种形态:单事务/单临界区内用工作队列收敛整棵子树(memory/sqlite,M1 采用,语义更强),或逐层小事务链式触发+reaper 防御修复兜底(大子树、redis Lua 场景)。禁止的是**无收敛保证的裸递归**与"拆两步不原子"。

### IV. 数据模型纪律(MUST)

1. Payload / Result 一律用 `json.RawMessage`,禁止 `interface{}`。取证:`grep -n 'interface{}' taskgate.go broker.go` 中不得出现在 Task 字段与 Broker 方法签名里。
2. 任务 ID 用 ulid 自动生成,支持自定义 ID 做幂等去重;Enqueue 遇到已存在 ID 必须返回 ErrTaskExists。
3. 对外错误必须是导出的哨兵错误或错误类型(ErrTaskExists、ErrLeaseLost、ErrThrottled、ErrSkipRetry 等),供调用方 errors.Is/As 判断。

### V. 测试纪律:分层、离线、确定(MUST)

1. 测试分层:L1 单元 → L2 brokertest 合规 → L3 集成 → L4 mock LLM/OCR 仿真 E2E → 故障竞态专项;真实网关只留 `//go:build realgw` 手动冒烟档,禁止进 CI。
2. 修改 Go 代码后必须 `go build ./...` 通过;提交前必须 `go test ./... -race` 全绿。
3. 测试必须离线可跑、结果确定:外部 HTTP 一律 httptest mock;时间相关逻辑(租约、退避、限流)走可注入的 clock 接口,测试不真 sleep。
4. 覆盖率目标:核心包 ≥85%,各 broker 实现 ≥80%。取证:`go test -cover`。

### VI. 文档与流程纪律(MUST)

1. 计划文档一律写入 `docs/plans/`,命名 `YYYY-MM-DD-功能描述.md`,提交 git;spec-kit 产物归档在 `specs/NNN-xxx/` 下。
2. 所有输出使用中文,解释用大白话;写长文件分批写入,避免一次性写入被中断。
3. 调试必须先验证假设(加日志/写最小复现)再改代码;被用户纠正后彻底放弃旧假设,不得在旧假设上打补丁。

## 技术栈

| 项 | 选型 | 说明 |
|---|---|---|
| 语言 | Go ≥ 1.25 | 本机 go1.25.0 |
| 模块路径 | github.com/ambrose/taskgate(占位,可改) | 库,非服务 |
| 文件后端 | modernc.org/sqlite | 纯 Go 免 cgo,WAL 模式 |
| Redis 后端 | github.com/redis/go-redis/v9 + go-redis/redis_rate | M2 引入 |
| 单机限流 | golang.org/x/time/rate | 令牌桶 |
| 任务 ID | github.com/oklog/ulid/v2 | 可排序 |
| 测试 | 标准库 testing + httptest;miniredis(M2) | 禁重型测试框架 |

## 禁止事项汇总

- 禁止实现第 I 条列出的六类功能
- 禁止绕过 Broker 接口对具体后端特判
- 禁止把终态更新与依赖唤醒拆成非原子的两步
- 禁止 Task 字段 / Broker 签名使用 `interface{}`
- 禁止测试依赖真实外部服务或真 sleep 控制时序
- 禁止跳过 brokertest 直接给单一后端加行为

## 治理

1. 修订本宪法须通过 `/speckit-constitution` 进行,并在修订记录追加条目。
2. 版本规则:PATCH=措辞修正;MINOR=新增原则或重大扩展;MAJOR=删除/重定义核心原则。
3. spec/plan/tasks 与宪法冲突时,以宪法为准;确需突破时先修宪再动工。

## 修订记录

- v1.0.0 (2026-07-14): 首版,基于 taskgate 设计方案 v5 与测试方案生成。
- v1.1.0 (2026-07-14): MINOR——III.4 连锁取消从"仅逐层小事务"放宽为"调用返回前收敛的两种合法形态"(M1 双后端实测采用单事务收敛,强于原条款;审查发现文档与实现冲突,按"实现语义更强"方向修宪对齐)。
