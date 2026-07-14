# Research: taskgate M1

> 大部分决策已在设计方案 v5 定案,此处记录 M1 编码期仍需拍板的实现级决策。

## 1. sqlite 认领语句写法

**问题**: 如何在 sqlite 上实现"认领互斥 + 包含 retrying + 不依赖编译开关"。

**研究结果**: `UPDATE ... ORDER BY ... LIMIT` 是 SQLite 可选编译特性(SQLITE_ENABLE_UPDATE_DELETE_LIMIT),modernc.org/sqlite 是否开启不可赌;子查询写法是标准 SQL,任何编译都支持。

**Decision**: 事务内
```sql
UPDATE tasks SET status='running', lease_token=?, lease_until=?, started_at=?, ...
WHERE id = (SELECT id FROM tasks WHERE queue IN (...) AND status IN ('pending','retrying')
            AND run_at <= ? ORDER BY run_at, created_at LIMIT 1)
RETURNING *;
```
配合 `BEGIN IMMEDIATE` 写事务,天然防重复认领。

**Rationale**: 方案 v5 第 7.1 节定死;RETURNING 让认领一条 SQL 完成。

**Alternatives considered**:
- SELECT 再 UPDATE 两步:两个事务间有竞态窗口,必须再加 WHERE status 复核,复杂且易错。
- `UPDATE ... LIMIT`:依赖编译开关,排除。

## 2. Dequeue 阻塞的实现手段

**问题**: 接口合同是"阻塞到有任务或 ctx 取消",memory/sqlite 各怎么实现。

**Decision**: memory 用 sync.Cond(Enqueue/到点/回收时 Broadcast);sqlite 用"事务认领一次 + 无果则等 100ms 或唤醒信号"循环,同进程 Enqueue 后主动踢一下内部 channel,跨进程靠 100ms 轮询兜底。

**Rationale**: 方案 v4 审查点 4 定案;brokertest 只测语义不测手段。

**Alternatives considered**:
- sqlite data_version PRAGMA 侦测变更:仍要轮询,徒增复杂度。

## 3. 延迟任务与 retrying 到点的驱动

**问题**: RunAt 未到的 pending/retrying 任务谁来"到点放行"。

**Decision**: 不做独立 delayed 结构(那是 redis 的事)。sqlite/memory 认领条件本身带 `run_at <= now`,到点自然可被认领;Dequeue 轮询循环的等待时间取 `min(100ms, 最近的 run_at - now)`。

**Rationale**: 单机后端里"到点"就是查询条件,无须搬运状态;Redis 后端 M2 再用 delayed zset。

**Alternatives considered**:
- 后台 goroutine 定期把到点任务"翻状态":多一次写、多一处竞态,无收益。

## 4. 自动续租与取消检查的载体

**问题**: 心跳 goroutine 放 scheduler 还是 broker?跨进程取消怎么发现?

**Decision**: scheduler 为每个在跑任务起一个心跳 goroutine,每 LeaseTTL/3 调 `Heartbeat(id, token)`;Heartbeat 返回 ErrTaskCanceled(任务已被外部 Cancel 标记)时 cancel 该任务的 ctx——跨进程取消靠这条通道生效(M1 单机时同进程 Cancel 直接查 cancel func 即时生效,Heartbeat 路径作为兜底,也为 M2 铺路)。

**Rationale**: 方案第 8 节"别的进程等下一次 Heartbeat 发现取消标记";把发现逻辑挂在本来就要跳的心跳上,零新增轮询。

**Alternatives considered**:
- broker 内部自动续租:broker 不知道任务还在不在跑,续租决策必须在持有 handler goroutine 的 scheduler。

## 5. 依赖唤醒的代码复用方式

**问题**: "终态→找子→减计数→唤醒/取消"两个后端都要做,怎么不写两遍还保住原子性。

**Decision**: deps.go 提供纯函数(如 `NextActions(task, children) []Action`)只做决策不做 IO;每个后端在自己的事务/锁临界区内执行这些 Action。原子性边界留在后端,公共逻辑只有纯计算。

**Rationale**: 宪法 III 要求同事务;若公共代码做 IO 就会诱导拆两步。

**Alternatives considered**:
- 模板方法(公共代码回调后端钩子):控制流倒置,事务边界看不清,排除。

## 6. 状态机校验的落点

**问题**: 非法流转(completed→running 等)在哪一层拒绝。

**Decision**: taskgate.go 提供 `canTransition(from, to Status) bool` 表驱动函数,broker 实现层在每次状态写入前调用;brokertest 枚举全表验证两后端一致。

**Rationale**: 单一事实来源,两个后端共享同一张表。

## 7. Wait 的实现

**问题**: Wait 阻塞等结果,轮询还是订阅?

**Decision**: M1 用内部轮询 Get(间隔 50ms,走注入 clock),终态即返;OnStateChange 已有注册口但先不做多订阅分发,避免过度设计。

**Rationale**: 方案第 5 节明说"内部轮询 Get";YAGNI。

## 8. OnStateChange 回调的执行位置

**问题**: 谁触发回调?broker 层还是 scheduler 层?

**Decision**: 状态流转发生点分散在 broker(Enqueue/Ack/Fail/Cancel/唤醒)与 scheduler(认领后 running)。统一做法:broker 构造时注入 `notify func(Task)`(由 New 装配,默认空),每次成功的状态写入后、锁/事务之外异步调用(recover 包住)。

**Rationale**: 回调 panic/阻塞不得影响主流程(spec US8);事务外调用避免回调拖长临界区。

**Alternatives considered**:
- 只在 client/scheduler 层回调:漏掉依赖唤醒等 broker 内部流转,排除。

## 9. 模块路径

**Decision**: `github.com/ambrose/taskgate`(占位,发布前可改);M1 不发布,仅本地。

## 10. kill -9 崩溃测试的实现

**Decision**: `crash_test.go` 用 `os/exec` 以 `-test.run=TestCrashHelper` + 环境变量开关的子进程模式跑 worker,处理到一半 `Process.Kill()`;主测试重开同一 sqlite 文件,断言 reaper 回收、LeaseLost=1、最终 completed。"唤醒中途崩"用注入点(仅测试构造:父 Ack 事务提交前 panic 的 hook)验证重启后不丢唤醒。

**Rationale**: 测试方案第 5 节;标准 Go 子进程测试模式,离线可跑。
