# Research: Identity 身份模型

## 1. Replay 放哪一层:Broker 接口方法,不做 client 组合

**问题**: Replay = "校验目标终态/链尾 + 创建新执行 + 写 ReplayOf",这套动作放 Gate 层用 Get+Enqueue 组合,还是进 Broker 接口?

**研究结果**: FR-010 要求"并发同目标 Replay 恰好一个成功"。client 组合(先 Get 校验再 Enqueue)在校验与创建之间有窗口:两个进程同时过校验、各建一个新执行 → 链分叉。多进程后端(redis/pg/mysql)无法靠进程内锁堵这个窗口,必须在共享介质里原子完成——这正是宪法 III"同事务/同 Lua 是生命线"的场景。

**Decision**: Broker 接口新增 `Replay(ctx, req ReplayRequest) (*Task, error)`(15 → 16),五后端各自用自己的原子手段实现。

**Rationale**: 五后端都有现成的原子手段(memory 单锁、sqlite/sqlbroker 事务、redis Lua),满足宪法 II.1"最小公倍数";而且即便应用层校验有洞,存储级唯一约束(见决策 2)仍是最后防线。

**Alternatives considered**:
- Gate 层 Get+校验+Enqueue 组合:多进程下有竞态窗口,链会分叉,否决;
- 能力接口(类似 LimiterProvider,可选实现):Replay 是核心语义不是可选能力,"某后端没有 Replay"会让 cron 配方在该后端继续死锁,否决。

## 2. 两条不变式的落法:存储级唯一约束兜底,应用层检查只为报错友好

**问题**: "同键非终态 ≤1"与"每个执行至多被重放一次"在并发下怎么保证?

**研究结果**: 两条不变式可以**改写成两条"至多一行"约束**,直接用唯一索引表达:

1. **链头唯一**:带键 Submit 拒绝的判据是"键下存在任何执行"。每条链恰好有一个链头(`replay_of` 为空的那条),Submit 只创建链头 → 唯一约束"同 `business_key` 下 `replay_of` 为空的行至多一条"精确等价于"键下至多一条链"。并发 Submit 双双通过应用层检查时,第二个 INSERT 撞唯一索引失败。
2. **重放来源唯一**:链不分叉 = 至多一个执行的 `replay_of` 指向同一目标 → 唯一约束"非空 `replay_of` 值唯一"。并发 Replay 同目标,第二个 INSERT 撞索引。

各介质落法:sqlite/PG 用**部分唯一索引**(`WHERE replay_of = ''` / `WHERE business_key <> ''`);MySQL 不支持部分索引,用**生成列**(非链头/空值时为 NULL,MySQL 唯一索引允许多个 NULL)+ 唯一索引;redis 用**单段 Lua**(检查与写入同脚本,天然串行);memory 用现有单锁。

**Decision**: 应用层(事务内 SELECT / Lua 内 EXISTS)先检查、给出友好错误;唯一约束兜底竞态,撞约束时翻译回同一个导出错误。

**Rationale**: "检查+约束"双层是 SQL 系统处理幂等冲突的标准做法;约束保证正确性,检查保证错误信息质量(能带出链尾 ExecutionID 与状态)。

**Alternatives considered**:
- 只靠应用层检查 + SELECT FOR UPDATE 串行化:MySQL/PG 可行但 sqlite 无 FOR UPDATE(单写者天然串行,倒不需要)、redis 无此概念,五后端手段不齐;唯一约束是五介质里表达力最一致的;
- 每键一行的"链元数据表"加行锁:多一张表、多一份一致性负担,YAGNI。

## 3. Replay 前置条件②(键下无在途)由①+③推出,不单独实现扫描

**问题**: 模型列了三条前置:①目标终态;②键下无非终态执行;③目标是链尾。②要不要扫全链?

**研究结果**: 带键 Submit"存在即拒"保证键下所有执行都在**同一条链**上;链只能从链尾延长(③),且延长要求链尾终态(①)。归纳:链上任何非终态执行只可能是链尾。所以"③目标是链尾 + ①链尾终态"成立时,链上不可能有其他非终态执行——②是①+③的推论(模型文档第 4 节"链不分叉规则"一节自己也论证了这一点,并覆盖无键链)。

**Decision**: 实现只校验①(目标状态终态)+③(目标未被重放过)+ completed 显式参数;②不写扫描代码,但作为不变式进 brokertest 断言(并发场景下验证"键下非终态 ≤1"恒成立)。

**Rationale**: 少一次全链扫描,少一处每介质各写一遍的逻辑;正确性由模型归纳保证,测试负责验证归纳没被实现破坏。

**Alternatives considered**:
- 每次 Replay 扫全链查在途:redis 侧要在 Lua 里循环 HGET,SQL 侧多一个查询;是冗余防御,且给出的错误(ErrInFlight)实际永远打不中(能打中说明约束已被破坏,该 panic 级别报警而不是常规错误),否决。

## 4. 公开 API 形态

**问题**: Gate 层的 Replay 入口、选项、查询能力长什么样?

**研究结果**: 原型(`prototype/identity/`)用 `ReplayByID`/`ReplayByKey` 两个入口验证通过;"目标既可 ExecutionID 又可 BusinessKey"合在一个入口会产生字符串歧义(键和 ulid 无法可靠区分)。查询能力:Filter 现有排序合同是 `(CreatedAt, ID)` 升序,链内执行的创建时间严格递增(重放必然晚于目标终态),所以"按键过滤 + 现有排序"天然就是链序,不需要新的链遍历方法。

**Decision**:

```go
// Gate 层
func (g *Gate) Replay(ctx, executionID string, opts ...ReplayOption) (string, error)      // 按执行身份,目标必须是链尾
func (g *Gate) ReplayByKey(ctx, businessKey string, opts ...ReplayOption) (string, error) // 按键,天然打链尾
func (g *Gate) History(ctx, businessKey string) ([]*Task, error)                          // 键下历史链,链序;空切片=键不存在
// 选项(函数式,与 SubmitOption 同风格)
AllowCompleted()                    // 重放 completed 必须显式带上
WithPayload(p json.RawMessage)      // 覆盖 Payload;不带=复制旧执行的
// 提交选项
WithBusinessKey(key string)         // 新
WithID(id string)                   // Deprecated:= WithBusinessKey,键不再是任务 ID
// Broker 层单入口(Gate 两个入口都翻译成它)
type ReplayRequest struct {
    ExecutionID    string          // 与 BusinessKey 二选一
    BusinessKey    string
    AllowCompleted bool
    Payload        json.RawMessage // nil = 复制旧执行的 Payload
}
Replay(ctx context.Context, req ReplayRequest) (*Task, error)
// Filter 增字段
BusinessKey string                  // 非空时按键过滤
```

**Rationale**: 两个显式入口消除歧义,与原型验证过的形态一致;History 是 List 的便捷封装(`Filter{BusinessKey: k}`),不进 Broker 接口;Payload 用 nil 判"没传"(要覆盖成空传 `json.RawMessage("{}")`/`("null")`,非 nil 即覆盖),满足 spec 边界"没传和传了空可区分"。

**Alternatives considered**:
- 单入口 `Replay(ctx, target string)` 自动识别键/ID:字符串歧义(用户的键完全可能长得像 ulid),否决;
- History 进 Broker 接口:List+Filter 已覆盖,新方法违反最小公倍数纪律,否决。

## 5. 错误形态:兼容 ErrTaskExists,新增可解构类型与三个哨兵

**问题**: Submit 键冲突要携带链尾信息(Clarification Q7),Replay 三种拒绝要可区分。

**Decision**:

```go
// Submit 键冲突:类型化错误,errors.Is(err, ErrTaskExists) 保持成立
type TaskExistsError struct {
    BusinessKey string
    ExecutionID string // 键下链尾(最新执行)
    Status      Status // 链尾状态,调用方据此决定要不要 Replay
}
func (e *TaskExistsError) Error() string
func (e *TaskExistsError) Unwrap() error // 返回 ErrTaskExists

// Replay 拒绝(哨兵,errors.Is 判断)
ErrReplayNotFinal      // 目标未终态
ErrAlreadyReplayed     // 目标已被重放(链不分叉)
ErrCompletedNotAllowed // 目标 completed 且未显式 AllowCompleted
```

**Rationale**: `Unwrap() → ErrTaskExists` 让所有现存 `errors.Is(err, ErrTaskExists)` 代码零改动;`errors.As` 取 `*TaskExistsError` 拿链尾信息,cron 配方"被拒 → 决定 Replay"一步到位。Replay 目标不存在复用 `ErrTaskNotFound`;哨兵命名避开已有 `ErrAlreadyFinal`(Cancel 语义),取 `ErrReplayNotFinal` 防混淆。原型里的 `ErrInFlight` 按决策 3 不再需要。

**Alternatives considered**:
- 改 ErrTaskExists 本身为类型:破坏"哨兵错误用 errors.Is"的既有合同,否决;
- Replay 拒绝共用一个错误:调用方无法区分"该走 AllowCompleted"和"链已延长过"两种完全不同的处置,否决。

## 6. Broker.Enqueue 保留"尊重调用方预置 ID"的内部行为

**问题**: ExecutionID 用户不可指定,但 brokertest 全套用例靠可读的自定义 ID(`task("A", …)`)构造场景;五后端 Enqueue 也都实现了"ID 非空则查重后沿用"。

**Decision**: Broker 层 `Enqueue` 维持现状(t.ID 非空则沿用,重复返回 ErrTaskExists——主键防御);"用户不可指定"收在 **Gate.Submit 层**(不再有任何 SubmitOption 能写 t.ID)。

**Rationale**: 分层各管一段:公开 API 保证模型语义(ID 系统生成),Broker 合同保证存储完整性(ID 唯一);brokertest/内部测试继续用预置 ID 构造场景,零改动成本。契约文档明写:预置 ID 是测试与嵌入方入口,库的公开路径永不使用。

**Alternatives considered**: Broker 层也禁止预置 ID——brokertest 十八条契约全要改成"先入队再捞真实 ID",可读性大跌且没有换来任何语义收益,否决。
