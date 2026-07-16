# Data Model: Identity 身份模型

## 1. Go 类型变更

### Task(taskgate.go)

| 字段 | 类型 | 默认值 | 语义 |
|---|---|---|---|
| `ID` | string | broker 生成 ulid | **语义收紧为 ExecutionID**:公开 API 不再提供任何写入口;创建后不可变、永不复用。字段本身不改名(兼容) |
| `BusinessKey` | string | ""(无键) | 新增。业务幂等键;创建时写入,不可变;`json:"business_key,omitempty"` |
| `ReplayOf` | string | ""(非重放) | 新增。本执行重放自哪个 ExecutionID;创建时写入,不可变;`json:"replay_of,omitempty"` |

其余字段、状态机(七状态,`canTransition` 不动)、三计数语义全部不变更。

### 提交选项(taskgate.go)

| 选项 | 变更 |
|---|---|
| `WithBusinessKey(key string)` | 新增:写 `submitOptions.businessKey` → `t.BusinessKey` |
| `WithID(id string)` | **Deprecated 别名**:实现改为 `return WithBusinessKey(id)`;godoc 写明"键不再是任务 ID,不能用于 Get/DependsOn" |
| `submitOptions.id` | 删除:没有任何入口能写 `t.ID` |

### ReplayRequest / ReplayOption(broker.go / taskgate.go)

```go
// Broker 层单入口
type ReplayRequest struct {
    ExecutionID    string          // 与 BusinessKey 恰好一个非空
    BusinessKey    string          // 按键时作用于链尾
    AllowCompleted bool
    Payload        json.RawMessage // nil = 复制目标的 Payload
}
// Gate 层函数式选项
type ReplayOption func(*replayOptions) // AllowCompleted() / WithPayload(p)
```

### Filter(broker.go)

新增 `BusinessKey string`(非空时按键过滤);排序/分页合同不变——链内 CreatedAt 严格递增,`(CreatedAt, ID)` 升序天然是链序。

### 错误(errors.go)

```go
var (
    ErrReplayNotFinal      = errors.New("taskgate: replay target not in final state")
    ErrAlreadyReplayed     = errors.New("taskgate: execution already replayed (chain must not fork)")
    ErrCompletedNotAllowed = errors.New("taskgate: replaying a completed execution requires AllowCompleted")
)
type TaskExistsError struct {
    BusinessKey string
    ExecutionID string // 键下链尾
    Status      Status
}
func (e *TaskExistsError) Unwrap() error { return ErrTaskExists } // errors.Is 兼容
```

## 2. Replay 的行为合同(五后端一致,进 brokertest)

输入合法性(Gate 层先挡一遍,broker 层兜底):`ExecutionID`/`BusinessKey` 恰好一个非空。

原子动作序列(**同事务 / 同 Lua / 同临界区**):

1. 定位目标:按 ID 直取;按键取链尾(该键下 `ReplayOf` 链的末端 = CreatedAt 最大那条)。不存在 → `ErrTaskNotFound`。
2. 校验目标已终态,否则 `ErrReplayNotFinal`。
3. 目标为 `completed` 且未 `AllowCompleted` → `ErrCompletedNotAllowed`。
4. 校验目标未被重放过(不存在 `ReplayOf=目标` 的执行),否则 `ErrAlreadyReplayed`。
5. 创建新执行:新 ulid、`ReplayOf=目标ID`、`BusinessKey=目标的键`(可空)、`Type/Queue/MaxRetry/OnParentFailure` 沿用目标、`Payload` 按 req(nil 复制)、三计数清零、`DependsOn` 置空(重放不继承依赖边——目标的父早已终态,继承只会引入"引用旧父"的赘边;需要依赖的重跑走正常 Submit)、`RunAt=now`、状态按无依赖判定(pending)。
6. 新执行照常进队列结构、计数、通知(Notify 语义与 Enqueue 一致)。

前置条件②(键下无在途)由①+④推出,不单独实现(research 决策 3);目标记录**零写入**(不可变)。

## 3. sqlite(sqlitebroker/schema.sql)

```sql
-- tasks 表加两列(存量库升级:ALTER TABLE ... ADD COLUMN 幂等执行,默认空串,零迁移)
business_key TEXT NOT NULL DEFAULT '',
replay_of    TEXT NOT NULL DEFAULT '',

-- 不变式 1:链头唯一(同键下 replay_of 为空的行至多一条)→ 带键 Submit 并发兜底
CREATE UNIQUE INDEX IF NOT EXISTS uq_chain_head
    ON tasks(business_key) WHERE business_key <> '' AND replay_of = '';
-- 不变式 2:重放来源唯一(链不分叉)→ 并发 Replay 兜底
CREATE UNIQUE INDEX IF NOT EXISTS uq_replay_of
    ON tasks(replay_of) WHERE replay_of <> '';
-- 按键查询(History / Filter.BusinessKey)
CREATE INDEX IF NOT EXISTS idx_business_key ON tasks(business_key) WHERE business_key <> '';
```

- Enqueue 事务内:`business_key` 非空时先 `SELECT id,status FROM tasks WHERE business_key=? ORDER BY created_at DESC,id DESC LIMIT 1`,命中 → 构造 `*TaskExistsError`(带链尾)返回;未命中继续 INSERT,撞 `uq_chain_head`(并发窗口)→ 重查一次链尾再返回 `*TaskExistsError`。
- Replay 事务内:按第 2 节序列执行;链尾定位 `WHERE business_key=? ORDER BY created_at DESC,id DESC LIMIT 1`;步骤 4 用 `SELECT 1 FROM tasks WHERE replay_of=?`;INSERT 撞 `uq_replay_of` → `ErrAlreadyReplayed`。sqlite 单写者,事务内检查即串行化,索引纯兜底。
- 升级:`Init` 的建表流程后追加幂等 `ALTER TABLE`(捕获"duplicate column"错误跳过)+ `CREATE INDEX IF NOT EXISTS`。

## 4. PG / MySQL(internal/sqlbroker + Dialect)

共享核心与 sqlite 同一套语句形态;差异收进 `Dialect.SchemaSQL`:

- **PG**:与 sqlite 相同的**部分唯一索引**(`CREATE UNIQUE INDEX ... WHERE ...`,PG 原生支持)。
- **MySQL**(无部分索引,用**生成列 + 唯一索引**,唯一索引对 NULL 不去重):

```sql
business_key VARCHAR(255) NOT NULL DEFAULT '' COLLATE utf8mb4_bin,
replay_of    VARCHAR(26)  NOT NULL DEFAULT '',
chain_head_key VARCHAR(255) COLLATE utf8mb4_bin
    GENERATED ALWAYS AS (IF(business_key <> '' AND replay_of = '', business_key, NULL)) STORED,
replay_of_uq VARCHAR(26)
    GENERATED ALWAYS AS (IF(replay_of <> '', replay_of, NULL)) STORED,
UNIQUE KEY uq_chain_head (chain_head_key),
UNIQUE KEY uq_replay_of (replay_of_uq),
KEY idx_business_key (business_key)
```

- 并发串行化:Replay 事务内对目标行 `SELECT ... FOR UPDATE`(两库语法一致,沿用 claim 的既有模式),再做校验与 INSERT;唯一索引兜底撞车 → `Dialect.IsDuplicateKey` 命中后**按撞的是哪个约束**翻译:约束名含 `uq_replay_of` → `ErrAlreadyReplayed`;含 `uq_chain_head` → 重查链尾返 `*TaskExistsError`;否则(主键)→ `ErrTaskExists`。PG 用 `pgconn.PgError.ConstraintName`,MySQL 从 errno 1062 的 message 提取索引名(驱动无结构化字段,此处是唯一允许的字符串匹配,注释写明原因)。
- 存量升级:`SchemaSQL` 后追加幂等 `ALTER TABLE`(PG `ADD COLUMN IF NOT EXISTS`;MySQL 查 `information_schema.COLUMNS` 决定)。

## 5. redis(redisbroker/lua)

新增键(键名工具进 common.lua):

| 键 | 类型 | 用途 |
|---|---|---|
| `tg:bk:<key>` | LIST | 该 BusinessKey 下的执行历史链,RPUSH 链序追加;`LLEN>0` 即"键已存在";`LINDEX -1` 即链尾 |
| task hash 加字段 | HASH | `business_key`、`replay_of`(空串省略);`replayed=1` 标记"已被重放"(链不分叉判据,免建反向索引) |

- **enqueue.lua**:`business_key` 非空时先查 `kBk(key)`:`LLEN>0` → `LINDEX -1` 取链尾 id、`HGET status`,返回 `TGERR:exists:<tailID>:<tailStatus>`,Go 翻译成 `*TaskExistsError`;否则落 task hash 后 `RPUSH kBk(key) id`。同脚本原子,无竞态窗口。
- **replay.lua**(新脚本):按第 2 节序列——定位目标(按键走 `LINDEX -1`)→ 校验 `status` 终态 / `replayed` 未置位 / completed 需 flag → 建新 hash(Go 注入新 ulid 与 now)→ `HSET 目标 replayed 1` → `RPUSH kBk` → 入队结构 + moveStatus 计数。**目标 hash 除 `replayed` 标记外零字段改写**(`replayed` 是链元数据不是执行记录,不违反"终态不可变"——Get 不回读该字段)。
- **query**:`Filter.BusinessKey` 非空时以 `kBk(key)` 的 LIST 为候选集(代替全量扫描),再走既有过滤/排序管道。
- ID 校验:BusinessKey 复用 `validateID` 的控制字符检查(键会拼进 redis key,另加禁空格与长度上限 512,防 key 注入)。

## 6. memory(memorybroker/broker.go)

现有单锁临界区内加 sidecar 索引(与原型同构,但进正式结构体):

```go
chains   map[string][]string // BusinessKey → 链(创建序)
replayed map[string]bool     // ExecutionID → 已被重放
```

Enqueue 带键:`len(chains[key])>0` → 取链尾构造 `*TaskExistsError`;Replay:临界区内走第 2 节序列。语义参考实现(宪法 II.3),先于其他后端落地。

## 7. 配置校验与计数

- 无新增 Config/QueueConfig 字段,`New()` 校验规则不变。
- 计数:Replay 创建的新执行走 Enqueue 同一套 stats(`Type:pending +1`);目标执行的计数不动。三计数(Attempts/LeaseLost/Throttled)在新执行上从零起算。
- Notify:新执行触发与 Enqueue 相同的入队通知;目标执行不触发任何通知(零状态变更)。

## 8. 状态机

无变更。七状态、全部流转、终态无出边照旧;Replay 不是状态流转,是**新记录的创建**。
