# Data Model: taskgate M1

## 1. 公共类型(taskgate 包)

### Task

| 字段 | 类型 | 默认 | 语义 |
|---|---|---|---|
| ID | string | ulid 自动生成 | 可自定义做幂等去重;重复 Enqueue → ErrTaskExists |
| Type | string | 必填 | 决定 handler + 默认队列 |
| Queue | string | =Type(经 Routes 改写) | 限流单元,入队那一刻定死 |
| Payload | json.RawMessage | nil | 入参 |
| Status | Status | pending/blocked | 见状态机 |
| Result | json.RawMessage | nil | Ack 时写入 |
| LastError | string | "" | 最近一次失败/取消原因 |
| Attempts | int | 0 | 业务失败次数(每次认领后失败 +1) |
| MaxRetry | int | 0(不重试) | Attempts > MaxRetry → failed |
| LeaseLost | int | 0 | 租约过期回收次数,≥3(可配)→ failed |
| Throttled | int | 0 | ErrThrottled 重排次数,≥100(可配)→ failed |
| RunAt | time.Time | 提交时刻 | 延迟执行/重试退避都靠它 |
| DependsOn | []string | nil | 父任务 ID 列表 |
| OnParentFailure | ParentFailurePolicy | FailFast | FailFast / IgnoreParentFailure |
| LeaseToken | string | "" | 当前租约令牌(Dequeue 返回时携带;对外只读) |
| CreatedAt/StartedAt/FinishedAt | time.Time | - | 时间戳链 |

内部持久化字段(不出现在公开 struct 或以非导出方式承载):`lease_until`、`pending_parents`。

### Status 状态机

```
blocked ──唤醒(父全终态)──> pending ──认领──> running ──Ack──> completed
   │                          ↑  ↑              │ Fail(可重试)
   │                          │  └─Requeue──────┤
   │                          │                 ├──Fail(耗尽/SkipRetry/封顶)──> failed
   │              到点重新认领 │                 │
   │                       retrying <──────────┘(退避重排)
   │                          
   └──Cancel/父失败传播──> canceled   (pending/retrying 同;running 经 FinishCanceled)
```

合法流转表(其余全部非法,表驱动校验):

- blocked → pending | canceled
- pending → running | canceled
- running → completed | failed | retrying | canceled(FinishCanceled) | pending(Requeue/Reap)
- retrying → running | canceled
- 终态(completed/failed/canceled)→ 无出边

### Config / QueueConfig

```go
type Config struct {
    Broker        Broker                  `yaml:"-" json:"-"`
    Queues        map[string]QueueConfig  `yaml:"queues" json:"queues"`
    Routes        map[string]string       `yaml:"routes" json:"routes"`        // Type→Queue
    DefaultQueue  QueueConfig             `yaml:"default_queue" json:"default_queue"`
    OnStateChange func(Task)              `yaml:"-" json:"-"`
    LeaseLostMax  int                     `yaml:"lease_lost_max" json:"lease_lost_max"`   // 默认 3
    ThrottledMax  int                     `yaml:"throttled_max" json:"throttled_max"`     // 默认 100
}
type QueueConfig struct {
    Workers  int      `yaml:"workers" json:"workers"`
    RPS      float64  `yaml:"rps" json:"rps"`          // 0 = 不限速
    Burst    int      `yaml:"burst" json:"burst"`      // 0 时取 max(1, int(RPS))
    LeaseTTL Duration `yaml:"lease_ttl" json:"lease_ttl"` // 默认 60s;Duration 带 UnmarshalText
}
```

### New(cfg) 校验规则(fail fast,返回 error)

1. Broker 非 nil
2. 每个 QueueConfig:Workers ≥ 1(DefaultQueue 同);RPS ≥ 0;LeaseTTL > 0(零值补默认 60s);Burst ≥ 0
3. Routes 的目标队列必须在 Queues 里,或 DefaultQueue.Workers ≥ 1
4. LeaseLostMax/ThrottledMax ≥ 0(零值补默认 3/100)

### 哨兵错误(errors.go)

`ErrTaskExists`、`ErrTaskNotFound`、`ErrLeaseLost`、`ErrTaskCanceled`(Heartbeat 发现取消标记)、`ErrAlreadyFinal`(对终态 Cancel)、`ErrUnknownType`(Run 时遇到没 handler 的 Type 不认领;Submit 不校验)、`ErrShutdown`;错误类型 `ErrThrottled{RetryAfter time.Duration}`、`ErrSkipRetry{Err error}`(均实现 error,供 handler 返回)。

## 2. 计数语义

| 计数 | 谁 +1 | 封顶行为 |
|---|---|---|
| Attempts | Fail(业务失败) | > MaxRetry → failed,LastError=业务错误 |
| LeaseLost | ReapExpired 回收 | ≥ LeaseLostMax → failed,LastError="lease expired N times" |
| Throttled | Fail 且 errors.As(ErrThrottled) | ≥ ThrottledMax → failed,LastError="throttled N times" |
| (无) | Requeue(Shutdown 归还) | 三个计数都不动,RunAt 不变,回 pending |

ErrThrottled 走 Fail 接口但语义区分:Attempts 不 +1、状态 retrying、RunAt=now+RetryAfter。实现上 Fail 签名带 `kind FailKind`(business/throttled/skip)或由 scheduler 预先算好 `retryAt` 与目标状态——定型见 contracts/broker-contract.md。

## 3. sqlite 后端

### DDL(schema.sql,go:embed)

```sql
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    queue           TEXT NOT NULL,
    payload         BLOB,
    status          TEXT NOT NULL,
    result          BLOB,
    last_error      TEXT NOT NULL DEFAULT '',
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_retry       INTEGER NOT NULL DEFAULT 0,
    lease_lost      INTEGER NOT NULL DEFAULT 0,
    throttled       INTEGER NOT NULL DEFAULT 0,
    run_at          INTEGER NOT NULL,            -- unix 毫秒,下同
    depends_on      TEXT NOT NULL DEFAULT '[]',  -- JSON 数组
    on_parent_fail  TEXT NOT NULL DEFAULT 'fail_fast',
    pending_parents INTEGER NOT NULL DEFAULT 0,
    lease_token     TEXT NOT NULL DEFAULT '',
    lease_until     INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0, -- running 任务的取消标记,Heartbeat 时发现

    created_at      INTEGER NOT NULL,
    started_at      INTEGER NOT NULL DEFAULT 0,
    finished_at     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_claim ON tasks(queue, status, run_at);
CREATE INDEX IF NOT EXISTS idx_status ON tasks(status, lease_until);
CREATE TABLE IF NOT EXISTS task_deps (
    child_id  TEXT NOT NULL,
    parent_id TEXT NOT NULL,
    done      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (child_id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_deps_parent ON task_deps(parent_id, done);
```

连接参数:`_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)`。

### 关键 SQL

- **认领**(BEGIN IMMEDIATE 事务):research.md 第 1 节的子查询 UPDATE ... RETURNING。
- **终态+唤醒**(同事务):① UPDATE 父任务终态;② `UPDATE task_deps SET done=1 WHERE parent_id=?`;③ completed 时 `UPDATE tasks SET pending_parents=pending_parents-1 WHERE id IN (SELECT child_id ...) AND pending_parents>0`,再 `UPDATE tasks SET status='pending' WHERE ... pending_parents=0 AND status='blocked'`;failed/canceled 且 FailFast 时把子任务翻 canceled(M1 实现:同一事务内工作队列收敛整棵子树,强于"逐层小事务";两种形态都合法,见宪法 v1.1.0 III.4)。
- **回收**:`UPDATE tasks SET status=CASE WHEN lease_lost+1>=? THEN 'failed' ELSE 'pending' END, lease_lost=lease_lost+1, lease_token='' WHERE status='running' AND lease_until < ? RETURNING id,status` — 一条 SQL 原子回收;随后对翻 failed 的行触发链式取消。
- **Counts**:`SELECT type, status, COUNT(*) FROM tasks GROUP BY type, status`。

## 4. memory 后端

`map[string]*Task` + 每队列 ready 判断即时计算;单 `sync.Mutex` + `sync.Cond` 做阻塞 Dequeue;所有状态流转在锁内完成(等价"同事务");语义参考实现,brokertest 全契约必须过。

## 5. redis 后端

M1 无变更(接口定型已把 redis 语义考虑在内,见方案 7.2;实现留 M2)。
