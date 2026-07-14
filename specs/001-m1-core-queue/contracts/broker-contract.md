# Broker 接口合同(M1 定型,M2 不改签名)

```go
type BrokerOptions struct {
    LeaseTTL        map[string]time.Duration // 队列→租约 TTL
    DefaultLeaseTTL time.Duration            // 缺省 60s
    LeaseLostMax    int                      // 缺省 3
    ThrottledMax    int                      // 缺省 100
    Notify          func(Task)               // 状态流转回调,可 nil;必须在锁/事务外异步调
    Clock           Clock                    // 可 nil=真时钟
}

type FailKind int
const (
    FailBusiness  FailKind = iota // Attempts+1;Attempts>MaxRetry → failed,否则 retrying
    FailThrottled                 // Throttled+1;≥ThrottledMax → failed,否则 retrying;Attempts 不动
    FailSkip                      // 直接 failed
)

type Broker interface {
    Init(opts BrokerOptions) error                      // New(cfg) 时调用一次,Dequeue 前必须先 Init
    Enqueue(ctx context.Context, t *Task) error
    Dequeue(ctx context.Context, queues []string) (*Task, error)
    Ack(ctx context.Context, id, leaseToken string, result []byte) error
    Fail(ctx context.Context, id, leaseToken, errMsg string, kind FailKind, retryAt time.Time) error
    Cancel(ctx context.Context, id string) error
    FinishCanceled(ctx context.Context, id, leaseToken string) error
    Requeue(ctx context.Context, id, leaseToken string) error
    Heartbeat(ctx context.Context, id, leaseToken string) error
    Get(ctx context.Context, id string) (*Task, error)
    List(ctx context.Context, f Filter) ([]*Task, error)
    QueueLen(ctx context.Context, queue string) (int, error)
    Counts(ctx context.Context) (map[string]map[Status]int64, error)
    ReapExpired(ctx context.Context) (int, error)
    Close() error
}

type Filter struct {
    Type   string
    Queue  string
    Status Status
    Limit  int // 0=不限
}
```

## 各方法语义合同

### Enqueue
- t.ID 为空则生成 ulid;已存在同 ID → `ErrTaskExists`,原任务不动。
- 同一事务内完成:校验 DependsOn 的父任务全部存在(缺失 → `ErrTaskNotFound` 包装说明);计算初始状态:
  - 有未终态父 → `blocked`(记 pending_parents);
  - 父全部 completed(或 IgnoreParentFailure 下全部终态)→ `pending`;
  - 有父 failed/canceled 且 FailFast → 直接 `canceled` 落库,LastError=`"parent <id> failed"`,并触发它自己的传播。
- RunAt 零值取 now;Queue 字段由调用方(client 按 Routes)填好,broker 不做路由。

### Dequeue
- 阻塞到"某队列有 status∈{pending,retrying} 且 run_at≤now 的任务"或 ctx 取消(返回 ctx.Err())。实现允许轮询,同进程建议即时唤醒。
- 认领原子:置 running、生成新 lease_token(ulid)、lease_until=now+TTL(按队列)、StartedAt=now(首次),返回带 LeaseToken 的 Task 副本。
- 同一任务并发 Dequeue 只能被认领一次。
- queues 为空 → 报错。多队列时无优先级要求,公平性不做合同。

### Ack
- 校验 id 存在且 status=running 且令牌一致,否则 `ErrLeaseLost`(任务不存在 → `ErrTaskNotFound`)。
- 同一事务:置 completed、写 Result、FinishedAt;标记 task_deps done;子任务 pending_parents 减一,减到 0 且 blocked → pending(唤醒)。

### Fail
- 令牌校验同 Ack。按 FailKind 与计数封顶决定 retrying(写 RunAt=retryAt)或 failed(FinishedAt=now)。
- 进 failed 的,同一事务内按策略处理**直接**子任务(FailFast → canceled 并各自触发下一层;链式,不递归整棵树)。
- errMsg 写 LastError;封顶导致的 failed 用固定文案:`"lease expired N times"` / `"throttled N times"`(前者在 ReapExpired 里)。

### Cancel
- blocked/pending/retrying → canceled(FinishedAt=now),同事务触发直接子任务传播;返回 nil。
- running → 仅置 cancel_requested 标记,返回 nil(落库终态由 FinishCanceled 完成)。
- 终态 → `ErrAlreadyFinal`;不存在 → `ErrTaskNotFound`。

### FinishCanceled
- 令牌校验;running → canceled,FinishedAt=now,LastError 可写 "canceled";同事务触发传播。

### Requeue
- 令牌校验;running → pending,清租约与 cancel_requested,**Attempts/LeaseLost/Throttled/RunAt 全部不动**。

### Heartbeat
- 令牌校验;续租 lease_until=now+TTL。
- 若 cancel_requested=1 → 返回 `ErrTaskCanceled`(续租照做),scheduler 收到后 cancel handler ctx。

### Get / List / QueueLen / Counts
- Get 不存在 → `ErrTaskNotFound`;返回副本,调用方改了不影响存储。
- QueueLen = 该队列 status∈{pending,retrying} 的数量。
- Counts = 出现过的 Type×Status 稀疏矩阵(只含计数非零的组合),与逐个 Get 汇总一致(brokertest 验证)。

### ReapExpired
- 扫 status=running 且 lease_until<now 的任务:LeaseLost+1;≥LeaseLostMax → failed(LastError="lease expired N times",触发传播),否则 → pending(清令牌)。返回回收条数。
- 例外:过期任务若带 cancel_requested=1(用户已请求取消,而此刻无 worker 持有租约)→ 直接置 canceled(FinishedAt=now,LastError="canceled",触发传播),不占 LeaseLost;取消请求不得因 worker 崩溃而丢失。该条同样计入回收条数。
- 顺带防御性修复:"blocked 但父任务实际全部终态"的任务,按唤醒/传播规则补齐(兜底,不是正常路径)。

### 状态写入通用规则
- 所有写入前经 canTransition 表校验,非法流转返回错误(带具体 from→to)。
- 每次成功流转调用 opts.Notify(事务外、异步、recover 包住)。回调**不保证跨操作顺序**、不保证即时可见;回调 panic 不得影响触发它的主流程。

## brokertest 契约用例清单

| # | 用例 | Given/When/Then 一句话 |
|---|---|---|
| 1 | RoundTrip | 入队再 Get,全字段一致(Payload/RunAt/DependsOn) |
| 2 | IdempotentID | 同 ID 二次 Enqueue → ErrTaskExists 且原任务不被覆盖 |
| 3 | ClaimMutex | 1 个任务 100 并发 Dequeue 恰好 1 个成功 |
| 4 | BlockingDequeue | 空队列阻塞;Enqueue 后返回;ctx 取消立即退出 |
| 5 | DelayedTask | RunAt 未到不出队,到点可出队 |
| 6 | AckFail | Ack 置 completed 写 Result;FailBusiness 置 retrying,耗尽置 failed;FailSkip 直接 failed;FailThrottled 不涨 Attempts、Throttled 封顶 |
| 7 | LeaseReap | 认领不 Ack,过期 ReapExpired 回 pending 且 LeaseLost+1;到 LeaseLostMax 置 failed;Heartbeat 能续租 |
| 8 | StaleToken | 回收后旧令牌 Ack/Fail/Heartbeat/Requeue/FinishCanceled → ErrLeaseLost |
| 9 | RetryingReclaim | retrying 到点后可被重新认领 |
| 10 | DepWake | 父 Ack 后子 blocked→pending 原子完成;fan-in 两父都完成才唤醒;提交时父已完成直接 pending |
| 11 | CascadeCancel | 父 failed → FailFast 子 canceled → 孙 canceled(链式最终一致);IgnoreParentFailure 子照常唤醒;提交时父已 failed 子直接 canceled |
| 12 | CancelStates | blocked/pending/retrying Cancel 即 canceled;running Cancel 置标记,Heartbeat 返回 ErrTaskCanceled,FinishCanceled 落库;终态 Cancel → ErrAlreadyFinal |
| 13 | CountsConsistency | 任意操作序列后 Counts 与逐个 Get 汇总一致;QueueLen 正确 |
| 14 | ListFilter | 按 Type/Status/Queue/Limit 过滤正确 |
| 15 | RequeueNoCount | Requeue 后回 pending 且三计数与 RunAt 不变 |
| 16 | IllegalTransition | 对 completed 任务 Ack/Fail → 错误;表驱动抽查非法流转 |
| 17 | Notify | Enqueue/Dequeue/Ack 一条链,最终能观测到 pending/running/completed 三个快照且字段正确(异步、不保证跨操作顺序);回调 panic 不影响主流程 |

`brokertest.Run(t, factory func(t *testing.T) taskgate.Broker)`,内部用注入 fakeclock 控制时间(factory 返回的 broker 必须已 Init 且使用套件提供的 clock —— 具体做法:套件导出 `brokertest.Options(clk)` 供 factory 构造时传入)。
