package taskgate

import (
	"context"
	"time"
)

// BrokerOptions New(cfg) 装配时传给后端的运行参数,签名照 contracts/broker-contract.md。
type BrokerOptions struct {
	LeaseTTL        map[string]time.Duration // 队列→租约 TTL
	DefaultLeaseTTL time.Duration            // 缺省 60s
	LeaseLostMax    int                      // 缺省 3
	ThrottledMax    int                      // 缺省 100
	Notify          func(Task)               // 状态流转回调,可 nil;必须在锁/事务外异步调
	Clock           Clock                    // 可 nil=真时钟
}

// FailKind Fail 的三种语义,决定动哪个计数、进 retrying 还是 failed。
type FailKind int

const (
	// FailBusiness 业务失败:Attempts+1;Attempts>MaxRetry → failed,否则 retrying。
	FailBusiness FailKind = iota
	// FailThrottled 被网关限流:Throttled+1;≥ThrottledMax → failed,否则 retrying;Attempts 不动。
	FailThrottled
	// FailSkip 明确不重试:直接 failed。
	FailSkip
)

// Broker 存储后端接口。只收 memory/sqlite/redis 三后端都能同语义实现的方法(宪法第 II 条);
// 各方法的行为合同见 contracts/broker-contract.md,由 brokertest 套件统一验收。
type Broker interface {
	Init(opts BrokerOptions) error // New(cfg) 时调用一次,Dequeue 前必须先 Init
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

// LimiterProvider 后端的**可选能力接口**:能为队列提供跨进程共享的限流器。
//
// 限流不是所有后端都能做(memory/sqlite 没有跨进程共享的介质),进不了 Broker
// 接口的"最小公倍数",所以单独拆成能力接口。scheduler 装配限流器时只做
// `broker.(LimiterProvider)` 这一次**能力断言**——断言的是接口不是具体后端类型,
// 上层依然不 import 任何后端包,不违反宪法 II.2"上层不特判后端":
// 新后端想提供分布式限流,实现本接口即可;memory/sqlite 不实现,
// scheduler 自动退回进程内限流(localLimiter),行为与 M1 完全一致。
//
// 实现约束:QueueLimiter 的构造必须廉价、不得持有需要显式释放的资源——
// 本接口没有 Close,某个队列构造失败时之前已建成的限流器不会被回收,
// 构造期占了资源就是泄漏(redisbroker 的实现只是复用 Broker 连接拼参数,零资源)。
type LimiterProvider interface {
	// QueueLimiter 按队列配置构造该队列的限流器;出错时 Gate.Run 直接返回该错误。
	QueueLimiter(queue string, qc QueueConfig) (QueueLimiter, error)
}

// Filter List 的过滤条件,零值字段表示不过滤。
//
// 排序与分页合同(M3 定型,三后端一致,见 contracts/broker-contract.md):
//   - 结果一律按 (CreatedAt, ID) 升序:CreatedAt 由 broker 落库时统一写,
//     同一毫秒内再按 ID 定序,保证全序;
//   - 执行顺序写死:先按 Type/Queue/Status 过滤 → 排序 → 跳过 Offset 条 → 取 Limit 条;
//   - Offset ≥ 匹配总数 → 返回空列表(nil error);Offset < 0 按 0 处理;
//   - 翻页弱一致:翻页期间数据变动不承诺快照一致,只承诺"未变动的任务不丢不重"。
type Filter struct {
	Type   string
	Queue  string
	Status Status
	Limit  int // 0=不限
	Offset int // 排序后跳过的条数,0=不跳过
}
