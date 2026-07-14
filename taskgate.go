// Package taskgate 是一个轻量的 Go 任务排队限流库:排队、限流、重试、依赖、取消。
// 它是库不是服务:不读环境变量、不读配置文件,只吃调用方传进来的 Config。
package taskgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Status 任务状态,共七态。用字符串是为了落库和日志里直接可读。
type Status string

const (
	StatusBlocked   Status = "blocked"   // 有父任务还没跑完,等唤醒
	StatusPending   Status = "pending"   // 排队中,可被认领
	StatusRunning   Status = "running"   // 已被 worker 认领,持有租约
	StatusRetrying  Status = "retrying"  // 失败后等退避时间到点重跑
	StatusCompleted Status = "completed" // 终态:成功
	StatusFailed    Status = "failed"    // 终态:失败(重试耗尽/跳过重试/计数封顶)
	StatusCanceled  Status = "canceled"  // 终态:被取消(主动取消或父失败传播)
)

// allStatuses 七态全集,给表驱动校验和测试枚举用。
var allStatuses = []Status{
	StatusBlocked, StatusPending, StatusRunning, StatusRetrying,
	StatusCompleted, StatusFailed, StatusCanceled,
}

// IsFinal 是否终态。终态没有任何出边,不允许再流转。
func (s Status) IsFinal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCanceled
}

// legalTransitions 合法流转表,唯一事实来源是 data-model.md 的状态机:
//   - blocked  → pending | canceled
//   - pending  → running | canceled
//   - running  → completed | failed | retrying | canceled | pending(Requeue/Reap 归还)
//   - retrying → running | canceled
//   - 三个终态没有出边
var legalTransitions = map[Status]map[Status]bool{
	StatusBlocked:  {StatusPending: true, StatusCanceled: true},
	StatusPending:  {StatusRunning: true, StatusCanceled: true},
	StatusRunning:  {StatusCompleted: true, StatusFailed: true, StatusRetrying: true, StatusCanceled: true, StatusPending: true},
	StatusRetrying: {StatusRunning: true, StatusCanceled: true},
}

// canTransition 表驱动判断 from→to 是否合法。所有状态写入前都必须过这一关。
func canTransition(from, to Status) bool {
	return legalTransitions[from][to]
}

// ParentFailurePolicy 父任务失败时子任务怎么办。
type ParentFailurePolicy string

const (
	// FailFast 父任务失败/取消 → 子任务连锁取消(默认)。
	FailFast ParentFailurePolicy = "fail_fast"
	// IgnoreParentFail 父任务只要进了终态(哪怕失败)就照常唤醒子任务。
	// 注:同名选项函数是 IgnoreParentFailure(),常量名少个 ure 是为了避开重名。
	IgnoreParentFail ParentFailurePolicy = "ignore_parent_failure"
)

// Task 任务实体。字段语义照 data-model.md 第 1 节,Payload/Result 一律 json.RawMessage。
type Task struct {
	ID              string              `json:"id"`                // 空则由 broker 生成 ulid;自定义 ID 可做幂等去重
	Type            string              `json:"type"`              // 决定 handler 和默认队列
	Queue           string              `json:"queue"`             // 限流单元,入队那一刻定死
	Payload         json.RawMessage     `json:"payload,omitempty"` // 入参
	Status          Status              `json:"status"`
	Result          json.RawMessage     `json:"result,omitempty"` // Ack 时写入
	LastError       string              `json:"last_error,omitempty"`
	Attempts        int                 `json:"attempts"`  // 业务失败次数,> MaxRetry → failed
	MaxRetry        int                 `json:"max_retry"` // 0 = 不重试
	LeaseLost       int                 `json:"lease_lost"`
	Throttled       int                 `json:"throttled"`
	RunAt           time.Time           `json:"run_at"` // 延迟执行和重试退避都靠它
	DependsOn       []string            `json:"depends_on,omitempty"`
	OnParentFailure ParentFailurePolicy `json:"on_parent_failure"`
	LeaseToken      string              `json:"lease_token,omitempty"` // Dequeue 时携带,对外只读
	CreatedAt       time.Time           `json:"created_at"`
	StartedAt       time.Time           `json:"started_at,omitzero"`
	FinishedAt      time.Time           `json:"finished_at,omitzero"`
}

// Handler 业务处理函数:返回的 []byte 会作为 Result 写库;返回错误决定重试路径。
type Handler func(ctx context.Context, t *Task) ([]byte, error)

// Duration 包一层 time.Duration,让 yaml/json 配置里能直接写 "10m"、"60s" 这种人话。
type Duration time.Duration

// UnmarshalText 支持 "10m" 这类写法,yaml 和 json 解码都走这里。
func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return fmt.Errorf("taskgate: invalid duration %q: %w", string(b), err)
	}
	*d = Duration(v)
	return nil
}

// MarshalText 序列化回 "10m0s" 这种标准格式。
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

// Config 全局配置。库不读 env/文件,应用自己 unmarshal 好再传进来;
// Broker 和 OnStateChange 是运行期对象,序列化时跳过。
type Config struct {
	Broker        Broker                 `yaml:"-" json:"-"`
	Queues        map[string]QueueConfig `yaml:"queues" json:"queues"`
	Routes        map[string]string      `yaml:"routes" json:"routes"` // Type → Queue
	DefaultQueue  QueueConfig            `yaml:"default_queue" json:"default_queue"`
	OnStateChange func(Task)             `yaml:"-" json:"-"`
	LeaseLostMax  int                    `yaml:"lease_lost_max" json:"lease_lost_max"` // 0 补默认 3
	ThrottledMax  int                    `yaml:"throttled_max" json:"throttled_max"`   // 0 补默认 100
}

// QueueConfig 单个队列的限流参数。
type QueueConfig struct {
	Workers  int      `yaml:"workers" json:"workers"`
	RPS      float64  `yaml:"rps" json:"rps"`             // 0 = 不限速
	Burst    int      `yaml:"burst" json:"burst"`         // 0 时取 max(1, int(RPS))
	LeaseTTL Duration `yaml:"lease_ttl" json:"lease_ttl"` // 0 补默认 60s
}

// 零值补默认的取值,来自 data-model.md。
const (
	defaultLeaseTTL     = Duration(60 * time.Second)
	defaultLeaseLostMax = 3
	defaultThrottledMax = 100
)

// validateQueue 校验单个队列并补默认值,name 只用来拼错误信息。
func validateQueue(name string, q QueueConfig) (QueueConfig, error) {
	if q.Workers < 1 {
		return q, fmt.Errorf("taskgate: queue %q: workers must be >= 1, got %d", name, q.Workers)
	}
	if q.RPS < 0 {
		return q, fmt.Errorf("taskgate: queue %q: rps must be >= 0, got %v", name, q.RPS)
	}
	if q.Burst < 0 {
		return q, fmt.Errorf("taskgate: queue %q: burst must be >= 0, got %d", name, q.Burst)
	}
	if q.LeaseTTL < 0 {
		return q, fmt.Errorf("taskgate: queue %q: lease_ttl must be > 0, got %v", name, time.Duration(q.LeaseTTL))
	}
	if q.LeaseTTL == 0 {
		q.LeaseTTL = defaultLeaseTTL
	}
	return q, nil
}

// validate 按 data-model.md 的 4 条规则做 fail-fast 校验,顺手把零值补成默认值。
func (c *Config) validate() error {
	// 规则 1:Broker 必填。
	if c.Broker == nil {
		return errors.New("taskgate: config.Broker is required")
	}
	// 规则 2:逐个队列校验并补默认。
	for name, q := range c.Queues {
		fixed, err := validateQueue(name, q)
		if err != nil {
			return err
		}
		c.Queues[name] = fixed
	}
	// 规则 3:Routes 指到 Queues 里没有的队列时,必须有可用的 DefaultQueue 兜底。
	needDefault := false
	for typ, target := range c.Routes {
		if _, ok := c.Queues[target]; !ok {
			if c.DefaultQueue.Workers < 1 {
				return fmt.Errorf("taskgate: route %q -> %q: target queue not configured and no usable default_queue", typ, target)
			}
			needDefault = true
		}
	}
	// DefaultQueue 允许整个不配(全零值 = 没有兜底队列);
	// 只要配了(或被 Routes 用到),就按普通队列的规则校验并补默认。
	if needDefault || c.DefaultQueue != (QueueConfig{}) {
		fixed, err := validateQueue("(default)", c.DefaultQueue)
		if err != nil {
			return err
		}
		c.DefaultQueue = fixed
	}
	// 规则 4:两个封顶计数不能为负,零值补默认。
	if c.LeaseLostMax < 0 {
		return fmt.Errorf("taskgate: lease_lost_max must be >= 0, got %d", c.LeaseLostMax)
	}
	if c.LeaseLostMax == 0 {
		c.LeaseLostMax = defaultLeaseLostMax
	}
	if c.ThrottledMax < 0 {
		return fmt.Errorf("taskgate: throttled_max must be >= 0, got %d", c.ThrottledMax)
	}
	if c.ThrottledMax == 0 {
		c.ThrottledMax = defaultThrottledMax
	}
	return nil
}

// submitOptions 收集提交选项,由 client.Submit 消费(Delay 需要在提交那一刻换算成 RunAt)。
type submitOptions struct {
	id                  string
	delay               time.Duration
	runAt               time.Time
	maxRetry            int
	dependsOn           []string
	ignoreParentFailure bool
}

// SubmitOption 提交任务时的函数式选项。
type SubmitOption func(*submitOptions)

// WithID 自定义任务 ID,用来做幂等去重;重复提交同 ID 会拿到 ErrTaskExists。
func WithID(id string) SubmitOption {
	return func(o *submitOptions) { o.id = id }
}

// Delay 延迟 d 之后才允许执行(提交时换算成 RunAt)。
func Delay(d time.Duration) SubmitOption {
	return func(o *submitOptions) { o.delay = d }
}

// RunAt 指定最早执行时刻,和 Delay 二选一,后设置的生效。
func RunAt(t time.Time) SubmitOption {
	return func(o *submitOptions) { o.runAt = t }
}

// MaxRetry 业务失败最多重试 n 次(Attempts > n 进 failed)。
func MaxRetry(n int) SubmitOption {
	return func(o *submitOptions) { o.maxRetry = n }
}

// DependsOn 声明父任务,父全部完成才会被唤醒;父 ID 必须已存在,否则拒收。
func DependsOn(ids ...string) SubmitOption {
	return func(o *submitOptions) { o.dependsOn = append(o.dependsOn, ids...) }
}

// IgnoreParentFailure 父任务失败也照常执行(默认是 FailFast 连锁取消)。
func IgnoreParentFailure() SubmitOption {
	return func(o *submitOptions) { o.ignoreParentFailure = true }
}

// applySubmitOptions 把选项收拢成一个结构,给 Submit 和单测用。
func applySubmitOptions(opts ...SubmitOption) submitOptions {
	var o submitOptions
	for _, fn := range opts {
		fn(&o)
	}
	return o
}
