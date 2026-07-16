// Package redisbroker 是 Broker 的 Redis 后端:所有"多步读写必须原子"的操作
// 都收进单段 Lua 脚本执行(宪法 III:终态更新与子任务唤醒同一段脚本收敛),
// 语义以 memorybroker 为基准,由 brokertest 的 18 条契约统一验收。
//
// 关键约定(specs/002-m2-redis/research.md):
//   - 第 1 节:认领 = 单段 claim.lua 原子完成,不用 BLMOVE 两步;Dequeue 是 Go 侧轮询循环;
//   - 第 2 节:脚本内禁用 TIME/math.random,时间与 ulid 全部由 Go 经 ARGV 注入(fakeclock 生效);
//   - 第 3 节:传播在同一段脚本内用工作队列收敛整棵子树;
//   - 第 9 节:哨兵错误只在 Lua 明确判定时返回(TGERR: 错误码),网络错误原样透传,
//     绝不折叠成 ErrLeaseLost/ErrTaskNotFound。
//
// 键设计见 specs/002-m2-redis/data-model.md 第 2 节;脚本内用 ARGV[1] 传前缀自行拼键,
// 因此本后端面向单实例/主从 Redis,不支持 Redis Cluster(键不带 hash tag)。
package redisbroker

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/redis/go-redis/v9"
)

// 各业务脚本共享 common.lua 前奏(键名工具/状态机表/传播工作队列),Go 侧拼接后加载;
// go-redis 的 Script.Run 自带 EVALSHA→EVAL 降级,首次执行自动上载。
var (
	//go:embed lua/common.lua
	luaCommon string
	//go:embed lua/enqueue.lua
	luaEnqueue string
	//go:embed lua/claim.lua
	luaClaim string
	//go:embed lua/finish.lua
	luaFinish string
	//go:embed lua/heartbeat.lua
	luaHeartbeat string
	//go:embed lua/reap.lua
	luaReap string
	//go:embed lua/replay.lua
	luaReplay string
	//go:embed lua/quota_reserve.lua
	luaQuotaReserve string
	//go:embed lua/quota_release.lua
	luaQuotaRelease string

	scriptEnqueue = redis.NewScript(luaCommon + "\n" + luaEnqueue)
	scriptReplay  = redis.NewScript(luaCommon + "\n" + luaReplay)
	// 配额脚本不拼 common.lua:不碰任务键、不走状态机;TIME 豁免见脚本头注释。
	scriptQuotaReserve = redis.NewScript(luaQuotaReserve)
	scriptQuotaRelease = redis.NewScript(luaQuotaRelease)
	scriptClaim     = redis.NewScript(luaCommon + "\n" + luaClaim)
	scriptFinish    = redis.NewScript(luaCommon + "\n" + luaFinish)
	scriptHeartbeat = redis.NewScript(luaCommon + "\n" + luaHeartbeat)
	scriptReap      = redis.NewScript(luaCommon + "\n" + luaReap)
)

// pollInterval Dequeue 无果时的兜底轮询间隔:同进程写入靠内部唤醒信号即时响应,
// 别的进程写同一个 Redis 时靠这个间隔兜底发现(等待走注入 clock,测试不真 sleep)。
const pollInterval = 100 * time.Millisecond

// Options 连接参数。库不读环境变量,应用自己填好传进来(宪法 I)。
type Options struct {
	Addr     string // Redis 地址,如 "127.0.0.1:6379"
	Password string // 密码,空 = 不认证
	DB       int    // 库号
	// KeyPrefix 所有键的前缀,默认 "tg:";多应用共用一个 Redis 时用它隔离。
	// 例外:RPS 限速状态键不在本前缀命名空间内——redis_rate 自己会再加 "rate:"
	// 前缀,最终键形如 "rate:<KeyPrefix><queue>";按前缀批量清理/统计时别漏了它。
	KeyPrefix string
}

// Broker Redis 后端。实现 taskgate.Broker。
type Broker struct {
	rdb    *redis.Client
	prefix string
	mu     sync.Mutex    // 保护 wakeCh 换代与 inited/closed 标记
	wakeCh chan struct{} // 换代式广播:每次唤醒 close 掉再换一个新的(照 sqlitebroker)
	opts   taskgate.BrokerOptions
	clk    taskgate.Clock
	inited bool
	closed bool

	limMu    sync.Mutex      // 保护 limiters 记账(与 mu 分开,互不嵌套)
	limiters []*queueLimiter // QueueLimiter 发出去的限流器,Close 时统一停续期
}

// 编译期断言:*Broker 必须实现完整的 Broker 接口。
var _ taskgate.Broker = (*Broker)(nil)

// New 按 Options 建连接并 PING 一次探活(配置错误尽早暴露)。
// 返回的 Broker 用之前必须先 Init(由 taskgate.New(cfg) 统一调用)。
func New(opts Options) (*Broker, error) {
	if opts.KeyPrefix == "" {
		opts.KeyPrefix = "tg:"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redisbroker: ping %s: %w", opts.Addr, err)
	}
	return &Broker{rdb: rdb, prefix: opts.KeyPrefix, wakeCh: make(chan struct{})}, nil
}

// Init 装配运行参数,零值补默认(TTL 60s / LeaseLostMax 3 / ThrottledMax 100 / 真时钟)。
func (b *Broker) Init(opts taskgate.BrokerOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if opts.DefaultLeaseTTL <= 0 {
		opts.DefaultLeaseTTL = 60 * time.Second
	}
	if opts.LeaseLostMax <= 0 {
		opts.LeaseLostMax = 3
	}
	if opts.ThrottledMax <= 0 {
		opts.ThrottledMax = 100
	}
	if opts.Clock == nil {
		opts.Clock = taskgate.RealClock()
	}
	b.opts = opts
	b.clk = opts.Clock
	b.inited = true
	return nil
}

// Close 关连接:标记 closed 并广播,让阻塞中的 Dequeue 尽快退出,再关客户端,
// 最后统一停掉所有已发限流器仍在续期的槽(不然续期 goroutine 会一直空转泄漏)。
// 顺序讲究:先关客户端再收尾限流器——归还路径的 ZREM 在关掉的连接上失败被容忍,
// Redis 里的槽按 TTL 自然过期回收,语义与"进程崩溃"一致(槽不许被 Close 抢先删掉,
// 别的进程要等 TTL 才能占,这正是崩溃自愈的既定口径)。
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.wakeCh)
	b.wakeCh = make(chan struct{})
	b.mu.Unlock()
	err := b.rdb.Close()

	b.limMu.Lock()
	lims := b.limiters
	b.limiters = nil
	b.limMu.Unlock()
	for _, l := range lims {
		l.shutdown()
	}
	return err
}

// requireInit 没 Init 就用是接线错误,直接报错而不是悄悄用坏参数跑。
func (b *Broker) requireInit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.inited {
		return errors.New("redisbroker: Init must be called before use")
	}
	if b.closed {
		return errors.New("redisbroker: broker is closed")
	}
	return nil
}

// ttlFor 队列的租约时长:按队列配置,没配走缺省。
func (b *Broker) ttlFor(queue string) time.Duration {
	if d, ok := b.opts.LeaseTTL[queue]; ok && d > 0 {
		return d
	}
	return b.opts.DefaultLeaseTTL
}

// wakeAll 换代式广播:close 旧 channel 唤醒所有等待者,再换一个新的。
// 等价于 memorybroker 的 cond.Broadcast(),同进程的 Dequeue 靠它即时醒来。
func (b *Broker) wakeAll() {
	b.mu.Lock()
	close(b.wakeCh)
	b.wakeCh = make(chan struct{})
	b.mu.Unlock()
}

// wakeChan 取当前代的唤醒信号。Dequeue 必须先取信号再试认领,
// 这样"试完没抢到 → 挂起等待"之间发生的写入不会丢信号。
func (b *Broker) wakeChan() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.wakeCh
}

// fireNotify 在脚本执行之后异步触发状态流转回调,recover 包住:
// 回调 panic/阻塞不能砸主流程(合同要求),与另两后端行为一致。
func (b *Broker) fireNotify(snaps []taskgate.Task) {
	fn := b.opts.Notify
	if fn == nil || len(snaps) == 0 {
		return
	}
	go func() {
		for _, s := range snaps {
			func() {
				defer func() { _ = recover() }()
				fn(s)
			}()
		}
	}()
}

// ---- 键名工具(Go 侧只有 query.go 直接摸键,其余都走 Lua) ----

func (b *Broker) kTask(id string) string     { return b.prefix + "task:" + id }
func (b *Broker) kPending(q string) string   { return b.prefix + "pending:" + q }
func (b *Broker) kDelayed(q string) string   { return b.prefix + "delayed:" + q }
func (b *Broker) kIdxStatus(s string) string { return b.prefix + "idx:status:" + s }
func (b *Broker) kIdxType(typ string) string { return b.prefix + "idx:type:" + typ }
func (b *Broker) kStats() string             { return b.prefix + "stats" }
func (b *Broker) kBk(key string) string      { return b.prefix + "bk:" + key }

// ---- 错误映射(research 第 9 节) ----

// mapLuaErr 把 Lua 返回的 "TGERR:<code>:<detail>" 翻译成哨兵错误;
// 没有 TGERR 前缀的(网络/IO/脚本本身炸了)原样透传,绝不折叠成哨兵。
func mapLuaErr(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	i := strings.Index(msg, "TGERR:")
	if i < 0 {
		return err
	}
	rest := msg[i+len("TGERR:"):]
	code, detail := rest, ""
	if j := strings.Index(rest, ":"); j >= 0 {
		code, detail = rest[:j], rest[j+1:]
	}
	switch code {
	case "exists":
		return fmt.Errorf("%w: %s", taskgate.ErrTaskExists, detail)
	case "bk_exists":
		// detail 形如 "<key>\31<链尾ID>\31<链尾状态>"(\31 分隔,键自身可能含冒号)。
		if parts := strings.SplitN(detail, "\x1f", 3); len(parts) == 3 {
			return &taskgate.TaskExistsError{
				BusinessKey: parts[0],
				ExecutionID: parts[1],
				Status:      taskgate.Status(parts[2]),
			}
		}
		return fmt.Errorf("%w: %s", taskgate.ErrTaskExists, detail)
	case "replay_not_final":
		if parts := strings.SplitN(detail, "\x1f", 2); len(parts) == 2 {
			return fmt.Errorf("%w: %s is %s", taskgate.ErrReplayNotFinal, parts[0], parts[1])
		}
		return fmt.Errorf("%w: %s", taskgate.ErrReplayNotFinal, detail)
	case "already_replayed":
		return fmt.Errorf("%w: %s", taskgate.ErrAlreadyReplayed, detail)
	case "completed_not_allowed":
		return fmt.Errorf("%w: %s", taskgate.ErrCompletedNotAllowed, detail)
	case "not_found":
		return fmt.Errorf("%w: %s", taskgate.ErrTaskNotFound, detail)
	case "lease_lost":
		return fmt.Errorf("%w: %s", taskgate.ErrLeaseLost, detail)
	case "already_final":
		return fmt.Errorf("%w: %s", taskgate.ErrAlreadyFinal, detail)
	case "illegal":
		// detail 形如 "<from>:<to>:<id>",拼成与另两后端同款文案(带 from→to)。
		parts := strings.SplitN(detail, ":", 3)
		if len(parts) == 3 {
			return fmt.Errorf("redisbroker: illegal transition %s -> %s (task %s)", parts[0], parts[1], parts[2])
		}
		return fmt.Errorf("redisbroker: illegal transition %s", detail)
	default:
		return fmt.Errorf("redisbroker: lua error %s: %s", code, detail)
	}
}

// ---- Task ↔ hash 编解码 ----

// ms 时间转 unix 毫秒;零值时间统一存 0(与 sqlite 同约定,fromMS 的反向)。
func ms(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// fromMS 毫秒转回 time.Time;0 还原成零值时间(零值时间落库统一存 0,与 sqlite 同约定)。
func fromMS(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMilli(v).UTC()
}

// decodeTask 把 hash 字段表还原成 Task。所有值都是 Redis 里的字符串:
// 时间是 unix 毫秒、DependsOn 是 JSON 数组文本、计数是十进制整数。
// 内部字段(parents/pending_parents/lease_until/cancel_requested)不进 Task。
func decodeTask(fields map[string]string) (*taskgate.Task, error) {
	t := &taskgate.Task{
		ID:              fields["id"],
		Type:            fields["type"],
		Queue:           fields["queue"],
		Status:          taskgate.Status(fields["status"]),
		LastError:       fields["last_error"],
		OnParentFailure: taskgate.ParentFailurePolicy(fields["on_parent_fail"]),
		LeaseToken:      fields["lease_token"],
		BusinessKey:     fields["business_key"], // 旧数据无此字段,缺省空串(spec 005 升级兼容)
		ReplayOf:        fields["replay_of"],
	}
	if v := fields["payload"]; v != "" {
		t.Payload = []byte(v)
	}
	if v := fields["result"]; v != "" {
		t.Result = []byte(v)
	}
	var err error
	if t.Attempts, err = atoi(fields, "attempts"); err != nil {
		return nil, err
	}
	if t.MaxRetry, err = atoi(fields, "max_retry"); err != nil {
		return nil, err
	}
	if t.LeaseLost, err = atoi(fields, "lease_lost"); err != nil {
		return nil, err
	}
	if t.Throttled, err = atoi(fields, "throttled"); err != nil {
		return nil, err
	}
	for _, f := range []struct {
		name string
		dst  *time.Time
	}{
		{"run_at", &t.RunAt}, {"created_at", &t.CreatedAt},
		{"started_at", &t.StartedAt}, {"finished_at", &t.FinishedAt},
	} {
		n, err := strconv.ParseInt(fields[f.name], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("redisbroker: bad %s of task %s: %w", f.name, t.ID, err)
		}
		*f.dst = fromMS(n)
	}
	if deps := fields["depends_on"]; deps != "" && deps != "[]" && deps != "null" {
		if err := json.Unmarshal([]byte(deps), &t.DependsOn); err != nil {
			return nil, fmt.Errorf("redisbroker: bad depends_on of task %s: %w", t.ID, err)
		}
	}
	return t, nil
}

// atoi 解析 hash 里的整型字段,缺省空串按 0。
func atoi(fields map[string]string, name string) (int, error) {
	v := fields[name]
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("redisbroker: bad %s: %w", name, err)
	}
	return n, nil
}

// flatToMap 把 Lua 返回的 [k1,v1,k2,v2,...] 扁平数组转成字段表。
func flatToMap(vals []any) (map[string]string, error) {
	if len(vals)%2 != 0 {
		return nil, fmt.Errorf("redisbroker: odd hash reply length %d", len(vals))
	}
	m := make(map[string]string, len(vals)/2)
	for i := 0; i < len(vals); i += 2 {
		k, ok1 := vals[i].(string)
		v, ok2 := vals[i+1].(string)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("redisbroker: unexpected hash reply element %T/%T", vals[i], vals[i+1])
		}
		m[k] = v
	}
	return m, nil
}

// decodeSnaps 把脚本返回的快照列表([ [k,v,...], [k,v,...] ])解码成 Task 切片。
func decodeSnaps(res any) ([]taskgate.Task, error) {
	list, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("redisbroker: unexpected snapshots reply %T", res)
	}
	snaps := make([]taskgate.Task, 0, len(list))
	for _, item := range list {
		flat, ok := item.([]any)
		if !ok {
			return nil, fmt.Errorf("redisbroker: unexpected snapshot element %T", item)
		}
		m, err := flatToMap(flat)
		if err != nil {
			return nil, err
		}
		t, err := decodeTask(m)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, *t)
	}
	return snaps, nil
}
