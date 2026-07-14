// Package memorybroker 是 Broker 的内存参考实现:单进程、单 sync.Mutex + sync.Cond。
// 它是三后端的"语义基准":所有状态流转都在同一个锁临界区内完成,
// 等价于 sqlite 的"同一个事务"——终态更新和子任务唤醒天然原子,不可能丢唤醒。
// brokertest 的 18 条契约以它的行为为准。
package memorybroker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ambrose/taskgate"
	"github.com/oklog/ulid/v2"
)

// record 一条任务的存储单元:公开的 Task 快照 + 只在后端内部用的租约/依赖字段
// (对应 sqlite 里的 lease_until / pending_parents / cancel_requested 列)。
type record struct {
	task            taskgate.Task
	pendingParents  int       // 还没到终态的父任务数,减到 0 才唤醒
	leaseUntil      time.Time // 租约到期时刻,ReapExpired 按它回收
	cancelRequested bool      // running 任务的取消标记,Heartbeat 时暴露给 worker
	children        []string  // 反向索引:哪些任务依赖我(入队时登记,唤醒/传播用)
}

// Broker 内存后端。所有读写都拿同一把锁,Cond 用来唤醒阻塞中的 Dequeue。
type Broker struct {
	mu     sync.Mutex
	cond   *sync.Cond
	opts   taskgate.BrokerOptions
	clk    taskgate.Clock
	recs   map[string]*record
	inited bool
	closed bool
}

// New 构造一个空的内存后端;用之前必须先 Init(由 taskgate.New(cfg) 统一调用)。
func New() *Broker {
	b := &Broker{recs: make(map[string]*record)}
	b.cond = sync.NewCond(&b.mu)
	return b
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

// now 当前时刻截到毫秒。合同规定时间戳由 broker 统一按毫秒精度落库
// (sqlite/redis 存 unix 毫秒天然截断),memory 后端在所有写入点跟着截,
// 否则同一毫秒内入队的任务排序会跟另外两个后端不一致。
func (b *Broker) now() time.Time {
	return b.clk.Now().Truncate(time.Millisecond)
}

// ttlFor 队列的租约时长:按队列配置,没配走缺省。
func (b *Broker) ttlFor(queue string) time.Duration {
	if d, ok := b.opts.LeaseTTL[queue]; ok && d > 0 {
		return d
	}
	return b.opts.DefaultLeaseTTL
}

// cloneTask 深拷贝任务:对外只交副本,调用方改了不影响存储(合同要求)。
func cloneTask(t *taskgate.Task) *taskgate.Task {
	c := *t
	if t.Payload != nil {
		c.Payload = append([]byte(nil), t.Payload...)
	}
	if t.Result != nil {
		c.Result = append([]byte(nil), t.Result...)
	}
	if t.DependsOn != nil {
		c.DependsOn = append([]string(nil), t.DependsOn...)
	}
	return &c
}

// fireNotify 在锁外异步触发状态流转回调,recover 包住:回调 panic 不能砸主流程(合同要求)。
// 传入的快照必须是 cloneTask 的深拷贝:回调改快照不能污染存储,与 sqlite 后端行为一致。
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

// setStatus 唯一的状态写入口:先过 canTransition 表,非法流转带 from→to 报错。
func (b *Broker) setStatus(rec *record, to taskgate.Status) error {
	from := rec.task.Status
	if !taskgate.CanTransition(from, to) {
		return fmt.Errorf("memorybroker: illegal transition %s -> %s (task %s)", from, to, rec.task.ID)
	}
	rec.task.Status = to
	return nil
}

// clearLease 清掉租约痕迹(令牌 + 到期时刻)。
func clearLease(rec *record) {
	rec.task.LeaseToken = ""
	rec.leaseUntil = time.Time{}
}

// checkLease 令牌校验三连:任务存在?在 running?令牌一致?
// 不存在 → ErrTaskNotFound;其余不满足 → ErrLeaseLost(合同的统一口径)。
func (b *Broker) checkLease(id, token string) (*record, error) {
	rec, ok := b.recs[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", taskgate.ErrTaskNotFound, id)
	}
	if rec.task.Status != taskgate.StatusRunning || token == "" || rec.task.LeaseToken != token {
		return nil, fmt.Errorf("%w: task %s (status=%s)", taskgate.ErrLeaseLost, id, rec.task.Status)
	}
	return rec, nil
}

// requireInit 没 Init 就用是接线错误,直接报错而不是悄悄用坏参数跑。
func (b *Broker) requireInit() error {
	if !b.inited {
		return errors.New("memorybroker: Init must be called before use")
	}
	if b.closed {
		return errors.New("memorybroker: broker is closed")
	}
	return nil
}

// Enqueue 入队。同一临界区内完成:查重、父存在性校验、初始状态判定、
// 登记依赖反向索引;生成的 ID 会回填到 t.ID。
func (b *Broker) Enqueue(ctx context.Context, t *taskgate.Task) error {
	notifs, err := func() ([]taskgate.Task, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if err := b.requireInit(); err != nil {
			return nil, err
		}
		// ID 先在局部变量里生成/使用:全部校验通过、落库成功后才回填 t.ID,
		// 报错路径不能让调用方拿到一个根本不存在的孤儿 ID。
		id := t.ID
		if id != "" {
			if _, exists := b.recs[id]; exists {
				return nil, fmt.Errorf("%w: %s", taskgate.ErrTaskExists, id)
			}
		} else {
			id = ulid.Make().String()
		}
		now := b.now()

		// 父任务必须全部已存在(依赖无环靠这条提交校验,不做环检测)。
		parents := make([]taskgate.ParentState, 0, len(t.DependsOn))
		for _, pid := range t.DependsOn {
			p, ok := b.recs[pid]
			if !ok {
				return nil, fmt.Errorf("%w: parent %s (child %s)", taskgate.ErrTaskNotFound, pid, id)
			}
			parents = append(parents, taskgate.ParentState{ID: pid, Status: p.task.Status})
		}
		policy := t.OnParentFailure
		if policy == "" {
			policy = taskgate.FailFast
		}
		dec := taskgate.DecideOnSubmit(parents, policy)

		rec := &record{task: *cloneTask(t), pendingParents: dec.PendingParents}
		rec.task.ID = id
		rec.task.Status = dec.Status
		rec.task.OnParentFailure = policy
		rec.task.CreatedAt = now
		// 调用方传入的时间字段(RunAt 与迁移/导入场景预置的 StartedAt/FinishedAt)
		// 同样截毫秒:sqlite/redis 落库即毫秒,memory 必须同精度,读回值三后端一致。
		rec.task.RunAt = rec.task.RunAt.Truncate(time.Millisecond)
		rec.task.StartedAt = rec.task.StartedAt.Truncate(time.Millisecond)
		rec.task.FinishedAt = rec.task.FinishedAt.Truncate(time.Millisecond)
		if rec.task.RunAt.IsZero() {
			rec.task.RunAt = now
		}
		rec.task.LeaseToken = "" // 入队不可能自带租约
		if dec.Status == taskgate.StatusCanceled {
			// 提交时父已失败且 FailFast:直接以 canceled 落库。
			rec.task.LastError = dec.LastError
			rec.task.FinishedAt = now
		}
		b.recs[rec.task.ID] = rec

		// 登记反向索引(去重后的父列表),父到终态时按它找直接子任务。
		seen := make(map[string]bool, len(parents))
		for _, p := range parents {
			if seen[p.ID] {
				continue
			}
			seen[p.ID] = true
			b.recs[p.ID].children = append(b.recs[p.ID].children, rec.task.ID)
		}

		*t = *cloneTask(&rec.task) // 回填生成的 ID 与判定结果,调用方直接可用
		b.cond.Broadcast()         // 可能有 Dequeue 正等着新任务
		return []taskgate.Task{*cloneTask(&rec.task)}, nil
	}()
	b.fireNotify(notifs)
	return err
}

// pickReady 挑一个可认领的任务:status∈{pending,retrying} 且 run_at≤now。
// 取 RunAt 最早的(同刻取 ID 最小),让行为确定、便于排查。
func (b *Broker) pickReady(qset map[string]bool, now time.Time) *record {
	var best *record
	for _, rec := range b.recs {
		st := rec.task.Status
		if !qset[rec.task.Queue] || (st != taskgate.StatusPending && st != taskgate.StatusRetrying) {
			continue
		}
		if rec.task.RunAt.After(now) {
			continue
		}
		if best == nil || rec.task.RunAt.Before(best.task.RunAt) ||
			(rec.task.RunAt.Equal(best.task.RunAt) && rec.task.ID < best.task.ID) {
			best = rec
		}
	}
	return best
}

// nextRunAt 算下一个"到点时刻":没就绪任务时,Dequeue 要挂在 clock 上等它。
func (b *Broker) nextRunAt(qset map[string]bool, now time.Time) time.Time {
	var next time.Time
	for _, rec := range b.recs {
		st := rec.task.Status
		if !qset[rec.task.Queue] || (st != taskgate.StatusPending && st != taskgate.StatusRetrying) {
			continue
		}
		if rec.task.RunAt.After(now) && (next.IsZero() || rec.task.RunAt.Before(next)) {
			next = rec.task.RunAt
		}
	}
	return next
}

// waitLocked 在锁内挂起等待,三种唤醒源:Cond 广播(状态变了)、clock 到点
// (延迟/退避任务就绪)、ctx 取消。合同要求"到点任务也能唤醒阻塞的 Dequeue",
// 所以等待必须同时挂在 clock 上,不能只挂 Cond。
func (b *Broker) waitLocked(ctx context.Context, next, now time.Time) {
	var timerCh <-chan time.Time
	if !next.IsZero() {
		timerCh = b.clk.After(next.Sub(now)) // nil channel 永远阻塞,没有到点任务时只等广播
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-timerCh:
		case <-done:
			return
		}
		// 先拿锁再广播:保证此刻等待者确实已挂在 cond.Wait 上(它释放了锁),不会丢唤醒。
		b.mu.Lock()
		b.cond.Broadcast()
		b.mu.Unlock()
	}()
	b.cond.Wait()
	close(done) // 收掉守护 goroutine,防泄漏
}

// Dequeue 阻塞认领:直到某队列出现就绪任务,或 ctx 取消(返回 ctx.Err())。
// 认领本身原子:置 running、发新令牌、记租约、首次写 StartedAt。
func (b *Broker) Dequeue(ctx context.Context, queues []string) (*taskgate.Task, error) {
	if len(queues) == 0 {
		return nil, errors.New("memorybroker: dequeue needs at least one queue")
	}
	qset := make(map[string]bool, len(queues))
	for _, q := range queues {
		qset[q] = true
	}

	b.mu.Lock()
	for {
		if err := ctx.Err(); err != nil {
			b.mu.Unlock()
			return nil, err
		}
		if err := b.requireInit(); err != nil {
			b.mu.Unlock()
			return nil, err
		}
		now := b.now()
		if rec := b.pickReady(qset, now); rec != nil {
			if err := b.setStatus(rec, taskgate.StatusRunning); err != nil {
				b.mu.Unlock()
				return nil, err
			}
			rec.task.LeaseToken = ulid.Make().String() // 每次认领都发全新令牌
			rec.leaseUntil = now.Add(b.ttlFor(rec.task.Queue))
			if rec.task.StartedAt.IsZero() {
				rec.task.StartedAt = now // 只记首次开跑
			}
			snap := cloneTask(&rec.task)
			b.mu.Unlock()
			b.fireNotify([]taskgate.Task{*cloneTask(snap)})
			return snap, nil
		}
		b.waitLocked(ctx, b.nextRunAt(qset, now), now)
	}
}

// propagateFinal 任务进终态后的依赖传播,和触发它的状态写入同处一个锁临界区
// (等价 sqlite 的同事务),这是"不丢唤醒"的生命线。
// 用工作队列逐层处理:每层只碰直接子任务,子被连锁取消后再入队处理孙,不递归整棵树。
func (b *Broker) propagateFinal(start *record, now time.Time, notifs *[]taskgate.Task) {
	work := []*record{start}
	for len(work) > 0 {
		p := work[0]
		work = work[1:]
		for _, cid := range p.children {
			c, ok := b.recs[cid]
			if !ok {
				continue
			}
			newPending, action := taskgate.DecideOnParentFinal(
				p.task.Status, c.task.Status, c.task.OnParentFailure, c.pendingParents)
			c.pendingParents = newPending
			switch action {
			case taskgate.ChildWake:
				if err := b.setStatus(c, taskgate.StatusPending); err == nil {
					*notifs = append(*notifs, *cloneTask(&c.task))
				}
			case taskgate.ChildCancel:
				if c.task.Status == taskgate.StatusRunning {
					// 防御:正常流程里子等父时不可能在跑;万一出现,打取消标记走 Heartbeat 通道。
					c.cancelRequested = true
					continue
				}
				if err := b.setStatus(c, taskgate.StatusCanceled); err == nil {
					c.task.LastError = taskgate.ParentFailureReason(p.task.ID, p.task.Status)
					c.task.FinishedAt = now
					clearLease(c)
					*notifs = append(*notifs, *cloneTask(&c.task))
					work = append(work, c) // 链式:这个子也终态了,接着处理它的直接子任务
				}
			}
		}
	}
}

// Ack 成功完结:completed + Result + FinishedAt,并在同一临界区内唤醒子任务。
func (b *Broker) Ack(ctx context.Context, id, leaseToken string, result []byte) error {
	notifs, err := func() ([]taskgate.Task, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if err := b.requireInit(); err != nil {
			return nil, err
		}
		rec, err := b.checkLease(id, leaseToken)
		if err != nil {
			return nil, err
		}
		if err := b.setStatus(rec, taskgate.StatusCompleted); err != nil {
			return nil, err
		}
		now := b.now()
		if result != nil {
			rec.task.Result = append([]byte(nil), result...)
		}
		rec.task.FinishedAt = now
		clearLease(rec)
		rec.cancelRequested = false
		notifs := []taskgate.Task{*cloneTask(&rec.task)}
		b.propagateFinal(rec, now, &notifs)
		b.cond.Broadcast() // 被唤醒的子任务可能正被 Dequeue 等着
		return notifs, nil
	}()
	b.fireNotify(notifs)
	return err
}

// Fail 失败路径:按 FailKind 动对应计数,封顶或耗尽进 failed(触发传播),否则 retrying。
func (b *Broker) Fail(ctx context.Context, id, leaseToken, errMsg string, kind taskgate.FailKind, retryAt time.Time) error {
	notifs, err := func() ([]taskgate.Task, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if err := b.requireInit(); err != nil {
			return nil, err
		}
		rec, err := b.checkLease(id, leaseToken)
		if err != nil {
			return nil, err
		}
		now := b.now()

		toFailed := false
		rec.task.LastError = errMsg
		switch kind {
		case taskgate.FailBusiness:
			// 业务失败:只动 Attempts,超过 MaxRetry 就没救了。
			rec.task.Attempts++
			toFailed = rec.task.Attempts > rec.task.MaxRetry
		case taskgate.FailThrottled:
			// 被网关限流:只动 Throttled,Attempts 一根汗毛都不动。
			rec.task.Throttled++
			if rec.task.Throttled >= b.opts.ThrottledMax {
				toFailed = true
				rec.task.LastError = fmt.Sprintf("throttled %d times", rec.task.Throttled) // 封顶用固定文案
			}
		case taskgate.FailSkip:
			toFailed = true // 明确不重试,计数全不动
		default:
			return nil, fmt.Errorf("memorybroker: unknown FailKind %d", kind)
		}

		if toFailed {
			if err := b.setStatus(rec, taskgate.StatusFailed); err != nil {
				return nil, err
			}
			rec.task.FinishedAt = now
			clearLease(rec)
			rec.cancelRequested = false
			notifs := []taskgate.Task{*cloneTask(&rec.task)}
			b.propagateFinal(rec, now, &notifs) // failed 也要在同一临界区里连锁处理子任务
			b.cond.Broadcast()
			return notifs, nil
		}

		// 还有机会:进 retrying,到点(retryAt)才能被重新认领。
		if err := b.setStatus(rec, taskgate.StatusRetrying); err != nil {
			return nil, err
		}
		if retryAt.IsZero() {
			retryAt = now
		}
		rec.task.RunAt = retryAt.Truncate(time.Millisecond) // 与 sqlite/redis 的毫秒落库同精度
		clearLease(rec)
		b.cond.Broadcast() // 让等待中的 Dequeue 重算下一个到点时刻
		return []taskgate.Task{*cloneTask(&rec.task)}, nil
	}()
	b.fireNotify(notifs)
	return err
}

// Cancel 取消:排队类状态直接 canceled 并传播;running 只打标记(终态由 FinishCanceled 落);
// 终态报 ErrAlreadyFinal,不存在报 ErrTaskNotFound。
func (b *Broker) Cancel(ctx context.Context, id string) error {
	notifs, err := func() ([]taskgate.Task, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if err := b.requireInit(); err != nil {
			return nil, err
		}
		rec, ok := b.recs[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", taskgate.ErrTaskNotFound, id)
		}
		if rec.task.Status.IsFinal() {
			return nil, fmt.Errorf("%w: task %s (status=%s)", taskgate.ErrAlreadyFinal, id, rec.task.Status)
		}
		if rec.task.Status == taskgate.StatusRunning {
			rec.cancelRequested = true // worker 下次 Heartbeat 会收到 ErrTaskCanceled
			return nil, nil
		}
		if err := b.setStatus(rec, taskgate.StatusCanceled); err != nil {
			return nil, err
		}
		now := b.now()
		rec.task.LastError = "canceled"
		rec.task.FinishedAt = now
		clearLease(rec)
		notifs := []taskgate.Task{*cloneTask(&rec.task)}
		b.propagateFinal(rec, now, &notifs) // 向下传播:FailFast 子连锁取消
		b.cond.Broadcast()
		return notifs, nil
	}()
	b.fireNotify(notifs)
	return err
}

// FinishCanceled worker 响应取消后收尾:running → canceled 落库并传播。
func (b *Broker) FinishCanceled(ctx context.Context, id, leaseToken string) error {
	notifs, err := func() ([]taskgate.Task, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if err := b.requireInit(); err != nil {
			return nil, err
		}
		rec, err := b.checkLease(id, leaseToken)
		if err != nil {
			return nil, err
		}
		if err := b.setStatus(rec, taskgate.StatusCanceled); err != nil {
			return nil, err
		}
		now := b.now()
		rec.task.LastError = "canceled"
		rec.task.FinishedAt = now
		clearLease(rec)
		rec.cancelRequested = false
		notifs := []taskgate.Task{*cloneTask(&rec.task)}
		b.propagateFinal(rec, now, &notifs)
		b.cond.Broadcast()
		return notifs, nil
	}()
	b.fireNotify(notifs)
	return err
}

// Requeue 优雅停机时归还任务:running → pending,三计数与 RunAt 全不动,
// 清租约和取消标记。这不算失败,一个计数都不许占(合同要求)。
func (b *Broker) Requeue(ctx context.Context, id, leaseToken string) error {
	notifs, err := func() ([]taskgate.Task, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if err := b.requireInit(); err != nil {
			return nil, err
		}
		rec, err := b.checkLease(id, leaseToken)
		if err != nil {
			return nil, err
		}
		if err := b.setStatus(rec, taskgate.StatusPending); err != nil {
			return nil, err
		}
		clearLease(rec)
		rec.cancelRequested = false
		b.cond.Broadcast()
		return []taskgate.Task{*cloneTask(&rec.task)}, nil
	}()
	b.fireNotify(notifs)
	return err
}

// Heartbeat 续租:lease_until = now + TTL。发现取消标记时续租照做,
// 但返回 ErrTaskCanceled 提醒 scheduler 去 cancel handler 的 ctx。
func (b *Broker) Heartbeat(ctx context.Context, id, leaseToken string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.requireInit(); err != nil {
		return err
	}
	rec, err := b.checkLease(id, leaseToken)
	if err != nil {
		return err
	}
	rec.leaseUntil = b.now().Add(b.ttlFor(rec.task.Queue))
	if rec.cancelRequested {
		return fmt.Errorf("%w: task %s", taskgate.ErrTaskCanceled, id)
	}
	return nil
}

// Get 取单个任务的副本。
func (b *Broker) Get(ctx context.Context, id string) (*taskgate.Task, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.recs[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", taskgate.ErrTaskNotFound, id)
	}
	return cloneTask(&rec.task), nil
}

// List 按 Filter 过滤,零值字段不过滤;先过滤 → 按 (CreatedAt, ID) 升序 → 跳过
// Offset 再取 Limit(排序分页合同见 broker-contract.md,Offset 越界返回空)。
func (b *Broker) List(ctx context.Context, f taskgate.Filter) ([]*taskgate.Task, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []*taskgate.Task
	for _, rec := range b.recs {
		t := &rec.task
		if f.Type != "" && t.Type != f.Type {
			continue
		}
		if f.Queue != "" && t.Queue != f.Queue {
			continue
		}
		if f.Status != "" && t.Status != f.Status {
			continue
		}
		out = append(out, cloneTask(t))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return pageSlice(out, f), nil
}

// pageSlice 排序后的结果按 Offset/Limit 切页:Offset<0 按 0 处理(宽容读接口),
// 越界返回空;Limit=0 不限量。
func pageSlice(out []*taskgate.Task, f taskgate.Filter) []*taskgate.Task {
	off := f.Offset
	if off < 0 {
		off = 0
	}
	if off >= len(out) {
		return nil
	}
	out = out[off:]
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out
}

// QueueLen 队列积压:status∈{pending,retrying} 的数量(不看 RunAt 到没到点)。
func (b *Broker) QueueLen(ctx context.Context, queue string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, rec := range b.recs {
		st := rec.task.Status
		if rec.task.Queue == queue && (st == taskgate.StatusPending || st == taskgate.StatusRetrying) {
			n++
		}
	}
	return n, nil
}

// Counts 出现过的 Type×Status 稀疏矩阵,和逐个 Get 汇总必须一致(brokertest 验证)。
func (b *Broker) Counts(ctx context.Context) (map[string]map[taskgate.Status]int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]map[taskgate.Status]int64)
	for _, rec := range b.recs {
		byStatus := out[rec.task.Type]
		if byStatus == nil {
			byStatus = make(map[taskgate.Status]int64)
			out[rec.task.Type] = byStatus
		}
		byStatus[rec.task.Status]++
	}
	return out, nil
}

// ReapExpired 回收过期租约:带取消标记的直接落 canceled(不占 LeaseLost,触发传播);
// 其余 LeaseLost+1,封顶进 failed(触发传播),否则回 pending。
// 顺带做防御性修复:blocked 但父实际全部终态的任务,按正常规则补唤醒/补取消
// (这不是正常路径,是给"唤醒中途崩"这类事故兜底)。返回值只算租约回收条数。
func (b *Broker) ReapExpired(ctx context.Context) (int, error) {
	count, notifs, err := func() (int, []taskgate.Task, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if err := b.requireInit(); err != nil {
			return 0, nil, err
		}
		now := b.now()
		var notifs []taskgate.Task
		count := 0

		// 第一步:回收过期租约(lease_until < now 严格小于,压线不算过期)。
		for _, rec := range b.recs {
			if rec.task.Status != taskgate.StatusRunning || !rec.leaseUntil.Before(now) {
				continue
			}
			if rec.cancelRequested {
				// 用户已请求取消,而此刻租约过期、没有任何 worker 持有它:
				// 直接落 canceled(不占 LeaseLost),取消不能因为 worker 崩了就凭空丢失。
				if err := b.setStatus(rec, taskgate.StatusCanceled); err != nil {
					return count, notifs, err
				}
				rec.task.LastError = "canceled"
				rec.task.FinishedAt = now
				clearLease(rec)
				rec.cancelRequested = false
				notifs = append(notifs, *cloneTask(&rec.task))
				b.propagateFinal(rec, now, &notifs) // canceled 同样要连锁处理子任务
				count++
				continue
			}
			rec.task.LeaseLost++
			clearLease(rec)
			rec.cancelRequested = false
			if rec.task.LeaseLost >= b.opts.LeaseLostMax {
				if err := b.setStatus(rec, taskgate.StatusFailed); err != nil {
					return count, notifs, err
				}
				rec.task.LastError = fmt.Sprintf("lease expired %d times", rec.task.LeaseLost)
				rec.task.FinishedAt = now
				notifs = append(notifs, *cloneTask(&rec.task))
				b.propagateFinal(rec, now, &notifs) // 封顶 failed 同样要连锁处理子任务
			} else {
				if err := b.setStatus(rec, taskgate.StatusPending); err != nil {
					return count, notifs, err
				}
				notifs = append(notifs, *cloneTask(&rec.task))
			}
			count++
		}

		// 第二步:防御修复。blocked 却发现父全是终态 → 用和提交时同一套决策函数补齐。
		for _, rec := range b.recs {
			if rec.task.Status != taskgate.StatusBlocked {
				continue
			}
			parents := make([]taskgate.ParentState, 0, len(rec.task.DependsOn))
			allExist := true
			for _, pid := range rec.task.DependsOn {
				p, ok := b.recs[pid]
				if !ok {
					allExist = false // 父记录都没了,没法判定,跳过不硬修
					break
				}
				parents = append(parents, taskgate.ParentState{ID: pid, Status: p.task.Status})
			}
			if !allExist {
				continue
			}
			dec := taskgate.DecideOnSubmit(parents, rec.task.OnParentFailure)
			switch dec.Status {
			case taskgate.StatusPending:
				if err := b.setStatus(rec, taskgate.StatusPending); err == nil {
					rec.pendingParents = 0
					notifs = append(notifs, *cloneTask(&rec.task))
				}
			case taskgate.StatusCanceled:
				if err := b.setStatus(rec, taskgate.StatusCanceled); err == nil {
					rec.task.LastError = dec.LastError
					rec.task.FinishedAt = now
					notifs = append(notifs, *cloneTask(&rec.task))
					b.propagateFinal(rec, now, &notifs)
				}
			default:
				rec.pendingParents = dec.PendingParents // 还该等着,顺手校准计数
			}
		}

		if len(notifs) > 0 {
			b.cond.Broadcast() // 有任务回 pending / 被唤醒,叫醒等待中的 Dequeue
		}
		return count, notifs, nil
	}()
	b.fireNotify(notifs)
	return count, err
}

// Close 关闭:标记 closed 并广播,让所有阻塞中的 Dequeue 尽快退出。内存后端没有别的资源要释放。
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.cond.Broadcast()
	return nil
}
