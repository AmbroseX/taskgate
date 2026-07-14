package taskgate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// dequeueRetryWait 后端 Dequeue 出非 ctx 错误时的冷却时间,避免热循环打爆后端。
const dequeueRetryWait = 100 * time.Millisecond

// scheduler 消费侧编排:每个队列一条认领循环 + Workers 个并发槽,
// 拿到任务后按 Type 找 handler 执行,成功 Ack、失败按错误分类 Fail。
// 每个在跑任务配一条心跳 goroutine 自动续租(LeaseTTL/3),Run 期间有全局 reaper
// 定期回收过期租约;Cancel 通过 id→cancelFunc 表即时打断本进程在跑的 handler。
type scheduler struct {
	gate *Gate

	mu      sync.Mutex
	started bool
	// stopClaims 本轮 Run 的内部取消:Shutdown 靠它停掉认领循环和 reaper。没在 Run 时为 nil。
	stopClaims context.CancelFunc
	// runExited 本轮 Run 完全收尾(worker/reaper 全退出)后关闭,Shutdown 等它。
	runExited chan struct{}
	// running 每队列"本进程正在执行的任务数",给 Stats 用;没 Run 过的队列读到 0。
	running map[string]*atomic.Int64

	// tasks 本进程在跑任务的取消句柄表(id → runningTask),Gate.Cancel 靠它即时打断。
	tasksMu sync.Mutex
	tasks   map[string]*runningTask
	// draining Shutdown 已超时、正在打断在跑任务:此后新登记的任务也一律打断走 Requeue
	// (堵住"任务刚认领还没来得及登记,躲过了 Shutdown 那一轮扫描"的窗口)。
	draining bool
}

// runningTask 一个在跑任务的运行期状态,心跳 goroutine 和 handler 收尾逻辑共享。
type runningTask struct {
	cancel    context.CancelFunc
	canceled  atomic.Bool // 被请求取消(本地 Cancel 即时打断,或 Heartbeat 发现取消标记)
	leaseLost atomic.Bool // 租约已丢:任务已被 reaper 处理掉,handler 的结果必须作废
	requeue   atomic.Bool // Shutdown 超时打断:handler 退出后走 Requeue 归还,不算取消也不算失败
}

func newScheduler(g *Gate) *scheduler {
	return &scheduler{
		gate:    g,
		running: make(map[string]*atomic.Int64),
		tasks:   make(map[string]*runningTask),
	}
}

// track / untrack 登记与注销在跑任务的取消句柄。
// Shutdown 超时扫描之后才登记进来的任务(认领和扫描赛跑的窗口),在这里补打断。
func (s *scheduler) track(id string, rt *runningTask) {
	s.tasksMu.Lock()
	defer s.tasksMu.Unlock()
	s.tasks[id] = rt
	if s.draining {
		rt.requeue.Store(true)
		rt.cancel()
	}
}

// untrack 只在表里还是"自己那条"时才删:任务 Fail 落库后、untrack 执行前,
// 同一 ID 可能已被 claimLoop 重新认领并 track 了新句柄,直接按 ID 删会把新句柄误删,
// 让 Gate.Cancel 对新一轮执行的即时打断失效。
func (s *scheduler) untrack(id string, rt *runningTask) {
	s.tasksMu.Lock()
	defer s.tasksMu.Unlock()
	if s.tasks[id] == rt {
		delete(s.tasks, id)
	}
}

// cancelLocal 任务正在本进程跑的话,立即 cancel 它的 ctx(Gate.Cancel 的即时路径)。
// 不在本进程跑(或还没登记)也没关系:broker 已打了取消标记,下一次 Heartbeat 会兜底。
func (s *scheduler) cancelLocal(id string) {
	s.tasksMu.Lock()
	rt := s.tasks[id]
	s.tasksMu.Unlock()
	if rt != nil {
		rt.canceled.Store(true)
		rt.cancel()
	}
}

// runningCount 读某队列当前在跑任务数(纯生产者 Gate 恒为 0)。
func (s *scheduler) runningCount(queue string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.running[queue]; ok {
		return int(c.Load())
	}
	return 0
}

// consumeQueues 算出要消费哪些队列:只有"注册过 handler 的 Type"对应的队列才起认领循环。
// 多个 Type 可以路由到同一队列,所以结果按队列去重;某个 Type 路由不到可用队列直接报错。
func (s *scheduler) consumeQueues() (map[string]QueueConfig, error) {
	g := s.gate
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make(map[string]QueueConfig, len(g.handlers))
	for typ := range g.handlers {
		name, qc, err := g.queueFor(typ)
		if err != nil {
			return nil, err
		}
		out[name] = qc
	}
	return out, nil
}

// run 消费主入口:每队列起一条认领循环,阻塞到 ctx 取消或 Shutdown,
// 然后停止认领、等所有在跑的 handler 退出后返回 nil。
func (s *scheduler) run(ctx context.Context) error {
	queues, err := s.consumeQueues()
	if err != nil {
		return err
	}
	if len(queues) == 0 {
		return errors.New("taskgate: run: no handler registered, nothing to consume")
	}

	// 内部 ctx:外层 ctx 取消或 Shutdown 调 stopClaims 都能停掉认领循环和 reaper。
	runCtx, stopClaims := context.WithCancel(ctx)
	defer stopClaims()
	runExited := make(chan struct{})

	s.mu.Lock()
	if s.gate.isShutdown() {
		// Shutdown 之后再 Run 是接线错误:认领循环一旦起来就没人负责停它了。
		s.mu.Unlock()
		return ErrShutdown
	}
	if s.started {
		s.mu.Unlock()
		return errors.New("taskgate: run: already running")
	}
	s.started = true
	s.stopClaims = stopClaims
	s.runExited = runExited
	// 构造每队列限流器:后端实现 LimiterProvider(可选能力接口)就用后端给的
	// 跨进程共享限流器,否则退回进程内限流——memory/sqlite 走后一条路,行为与 M1 一致。
	// 放在锁内做:构造失败时状态还没对外暴露,回滚后 Shutdown/再次 Run 都不受影响。
	limiters := make(map[string]QueueLimiter, len(queues))
	lp, hasLP := s.gate.broker.(LimiterProvider)
	for name, qc := range queues {
		if _, ok := s.running[name]; !ok {
			s.running[name] = &atomic.Int64{}
		}
		if hasLP {
			ql, lerr := lp.QueueLimiter(name, qc)
			if lerr != nil {
				// 构造失败:回滚启动标记,Run 返回该错误,之后还能重新 Run。
				s.started = false
				s.stopClaims = nil
				s.runExited = nil
				s.mu.Unlock()
				return lerr
			}
			limiters[name] = ql
			continue
		}
		limiters[name] = newLocalLimiter(qc.Workers, qc.RPS, qc.Burst)
	}
	s.mu.Unlock()
	ctx = runCtx

	// 全局 reaper:周期 = min(各消费队列 LeaseTTL)/2,定期捞回过期租约(T019)。
	minTTL := time.Duration(0)
	for _, qc := range queues {
		if ttl := time.Duration(qc.LeaseTTL); minTTL == 0 || ttl < minTTL {
			minTTL = ttl
		}
	}
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		s.reapLoop(ctx, minTTL/2)
	}()

	var workerWG sync.WaitGroup // 在跑的 handler
	var claimWG sync.WaitGroup  // 认领循环
	for name, qc := range queues {
		claimWG.Add(1)
		go func(queue string, ttl time.Duration) {
			defer claimWG.Done()
			s.claimLoop(ctx, queue, limiters[queue], &workerWG, ttl)
		}(name, time.Duration(qc.LeaseTTL))
	}
	claimWG.Wait()  // ctx 取消/Shutdown 后认领循环全部退出
	workerWG.Wait() // 等在跑任务收尾(不打断 handler;Shutdown 超时打断走 requeue 标记)
	<-reaperDone    // reaper 随 Run 结束一起停

	s.mu.Lock()
	s.started = false
	s.stopClaims = nil
	s.runExited = nil
	s.mu.Unlock()
	close(runExited) // 告诉 Shutdown:全部后台 goroutine 已收尾
	return nil
}

// shutdown Shutdown 的编排本体(Gate.Shutdown 保证只进来一次):
//  1. 停认领:cancel 内部 runCtx,认领循环、限流等待、reaper 一起停;
//  2. 等在跑任务善终(runExited 在 workerWG.Wait 之后才关);
//  3. ctx 先到期:给所有在跑任务打 requeue 标记并 cancel 其 ctx,
//     handler 退出后由 execute 调 Broker.Requeue 归还(不占任何计数),
//     等收尾完成后返回 ctx 的超时错误。
//
// 没在 Run(纯生产者)时没有在跑任务,直接返回 nil。
func (s *scheduler) shutdown(ctx context.Context) error {
	s.mu.Lock()
	stopClaims := s.stopClaims
	runExited := s.runExited
	s.mu.Unlock()
	if stopClaims == nil {
		return nil // 没有消费循环在跑,停机标记已生效,没别的要等
	}
	stopClaims()

	select {
	case <-runExited:
		return nil // 全部在跑任务善终,后台 goroutine 已收尾
	case <-ctx.Done():
	}

	// 超时:打断所有在跑任务。requeue 标记必须先打再 cancel,
	// 保证 execute 看到 ctx 被取消时一定能读到标记,不会误走 failTask。
	s.tasksMu.Lock()
	s.draining = true
	for _, rt := range s.tasks {
		rt.requeue.Store(true)
		rt.cancel()
	}
	s.tasksMu.Unlock()

	<-runExited // handler 退出 → Requeue 落库 → worker/reaper 全收尾,零泄漏
	return ctx.Err()
}

// claimLoop 单队列认领循环。
// 顺序写死:先占并发槽,再等 RPS 令牌,最后才 Dequeue。
// 为什么先占槽:令牌桶按时间匀速放行,拿了令牌却没槽跑,等槽期间这个令牌等于白烧
// (启动配额被浪费),Workers 满载时实际吞吐会低于配置的 RPS;
// 先占槽保证"每个令牌都花在马上能跑的任务上"。
func (s *scheduler) claimLoop(ctx context.Context, queue string, lim QueueLimiter, workerWG *sync.WaitGroup, ttl time.Duration) {
	cnt := s.running[queue]
	for {
		if err := lim.AcquireSlot(ctx); err != nil {
			return // ctx 取消,停止认领
		}
		if err := lim.WaitToken(ctx); err != nil {
			lim.ReleaseSlot()
			return
		}
		// 先拿令牌再 Dequeue:队列空闲期会预烧至多 1 个令牌等在 Dequeue 上,
		// Dequeue 出错也白烧 1 个,这点偏差在 M1 spec SC-001 的 ±1 容差内。
		t, err := s.gate.broker.Dequeue(ctx, []string{queue})
		if err != nil {
			lim.ReleaseSlot()
			if ctx.Err() != nil {
				return
			}
			// 后端抖了一下:冷却后重试,不能空转打爆它。
			_ = s.gate.clock.Sleep(ctx, dequeueRetryWait)
			continue
		}
		cnt.Add(1)
		workerWG.Add(1)
		go func() {
			defer func() {
				cnt.Add(-1)
				lim.ReleaseSlot()
				workerWG.Done()
			}()
			s.execute(t, ttl)
		}()
	}
}

// reapLoop 全局 reaper:每 interval 调一次 Broker.ReapExpired,把过期租约的任务捞回。
// 用 Background ctx 调后端:ctx 只管"什么时候停",不该让回收动作半途被打断。
func (s *scheduler) reapLoop(ctx context.Context, interval time.Duration) {
	tk := s.gate.clock.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C():
			_, _ = s.gate.broker.ReapExpired(context.Background()) // 后端抖一下容忍,下个周期再试
		}
	}
}

// heartbeatLoop 单个在跑任务的心跳:每 interval(=LeaseTTL/3)续租一次,直到 done 关闭。
// 三种回音三种处理:
//   - ErrTaskCanceled:任务被外部 Cancel 了(续租照做),cancel handler 的 ctx,继续跳;
//   - ErrLeaseLost / ErrTaskNotFound:租约已经没了(reaper 把任务处理掉了),
//     标记"租约已丢"让结果作废,cancel ctx 让 handler 尽早退出,心跳没必要再跳;
//   - 其他错误(网络抖动等):容忍,下一个周期再试。
func (s *scheduler) heartbeatLoop(done <-chan struct{}, t *Task, rt *runningTask, interval time.Duration) {
	tk := s.gate.clock.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-done:
			return
		case <-tk.C():
			err := s.gate.broker.Heartbeat(context.Background(), t.ID, t.LeaseToken)
			switch {
			case err == nil:
			case errors.Is(err, ErrTaskCanceled):
				rt.canceled.Store(true)
				rt.cancel()
			case errors.Is(err, ErrLeaseLost), errors.Is(err, ErrTaskNotFound):
				rt.leaseLost.Store(true)
				rt.cancel()
				return
			}
		}
	}
}

// execute 执行单个已认领的任务:找 handler → 起心跳 → 跑 → 按退出原因回执。
// 回执(Ack/Fail/FinishCanceled)用 Background ctx:哪怕 Run 的 ctx 已取消,
// 跑完的结果也必须落库。三种退出三种回执(T019/T024):
//   - 租约已丢 → 丢弃结果,不回执(reaper 已把任务处理掉,旧结果不许覆盖新事实);
//   - 被取消且 ctx 确实被 cancel → FinishCanceled 落库 canceled;
//   - 正常返回 → Ack;返回错误 → failTask 按错误分类 Fail。
func (s *scheduler) execute(t *Task, ttl time.Duration) {
	ctx := context.Background()

	h := s.gate.handlerFor(t.Type)
	if h == nil {
		// 认领是按队列的,同队列里可能混着没注册 handler 的 Type。
		// 没法"退回不认领"(Requeue 会立刻被自己再抢到,热循环),
		// 裁决:按 FailSkip 落死信,LastError 用 ErrUnknownType 的文案做前缀,
		// 调用方能靠文案对上这个哨兵错误;可查可手动重放。
		_ = s.gate.broker.Fail(ctx, t.ID, t.LeaseToken,
			fmt.Sprintf("%s: %s", ErrUnknownType.Error(), t.Type), FailSkip, time.Time{})
		return
	}

	// handler 的 ctx 独立于 Run 的 ctx:Run 停止时在跑任务要善终,不被连坐取消。
	// cancel 句柄登记进 tasks 表,Gate.Cancel 靠它对本进程在跑任务即时生效。
	taskCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := &runningTask{cancel: cancel}
	s.track(t.ID, rt)
	defer s.untrack(t.ID, rt)

	// 心跳 goroutine:handler 退出后必须先停心跳再回执,
	// 否则回执把任务写成终态后心跳还在飞,会白吃一堆 ErrLeaseLost。
	hbDone := make(chan struct{})
	hbExited := make(chan struct{})
	go func() {
		defer close(hbExited)
		s.heartbeatLoop(hbDone, t, rt, ttl/3)
	}()

	result, err := s.runHandler(taskCtx, h, t)
	close(hbDone)
	<-hbExited // 等心跳彻底退出,防泄漏(硬性约束)

	switch {
	case rt.leaseLost.Load():
		// 租约已丢:reaper 已经把任务回收(回 pending 重跑或封顶 failed),
		// 这份结果作废,Ack/Fail 都不许发。
	case rt.canceled.Load() && taskCtx.Err() != nil:
		// 取消导致的退出:以 canceled 落库并触发依赖传播。
		// 注意 canceled 标记只有"用户取消"路径会打(cancelLocal / Heartbeat 回音),
		// Shutdown 的打断不打它,所以停机不会被误判成用户取消。
		_ = s.gate.broker.FinishCanceled(ctx, t.ID, t.LeaseToken)
	case err == nil:
		// 正常干完的结果照收(哪怕 Shutdown 已打断:活都干完了,归还回去重跑才是浪费)。
		_ = s.gate.broker.Ack(ctx, t.ID, t.LeaseToken, result)
	case rt.requeue.Load() && taskCtx.Err() != nil:
		// Shutdown 超时打断:没干完不算失败,原样归还回 pending,
		// Attempts/LeaseLost/Throttled/RunAt 一个都不动(Requeue 合同)。
		_ = s.gate.broker.Requeue(ctx, t.ID, t.LeaseToken)
	default:
		s.failTask(ctx, t, err)
	}
}

// runHandler 包一层 recover:handler panic 按业务失败处理,不许砸掉调度器。
func (s *scheduler) runHandler(ctx context.Context, h Handler, t *Task) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("taskgate: handler panic: %v", r)
		}
	}()
	return h(ctx, t)
}

// failTask handler 出错后的重试编排,错误分类三路(T017):
//   - ErrSkipRetry:明确没救,FailSkip 直接死信;
//   - ErrThrottled:网关限流,按 RetryAfter 延后重排,FailThrottled(不占 Attempts);
//   - 其余(含 panic):FailBusiness,退避 backoff(Attempts) 后重试。
//
// 必须先判 ErrSkipRetry:它带 Unwrap,如果先判 ErrThrottled,
// ErrSkipRetry{Err: ErrThrottled{...}} 会穿透匹配到内层限流走无限重排,
// 违背"明确不重试"的意图。
//
// t.Attempts 是认领时的快照(本次失败还没 +1),所以首次失败传 backoff(0)=1s 起步。
func (s *scheduler) failTask(ctx context.Context, t *Task, herr error) {
	now := s.gate.clock.Now()
	var thr ErrThrottled
	var skip ErrSkipRetry
	switch {
	case errors.As(herr, &skip):
		_ = s.gate.broker.Fail(ctx, t.ID, t.LeaseToken, herr.Error(),
			FailSkip, time.Time{})
	case errors.As(herr, &thr):
		_ = s.gate.broker.Fail(ctx, t.ID, t.LeaseToken, herr.Error(),
			FailThrottled, now.Add(thr.RetryAfter))
	default:
		_ = s.gate.broker.Fail(ctx, t.ID, t.LeaseToken, herr.Error(),
			FailBusiness, now.Add(s.gate.backoff(t.Attempts)))
	}
}
