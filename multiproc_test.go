// multiproc_test.go 多进程"恰好执行一次"专项(M2 US2,T108/T109,`go test -short` 跳过):
//
//	① 双进程恰好一次:两个子进程 worker 抢同一批 1000 个任务,执行记录恰好 1000 条且每任务恰好 1 次;
//	② kill -9 回收:子进程认领后被杀,新进程经 reaper 按租约过期回收重跑,LeaseLost=1 最终 completed;
//	③ 断连恢复(T109):worker 经本地 TCP 代理连 Redis,掐断代理模拟网络断连,
//	   恢复后消费循环自动续上,20 个任务全部 completed 不丢失。
//
// miniredis 起在父测试进程(它监听真实 TCP 端口,子进程连得上);"多进程"沿用
// sqlitebroker/crash_test.go 的成熟模式:exec 自身测试二进制 + 环境变量开关 + 哨兵文件。
// 子进程用不了 fakeclock(跨进程没法推进假时间),全部走真时钟 + 秒级短租约;
// 父进程的轮询断言一律带超时(waitFor),防挂死。
package taskgate_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/redisbroker"
)

// 多进程专项的公共约定:任务类型 = 队列名 = "mp";
// 执行记录写在 mpexec:<任务ID>(独立于 broker 的 tg: 前缀,互不干扰),
// handler 每真正执行一次就 INCR 一次,父进程逐键断言"恰好一次"。
const (
	mpQueue      = "mp"
	mpExecPrefix = "mpexec:"
)

// mpConfig 多进程用例统一的 Gate 配置:单队列,8 并发,租约时长按用例传入。
func mpConfig(b taskgate.Broker, ttl time.Duration) taskgate.Config {
	return taskgate.Config{
		Broker: b,
		Queues: map[string]taskgate.QueueConfig{
			mpQueue: {Workers: 8, LeaseTTL: taskgate.Duration(ttl)},
		},
	}
}

// TestMultiprocHelper 不是常规测试:只在被父测试作为子进程拉起、且带环境变量开关时
// 才真正干活(research.md 第 8 节的子进程模式)。两种角色由 TASKGATE_MP_MODE 区分:
//   - "run":正常 worker,handler 往 Redis INCR 执行记录后返回成功;
//     后台监视全局 completed 数,达到 TASKGATE_MP_TOTAL 后 Shutdown 正常退出(退出码 0);
//   - "stall":kill -9 用例的受害者,handler 写哨兵文件通知父进程"任务已认领开跑",
//     然后 select{} 永久卡死,等着被父进程 kill -9。
func TestMultiprocHelper(t *testing.T) {
	if os.Getenv("TASKGATE_MP_HELPER") != "1" {
		t.Skip("仅作为多进程专项的子进程 worker 运行")
	}
	addr := os.Getenv("TASKGATE_MP_ADDR")
	mode := os.Getenv("TASKGATE_MP_MODE")
	ttl, err := time.ParseDuration(os.Getenv("TASKGATE_MP_TTL"))
	if addr == "" || err != nil {
		t.Fatalf("子进程环境变量不全: addr=%q ttl=%v", addr, err)
	}

	b, err := redisbroker.New(redisbroker.Options{Addr: addr})
	if err != nil {
		t.Fatalf("子进程连 Redis(%s) 失败: %v", addr, err)
	}
	defer func() { _ = b.Close() }()
	g, err := taskgate.New(mpConfig(b, ttl))
	if err != nil {
		t.Fatalf("子进程 New 失败: %v", err)
	}

	// 执行记录客户端:走独立连接,不借道 broker(broker 不导出它的客户端,也不该导出)。
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()

	sentinel := os.Getenv("TASKGATE_MP_SENTINEL")
	g.Handle(mpQueue, func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		if mode == "stall" {
			if err := os.WriteFile(sentinel, []byte(task.ID), 0o644); err != nil {
				return nil, err
			}
			select {} // 卡死:心跳还在续租,直到整个进程被 kill -9
		}
		if err := rdb.Incr(ctx, mpExecPrefix+task.ID).Err(); err != nil {
			return nil, err
		}
		return []byte(`"ok"`), nil
	})

	if mode != "stall" {
		total, err := strconv.Atoi(os.Getenv("TASKGATE_MP_TOTAL"))
		if err != nil || total <= 0 {
			t.Fatalf("TASKGATE_MP_TOTAL 无效: %v", err)
		}
		// 收工监视:全局 completed 达标后 Shutdown,让 Run 正常返回、进程退出码 0。
		// 两个 worker 各自监视各自退,谁先看到都行,不用进程间协商。
		go func() {
			for {
				counts, cerr := b.Counts(context.Background())
				if cerr == nil {
					var done int64
					for _, byStatus := range counts {
						done += byStatus[taskgate.StatusCompleted]
					}
					if done >= int64(total) {
						sdCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						_ = g.Shutdown(sdCtx)
						return
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}
	if err := g.Run(context.Background()); err != nil {
		t.Fatalf("子进程 Run 返回错误: %v", err) // stall 模式跑到被杀,走不到这里
	}
}

// workerProc 一个子进程 worker 的句柄:输出攒在缓冲区,失败时才打出来。
type workerProc struct {
	cmd *exec.Cmd
	out bytes.Buffer
}

// spawnWorker exec 自身测试二进制拉起子进程 worker,extra 放模式相关的环境变量。
// Cleanup 兜底 Kill,不留孤儿进程。
func spawnWorker(t *testing.T, addr string, ttl time.Duration, extra ...string) *workerProc {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("拿不到测试二进制路径: %v", err)
	}
	w := &workerProc{}
	w.cmd = exec.Command(exe, "-test.run=^TestMultiprocHelper$", "-test.timeout=2m")
	w.cmd.Env = append(os.Environ(),
		"TASKGATE_MP_HELPER=1",
		"TASKGATE_MP_ADDR="+addr,
		"TASKGATE_MP_TTL="+ttl.String(),
	)
	w.cmd.Env = append(w.cmd.Env, extra...)
	w.cmd.Stdout = &w.out
	w.cmd.Stderr = &w.out
	if err := w.cmd.Start(); err != nil {
		t.Fatalf("拉起子进程失败: %v", err)
	}
	t.Cleanup(func() { _ = w.cmd.Process.Kill(); _, _ = w.cmd.Process.Wait() })
	return w
}

// waitOK 等子进程退出并要求退出码 0,失败时连同子进程输出一起报出来。
func (w *workerProc) waitOK(t *testing.T) {
	t.Helper()
	if err := w.cmd.Wait(); err != nil {
		t.Fatalf("子进程未正常退出: %v\n子进程输出:\n%s", err, w.out.String())
	}
}

// TestMultiprocExactlyOnce 双进程恰好一次(T108-①,spec US2 场景 1):
// 父进程灌 1000 个任务,两个子进程 worker 共抢同一个 miniredis 上的同一队列;
// 断言:全部 completed、无 failed/canceled、执行记录恰好 1000 条且每任务恰好 1 次。
func TestMultiprocExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过多进程专项")
	}
	mr := miniredis.RunT(t)

	const total = 1000
	// 租约给足 10s:handler 微秒级完成,正常路径绝不该出现租约过期;
	// 慢 CI 下若给秒级短租约,误过期会造成重跑,"恰好一次"断言就会误挂。
	const ttl = 10 * time.Second

	// 父进程只当生产者:Submit 不 Handle 不 Run。
	pb, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("生产者连 Redis 失败: %v", err)
	}
	t.Cleanup(func() { _ = pb.Close() })
	pg, err := taskgate.New(mpConfig(pb, ttl))
	if err != nil {
		t.Fatalf("New(生产者) 失败: %v", err)
	}
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		id, err := pg.Submit(context.Background(), mpQueue, []byte(`{"n":`+strconv.Itoa(i)+`}`))
		if err != nil {
			t.Fatalf("Submit 第 %d 个失败: %v", i, err)
		}
		ids = append(ids, id)
	}

	// 两个子进程 worker 同时开抢,各自看到 1000 个 completed 后 Shutdown 正常退出。
	extra := []string{"TASKGATE_MP_MODE=run", "TASKGATE_MP_TOTAL=" + strconv.Itoa(total)}
	w1 := spawnWorker(t, mr.Addr(), ttl, extra...)
	w2 := spawnWorker(t, mr.Addr(), ttl, extra...)

	// 等全部 completed(Overview 是 O(1) 计数读取,轮询不贵)。
	waitFor(t, 60*time.Second, func() bool {
		counts, cerr := pg.Overview(context.Background())
		return cerr == nil && counts[mpQueue][taskgate.StatusCompleted] == total
	}, "1000 个任务应全部 completed")

	counts, err := pg.Overview(context.Background())
	if err != nil {
		t.Fatalf("Overview 失败: %v", err)
	}
	if n := counts[mpQueue][taskgate.StatusFailed]; n != 0 {
		t.Fatalf("不应有任务 failed,实际 %d", n)
	}
	if n := counts[mpQueue][taskgate.StatusCanceled]; n != 0 {
		t.Fatalf("不应有任务 canceled,实际 %d", n)
	}

	// 恰好一次:逐任务执行记录 = 1;记录键总数恰好 1000(没有多余的键 = 没有幽灵执行)。
	for _, id := range ids {
		v, gerr := mr.Get(mpExecPrefix + id)
		if gerr != nil {
			t.Fatalf("任务 %s 没有执行记录: %v", id, gerr)
		}
		if v != "1" {
			t.Fatalf("任务 %s 应恰好执行 1 次,实际 %s 次", id, v)
		}
	}
	execKeys := 0
	for _, k := range mr.Keys() {
		if strings.HasPrefix(k, mpExecPrefix) {
			execKeys++
		}
	}
	if execKeys != total {
		t.Fatalf("执行记录应恰好 %d 条,实际 %d 条", total, execKeys)
	}

	// 两个 worker 都必须是 Shutdown 正常退出(退出码 0),不是被杀或报错。
	w1.waitOK(t)
	w2.waitOK(t)
}

// TestMultiprocKillRecovery kill -9 回收(T108-②,spec US2 场景 2):
// 子进程 A 认领任务后卡死,被父进程 kill -9(心跳戛然而止,没有任何善后)→
// 子进程 B(同 handler,这次正常干活)的 reaper 发现租约过期,把任务捞回 pending
// (LeaseLost=1)→ B 重跑至 completed。
func TestMultiprocKillRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过多进程专项")
	}
	mr := miniredis.RunT(t)
	sentinel := filepath.Join(t.TempDir(), "started")
	const ttl = 2 * time.Second // 短租约让回收快:B 的 reaper 周期 = TTL/2 = 1s

	pb, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("生产者连 Redis 失败: %v", err)
	}
	t.Cleanup(func() { _ = pb.Close() })
	pg, err := taskgate.New(mpConfig(pb, ttl))
	if err != nil {
		t.Fatalf("New(生产者) 失败: %v", err)
	}
	id, err := pg.Submit(context.Background(), mpQueue, nil, taskgate.WithID("kill-1"))
	if err != nil {
		t.Fatalf("Submit 失败: %v", err)
	}

	// 子进程 A(stall 模式):认领后写哨兵然后卡死,等着被 kill -9。
	a := spawnWorker(t, mr.Addr(), ttl, "TASKGATE_MP_MODE=stall", "TASKGATE_MP_SENTINEL="+sentinel)
	waitFor(t, 30*time.Second, func() bool {
		_, serr := os.Stat(sentinel)
		return serr == nil
	}, "子进程 A 应认领任务并写出哨兵文件")
	if err := a.cmd.Process.Kill(); err != nil { // kill -9(SIGKILL),没有任何善后机会
		t.Fatalf("kill 子进程 A 失败: %v", err)
	}
	_ = a.cmd.Wait() // 必然报 signal: killed,忽略

	// 子进程 B(run 模式):自己跑 reaper 回收 A 留下的过期租约,重跑后正常完成。
	b := spawnWorker(t, mr.Addr(), ttl, "TASKGATE_MP_MODE=run", "TASKGATE_MP_TOTAL=1")

	waitFor(t, 60*time.Second, func() bool {
		task, gerr := pg.Get(context.Background(), id)
		return gerr == nil && task.Status == taskgate.StatusCompleted
	}, "任务应在租约过期回收后由 B 重跑至 completed")

	task := mustGet(t, pg, id)
	if task.LeaseLost != 1 {
		t.Fatalf("kill -9 回收应恰好记 1 次 LeaseLost,实际 %d", task.LeaseLost)
	}
	// 执行记录:A 卡死在写记录之前,只有 B 真正执行,恰好 1 次。
	if v, gerr := mr.Get(mpExecPrefix + id); gerr != nil || v != "1" {
		t.Fatalf("任务应恰好执行 1 次,实际 v=%q err=%v", v, gerr)
	}
	b.waitOK(t)
}

// ---- T109 断连恢复:本地 TCP 代理精确模拟网络断连 ----

// tcpProxy 手写的本地 TCP 转发代理,站在 worker 与 miniredis 之间:
// Cut() 掐断全部在途连接并拒绝新连接(worker 侧表现为纯网络错误),Restore() 恢复转发。
// 为什么不用"miniredis 关掉重启":重启会把数据清空,变成"数据丢失"而不是"网络断连",
// 语义不对;代理断开期间 Redis 数据原封不动,精确对应 spec US2 场景 4 / FR-009。
type tcpProxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	down  bool
	conns map[net.Conn]struct{}
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("代理监听失败: %v", err)
	}
	p := &tcpProxy{ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	go p.acceptLoop()
	t.Cleanup(func() { _ = ln.Close(); p.Cut() })
	return p
}

// Addr 代理的监听地址,worker 连这里而不是直连 miniredis。
func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) acceptLoop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return // 监听已关,测试结束
		}
		up, derr := net.Dial("tcp", p.target)
		if derr != nil {
			_ = c.Close()
			continue
		}
		if !p.admit(c, up) { // 断连期:新连接直接掐掉
			_ = c.Close()
			_ = up.Close()
			continue
		}
		go p.pipe(c, up)
		go p.pipe(up, c)
	}
}

// admit 登记一对连接;与 Cut 在同一把锁下串行,杜绝"Cut 扫完之后才登记进来漏杀"的窗口。
func (p *tcpProxy) admit(c, up net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.down {
		return false
	}
	p.conns[c] = struct{}{}
	p.conns[up] = struct{}{}
	return true
}

// pipe 单向转发;一头断了就把两头都关掉,go-redis 会立刻感知到连接坏死。
func (p *tcpProxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}

// Cut 模拟断网:拒收新连接 + 掐断全部在途连接。
func (p *tcpProxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = make(map[net.Conn]struct{})
}

// Restore 恢复转发,之后的新连接正常放行(go-redis 自己会重连)。
func (p *tcpProxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = false
}

// TestRedisDisconnectRecovery 断连恢复(T109,spec US2 场景 4,FR-009):
// 本进程 worker 经代理消费 20 个任务,消费到一半掐断代理 1.5s(Dequeue/Ack/心跳
// 全部报网络错误),断连期间消费循环必须活着(claimLoop 对 Dequeue 错误是冷却
// 100ms 重试,心跳对非哨兵错误容忍);恢复后自动续上,最终 20 个全部 completed。
// 断连时在途任务的 Ack 丢了也不丢任务:租约过期后由 reaper 捞回重跑(允许 LeaseLost,
// 网络分区下的重跑是合同行为,这里断言的是"不丢、不 failed",不是恰好一次)。
func TestRedisDisconnectRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过断连专项")
	}
	mr := miniredis.RunT(t)
	proxy := newTCPProxy(t, mr.Addr())

	const total = 20
	const ttl = 2 * time.Second // 短租约:断连期丢掉的 Ack 能被 reaper 快速补救
	b, err := redisbroker.New(redisbroker.Options{Addr: proxy.Addr()})
	if err != nil {
		t.Fatalf("经代理连 Redis 失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	g, err := taskgate.New(taskgate.Config{
		Broker: b,
		Queues: map[string]taskgate.QueueConfig{
			"dc": {Workers: 4, LeaseTTL: taskgate.Duration(ttl)},
		},
	})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	var startedRuns atomic.Int32
	g.Handle("dc", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		startedRuns.Add(1)
		time.Sleep(100 * time.Millisecond) // 拖一拍,让断连时刻能落在"有任务在跑"的窗口里
		return []byte(`"ok"`), nil
	})

	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		id, serr := g.Submit(context.Background(), "dc", nil)
		if serr != nil {
			t.Fatalf("Submit 第 %d 个失败: %v", i, serr)
		}
		ids = append(ids, id)
	}

	// Run 放后台;runDone 专门用来断言"断连期间消费循环没有退出"。
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = g.Run(runCtx) }()
	t.Cleanup(func() { cancelRun(); <-runDone })

	// 等消费到一半(至少 4 个已开跑、还有余量没消费)再掐断:断连点落在"已入队未消费完"。
	waitFor(t, 30*time.Second, func() bool { return startedRuns.Load() >= 4 }, "断连前应至少有 4 个任务开跑")
	proxy.Cut()
	time.Sleep(1500 * time.Millisecond) // 断连窗口:worker 持续报网络错误、冷却重试
	select {
	case <-runDone:
		t.Fatal("断连期间消费循环(Run)不应退出")
	default:
	}
	proxy.Restore()

	// 恢复后全部跑完,一个不丢、一个不 failed。
	waitFor(t, 60*time.Second, func() bool {
		counts, cerr := g.Overview(context.Background())
		return cerr == nil && counts["dc"][taskgate.StatusCompleted] == total
	}, "恢复后 20 个任务应全部 completed")
	counts, err := g.Overview(context.Background())
	if err != nil {
		t.Fatalf("Overview 失败: %v", err)
	}
	if n := counts["dc"][taskgate.StatusFailed]; n != 0 {
		t.Fatalf("断连恢复不应产生 failed 任务,实际 %d", n)
	}
	for _, id := range ids {
		if task := mustGet(t, g, id); task.Status != taskgate.StatusCompleted {
			t.Fatalf("任务 %s 最终应 completed,实际 %s", id, task.Status)
		}
	}
}
