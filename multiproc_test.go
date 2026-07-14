// multiproc_test.go 多进程"恰好执行一次"专项(M2 US2,T108/T109,`go test -short` 跳过):
//
//	① 双进程恰好一次:两个子进程 worker 抢同一批 1000 个任务,执行记录恰好 1000 条且每任务恰好 1 次;
//	② kill -9 回收:子进程认领后被杀,新进程经 reaper 按租约过期回收重跑,LeaseLost=1 最终 completed;
//	③ 断连恢复(T109):worker 经本地 TCP 代理连 Redis,掐断代理模拟网络断连,
//	   恢复后消费循环自动续上,20 个任务全部 completed 不丢失;
//	④ 并发配额全局共享(T112):双进程同队列 {Workers:2},handler 上报并发水位,
//	   全局峰值恰好到 2 且从未超过 2。
//
// miniredis 起在父测试进程(它监听真实 TCP 端口,子进程连得上);"多进程"沿用
// sqlitebroker/crash_test.go 的成熟模式:exec 自身测试二进制 + 环境变量开关 + 哨兵文件。
// 子进程用不了 fakeclock(跨进程没法推进假时间),全部走真时钟 + 秒级短租约;
// 父进程的轮询断言一律带超时(waitFor),防挂死。
package taskgate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
// 并发水位专项(T112)另用三个键:mpconc 当前并发(INCR/DECR)、
// mppeak2 并发确实到过 2 的证据、mpviol 超限违规标记。
const (
	mpQueue      = "mp"
	mpExecPrefix = "mpexec:"
	mpConcKey    = "mpconc"
	mpPeakKey    = "mppeak2"
	mpViolKey    = "mpviol"
	mpConcLimit  = 2 // 并发水位专项的全局 Workers 上限
)

// mpConfig 多进程用例统一的 Gate 配置:单队列,并发数与租约时长按用例传入。
func mpConfig(b taskgate.Broker, ttl time.Duration, workers int) taskgate.Config {
	return taskgate.Config{
		Broker: b,
		Queues: map[string]taskgate.QueueConfig{
			mpQueue: {Workers: workers, LeaseTTL: taskgate.Duration(ttl)},
		},
	}
}

// TestMultiprocHelper 不是常规测试:只在被父测试作为子进程拉起、且带环境变量开关时
// 才真正干活(research.md 第 8 节的子进程模式)。两种角色由 TASKGATE_MP_MODE 区分:
//   - "run":正常 worker,handler 往 Redis INCR 执行记录后返回成功;
//     后台监视全局 completed 数,达到 TASKGATE_MP_TOTAL 后 Shutdown 正常退出(退出码 0);
//   - "stall":kill -9 用例的受害者,handler 写哨兵文件通知父进程"任务已认领开跑",
//     然后 select{} 永久卡死,等着被父进程 kill -9;
//   - "conc":并发水位观察者(T112),行为同 "run",但 handler 进门先 INCR 当前并发数,
//     超过 mpConcLimit 就写违规标记,停留一拍(制造真实的并发重叠窗口)再 DECR。
//
// 队列 Workers 数从 TASKGATE_MP_WORKERS 读(缺省 8),并发水位专项要压到 2。
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
	workers := 8
	if v := os.Getenv("TASKGATE_MP_WORKERS"); v != "" {
		if workers, err = strconv.Atoi(v); err != nil {
			t.Fatalf("TASKGATE_MP_WORKERS 无效: %v", err)
		}
	}

	b, err := redisbroker.New(redisbroker.Options{Addr: addr})
	if err != nil {
		t.Fatalf("子进程连 Redis(%s) 失败: %v", addr, err)
	}
	defer func() { _ = b.Close() }()
	g, err := taskgate.New(mpConfig(b, ttl, workers))
	if err != nil {
		t.Fatalf("子进程 New 失败: %v", err)
	}

	// 执行记录客户端:走独立连接,不借道 broker(broker 不导出它的客户端,也不该导出)。
	// MaxRetries=-1 关掉 go-redis 的自动重试:INCR/DECR 不幂等,机器高负载下
	// 一次"响应丢失+自动重试"就会把观测计数悄悄写两遍,制造假的并发超限/重复执行;
	// 关掉后这类故障表现为 handler 报错走任务重试,INCR/DECR 整对重来,观测不失真。
	rdb := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
	defer func() { _ = rdb.Close() }()

	sentinel := os.Getenv("TASKGATE_MP_SENTINEL")
	g.Handle(mpQueue, func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		if mode == "stall" {
			if err := os.WriteFile(sentinel, []byte(task.ID), 0o644); err != nil {
				return nil, err
			}
			select {} // 卡死:心跳还在续租,直到整个进程被 kill -9
		}
		if mode == "conc" {
			// 全局当前并发 +1;INCR 原子且返回新值,两个进程看到的是同一个计数。
			v, verr := rdb.Incr(ctx, mpConcKey).Result()
			if verr != nil {
				return nil, verr
			}
			if v >= mpConcLimit {
				// 记下"确实并发到过上限":没有这个证据,"峰值 ≤2"可能只是串行跑出来的空话。
				_ = rdb.Set(ctx, mpPeakKey, strconv.FormatInt(v, 10), 0).Err()
			}
			if v > mpConcLimit {
				// 违规:全局并发超过 Workers 上限,写标记让父进程一票否决。
				_ = rdb.Set(ctx, mpViolKey, strconv.FormatInt(v, 10), 0).Err()
			}
			time.Sleep(150 * time.Millisecond) // 停留一拍,让两个进程的执行窗口真实重叠
			if derr := rdb.Decr(ctx, mpConcKey).Err(); derr != nil {
				return nil, derr
			}
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
		shutdownWhenCounted(g, b, total, taskgate.StatusCompleted)
	}
	if err := g.Run(context.Background()); err != nil {
		t.Fatalf("子进程 Run 返回错误: %v", err) // stall 模式跑到被杀,走不到这里
	}
}

// shutdownWhenCounted 后台监视:全局 status 状态的任务数(跨 Type 求和)达到 total 后
// Shutdown,让 Run 正常返回、子进程以退出码 0 收场。各 worker 各自监视各自退,
// 谁先看到都行,不用进程间协商。
func shutdownWhenCounted(g *taskgate.Gate, b taskgate.Broker, total int, status taskgate.Status) {
	go func() {
		for {
			counts, cerr := b.Counts(context.Background())
			if cerr == nil {
				var done int64
				for _, byStatus := range counts {
					done += byStatus[status]
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

// workerProc 一个子进程 worker 的句柄:输出攒在缓冲区,失败时才打出来。
type workerProc struct {
	cmd *exec.Cmd
	out bytes.Buffer
}

// spawnWorker exec 自身测试二进制拉起子进程 worker(跑 TestMultiprocHelper),
// extra 放模式相关的环境变量。
func spawnWorker(t *testing.T, addr string, ttl time.Duration, extra ...string) *workerProc {
	t.Helper()
	return spawnHelper(t, "TestMultiprocHelper", addr, ttl, extra...)
}

// spawnHelper exec 自身测试二进制拉起指定的 helper 测试当子进程 worker。
// Cleanup 兜底 Kill,不留孤儿进程。
func spawnHelper(t *testing.T, helper, addr string, ttl time.Duration, extra ...string) *workerProc {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("拿不到测试二进制路径: %v", err)
	}
	w := &workerProc{}
	w.cmd = exec.Command(exe, "-test.run=^"+helper+"$", "-test.timeout=2m")
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
	pg, err := taskgate.New(mpConfig(pb, ttl, 8))
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
	pg, err := taskgate.New(mpConfig(pb, ttl, 8))
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

// TestMultiprocWorkersLimit 双进程共享并发配额(T112,spec US3 / SC-003):
// 队列 {Workers:2},两个子进程 worker 同抢——若限流是进程内的,全局最多能同时跑 4 个;
// 分布式信号量下全局必须 ≤2。handler 用 INCR/DECR 维护全局"当前并发"计数:
// 峰值超过 2 就写违规标记(mpviol),父进程一票否决;
// 同时要求峰值确实到过 2(mppeak2),否则"≤2"可能只是串行跑出来的空话。
func TestMultiprocWorkersLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过多进程专项")
	}
	mr := miniredis.RunT(t)

	// 24 个任务 × 每个停留 150ms ÷ 全局 2 并发 ≈ 1.8s,时长可控;
	// 租约给足 10s,正常路径不出现租约过期(理由同 TestMultiprocExactlyOnce)。
	const total = 24
	const ttl = 10 * time.Second

	pb, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("生产者连 Redis 失败: %v", err)
	}
	t.Cleanup(func() { _ = pb.Close() })
	pg, err := taskgate.New(mpConfig(pb, ttl, mpConcLimit))
	if err != nil {
		t.Fatalf("New(生产者) 失败: %v", err)
	}
	for i := 0; i < total; i++ {
		if _, err := pg.Submit(context.Background(), mpQueue, nil); err != nil {
			t.Fatalf("Submit 第 %d 个失败: %v", i, err)
		}
	}

	// 两个子进程各配 Workers=2:配额是全局共享的,不是每进程各 2。
	extra := []string{
		"TASKGATE_MP_MODE=conc",
		"TASKGATE_MP_TOTAL=" + strconv.Itoa(total),
		"TASKGATE_MP_WORKERS=" + strconv.Itoa(mpConcLimit),
	}
	w1 := spawnWorker(t, mr.Addr(), ttl, extra...)
	w2 := spawnWorker(t, mr.Addr(), ttl, extra...)

	waitFor(t, 60*time.Second, func() bool {
		counts, cerr := pg.Overview(context.Background())
		return cerr == nil && counts[mpQueue][taskgate.StatusCompleted] == total
	}, "24 个任务应全部 completed")

	// 违规标记必须不存在:全局并发峰值从未超过 2。
	if v, gerr := mr.Get(mpViolKey); gerr == nil {
		t.Fatalf("全局并发超限:观察到峰值 %s(上限 %d)", v, mpConcLimit)
	}
	// 峰值证据必须存在:两个进程确实同时各跑过任务,断言不是串行空转出来的。
	if _, gerr := mr.Get(mpPeakKey); gerr != nil {
		t.Fatalf("并发从未达到 %d,测试没有真正压到并发窗口: %v", mpConcLimit, gerr)
	}

	w1.waitOK(t)
	w2.waitOK(t)
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

// ---- T113 跨进程流水线与跨进程取消(spec US4 / SC-004) ----

// 跨进程流水线的公共约定:两条队列 ocr / extract(队列名 = 任务类型名),
// 生产者和两个消费者进程共用同一份双队列 Config(文档约定:共享 Config),
// 但进程 A 只 Handle ocr、进程 B 只 Handle extract——scheduler 只消费
// "注册过 handler 的 Type 对应的队列",所以 A 绝不会碰 extract 的任务,反之亦然。
const (
	ppOcrQueue     = "ocr"
	ppExtractQueue = "extract"
)

// ppConfig 跨进程流水线统一的 Gate 配置(双队列写全,谁消费哪条由 Handle 决定)。
func ppConfig(b taskgate.Broker, ttl time.Duration) taskgate.Config {
	return taskgate.Config{
		Broker: b,
		Queues: map[string]taskgate.QueueConfig{
			ppOcrQueue:     {Workers: 4, LeaseTTL: taskgate.Duration(ttl)},
			ppExtractQueue: {Workers: 4, LeaseTTL: taskgate.Duration(ttl)},
		},
	}
}

// TestMultiprocPipelineHelper 跨进程流水线的子进程 worker,按 TASKGATE_MP_ROLE 分工:
//   - "ocr":只 Handle ocr,handler 从 Payload 里读页号 n,产出 {"text":"page-n"};
//   - "extract":只 Handle extract,handler 逐个 Get 父任务、把父的 Result 原样拼进
//     自己的 Result——断言"子任务确实读到了另一个进程写下的父结果"就靠这份拼接。
//
// 全局 completed 达到 TASKGATE_MP_TOTAL 后 Shutdown 正常退出(退出码 0)。
func TestMultiprocPipelineHelper(t *testing.T) {
	if os.Getenv("TASKGATE_MP_HELPER") != "1" {
		t.Skip("仅作为跨进程流水线专项的子进程 worker 运行")
	}
	addr := os.Getenv("TASKGATE_MP_ADDR")
	role := os.Getenv("TASKGATE_MP_ROLE")
	ttl, err := time.ParseDuration(os.Getenv("TASKGATE_MP_TTL"))
	if addr == "" || err != nil {
		t.Fatalf("子进程环境变量不全: addr=%q ttl=%v", addr, err)
	}
	total, err := strconv.Atoi(os.Getenv("TASKGATE_MP_TOTAL"))
	if err != nil || total <= 0 {
		t.Fatalf("TASKGATE_MP_TOTAL 无效: %v", err)
	}

	b, err := redisbroker.New(redisbroker.Options{Addr: addr})
	if err != nil {
		t.Fatalf("子进程连 Redis(%s) 失败: %v", addr, err)
	}
	defer func() { _ = b.Close() }()
	g, err := taskgate.New(ppConfig(b, ttl))
	if err != nil {
		t.Fatalf("子进程 New 失败: %v", err)
	}

	switch role {
	case "ocr":
		g.Handle(ppOcrQueue, func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			var in struct {
				N int `json:"n"`
			}
			if err := json.Unmarshal(task.Payload, &in); err != nil {
				return nil, err
			}
			return []byte(fmt.Sprintf(`{"text":"page-%d"}`, in.N)), nil
		})
	case "extract":
		g.Handle(ppExtractQueue, func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
			// 依赖唤醒的合同保证走到这里时父必然已终态;FailFast 下还必然是 completed。
			parts := make([]string, 0, len(task.DependsOn))
			for _, pid := range task.DependsOn {
				parent, gerr := g.Get(ctx, pid)
				if gerr != nil {
					return nil, gerr
				}
				if parent.Status != taskgate.StatusCompleted {
					return nil, fmt.Errorf("父任务 %s 未完成就唤醒了子任务: %s", pid, parent.Status)
				}
				parts = append(parts, string(parent.Result))
			}
			return []byte(`{"merged":[` + strings.Join(parts, ",") + `]}`), nil
		})
	default:
		t.Fatalf("TASKGATE_MP_ROLE 无效: %q", role)
	}

	shutdownWhenCounted(g, b, total, taskgate.StatusCompleted)
	if err := g.Run(context.Background()); err != nil {
		t.Fatalf("子进程 Run 返回错误: %v", err)
	}
}

// TestMultiprocPipeline 跨进程流水线(T113-①,spec US4 场景 1/3):
// 进程 A 只消费 ocr 队列、进程 B 只消费 extract 队列,父进程提交 5 条 ocr→extract
// 依赖链。断言:全部 completed(A 完成父任务能跨进程唤醒 B 阻塞等待中的子任务),
// 且每个 extract 任务的 Result 里拼进了对应 ocr 任务的产出(跨进程 Get 父 Result)。
func TestMultiprocPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过多进程专项")
	}
	mr := miniredis.RunT(t)

	const chains = 5
	const total = chains * 2     // ocr 5 + extract 5
	const ttl = 10 * time.Second // 租约给足,正常路径不许出现误过期重跑

	// 父进程只当生产者:Submit 不 Handle 不 Run。
	pb, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("生产者连 Redis 失败: %v", err)
	}
	t.Cleanup(func() { _ = pb.Close() })
	pg, err := taskgate.New(ppConfig(pb, ttl))
	if err != nil {
		t.Fatalf("New(生产者) 失败: %v", err)
	}

	// 先拉起两个分工 worker 再提交也行,顺序无所谓:blocked 子任务不进 pending,
	// 这里先提交再拉起,顺带盖住"任务先于消费者存在"的路径。
	ocrIDs := make([]string, 0, chains)
	extractIDs := make([]string, 0, chains)
	for i := 0; i < chains; i++ {
		oid, serr := pg.Submit(context.Background(), ppOcrQueue,
			[]byte(fmt.Sprintf(`{"n":%d}`, i)))
		if serr != nil {
			t.Fatalf("Submit ocr 第 %d 个失败: %v", i, serr)
		}
		eid, serr := pg.Submit(context.Background(), ppExtractQueue, nil, taskgate.DependsOn(oid))
		if serr != nil {
			t.Fatalf("Submit extract 第 %d 个失败: %v", i, serr)
		}
		ocrIDs = append(ocrIDs, oid)
		extractIDs = append(extractIDs, eid)
	}

	extra := []string{"TASKGATE_MP_TOTAL=" + strconv.Itoa(total)}
	wa := spawnHelper(t, "TestMultiprocPipelineHelper", mr.Addr(), ttl,
		append([]string{"TASKGATE_MP_ROLE=ocr"}, extra...)...)
	wb := spawnHelper(t, "TestMultiprocPipelineHelper", mr.Addr(), ttl,
		append([]string{"TASKGATE_MP_ROLE=extract"}, extra...)...)

	waitFor(t, 60*time.Second, func() bool {
		counts, cerr := pg.Overview(context.Background())
		return cerr == nil &&
			counts[ppOcrQueue][taskgate.StatusCompleted] == chains &&
			counts[ppExtractQueue][taskgate.StatusCompleted] == chains
	}, "5 条 ocr→extract 依赖链应跨进程全部 completed")

	counts, err := pg.Overview(context.Background())
	if err != nil {
		t.Fatalf("Overview 失败: %v", err)
	}
	for _, typ := range []string{ppOcrQueue, ppExtractQueue} {
		if n := counts[typ][taskgate.StatusFailed]; n != 0 {
			t.Fatalf("%s 不应有任务 failed,实际 %d", typ, n)
		}
		if n := counts[typ][taskgate.StatusCanceled]; n != 0 {
			t.Fatalf("%s 不应有任务 canceled,实际 %d", typ, n)
		}
	}

	// 逐链核对:extract 的 Result 里必须能看到从父任务 Get 来的内容。
	for i := range extractIDs {
		et := mustGet(t, pg, extractIDs[i])
		if et.Status != taskgate.StatusCompleted {
			t.Fatalf("extract 任务 %s 应 completed,实际 %s", et.ID, et.Status)
		}
		want := fmt.Sprintf(`"page-%d"`, i)
		if !strings.Contains(string(et.Result), want) {
			t.Fatalf("extract 任务 %s 的 Result 应拼进父任务产出 %s,实际 %s",
				et.ID, want, et.Result)
		}
		ot := mustGet(t, pg, ocrIDs[i])
		if !strings.Contains(string(et.Result), string(ot.Result)) {
			t.Fatalf("extract Result 应原样包含 ocr Result %s,实际 %s", ot.Result, et.Result)
		}
	}

	wa.waitOK(t)
	wb.waitOK(t)
}

// TestMultiprocCancelHelper 跨进程取消专项的子进程 worker:
// handler 认领后写哨兵文件(内容=任务 ID)通知父进程"开跑了",然后挂在 ctx.Done 上
// 等取消——跨进程 Cancel 只打标记,要靠本进程下一次 Heartbeat(LeaseTTL/3)发现标记
// 后 cancel handler ctx。全局 canceled 达到 TASKGATE_MP_TOTAL 后 Shutdown 正常退出。
func TestMultiprocCancelHelper(t *testing.T) {
	if os.Getenv("TASKGATE_MP_HELPER") != "1" {
		t.Skip("仅作为跨进程取消专项的子进程 worker 运行")
	}
	addr := os.Getenv("TASKGATE_MP_ADDR")
	sentinel := os.Getenv("TASKGATE_MP_SENTINEL")
	ttl, err := time.ParseDuration(os.Getenv("TASKGATE_MP_TTL"))
	if addr == "" || sentinel == "" || err != nil {
		t.Fatalf("子进程环境变量不全: addr=%q sentinel=%q ttl=%v", addr, sentinel, err)
	}
	total, err := strconv.Atoi(os.Getenv("TASKGATE_MP_TOTAL"))
	if err != nil || total <= 0 {
		t.Fatalf("TASKGATE_MP_TOTAL 无效: %v", err)
	}

	b, err := redisbroker.New(redisbroker.Options{Addr: addr})
	if err != nil {
		t.Fatalf("子进程连 Redis(%s) 失败: %v", addr, err)
	}
	defer func() { _ = b.Close() }()
	g, err := taskgate.New(mpConfig(b, ttl, 2))
	if err != nil {
		t.Fatalf("子进程 New 失败: %v", err)
	}
	g.Handle(mpQueue, func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		if err := os.WriteFile(sentinel, []byte(task.ID), 0o644); err != nil {
			return nil, err
		}
		<-ctx.Done() // 长任务:一直跑到被取消(心跳发现取消标记后 ctx 被 cancel)
		return nil, ctx.Err()
	})

	shutdownWhenCounted(g, b, total, taskgate.StatusCanceled)
	if err := g.Run(context.Background()); err != nil {
		t.Fatalf("子进程 Run 返回错误: %v", err)
	}
}

// TestMultiprocCancel 跨进程取消(T113-②,spec US4 场景 2 / SC-004):
// 长任务在子进程 running,父进程用另一个 Gate(纯客户端,不 Run)发 Cancel——
// cancelLocal 摸不到别的进程,只能靠子进程的心跳(LeaseTTL/3 ≈ 667ms)发现取消标记。
// 断言:Cancel 后 3s(一个心跳周期 + 余量)内任务落 canceled;
// 它的 FailFast 子任务(blocked,从没跑过)被同一次传播连锁 canceled。
func TestMultiprocCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过多进程专项")
	}
	mr := miniredis.RunT(t)
	sentinel := filepath.Join(t.TempDir(), "started")
	const ttl = 2 * time.Second // 短租约:心跳周期 LeaseTTL/3 ≈ 667ms,取消尽快被发现

	pb, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("生产者连 Redis 失败: %v", err)
	}
	t.Cleanup(func() { _ = pb.Close() })
	pg, err := taskgate.New(mpConfig(pb, ttl, 2))
	if err != nil {
		t.Fatalf("New(生产者) 失败: %v", err)
	}
	parentID, err := pg.Submit(context.Background(), mpQueue, nil, taskgate.WithID("cancel-parent"))
	if err != nil {
		t.Fatalf("Submit 父任务失败: %v", err)
	}
	childID, err := pg.Submit(context.Background(), mpQueue, nil,
		taskgate.WithID("cancel-child"), taskgate.DependsOn(parentID))
	if err != nil {
		t.Fatalf("Submit 子任务失败: %v", err)
	}

	// 子进程 worker:认领父任务后写哨兵,挂在 ctx.Done 上等取消。
	w := spawnHelper(t, "TestMultiprocCancelHelper", mr.Addr(), ttl,
		"TASKGATE_MP_SENTINEL="+sentinel, "TASKGATE_MP_TOTAL=2")
	waitFor(t, 30*time.Second, func() bool {
		got, serr := os.ReadFile(sentinel)
		return serr == nil && string(got) == parentID
	}, "子进程应认领父任务并写出哨兵文件")
	waitFor(t, 10*time.Second, func() bool {
		task, gerr := pg.Get(context.Background(), parentID)
		return gerr == nil && task.Status == taskgate.StatusRunning
	}, "父任务应处于 running(被子进程持有)")

	// 跨进程 Cancel:父进程本地没有这个任务在跑,纯打标记。
	canceledAt := time.Now()
	if err := pg.Cancel(context.Background(), parentID); err != nil {
		t.Fatalf("Cancel 失败: %v", err)
	}
	// 一个心跳周期(667ms)+ 传播与落库余量,3s 封顶(spec US4:心跳周期内生效)。
	waitFor(t, 3*time.Second, func() bool {
		task, gerr := pg.Get(context.Background(), parentID)
		return gerr == nil && task.Status == taskgate.StatusCanceled
	}, "跨进程 Cancel 应在一个心跳周期(LeaseTTL/3+余量)内让任务落 canceled")
	t.Logf("跨进程 Cancel 生效耗时 %v(心跳周期 %v)", time.Since(canceledAt), ttl/3)

	// FailFast 传播:blocked 的子任务被连锁 canceled(与父同一段脚本收敛,
	// 父落 canceled 后子必然可见,这里仍给个短轮询防偶发)。
	waitFor(t, 3*time.Second, func() bool {
		task, gerr := pg.Get(context.Background(), childID)
		return gerr == nil && task.Status == taskgate.StatusCanceled
	}, "FailFast 子任务应被连锁 canceled")

	w.waitOK(t)
}
