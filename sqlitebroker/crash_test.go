// crash_test.go 崩溃恢复专项(L3 级,只针对 sqlite 后端,`go test -short` 跳过):
//
//	① kill -9:子进程 worker 处理到一半被杀,父进程重开同一 db,reaper 回收后重跑成功;
//	② 唤醒中途崩:父任务 Ack 事务提交前"进程消失"(注入点模拟),重启恢复后子任务不丢唤醒。
//
// 放在 sqlitebroker 包(外部测试包)而不是仓库根:注入点 SetTestHookBeforeAckCommit
// 定义在本包的 export_test.go,只在本包的测试二进制里编译,根包的测试够不着它。
// 两个用例都走完整 Gate 链路(New/Handle/Run/Submit/Wait),不绕过 scheduler。
package sqlitebroker_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/sqlitebroker"
)

// crashQueue 崩溃用例统一的队列配置:短租约让恢复快(1s),Workers 1 保证时序单纯。
func crashQueue(ttl time.Duration) map[string]taskgate.QueueConfig {
	return map[string]taskgate.QueueConfig{
		"crash": {Workers: 1, LeaseTTL: taskgate.Duration(ttl)},
	}
}

// waitCond 真时钟轮询等条件成立(崩溃用例没法用 fakeclock:跨进程/真 goroutine 死亡)。
func waitCond(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等待超时(%v): %s", timeout, msg)
}

// TestCrashWorkerHelper 不是常规测试:只在被 TestKillMinus9Recovery 作为子进程拉起、
// 且带上环境变量开关时才真正干活(research.md 第 10 节的标准子进程模式)。
// 它注册的 handler 会先写哨兵文件通知父进程"任务已开跑",然后 select{} 永久卡死,
// 等着被父进程 kill -9 —— 模拟"worker 处理到一半进程没了"。
func TestCrashWorkerHelper(t *testing.T) {
	if os.Getenv("TASKGATE_CRASH_HELPER") != "1" {
		t.Skip("仅作为 kill -9 崩溃测试的子进程运行")
	}
	dbPath := os.Getenv("TASKGATE_CRASH_DB")
	sentinel := os.Getenv("TASKGATE_CRASH_SENTINEL")
	if dbPath == "" || sentinel == "" {
		t.Fatal("子进程缺少 TASKGATE_CRASH_DB / TASKGATE_CRASH_SENTINEL 环境变量")
	}

	b, err := sqlitebroker.Open(dbPath)
	if err != nil {
		t.Fatalf("子进程 Open(%s) 失败: %v", dbPath, err)
	}
	g, err := taskgate.New(taskgate.Config{
		Broker: b,
		Queues: crashQueue(time.Second),
	})
	if err != nil {
		t.Fatalf("子进程 New 失败: %v", err)
	}
	g.Handle("crash", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		if err := os.WriteFile(sentinel, []byte(task.ID), 0o644); err != nil {
			return nil, err
		}
		select {} // 卡死:心跳 goroutine 还在续租,直到整个进程被 kill -9
	})
	_ = g.Run(context.Background()) // 一直跑到被父进程杀掉
}

// TestKillMinus9Recovery kill -9 崩溃恢复(SC-004,spec US4 场景 4):
// 子进程认领任务后被 kill -9(心跳戛然而止)→ 父进程重开同一 db 文件起新 Gate,
// reaper 发现租约过期把任务捞回 pending(LeaseLost=1)→ 新 worker 重跑,这次正常完成。
func TestKillMinus9Recovery(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过崩溃专项")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	sentinel := filepath.Join(dir, "started")

	// 第一步:以生产者身份建库并提交任务(不注册 handler、不 Run)。
	prod, err := sqlitebroker.Open(dbPath)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	pg, err := taskgate.New(taskgate.Config{Broker: prod, Queues: crashQueue(time.Second)})
	if err != nil {
		t.Fatalf("New(生产者) 失败: %v", err)
	}
	id, err := pg.Submit(context.Background(), "crash", nil, taskgate.WithID("crash-1"))
	if err != nil {
		t.Fatalf("Submit 失败: %v", err)
	}
	_ = prod.Close() // 库交给子进程,父进程稍后重开

	// 第二步:拉起子进程 worker(exec 自身测试二进制,只跑 helper 用例),
	// 等哨兵文件出现(handler 已开跑、任务在 running)后 kill -9。
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("拿不到测试二进制路径: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^TestCrashWorkerHelper$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(),
		"TASKGATE_CRASH_HELPER=1",
		"TASKGATE_CRASH_DB="+dbPath,
		"TASKGATE_CRASH_SENTINEL="+sentinel,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("拉起子进程失败: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }() // 兜底,別留孤儿进程
	waitCond(t, 30*time.Second, func() bool {
		_, err := os.Stat(sentinel)
		return err == nil
	}, "子进程 worker 应认领任务并写出哨兵文件")
	if err := cmd.Process.Kill(); err != nil { // kill -9(SIGKILL),没有任何善后机会
		t.Fatalf("kill 子进程失败: %v", err)
	}
	_ = cmd.Wait() // 必然报 signal: killed,忽略

	// 第三步:父进程重开同一 db,起新 Gate(同 handler 这次正常完成)。
	b2, err := sqlitebroker.Open(dbPath)
	if err != nil {
		t.Fatalf("重开 db 失败: %v", err)
	}
	t.Cleanup(func() { _ = b2.Close() })
	g2, err := taskgate.New(taskgate.Config{Broker: b2, Queues: crashQueue(time.Second)})
	if err != nil {
		t.Fatalf("New(恢复者) 失败: %v", err)
	}
	g2.Handle("crash", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		return []byte(`"recovered"`), nil
	})
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = g2.Run(runCtx) }()
	t.Cleanup(func() { cancelRun(); <-runDone })

	// reaper(周期 = LeaseTTL/2 = 500ms)会把过期租约捞回 pending,随后重跑成功。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := g2.Wait(ctx, id)
	if err != nil {
		t.Fatalf("恢复后 Wait 失败: %v", err)
	}
	if string(result) != `"recovered"` {
		t.Fatalf("Result 不对: %s", result)
	}
	task, err := g2.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get 失败: %v", err)
	}
	if task.Status != taskgate.StatusCompleted {
		t.Fatalf("最终状态应为 completed,实际 %s", task.Status)
	}
	if task.LeaseLost != 1 {
		t.Fatalf("崩溃恢复应恰好记 1 次 LeaseLost,实际 %d", task.LeaseLost)
	}
}

// TestWakeCrashRecoveryL3 "唤醒中途崩"全链路版(spec Edge Case:父 Ack 事务提交前进程崩):
// 走完整 Gate 链路,父任务 Ack 的事务在"终态+子唤醒全部写完、提交之前"被注入点掐死
// (worker goroutine 用 runtime.Goexit 消失,defer 的 Rollback 生效 = 什么都没写)。
// 断言:崩溃后父仍 running、子仍 blocked → reaper 按租约过期回收父 → 父重跑 Ack 成功 →
// 子被唤醒并跑完,一个唤醒都不丢。
//
// 裁决说明:注入点原本设想 hook 里 panic + 测试 recover,但 Ack 是 scheduler 的 worker
// goroutine 在调,panic 跨 goroutine 无法 recover,会直接砸掉测试进程;
// 改用 runtime.Goexit —— 同样让事务停在提交之前(defer Rollback 收尾),
// 且该 goroutine 沿途的 defer(停心跳、归还并发槽)照常执行,语义等同"提交前进程消失"。
func TestWakeCrashRecoveryL3(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过崩溃专项")
	}
	b, err := sqlitebroker.Open(filepath.Join(t.TempDir(), "wake.db"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	const ttl = 2 * time.Second // 租约给足 2s:崩溃点之后要来得及断言"父仍 running"
	g, err := taskgate.New(taskgate.Config{
		Broker: b,
		Queues: map[string]taskgate.QueueConfig{
			"pq": {Workers: 1, LeaseTTL: taskgate.Duration(ttl)},
			"cq": {Workers: 1, LeaseTTL: taskgate.Duration(ttl)},
		},
	})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	var parentRuns atomic.Int32
	g.Handle("pq", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		parentRuns.Add(1)
		return []byte(`"p"`), nil
	})
	g.Handle("cq", func(ctx context.Context, task *taskgate.Task) ([]byte, error) {
		return []byte(`"c"`), nil
	})

	// 一次性注入:第一次 Ack 走到提交前就让 worker goroutine 消失;之后的 Ack 一律放行。
	// hook 用完由 Cleanup 复位;Cleanup 先注册,LIFO 保证它在消费循环停掉之后才执行。
	t.Cleanup(func() { sqlitebroker.SetTestHookBeforeAckCommit(nil) })
	hookFired := make(chan struct{})
	var fired atomic.Bool
	sqlitebroker.SetTestHookBeforeAckCommit(func() {
		if fired.CompareAndSwap(false, true) {
			close(hookFired)
			runtime.Goexit() // worker goroutine 就地消失,事务停在提交前
		}
	})

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = g.Run(runCtx) }()
	t.Cleanup(func() { cancelRun(); <-runDone })

	ctx := context.Background()
	pid, err := g.Submit(ctx, "pq", nil, taskgate.WithID("wake-p"))
	if err != nil {
		t.Fatalf("Submit 父任务失败: %v", err)
	}
	cid, err := g.Submit(ctx, "cq", nil, taskgate.WithID("wake-c"), taskgate.DependsOn(pid))
	if err != nil {
		t.Fatalf("Submit 子任务失败: %v", err)
	}

	select {
	case <-hookFired:
	case <-time.After(15 * time.Second):
		t.Fatal("等待崩溃注入点触发超时")
	}
	// 崩溃点已触发:事务回滚(sqlite 单连接,下面的 Get 会排在 Rollback 之后执行),
	// 父必须还在 running(结果没写进去)、子必须还在 blocked(唤醒没发生)。
	p, err := b.Get(ctx, pid)
	if err != nil {
		t.Fatalf("Get 父任务失败: %v", err)
	}
	if p.Status != taskgate.StatusRunning || p.Result != nil {
		t.Fatalf("崩溃后父应仍 running 且无 Result,实际 status=%s result=%s", p.Status, p.Result)
	}
	c, err := b.Get(ctx, cid)
	if err != nil {
		t.Fatalf("Get 子任务失败: %v", err)
	}
	if c.Status != taskgate.StatusBlocked {
		t.Fatalf("崩溃后子应仍 blocked,实际 %s", c.Status)
	}

	// 恢复:租约 2s 过期 → reaper(1s 一扫)回收父 → 重新认领重跑 → Ack 放行 → 子被唤醒跑完。
	waitCond(t, 30*time.Second, func() bool {
		got, err := b.Get(ctx, cid)
		return err == nil && got.Status == taskgate.StatusCompleted
	}, "子任务应在父恢复重跑后被唤醒并跑完")

	p, err = b.Get(ctx, pid)
	if err != nil {
		t.Fatalf("Get 父任务失败: %v", err)
	}
	if p.Status != taskgate.StatusCompleted {
		t.Fatalf("父最终应 completed,实际 %s", p.Status)
	}
	if p.LeaseLost != 1 {
		t.Fatalf("父应恰好记 1 次 LeaseLost,实际 %d", p.LeaseLost)
	}
	if got := parentRuns.Load(); got != 2 {
		t.Fatalf("父 handler 应恰好跑 2 次(崩一次+重跑一次),实际 %d", got)
	}
}
