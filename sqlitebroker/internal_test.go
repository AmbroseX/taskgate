package sqlitebroker

// 包内补充测试:brokertest 契约之外的三块——
// ① Ack 提交前崩溃注入点(testHookBeforeAckCommit):事务回滚 = 什么都没写,不丢唤醒;
// ② ReapExpired 的防御修复:blocked 但父实际全终态 → 补唤醒 / 补取消;
// ③ Notify 回调 panic 不砸主流程、Open 坏路径报错。
// ①+② 合起来就是"唤醒中途崩"的完整恢复剧本,后续 kill -9 crash 测试照此接线。

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/internal/fakeclock"
)

// newTestBroker 起一个用 fakeclock 的临时库 broker。
func newTestBroker(t *testing.T) (*Broker, *fakeclock.Clock) {
	t.Helper()
	clk := fakeclock.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	b, err := Open(filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Init(taskgate.BrokerOptions{
		DefaultLeaseTTL: 60 * time.Second,
		LeaseLostMax:    3,
		ThrottledMax:    100,
		Clock:           clk,
	}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	return b, clk
}

// TestAckCrashHookRecovery "唤醒中途崩"完整剧本:
// 父 Ack 在提交前 panic(模拟进程崩)→ 事务回滚,父仍 running、子仍 blocked、不丢数据;
// 租约过期后 ReapExpired 回收父(LeaseLost+1 回 pending)→ 重跑 Ack 成功 → 子被唤醒。
func TestAckCrashHookRecovery(t *testing.T) {
	b, clk := newTestBroker(t)
	ctx := context.Background()

	parent := &taskgate.Task{ID: "p", Type: "step", Queue: "qp"}
	if err := b.Enqueue(ctx, parent); err != nil {
		t.Fatalf("Enqueue(p) 失败: %v", err)
	}
	child := &taskgate.Task{ID: "c", Type: "step", Queue: "qc", DependsOn: []string{"p"}}
	if err := b.Enqueue(ctx, child); err != nil {
		t.Fatalf("Enqueue(c) 失败: %v", err)
	}

	claimed, err := b.Dequeue(ctx, []string{"qp"})
	if err != nil {
		t.Fatalf("Dequeue 失败: %v", err)
	}

	// 注入:Ack 事务所有写入完成之后、提交之前 panic。
	SetTestHookBeforeAckCommit(func() { panic("simulated crash before commit") })
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("注入 hook 应让 Ack panic")
			}
		}()
		_ = b.Ack(ctx, "p", claimed.LeaseToken, []byte(`"r"`))
	}()
	SetTestHookBeforeAckCommit(nil)

	// 事务必须已回滚:父还在 running,子还在 blocked,结果没写进去。
	p, _ := b.Get(ctx, "p")
	if p.Status != taskgate.StatusRunning || p.Result != nil {
		t.Fatalf("崩溃后父应保持 running 且无 Result,实际 status=%s result=%s", p.Status, p.Result)
	}
	c, _ := b.Get(ctx, "c")
	if c.Status != taskgate.StatusBlocked {
		t.Fatalf("崩溃后子应保持 blocked,实际 %s", c.Status)
	}

	// 恢复路径:租约过期 → 回收 → 重跑 → 子正常唤醒,一个唤醒都不丢。
	clk.Advance(61 * time.Second)
	if n, err := b.ReapExpired(ctx); err != nil || n != 1 {
		t.Fatalf("回收应捞回 1 条,实际 n=%d err=%v", n, err)
	}
	claimed, err = b.Dequeue(ctx, []string{"qp"})
	if err != nil {
		t.Fatalf("重新认领失败: %v", err)
	}
	if err := b.Ack(ctx, "p", claimed.LeaseToken, []byte(`"r"`)); err != nil {
		t.Fatalf("重跑 Ack 失败: %v", err)
	}
	if c, _ = b.Get(ctx, "c"); c.Status != taskgate.StatusPending {
		t.Fatalf("父完成后子应被唤醒为 pending,实际 %s", c.Status)
	}
	if p, _ = b.Get(ctx, "p"); p.LeaseLost != 1 {
		t.Fatalf("崩溃恢复应记 LeaseLost=1,实际 %d", p.LeaseLost)
	}
}

// TestReapDefensiveRepair 防御修复:人为制造"父已终态但子还卡在 blocked"的事故现场
// (直接改库,模拟分层事务时代的半程崩溃残留),ReapExpired 必须按提交时同一套决策补齐。
func TestReapDefensiveRepair(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()

	// 现场 1:父 completed,子却还 blocked → 应补唤醒成 pending。
	mustEnqueue(t, b, &taskgate.Task{ID: "p1", Type: "s", Queue: "q1"})
	mustEnqueue(t, b, &taskgate.Task{ID: "c1", Type: "s", Queue: "qx", DependsOn: []string{"p1"}})
	claimAck(t, b, "q1", "p1")
	// 把 c1 打回事故状态。
	if _, err := b.db.Exec(`UPDATE tasks SET status='blocked', pending_parents=1 WHERE id='c1'`); err != nil {
		t.Fatalf("制造现场失败: %v", err)
	}

	// 现场 2:父 failed 且 FailFast,子却还 blocked → 应补取消,并把取消传播给孙。
	mustEnqueue(t, b, &taskgate.Task{ID: "p2", Type: "s", Queue: "q2"})
	mustEnqueue(t, b, &taskgate.Task{ID: "c2", Type: "s", Queue: "qx", DependsOn: []string{"p2"}})
	mustEnqueue(t, b, &taskgate.Task{ID: "g2", Type: "s", Queue: "qx", DependsOn: []string{"c2"}})
	claimed, err := b.Dequeue(ctx, []string{"q2"})
	if err != nil {
		t.Fatalf("Dequeue(q2) 失败: %v", err)
	}
	if err := b.Fail(ctx, "p2", claimed.LeaseToken, "boom", taskgate.FailSkip, time.Time{}); err != nil {
		t.Fatalf("Fail(p2) 失败: %v", err)
	}
	if _, err := b.db.Exec(`UPDATE tasks SET status='blocked', pending_parents=1, last_error='', finished_at=0 WHERE id IN ('c2','g2')`); err != nil {
		t.Fatalf("制造现场失败: %v", err)
	}

	// 触发防御修复(没有过期租约,返回 0,但 blocked 事故要全被补齐)。
	if n, err := b.ReapExpired(ctx); err != nil || n != 0 {
		t.Fatalf("无过期租约,回收数应为 0,实际 n=%d err=%v", n, err)
	}
	if got, _ := b.Get(ctx, "c1"); got.Status != taskgate.StatusPending {
		t.Fatalf("c1 应被补唤醒为 pending,实际 %s", got.Status)
	}
	if got, _ := b.Get(ctx, "c2"); got.Status != taskgate.StatusCanceled || got.LastError != "parent p2 failed" {
		t.Fatalf("c2 应被补取消(parent p2 failed),实际 status=%s lastError=%q", got.Status, got.LastError)
	}
	if got, _ := b.Get(ctx, "g2"); got.Status != taskgate.StatusCanceled || got.LastError != "parent c2 canceled" {
		t.Fatalf("孙 g2 应被连锁取消(parent c2 canceled),实际 status=%s lastError=%q", got.Status, got.LastError)
	}
}

// TestNotifyPanicAndOpenError Notify 回调 panic 不影响主流程;Open 坏路径返回错误。
func TestNotifyPanicAndOpenError(t *testing.T) {
	clk := fakeclock.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	b, err := Open(filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer b.Close()
	notified := make(chan taskgate.Task, 4)
	if err := b.Init(taskgate.BrokerOptions{
		Clock: clk,
		Notify: func(tk taskgate.Task) {
			notified <- tk
			panic("callback boom") // 回调 panic 必须被 recover 包住
		},
	}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	if err := b.Enqueue(context.Background(), &taskgate.Task{ID: "n1", Type: "s", Queue: "q"}); err != nil {
		t.Fatalf("Enqueue 失败: %v", err)
	}
	select {
	case tk := <-notified:
		if tk.ID != "n1" || tk.Status != taskgate.StatusPending {
			t.Fatalf("Notify 快照不对: %+v", tk)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Notify 回调没被触发")
	}
	// 回调炸了,后续操作必须照常。
	if _, err := b.Get(context.Background(), "n1"); err != nil {
		t.Fatalf("回调 panic 后 Get 应照常工作: %v", err)
	}

	// Open 指向不存在的目录 → 建表失败报错,不 panic。
	if _, err := Open(filepath.Join(t.TempDir(), "no-such-dir", "x.db")); err == nil {
		t.Fatal("Open 到不存在目录应返回错误")
	}

	// 未 Init 就用 → 明确报错。
	raw, err := Open(filepath.Join(t.TempDir(), "raw.db"))
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer raw.Close()
	if err := raw.Enqueue(context.Background(), &taskgate.Task{Type: "s", Queue: "q"}); err == nil {
		t.Fatal("未 Init 的 broker 应拒绝使用")
	}
}

// mustEnqueue 入队断言成功。
func mustEnqueue(t *testing.T, b *Broker, tk *taskgate.Task) {
	t.Helper()
	if err := b.Enqueue(context.Background(), tk); err != nil {
		t.Fatalf("Enqueue(%s) 失败: %v", tk.ID, err)
	}
}

// claimAck 认领指定队列里的任务并 Ack 掉(断言认领到的是期望任务)。
func claimAck(t *testing.T, b *Broker, queue, id string) {
	t.Helper()
	claimed, err := b.Dequeue(context.Background(), []string{queue})
	if err != nil {
		t.Fatalf("Dequeue(%s) 失败: %v", queue, err)
	}
	if claimed.ID != id {
		t.Fatalf("预期认领到 %s,实际 %s", id, claimed.ID)
	}
	if err := b.Ack(context.Background(), id, claimed.LeaseToken, nil); err != nil {
		t.Fatalf("Ack(%s) 失败: %v", id, err)
	}
}

// TestLegacySchemaUpgrade(spec 005,SC-006)存量库升级:用旧 schema(无
// business_key/replay_of 列)手工建库,Open 后必须完成幂等迁移——旧任务可读、
// 字段解释正确(ID 即 ExecutionID、无键无重放来源),新写路径(键幂等/Replay)可用。
func TestLegacySchemaUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")

	// 用旧版列清单手工建表并塞一条"历史任务"(模拟 spec 004 时代写入的数据)。
	legacy, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE tasks (
		id TEXT PRIMARY KEY, type TEXT NOT NULL, queue TEXT NOT NULL, payload BLOB,
		status TEXT NOT NULL, result BLOB, last_error TEXT NOT NULL DEFAULT '',
		attempts INTEGER NOT NULL DEFAULT 0, max_retry INTEGER NOT NULL DEFAULT 0,
		lease_lost INTEGER NOT NULL DEFAULT 0, throttled INTEGER NOT NULL DEFAULT 0,
		run_at INTEGER NOT NULL, depends_on TEXT NOT NULL DEFAULT '[]',
		on_parent_fail TEXT NOT NULL DEFAULT 'fail_fast',
		pending_parents INTEGER NOT NULL DEFAULT 0, lease_token TEXT NOT NULL DEFAULT '',
		lease_until INTEGER NOT NULL DEFAULT 0, cancel_requested INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL, started_at INTEGER NOT NULL DEFAULT 0,
		finished_at INTEGER NOT NULL DEFAULT 0);
		CREATE TABLE task_deps (child_id TEXT NOT NULL, parent_id TEXT NOT NULL,
		done INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (child_id, parent_id));`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if _, err := legacy.Exec(`INSERT INTO tasks
		(id, type, queue, payload, status, run_at, created_at)
		VALUES ('old-1', 'llm', 'q', X'7B7D', 'completed', 1000, 1000)`); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	_ = legacy.Close()

	// 新版本 Open:迁移必须幂等完成,不动存量数据。
	b, err := Open(path)
	if err != nil {
		t.Fatalf("Open 应完成存量库迁移,却失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Init(taskgate.BrokerOptions{Clock: fakeclock.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	ctx := context.Background()

	old, err := b.Get(ctx, "old-1")
	if err != nil {
		t.Fatalf("存量任务应可读: %v", err)
	}
	if old.BusinessKey != "" || old.ReplayOf != "" || old.Status != taskgate.StatusCompleted {
		t.Fatalf("存量任务字段解释不对: %+v", old)
	}

	// 新写路径可用:键幂等 + Replay(对存量 completed 任务显式重放)。
	tk := &taskgate.Task{Type: "llm", Queue: "q", BusinessKey: "new-key"}
	if err := b.Enqueue(ctx, tk); err != nil {
		t.Fatalf("迁移后带键入队应可用: %v", err)
	}
	if err := b.Enqueue(ctx, &taskgate.Task{Type: "llm", Queue: "q", BusinessKey: "new-key"}); !errors.Is(err, taskgate.ErrTaskExists) {
		t.Fatalf("迁移后键幂等应生效: %v", err)
	}
	nt, err := b.Replay(ctx, taskgate.ReplayRequest{ExecutionID: "old-1", AllowCompleted: true})
	if err != nil || nt.ReplayOf != "old-1" {
		t.Fatalf("迁移后对存量任务 Replay 应可用: nt=%+v err=%v", nt, err)
	}

	// 再 Open 一次:迁移必须幂等(duplicate column 被吞掉)。
	b2, err := Open(path)
	if err != nil {
		t.Fatalf("二次 Open(已迁移库)应幂等成功: %v", err)
	}
	_ = b2.Close()
}
