package brokertest

// 契约 17:Notify 状态流转回调。回调在锁/事务外异步触发,**不保证跨操作顺序**、
// 不保证即时可见,用例只带超时轮询断言"每次流转最终都能观测到一个快照";
// 收集器每次记录完就故意 panic,顺带验证"回调 panic 不影响主流程"。

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ambrose/taskgate"
)

// notifyCollector 线程安全的回调收集器。record 记录完快照后故意 panic:
// 合同要求后端必须 recover 包住回调,panic 不得砸掉触发它的主流程。
type notifyCollector struct {
	mu    sync.Mutex
	snaps []taskgate.Task
}

// record 作为 BrokerOptions.Notify 注入。
func (c *notifyCollector) record(t taskgate.Task) {
	c.mu.Lock()
	c.snaps = append(c.snaps, t)
	c.mu.Unlock()
	panic("notify callback panic(契约:必须被 recover,不得影响主流程)")
}

// snapshot 取当前已收到的全部快照副本。
func (c *notifyCollector) snapshot() []taskgate.Task {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]taskgate.Task(nil), c.snaps...)
}

// 契约 17 Notify:Enqueue/Dequeue/Ack 一条链,最终能观测到 pending/running/completed
// 三个快照且字段正确;回调 panic 不影响主流程。
func caseNotify(t *testing.T, h *harness) {
	ctx := context.Background()
	h.enqueue(t, task("nt", "llm", "q"))
	claimed := h.dequeue(t, "q")
	// 收集器每次都 panic:Ack 必须照常成功(recover 是后端的责任)。
	if err := h.b.Ack(ctx, "nt", claimed.LeaseToken, []byte(`"done"`)); err != nil {
		t.Fatalf("回调 panic 不得影响主流程,Ack 却失败: %v", err)
	}

	// 回调是异步的:带超时轮询真时间,直到三个状态的快照都出现。
	// 注意:不断言快照到达顺序(跨操作顺序不做合同),只断言最终可观测。
	deadline := time.Now().Add(waitTimeout)
	for {
		seen := map[taskgate.Status]taskgate.Task{}
		for _, s := range h.notify.snapshot() {
			if s.ID == "nt" {
				seen[s.Status] = s
			}
		}
		p, hasP := seen[taskgate.StatusPending]
		r, hasR := seen[taskgate.StatusRunning]
		c, hasC := seen[taskgate.StatusCompleted]
		if hasP && hasR && hasC {
			// 字段抽查:快照必须携带任务的真实内容。
			if p.Type != "llm" || p.Queue != "q" {
				t.Fatalf("pending 快照字段不对: Type=%q Queue=%q", p.Type, p.Queue)
			}
			if r.LeaseToken == "" {
				t.Fatal("running 快照应携带认领时发的租约令牌")
			}
			if !bytes.Equal(c.Result, []byte(`"done"`)) {
				t.Fatalf("completed 快照的 Result 应为 %q,实际 %s", `"done"`, c.Result)
			}
			if c.FinishedAt.IsZero() {
				t.Fatal("completed 快照应带 FinishedAt")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 Notify 快照超时(%v):期望观测到 pending/running/completed,实际只有 %v",
				waitTimeout, seen)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 回调 panic 之后,后续操作必须照常工作。
	h.enqueue(t, task("nt2", "llm", "q"))
	h.mustStatus(t, "nt2", taskgate.StatusPending)
}
