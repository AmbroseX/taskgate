package brokertest

// 契约 10~12:依赖唤醒(含 fan-in、提交时父已完成)、连锁取消(含链式孙、
// IgnoreParentFailure、提交时父已失败)、各状态下的 Cancel 语义。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ambrose/taskgate"
)

// dependsOn 造一个带依赖的任务。
func dependsOn(id, typ, queue string, parents ...string) *taskgate.Task {
	tk := task(id, typ, queue)
	tk.DependsOn = parents
	return tk
}

// ackByID 认领指定队列里的任务并断言是期望的那个,然后 Ack 掉。
func (h *harness) ackByID(t *testing.T, queue, id string) {
	t.Helper()
	claimed := h.dequeue(t, queue)
	if claimed.ID != id {
		t.Fatalf("预期认领到 %s,实际 %s(队列 %s)", id, claimed.ID, queue)
	}
	if err := h.b.Ack(context.Background(), id, claimed.LeaseToken, nil); err != nil {
		t.Fatalf("Ack(%s) 失败: %v", id, err)
	}
}

// failByID 认领指定任务并用 FailSkip 直接打成 failed。
func (h *harness) failByID(t *testing.T, queue, id string) {
	t.Helper()
	claimed := h.dequeue(t, queue)
	if claimed.ID != id {
		t.Fatalf("预期认领到 %s,实际 %s(队列 %s)", id, claimed.ID, queue)
	}
	if err := h.b.Fail(context.Background(), id, claimed.LeaseToken, "boom", taskgate.FailSkip, time.Time{}); err != nil {
		t.Fatalf("Fail(%s, FailSkip) 失败: %v", id, err)
	}
}

// 契约 10 DepWake:父 Ack 后子原子唤醒;fan-in 全父完成才醒;提交时父已完成直接 pending。
func caseDepWake(t *testing.T, h *harness) {
	// 父不存在 → 拒收,ErrTaskNotFound。
	err := h.b.Enqueue(context.Background(), dependsOn("orphan", "llm", "q", "no-such-parent"))
	if !errors.Is(err, taskgate.ErrTaskNotFound) {
		t.Fatalf("DependsOn 指向不存在的父任务应拒收(ErrTaskNotFound),实际 %v", err)
	}

	// 报错路径不得泄漏生成的 ID:空 ID 入队失败后,调用方的 t.ID 必须保持为空,
	// 否则调用方会拿着一个根本不存在的孤儿 ID 去查任务。
	noID := dependsOn("", "llm", "q", "no-such-parent")
	if err := h.b.Enqueue(context.Background(), noID); !errors.Is(err, taskgate.ErrTaskNotFound) {
		t.Fatalf("空 ID + 父不存在应拒收(ErrTaskNotFound),实际 %v", err)
	}
	if noID.ID != "" {
		t.Fatalf("Enqueue 失败后不得把生成的 ID 回填给调用方,实际泄漏了 %q", noID.ID)
	}

	// 基本唤醒:子在父完成前是 blocked 且不可认领;父 Ack 后,
	// 阻塞在子队列上的 Dequeue 必须被唤醒(证明"终态+唤醒"对认领方立即可见)。
	h.enqueue(t, task("p", "ocr", "qp"))
	h.enqueue(t, dependsOn("c", "llm", "qc", "p"))
	h.mustStatus(t, "c", taskgate.StatusBlocked)
	h.expectBlocked(t, "qc")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := h.asyncDequeue(ctx, "qc")
	stillBlocked(t, ch, "子任务 blocked 期间的 Dequeue")

	h.ackByID(t, "qp", "p")
	r := waitResult(t, ch, "父 Ack 后唤醒子队列的 Dequeue")
	if r.err != nil || r.task.ID != "c" {
		t.Fatalf("父完成后子应被唤醒并可认领,实际 task=%v err=%v", r.task, r.err)
	}

	// fan-in:两个父都完成才唤醒。
	h.enqueue(t, task("p1", "ocr", "qp"))
	h.enqueue(t, task("p2", "ocr", "qp"))
	h.enqueue(t, dependsOn("fan", "llm", "qc", "p1", "p2"))
	h.mustStatus(t, "fan", taskgate.StatusBlocked)

	h.ackByID(t, "qp", "p1")
	h.mustStatus(t, "fan", taskgate.StatusBlocked) // 只完成一个父,还不能醒
	h.ackByID(t, "qp", "p2")
	h.mustStatus(t, "fan", taskgate.StatusPending) // 两个父都完成,唤醒

	// 提交时父已完成 → 不经过 blocked,直接 pending。
	h.enqueue(t, dependsOn("late", "llm", "qc", "p"))
	h.mustStatus(t, "late", taskgate.StatusPending)
}

// 契约 11 CascadeCancel:父 failed → FailFast 子 canceled → 孙 canceled(链式);
// IgnoreParentFailure 子照常唤醒;提交时父已 failed 子直接 canceled。
func caseCascadeCancel(t *testing.T, h *harness) {
	// 三级链:A ← B ← C,全部默认 FailFast。
	h.enqueue(t, task("A", "step", "qa"))
	h.enqueue(t, dependsOn("B", "step", "qb", "A"))
	h.enqueue(t, dependsOn("C", "step", "qc", "B"))

	h.failByID(t, "qa", "A")
	h.mustStatus(t, "A", taskgate.StatusFailed)

	// 链式传播:Fail 调用返回时整条链应已取消(memory 在同一临界区、
	// sqlite 逐层事务,但都在触发调用内收敛)。
	b := h.mustStatus(t, "B", taskgate.StatusCanceled)
	if b.LastError != "parent A failed" {
		t.Fatalf("B 的 LastError 应为固定文案 %q,实际 %q", "parent A failed", b.LastError)
	}
	if b.FinishedAt.IsZero() {
		t.Fatal("连锁取消的 B 应写 FinishedAt")
	}
	c := h.mustStatus(t, "C", taskgate.StatusCanceled)
	if c.LastError != "parent B canceled" {
		t.Fatalf("孙任务 C 的 LastError 应为 %q(由 B 的取消传下来),实际 %q", "parent B canceled", c.LastError)
	}

	// IgnoreParentFailure:父失败也照常唤醒。
	h.enqueue(t, task("A2", "step", "qa"))
	ign := dependsOn("B2", "step", "qb", "A2")
	ign.OnParentFailure = taskgate.IgnoreParentFail
	h.enqueue(t, ign)
	h.failByID(t, "qa", "A2")
	h.mustStatus(t, "B2", taskgate.StatusPending)

	// 提交时父已 failed:FailFast 子直接 canceled 落库。
	h.enqueue(t, dependsOn("D", "step", "qd", "A"))
	d := h.mustStatus(t, "D", taskgate.StatusCanceled)
	if d.LastError != "parent A failed" {
		t.Fatalf("提交即取消的 D,LastError 应为 %q,实际 %q", "parent A failed", d.LastError)
	}
	if d.FinishedAt.IsZero() {
		t.Fatal("提交即取消的 D 应写 FinishedAt")
	}

	// 提交时父已 failed 但 IgnoreParentFailure → 直接 pending。
	e := dependsOn("E", "step", "qd", "A")
	e.OnParentFailure = taskgate.IgnoreParentFail
	h.enqueue(t, e)
	h.mustStatus(t, "E", taskgate.StatusPending)
}

// 契约 12 CancelStates:blocked/pending/retrying 直接 canceled;running 只打标记,
// Heartbeat 报 ErrTaskCanceled,FinishCanceled 落库;终态 Cancel → ErrAlreadyFinal。
func caseCancelStates(t *testing.T, h *harness) {
	ctx := context.Background()

	// pending → canceled,且向下传播到 blocked 的子任务。
	h.enqueue(t, task("pd", "llm", "q"))
	h.enqueue(t, dependsOn("pd-child", "llm", "q2", "pd"))
	if err := h.b.Cancel(ctx, "pd"); err != nil {
		t.Fatalf("Cancel(pending) 失败: %v", err)
	}
	got := h.mustStatus(t, "pd", taskgate.StatusCanceled)
	if !got.FinishedAt.Equal(h.now()) {
		t.Fatalf("Cancel(pending) 应写 FinishedAt=%v,实际 %v", h.now(), got.FinishedAt)
	}
	child := h.mustStatus(t, "pd-child", taskgate.StatusCanceled)
	if child.LastError != "parent pd canceled" {
		t.Fatalf("被传播取消的子任务 LastError 应为 %q,实际 %q", "parent pd canceled", child.LastError)
	}

	// blocked → canceled(单独取消一个 blocked 任务,不影响父)。
	h.enqueue(t, task("bp", "llm", "q-bp"))
	h.enqueue(t, dependsOn("bk", "llm", "q2", "bp"))
	if err := h.b.Cancel(ctx, "bk"); err != nil {
		t.Fatalf("Cancel(blocked) 失败: %v", err)
	}
	h.mustStatus(t, "bk", taskgate.StatusCanceled)
	h.mustStatus(t, "bp", taskgate.StatusPending) // 父不受影响

	// retrying → canceled(独立队列,避免和其它排队任务抢认领)。
	rt := task("rt", "llm", "q-rt")
	rt.MaxRetry = 5
	h.enqueue(t, rt)
	claimed := h.dequeue(t, "q-rt")
	if err := h.b.Fail(ctx, "rt", claimed.LeaseToken, "x", taskgate.FailBusiness, h.now().Add(time.Hour)); err != nil {
		t.Fatalf("预置 Fail 失败: %v", err)
	}
	h.mustStatus(t, "rt", taskgate.StatusRetrying)
	if err := h.b.Cancel(ctx, "rt"); err != nil {
		t.Fatalf("Cancel(retrying) 失败: %v", err)
	}
	h.mustStatus(t, "rt", taskgate.StatusCanceled)

	// running:Cancel 只打标记不改状态;Heartbeat 报 ErrTaskCanceled(续租照做);
	// FinishCanceled 用有效令牌落库 canceled。
	h.enqueue(t, task("rn", "llm", "q3"))
	claimed = h.dequeue(t, "q3")
	if err := h.b.Cancel(ctx, "rn"); err != nil {
		t.Fatalf("Cancel(running) 应打标记并返回 nil,实际 %v", err)
	}
	h.mustStatus(t, "rn", taskgate.StatusRunning) // 状态不变,等 FinishCanceled
	if err := h.b.Heartbeat(ctx, "rn", claimed.LeaseToken); !errors.Is(err, taskgate.ErrTaskCanceled) {
		t.Fatalf("running 任务被请求取消后,Heartbeat 应返回 ErrTaskCanceled,实际 %v", err)
	}
	if err := h.b.FinishCanceled(ctx, "rn", claimed.LeaseToken); err != nil {
		t.Fatalf("FinishCanceled 失败: %v", err)
	}
	got = h.mustStatus(t, "rn", taskgate.StatusCanceled)
	if got.FinishedAt.IsZero() {
		t.Fatal("FinishCanceled 应写 FinishedAt")
	}

	// 终态 Cancel → ErrAlreadyFinal。
	if err := h.b.Cancel(ctx, "rn"); !errors.Is(err, taskgate.ErrAlreadyFinal) {
		t.Fatalf("Cancel(canceled) 应返回 ErrAlreadyFinal,实际 %v", err)
	}

	// running 被请求取消后 worker 崩了(不心跳):租约过期时 ReapExpired 必须把它
	// 落成 canceled(不占 LeaseLost)并向下传播,取消请求不能凭空丢失。
	h.enqueue(t, task("rp", "llm", "q-rp"))
	h.enqueue(t, dependsOn("rp-child", "llm", "q-rp2", "rp"))
	h.dequeue(t, "q-rp")
	if err := h.b.Cancel(ctx, "rp"); err != nil {
		t.Fatalf("Cancel(running) 应打标记并返回 nil,实际 %v", err)
	}
	h.advance(61 * time.Second) // 过 TTL(60s),模拟 worker 崩溃后不再心跳
	if n, err := h.b.ReapExpired(ctx); err != nil || n != 1 {
		t.Fatalf("带取消标记的过期任务应被回收 1 条,实际 n=%d err=%v", n, err)
	}
	got = h.mustStatus(t, "rp", taskgate.StatusCanceled)
	if got.LeaseLost != 0 {
		t.Fatalf("带取消标记的过期任务直接落 canceled,不占 LeaseLost,实际 %d", got.LeaseLost)
	}
	if got.FinishedAt.IsZero() {
		t.Fatal("ReapExpired 落 canceled 应写 FinishedAt")
	}
	child = h.mustStatus(t, "rp-child", taskgate.StatusCanceled)
	if child.LastError != "parent rp canceled" {
		t.Fatalf("Reap 落 canceled 后应向下传播,子任务 LastError 应为 %q,实际 %q",
			"parent rp canceled", child.LastError)
	}
	// 过期后 Dequeue 不得再认领到它(已是终态)。
	h.expectBlocked(t, "q-rp")
}
