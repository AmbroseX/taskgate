package brokertest

// 契约 1~5:入队取回、幂等 ID、认领互斥、阻塞出队、延迟任务。

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ambrose/taskgate"
)

// 契约 1 RoundTrip:入队再 Get,全字段一致;Get 返回副本,改了不影响存储。
func caseRoundTrip(t *testing.T, h *harness) {
	// 先放一个父任务,让 DependsOn 字段也能走一遍完整往返。
	parent := h.enqueue(t, task("p1", "ocr", "q-ocr"))

	runAt := h.now().Add(3 * time.Minute)
	in := &taskgate.Task{
		ID:              "t1",
		Type:            "llm",
		Queue:           "q-llm",
		Payload:         []byte(`{"prompt":"hi"}`),
		MaxRetry:        5,
		RunAt:           runAt,
		DependsOn:       []string{parent.ID},
		OnParentFailure: taskgate.IgnoreParentFail,
	}
	h.enqueue(t, in)

	got := h.get(t, "t1")
	if got.ID != "t1" || got.Type != "llm" || got.Queue != "q-llm" {
		t.Fatalf("往返后基础字段不一致: ID=%q Type=%q Queue=%q", got.ID, got.Type, got.Queue)
	}
	if !bytes.Equal(got.Payload, in.Payload) {
		t.Fatalf("往返后 Payload 不一致: %s != %s", got.Payload, in.Payload)
	}
	if got.MaxRetry != 5 {
		t.Fatalf("往返后 MaxRetry 不一致: %d != 5", got.MaxRetry)
	}
	if !got.RunAt.Equal(runAt) {
		t.Fatalf("往返后 RunAt 不一致: %v != %v", got.RunAt, runAt)
	}
	if len(got.DependsOn) != 1 || got.DependsOn[0] != parent.ID {
		t.Fatalf("往返后 DependsOn 不一致: %v != [%s]", got.DependsOn, parent.ID)
	}
	if got.OnParentFailure != taskgate.IgnoreParentFail {
		t.Fatalf("往返后 OnParentFailure 不一致: %q", got.OnParentFailure)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("CreatedAt 应在入队时写入,不能为零值")
	}
	// 父在 pending(未终态)→ 子应为 blocked。
	if got.Status != taskgate.StatusBlocked {
		t.Fatalf("父未完成,子任务状态应为 blocked,实际 %s", got.Status)
	}

	// 没给 RunAt 的任务默认取当前时刻。
	plain := h.enqueue(t, task("t2", "llm", "q-llm"))
	if got2 := h.get(t, plain.ID); !got2.RunAt.Equal(h.now()) {
		t.Fatalf("RunAt 零值应取入队时刻 %v,实际 %v", h.now(), got2.RunAt)
	}

	// Get 必须返回副本:调用方改返回值不能影响存储。
	got.Payload[0] = 'X'
	got.DependsOn[0] = "hacked"
	got.Status = taskgate.StatusFailed
	again := h.get(t, "t1")
	if !bytes.Equal(again.Payload, in.Payload) || again.DependsOn[0] != parent.ID || again.Status != taskgate.StatusBlocked {
		t.Fatal("Get 返回的必须是副本:修改返回值污染了存储中的任务")
	}
}

// 契约 2 IdempotentID:同 ID 二次入队 → ErrTaskExists,且原任务原封不动。
func caseIdempotentID(t *testing.T, h *harness) {
	first := task("dup", "llm", "q")
	first.Payload = []byte(`"original"`)
	h.enqueue(t, first)

	second := task("dup", "other", "q2")
	second.Payload = []byte(`"overwrite"`)
	err := h.b.Enqueue(context.Background(), second)
	if !errors.Is(err, taskgate.ErrTaskExists) {
		t.Fatalf("同 ID 二次 Enqueue 应返回 ErrTaskExists,实际 %v", err)
	}

	got := h.get(t, "dup")
	if got.Type != "llm" || got.Queue != "q" || !bytes.Equal(got.Payload, []byte(`"original"`)) {
		t.Fatalf("二次 Enqueue 不得覆盖原任务,实际变成了 Type=%q Queue=%q Payload=%s",
			got.Type, got.Queue, got.Payload)
	}
}

// 契约 3 ClaimMutex:1 个任务 100 并发 Dequeue,恰好 1 个成功。
func caseClaimMutex(t *testing.T, h *harness) {
	h.enqueue(t, task("only", "llm", "q"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 100
	results := make(chan dequeueResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk, err := h.b.Dequeue(ctx, []string{"q"})
			results <- dequeueResult{task: tk, err: err}
		}()
	}

	// 等第一个抢到的出现(带保护超时),然后取消其余 99 个的阻塞等待。
	deadline := time.After(waitTimeout)
	got := 0
	var tokens []string
	for done := 0; done < n; done++ {
		select {
		case r := <-results:
			if r.err == nil {
				got++
				tokens = append(tokens, r.task.LeaseToken)
				cancel() // 有人抢到了,放其它 goroutine 回家
			} else if !errors.Is(r.err, context.Canceled) {
				t.Fatalf("并发 Dequeue 失败时只应返回 ctx 取消错误,实际 %v", r.err)
			}
		case <-deadline:
			t.Fatalf("100 并发 Dequeue 超过 %v 未全部返回(已返回 %d 个):可能有 goroutine 挂死", waitTimeout, done)
		}
	}
	wg.Wait()

	if got != 1 {
		t.Fatalf("1 个任务被 %d 个 worker 同时认领成功,认领互斥被打破(必须恰好 1 个)", got)
	}
	if tokens[0] == "" {
		t.Fatal("认领成功必须携带新租约令牌")
	}
	h.mustStatus(t, "only", taskgate.StatusRunning)
}

// 契约 4 BlockingDequeue:空队列阻塞;入队后被唤醒;ctx 取消立即退出;空 queues 报错。
func caseBlockingDequeue(t *testing.T, h *harness) {
	// queues 为空 → 直接报错,不阻塞。
	if _, err := h.b.Dequeue(context.Background(), nil); err == nil {
		t.Fatal("Dequeue(queues=nil) 必须报错:不指定队列的认领没有意义")
	}

	// 空队列阻塞,入队后被唤醒拿到那个任务。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := h.asyncDequeue(ctx, "q")
	stillBlocked(t, ch, "空队列上的 Dequeue")

	h.enqueue(t, task("wake", "llm", "q"))
	r := waitResult(t, ch, "入队后唤醒阻塞中的 Dequeue")
	if r.err != nil {
		t.Fatalf("入队后阻塞的 Dequeue 应拿到任务,却报错: %v", r.err)
	}
	if r.task.ID != "wake" {
		t.Fatalf("阻塞的 Dequeue 应拿到刚入队的任务 wake,实际 %s", r.task.ID)
	}

	// ctx 取消 → 立即返回 ctx.Err()。
	ctx2, cancel2 := context.WithCancel(context.Background())
	ch2 := h.asyncDequeue(ctx2, "q")
	stillBlocked(t, ch2, "第二个空队列 Dequeue")
	cancel2()
	r2 := waitResult(t, ch2, "ctx 取消后 Dequeue 退出")
	if !errors.Is(r2.err, context.Canceled) {
		t.Fatalf("ctx 取消后 Dequeue 应返回 context.Canceled,实际 task=%v err=%v", r2.task, r2.err)
	}
}

// 契约 5 DelayedTask:RunAt 未到不出队;时间推进到点后,阻塞中的 Dequeue 也必须被唤醒。
func caseDelayedTask(t *testing.T, h *harness) {
	delayed := task("later", "llm", "q")
	delayed.RunAt = h.now().Add(5 * time.Minute)
	h.enqueue(t, delayed)

	// 没到点:不可认领。
	h.expectBlocked(t, "q")

	// 差 1 秒也不行:边界必须卡在 RunAt 上。
	h.advance(5*time.Minute - time.Second)
	h.expectBlocked(t, "q")

	// 挂一个阻塞 Dequeue,再把时间推过 RunAt:实现必须把等待挂在 clock 上,
	// 到点自动唤醒,而不是只等新任务入队的信号。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := h.asyncDequeue(ctx, "q")
	stillBlocked(t, ch, "延迟任务到点前的 Dequeue")

	h.advance(time.Second)
	r := waitResult(t, ch, "延迟任务到点后唤醒 Dequeue")
	if r.err != nil {
		t.Fatalf("到点后阻塞的 Dequeue 应拿到延迟任务,却报错: %v", r.err)
	}
	if r.task.ID != "later" {
		t.Fatalf("到点后应认领到延迟任务 later,实际 %s", r.task.ID)
	}
}
