package brokertest

// 契约 6~9、15、16:Ack/Fail 三种语义、租约回收、旧令牌、retrying 重认领、
// Requeue 不占计数、非法流转。

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ambrose/taskgate"
)

// 契约 6 AckFail:Ack 写结果;FailBusiness 走 Attempts;FailSkip 直接 failed;
// FailThrottled 走 Throttled 计数且不涨 Attempts(本用例 ThrottledMax 调成 3)。
func caseAckFail(t *testing.T, h *harness) {
	ctx := context.Background()

	// --- Ack:completed + Result + FinishedAt ---
	h.enqueue(t, task("ok", "llm", "q"))
	claimed := h.dequeue(t, "q")
	if !claimed.StartedAt.Equal(h.now()) {
		t.Fatalf("首次认领应写 StartedAt=%v,实际 %v", h.now(), claimed.StartedAt)
	}
	if err := h.b.Ack(ctx, "ok", claimed.LeaseToken, []byte(`{"answer":42}`)); err != nil {
		t.Fatalf("持有效令牌 Ack 失败: %v", err)
	}
	done := h.mustStatus(t, "ok", taskgate.StatusCompleted)
	if !bytes.Equal(done.Result, []byte(`{"answer":42}`)) {
		t.Fatalf("Ack 后 Result 应为 %s,实际 %s", `{"answer":42}`, done.Result)
	}
	if !done.FinishedAt.Equal(h.now()) {
		t.Fatalf("Ack 后 FinishedAt 应为 %v,实际 %v", h.now(), done.FinishedAt)
	}

	// --- FailBusiness:MaxRetry=2,失败 3 次(Attempts=3 > 2)进 failed ---
	biz := task("biz", "llm", "q")
	biz.MaxRetry = 2
	h.enqueue(t, biz)
	for i := 1; i <= 3; i++ {
		claimed = h.dequeue(t, "q")
		retryAt := h.now().Add(time.Second)
		errMsg := fmt.Sprintf("boom %d", i)
		if err := h.b.Fail(ctx, "biz", claimed.LeaseToken, errMsg, taskgate.FailBusiness, retryAt); err != nil {
			t.Fatalf("第 %d 次 FailBusiness 失败: %v", i, err)
		}
		got := h.get(t, "biz")
		if got.Attempts != i {
			t.Fatalf("第 %d 次业务失败后 Attempts 应为 %d,实际 %d", i, i, got.Attempts)
		}
		if got.LastError != errMsg {
			t.Fatalf("第 %d 次失败后 LastError 应为 %q,实际 %q", i, errMsg, got.LastError)
		}
		if i <= 2 {
			if got.Status != taskgate.StatusRetrying {
				t.Fatalf("Attempts=%d ≤ MaxRetry=2 应进 retrying,实际 %s", i, got.Status)
			}
			if !got.RunAt.Equal(retryAt) {
				t.Fatalf("retrying 的 RunAt 应写成 retryAt=%v,实际 %v", retryAt, got.RunAt)
			}
			h.advance(time.Second) // 推到退避到点,下一轮才能认领
		} else {
			if got.Status != taskgate.StatusFailed {
				t.Fatalf("Attempts=3 > MaxRetry=2 应进 failed,实际 %s", got.Status)
			}
			if !got.FinishedAt.Equal(h.now()) {
				t.Fatalf("耗尽进 failed 应写 FinishedAt=%v,实际 %v", h.now(), got.FinishedAt)
			}
		}
	}

	// --- FailSkip:直接 failed,不动任何计数 ---
	h.enqueue(t, task("skip", "llm", "q"))
	claimed = h.dequeue(t, "q")
	if err := h.b.Fail(ctx, "skip", claimed.LeaseToken, "no rescue", taskgate.FailSkip, time.Time{}); err != nil {
		t.Fatalf("FailSkip 失败: %v", err)
	}
	got := h.mustStatus(t, "skip", taskgate.StatusFailed)
	if got.Attempts != 0 || got.Throttled != 0 || got.LeaseLost != 0 {
		t.Fatalf("FailSkip 不是业务失败也不是限流,三计数都不该动,实际 Attempts=%d Throttled=%d LeaseLost=%d",
			got.Attempts, got.Throttled, got.LeaseLost)
	}
	if got.LastError != "no rescue" {
		t.Fatalf("FailSkip 后 LastError 应为 %q,实际 %q", "no rescue", got.LastError)
	}

	// --- FailThrottled:Attempts 不涨,Throttled 到 3(=ThrottledMax)封顶进 failed ---
	h.enqueue(t, task("thr", "llm", "q"))
	for i := 1; i <= 3; i++ {
		claimed = h.dequeue(t, "q")
		retryAt := h.now().Add(time.Second)
		if err := h.b.Fail(ctx, "thr", claimed.LeaseToken, "429 from gateway", taskgate.FailThrottled, retryAt); err != nil {
			t.Fatalf("第 %d 次 FailThrottled 失败: %v", i, err)
		}
		got = h.get(t, "thr")
		if got.Throttled != i {
			t.Fatalf("第 %d 次限流后 Throttled 应为 %d,实际 %d", i, i, got.Throttled)
		}
		if got.Attempts != 0 {
			t.Fatalf("FailThrottled 不占 Attempts,实际涨到了 %d", got.Attempts)
		}
		if i < 3 {
			if got.Status != taskgate.StatusRetrying {
				t.Fatalf("Throttled=%d < ThrottledMax=3 应进 retrying,实际 %s", i, got.Status)
			}
			if !got.RunAt.Equal(retryAt) {
				t.Fatalf("限流重排的 RunAt 应为 retryAt=%v,实际 %v", retryAt, got.RunAt)
			}
			h.advance(time.Second)
		} else {
			if got.Status != taskgate.StatusFailed {
				t.Fatalf("Throttled=3 ≥ ThrottledMax=3 应封顶进 failed,实际 %s", got.Status)
			}
			if got.LastError != "throttled 3 times" {
				t.Fatalf("限流封顶的 LastError 应为固定文案 %q,实际 %q", "throttled 3 times", got.LastError)
			}
		}
	}
}

// 契约 7 LeaseReap:租约过期被回收(LeaseLost+1 回 pending);到 LeaseLostMax 封顶 failed;
// Heartbeat 能续租不被误杀。LeaseTTL=60s,LeaseLostMax=3。
func caseLeaseReap(t *testing.T, h *harness) {
	ctx := context.Background()
	h.enqueue(t, task("poison", "llm", "q"))

	// 第 1 次:认领后不 Ack,没过期时 Reap 不该动它。
	h.dequeue(t, "q")
	if n, err := h.b.ReapExpired(ctx); err != nil || n != 0 {
		t.Fatalf("租约未过期时 ReapExpired 应回收 0 条,实际 n=%d err=%v", n, err)
	}
	h.advance(61 * time.Second) // 过 TTL(60s)
	n, err := h.b.ReapExpired(ctx)
	if err != nil || n != 1 {
		t.Fatalf("租约过期后 ReapExpired 应回收 1 条,实际 n=%d err=%v", n, err)
	}
	got := h.mustStatus(t, "poison", taskgate.StatusPending)
	if got.LeaseLost != 1 {
		t.Fatalf("第 1 次回收后 LeaseLost 应为 1,实际 %d", got.LeaseLost)
	}
	if got.LeaseToken != "" {
		t.Fatalf("回收后必须清掉租约令牌,实际还留着 %q", got.LeaseToken)
	}

	// Heartbeat 续租:45s 时续一次,90s 时(距续租仅 45s < TTL)不该被回收。
	claimed := h.dequeue(t, "q")
	h.advance(45 * time.Second)
	if err := h.b.Heartbeat(ctx, "poison", claimed.LeaseToken); err != nil {
		t.Fatalf("持有效令牌 Heartbeat 失败: %v", err)
	}
	h.advance(45 * time.Second) // 距认领 90s,但距续租只有 45s
	if n, err := h.b.ReapExpired(ctx); err != nil || n != 0 {
		t.Fatalf("Heartbeat 续租后不应被回收(距续租 45s < TTL 60s),实际 n=%d err=%v", n, err)
	}
	h.mustStatus(t, "poison", taskgate.StatusRunning)

	// 第 2 次回收。
	h.advance(61 * time.Second)
	if n, _ := h.b.ReapExpired(ctx); n != 1 {
		t.Fatalf("第 2 次过期应回收 1 条,实际 %d", n)
	}
	if got = h.mustStatus(t, "poison", taskgate.StatusPending); got.LeaseLost != 2 {
		t.Fatalf("第 2 次回收后 LeaseLost 应为 2,实际 %d", got.LeaseLost)
	}

	// 第 3 次:LeaseLost=3 ≥ LeaseLostMax=3 → failed,固定文案。
	h.dequeue(t, "q")
	h.advance(61 * time.Second)
	if n, _ := h.b.ReapExpired(ctx); n != 1 {
		t.Fatalf("第 3 次过期应回收 1 条,实际 %d", n)
	}
	got = h.mustStatus(t, "poison", taskgate.StatusFailed)
	if got.LeaseLost != 3 {
		t.Fatalf("封顶时 LeaseLost 应为 3,实际 %d", got.LeaseLost)
	}
	if got.LastError != "lease expired 3 times" {
		t.Fatalf("租约封顶的 LastError 应为固定文案 %q,实际 %q", "lease expired 3 times", got.LastError)
	}
}

// 契约 8 StaleToken:任务被回收再认领后,旧令牌的五个写操作全部 ErrLeaseLost。
func caseStaleToken(t *testing.T, h *harness) {
	ctx := context.Background()
	h.enqueue(t, task("st", "llm", "q"))
	old := h.dequeue(t, "q")

	h.advance(61 * time.Second)
	if n, _ := h.b.ReapExpired(ctx); n != 1 {
		t.Fatal("预置失败:租约过期后应回收 1 条")
	}
	fresh := h.dequeue(t, "q") // 重新认领,生成新令牌
	if fresh.LeaseToken == old.LeaseToken {
		t.Fatal("重新认领必须生成新令牌,不能复用旧令牌")
	}

	checks := []struct {
		op  string
		err error
	}{
		{"Ack", h.b.Ack(ctx, "st", old.LeaseToken, nil)},
		{"Fail", h.b.Fail(ctx, "st", old.LeaseToken, "x", taskgate.FailBusiness, h.now())},
		{"Heartbeat", h.b.Heartbeat(ctx, "st", old.LeaseToken)},
		{"Requeue", h.b.Requeue(ctx, "st", old.LeaseToken)},
		{"FinishCanceled", h.b.FinishCanceled(ctx, "st", old.LeaseToken)},
	}
	for _, c := range checks {
		if !errors.Is(c.err, taskgate.ErrLeaseLost) {
			t.Fatalf("旧令牌 %s 应返回 ErrLeaseLost,实际 %v", c.op, c.err)
		}
	}
	// 旧令牌全被拒后,新令牌必须仍然可用。
	if err := h.b.Ack(ctx, "st", fresh.LeaseToken, nil); err != nil {
		t.Fatalf("新令牌 Ack 应成功,实际 %v", err)
	}
}

// 契约 9 RetryingReclaim:retrying 到点后可被重新认领。
func caseRetryingReclaim(t *testing.T, h *harness) {
	ctx := context.Background()
	tk := task("rt", "llm", "q")
	tk.MaxRetry = 5
	h.enqueue(t, tk)

	claimed := h.dequeue(t, "q")
	retryAt := h.now().Add(30 * time.Second)
	if err := h.b.Fail(ctx, "rt", claimed.LeaseToken, "flaky", taskgate.FailBusiness, retryAt); err != nil {
		t.Fatalf("FailBusiness 失败: %v", err)
	}
	h.mustStatus(t, "rt", taskgate.StatusRetrying)

	// 退避没到点:不可认领。
	h.expectBlocked(t, "q")

	h.advance(30 * time.Second)
	again := h.dequeue(t, "q")
	if again.ID != "rt" {
		t.Fatalf("到点后应重新认领到 rt,实际 %s", again.ID)
	}
	if again.LeaseToken == claimed.LeaseToken {
		t.Fatal("重新认领必须生成新令牌")
	}
	if again.Attempts != 1 {
		t.Fatalf("重新认领不动 Attempts,应保持 1,实际 %d", again.Attempts)
	}
}

// 契约 15 RequeueNoCount:Requeue 回 pending,三计数与 RunAt 全不动,取消标记被清。
func caseRequeueNoCount(t *testing.T, h *harness) {
	ctx := context.Background()
	tk := task("rq", "llm", "q")
	tk.MaxRetry = 3
	h.enqueue(t, tk)

	// 先制造一点"历史":业务失败一次,让 Attempts=1、RunAt=退避时刻。
	claimed := h.dequeue(t, "q")
	retryAt := h.now().Add(time.Second)
	if err := h.b.Fail(ctx, "rq", claimed.LeaseToken, "once", taskgate.FailBusiness, retryAt); err != nil {
		t.Fatalf("预置 FailBusiness 失败: %v", err)
	}
	h.advance(time.Second)
	claimed = h.dequeue(t, "q")
	before := h.get(t, "rq")

	// running 中收到取消请求(只打标记),随后 Shutdown 场景下被 Requeue 归还。
	if err := h.b.Cancel(ctx, "rq"); err != nil {
		t.Fatalf("Cancel(running) 应只打标记并返回 nil,实际 %v", err)
	}
	if err := h.b.Requeue(ctx, "rq", claimed.LeaseToken); err != nil {
		t.Fatalf("Requeue 失败: %v", err)
	}

	got := h.mustStatus(t, "rq", taskgate.StatusPending)
	if got.Attempts != before.Attempts || got.LeaseLost != before.LeaseLost || got.Throttled != before.Throttled {
		t.Fatalf("Requeue 三计数全不动,期望 (%d,%d,%d),实际 (%d,%d,%d)",
			before.Attempts, before.LeaseLost, before.Throttled, got.Attempts, got.LeaseLost, got.Throttled)
	}
	if !got.RunAt.Equal(before.RunAt) {
		t.Fatalf("Requeue 不动 RunAt,期望 %v,实际 %v", before.RunAt, got.RunAt)
	}
	if got.LeaseToken != "" {
		t.Fatalf("Requeue 后必须清租约令牌,实际 %q", got.LeaseToken)
	}

	// 取消标记必须被清:重新认领后 Heartbeat 不该再报 ErrTaskCanceled。
	claimed = h.dequeue(t, "q")
	if err := h.b.Heartbeat(ctx, "rq", claimed.LeaseToken); err != nil {
		t.Fatalf("Requeue 应清掉 cancel_requested 标记,重新认领后 Heartbeat 却返回: %v", err)
	}
}

// 契约 16 IllegalTransition:对终态任务的写操作全部报错;抽查非法流转。
func caseIllegalTransition(t *testing.T, h *harness) {
	ctx := context.Background()

	// 预置一个 completed 任务,令牌是它生前最后一枚(Ack 后已失效)。
	h.enqueue(t, task("fin", "llm", "q"))
	claimed := h.dequeue(t, "q")
	token := claimed.LeaseToken
	if err := h.b.Ack(ctx, "fin", token, nil); err != nil {
		t.Fatalf("预置 Ack 失败: %v", err)
	}

	// completed 任务:五个令牌操作全应报 ErrLeaseLost(已不在 running,租约等于没了)。
	tokenOps := []struct {
		op  string
		err error
	}{
		{"Ack", h.b.Ack(ctx, "fin", token, nil)},
		{"Fail", h.b.Fail(ctx, "fin", token, "x", taskgate.FailBusiness, h.now())},
		{"Heartbeat", h.b.Heartbeat(ctx, "fin", token)},
		{"Requeue", h.b.Requeue(ctx, "fin", token)},
		{"FinishCanceled", h.b.FinishCanceled(ctx, "fin", token)},
	}
	for _, c := range tokenOps {
		if !errors.Is(c.err, taskgate.ErrLeaseLost) {
			t.Fatalf("对 completed 任务 %s 应返回 ErrLeaseLost,实际 %v", c.op, c.err)
		}
	}
	// completed 任务 Cancel → ErrAlreadyFinal(终态无出边)。
	if err := h.b.Cancel(ctx, "fin"); !errors.Is(err, taskgate.ErrAlreadyFinal) {
		t.Fatalf("Cancel(completed) 应返回 ErrAlreadyFinal,实际 %v", err)
	}
	// 状态必须原封不动。
	h.mustStatus(t, "fin", taskgate.StatusCompleted)

	// pending 任务(从未认领,没有租约):令牌操作全应报 ErrLeaseLost。
	h.enqueue(t, task("idle", "llm", "q2"))
	idleOps := []struct {
		op  string
		err error
	}{
		{"Ack", h.b.Ack(ctx, "idle", "no-such-token", nil)},
		{"Fail", h.b.Fail(ctx, "idle", "no-such-token", "x", taskgate.FailBusiness, h.now())},
		{"Requeue", h.b.Requeue(ctx, "idle", "no-such-token")},
		{"FinishCanceled", h.b.FinishCanceled(ctx, "idle", "no-such-token")},
		{"Heartbeat", h.b.Heartbeat(ctx, "idle", "no-such-token")},
	}
	for _, c := range idleOps {
		if !errors.Is(c.err, taskgate.ErrLeaseLost) {
			t.Fatalf("对 pending(未认领)任务 %s 应返回 ErrLeaseLost,实际 %v", c.op, c.err)
		}
	}
	h.mustStatus(t, "idle", taskgate.StatusPending)

	// 不存在的任务:统一 ErrTaskNotFound。
	if _, err := h.b.Get(ctx, "ghost"); !errors.Is(err, taskgate.ErrTaskNotFound) {
		t.Fatalf("Get(不存在) 应返回 ErrTaskNotFound,实际 %v", err)
	}
	if err := h.b.Cancel(ctx, "ghost"); !errors.Is(err, taskgate.ErrTaskNotFound) {
		t.Fatalf("Cancel(不存在) 应返回 ErrTaskNotFound,实际 %v", err)
	}
	if err := h.b.Ack(ctx, "ghost", "tok", nil); !errors.Is(err, taskgate.ErrTaskNotFound) {
		t.Fatalf("Ack(不存在) 应返回 ErrTaskNotFound,实际 %v", err)
	}
}
