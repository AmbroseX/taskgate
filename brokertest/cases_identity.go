package brokertest

// 契约 19~22:Identity 身份模型(005)——Replay 基本语义、链不分叉、按键查询、并发竞态。
// 行为合同见 specs/005-identity-replay/contracts/broker-contract-delta.md。

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
)

// taskWithKey 造一个带 BusinessKey 的最小任务(id 留空 = broker 生成 ulid)。
func taskWithKey(id, typ, queue, key string) *taskgate.Task {
	tk := task(id, typ, queue)
	tk.BusinessKey = key
	return tk
}

// failNow 认领并以指定 kind 失败(retryAt 取当前时刻,FailBusiness 时立即可重新认领)。
func (h *harness) failNow(t *testing.T, queue string, kind taskgate.FailKind) *taskgate.Task {
	t.Helper()
	claimed := h.dequeue(t, queue)
	if err := h.b.Fail(context.Background(), claimed.ID, claimed.LeaseToken, "boom", kind, h.now()); err != nil {
		t.Fatalf("Fail(%s) 意外失败: %v", claimed.ID, err)
	}
	return claimed
}

// replay Replay 并断言成功。
func (h *harness) replay(t *testing.T, req taskgate.ReplayRequest) *taskgate.Task {
	t.Helper()
	nt, err := h.b.Replay(context.Background(), req)
	if err != nil {
		t.Fatalf("Replay(%+v) 意外失败: %v", req, err)
	}
	if nt == nil || nt.ID == "" {
		t.Fatalf("Replay 成功必须返回新执行的完整快照,实际 %+v", nt)
	}
	return nt
}

// 契约 19 ReplayBasic:终态链尾可重放——新执行字段合同、目标逐字段不可变、依赖溯源不漂移。
func caseReplayBasic(t *testing.T, h *harness) {
	ctx := context.Background()

	// E1(带键)跑到 completed,结果 r1。
	e1 := taskWithKey("", "report", "q", "job-1")
	e1.Payload = []byte(`{"day":"2026-07-16"}`)
	e1.MaxRetry = 2
	h.enqueue(t, e1)
	claimed := h.dequeue(t, "q")
	r1 := []byte(`{"ok":1}`)
	if err := h.b.Ack(ctx, claimed.ID, claimed.LeaseToken, r1); err != nil {
		t.Fatalf("Ack 意外失败: %v", err)
	}
	h.mustStatus(t, e1.ID, taskgate.StatusCompleted)

	// 子任务 C 引用 E1(父已终态 → 直接 pending)。
	c := task("", "child", "qc")
	c.DependsOn = []string{e1.ID}
	h.enqueue(t, c)

	before := h.get(t, e1.ID)

	// completed 不带显式允许 → 拒绝。
	if _, err := h.b.Replay(ctx, taskgate.ReplayRequest{ExecutionID: e1.ID}); !errors.Is(err, taskgate.ErrCompletedNotAllowed) {
		t.Fatalf("重放 completed 不带 AllowCompleted 应返回 ErrCompletedNotAllowed,实际 %v", err)
	}

	// 显式允许 → 新执行 E2:字段合同逐条验。
	h.advance(time.Second)
	e2 := h.replay(t, taskgate.ReplayRequest{ExecutionID: e1.ID, AllowCompleted: true})
	if e2.ID == e1.ID {
		t.Fatal("Replay 必须生成新 ExecutionID,不得复用目标 ID")
	}
	if e2.ReplayOf != e1.ID || e2.BusinessKey != "job-1" {
		t.Fatalf("新执行的链字段不对: ReplayOf=%q(期望 %s) BusinessKey=%q", e2.ReplayOf, e1.ID, e2.BusinessKey)
	}
	if e2.Type != "report" || e2.Queue != "q" || e2.MaxRetry != 2 {
		t.Fatalf("新执行应沿用目标的 Type/Queue/MaxRetry,实际 %q/%q/%d", e2.Type, e2.Queue, e2.MaxRetry)
	}
	if !bytes.Equal(e2.Payload, e1.Payload) {
		t.Fatalf("不带 Payload 的 Replay 应复制目标 Payload,实际 %s", e2.Payload)
	}
	if e2.Attempts != 0 || e2.LeaseLost != 0 || e2.Throttled != 0 {
		t.Fatalf("新执行三计数必须清零,实际 %d/%d/%d", e2.Attempts, e2.LeaseLost, e2.Throttled)
	}
	if len(e2.DependsOn) != 0 || e2.Status != taskgate.StatusPending {
		t.Fatalf("新执行应无依赖且为 pending,实际 deps=%v status=%s", e2.DependsOn, e2.Status)
	}
	if e2.Result != nil && len(e2.Result) != 0 {
		t.Fatalf("新执行不得继承目标的 Result,实际 %s", e2.Result)
	}
	// 快照与落库一致(ReplayOf/BusinessKey 往返)。
	got2 := h.get(t, e2.ID)
	if got2.ReplayOf != e1.ID || got2.BusinessKey != "job-1" || got2.Status != taskgate.StatusPending {
		t.Fatalf("新执行落库往返不一致: %+v", got2)
	}

	// 目标 E1 逐字段不可变。
	after := h.get(t, e1.ID)
	if after.Status != before.Status || !bytes.Equal(after.Result, before.Result) ||
		after.LastError != before.LastError || after.Attempts != before.Attempts ||
		after.LeaseLost != before.LeaseLost || after.Throttled != before.Throttled ||
		!after.CreatedAt.Equal(before.CreatedAt) || !after.StartedAt.Equal(before.StartedAt) ||
		!after.FinishedAt.Equal(before.FinishedAt) || !bytes.Equal(after.Payload, before.Payload) ||
		after.ReplayOf != "" || after.BusinessKey != before.BusinessKey {
		t.Fatalf("Replay 后目标执行被改写:\nbefore=%+v\nafter=%+v", before, after)
	}

	// 依赖溯源:C 视角永远是 E1 与 r1;新提交的子任务引用 E1 也一样。
	cGot := h.get(t, c.ID)
	parent := h.get(t, cGot.DependsOn[0])
	if parent.ID != e1.ID || !bytes.Equal(parent.Result, r1) {
		t.Fatalf("C.DependsOn[0] 视角漂移: 拿到 %s(result=%s),期望 %s(result=%s)", parent.ID, parent.Result, e1.ID, r1)
	}
	c2 := task("", "child", "qc")
	c2.DependsOn = []string{e1.ID}
	h.enqueue(t, c2)
	if p2 := h.get(t, h.get(t, c2.ID).DependsOn[0]); p2.ID != e1.ID || !bytes.Equal(p2.Result, r1) {
		t.Fatalf("E2 存在后新子任务引用 E1 看到的应仍是 r1,实际 %s", p2.Result)
	}

	// E2 可正常认领执行;把它打到 failed(先业务失败一次再跳过重试,验证计数非零)。
	claimed2 := h.dequeue(t, "q")
	if claimed2.ID != e2.ID {
		t.Fatalf("队列里应只有新执行 E2 可认领,实际认领到 %s", claimed2.ID)
	}
	if err := h.b.Fail(ctx, claimed2.ID, claimed2.LeaseToken, "biz fail", taskgate.FailBusiness, h.now()); err != nil {
		t.Fatalf("Fail(FailBusiness) 意外失败: %v", err)
	}
	h.mustStatus(t, e2.ID, taskgate.StatusRetrying)
	h.failNow(t, "q", taskgate.FailSkip)
	tail := h.mustStatus(t, e2.ID, taskgate.StatusFailed)
	if tail.Attempts == 0 {
		t.Fatal("测试前提不成立:E2 应带非零 Attempts 进入 failed")
	}

	// failed 无需显式允许;WithPayload 覆盖生效;计数再次清零。
	h.advance(time.Second)
	override := []byte(`{"fix":true}`)
	e3 := h.replay(t, taskgate.ReplayRequest{ExecutionID: e2.ID, Payload: override})
	if !bytes.Equal(e3.Payload, override) {
		t.Fatalf("带 Payload 的 Replay 应用覆盖值,实际 %s", e3.Payload)
	}
	if e3.ReplayOf != e2.ID || e3.Attempts != 0 || e3.LastError != "" {
		t.Fatalf("E3 链字段/计数不对: ReplayOf=%q Attempts=%d LastError=%q", e3.ReplayOf, e3.Attempts, e3.LastError)
	}
}

// 契约 20 ReplayChain:链不分叉——已重放目标拒绝、在途链尾拒绝、按键打链尾、无键链同样成立。
func caseReplayChain(t *testing.T, h *harness) {
	ctx := context.Background()

	// 键链:E1 failed → Replay 得 E2。
	e1 := taskWithKey("", "t", "q", "k1")
	h.enqueue(t, e1)
	h.failNow(t, "q", taskgate.FailSkip)
	h.advance(time.Second)
	e2 := h.replay(t, taskgate.ReplayRequest{ExecutionID: e1.ID})

	// 对 E1 再重放 → 链不分叉。
	if _, err := h.b.Replay(ctx, taskgate.ReplayRequest{ExecutionID: e1.ID}); !errors.Is(err, taskgate.ErrAlreadyReplayed) {
		t.Fatalf("对已重放的 E1 再 Replay 应返回 ErrAlreadyReplayed,实际 %v", err)
	}
	// 在途链尾(E2 pending)→ 未终态拒绝;按键指定同样打在 E2 上。
	if _, err := h.b.Replay(ctx, taskgate.ReplayRequest{ExecutionID: e2.ID}); !errors.Is(err, taskgate.ErrReplayNotFinal) {
		t.Fatalf("重放在途链尾应返回 ErrReplayNotFinal,实际 %v", err)
	}
	if _, err := h.b.Replay(ctx, taskgate.ReplayRequest{BusinessKey: "k1"}); !errors.Is(err, taskgate.ErrReplayNotFinal) {
		t.Fatalf("按键重放(链尾在途)应返回 ErrReplayNotFinal,实际 %v", err)
	}

	// E2 failed 后按键重放:天然作用于链尾——E3.ReplayOf 必须是 E2 不是 E1。
	h.failNow(t, "q", taskgate.FailSkip)
	h.advance(time.Second)
	e3 := h.replay(t, taskgate.ReplayRequest{BusinessKey: "k1"})
	if e3.ReplayOf != e2.ID {
		t.Fatalf("按键 Replay 应打在链尾 E2 上,实际 ReplayOf=%q(E1=%s E2=%s)", e3.ReplayOf, e1.ID, e2.ID)
	}

	// canceled 终态无需显式允许。
	if err := h.b.Cancel(ctx, e3.ID); err != nil {
		t.Fatalf("Cancel(E3) 意外失败: %v", err)
	}
	h.mustStatus(t, e3.ID, taskgate.StatusCanceled)
	h.advance(time.Second)
	e4 := h.replay(t, taskgate.ReplayRequest{ExecutionID: e3.ID})
	if e4.ReplayOf != e3.ID {
		t.Fatalf("重放 canceled 执行应成功且指回 E3,实际 ReplayOf=%q", e4.ReplayOf)
	}

	// 无键执行同样可重放,链不分叉不变式同样生效。
	nk := task("", "t", "q2")
	h.enqueue(t, nk)
	h.failNow(t, "q2", taskgate.FailSkip)
	h.advance(time.Second)
	nk2 := h.replay(t, taskgate.ReplayRequest{ExecutionID: nk.ID})
	if nk2.BusinessKey != "" || nk2.ReplayOf != nk.ID {
		t.Fatalf("无键重放的新执行应无键且指回目标,实际 key=%q replayOf=%q", nk2.BusinessKey, nk2.ReplayOf)
	}
	if _, err := h.b.Replay(ctx, taskgate.ReplayRequest{ExecutionID: nk.ID}); !errors.Is(err, taskgate.ErrAlreadyReplayed) {
		t.Fatalf("无键链的链不分叉同样生效,实际 %v", err)
	}

	// 目标不存在与非法入参。
	if _, err := h.b.Replay(ctx, taskgate.ReplayRequest{ExecutionID: "no-such"}); !errors.Is(err, taskgate.ErrTaskNotFound) {
		t.Fatalf("重放不存在的执行应返回 ErrTaskNotFound,实际 %v", err)
	}
	if _, err := h.b.Replay(ctx, taskgate.ReplayRequest{BusinessKey: "no-such-key"}); !errors.Is(err, taskgate.ErrTaskNotFound) {
		t.Fatalf("重放不存在的键应返回 ErrTaskNotFound,实际 %v", err)
	}
	if _, err := h.b.Replay(ctx, taskgate.ReplayRequest{}); err == nil {
		t.Fatal("ExecutionID 与 BusinessKey 都为空必须报错")
	}
	if _, err := h.b.Replay(ctx, taskgate.ReplayRequest{ExecutionID: e1.ID, BusinessKey: "k1"}); err == nil {
		t.Fatal("ExecutionID 与 BusinessKey 同时非空必须报错")
	}
}

// 契约 21 BusinessKeyQuery:Filter.BusinessKey 过滤准确、链序稳定、与其余条件 AND、分页照旧。
func caseBusinessKeyQuery(t *testing.T, h *harness) {
	ctx := context.Background()

	// 键 k 的链:E1(failed) ← E2(failed) ← E3(pending)。
	e1 := taskWithKey("", "t", "q", "k")
	h.enqueue(t, e1)
	h.failNow(t, "q", taskgate.FailSkip)
	h.advance(time.Second)
	e2 := h.replay(t, taskgate.ReplayRequest{BusinessKey: "k"})
	h.failNow(t, "q", taskgate.FailSkip)
	h.advance(time.Second)
	e3 := h.replay(t, taskgate.ReplayRequest{BusinessKey: "k"})

	// 干扰项:另一个键 + 无键任务。
	h.enqueue(t, taskWithKey("", "t", "q", "other"))
	h.enqueue(t, task("", "t", "q"))

	list, err := h.b.List(ctx, taskgate.Filter{BusinessKey: "k"})
	if err != nil {
		t.Fatalf("List(BusinessKey) 意外失败: %v", err)
	}
	if len(list) != 3 || list[0].ID != e1.ID || list[1].ID != e2.ID || list[2].ID != e3.ID {
		ids := make([]string, len(list))
		for i, tk := range list {
			ids[i] = tk.ID
		}
		t.Fatalf("按键查询应返回链序 [%s %s %s],实际 %v", e1.ID, e2.ID, e3.ID, ids)
	}

	// 与 Status 是 AND。
	failedOnly, err := h.b.List(ctx, taskgate.Filter{BusinessKey: "k", Status: taskgate.StatusFailed})
	if err != nil || len(failedOnly) != 2 {
		t.Fatalf("BusinessKey+Status 应是 AND 关系(期望 2 条 failed),实际 n=%d err=%v", len(failedOnly), err)
	}

	// 分页合同不变。
	page, err := h.b.List(ctx, taskgate.Filter{BusinessKey: "k", Offset: 1, Limit: 1})
	if err != nil || len(page) != 1 || page[0].ID != e2.ID {
		t.Fatalf("按键分页(Offset=1,Limit=1)应返回 [E2],实际 %+v err=%v", page, err)
	}

	// 不存在的键 → 空列表,nil error。
	empty, err := h.b.List(ctx, taskgate.Filter{BusinessKey: "nope"})
	if err != nil || len(empty) != 0 {
		t.Fatalf("不存在的键应返回空列表(nil error),实际 n=%d err=%v", len(empty), err)
	}
}

// 契约 22 IdentityRace:同键并发入队恰 1 成功;同目标并发重放恰 1 成功。
func caseIdentityRace(t *testing.T, h *harness) {
	ctx := context.Background()
	const n = 50

	// (a) 同键 n 并发 Enqueue。
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- h.b.Enqueue(ctx, taskWithKey("", "t", "q", "race-key"))
		}()
	}
	wg.Wait()
	close(errs)
	okN := 0
	for err := range errs {
		switch {
		case err == nil:
			okN++
		case errors.Is(err, taskgate.ErrTaskExists):
		default:
			t.Fatalf("并发同键入队的失败方必须拿 ErrTaskExists,实际 %v", err)
		}
	}
	if okN != 1 {
		t.Fatalf("同键 %d 并发入队应恰好 1 个成功,实际 %d 个", n, okN)
	}
	chain, err := h.b.List(ctx, taskgate.Filter{BusinessKey: "race-key"})
	if err != nil || len(chain) != 1 {
		t.Fatalf("竞态后键下应恰好 1 个执行,实际 n=%d err=%v", len(chain), err)
	}

	// (b) 把它打到 failed,同目标 n 并发 Replay。
	target := chain[0].ID
	h.failNow(t, "q", taskgate.FailSkip)
	rerrs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.b.Replay(ctx, taskgate.ReplayRequest{ExecutionID: target})
			rerrs <- err
		}()
	}
	wg.Wait()
	close(rerrs)
	okR := 0
	for err := range rerrs {
		switch {
		case err == nil:
			okR++
		case errors.Is(err, taskgate.ErrAlreadyReplayed):
		default:
			t.Fatalf("并发同目标 Replay 的失败方必须拿 ErrAlreadyReplayed,实际 %v", err)
		}
	}
	if okR != 1 {
		t.Fatalf("同目标 %d 并发 Replay 应恰好 1 个成功,实际 %d 个", n, okR)
	}
	chain, err = h.b.List(ctx, taskgate.Filter{BusinessKey: "race-key"})
	if err != nil || len(chain) != 2 {
		t.Fatalf("竞态后链长应为 2(不分叉),实际 n=%d err=%v", len(chain), err)
	}
}
