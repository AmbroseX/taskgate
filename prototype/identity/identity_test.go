// Identity 领域模型原型(裁决第 4 步之一):在 memorybroker 上跑真调度,
// 验证模型文档第 7 节的五条断言 + 三条规则性拒绝。
package identity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/memorybroker"
)

// proto 型 handler 每次执行产出递增的结果("r1"、"r2"…),
// 用来区分同一条链上先后两次执行的结果。
// child 型 handler 按自己的 DependsOn[0] 去 Get 父结果并记录——
// 模拟"子任务消费父结果",断言②靠它。
func newHarness(t *testing.T) (*taskgate.Gate, *Layer, *sync.Map, chan struct{}) {
	t.Helper()
	g, err := taskgate.New(taskgate.Config{
		Broker:       memorybroker.New(),
		DefaultQueue: taskgate.QueueConfig{Workers: 4},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var seq atomic.Int64
	g.Handle("proto", func(ctx context.Context, tk *taskgate.Task) ([]byte, error) {
		return json.Marshal(fmt.Sprintf("r%d", seq.Add(1)))
	})
	seen := &sync.Map{} // 子任务 ID → 它看到的父结果
	g.Handle("child", func(ctx context.Context, tk *taskgate.Task) ([]byte, error) {
		p, err := g.Get(ctx, tk.DependsOn[0])
		if err != nil {
			return nil, err
		}
		seen.Store(tk.ID, string(p.Result))
		return nil, nil
	})
	block := make(chan struct{}) // block 型 handler 卡在这,制造"在途 execution"
	g.Handle("block", func(ctx context.Context, tk *taskgate.Task) ([]byte, error) {
		select {
		case <-block:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	go func() { _ = g.Run(context.Background()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
	})
	return g, New(g), seen, block
}

func mustWait(t *testing.T, g *taskgate.Gate, id string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := g.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait(%s): %v", id, err)
	}
	return string(r)
}

// TestReplayModel 主线:E1(completed,r1) ← C(DependsOn=[E1],已消费 r1),
// Replay E1 得 E2(r2),逐条过五断言。
func TestReplayModel(t *testing.T) {
	g, l, seen, _ := newHarness(t)
	ctx := context.Background()
	const key = "ocr:doc-42"

	// E1 完成,结果 r1。
	e1, err := l.Submit(ctx, "proto", nil, key)
	if err != nil {
		t.Fatalf("Submit E1: %v", err)
	}
	if r := mustWait(t, g, e1); r != `"r1"` {
		t.Fatalf("E1 result = %s, want \"r1\"", r)
	}

	// 强幂等:同键二次 Submit 一律拒绝(评审确认 #1,契约 2 语义)。
	if _, err := l.Submit(ctx, "proto", nil, key); !errors.Is(err, taskgate.ErrTaskExists) {
		t.Fatalf("同键二次 Submit = %v, want ErrTaskExists", err)
	}

	// C 依赖 E1 并消费其结果。
	c, err := l.Submit(ctx, "child", nil, "", taskgate.DependsOn(e1))
	if err != nil {
		t.Fatalf("Submit C: %v", err)
	}
	mustWait(t, g, c)
	if v, _ := seen.Load(c); v != `"r1"` {
		t.Fatalf("C 消费到的父结果 = %v, want \"r1\"", v)
	}

	// 快照 E1 终态,Replay 后逐字段比对。
	before, err := g.Get(ctx, e1)
	if err != nil {
		t.Fatalf("Get E1: %v", err)
	}

	// completed 的重放必须显式允许(评审确认 #2)。
	if _, err := l.ReplayByKey(ctx, key, ReplayOptions{}); !errors.Is(err, ErrCompletedNotAllowed) {
		t.Fatalf("重放 completed 未带 AllowCompleted = %v, want ErrCompletedNotAllowed", err)
	}
	e2, err := l.ReplayByKey(ctx, key, ReplayOptions{AllowCompleted: true})
	if err != nil {
		t.Fatalf("Replay E1: %v", err)
	}
	if r := mustWait(t, g, e2); r != `"r2"` {
		t.Fatalf("E2 result = %s, want \"r2\"", r)
	}

	// 断言①:E1 的 Result、计数、错误原封不动。
	after, err := g.Get(ctx, e1)
	if err != nil {
		t.Fatalf("Get E1 after replay: %v", err)
	}
	if string(after.Result) != string(before.Result) ||
		after.Attempts != before.Attempts ||
		after.LastError != before.LastError ||
		after.Status != before.Status ||
		!after.FinishedAt.Equal(before.FinishedAt) {
		t.Fatalf("E1 被改写了:before=%+v after=%+v", before, after)
	}

	// 断言②:按 C.DependsOn[0] 查到的还是 E1 的 r1,不是 E2;
	// 新提交的子任务引用 E1 也一样。
	ct, err := g.Get(ctx, c)
	if err != nil {
		t.Fatalf("Get C: %v", err)
	}
	if ct.DependsOn[0] != e1 {
		t.Fatalf("C.DependsOn[0] = %s, want %s(不自动跟随 replay)", ct.DependsOn[0], e1)
	}
	c2, err := l.Submit(ctx, "child", nil, "", taskgate.DependsOn(e1))
	if err != nil {
		t.Fatalf("Submit C2: %v", err)
	}
	mustWait(t, g, c2)
	if v, _ := seen.Load(c2); v != `"r1"` {
		t.Fatalf("replay 后新子任务引用 E1 看到 %v, want \"r1\"", v)
	}

	// 断言③:E2 有 ReplayOf=E1。
	if src, ok := l.ReplayOf(e2); !ok || src != e1 {
		t.Fatalf("ReplayOf(E2) = (%s,%v), want (%s,true)", src, ok, e1)
	}

	// 断言④:键下历史链可枚举且有序。
	h := l.History(key)
	if len(h) != 2 || h[0] != e1 || h[1] != e2 {
		t.Fatalf("History = %v, want [%s %s]", h, e1, e2)
	}

	// 断言⑤:E2 存在后再对 E1 做 Replay 被拒绝(链不分叉)。
	if _, err := l.ReplayByID(ctx, e1, ReplayOptions{AllowCompleted: true}); !errors.Is(err, ErrAlreadyReplayed) {
		t.Fatalf("对非链尾 E1 重放 = %v, want ErrAlreadyReplayed", err)
	}

	// 补充:按键重放天然作用于链尾 E2,链继续延长为 [E1 E2 E3]。
	e3, err := l.ReplayByKey(ctx, key, ReplayOptions{AllowCompleted: true})
	if err != nil {
		t.Fatalf("Replay E2: %v", err)
	}
	mustWait(t, g, e3)
	if src, _ := l.ReplayOf(e3); src != e2 {
		t.Fatalf("ReplayOf(E3) = %s, want %s(按键重放要打在链尾)", src, e2)
	}
	if h := l.History(key); len(h) != 3 || h[2] != e3 {
		t.Fatalf("History = %v, want 长度 3 且链尾是 %s", h, e3)
	}
}

// TestReplayInFlightRejected 键下有在途 execution 时 Replay 被拒绝
// ("≤1 非终态"不变式;目标本身非终态,撞前置条件①)。
func TestReplayInFlightRejected(t *testing.T) {
	g, l, _, block := newHarness(t)
	ctx := context.Background()

	id, err := l.Submit(ctx, "block", nil, "k2")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	// 等它真的跑起来(非终态在途),再试 Replay。
	deadline := time.Now().Add(5 * time.Second)
	for {
		tk, err := g.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if tk.Status == taskgate.StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 running 超时,当前 %s", tk.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := l.ReplayByKey(ctx, "k2", ReplayOptions{}); !errors.Is(err, ErrNotFinal) {
		t.Fatalf("对在途执行重放 = %v, want ErrNotFinal", err)
	}
	close(block)
	mustWait(t, g, id)
}

// TestKeylessReplay 无 BusinessKey 的执行也能 Replay(模型问题 #4),
// 且链不分叉不变式同样生效——不依赖键。
func TestKeylessReplay(t *testing.T) {
	g, l, _, _ := newHarness(t)
	ctx := context.Background()

	e0, err := l.Submit(ctx, "proto", nil, "")
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	mustWait(t, g, e0)

	e1, err := l.ReplayByID(ctx, e0, ReplayOptions{AllowCompleted: true})
	if err != nil {
		t.Fatalf("无键 Replay: %v", err)
	}
	mustWait(t, g, e1)
	if src, _ := l.ReplayOf(e1); src != e0 {
		t.Fatalf("ReplayOf = %s, want %s", src, e0)
	}
	// 再对 e0 重放 → 拒绝:无键链同样不分叉。
	if _, err := l.ReplayByID(ctx, e0, ReplayOptions{AllowCompleted: true}); !errors.Is(err, ErrAlreadyReplayed) {
		t.Fatalf("无键链二次重放同一目标 = %v, want ErrAlreadyReplayed", err)
	}
}
