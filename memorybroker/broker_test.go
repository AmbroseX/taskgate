package memorybroker_test

import (
	"context"
	"testing"
	"time"

	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/brokertest"
	"github.com/ambrose/taskgate/memorybroker"
)

// TestBrokerContract 一行接入统一契约套件:memory 后端必须过全部 18 条契约。
func TestBrokerContract(t *testing.T) {
	brokertest.Run(t, func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker {
		b := memorybroker.New()
		if err := b.Init(opts); err != nil {
			t.Fatalf("memorybroker Init 失败: %v", err)
		}
		return b
	})
}

// TestUseBeforeInit 未 Init 就调用任何方法必须返回错误,而不是 nil 指针 panic。
func TestUseBeforeInit(t *testing.T) {
	b := memorybroker.New()
	ctx := context.Background()
	checks := map[string]error{
		"Enqueue":        b.Enqueue(ctx, &taskgate.Task{Type: "x", Queue: "q"}),
		"Ack":            b.Ack(ctx, "id", "tok", nil),
		"Fail":           b.Fail(ctx, "id", "tok", "x", taskgate.FailBusiness, time.Time{}),
		"Cancel":         b.Cancel(ctx, "id"),
		"FinishCanceled": b.FinishCanceled(ctx, "id", "tok"),
		"Requeue":        b.Requeue(ctx, "id", "tok"),
		"Heartbeat":      b.Heartbeat(ctx, "id", "tok"),
	}
	for op, err := range checks {
		if err == nil {
			t.Fatalf("未 Init 调 %s 应返回错误,实际 nil", op)
		}
	}
	if _, err := b.ReapExpired(ctx); err == nil {
		t.Fatal("未 Init 调 ReapExpired 应返回错误,实际 nil")
	}
}
