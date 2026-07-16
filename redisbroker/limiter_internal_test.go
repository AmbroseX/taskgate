// limiter_internal_test.go 包内白盒测试:观测 semSlot 的 done 通道,
// 验证 Broker.Close 会统一停掉已发限流器的续期 goroutine(黑盒测不到内部通道)。
package redisbroker

import (
	"context"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/alicebob/miniredis/v2"
)

// TestCloseStopsRenewGoroutines 占槽后不 ReleaseSlot、直接 Broker.Close:
// 所有槽的续期 goroutine 必须退出(done 通道关闭),不许留下空转泄漏;
// Redis 里的槽不被 Close 删除,靠 TTL 自然过期(与进程崩溃同一条自愈路径)。
func TestCloseStopsRenewGoroutines(t *testing.T) {
	mr := miniredis.RunT(t)
	b, err := New(Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	if err := b.Init(taskgate.BrokerOptions{}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	lim, err := b.QueueLimiter("q", taskgate.QueueConfig{Workers: 2})
	if err != nil {
		t.Fatalf("QueueLimiter 失败: %v", err)
	}
	ql := lim.(*queueLimiter)

	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		if err := lim.AcquireSlot(ctx); err != nil {
			t.Fatalf("第 %d 个槽占用失败: %v", i, err)
		}
	}

	// 先把两个槽的 done 通道抓在手里(Close 的收尾会清空 held)。
	ql.mu.Lock()
	dones := make([]chan struct{}, 0, len(ql.held))
	for _, s := range ql.held {
		dones = append(dones, s.done)
	}
	ql.mu.Unlock()
	if len(dones) != 2 {
		t.Fatalf("应持有 2 个槽,实际 %d", len(dones))
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	for i, done := range dones {
		select {
		case <-done:
			// 续期 goroutine 已退出
		case <-time.After(5 * time.Second):
			t.Fatalf("Close 后第 %d 个槽的续期 goroutine 未退出(泄漏)", i+1)
		}
	}

	// 重复 Close 幂等,不 panic 不报错。
	if err := b.Close(); err != nil {
		t.Fatalf("重复 Close 应为无害 no-op: %v", err)
	}
}
