package redisbroker_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/brokertest"
	"github.com/AmbroseX/taskgate/internal/fakeclock"
	"github.com/AmbroseX/taskgate/redisbroker"
	"github.com/alicebob/miniredis/v2"
	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"
)

// TestBrokerContract 一行接入统一契约套件:redis 后端必须过全部 18 条契约。
// 每条用例一个独立的 miniredis 实例,互不串数据;时间全靠套件注入的 fakeclock
// (脚本内时间由 ARGV 传入,miniredis 自身的时钟不参与)。
func TestBrokerContract(t *testing.T) {
	brokertest.Run(t, func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker {
		mr := miniredis.RunT(t)
		b, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
		if err != nil {
			t.Fatalf("redisbroker New 失败: %v", err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("redisbroker Init 失败: %v", err)
		}
		return b
	})
}

// TestBrokerContractRealRedis 真 Redis 档:设 TASKGATE_REDIS_ADDR 才跑
// (如 TASKGATE_REDIS_ADDR=127.0.0.1:6379),同一套 18 条契约。
// 每条用例用随机 KeyPrefix 隔离,测后 SCAN+DEL 清理,不污染共用实例。
func TestBrokerContractRealRedis(t *testing.T) {
	addr := os.Getenv("TASKGATE_REDIS_ADDR")
	if addr == "" {
		t.Skip("未设置 TASKGATE_REDIS_ADDR,跳过真 Redis 契约档")
	}
	brokertest.Run(t, func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker {
		prefix := "tgtest:" + ulid.Make().String() + ":"
		b, err := redisbroker.New(redisbroker.Options{Addr: addr, KeyPrefix: prefix})
		if err != nil {
			t.Fatalf("redisbroker New(%s) 失败: %v", addr, err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("redisbroker Init 失败: %v", err)
		}
		t.Cleanup(func() { cleanupPrefix(t, addr, prefix) })
		return b
	})
}

// cleanupPrefix 删掉本用例前缀下的所有键(broker 自身可能已 Close,单开一个连接)。
// 除了 prefix* 还要扫 "rate:"+prefix+"*":redis_rate 的 GCRA 键自带 "rate:" 前缀
// (形如 rate:<prefix><queue>),不在 KeyPrefix 命名空间内,漏扫会残留在共用实例上。
func cleanupPrefix(t *testing.T, addr, prefix string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, pattern := range []string{prefix + "*", "rate:" + prefix + "*"} {
		var cursor uint64
		for {
			keys, next, err := rdb.Scan(ctx, cursor, pattern, 500).Result()
			if err != nil {
				t.Logf("清理 %s 失败(不影响断言): %v", pattern, err)
				break
			}
			if len(keys) > 0 {
				if err := rdb.Del(ctx, keys...).Err(); err != nil {
					t.Logf("清理 %s 失败(不影响断言): %v", pattern, err)
					break
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
}

// TestEnqueueRejectsControlCharID 自定义 ID 含 ASCII 控制字符必须在 Go 侧拒收:
// \x1F 是 hash 里 parents 字段的拼接分隔符,混进 ID 会破坏 reap.lua 的解析;
// 其余控制字符一并挡掉(0x00~0x1F 与 0x7F)。合法 ID 不受影响。
func TestEnqueueRejectsControlCharID(t *testing.T) {
	mr := miniredis.RunT(t)
	b, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Init(taskgate.BrokerOptions{}); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	ctx := context.Background()
	for _, id := range []string{"a\x1fb", "a\x00b", "\tab", "a\x7f"} {
		err := b.Enqueue(ctx, &taskgate.Task{ID: id, Type: "x", Queue: "q"})
		if err == nil {
			t.Fatalf("含控制字符的 ID %q 应被拒收,实际入队成功", id)
		}
		if _, gerr := b.Get(ctx, id); gerr == nil {
			t.Fatalf("被拒收的 ID %q 不应落库", id)
		}
	}
	// 合法的自定义 ID(可见 ASCII、带空格/中文都行)照常入队。
	ok := &taskgate.Task{ID: "自定义 id-1", Type: "x", Queue: "q"}
	if err := b.Enqueue(ctx, ok); err != nil {
		t.Fatalf("合法自定义 ID 不应被拒: %v", err)
	}
}

// TestUseBeforeInit 未 Init 就调用任何方法必须返回错误,而不是 nil 指针 panic
// (与 memory/sqlite 两后端的同名测试对齐)。
func TestUseBeforeInit(t *testing.T) {
	mr := miniredis.RunT(t)
	b, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
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
	if _, err := b.Get(ctx, "id"); err == nil {
		t.Fatal("未 Init 调 Get 应返回错误,实际 nil")
	}
	if _, err := b.Dequeue(ctx, []string{"q"}); err == nil {
		t.Fatal("未 Init 调 Dequeue 应返回错误,实际 nil")
	}
}

// TestQuotaCapability 周期配额能力套件(spec 006):miniredis 介质,
// 介质时间用 SetTime 控制(脚本内 TIME 的回音),与 fakeclock 同步推进,不真 sleep。
func TestQuotaCapability(t *testing.T) {
	brokertest.RunQuota(t, func(t *testing.T, opts taskgate.BrokerOptions) (taskgate.Broker, func(time.Duration)) {
		mr := miniredis.RunT(t)
		b, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
		if err != nil {
			t.Fatalf("redisbroker New 失败: %v", err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("redisbroker Init 失败: %v", err)
		}
		clk := opts.Clock.(*fakeclock.Clock)
		mr.SetTime(clk.Now())
		return b, func(d time.Duration) {
			clk.Advance(d)
			mr.SetTime(clk.Now())
		}
	})
}
