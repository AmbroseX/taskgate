package redisbroker_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/brokertest"
	"github.com/ambrose/taskgate/redisbroker"
	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"
)

// TestBrokerContract 一行接入统一契约套件:redis 后端必须过全部 17 条契约。
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
// (如 TASKGATE_REDIS_ADDR=127.0.0.1:6379),同一套 17 条契约。
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
func cleanupPrefix(t *testing.T, addr, prefix string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, prefix+"*", 500).Result()
		if err != nil {
			t.Logf("清理前缀 %s 失败(不影响断言): %v", prefix, err)
			return
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Logf("清理前缀 %s 失败(不影响断言): %v", prefix, err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
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
