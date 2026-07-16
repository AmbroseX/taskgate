package pgbroker_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/brokertest"
	"github.com/AmbroseX/taskgate/pgbroker"
	"github.com/oklog/ulid/v2"
)

// TestBrokerContractPG 真 PG 档:设 TASKGATE_PG_DSN 才跑
// (如 TASKGATE_PG_DSN="postgres://postgres:pass@localhost:5432/postgres?sslmode=disable"),
// 同一套 18 条契约。每条用例用随机表前缀隔离,测后 DROP 清理,不污染共用库。
// 本地无 DSN 时 skip——PG 后端本地零覆盖,回归唯一防线是 CI(宪法 V.5)。
func TestBrokerContractPG(t *testing.T) {
	dsn := os.Getenv("TASKGATE_PG_DSN")
	if dsn == "" {
		t.Skip("未设置 TASKGATE_PG_DSN,跳过真 PG 契约档")
	}
	brokertest.Run(t, func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker {
		prefix := "tgtest_" + strings.ToLower(ulid.Make().String()) + "_"
		b, err := pgbroker.Open(dsn, pgbroker.WithTablePrefix(prefix))
		if err != nil {
			t.Fatalf("pgbroker Open 失败: %v", err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("pgbroker Init 失败: %v", err)
		}
		t.Cleanup(func() {
			_ = b.Close()
			dropTables(t, dsn, prefix)
		})
		return b
	})
}

// dropTables 删掉本用例前缀下的两张表(broker 可能已 Close,单开一个连接)。
func dropTables(t *testing.T, dsn, prefix string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Logf("清理 open 失败(不影响断言): %v", err)
		return
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, tbl := range []string{prefix + "task_deps", prefix + "tasks"} {
		if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS `+tbl); err != nil {
			t.Logf("清理 %s 失败(不影响断言): %v", tbl, err)
		}
	}
}

// TestUseBeforeInit 未 Init 就调用受 requireInit 保护的方法必须返回错误,而不是 nil 指针 panic
// (与 memory/sqlite/redis 后端对齐)。这些方法在碰数据库之前就短路,故无需真库、离线可跑。
func TestUseBeforeInit(t *testing.T) {
	b, err := pgbroker.Open("postgres://localhost:5432/none?sslmode=disable")
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
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
	if _, err := b.Dequeue(ctx, []string{"q"}); err == nil {
		t.Fatal("未 Init 调 Dequeue 应返回错误,实际 nil")
	}
}
