package mysqlbroker_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/brokertest"
	"github.com/AmbroseX/taskgate/mysqlbroker"
	"github.com/oklog/ulid/v2"
)

// TestBrokerContractMySQL 真 MySQL 档:设 TASKGATE_MYSQL_DSN 才跑
// (如 TASKGATE_MYSQL_DSN="root:pass@tcp(localhost:3306)/taskgate"),同一套 18 条契约。
// 每条用例随机表前缀隔离,测后 DROP 清理。本地无 DSN 时 skip——回归唯一防线是 CI(宪法 V.5)。
// 要求 MySQL 8.0+(FOR UPDATE SKIP LOCKED)。
func TestBrokerContractMySQL(t *testing.T) {
	dsn := os.Getenv("TASKGATE_MYSQL_DSN")
	if dsn == "" {
		t.Skip("未设置 TASKGATE_MYSQL_DSN,跳过真 MySQL 契约档")
	}
	brokertest.Run(t, func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker {
		prefix := "tgtest_" + strings.ToLower(ulid.Make().String()) + "_"
		b, err := mysqlbroker.Open(dsn, mysqlbroker.WithTablePrefix(prefix))
		if err != nil {
			t.Fatalf("mysqlbroker Open 失败: %v", err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("mysqlbroker Init 失败: %v", err)
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
	db, err := sql.Open("mysql", dsn)
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

// TestEnqueueRejectsOversizeID id/type/queue 超过 255 字符必须在 Go 侧拒收(VARCHAR(255) 上限),
// 不落库;255 字符正好放行。这是 MySQL 独有的击穿点防护(FR-015)。离线可跑(校验在碰库前短路)。
func TestEnqueueRejectsOversizeID(t *testing.T) {
	b, err := mysqlbroker.Open("root@tcp(localhost:3306)/none")
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Init(taskgate.BrokerOptions{}); err != nil {
		// Init 建表需要真库;无库时 Init 会失败,这里只测校验短路,不依赖库。
		t.Logf("Init 失败(无库,预期):%v;继续只测入口校验", err)
	}
	ctx := context.Background()
	over := strings.Repeat("a", 256)
	cases := []taskgate.Task{
		{ID: over, Type: "x", Queue: "q"},
		{Type: over, Queue: "q"},
		{Type: "x", Queue: over},
	}
	for i, tk := range cases {
		task := tk
		if err := b.Enqueue(ctx, &task); err == nil {
			t.Fatalf("用例 %d:超 255 字符的字段应被拒收,实际未报错", i)
		}
	}
	// 255 字符正好合法:入口校验应放行(能不能真正落库取决于有没有库,这里只验证不被长度校验拦下)。
	ok := strings.Repeat("a", 255)
	task := taskgate.Task{ID: ok, Type: "x", Queue: "q"}
	if err := b.Enqueue(ctx, &task); err != nil && strings.Contains(err.Error(), "too long") {
		t.Fatalf("255 字符不应被长度校验拦下: %v", err)
	}
}

// TestUseBeforeInit 未 Init 就调用受 requireInit 保护的方法必须返回错误(与其它后端对齐)。
// 离线可跑:这些方法碰库前就短路。
func TestUseBeforeInit(t *testing.T) {
	b, err := mysqlbroker.Open("root@tcp(localhost:3306)/none")
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
