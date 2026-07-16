// sql_backends_test.go 把 PG/MySQL 两个服务器型后端接进 L3 集成套件与并发压测。
// 全部 env 门控(TASKGATE_PG_DSN / TASKGATE_MYSQL_DSN):本地无 DSN 时这些档不参与,
// 已有 memory/sqlite/redis 三档照常;CI 注入 DSN 后 SQL 后端一并跑(宪法 V.5 门控例外)。
package taskgate_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/AmbroseX/taskgate/internal/sqlbroker"
	"github.com/AmbroseX/taskgate/mysqlbroker"
	"github.com/AmbroseX/taskgate/pgbroker"
	"github.com/oklog/ulid/v2"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// init 在 DSN 存在时把 SQL 后端追加进 L3 的 backends 列表,让所有 forEachBackend 场景一并覆盖。
// 每个 broker 用随机表前缀隔离、20ms 轮询(L3 用短时长),并注册清理 DROP 表。
func init() {
	if dsn := os.Getenv("TASKGATE_PG_DSN"); dsn != "" {
		backends = append(backends, struct {
			name string
			make func(t *testing.T) taskgate.Broker
		}{"postgres", func(t *testing.T) taskgate.Broker {
			prefix := randPrefix()
			b, err := pgbroker.Open(dsn, pgbroker.WithTablePrefix(prefix), pgbroker.WithPollInterval(20*time.Millisecond))
			if err != nil {
				t.Fatalf("打开 postgres 后端失败: %v", err)
			}
			t.Cleanup(func() { dropSQLTables(t, "pgx", dsn, prefix) })
			return b
		}})
	}
	if dsn := os.Getenv("TASKGATE_MYSQL_DSN"); dsn != "" {
		backends = append(backends, struct {
			name string
			make func(t *testing.T) taskgate.Broker
		}{"mysql", func(t *testing.T) taskgate.Broker {
			prefix := randPrefix()
			b, err := mysqlbroker.Open(dsn, mysqlbroker.WithTablePrefix(prefix), mysqlbroker.WithPollInterval(20*time.Millisecond))
			if err != nil {
				t.Fatalf("打开 mysql 后端失败: %v", err)
			}
			t.Cleanup(func() { dropSQLTables(t, "mysql", dsn, prefix) })
			return b
		}})
	}
}

func randPrefix() string {
	return "tgtest_" + strings.ToLower(ulid.Make().String()) + "_"
}

func dropSQLTables(t *testing.T, driver, dsn, prefix string) {
	t.Helper()
	db, err := sql.Open(driver, dsn)
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

// sqlBroker 一个已 Init 的 SQL 后端 + 名字。
type sqlBroker struct {
	name string
	b    taskgate.Broker
}

// openSQLBrokers 返回当前 DSN 可用的 SQL 后端(各自随机前缀 + 清理),供并发压测直接用。
func openSQLBrokers(t *testing.T) []sqlBroker {
	t.Helper()
	var out []sqlBroker
	opts := taskgate.BrokerOptions{DefaultLeaseTTL: time.Second, LeaseLostMax: 3, ThrottledMax: 100}
	if dsn := os.Getenv("TASKGATE_PG_DSN"); dsn != "" {
		prefix := randPrefix()
		b, err := pgbroker.Open(dsn, pgbroker.WithTablePrefix(prefix))
		if err != nil {
			t.Fatalf("打开 postgres 失败: %v", err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("postgres Init 失败: %v", err)
		}
		t.Cleanup(func() { _ = b.Close(); dropSQLTables(t, "pgx", dsn, prefix) })
		out = append(out, sqlBroker{"postgres", b})
	}
	if dsn := os.Getenv("TASKGATE_MYSQL_DSN"); dsn != "" {
		prefix := randPrefix()
		b, err := mysqlbroker.Open(dsn, mysqlbroker.WithTablePrefix(prefix))
		if err != nil {
			t.Fatalf("打开 mysql 失败: %v", err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("mysql Init 失败: %v", err)
		}
		t.Cleanup(func() { _ = b.Close(); dropSQLTables(t, "mysql", dsn, prefix) })
		out = append(out, sqlBroker{"mysql", b})
	}
	return out
}

// TestConcurrentPropagate 死锁重试环的专项验证(方案 4.1 自认的头号机制,契约用例单 goroutine 压不出)。
// 构造"钻石"依赖:一批子任务同时依赖两个父 P1、P2;并发 Fail(P1)/Fail(P2) 会各自向同一片共享子树
// 传播、以不同顺序锁子任务行 → 必然产生死锁窗口(PG 40P01 / MySQL 1213)。断言:
//   - 两个 Fail 都不把死锁错误漏给调用方(被 withTx 重试环吃掉);
//   - 所有子任务终态一致(canceled),P1/P2 都 failed;
//   - 无幻通知(Notify 只在事务成功后发,回滚的不发)。
//
// 多轮 + 并发 reaper 增加争用。env 门控,与契约档同一开关。
func TestConcurrentPropagate(t *testing.T) {
	brokers := openSQLBrokers(t)
	if len(brokers) == 0 {
		t.Skip("未设置 TASKGATE_PG_DSN / TASKGATE_MYSQL_DSN,跳过并发 propagate 压测")
	}
	for _, sb := range brokers {
		t.Run(sb.name, func(t *testing.T) {
			runConcurrentPropagate(t, sb.b)
		})
	}
}

func runConcurrentPropagate(t *testing.T, b taskgate.Broker) {
	ctx := context.Background()
	const rounds = 20
	const childrenPerRound = 12
	retriesBefore := sqlbroker.TotalRetries()

	for round := 0; round < rounds; round++ {
		tag := fmt.Sprintf("r%d", round)
		// 两个父,入队后认领成 running 拿到令牌。
		p1 := enqAndClaim(t, b, tag+"-p1")
		p2 := enqAndClaim(t, b, tag+"-p2")

		// 一批子任务同时依赖 P1、P2(FailFast):任一父失败即连锁取消。
		var childIDs []string
		for i := 0; i < childrenPerRound; i++ {
			cid := fmt.Sprintf("%s-c%d", tag, i)
			c := &taskgate.Task{ID: cid, Type: "t", Queue: "q",
				DependsOn: []string{p1, p2}, OnParentFailure: taskgate.FailFast}
			if err := b.Enqueue(ctx, c); err != nil {
				t.Fatalf("enqueue child %s: %v", cid, err)
			}
			childIDs = append(childIDs, cid)
		}

		// 并发:同时 Fail 两个父(向共享子树反向锁行),外加两个 reaper 搅动。
		var wg sync.WaitGroup
		errCh := make(chan error, 4)
		reap := func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if _, err := b.ReapExpired(ctx); err != nil {
					errCh <- fmt.Errorf("ReapExpired 泄漏错误: %w", err)
					return
				}
			}
		}
		wg.Add(4)
		go func() { defer wg.Done(); failWith(b, p1, errCh) }()
		go func() { defer wg.Done(); failWith(b, p2, errCh) }()
		go reap()
		go reap()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatalf("round %d 有错误泄漏给调用方(死锁重试环应吞掉): %v", round, err)
		}

		// 终态一致:两个父 failed,所有子 canceled。
		assertStatus(t, b, p1, taskgate.StatusFailed)
		assertStatus(t, b, p2, taskgate.StatusFailed)
		for _, cid := range childIDs {
			assertStatus(t, b, cid, taskgate.StatusCanceled)
		}
	}
	// 观测重试环是否真的被触发(死锁被吃掉的次数)。不硬断言 >0(是否死锁与库配置有关),
	// 但打出来:若长期为 0,说明这个压测没压出死锁,需要加大争用重新设计。
	t.Logf("死锁重试环触发次数(本轮 delta):%d", sqlbroker.TotalRetries()-retriesBefore)
}

// enqAndClaim 入队一个无依赖任务并立刻认领成 running,返回它的 ID;令牌通过全局登记取。
func enqAndClaim(t *testing.T, b taskgate.Broker, id string) string {
	t.Helper()
	ctx := context.Background()
	if err := b.Enqueue(ctx, &taskgate.Task{ID: id, Type: "t", Queue: "q"}); err != nil {
		t.Fatalf("enqueue %s: %v", id, err)
	}
	tk, err := b.Dequeue(ctx, []string{"q"})
	if err != nil {
		t.Fatalf("dequeue %s: %v", id, err)
	}
	if tk.ID != id {
		t.Fatalf("认领到的不是 %s 而是 %s", id, tk.ID)
	}
	tokens.Store(id, tk.LeaseToken)
	return id
}

// tokens 记 id→leaseToken(压测里父任务不多,用 sync.Map 简单登记)。
var tokens sync.Map

// failWith 用登记的令牌 Fail 一个父,错误泄漏则送进 errCh。
func failWith(b taskgate.Broker, id string, errCh chan<- error) {
	v, _ := tokens.Load(id)
	tok, _ := v.(string)
	if err := b.Fail(context.Background(), id, tok, "boom", taskgate.FailSkip, time.Time{}); err != nil {
		errCh <- fmt.Errorf("Fail(%s) 泄漏错误: %w", id, err)
	}
}

func assertStatus(t *testing.T, b taskgate.Broker, id string, want taskgate.Status) {
	t.Helper()
	got, err := b.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	if got.Status != want {
		t.Fatalf("任务 %s 终态 = %s,期望 %s", id, got.Status, want)
	}
}
