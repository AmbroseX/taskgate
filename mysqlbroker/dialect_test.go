package mysqlbroker

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/AmbroseX/taskgate/internal/sqlbroker"
	"github.com/go-sql-driver/mysql"
)

// 这些是纯函数单测(离线、确定),覆盖错误分类等 happy-path 契约压不到的分支。

func TestMySQLRebindNoop(t *testing.T) {
	in := `SELECT ? FROM t WHERE a = ? AND b IN (?,?)`
	if got := (mysqlDialect{}).Rebind(in); got != in {
		t.Fatalf("MySQL Rebind 应原样返回,got %q", got)
	}
}

func TestMySQLIsDuplicateKey(t *testing.T) {
	d := mysqlDialect{}
	// 构造驱动错误类型,errno 1062 = 主键冲突。errors.As 应命中(而非字符串匹配)。
	dup := fmt.Errorf("wrap: %w", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry"})
	if !d.IsDuplicateKey(dup) {
		t.Fatal("errno 1062 应判为重复键")
	}
	if d.IsDuplicateKey(&mysql.MySQLError{Number: 1213}) {
		t.Fatal("errno 1213 不是重复键")
	}
	if d.IsDuplicateKey(errors.New("Duplicate entry '1' for key")) {
		t.Fatal("纯文案(非驱动错误类型)不应被误判为重复键")
	}
}

func TestMySQLRetryable(t *testing.T) {
	d := mysqlDialect{}
	cases := []struct {
		errno uint16
		want  sqlbroker.RetryClass
	}{
		{1213, sqlbroker.RetryImmediate}, // 死锁:立即重试
		{1205, sqlbroker.RetryLimited},   // 锁等待超时:有限重试
		{1062, sqlbroker.NotRetryable},   // 重复键:不重试
	}
	for _, c := range cases {
		got := d.Retryable(fmt.Errorf("x: %w", &mysql.MySQLError{Number: c.errno}))
		if got != c.want {
			t.Fatalf("errno %d:Retryable = %d,期望 %d", c.errno, got, c.want)
		}
	}
	if d.Retryable(errors.New("deadlock found")) != sqlbroker.NotRetryable {
		t.Fatal("纯文案不应被判为可重试(禁字符串匹配)")
	}
}

func TestMySQLSchemaSQL(t *testing.T) {
	ddl := (mysqlDialect{}).SchemaSQL("myapp_")
	joined := strings.Join(ddl, "\n")
	for _, want := range []string{"myapp_tasks", "myapp_task_deps", "utf8mb4_bin",
		"myapp_idx_claim", "VARCHAR(255)", "LONGBLOB"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("DDL 应含 %q", want)
		}
	}
}

func TestMySQLLockNameTruncate(t *testing.T) {
	long := strings.Repeat("x", 200)
	if got := lockName(long); len(got) > 64 {
		t.Fatalf("GET_LOCK 名字应截断到 ≤64,got %d", len(got))
	}
}

func TestValidateLen(t *testing.T) {
	if err := validateLen("id", strings.Repeat("a", 255)); err != nil {
		t.Fatalf("255 字符应放行: %v", err)
	}
	if err := validateLen("id", strings.Repeat("a", 256)); err == nil {
		t.Fatal("256 字符应报错")
	}
	// 多字节字符按"字符数"算,不是字节数:255 个中文合法(字节数远超 255)。
	if err := validateLen("queue", strings.Repeat("中", 255)); err != nil {
		t.Fatalf("255 个中文字符应放行: %v", err)
	}
}
