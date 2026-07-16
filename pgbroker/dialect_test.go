package pgbroker

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/AmbroseX/taskgate/internal/sqlbroker"
	"github.com/jackc/pgx/v5/pgconn"
)

// 纯函数单测(离线、确定),覆盖 ?→$n 改写与错误分类等 happy-path 契约压不到的分支。

func TestPGRebind(t *testing.T) {
	cases := []struct{ in, want string }{
		{`SELECT ? FROM t`, `SELECT $1 FROM t`},
		{`WHERE a=? AND b IN (?,?)`, `WHERE a=$1 AND b IN ($2,$3)`},
		// 单引号内的 ? 不应被替换(核心 SQL 其实没有,稳妥起见验证)。
		{`WHERE s='a?b' AND x=?`, `WHERE s='a?b' AND x=$1`},
		{`no placeholders`, `no placeholders`},
	}
	for _, c := range cases {
		if got := (pgDialect{}).Rebind(c.in); got != c.want {
			t.Fatalf("Rebind(%q) = %q,期望 %q", c.in, got, c.want)
		}
	}
}

func TestPGIsDuplicateKey(t *testing.T) {
	d := pgDialect{}
	dup := fmt.Errorf("wrap: %w", &pgconn.PgError{Code: "23505"})
	if !d.IsDuplicateKey(dup) {
		t.Fatal("SQLSTATE 23505 应判为重复键")
	}
	if d.IsDuplicateKey(&pgconn.PgError{Code: "40P01"}) {
		t.Fatal("40P01 不是重复键")
	}
	if d.IsDuplicateKey(errors.New("duplicate key value violates unique constraint")) {
		t.Fatal("纯文案不应被误判为重复键(禁字符串匹配)")
	}
}

func TestPGRetryable(t *testing.T) {
	d := pgDialect{}
	cases := []struct {
		code string
		want sqlbroker.RetryClass
	}{
		{"40P01", sqlbroker.RetryImmediate}, // deadlock_detected
		{"40001", sqlbroker.RetryImmediate}, // serialization_failure
		{"23505", sqlbroker.NotRetryable},   // unique_violation
	}
	for _, c := range cases {
		got := d.Retryable(fmt.Errorf("x: %w", &pgconn.PgError{Code: c.code}))
		if got != c.want {
			t.Fatalf("SQLSTATE %s:Retryable = %d,期望 %d", c.code, got, c.want)
		}
	}
	if d.Retryable(errors.New("deadlock detected")) != sqlbroker.NotRetryable {
		t.Fatal("纯文案不应被判为可重试(禁字符串匹配)")
	}
}

func TestPGSchemaSQL(t *testing.T) {
	ddl := (pgDialect{}).SchemaSQL("myapp_")
	joined := strings.Join(ddl, "\n")
	for _, want := range []string{"myapp_tasks", "myapp_task_deps", "BYTEA", "BIGINT", "myapp_idx_claim"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("DDL 应含 %q", want)
		}
	}
}

func TestPGAdvisoryKeyStable(t *testing.T) {
	// 同一个 key 必须 hash 出同一个锁号(否则加锁/解锁对不上)。
	k1 := advisoryKey("taskgate_schema")
	k2 := advisoryKey("taskgate_schema")
	if k1 != k2 {
		t.Fatal("advisoryKey 对同一 key 应稳定")
	}
	if advisoryKey("a_schema") == advisoryKey("b_schema") {
		t.Fatal("不同前缀应大概率得到不同锁号")
	}
}
