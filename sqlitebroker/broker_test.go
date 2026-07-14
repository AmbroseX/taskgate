package sqlitebroker_test

import (
	"path/filepath"
	"testing"

	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/brokertest"
	"github.com/ambrose/taskgate/sqlitebroker"
)

// TestBrokerContract 一行接入统一契约套件:sqlite 后端必须过全部 16 条契约。
// 每条用例一个独立的临时库文件,互不串数据。
func TestBrokerContract(t *testing.T) {
	brokertest.Run(t, func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker {
		b, err := sqlitebroker.Open(filepath.Join(t.TempDir(), "tasks.db"))
		if err != nil {
			t.Fatalf("sqlitebroker Open 失败: %v", err)
		}
		if err := b.Init(opts); err != nil {
			t.Fatalf("sqlitebroker Init 失败: %v", err)
		}
		return b
	})
}
