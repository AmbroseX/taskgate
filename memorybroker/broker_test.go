package memorybroker_test

import (
	"testing"

	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/brokertest"
	"github.com/ambrose/taskgate/memorybroker"
)

// TestBrokerContract 一行接入统一契约套件:memory 后端必须过全部 16 条契约。
func TestBrokerContract(t *testing.T) {
	brokertest.Run(t, func(t *testing.T, opts taskgate.BrokerOptions) taskgate.Broker {
		b := memorybroker.New()
		if err := b.Init(opts); err != nil {
			t.Fatalf("memorybroker Init 失败: %v", err)
		}
		return b
	})
}
