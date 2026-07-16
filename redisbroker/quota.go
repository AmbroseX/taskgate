package redisbroker

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/AmbroseX/taskgate"
)

// testQuotaNow 测试专用的介质时间覆盖(unix 秒):非 nil 时其返回值经 ARGV 传给脚本,
// 生产恒为 nil → 脚本内用 redis.call('TIME')(介质服务端钟,裁决 #5)。
// miniredis 档通常用 SetTime 控制 TIME 回音即可,这个钩子是备用缝。
var testQuotaNow func() int64

// QueueQuota 构造该队列的配额闸(taskgate.QuotaProvider 能力实现)。
func (b *Broker) QueueQuota(queue string, qc taskgate.QueueConfig) (taskgate.QuotaGate, error) {
	key := qc.QuotaKey
	if key == "" {
		key = queue
	}
	if err := validateBusinessKey(key); err != nil { // quota key 同样拼进 redis 键,复用校验
		return nil, err
	}
	periodSec := int64(time.Duration(qc.QuotaPeriod) / time.Second)
	if qc.QuotaLimit <= 0 || periodSec < 1 {
		return nil, fmt.Errorf("redisbroker: invalid quota config for queue %q: limit=%d period=%v",
			queue, qc.QuotaLimit, time.Duration(qc.QuotaPeriod))
	}
	return &redisQuotaGate{b: b, key: key, limit: qc.QuotaLimit, periodSec: periodSec}, nil
}

// redisQuotaGate redis 介质的配额闸:预留/退还各一段 Lua,天然串行原子。
type redisQuotaGate struct {
	b         *Broker
	key       string
	limit     int
	periodSec int64
}

// Reserve 三态合同见 taskgate.QuotaGate。
func (g *redisQuotaGate) Reserve(ctx context.Context) (*taskgate.QuotaReservation, error) {
	if err := g.b.requireInit(); err != nil {
		return nil, err
	}
	override := ""
	if testQuotaNow != nil {
		override = strconv.FormatInt(testQuotaNow(), 10)
	}
	res, err := scriptQuotaReserve.Run(ctx, g.b.rdb, nil,
		g.b.prefix, g.key, g.periodSec, g.limit, override).Result()
	if err != nil {
		return nil, err // 介质故障:调用方 fail-closed
	}
	reply, ok := res.([]any)
	if !ok || len(reply) != 2 {
		return nil, fmt.Errorf("redisbroker: unexpected quota reply %T", res)
	}
	okFlag, _ := reply[0].(int64)
	win, _ := reply[1].(int64)
	if okFlag != 1 {
		return nil, nil // 本窗口耗尽:不是错误
	}
	return &taskgate.QuotaReservation{Window: win}, nil
}

// Release 尽力退还:键已过期/窗口切走则落空无害;错误原样返回但调用方不重试(保守)。
func (g *redisQuotaGate) Release(ctx context.Context, r *taskgate.QuotaReservation) error {
	if r == nil {
		return nil
	}
	return scriptQuotaRelease.Run(ctx, g.b.rdb, nil,
		g.b.prefix, g.key, strconv.FormatInt(r.Window, 10)).Err()
}
