package sqlitebroker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/AmbroseX/taskgate"
)

// testQuotaNow 测试专用的介质时间覆盖(spec 006):非 nil 时其返回值(unix 秒)作为
// 服务端时间传给 COALESCE 首参;生产恒为 nil → 传 NULL → 窗口用 sqlite 自己的钟。
// 生产窗口计算不依赖应用进程时钟是硬配额的裁决(#5),这个钩子只在 export_test.go 暴露。
var testQuotaNow func() int64

// quotaReserveSQL 预留:一条原子语句完成"取时 → 算窗口 → 检查余额 → 扣减"
// (原型 prototype/quota 验证过的形态,加了测试时间缝)。
// 参数:qkey, 时间覆盖(生产 NULL), period, period, limit。
// 已有行且 used ≥ limit 时 DO UPDATE 的 WHERE 不命中 → 零行 → 耗尽。
const quotaReserveSQL = `
INSERT INTO quota (qkey, win, used)
VALUES (?, CAST(COALESCE(?, strftime('%s','now')) AS INTEGER) / ? * ?, 1)
ON CONFLICT (qkey, win) DO UPDATE SET used = used + 1 WHERE used < ?
RETURNING win`

// QueueQuota 构造该队列的配额闸(taskgate.QuotaProvider 能力实现)。
func (b *Broker) QueueQuota(queue string, qc taskgate.QueueConfig) (taskgate.QuotaGate, error) {
	key := qc.QuotaKey
	if key == "" {
		key = queue
	}
	periodSec := int64(time.Duration(qc.QuotaPeriod) / time.Second)
	if qc.QuotaLimit <= 0 || periodSec < 1 {
		return nil, fmt.Errorf("sqlitebroker: invalid quota config for queue %q: limit=%d period=%v",
			queue, qc.QuotaLimit, time.Duration(qc.QuotaPeriod))
	}
	return &sqliteQuotaGate{b: b, key: key, limit: qc.QuotaLimit, periodSec: periodSec}, nil
}

// sqliteQuotaGate 文件介质的配额闸。计数表与任务表同库,跨进程天然共享;
// 复用 broker 的单连接池,busy_timeout 即"介质不可达"的判定上限。
type sqliteQuotaGate struct {
	b         *Broker
	key       string
	limit     int
	periodSec int64
	lastWin   atomic.Int64 // 上次预留的窗口起点,轮换时触发一次机会主义清理
}

// Reserve 三态合同见 taskgate.QuotaGate:成功 / (nil,nil) 耗尽 / err 介质故障。
func (g *sqliteQuotaGate) Reserve(ctx context.Context) (*taskgate.QuotaReservation, error) {
	if err := g.b.requireInit(); err != nil {
		return nil, err
	}
	var override any // 生产 NULL:窗口用介质自己的钟
	if testQuotaNow != nil {
		override = testQuotaNow()
	}
	var win int64
	err := g.b.db.QueryRowContext(ctx, quotaReserveSQL,
		g.key, override, g.periodSec, g.periodSec, g.limit).Scan(&win)
	switch {
	case err == nil:
		// 窗口轮换:尽力清一次旧窗行(每窗每进程摊销一次,失败无所谓)。
		if last := g.lastWin.Swap(win); last != 0 && last != win {
			_, _ = g.b.db.ExecContext(ctx,
				`DELETE FROM quota WHERE qkey = ? AND win < ?`, g.key, win-2*g.periodSec)
		}
		return &taskgate.QuotaReservation{Window: win}, nil
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil // 本窗口耗尽:不是错误
	default:
		return nil, err // 介质故障:调用方 fail-closed
	}
}

// Release 尽力退还:只退预留窗口,行已清理/窗口已切走则零行,落空无害。
func (g *sqliteQuotaGate) Release(ctx context.Context, r *taskgate.QuotaReservation) error {
	if r == nil {
		return nil
	}
	_, err := g.b.db.ExecContext(ctx,
		`UPDATE quota SET used = used - 1 WHERE qkey = ? AND win = ? AND used > 0`, g.key, r.Window)
	return err
}
