package sqlbroker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/AmbroseX/taskgate"
)

// testQuotaNow 测试专用的介质时间覆盖(unix 秒):非 nil 时其返回值作为各语句的
// 时间覆盖参数;生产恒为 nil → 传 NULL → 窗口用数据库服务端钟(裁决 #5)。
var testQuotaNow func() int64

// SetTestQuotaNow 仅测试用(与 TotalRetries 同类的测试观测口):设置介质时间覆盖,
// 传 nil 恢复"用数据库服务端钟"。非并发安全,只应在跑用例前的单线程阶段设置。
func SetTestQuotaNow(fn func() int64) { testQuotaNow = fn }

// QueueQuota 构造该队列的配额闸(taskgate.QuotaProvider 能力实现)。
func (b *Broker) QueueQuota(queue string, qc taskgate.QueueConfig) (taskgate.QuotaGate, error) {
	key := qc.QuotaKey
	if key == "" {
		key = queue
	}
	periodSec := int64(time.Duration(qc.QuotaPeriod) / time.Second)
	if qc.QuotaLimit <= 0 || periodSec < 1 {
		return nil, fmt.Errorf("%s: invalid quota config for queue %q: limit=%d period=%v",
			b.dialect.Name(), queue, qc.QuotaLimit, time.Duration(qc.QuotaPeriod))
	}
	q := b.dialect.QuotaSQL(b.cfg.TablePrefix)
	table := b.cfg.TablePrefix + "quota"
	return &sqlQuotaGate{
		b: b, key: key, limit: qc.QuotaLimit, periodSec: periodSec,
		reserveSQL: b.dialect.Rebind(q.Reserve),
		nowSQL:     b.dialect.Rebind(q.Now),
		upsertSQL:  b.dialect.Rebind(q.Upsert),
		releaseSQL: b.dialect.Rebind(`UPDATE ` + table + ` SET used = used - 1 WHERE qkey = ? AND win = ? AND used > 0`),
		cleanupSQL: b.dialect.Rebind(`DELETE FROM ` + table + ` WHERE qkey = ? AND win < ?`),
	}, nil
}

// sqlQuotaGate 服务器型数据库介质的配额闸。同 key 的所有消费者争同一行,
// 争用代价由基准测试定论(spec FR-013)。
type sqlQuotaGate struct {
	b          *Broker
	key        string
	limit      int
	periodSec  int64
	reserveSQL string // 非空 = 单语句路径(PG)
	nowSQL     string
	upsertSQL  string
	releaseSQL string
	cleanupSQL string
	lastWin    atomic.Int64
}

// Reserve 三态合同见 taskgate.QuotaGate。
func (g *sqlQuotaGate) Reserve(ctx context.Context) (*taskgate.QuotaReservation, error) {
	if err := g.b.requireInit(); err != nil {
		return nil, err
	}
	var override any // 生产 NULL:窗口用数据库服务端钟
	if testQuotaNow != nil {
		override = testQuotaNow()
	}
	var win int64
	if g.reserveSQL != "" {
		// 单语句路径(PG):零行 = 耗尽。
		err := g.b.db.QueryRowContext(ctx, g.reserveSQL,
			g.key, override, g.periodSec, g.periodSec, g.limit).Scan(&win)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, nil
		case err != nil:
			return nil, err
		}
	} else {
		// 两步路径(MySQL):取时算窗 → 原子扣减,按 affected 判定。
		var now int64
		if err := g.b.db.QueryRowContext(ctx, g.nowSQL, override).Scan(&now); err != nil {
			return nil, err
		}
		win = now / g.periodSec * g.periodSec
		res, err := g.b.db.ExecContext(ctx, g.upsertSQL, g.key, win, g.limit)
		if err != nil {
			return nil, err
		}
		aff, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if aff == 0 {
			return nil, nil // 没变 = 本窗口耗尽
		}
	}
	// 窗口轮换:尽力清一次旧窗行(每窗每进程摊销一次,失败无所谓)。
	if last := g.lastWin.Swap(win); last != 0 && last != win {
		_, _ = g.b.db.ExecContext(ctx, g.cleanupSQL, g.key, win-2*g.periodSec)
	}
	return &taskgate.QuotaReservation{Window: win}, nil
}

// Release 尽力退还:只退预留窗口,行已清理/窗口已切走则零行,落空无害。
func (g *sqlQuotaGate) Release(ctx context.Context, r *taskgate.QuotaReservation) error {
	if r == nil {
		return nil
	}
	_, err := g.b.db.ExecContext(ctx, g.releaseSQL, g.key, r.Window)
	return err
}
