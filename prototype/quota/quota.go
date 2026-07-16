// Package quota 是 Quota 领域模型的原型验证(见
// docs/plans/2026-07-16-Quota领域模型.md),不进正式代码。
//
// 它在 sqlite 共享文件上实现硬配额的核心原子操作:
// "检查当前窗口余额 + 扣减/预留"一条语句完成,窗口键用介质自己的钟
// (sqlite 的共享介质是本机文件,strftime('%s','now') 就是介质钟)算,
// 不依赖任何应用进程的本地时间——这是模型第 2、4 节的落点。
package quota

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite" // 与 sqlitebroker 同款纯 Go 驱动
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS quota (
	qkey TEXT    NOT NULL,
	win  INTEGER NOT NULL, -- 窗口起点(unix 秒,对齐 period)
	used INTEGER NOT NULL,
	PRIMARY KEY (qkey, win)
);`

// 预留:一条原子语句完成"算窗口 + 检查余额 + 扣减"。
// 窗口起点在 SQL 里用 sqlite 自己的钟算(now/period*period,整数除法即对齐);
// 已有行且 used ≥ limit 时 DO UPDATE 的 WHERE 不命中 → 零行 → 预留失败。
const reserveSQL = `
INSERT INTO quota (qkey, win, used)
VALUES (?, CAST(strftime('%s','now') AS INTEGER) / ? * ?, 1)
ON CONFLICT (qkey, win) DO UPDATE SET used = used + 1 WHERE used < ?
RETURNING win`

// 退还:尽力而为的一条原子语句(模型第 2 节 released 路径)。
const releaseSQL = `UPDATE quota SET used = used - 1 WHERE qkey = ? AND win = ? AND used > 0`

// Store 一个进程视角的配额存储。介质是 sqlite 文件,多进程共享同一文件。
type Store struct {
	db        *sql.DB
	periodSec int64
	limit     int
}

// Open 打开(不存在则建表)共享配额库。busyTimeoutMS 是本进程等锁的上限,
// 超时即视为"介质不可达",调用方按 fail-closed 处理。
func Open(path string, periodSec int64, limit int, busyTimeoutMS int) (*Store, error) {
	if periodSec < 1 || limit < 1 {
		return nil, fmt.Errorf("quota: period/limit 必须 ≥1, got %d/%d", periodSec, limit)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(%d)&_pragma=synchronous(NORMAL)&_txlock=immediate",
		path, busyTimeoutMS)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("quota: open %s: %w", path, err)
	}
	db.SetMaxOpenConns(1) // 同 sqlitebroker:单进程内串行,避开自己人 SQLITE_BUSY
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("quota: apply schema: %w", err)
	}
	return &Store{db: db, periodSec: periodSec, limit: limit}, nil
}

// Reserve 预留一份额度。返回值三态:
//   - ok=true:预留成功,win 是本次预留落在的窗口起点(介质钟);
//   - ok=false 且 err=nil:本窗口额度耗尽(不是错误,等下个窗口);
//   - err≠nil:介质不可达等故障,调用方必须 fail-closed(零放行)。
func (s *Store) Reserve(ctx context.Context, qkey string) (win int64, ok bool, err error) {
	row := s.db.QueryRowContext(ctx, reserveSQL, qkey, s.periodSec, s.periodSec, s.limit)
	switch err := row.Scan(&win); {
	case err == nil:
		return win, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	default:
		return 0, false, err
	}
}

// Release 尽力退还一份预留(认领扑空/出错的补偿)。失败不重试——当 leaked,方向保守。
func (s *Store) Release(ctx context.Context, qkey string, win int64) error {
	_, err := s.db.ExecContext(ctx, releaseSQL, qkey, win)
	return err
}

// DB 暴露底层连接,只给原型测试模拟"介质不可达"(长持写锁)用。
func (s *Store) DB() *sql.DB { return s.db }

// Close 关库。
func (s *Store) Close() error { return s.db.Close() }
