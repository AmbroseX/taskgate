package sqlitebroker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/oklog/ulid/v2"
)

// Replay 重放一次终态执行(spec 005):定位目标、校验前置条件(终态/未被重放/
// completed 需显式允许)、创建新执行,全部在同一个事务内原子完成——并发同目标
// 重放恰好一个成功(BEGIN IMMEDIATE 串行化写者,uq_replay_of 唯一索引兜底)。
// 目标行零改写;新执行沿用目标的 Type/Queue/MaxRetry/OnParentFailure 与 BusinessKey,
// 三计数清零、无依赖、pending 落库。
func (b *Broker) Replay(ctx context.Context, req taskgate.ReplayRequest) (*taskgate.Task, error) {
	if err := b.requireInit(); err != nil {
		return nil, err
	}
	if (req.ExecutionID == "") == (req.BusinessKey == "") {
		return nil, errors.New("sqlitebroker: replay needs exactly one of ExecutionID / BusinessKey")
	}
	var stored taskgate.Task
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		// 定位目标:按 ID 直取;按键取链尾——链尾 = 键下没有任何执行的 replay_of 指向它的那条
		// (链不分叉不变式保证唯一),不依赖时间精度。
		var target *rec
		var err error
		if req.ExecutionID != "" {
			target, err = getRec(ctx, tx, req.ExecutionID)
		} else {
			target, err = scanRec(tx.QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks
				WHERE business_key = ?
				  AND NOT EXISTS (SELECT 1 FROM tasks c WHERE c.replay_of = tasks.id)
				LIMIT 1`, req.BusinessKey))
			if errors.Is(err, sql.ErrNoRows) {
				err = fmt.Errorf("%w: business key %q", taskgate.ErrTaskNotFound, req.BusinessKey)
			}
		}
		if err != nil {
			return err
		}

		// 前置校验:终态 → 链尾(未被重放) → completed 显式允许。
		if !target.task.Status.IsFinal() {
			return fmt.Errorf("%w: %s is %s", taskgate.ErrReplayNotFinal, target.task.ID, target.task.Status)
		}
		var one int
		err = tx.QueryRowContext(ctx, `SELECT 1 FROM tasks WHERE replay_of = ?`, target.task.ID).Scan(&one)
		if err == nil {
			return fmt.Errorf("%w: %s", taskgate.ErrAlreadyReplayed, target.task.ID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if target.task.Status == taskgate.StatusCompleted && !req.AllowCompleted {
			return fmt.Errorf("%w: %s", taskgate.ErrCompletedNotAllowed, target.task.ID)
		}

		// 创建新执行。Payload:nil 复制目标,非 nil 用覆盖值。
		payload := req.Payload
		if payload == nil {
			payload = target.task.Payload
		}
		now := b.clk.Now().Truncate(time.Millisecond)
		stored = taskgate.Task{
			ID:              ulid.Make().String(),
			BusinessKey:     target.task.BusinessKey,
			ReplayOf:        target.task.ID,
			Type:            target.task.Type,
			Queue:           target.task.Queue,
			Payload:         payload,
			Status:          taskgate.StatusPending,
			MaxRetry:        target.task.MaxRetry,
			OnParentFailure: target.task.OnParentFailure,
			RunAt:           now,
			CreatedAt:       now,
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO tasks (`+taskCols+`)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			stored.ID, stored.Type, stored.Queue, stored.Payload, string(stored.Status), nil,
			"", 0, stored.MaxRetry, 0, 0,
			ms(stored.RunAt), "[]", string(stored.OnParentFailure), 0, "", 0, 0,
			ms(stored.CreatedAt), 0, 0, stored.BusinessKey, stored.ReplayOf)
		if err != nil && strings.Contains(err.Error(), "uq_replay_of") {
			// 多进程写同一库文件时的并发兜底:唯一索引替我们裁决,翻译回合同错误。
			return fmt.Errorf("%w: %s", taskgate.ErrAlreadyReplayed, target.task.ID)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	b.wakeAll() // 新执行进了队列,叫醒等待中的 Dequeue
	b.fireNotify([]taskgate.Task{stored})
	out := stored
	return &out, nil
}
