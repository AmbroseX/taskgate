package sqlbroker

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

// Replay 重放一次终态执行(spec 005):定位目标(按 ID 或按键取链尾)、校验前置条件
// (终态/未被重放/completed 需显式允许)、创建新执行,全部在同一个事务内完成。
// 并发同目标重放:目标行 SELECT ... FOR UPDATE 串行化,uq_replay_of 唯一索引兜底,
// 恰好一个成功。目标行零改写;新执行沿用目标的 Type/Queue/MaxRetry/OnParentFailure
// 与 BusinessKey,三计数清零、无依赖、pending 落库。
func (b *Broker) Replay(ctx context.Context, req taskgate.ReplayRequest) (*taskgate.Task, error) {
	if err := b.requireInit(); err != nil {
		return nil, err
	}
	if (req.ExecutionID == "") == (req.BusinessKey == "") {
		return nil, fmt.Errorf("%s: replay needs exactly one of ExecutionID / BusinessKey", b.dialect.Name())
	}
	notifs, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		// 定位目标并加行锁:按 ID 直取;按键取链尾——链尾 = 键下没有任何执行的 replay_of
		// 指向它的那条(链不分叉不变式保证唯一),不依赖时间精度。
		var target *rec
		var err error
		if req.ExecutionID != "" {
			target, err = b.getRecForUpdate(ctx, tx, req.ExecutionID)
		} else {
			target, err = scanRec(tx.QueryRowContext(ctx, b.prep(`SELECT `+taskCols+` FROM {{t}}
				WHERE business_key = ?
				  AND NOT EXISTS (SELECT 1 FROM {{t}} c WHERE c.replay_of = {{t}}.id)
				FOR UPDATE`), req.BusinessKey))
			if errors.Is(err, sql.ErrNoRows) {
				err = fmt.Errorf("%w: business key %q", taskgate.ErrTaskNotFound, req.BusinessKey)
			}
		}
		if err != nil {
			return nil, err
		}

		// 前置校验:终态 → 链尾(未被重放) → completed 显式允许。
		// 目标行已被 FOR UPDATE 锁住,并发重放在这里严格先后,输家看到"已被重放"。
		if !target.task.Status.IsFinal() {
			return nil, fmt.Errorf("%w: %s is %s", taskgate.ErrReplayNotFinal, target.task.ID, target.task.Status)
		}
		var one int
		err = tx.QueryRowContext(ctx, b.prep(`SELECT 1 FROM {{t}} WHERE replay_of = ?`), target.task.ID).Scan(&one)
		if err == nil {
			return nil, fmt.Errorf("%w: %s", taskgate.ErrAlreadyReplayed, target.task.ID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if target.task.Status == taskgate.StatusCompleted && !req.AllowCompleted {
			return nil, fmt.Errorf("%w: %s", taskgate.ErrCompletedNotAllowed, target.task.ID)
		}

		// 创建新执行。Payload:nil 复制目标,非 nil 用覆盖值。
		payload := req.Payload
		if payload == nil {
			payload = target.task.Payload
		}
		now := b.clk.Now().Truncate(time.Millisecond)
		stored := taskgate.Task{
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
		if _, err := tx.ExecContext(ctx, b.prep(`INSERT INTO {{t}} (`+taskCols+`)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`),
			stored.ID, stored.Type, stored.Queue, stored.Payload, string(stored.Status), nil,
			"", 0, stored.MaxRetry, 0, 0,
			ms(stored.RunAt), "[]", string(stored.OnParentFailure), 0, "", int64(0), int64(0),
			ms(stored.CreatedAt), int64(0), int64(0), stored.BusinessKey, stored.ReplayOf); err != nil {
			// FOR UPDATE 已串行化正常路径;这里是极端兜底(如目标行锁被绕过的实现缺陷),
			// 唯一索引替我们裁决,翻译回合同错误。
			if b.dialect.IsDuplicateKey(err) &&
				strings.Contains(b.dialect.DuplicateKeyConstraint(err), "uq_replay_of") {
				return nil, fmt.Errorf("%w: %s", taskgate.ErrAlreadyReplayed, target.task.ID)
			}
			return nil, err
		}
		return []taskgate.Task{stored}, nil
	})
	if err != nil {
		return nil, err
	}
	b.wakeAll() // 新执行进了队列,叫醒等待中的 Dequeue
	b.fireNotify(notifs)
	out := notifs[0]
	return &out, nil
}
