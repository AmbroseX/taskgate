package sqlitebroker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ambrose/taskgate"
	"github.com/oklog/ulid/v2"
)

// Enqueue 入队。同一个事务内完成:ID 查重、父任务存在性校验、初始状态判定
// (DecideOnSubmit)、tasks 与 task_deps 落库;生成的 ID 与判定结果回填到 *t。
func (b *Broker) Enqueue(ctx context.Context, t *taskgate.Task) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	var stored taskgate.Task
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		if t.ID != "" {
			var one int
			err := tx.QueryRowContext(ctx, `SELECT 1 FROM tasks WHERE id = ?`, t.ID).Scan(&one)
			if err == nil {
				return fmt.Errorf("%w: %s", taskgate.ErrTaskExists, t.ID)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		} else {
			t.ID = ulid.Make().String()
		}
		now := b.clk.Now()

		// 父任务必须全部已存在(依赖无环靠这条提交校验,不做环检测);
		// 同一个父 ID 写了多遍只算一个,否则 pending_parents 会多计、永远唤不醒。
		seen := make(map[string]bool, len(t.DependsOn))
		var uniq []string
		var parents []taskgate.ParentState
		for _, pid := range t.DependsOn {
			if seen[pid] {
				continue
			}
			seen[pid] = true
			var st string
			err := tx.QueryRowContext(ctx, `SELECT status FROM tasks WHERE id = ?`, pid).Scan(&st)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: parent %s (child %s)", taskgate.ErrTaskNotFound, pid, t.ID)
			}
			if err != nil {
				return err
			}
			uniq = append(uniq, pid)
			parents = append(parents, taskgate.ParentState{ID: pid, Status: taskgate.Status(st)})
		}
		policy := t.OnParentFailure
		if policy == "" {
			policy = taskgate.FailFast
		}
		dec := taskgate.DecideOnSubmit(parents, policy)

		stored = *t
		stored.Status = dec.Status
		stored.OnParentFailure = policy
		stored.CreatedAt = now
		if stored.RunAt.IsZero() {
			stored.RunAt = now
		}
		stored.LeaseToken = "" // 入队不可能自带租约
		if dec.Status == taskgate.StatusCanceled {
			// 提交时父已失败/取消且 FailFast:直接以 canceled 落库。
			stored.LastError = dec.LastError
			stored.FinishedAt = now
		}

		depsJSON := "[]" // DependsOn 原样存 JSON 数组文本(往返一致),去重只作用于 task_deps
		if len(stored.DependsOn) > 0 {
			raw, err := json.Marshal(stored.DependsOn)
			if err != nil {
				return fmt.Errorf("sqlitebroker: marshal depends_on: %w", err)
			}
			depsJSON = string(raw)
		}

		if _, err := tx.ExecContext(ctx, `INSERT INTO tasks (`+taskCols+`)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			stored.ID, stored.Type, stored.Queue, stored.Payload, string(stored.Status), stored.Result,
			stored.LastError, stored.Attempts, stored.MaxRetry, stored.LeaseLost, stored.Throttled,
			ms(stored.RunAt), depsJSON, string(policy), dec.PendingParents, "", 0, 0,
			ms(stored.CreatedAt), ms(stored.StartedAt), ms(stored.FinishedAt)); err != nil {
			return err
		}
		// 登记依赖边(去重后的父列表);父已是终态的边直接标 done,传播时不再依赖它。
		for i, pid := range uniq {
			done := 0
			if parents[i].Status.IsFinal() {
				done = 1
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO task_deps (child_id, parent_id, done) VALUES (?,?,?)`,
				stored.ID, pid, done); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	*t = stored // 回填生成的 ID 与判定结果,调用方直接可用
	b.wakeAll() // 可能有 Dequeue 正等着新任务
	b.fireNotify([]taskgate.Task{stored})
	return nil
}
