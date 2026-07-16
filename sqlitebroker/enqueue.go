package sqlitebroker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/oklog/ulid/v2"
)

// Enqueue 入队。同一个事务内完成:ID 查重、父任务存在性校验、初始状态判定
// (DecideOnSubmit)、tasks 与 task_deps 落库;生成的 ID 与判定结果回填到 *t。
func (b *Broker) Enqueue(ctx context.Context, t *taskgate.Task) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	if t.ReplayOf != "" {
		// ReplayOf 只能由 Replay 写入:放行会绕过"链不分叉"约束(合同 Enqueue 条款)。
		return errors.New("sqlitebroker: enqueue must not carry ReplayOf (use Replay)")
	}
	var stored taskgate.Task
	err := b.withTx(ctx, func(tx *sql.Tx) error {
		// 业务键幂等:键下存在任何执行(不论状态)一律拒,错误携带链尾信息。
		// 事务内检查(BEGIN IMMEDIATE 串行化写者),uq_chain_head 唯一索引兜底。
		if t.BusinessKey != "" {
			var tailID, tailStatus string
			err := tx.QueryRowContext(ctx, `SELECT id, status FROM tasks
				WHERE business_key = ? ORDER BY created_at DESC, id DESC LIMIT 1`,
				t.BusinessKey).Scan(&tailID, &tailStatus)
			if err == nil {
				return &taskgate.TaskExistsError{
					BusinessKey: t.BusinessKey,
					ExecutionID: tailID,
					Status:      taskgate.Status(tailStatus),
				}
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		// ID 先在局部变量里生成/使用:全部校验通过、落库成功后才回填 t.ID,
		// 报错路径不能让调用方拿到一个根本不存在的孤儿 ID。
		id := t.ID
		if id != "" {
			var one int
			err := tx.QueryRowContext(ctx, `SELECT 1 FROM tasks WHERE id = ?`, id).Scan(&one)
			if err == nil {
				return fmt.Errorf("%w: %s", taskgate.ErrTaskExists, id)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		} else {
			id = ulid.Make().String()
		}
		// 时间截到毫秒:落库走 ms(unix 毫秒),回填快照若带纳秒尾巴,
		// 就会出现"同一个后端写进去和读出来精度不一致",这里统一掐掉。
		now := b.clk.Now().Truncate(time.Millisecond)

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
				return fmt.Errorf("%w: parent %s (child %s)", taskgate.ErrTaskNotFound, pid, id)
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
		stored.ID = id
		stored.Status = dec.Status
		stored.OnParentFailure = policy
		stored.CreatedAt = now
		// 调用方传入的时间字段(RunAt 与迁移/导入预置的 StartedAt/FinishedAt)
		// 同样截毫秒,快照与落库读回值同精度。
		stored.RunAt = stored.RunAt.Truncate(time.Millisecond)
		stored.StartedAt = stored.StartedAt.Truncate(time.Millisecond)
		stored.FinishedAt = stored.FinishedAt.Truncate(time.Millisecond)
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
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			stored.ID, stored.Type, stored.Queue, stored.Payload, string(stored.Status), stored.Result,
			stored.LastError, stored.Attempts, stored.MaxRetry, stored.LeaseLost, stored.Throttled,
			ms(stored.RunAt), depsJSON, string(policy), dec.PendingParents, "", 0, 0,
			ms(stored.CreatedAt), ms(stored.StartedAt), ms(stored.FinishedAt),
			stored.BusinessKey, stored.ReplayOf); err != nil {
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
