package sqlbroker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/oklog/ulid/v2"
)

// Enqueue 入队。同一个事务内完成:ID 查重、父任务存在性校验、初始状态判定(DecideOnSubmit)、
// tasks 与 task_deps 落库;生成的 ID 与判定结果回填到 *t。撞主键翻译成 ErrTaskExists。
func (b *Broker) Enqueue(ctx context.Context, t *taskgate.Task) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	if t.ReplayOf != "" {
		// ReplayOf 只能由 Replay 写入:放行会绕过"链不分叉"约束(合同 Enqueue 条款)。
		return fmt.Errorf("%s: enqueue must not carry ReplayOf (use Replay)", b.dialect.Name())
	}
	notifs, err := b.withTx(ctx, func(tx *sql.Tx) ([]taskgate.Task, error) {
		// 业务键幂等:键下存在任何执行(不论状态)一律拒,错误携带链尾信息。
		// 这里的 SELECT 只为报错友好;并发窗口由 uq_chain_head 唯一索引兜底(撞索引后
		// 在下方 INSERT 的错误翻译里重查链尾)。
		if t.BusinessKey != "" {
			if te, err := b.taskExistsErr(ctx, tx, t.BusinessKey); err != nil {
				return nil, err
			} else if te != nil {
				return nil, te
			}
		}
		// ID 先在局部变量里生成/使用:全部校验通过、落库成功后才回填,
		// 报错路径不能让调用方拿到一个根本不存在的孤儿 ID。
		id := t.ID
		if id != "" {
			var one int
			err := tx.QueryRowContext(ctx, b.prep(`SELECT 1 FROM {{t}} WHERE id = ?`), id).Scan(&one)
			if err == nil {
				return nil, fmt.Errorf("%w: %s", taskgate.ErrTaskExists, id)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		} else {
			id = ulid.Make().String()
		}
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
			// 父行必须 FOR UPDATE 加锁:否则"父完成后向子传播减计数"与"本次子入队"并发交错时,
			// 父的传播可能读不到本子尚未提交的依赖边(不给本子减计数),而本子又带着满额
			// pending_parents 提交、父不会再传播 → 子永远卡 blocked。加锁强制两者严格先后
			// (sqlite 靠单连接串行天然避开,SQL 后端必须显式锁;死锁交给 withTx 重试环)。
			err := tx.QueryRowContext(ctx, b.prep(`SELECT status FROM {{t}} WHERE id = ? FOR UPDATE`), pid).Scan(&st)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: parent %s (child %s)", taskgate.ErrTaskNotFound, pid, id)
			}
			if err != nil {
				return nil, err
			}
			uniq = append(uniq, pid)
			parents = append(parents, taskgate.ParentState{ID: pid, Status: taskgate.Status(st)})
		}
		policy := t.OnParentFailure
		if policy == "" {
			policy = taskgate.FailFast
		}
		dec := taskgate.DecideOnSubmit(parents, policy)

		stored := *t
		stored.ID = id
		stored.Status = dec.Status
		stored.OnParentFailure = policy
		stored.CreatedAt = now
		stored.RunAt = stored.RunAt.Truncate(time.Millisecond)
		stored.StartedAt = stored.StartedAt.Truncate(time.Millisecond)
		stored.FinishedAt = stored.FinishedAt.Truncate(time.Millisecond)
		if stored.RunAt.IsZero() {
			stored.RunAt = now
		}
		stored.LeaseToken = ""
		if dec.Status == taskgate.StatusCanceled {
			stored.LastError = dec.LastError
			stored.FinishedAt = now
		}

		depsJSON := "[]"
		if len(stored.DependsOn) > 0 {
			raw, err := json.Marshal(stored.DependsOn)
			if err != nil {
				return nil, fmt.Errorf("sqlbroker: marshal depends_on: %w", err)
			}
			depsJSON = string(raw)
		}

		if _, err := tx.ExecContext(ctx, b.prep(`INSERT INTO {{t}} (`+taskCols+`)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`),
			stored.ID, stored.Type, stored.Queue, stored.Payload, string(stored.Status), stored.Result,
			stored.LastError, stored.Attempts, stored.MaxRetry, stored.LeaseLost, stored.Throttled,
			ms(stored.RunAt), depsJSON, string(policy), dec.PendingParents, "", int64(0), int64(0),
			ms(stored.CreatedAt), ms(stored.StartedAt), ms(stored.FinishedAt),
			stored.BusinessKey, stored.ReplayOf); err != nil {
			// 并发下另一个 Enqueue 抢先插了同 ID 或同链头:预检没查到但插入撞键,
			// 按撞的约束翻译(撞链头 → errChainHeadDup,withTx 之后重查链尾拼 TaskExistsError)。
			if b.dialect.IsDuplicateKey(err) {
				if strings.Contains(b.dialect.DuplicateKeyConstraint(err), "uq_chain_head") {
					return nil, errChainHeadDup
				}
				return nil, fmt.Errorf("%w: %s", taskgate.ErrTaskExists, stored.ID)
			}
			return nil, err
		}
		// 登记依赖边(去重后的父列表);父已是终态的边直接标 done,传播时不再依赖它。
		for i, pid := range uniq {
			done := int64(0)
			if parents[i].Status.IsFinal() {
				done = 1
			}
			if _, err := tx.ExecContext(ctx,
				b.prep(`INSERT INTO {{d}} (child_id, parent_id, done) VALUES (?,?,?)`),
				stored.ID, pid, done); err != nil {
				return nil, err
			}
		}
		return []taskgate.Task{stored}, nil
	})
	if errors.Is(err, errChainHeadDup) {
		// 撞了链头唯一索引:事务已回滚,库外重查一次链尾把信息带给调用方;
		// 极端下链尾又被动过就退化成裸 ErrTaskExists,幂等语义不受影响。
		if te, terr := b.taskExistsErr(ctx, b.db, t.BusinessKey); terr == nil && te != nil {
			return te
		}
		return fmt.Errorf("%w: business key %q", taskgate.ErrTaskExists, t.BusinessKey)
	}
	if err != nil {
		return err
	}
	*t = notifs[0] // 回填生成的 ID 与判定结果,调用方直接可用
	b.wakeAll()    // 可能有 Dequeue 正等着新任务
	b.fireNotify(notifs)
	return nil
}

// errChainHeadDup 内部哨兵:INSERT 撞 uq_chain_head(并发同键入队输家)。
// 只在 Enqueue 的 withTx 与错误翻译之间传递,不出包。
var errChainHeadDup = errors.New("sqlbroker: chain head duplicate")

// taskExistsErr 查键下链尾(最新执行),存在则拼 *TaskExistsError,不存在返回 (nil, nil)。
func (b *Broker) taskExistsErr(ctx context.Context, q querier, key string) (*taskgate.TaskExistsError, error) {
	var tailID, tailStatus string
	err := q.QueryRowContext(ctx, b.prep(`SELECT id, status FROM {{t}}
		WHERE business_key = ? ORDER BY created_at DESC, id DESC LIMIT 1`), key).Scan(&tailID, &tailStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &taskgate.TaskExistsError{
		BusinessKey: key,
		ExecutionID: tailID,
		Status:      taskgate.Status(tailStatus),
	}, nil
}
