package redisbroker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ambrose/taskgate"
	"github.com/oklog/ulid/v2"
)

// Enqueue 入队。查重、父校验、初始状态判定、落库全部在 enqueue.lua 一段脚本内原子完成;
// ID 由 Go 生成注入(脚本内禁 math.random),生成的 ID 与判定结果只在成功后回填 *t——
// 报错路径不能让调用方拿到一个根本不存在的孤儿 ID(合同 Enqueue 条款)。
func (b *Broker) Enqueue(ctx context.Context, t *taskgate.Task) error {
	if err := b.requireInit(); err != nil {
		return err
	}
	id := t.ID
	if id == "" {
		id = ulid.Make().String()
	}
	now := b.clk.Now()
	runAt := t.RunAt
	if runAt.IsZero() {
		runAt = now // RunAt 零值取入队时刻
	}
	policy := t.OnParentFailure
	if policy == "" {
		policy = taskgate.FailFast
	}

	// 父 ID 去重(保持原有顺序):同一个父写多遍只算一个,否则 pending_parents
	// 会多计、永远唤不醒(与 DecideOnSubmit 内部去重同一口径)。
	seen := make(map[string]bool, len(t.DependsOn))
	var uniq []string
	for _, pid := range t.DependsOn {
		if seen[pid] {
			continue
		}
		seen[pid] = true
		uniq = append(uniq, pid)
	}

	// DependsOn 原样存 JSON 数组文本(往返一致),去重只作用于依赖边。
	depsJSON := "[]"
	if len(t.DependsOn) > 0 {
		raw, err := json.Marshal(t.DependsOn)
		if err != nil {
			return fmt.Errorf("redisbroker: marshal depends_on: %w", err)
		}
		depsJSON = string(raw)
	}

	args := make([]any, 0, 15+len(uniq))
	args = append(args, b.prefix, id, t.Type, t.Queue, string(t.Payload), string(t.Result),
		t.MaxRetry, t.Attempts, t.LeaseLost, t.Throttled,
		runAt.UnixMilli(), depsJSON, string(policy), now.UnixMilli(), len(uniq))
	for _, pid := range uniq {
		args = append(args, pid)
	}

	res, err := scriptEnqueue.Run(ctx, b.rdb, nil, args...).Result()
	if err != nil {
		return mapLuaErr(err)
	}
	reply, ok := res.([]any)
	if !ok || len(reply) < 1 {
		return fmt.Errorf("redisbroker: unexpected enqueue reply %T", res)
	}
	status, _ := reply[0].(string)
	lastError := ""
	if len(reply) > 1 {
		lastError, _ = reply[1].(string) // Lua 空串在数组回复里也可能缺位,缺位按 ""
	}

	// 成功才回填:生成的 ID 与判定结果,调用方直接可用。
	stored := *t
	stored.ID = id
	stored.Status = taskgate.Status(status)
	stored.OnParentFailure = policy
	stored.CreatedAt = now
	stored.RunAt = runAt
	stored.LeaseToken = "" // 入队不可能自带租约
	if stored.Status == taskgate.StatusCanceled {
		// 提交时父已失败/取消且 FailFast:直接以 canceled 落库。
		stored.LastError = lastError
		stored.FinishedAt = now
	}
	*t = stored
	b.wakeAll() // 可能有 Dequeue 正等着新任务
	b.fireNotify([]taskgate.Task{stored})
	return nil
}
