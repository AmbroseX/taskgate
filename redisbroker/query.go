package redisbroker

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ambrose/taskgate"
	"github.com/redis/go-redis/v9"
)

// allStatusNames 七态全集:任何任务任一时刻恰好在一个 idx:status:{s} 集合里,
// 七个集合的并集就是全量任务,List 不做全库 SCAN(research 第 4 节)。
var allStatusNames = []string{
	string(taskgate.StatusBlocked), string(taskgate.StatusPending), string(taskgate.StatusRunning),
	string(taskgate.StatusRetrying), string(taskgate.StatusCompleted), string(taskgate.StatusFailed),
	string(taskgate.StatusCanceled),
}

// Get 取单个任务。hash 解码出来的就是全新副本,调用方改了不影响存储。
func (b *Broker) Get(ctx context.Context, id string) (*taskgate.Task, error) {
	if err := b.requireInit(); err != nil {
		return nil, err
	}
	fields, err := b.rdb.HGetAll(ctx, b.kTask(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("%w: %s", taskgate.ErrTaskNotFound, id)
	}
	return decodeTask(fields)
}

// List 按 Filter 过滤,零值字段不过滤;结果按 CreatedAt(再按 ID)排序,行为确定。
// 候选 ID 从索引集合取(Status 优先、其次 Type,否则七个状态集合并集),
// 剩余条件读回 hash 后在 Go 侧过滤。
func (b *Broker) List(ctx context.Context, f taskgate.Filter) ([]*taskgate.Task, error) {
	if err := b.requireInit(); err != nil {
		return nil, err
	}
	var ids []string
	var err error
	switch {
	case f.Status != "":
		ids, err = b.rdb.SMembers(ctx, b.kIdxStatus(string(f.Status))).Result()
	case f.Type != "":
		ids, err = b.rdb.SMembers(ctx, b.kIdxType(f.Type)).Result()
	default:
		keys := make([]string, len(allStatusNames))
		for i, s := range allStatusNames {
			keys[i] = b.kIdxStatus(s)
		}
		ids, err = b.rdb.SUnion(ctx, keys...).Result()
	}
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	// 管道批量读 hash,减少往返;个别任务在读取间隙被删了就跳过。
	pipe := b.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, b.kTask(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	var out []*taskgate.Task
	for _, cmd := range cmds {
		fields, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		if len(fields) == 0 {
			continue
		}
		t, err := decodeTask(fields)
		if err != nil {
			return nil, err
		}
		if f.Type != "" && t.Type != f.Type {
			continue
		}
		if f.Queue != "" && t.Queue != f.Queue {
			continue
		}
		if f.Status != "" && t.Status != f.Status {
			continue
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

// QueueLen 队列积压:status∈{pending,retrying} 的数量(不看 RunAt 到没到点)。
// 排队中的任务要么在 pending list、要么在 delayed zset,恰好各占一处
// (离开排队状态时由各脚本立刻摘除),所以 LLEN+ZCARD 就是精确值,O(1)。
func (b *Broker) QueueLen(ctx context.Context, queue string) (int, error) {
	if err := b.requireInit(); err != nil {
		return 0, err
	}
	pipe := b.rdb.Pipeline()
	llen := pipe.LLen(ctx, b.kPending(queue))
	zcard := pipe.ZCard(ctx, b.kDelayed(queue))
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return int(llen.Val() + zcard.Val()), nil
}

// Counts 出现过的 Type×Status 稀疏矩阵:直接 HGETALL tg:stats(每段 Lua 流转时
// 顺手 HINCRBY 维护),O(矩阵大小),与逐个 Get 汇总一致(brokertest 契约 13 验证)。
// 流转来回抵消后可能留下 0 值字段,按"只含非零"的合同过滤掉。
func (b *Broker) Counts(ctx context.Context) (map[string]map[taskgate.Status]int64, error) {
	if err := b.requireInit(); err != nil {
		return nil, err
	}
	fields, err := b.rdb.HGetAll(ctx, b.kStats()).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[taskgate.Status]int64)
	for field, val := range fields {
		// 字段形如 "{type}:{status}";type 自身可能含冒号,从右边劈。
		i := strings.LastIndex(field, ":")
		if i < 0 {
			return nil, fmt.Errorf("redisbroker: bad stats field %q", field)
		}
		typ, st := field[:i], field[i+1:]
		var n int64
		if _, err := fmt.Sscanf(val, "%d", &n); err != nil {
			return nil, fmt.Errorf("redisbroker: bad stats value %q=%q: %w", field, val, err)
		}
		if n == 0 {
			continue
		}
		byStatus := out[typ]
		if byStatus == nil {
			byStatus = make(map[taskgate.Status]int64)
			out[typ] = byStatus
		}
		byStatus[taskgate.Status(st)] = n
	}
	return out, nil
}
