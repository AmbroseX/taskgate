package redisbroker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AmbroseX/taskgate"
	"github.com/oklog/ulid/v2"
)

// Replay 重放一次终态执行(spec 005):定位、校验、创建全部在 replay.lua 一段脚本内
// 原子完成——并发同目标重放天然串行,恰好一个成功。新执行 ID 与 now 由 Go 注入
// (脚本内禁 TIME/math.random,fakeclock 才有效)。目标记录对外零改写
// (脚本内部的 replayed 标记是链元数据,decodeTask 不外露)。
func (b *Broker) Replay(ctx context.Context, req taskgate.ReplayRequest) (*taskgate.Task, error) {
	if err := b.requireInit(); err != nil {
		return nil, err
	}
	if (req.ExecutionID == "") == (req.BusinessKey == "") {
		return nil, errors.New("redisbroker: replay needs exactly one of ExecutionID / BusinessKey")
	}
	if req.BusinessKey != "" {
		if err := validateBusinessKey(req.BusinessKey); err != nil {
			return nil, err
		}
	}
	newID := ulid.Make().String()
	now := b.clk.Now().Truncate(time.Millisecond)
	hasPayload, payload := "0", ""
	if req.Payload != nil {
		hasPayload, payload = "1", string(req.Payload)
	}
	allow := "0"
	if req.AllowCompleted {
		allow = "1"
	}
	res, err := scriptReplay.Run(ctx, b.rdb, nil,
		b.prefix, req.ExecutionID, req.BusinessKey, allow, hasPayload, payload,
		newID, now.UnixMilli()).Result()
	if err != nil {
		return nil, mapLuaErr(err)
	}
	flat, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("redisbroker: unexpected replay reply %T", res)
	}
	m, err := flatToMap(flat)
	if err != nil {
		return nil, err
	}
	t, err := decodeTask(m)
	if err != nil {
		return nil, err
	}
	b.wakeAll() // 新执行进了队列,叫醒等待中的 Dequeue
	b.fireNotify([]taskgate.Task{*t})
	return t, nil
}
