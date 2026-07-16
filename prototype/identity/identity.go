// Package identity 是 Identity 领域模型的原型验证层(见
// docs/plans/2026-07-16-Identity领域模型.md),不进正式代码。
//
// Task 结构还没有 BusinessKey/ReplayOf 字段,原型把四概念记在层内的侧车 map 里,
// 包一层 taskgate 公共 API 跑真调度——验证的是模型语义,不是存储。
// 侧车用一把互斥锁模拟正式实现里"唯一约束 + 原子校验"的职责
// (模型文档问题清单 #6),单进程内语义等价。
//
// 约定:走本层 Submit 的任务不要再传 taskgate.WithID(模型终态:ExecutionID
// 用户不可指定),ID 一律由 broker 生成 ulid。
package identity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/AmbroseX/taskgate"
)

// 模型规则对应的错误。Submit 同键拒绝直接复用 taskgate.ErrTaskExists(契约 2 语义)。
var (
	// ErrNotFinal Replay 目标还没到终态(前置条件①)。
	ErrNotFinal = errors.New("identity: replay target not in final state")
	// ErrInFlight 该 BusinessKey 下还有非终态 execution(前置条件②,"≤1 在途"不变式)。
	ErrInFlight = errors.New("identity: business key has an in-flight execution")
	// ErrAlreadyReplayed 目标已被重放过,链不分叉(前置条件③)。
	ErrAlreadyReplayed = errors.New("identity: execution already replayed (chain must not fork)")
	// ErrCompletedNotAllowed 重放 completed 执行必须显式传 AllowCompleted(评审确认 #2)。
	ErrCompletedNotAllowed = errors.New("identity: replaying a completed execution requires AllowCompleted")
	// ErrUnknownExecution 目标 execution 不存在于本层记录。
	ErrUnknownExecution = errors.New("identity: unknown execution")
	// ErrUnknownKey 该 BusinessKey 不存在。
	ErrUnknownKey = errors.New("identity: unknown business key")
)

// ReplayOptions Replay 的显式参数。
type ReplayOptions struct {
	AllowCompleted bool            // 允许重放 completed 的执行,必须显式打开
	Payload        json.RawMessage // nil = 复制旧执行的 Payload(模型问题 #3 的倾向)
}

// Layer Identity 原型层。所有侧车状态用一把锁保护,
// 模拟正式实现里后端原子完成的唯一性校验。
type Layer struct {
	g *taskgate.Gate

	mu       sync.Mutex
	chains   map[string][]string // BusinessKey → 执行历史链(创建顺序)
	keyOf    map[string]string   // ExecutionID → BusinessKey("" = 无键)
	replayOf map[string]string   // 新 ExecutionID → 被重放的 ExecutionID
	replayed map[string]bool     // ExecutionID → 已被重放(链不分叉的判据)
	known    map[string]bool     // 本层见过的所有 ExecutionID(无键 Replay 也要能找到目标)
}

// New 包一个已装配好的 Gate。
func New(g *taskgate.Gate) *Layer {
	return &Layer{
		g:        g,
		chains:   map[string][]string{},
		keyOf:    map[string]string{},
		replayOf: map[string]string{},
		replayed: map[string]bool{},
		known:    map[string]bool{},
	}
}

// Submit 带 BusinessKey 的提交。key 为空 = 无业务级幂等,每次都是新 execution;
// key 非空且键下已存在任何 execution(不论状态)→ 一律拒绝(评审确认 #1)。
func (l *Layer) Submit(ctx context.Context, taskType string, payload json.RawMessage, key string, opts ...taskgate.SubmitOption) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if key != "" && len(l.chains[key]) > 0 {
		return "", fmt.Errorf("%w (business key %q)", taskgate.ErrTaskExists, key)
	}
	id, err := l.g.Submit(ctx, taskType, payload, opts...)
	if err != nil {
		return "", err
	}
	l.record(id, key, "")
	return id, nil
}

// ReplayByID 按 ExecutionID 重放。目标必须是其链的链尾(未被重放过)。
func (l *Layer) ReplayByID(ctx context.Context, execID string, opt ReplayOptions) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.known[execID] {
		return "", ErrUnknownExecution
	}
	return l.replay(ctx, execID, opt)
}

// ReplayByKey 按 BusinessKey 重放,天然作用于链尾(最新 execution)。
func (l *Layer) ReplayByKey(ctx context.Context, key string, opt ReplayOptions) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	chain := l.chains[key]
	if len(chain) == 0 {
		return "", ErrUnknownKey
	}
	return l.replay(ctx, chain[len(chain)-1], opt)
}

// replay 校验三条前置条件后创建新 execution。调用方持锁。
func (l *Layer) replay(ctx context.Context, target string, opt ReplayOptions) (string, error) {
	// 前置③:链不分叉——目标必须尚未被重放过。
	if l.replayed[target] {
		return "", fmt.Errorf("%w (target %s)", ErrAlreadyReplayed, target)
	}
	t, err := l.g.Get(ctx, target)
	if err != nil {
		return "", err
	}
	// 前置①:目标已终态。
	if !t.Status.IsFinal() {
		return "", fmt.Errorf("%w (target %s is %s)", ErrNotFinal, target, t.Status)
	}
	// completed 的重放必须显式允许(评审确认 #2)。
	if t.Status == taskgate.StatusCompleted && !opt.AllowCompleted {
		return "", fmt.Errorf("%w (target %s)", ErrCompletedNotAllowed, target)
	}
	// 前置②:键下无非终态 execution。
	key := l.keyOf[target]
	if key != "" {
		for _, id := range l.chains[key] {
			et, err := l.g.Get(ctx, id)
			if err != nil {
				return "", err
			}
			if !et.Status.IsFinal() {
				return "", fmt.Errorf("%w (key %q, execution %s is %s)", ErrInFlight, key, id, et.Status)
			}
		}
	}
	payload := opt.Payload
	if payload == nil {
		payload = t.Payload
	}
	newID, err := l.g.Submit(ctx, t.Type, payload, taskgate.MaxRetry(t.MaxRetry))
	if err != nil {
		return "", err
	}
	l.record(newID, key, target)
	l.replayed[target] = true
	return newID, nil
}

// record 登记一次新 execution。调用方持锁。
func (l *Layer) record(id, key, replayOf string) {
	l.known[id] = true
	if key != "" {
		l.chains[key] = append(l.chains[key], id)
		l.keyOf[id] = key
	}
	if replayOf != "" {
		l.replayOf[id] = replayOf
	}
}

// History 返回该 BusinessKey 下的执行历史链(创建顺序)。
func (l *Layer) History(key string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.chains[key]))
	copy(out, l.chains[key])
	return out
}

// ReplayOf 返回该 execution 的重放来源,没有则 ok=false。
func (l *Layer) ReplayOf(execID string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	src, ok := l.replayOf[execID]
	return src, ok
}
