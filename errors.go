package taskgate

import (
	"errors"
	"fmt"
	"time"
)

// 哨兵错误:全部导出,调用方用 errors.Is 判断。
var (
	// ErrTaskExists 任务已存在:带 BusinessKey 提交撞键(此时错误可 errors.As 出
	// *TaskExistsError 拿链尾信息),或预置 ID 撞主键(测试/嵌入入口的存储防御)。
	ErrTaskExists = errors.New("taskgate: task already exists")
	// ErrTaskNotFound 任务不存在(Get/Cancel 找不到,或 Enqueue 时父任务缺失)。
	ErrTaskNotFound = errors.New("taskgate: task not found")
	// ErrLeaseLost 租约令牌不匹配:任务已被回收或被别人重新认领,结果作废。
	ErrLeaseLost = errors.New("taskgate: lease lost")
	// ErrTaskCanceled Heartbeat 发现任务被打了取消标记,scheduler 该 cancel handler 的 ctx 了。
	ErrTaskCanceled = errors.New("taskgate: task canceled")
	// ErrAlreadyFinal 对已经进终态的任务再 Cancel。
	ErrAlreadyFinal = errors.New("taskgate: task already in final state")
	// ErrUnknownType Run 时遇到没注册 handler 的任务类型(Submit 不校验)。
	ErrUnknownType = errors.New("taskgate: no handler registered for task type")
	// ErrShutdown Gate 已经 Shutdown,拒绝新提交。
	ErrShutdown = errors.New("taskgate: gate is shut down")
	// ErrNoTask 在任务 handler 之外的 ctx 上调 RenewLease:ctx 里没有续租闭包。
	ErrNoTask = errors.New("taskgate: no task associated with context")
	// ErrReplayNotFinal Replay 的目标执行还没进终态(只有终态执行可被重放)。
	ErrReplayNotFinal = errors.New("taskgate: replay target not in final state")
	// ErrAlreadyReplayed Replay 的目标已被重放过:历史链不分叉,每个执行至多被重放一次,
	// 重放只能打在链尾(最新执行)上。
	ErrAlreadyReplayed = errors.New("taskgate: execution already replayed (chain must not fork)")
	// ErrCompletedNotAllowed 重放 completed 的执行必须显式带 AllowCompleted() 选项,
	// 防止误触发重复计费。
	ErrCompletedNotAllowed = errors.New("taskgate: replaying a completed execution requires AllowCompleted")
)

// TaskExistsError 带 BusinessKey 提交撞键时的错误:errors.Is(err, ErrTaskExists)
// 照常成立,同时携带键下链尾(最新执行)的身份与状态,调用方据此直接决定要不要
// Replay,不必再按键查询绕一圈。errors.As 按 *TaskExistsError 匹配。
type TaskExistsError struct {
	BusinessKey string // 撞的键
	ExecutionID string // 键下链尾执行的 ID
	Status      Status // 链尾执行的状态
}

// Error 实现 error 接口。
func (e *TaskExistsError) Error() string {
	return fmt.Sprintf("taskgate: business key %q already has executions (latest %s is %s)",
		e.BusinessKey, e.ExecutionID, e.Status)
}

// Unwrap 让 errors.Is(err, ErrTaskExists) 保持成立,存量判错代码零改动。
func (e *TaskExistsError) Unwrap() error {
	return ErrTaskExists
}

// ErrThrottled handler 返回它表示"被网关限流了,过 RetryAfter 再来":
// 不占 Attempts,只涨 Throttled 计数,封顶(默认 100)才进 failed。
// 必须按值返回(errors.As 按值匹配),不要返回其指针。
type ErrThrottled struct {
	RetryAfter time.Duration
}

// Error 实现 error 接口。
func (e ErrThrottled) Error() string {
	return fmt.Sprintf("taskgate: throttled, retry after %v", e.RetryAfter)
}

// ErrSkipRetry handler 返回它表示"这个错没救,别重试了",任务直接进 failed。
// 必须按值返回(errors.As 按值匹配),不要返回其指针。
type ErrSkipRetry struct {
	Err error
}

// Error 实现 error 接口。
func (e ErrSkipRetry) Error() string {
	return fmt.Sprintf("taskgate: skip retry: %v", e.Err)
}

// Unwrap 让 errors.Is/As 能穿透到里面包的业务错误。
func (e ErrSkipRetry) Unwrap() error {
	return e.Err
}
