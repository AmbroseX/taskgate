package taskgate

import (
	"errors"
	"fmt"
	"time"
)

// 哨兵错误:全部导出,调用方用 errors.Is 判断。
var (
	// ErrTaskExists 同 ID 任务已存在(自定义 ID 幂等去重时会碰到)。
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
)

// ErrThrottled handler 返回它表示"被网关限流了,过 RetryAfter 再来":
// 不占 Attempts,只涨 Throttled 计数,封顶(默认 100)才进 failed。
type ErrThrottled struct {
	RetryAfter time.Duration
}

// Error 实现 error 接口。
func (e ErrThrottled) Error() string {
	return fmt.Sprintf("taskgate: throttled, retry after %v", e.RetryAfter)
}

// ErrSkipRetry handler 返回它表示"这个错没救,别重试了",任务直接进 failed。
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
