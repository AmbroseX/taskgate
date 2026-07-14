package taskgate

import "fmt"

// 本文件是依赖关系的"决策纯函数"层:不做任何 IO、不加锁、不碰存储。
// memory 后端在锁临界区内、sqlite 后端在事务内调用同一套函数,
// 这样"提交时初始状态""父到终态后子怎么办"两类判定在所有后端语义完全一致,
// 不会出现 memory 唤醒了、sqlite 却没唤醒这种分叉。

// ParentState 做决策需要的父任务快照:只要 ID(拼错误文案用)和状态。
type ParentState struct {
	ID     string
	Status Status
}

// SubmitDecision 提交(Enqueue)时的初始状态判定结果。
type SubmitDecision struct {
	// Status 只会是三者之一:pending(可直接排队)、blocked(等父)、canceled(父已失败且 FailFast)。
	Status Status
	// PendingParents 仅在 Status==blocked 时有意义:还没到终态的父任务数(同 ID 去重后)。
	PendingParents int
	// LastError 仅在 Status==canceled 时有意义:取消原因,如 "parent <id> failed"。
	LastError string
}

// DecideOnSubmit 判定一个带依赖的任务在提交那一刻应该落成什么初始状态。
// 规则(照 broker-contract.md 的 Enqueue 合同):
//   - FailFast 策略下,只要有任何一个父已经 failed/canceled → 直接 canceled。
//     哪怕其它父还没跑完也立即取消:这个子任务已经注定跑不成,等下去没有意义。
//   - IgnoreParentFail 策略下,父只要进了终态(哪怕失败)就算"满足"。
//   - 还有父没到终态 → blocked,并记下未完成父的数量(pending_parents)。
//   - 父全部满足 → pending,可以直接排队。
//
// 同一个父 ID 写了多遍只算一个:否则 pending_parents 会多计,父完成一次只减一次,
// 子任务就永远唤不醒了。调用方(后端)拿到 PendingParents 后按这个数落库。
func DecideOnSubmit(parents []ParentState, policy ParentFailurePolicy) SubmitDecision {
	seen := make(map[string]bool, len(parents))
	notFinal := 0
	for _, p := range parents {
		if seen[p.ID] {
			continue
		}
		seen[p.ID] = true
		if p.Status == StatusFailed || p.Status == StatusCanceled {
			if policy != IgnoreParentFail {
				return SubmitDecision{
					Status:    StatusCanceled,
					LastError: ParentFailureReason(p.ID, p.Status),
				}
			}
			continue // IgnoreParentFail:失败/取消也算满足,不计入未完成数
		}
		if !p.Status.IsFinal() {
			notFinal++
		}
	}
	if notFinal > 0 {
		return SubmitDecision{Status: StatusBlocked, PendingParents: notFinal}
	}
	return SubmitDecision{Status: StatusPending}
}

// ChildAction 父任务到终态之后,对一个直接子任务要执行的动作。
type ChildAction int

const (
	// ChildNone 什么都不做:计数减了但还没减到 0,或者子任务已经在终态。
	ChildNone ChildAction = iota
	// ChildWake 唤醒:子任务 blocked → pending(父全部满足了)。
	ChildWake
	// ChildCancel 连锁取消:子任务应流转到 canceled(FailFast 且父失败/取消)。
	// 调用方仍需过 canTransition 校验;若子在 running 等特殊状态,由调用方自行防御处理。
	ChildCancel
)

// DecideOnParentFinal 判定"父任务进入终态 parentStatus 之后,对一个子任务怎么办"。
// pendingParents 传入子任务当前剩余的未终态父计数(调用方在锁/事务内读出),
// 返回新的计数(递减不为负)与动作。约定:
//   - 子任务已是终态(比如已被手动取消)→ 不动。
//   - 父是 completed,或策略是 IgnoreParentFail(此时父任何终态都算满足)→ 计数减一;
//     减到 0 且子还在 blocked → 唤醒。
//   - 父是 failed/canceled 且策略是 FailFast → 连锁取消(计数保持原样,反正任务要没了)。
func DecideOnParentFinal(parentStatus, childStatus Status, policy ParentFailurePolicy, pendingParents int) (int, ChildAction) {
	if childStatus.IsFinal() {
		return pendingParents, ChildNone
	}
	if !parentStatus.IsFinal() {
		// 防御:父没到终态不该调这个函数,当无事发生。
		return pendingParents, ChildNone
	}
	satisfied := parentStatus == StatusCompleted || policy == IgnoreParentFail
	if !satisfied {
		return pendingParents, ChildCancel
	}
	if pendingParents > 0 {
		pendingParents-- // 递减不为负:重复触发或计数已修复时不会减穿
	}
	if pendingParents == 0 && childStatus == StatusBlocked {
		return 0, ChildWake
	}
	return pendingParents, ChildNone
}

// CanTransition 导出状态机校验,给 memorybroker/sqlitebroker 等后端包在
// 每次写状态前做统一防御(合同要求:所有写入先过 canTransition 表)。
// Phase 1 只定义了包内的 canTransition,后端在别的包里够不着,这里补一个只读出口。
func CanTransition(from, to Status) bool {
	return canTransition(from, to)
}

// ParentFailureReason 连锁取消时写进子任务 LastError 的固定文案。
// 固定下来是为了 brokertest 能逐字断言,三个后端文案一致。
func ParentFailureReason(parentID string, parentStatus Status) string {
	switch parentStatus {
	case StatusCanceled:
		return fmt.Sprintf("parent %s canceled", parentID)
	default:
		return fmt.Sprintf("parent %s failed", parentID)
	}
}
