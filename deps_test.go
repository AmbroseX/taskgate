package taskgate

import "testing"

// TestDecideOnSubmit 提交时初始状态判定的表驱动全覆盖。
func TestDecideOnSubmit(t *testing.T) {
	ps := func(id string, s Status) ParentState { return ParentState{ID: id, Status: s} }

	cases := []struct {
		name    string
		parents []ParentState
		policy  ParentFailurePolicy
		want    SubmitDecision
	}{
		{"无父任务→pending", nil, FailFast,
			SubmitDecision{Status: StatusPending}},
		{"父全completed→pending", []ParentState{ps("a", StatusCompleted), ps("b", StatusCompleted)}, FailFast,
			SubmitDecision{Status: StatusPending}},
		{"有running父→blocked计1", []ParentState{ps("a", StatusCompleted), ps("b", StatusRunning)}, FailFast,
			SubmitDecision{Status: StatusBlocked, PendingParents: 1}},
		{"两个未终态父→blocked计2", []ParentState{ps("a", StatusPending), ps("b", StatusRetrying)}, FailFast,
			SubmitDecision{Status: StatusBlocked, PendingParents: 2}},
		{"blocked父也算未终态", []ParentState{ps("a", StatusBlocked)}, FailFast,
			SubmitDecision{Status: StatusBlocked, PendingParents: 1}},
		{"父failed且FailFast→canceled", []ParentState{ps("a", StatusFailed)}, FailFast,
			SubmitDecision{Status: StatusCanceled, LastError: "parent a failed"}},
		{"父canceled且FailFast→canceled", []ParentState{ps("a", StatusCanceled)}, FailFast,
			SubmitDecision{Status: StatusCanceled, LastError: "parent a canceled"}},
		// FailFast 下哪怕还有别的父没跑完,只要有一个父败了就立即取消:任务已注定跑不成。
		{"failed父+running父且FailFast→立即canceled", []ParentState{ps("a", StatusRunning), ps("b", StatusFailed)}, FailFast,
			SubmitDecision{Status: StatusCanceled, LastError: "parent b failed"}},
		{"父failed但Ignore→pending", []ParentState{ps("a", StatusFailed)}, IgnoreParentFail,
			SubmitDecision{Status: StatusPending}},
		{"父canceled但Ignore→pending", []ParentState{ps("a", StatusCanceled)}, IgnoreParentFail,
			SubmitDecision{Status: StatusPending}},
		{"Ignore下failed父+running父→blocked计1", []ParentState{ps("a", StatusFailed), ps("b", StatusRunning)}, IgnoreParentFail,
			SubmitDecision{Status: StatusBlocked, PendingParents: 1}},
		{"Ignore下completed+failed混合→pending", []ParentState{ps("a", StatusCompleted), ps("b", StatusFailed)}, IgnoreParentFail,
			SubmitDecision{Status: StatusPending}},
		// 同一个父写两遍只算一个,否则 pending_parents 多计、永远唤不醒。
		{"重复父ID去重", []ParentState{ps("a", StatusRunning), ps("a", StatusRunning)}, FailFast,
			SubmitDecision{Status: StatusBlocked, PendingParents: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DecideOnSubmit(tc.parents, tc.policy)
			if got != tc.want {
				t.Fatalf("DecideOnSubmit(%v, %s) = %+v, 期望 %+v", tc.parents, tc.policy, got, tc.want)
			}
		})
	}
}

// TestDecideOnParentFinal 父到终态后对单个子任务的动作判定。
func TestDecideOnParentFinal(t *testing.T) {
	cases := []struct {
		name        string
		parent      Status
		child       Status
		policy      ParentFailurePolicy
		pending     int
		wantPending int
		wantAction  ChildAction
	}{
		{"父completed减计数未到0→不动", StatusCompleted, StatusBlocked, FailFast, 2, 1, ChildNone},
		{"父completed减到0且blocked→唤醒", StatusCompleted, StatusBlocked, FailFast, 1, 0, ChildWake},
		{"计数已是0不减穿且blocked→唤醒", StatusCompleted, StatusBlocked, FailFast, 0, 0, ChildWake},
		{"父failed且FailFast→连锁取消", StatusFailed, StatusBlocked, FailFast, 1, 1, ChildCancel},
		{"父canceled且FailFast→连锁取消", StatusCanceled, StatusBlocked, FailFast, 2, 2, ChildCancel},
		{"父failed但Ignore→当满足减到0唤醒", StatusFailed, StatusBlocked, IgnoreParentFail, 1, 0, ChildWake},
		{"父canceled但Ignore计数未到0→不动", StatusCanceled, StatusBlocked, IgnoreParentFail, 2, 1, ChildNone},
		{"子已终态(canceled)→不动", StatusCompleted, StatusCanceled, FailFast, 1, 1, ChildNone},
		{"子已终态(completed)→不动", StatusFailed, StatusCompleted, FailFast, 0, 0, ChildNone},
		// 子不在 blocked(比如防御修复场景下已是 pending):减计数但不重复唤醒。
		{"子已pending减到0→不动", StatusCompleted, StatusPending, FailFast, 1, 0, ChildNone},
		// 防御:父没到终态不该被调,当无事发生。
		{"父未终态→不动", StatusRunning, StatusBlocked, FailFast, 1, 1, ChildNone},
		// 子在 running(Ignore 策略先被别的父唤醒后又有父失败)→ FailFast 不适用于它,
		// 但纯函数仍然给出 Cancel 意图,由后端按 canTransition 防御处理。
		{"running子遇FailFast父失败→Cancel意图", StatusFailed, StatusRunning, FailFast, 0, 0, ChildCancel},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotPending, gotAction := DecideOnParentFinal(tc.parent, tc.child, tc.policy, tc.pending)
			if gotPending != tc.wantPending || gotAction != tc.wantAction {
				t.Fatalf("DecideOnParentFinal(parent=%s, child=%s, policy=%s, pending=%d) = (%d, %d), 期望 (%d, %d)",
					tc.parent, tc.child, tc.policy, tc.pending, gotPending, gotAction, tc.wantPending, tc.wantAction)
			}
		})
	}
}

// TestParentFailureReason 连锁取消文案固定,brokertest 会逐字断言。
func TestParentFailureReason(t *testing.T) {
	if got := ParentFailureReason("p1", StatusFailed); got != "parent p1 failed" {
		t.Fatalf("failed 文案 = %q, 期望 %q", got, "parent p1 failed")
	}
	if got := ParentFailureReason("p1", StatusCanceled); got != "parent p1 canceled" {
		t.Fatalf("canceled 文案 = %q, 期望 %q", got, "parent p1 canceled")
	}
}
