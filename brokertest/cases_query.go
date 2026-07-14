package brokertest

// 契约 13~14、18:Counts/QueueLen 统计一致性、List 过滤、List 分页。

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ambrose/taskgate"
)

// 契约 13 CountsConsistency:铺满七种状态后,Counts 必须与逐个 Get(经 List)汇总一致;
// QueueLen = 队列内 pending+retrying 的数量。
func caseCountsConsistency(t *testing.T, h *harness) {
	ctx := context.Background()

	// 队列 qa(type=llm):completed / failed / pending 各一。
	h.enqueue(t, task("c1", "llm", "qa"))
	h.ackByID(t, "qa", "c1")
	h.enqueue(t, task("f1", "llm", "qa"))
	h.failByID(t, "qa", "f1")
	h.enqueue(t, task("p1", "llm", "qa"))

	// 队列 qb(type=ocr):canceled / running / retrying 各一;外加一个 blocked(type=llm)。
	h.enqueue(t, task("x1", "ocr", "qb"))
	if err := h.b.Cancel(ctx, "x1"); err != nil {
		t.Fatalf("预置 Cancel 失败: %v", err)
	}
	h.enqueue(t, task("r1", "ocr", "qb"))
	running := h.dequeue(t, "qb")
	if running.ID != "r1" {
		t.Fatalf("预期认领到 r1,实际 %s", running.ID)
	}
	rt := task("rt1", "ocr", "qb")
	rt.MaxRetry = 3
	h.enqueue(t, rt)
	claimed := h.dequeue(t, "qb")
	if err := h.b.Fail(ctx, claimed.ID, claimed.LeaseToken, "x", taskgate.FailBusiness, h.now().Add(time.Hour)); err != nil {
		t.Fatalf("预置 Fail 失败: %v", err)
	}
	h.enqueue(t, dependsOn("b1", "llm", "qb", "r1")) // 父 r1 在跑,子 blocked

	// 七种状态是否都铺到了,先自检一遍,防止预置错了白测。
	wantStatus := map[string]taskgate.Status{
		"c1": taskgate.StatusCompleted, "f1": taskgate.StatusFailed, "p1": taskgate.StatusPending,
		"x1": taskgate.StatusCanceled, "r1": taskgate.StatusRunning, "rt1": taskgate.StatusRetrying,
		"b1": taskgate.StatusBlocked,
	}
	for id, want := range wantStatus {
		h.mustStatus(t, id, want)
	}

	// Counts 必须与 List 全量汇总一致。
	counts, err := h.b.Counts(ctx)
	if err != nil {
		t.Fatalf("Counts 失败: %v", err)
	}
	all, err := h.b.List(ctx, taskgate.Filter{})
	if err != nil {
		t.Fatalf("List(全量) 失败: %v", err)
	}
	if len(all) != len(wantStatus) {
		t.Fatalf("List(全量) 应返回 %d 条,实际 %d 条", len(wantStatus), len(all))
	}
	agg := map[string]map[taskgate.Status]int64{}
	for _, tk := range all {
		if agg[tk.Type] == nil {
			agg[tk.Type] = map[taskgate.Status]int64{}
		}
		agg[tk.Type][tk.Status]++
	}
	for typ, byStatus := range agg {
		for st, n := range byStatus {
			if counts[typ][st] != n {
				t.Fatalf("Counts[%s][%s]=%d 与逐个汇总 %d 不一致(完整 Counts=%v)", typ, st, counts[typ][st], n, counts)
			}
		}
	}
	for typ, byStatus := range counts {
		for st, n := range byStatus {
			if n != 0 && agg[typ][st] != n {
				t.Fatalf("Counts 多出了不存在的计数 [%s][%s]=%d(汇总=%v)", typ, st, n, agg)
			}
		}
	}

	// QueueLen:qa 只有 p1 排队;qb 只有 rt1(retrying 也算排队,blocked/running/终态不算)。
	if n, err := h.b.QueueLen(ctx, "qa"); err != nil || n != 1 {
		t.Fatalf("QueueLen(qa) 应为 1(p1),实际 n=%d err=%v", n, err)
	}
	if n, err := h.b.QueueLen(ctx, "qb"); err != nil || n != 1 {
		t.Fatalf("QueueLen(qb) 应为 1(rt1 retrying 算排队),实际 n=%d err=%v", n, err)
	}
	if n, err := h.b.QueueLen(ctx, "no-such-queue"); err != nil || n != 0 {
		t.Fatalf("QueueLen(不存在队列) 应为 0,实际 n=%d err=%v", n, err)
	}
}

// listIDs 按过滤条件取 ID 集合(排序后返回,方便和期望比对)。
func (h *harness) listIDs(t *testing.T, f taskgate.Filter) []string {
	t.Helper()
	got, err := h.b.List(context.Background(), f)
	if err != nil {
		t.Fatalf("List(%+v) 失败: %v", f, err)
	}
	ids := make([]string, 0, len(got))
	for _, tk := range got {
		ids = append(ids, tk.ID)
	}
	sort.Strings(ids)
	return ids
}

// equalIDs 比较两个 ID 集合(都已排序)。
func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 契约 14 ListFilter:按 Type/Queue/Status/组合/Limit 过滤正确。
func caseListFilter(t *testing.T, h *harness) {
	// 数据集:llm×qa 两条 pending、ocr×qa 一条 pending、
	// ocr×qb 一条 completed、llm×qb 一条 pending。
	h.enqueue(t, task("a1", "llm", "qa"))
	h.enqueue(t, task("a2", "llm", "qa"))
	h.enqueue(t, task("b1", "ocr", "qa"))
	h.enqueue(t, task("d1", "ocr", "qb"))
	h.ackByID(t, "qb", "d1") // 先消化 d1,再放 c1,避免认领顺序不确定
	h.enqueue(t, task("c1", "llm", "qb"))

	checks := []struct {
		name string
		f    taskgate.Filter
		want []string
	}{
		{"按Type", taskgate.Filter{Type: "llm"}, []string{"a1", "a2", "c1"}},
		{"按Queue", taskgate.Filter{Queue: "qa"}, []string{"a1", "a2", "b1"}},
		{"按Status", taskgate.Filter{Status: taskgate.StatusPending}, []string{"a1", "a2", "b1", "c1"}},
		{"Type+Queue组合", taskgate.Filter{Type: "llm", Queue: "qa"}, []string{"a1", "a2"}},
		{"Type+Status组合", taskgate.Filter{Type: "ocr", Status: taskgate.StatusCompleted}, []string{"d1"}},
		{"无匹配", taskgate.Filter{Type: "no-such-type"}, []string{}},
		{"零值过滤=全量", taskgate.Filter{}, []string{"a1", "a2", "b1", "c1", "d1"}},
	}
	for _, c := range checks {
		if got := h.listIDs(t, c.f); !equalIDs(got, c.want) {
			t.Fatalf("List[%s](%+v) 应返回 %v,实际 %v", c.name, c.f, c.want, got)
		}
	}

	// Limit:只限量,不约定返回哪几条,但每条都必须满足过滤条件。
	limited, err := h.b.List(context.Background(), taskgate.Filter{Type: "llm", Limit: 2})
	if err != nil {
		t.Fatalf("List(Limit=2) 失败: %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("List(Type=llm, Limit=2) 应恰好返回 2 条,实际 %d 条", len(limited))
	}
	for _, tk := range limited {
		if tk.Type != "llm" {
			t.Fatalf("Limit 结果里混入了不满足过滤条件的任务: %s(type=%s)", tk.ID, tk.Type)
		}
	}
}

// listOrdered 按过滤条件取 ID 序列(保留 List 的返回顺序,分页用例专用),
// 顺带断言结果本身满足 (CreatedAt, ID) 升序合同。
func (h *harness) listOrdered(t *testing.T, f taskgate.Filter) []string {
	t.Helper()
	got, err := h.b.List(context.Background(), f)
	if err != nil {
		t.Fatalf("List(%+v) 失败: %v", f, err)
	}
	ids := make([]string, 0, len(got))
	for i, tk := range got {
		if i > 0 {
			prev := got[i-1]
			ok := prev.CreatedAt.Before(tk.CreatedAt) ||
				(prev.CreatedAt.Equal(tk.CreatedAt) && prev.ID < tk.ID)
			if !ok {
				t.Fatalf("List(%+v) 第 %d/%d 条乱序:%s(created=%v) 应排在 %s(created=%v) 之前",
					f, i, i+1, tk.ID, tk.CreatedAt, prev.ID, prev.CreatedAt)
			}
		}
		ids = append(ids, tk.ID)
	}
	return ids
}

// equalOrdered 逐位比较两个 ID 序列(顺序敏感)。
func equalOrdered(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// 契约 18 ListPagination:List 的排序与分页合同(M3,broker-contract.md 修订版)。
// 25 个任务用 fakeclock 逐个 +1ms 入队保证 CreatedAt 全序;ID 故意按创建顺序**逆序**编号,
// 专抓"只按 ID 排序"的错误实现——合同要求 (CreatedAt, ID) 升序,CreatedAt 优先。
func caseListPagination(t *testing.T, h *harness) {
	const total = 25

	// created 按创建顺序记录 ID,就是全量结果的期望顺序。
	// Type 掺两种:i 为偶数 llm、奇数 ocr,给"过滤+分页组合"用。
	created := make([]string, 0, total)
	var llmIDs []string
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("z%02d", total-1-i) // z24, z23, ..., z00:ID 序 = 创建序的反序
		typ := "llm"
		if i%2 == 1 {
			typ = "ocr"
		}
		h.enqueue(t, task(id, typ, "qp"))
		created = append(created, id)
		if typ == "llm" {
			llmIDs = append(llmIDs, id)
		}
		h.advance(time.Millisecond) // 每条间隔 1ms,CreatedAt 严格递增
	}

	// 全量无分页:顺序必须恰好等于创建顺序(而不是 ID 字典序)。
	if got := h.listOrdered(t, taskgate.Filter{}); !equalOrdered(got, created) {
		t.Fatalf("List(全量) 应按 (CreatedAt,ID) 升序=创建顺序返回,期望 %v,实际 %v", created, got)
	}

	// 三页翻完:Limit=10,Offset=0/10/20 → 10/10/5 条;
	// 每页内容必须恰好是全量顺序的对应切片(页内升序 + 跨页升序 + 并集=全集无重无漏,一次盖住)。
	pages := []struct {
		offset, wantLen int
	}{{0, 10}, {10, 10}, {20, 5}}
	var union []string
	for _, p := range pages {
		got := h.listOrdered(t, taskgate.Filter{Limit: 10, Offset: p.offset})
		want := created[p.offset : p.offset+p.wantLen]
		if !equalOrdered(got, want) {
			t.Fatalf("List(Limit=10, Offset=%d) 期望 %v,实际 %v", p.offset, want, got)
		}
		union = append(union, got...)
	}
	if !equalOrdered(union, created) {
		t.Fatalf("三页并集应=全集无重无漏,期望 %v,实际 %v", created, union)
	}

	// Offset 越界:返回空列表,nil error(listOrdered 内部已断言 err=nil)。
	if got := h.listOrdered(t, taskgate.Filter{Limit: 10, Offset: 100}); len(got) != 0 {
		t.Fatalf("List(Offset=100) 越界应返回空,实际 %v", got)
	}

	// Offset=5 + Limit=0(不限):返回排序后剩余的 20 条。
	if got := h.listOrdered(t, taskgate.Filter{Offset: 5}); !equalOrdered(got, created[5:]) {
		t.Fatalf("List(Offset=5, Limit=0) 应返回剩余 20 条,期望 %v,实际 %v", created[5:], got)
	}

	// Type 过滤 + 分页组合:先过滤(llm 共 13 条)再排序再分页。
	if got := h.listOrdered(t, taskgate.Filter{Type: "llm", Limit: 5, Offset: 5}); !equalOrdered(got, llmIDs[5:10]) {
		t.Fatalf("List(Type=llm, Limit=5, Offset=5) 期望 %v,实际 %v", llmIDs[5:10], got)
	}
	// 过滤后越界:同样返回空。
	if got := h.listOrdered(t, taskgate.Filter{Type: "llm", Limit: 5, Offset: 13}); len(got) != 0 {
		t.Fatalf("List(Type=llm, Offset=13) 过滤后越界应返回空,实际 %v", got)
	}
}
