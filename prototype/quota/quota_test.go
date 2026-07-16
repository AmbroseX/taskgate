// 单进程单元测试:验证预留/耗尽/退还三条原子语句的形态
// (模型问题清单 #1 关心的就是这几条语句本身)。
package quota

import (
	"context"
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T, periodSec int64, limit int) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "quota.db"), periodSec, limit, 5000)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestReserveExhaustRelease 长窗口内:预留 limit 次全成功且同窗口,
// 第 limit+1 次耗尽(非错误),退还一份后又能预留。
func TestReserveExhaustRelease(t *testing.T) {
	const limit = 3
	st := openTestStore(t, 3600, limit) // 1 小时窗口,测试期内绝不切窗
	ctx := context.Background()

	var win int64
	for i := range limit {
		w, ok, err := st.Reserve(ctx, "gw")
		if err != nil || !ok {
			t.Fatalf("第 %d 次 Reserve = (%v,%v), want 成功", i+1, ok, err)
		}
		if i > 0 && w != win {
			t.Fatalf("同一测试期内窗口漂移:%d → %d", win, w)
		}
		win = w
	}
	if win%3600 != 0 {
		t.Fatalf("窗口起点 %d 没对齐 period", win)
	}
	// 耗尽:ok=false 且无错——"耗尽不是错误"(模型第 3 节)。
	if _, ok, err := st.Reserve(ctx, "gw"); ok || err != nil {
		t.Fatalf("耗尽后 Reserve = (%v,%v), want (false,nil)", ok, err)
	}
	// 退还一份 → 额度回来一份,且只回来一份。
	if err := st.Release(ctx, "gw", win); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, ok, err := st.Reserve(ctx, "gw"); !ok || err != nil {
		t.Fatalf("退还后 Reserve = (%v,%v), want 成功", ok, err)
	}
	if _, ok, _ := st.Reserve(ctx, "gw"); ok {
		t.Fatal("退还一份却预留出两份")
	}
	// 不同 quota key 互不占额。
	if _, ok, err := st.Reserve(ctx, "other"); !ok || err != nil {
		t.Fatalf("另一个 key 的 Reserve = (%v,%v), want 成功", ok, err)
	}
}

// TestReleaseOldWindowHarmless 对已经切走的旧窗口退还落空且无害
// (模型问题清单 #1:退还与窗口切换赛跑 = 无害)。
func TestReleaseOldWindowHarmless(t *testing.T) {
	st := openTestStore(t, 3600, 1)
	ctx := context.Background()
	if err := st.Release(ctx, "gw", 42); err != nil { // 42 是个不存在的旧窗口
		t.Fatalf("旧窗口 Release 应无害,got %v", err)
	}
	if _, ok, err := st.Reserve(ctx, "gw"); !ok || err != nil {
		t.Fatalf("Reserve = (%v,%v), want 成功", ok, err)
	}
	// 旧窗口的落空退还不能给当前窗口凭空加额度。
	if _, ok, _ := st.Reserve(ctx, "gw"); ok {
		t.Fatal("limit=1 却预留出第二份")
	}
}
