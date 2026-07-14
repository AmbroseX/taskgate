package taskgate

import (
	"math/rand"
	"testing"
	"time"
)

// TestBackoffCurve 退避曲线:每档基础值 2^n×1s(10min 封顶),抖动落在 ±20% 范围内。
// 固定种子注入,结果完全确定,不靠运气。
func TestBackoffCurve(t *testing.T) {
	bo := newBackoffFunc(rand.NewSource(42))

	cases := []struct {
		n    int
		base time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{5, 32 * time.Second},
		{9, 512 * time.Second},
		{10, 10 * time.Minute},  // 1024s > 600s,封顶
		{20, 10 * time.Minute},  // 深度封顶
		{100, 10 * time.Minute}, // 大 n 不溢出
		{-1, 10 * time.Minute},  // 防御:非法负数按封顶给,不 panic
	}
	for _, c := range cases {
		got := bo(c.n)
		lo := time.Duration(float64(c.base) * (1 - backoffJitter))
		hi := time.Duration(float64(c.base) * (1 + backoffJitter))
		if got < lo || got > hi {
			t.Errorf("backoff(%d) = %v,应落在 [%v, %v]", c.n, got, lo, hi)
		}
	}
}

// TestBackoffJitterSpread 抖动确实在起作用:同一档连抽多次,不该每次都一样,
// 且必须始终在 ±20% 区间内。
func TestBackoffJitterSpread(t *testing.T) {
	bo := newBackoffFunc(rand.NewSource(7))
	const n = 3 // 基础值 8s
	base := 8 * time.Second
	lo := time.Duration(float64(base) * (1 - backoffJitter))
	hi := time.Duration(float64(base) * (1 + backoffJitter))

	seen := make(map[time.Duration]bool)
	for i := 0; i < 50; i++ {
		got := bo(n)
		if got < lo || got > hi {
			t.Fatalf("第 %d 次 backoff(3) = %v 越界 [%v, %v]", i, got, lo, hi)
		}
		seen[got] = true
	}
	if len(seen) < 10 {
		t.Fatalf("50 次抽样只有 %d 种取值,抖动疑似没生效", len(seen))
	}
}

// TestBackoffDeterministic 同一个种子两次构造,序列逐项一致——rand 源注入是可复现的。
func TestBackoffDeterministic(t *testing.T) {
	a := newBackoffFunc(rand.NewSource(99))
	b := newBackoffFunc(rand.NewSource(99))
	for i := 0; i < 20; i++ {
		if x, y := a(i%6), b(i%6); x != y {
			t.Fatalf("第 %d 项不一致: %v vs %v", i, x, y)
		}
	}
}
