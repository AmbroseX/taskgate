package taskgate

import (
	"math/rand"
	"sync"
	"time"
)

// 退避曲线参数:backoff(n) = min(2^n × 1s, 10min) ± 20% 抖动(FR-011)。
const (
	backoffBase   = 1 * time.Second
	backoffCap    = 10 * time.Minute
	backoffJitter = 0.2
)

// newBackoffFunc 构造退避函数,n 传当前 Attempts(失败前的快照,首次失败传 0)。
// rand 源可注入:传 nil 用时间种子(生产);测试传固定种子拿确定的抖动序列。
// rand.Rand 不是并发安全的,多个 worker 会同时算退避,这里用小锁护住。
func newBackoffFunc(src rand.Source) func(n int) time.Duration {
	if src == nil {
		// time.Now() 在这里仅作随机种子,不参与任何时序逻辑,
		// 不走注入 clock 属有意豁免。
		src = rand.NewSource(time.Now().UnixNano())
	}
	rng := rand.New(src)
	var mu sync.Mutex
	return func(n int) time.Duration {
		// 基础值:2^n × 1s,封顶 10min。n≥10 时 2^10s=1024s 已超封顶,
		// 直接取封顶,顺便避开大位移溢出。
		d := backoffCap
		if n >= 0 && n < 10 {
			if v := backoffBase << uint(n); v < backoffCap {
				d = v
			}
		}
		// 抖动:乘上 [1-20%, 1+20%) 的随机系数,把同一批失败任务的重试时刻打散。
		mu.Lock()
		factor := 1 + backoffJitter*(2*rng.Float64()-1)
		mu.Unlock()
		return time.Duration(float64(d) * factor)
	}
}
