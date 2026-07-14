package taskgate

import "time"

// 本文件只在测试编译时生效,给根包的外部测试(package taskgate_test)开测试专用入口,
// 不污染正式 API。

// NewWithClock 测试注入口:用指定时钟构造 Gate(fakeclock 或真时钟都行)。
func NewWithClock(cfg Config, clk Clock) (*Gate, error) {
	return newGate(cfg, clk)
}

// SetBackoff 测试注入口:替换业务失败的退避函数,
// 让重试链路测试可以用毫秒级短退避,不用真等指数曲线。
func SetBackoff(g *Gate, fn func(n int) time.Duration) {
	g.backoff = fn
}
