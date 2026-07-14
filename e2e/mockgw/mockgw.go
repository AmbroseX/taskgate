// Package mockgw 是一个可注入故障的 mock LLM/OCR 网关,只给 e2e 测试用,不属于库的公开 API。
// 零新依赖:只用标准库(net/http/httptest + math/rand 固定种子)。
//
// 把生产环境踩过的坑做成开关:
//   - Latency:每个正常请求固定延迟(模拟网关真实处理耗时,也用来撑起可观测的并发);
//   - BusyAfterConcurrency:并发超过 n 时返回 HTTP 200、但 body 里藏 busy 错误事件
//     (复刻"错误藏在 200 的 SSE 体里"的真实网关行为——HTTP 状态码骗人);
//   - FailRate:固定种子的随机 HTTP 500(同种子同序列,CI 可复现);
//   - CrashAfterConcurrency:并发超过 n 时直接掐断 TCP 连接(复刻并发打爆后断连的网关);
//   - BusyFirstN:前 n 个请求定向返回 busy 体,与并发无关(单独复刻"SSE 藏错误"场景)。
//
// 响应体约定(见 specs/003-m3-polish/data-model.md 第 5 节):
//   - 正常:HTTP 200,body `data: {"ok":true}`(请求带 body 时回显成
//     `data: {"ok":true,"echo":<请求体>}`,要求请求体是合法 JSON);
//   - busy:HTTP 200,body `data: {"error":"busy"}`;
//   - 随机失败:HTTP 500;
//   - 断连:不写任何响应,直接关连接,客户端看到 EOF/连接重置。
package mockgw

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"
)

// Gateway 一个跑在 httptest.Server 上的 mock 网关,带原子的并发观测口。
type Gateway struct {
	srv *httptest.Server

	// 故障开关(New 之后只读,不需要锁)。
	latency    time.Duration // 正常请求的处理延迟
	busyAfter  int           // 并发 > n 返回 busy 体;0 = 关
	crashAfter int           // 并发 > n 直接断连;0 = 关
	busyFirstN int           // 前 n 个请求定向返回 busy 体;0 = 关
	failRate   float64       // p 概率返回 500;0 = 关

	// rng FailRate 的固定种子随机源。rand.Rand 不是并发安全的,必须加锁。
	rngMu sync.Mutex
	rng   *rand.Rand

	// 观测计数:全部原子,竞态下不许少算。
	inflight atomic.Int64 // 当前在处理的请求数(进 +1 出 -1)
	maxConc  atomic.Int64 // 观测到的最大并发(CAS 更新峰值)
	busy     atomic.Int64 // busy 响应次数
	crashes  atomic.Int64 // 断连次数
	requests atomic.Int64 // 总请求数
}

// Option 网关的故障开关。
type Option func(*Gateway)

// Latency 每个正常请求固定延迟 d(busy/断连是快速拒绝,不吃延迟)。
func Latency(d time.Duration) Option {
	return func(g *Gateway) { g.latency = d }
}

// BusyAfterConcurrency 并发超过 n 时返回 HTTP 200 + body 里的 busy 错误事件。
func BusyAfterConcurrency(n int) Option {
	return func(g *Gateway) { g.busyAfter = n }
}

// FailRate 以概率 p 返回 HTTP 500,随机源用固定种子 seed(同种子同序列,CI 确定)。
func FailRate(p float64, seed int64) Option {
	return func(g *Gateway) {
		g.failRate = p
		g.rng = rand.New(rand.NewSource(seed))
	}
}

// CrashAfterConcurrency 并发超过 n 时不写任何响应、直接关掉 TCP 连接。
func CrashAfterConcurrency(n int) Option {
	return func(g *Gateway) { g.crashAfter = n }
}

// BusyFirstN 前 n 个请求(按到达顺序)定向返回 busy 体,与并发无关。
// 专门给"SSE 藏错误 → handler 判定 ErrThrottled → 重排后成功"这条路径用。
func BusyFirstN(n int) Option {
	return func(g *Gateway) { g.busyFirstN = n }
}

// New 起一个 mock 网关(内部是 httptest.Server),用完必须 Close。
func New(opts ...Option) *Gateway {
	g := &Gateway{}
	for _, opt := range opts {
		opt(g)
	}
	g.srv = httptest.NewServer(http.HandlerFunc(g.handle))
	return g
}

// URL 网关的基地址(http://127.0.0.1:端口)。
func (g *Gateway) URL() string { return g.srv.URL }

// Close 关掉网关,等在处理的请求收尾。
func (g *Gateway) Close() { g.srv.Close() }

// MaxConcurrency 观测到的最大并发(原子峰值,竞态下不少算)。
func (g *Gateway) MaxConcurrency() int { return int(g.maxConc.Load()) }

// BusyCount busy 响应的总次数(含并发触发与 BusyFirstN 定向触发)。
func (g *Gateway) BusyCount() int { return int(g.busy.Load()) }

// CrashCount 断连的总次数。
func (g *Gateway) CrashCount() int { return int(g.crashes.Load()) }

// Requests 收到的总请求数(含 busy/500/断连)。
func (g *Gateway) Requests() int { return int(g.requests.Load()) }

// observePeak 用 CAS 把 cur 打进峰值:并发抬升的瞬间谁都可能来更新,输了就重读重试。
func (g *Gateway) observePeak(cur int64) {
	for {
		old := g.maxConc.Load()
		if cur <= old || g.maxConc.CompareAndSwap(old, cur) {
			return
		}
	}
}

// handle 请求处理主体。判定顺序:断连 → busy → 随机 500 → 延迟 → 正常响应。
// 断连/busy/500 是网关的快速拒绝(真实网关过载时立刻拒),只有正常请求吃 Latency。
func (g *Gateway) handle(w http.ResponseWriter, r *http.Request) {
	seq := g.requests.Add(1)
	cur := g.inflight.Add(1)
	defer g.inflight.Add(-1)
	g.observePeak(cur)

	// 断连:不写任何字节,直接关掉底层连接,客户端只能看到 EOF/连接重置。
	if g.crashAfter > 0 && cur > int64(g.crashAfter) {
		g.crashes.Add(1)
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
		return
	}

	// busy:HTTP 200 但错误藏在 SSE 体里(并发超限 或 前 N 个定向)。
	if (g.busyAfter > 0 && cur > int64(g.busyAfter)) ||
		(g.busyFirstN > 0 && seq <= int64(g.busyFirstN)) {
		g.busy.Add(1)
		writeSSE(w, `{"error":"busy"}`)
		return
	}

	// 随机 500:固定种子的序列,取数加锁保并发安全。
	if g.failRate > 0 {
		g.rngMu.Lock()
		fail := g.rng.Float64() < g.failRate
		g.rngMu.Unlock()
		if fail {
			http.Error(w, "mockgw: injected 500", http.StatusInternalServerError)
			return
		}
	}

	// 正常路径:睡出处理耗时,再回 ok 事件(带 body 就回显)。
	if g.latency > 0 {
		time.Sleep(g.latency)
	}
	body, _ := io.ReadAll(r.Body)
	if len(body) == 0 {
		writeSSE(w, `{"ok":true}`)
		return
	}
	writeSSE(w, `{"ok":true,"echo":`+string(body)+`}`)
}

// writeSSE 按 SSE 形态写一条 data 事件(HTTP 200)。
func writeSSE(w http.ResponseWriter, payload string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
}
