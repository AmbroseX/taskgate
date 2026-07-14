// bench_test.go 性能基准(T115,测试方案第 6 节):sqlite vs redis 的防退化基线。
// 只在 `go test -bench . -benchmem -run '^$' .` 时运行,不影响常规测试;
// 基准要真实数字,跑的时候别开 -race。三项:
//   - BenchmarkEnqueue{Sqlite,Redis}:走 Gate.Submit 的入队路径(单协程 + 32 并发两档);
//   - BenchmarkDequeueAck{Sqlite,Redis}:裸调 Broker 接口的 Enqueue→Dequeue→Ack 一整圈,
//     没有限流器/调度器(等价"限流关掉"),测的是后端框架开销;
//   - BenchmarkPipeline{Sqlite,Redis}:三级依赖链(每链 3 任务,取 b.N/3 条链)全链路
//     推进到全部 completed,ns/op 是"平均每个任务"的口径。
//
// redis 档用 miniredis(同进程内存实现),数字含 miniredis 自身开销,只作相对基线;
// 结果表回写 docs/plans/2026-07-14-测试方案.md 第 6 节。
package taskgate_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ambrose/taskgate"
	"github.com/ambrose/taskgate/redisbroker"
	"github.com/ambrose/taskgate/sqlitebroker"
)

// benchQueue 基准统一的队列名 = 任务类型名。
const benchQueue = "bench"

// newBenchSqlite 每个基准一个独立的 sqlite 文件后端。
func newBenchSqlite(b *testing.B) taskgate.Broker {
	b.Helper()
	br, err := sqlitebroker.Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatalf("打开 sqlite 后端失败: %v", err)
	}
	b.Cleanup(func() { _ = br.Close() })
	return br
}

// newBenchRedis 每个基准一个独立的 miniredis 后端(同进程,数字含 miniredis 开销)。
func newBenchRedis(b *testing.B) taskgate.Broker {
	b.Helper()
	mr := miniredis.RunT(b)
	br, err := redisbroker.New(redisbroker.Options{Addr: mr.Addr()})
	if err != nil {
		b.Fatalf("打开 redis 后端失败: %v", err)
	}
	b.Cleanup(func() { _ = br.Close() })
	return br
}

// newBenchGate 起一个 Gate(顺带完成 Broker.Init 装配),不 Handle 不 Run 时是纯生产者。
func newBenchGate(b *testing.B, br taskgate.Broker) *taskgate.Gate {
	b.Helper()
	g, err := taskgate.New(taskgate.Config{
		Broker: br,
		Queues: map[string]taskgate.QueueConfig{
			benchQueue: {Workers: 8}, // RPS=0 不限速:基准测框架开销,不测限流等待
		},
	})
	if err != nil {
		b.Fatalf("New 失败: %v", err)
	}
	return g
}

// ---- BenchmarkEnqueue:入队路径(Gate.Submit) ----

// benchEnqueue 单协程版:一个 op = 一次 Submit。
func benchEnqueue(b *testing.B, br taskgate.Broker) {
	g := newBenchGate(b, br)
	payload := json.RawMessage(`{"n":1}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.Submit(context.Background(), benchQueue, payload); err != nil {
			b.Fatalf("Submit 失败: %v", err)
		}
	}
}

// benchEnqueueParallel 32 并发版:RunParallel 的协程数 = GOMAXPROCS × parallelism,
// 用 SetParallelism 把它凑到 ≥32(取整导致的略超无伤大雅,竞争压力只多不少)。
func benchEnqueueParallel(b *testing.B, br taskgate.Broker) {
	g := newBenchGate(b, br)
	payload := json.RawMessage(`{"n":1}`)
	p := (32 + runtime.GOMAXPROCS(0) - 1) / runtime.GOMAXPROCS(0)
	if p < 1 {
		p = 1
	}
	b.SetParallelism(p)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := g.Submit(context.Background(), benchQueue, payload); err != nil {
				b.Fatalf("Submit 失败: %v", err)
			}
		}
	})
}

func BenchmarkEnqueueSqlite(b *testing.B)   { benchEnqueue(b, newBenchSqlite(b)) }
func BenchmarkEnqueueRedis(b *testing.B)    { benchEnqueue(b, newBenchRedis(b)) }
func BenchmarkEnqueueSqlite32(b *testing.B) { benchEnqueueParallel(b, newBenchSqlite(b)) }
func BenchmarkEnqueueRedis32(b *testing.B)  { benchEnqueueParallel(b, newBenchRedis(b)) }

// ---- BenchmarkDequeueAck:裸 Broker 的认领回执一整圈 ----

// benchDequeueAck 一个 op = 裸调 Broker 的 Enqueue→Dequeue→Ack 一整圈:
// 不经过 scheduler/limiter(等价"限流关掉测框架开销"),队列始终恰好一个任务在途,
// Dequeue 永不阻塞,量出来的是后端三次核心调用的纯开销。
func benchDequeueAck(b *testing.B, br taskgate.Broker) {
	newBenchGate(b, br) // 只为完成 Broker.Init 装配(TTL/时钟/封顶),不消费
	ctx := context.Background()
	payload := json.RawMessage(`{"n":1}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := &taskgate.Task{Type: benchQueue, Queue: benchQueue, Payload: payload}
		if err := br.Enqueue(ctx, t); err != nil {
			b.Fatalf("Enqueue 失败: %v", err)
		}
		got, err := br.Dequeue(ctx, []string{benchQueue})
		if err != nil {
			b.Fatalf("Dequeue 失败: %v", err)
		}
		if err := br.Ack(ctx, got.ID, got.LeaseToken, []byte(`"ok"`)); err != nil {
			b.Fatalf("Ack 失败: %v", err)
		}
	}
}

func BenchmarkDequeueAckSqlite(b *testing.B) { benchDequeueAck(b, newBenchSqlite(b)) }
func BenchmarkDequeueAckRedis(b *testing.B)  { benchDequeueAck(b, newBenchRedis(b)) }

// ---- BenchmarkPipeline:三级依赖链全链路 ----

// benchPipeline 全链路吞吐:起 Gate 消费,提交 b.N/3 条三级依赖链(A→B→C),
// 等全部 completed。ns/op 是"平均每个任务从提交到完成摊到的时间"(含依赖唤醒)。
func benchPipeline(b *testing.B, br taskgate.Broker) {
	g := newBenchGate(b, br)
	g.Handle(benchQueue, func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
		return []byte(`"ok"`), nil
	})
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = g.Run(runCtx) }()
	b.Cleanup(func() { cancelRun(); <-runDone })

	chains := b.N / 3
	if chains < 1 {
		chains = 1
	}
	total := int64(chains * 3)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < chains; i++ {
		id1, err := g.Submit(ctx, benchQueue, nil)
		if err != nil {
			b.Fatalf("Submit A 失败: %v", err)
		}
		id2, err := g.Submit(ctx, benchQueue, nil, taskgate.DependsOn(id1))
		if err != nil {
			b.Fatalf("Submit B 失败: %v", err)
		}
		if _, err := g.Submit(ctx, benchQueue, nil, taskgate.DependsOn(id2)); err != nil {
			b.Fatalf("Submit C 失败: %v", err)
		}
	}
	// 等全部 completed:轮询 O(1) 的 Counts,比逐任务 Wait(50ms 步长)采样细。
	deadline := time.Now().Add(10 * time.Minute)
	for {
		counts, err := br.Counts(ctx)
		if err != nil {
			b.Fatalf("Counts 失败: %v", err)
		}
		if counts[benchQueue][taskgate.StatusCompleted] >= total {
			break
		}
		if time.Now().After(deadline) {
			b.Fatalf("流水线 %d 条链超时未完成: %v", chains, counts)
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.StopTimer()
	if counts, err := br.Counts(ctx); err == nil {
		if n := counts[benchQueue][taskgate.StatusFailed]; n != 0 {
			b.Fatalf("基准过程中出现 failed 任务 %d 个", n)
		}
	}
}

func BenchmarkPipelineSqlite(b *testing.B) { benchPipeline(b, newBenchSqlite(b)) }
func BenchmarkPipelineRedis(b *testing.B)  { benchPipeline(b, newBenchRedis(b)) }
