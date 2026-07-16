# taskgate

[![CI](https://github.com/AmbroseX/taskgate/actions/workflows/ci.yml/badge.svg)](https://github.com/AmbroseX/taskgate/actions/workflows/ci.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/AmbroseX/taskgate)](https://pkg.go.dev/github.com/AmbroseX/taskgate)
[![Go Report Card](https://goreportcard.com/badge/github.com/AmbroseX/taskgate)](https://goreportcard.com/report/github.com/AmbroseX/taskgate)
![Go Version](https://img.shields.io/badge/go-1.25%2B-00ADD8)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

English | [简体中文](README.md)

taskgate is a `go get`-and-go task queueing & rate-limiting library for Go: queueing, rate limiting, retries, dependencies, cancellation, and graceful shutdown — one interface, three backends.

It is a **library, not a service** — there is no Web UI, it reads no environment variables or config files, it only takes the `Config` you hand it. Typical use case: when calling quota-bound external gateways like LLM / OCR, funnel the requests into a queue, isolate and rate-limit them per type, back off and retry on failure, and reclaim tasks via leases when a process crashes.

## Features

- **Per-type rate limiting**: each queue has its own `{Workers, RPS, Burst}`, so a slow queue never drags down a fast one; `Routes` lets multiple task types share a queue (and thus share one gateway's quota).
- **Three backends, one contract**: `memorybroker` (in-memory, zero deps), `sqlitebroker` (single-file on disk, pure Go, no cgo), `redisbroker` (multi-process shared, atomic Lua transitions); all three pass the same behavioral contract (`brokertest`, 18 cases).
- **Distributed rate limiting** (redis backend): a queue's Workers/RPS quota is shared across every process connected to the same Redis, so adding machines doesn't mean hammering the gateway; concurrency slots held by a crashed process are reclaimed by lease.
- **Lease reclaim**: claiming a task takes a lease, heartbeats renew it automatically; after a worker crash the reaper picks tasks back up to rerun, and poison tasks hit a cap and go to the dead-letter state; long tasks can call `RenewLease` inside the handler, or turn off the auto heartbeat per queue and go fully manual (`ManualHeartbeat`).
- **Three retry counters, clear division of labor**: `Attempts` handles business failures (exponential backoff `min(2^n×1s, 10min)±20%`, goes to `failed` past `MaxRetry`), `Throttled` handles gateway rate limiting (`ErrThrottled` doesn't consume retry attempts), `LeaseLost` handles crash reclaim; `ErrSkipRetry` goes straight to dead-letter.
- **Dependency pipelines**: `DependsOn` chains and fans in; the parent's final state and the child's wake-up happen in the same transaction, so no wake-up is lost; a failed parent cascades cancellation by default (opt out with `IgnoreParentFailure`).
- **Cancellation**: pending/blocked tasks are cancelled directly and propagate downward; a running task's handler ctx is cancelled immediately.
- **Graceful shutdown**: `Shutdown(ctx)` lets running tasks finish; on timeout it interrupts and returns the tasks as-is (consuming no counter), so a deploy restart doesn't burn task quota.
- **Observability**: `Get / List / Stats / Overview / Wait` for querying and waiting, `OnStateChange` callback for instrumentation; `List` supports stable `Offset+Limit` pagination.

## Supported backends

All three backends implement the same Broker contract and are verified by the same `brokertest` (18 cases). Switching backends means changing one constructor line — your business code stays untouched. Pick one by deployment shape:

| Backend | Fit | Deps | Persistence | Multi-process sharing |
|---|---|---|---|---|
| `memorybroker` | single process, tests, ephemeral tasks | none | no (lost on exit) | no |
| `sqlitebroker` | single machine, needs disk, no cgo | `modernc.org/sqlite` (pure Go) | single file | same-machine multi-process (file lock) |
| `redisbroker` | multi-process / multi-machine, exactly-once | Redis + go-redis | Redis | yes (atomic Lua transitions) |

The Redis backend targets single-instance / primary-replica / sentinel topologies. **Redis Cluster is not supported** (keys are built by prefix inside the scripts and carry no hash tag).

## Installation

taskgate requires Go 1.25+ with modules enabled. Initialize your module first:

```bash
go mod init github.com/my/repo
```

Then install taskgate:

```bash
go get github.com/AmbroseX/taskgate
```

Runtime deps: `modernc.org/sqlite` (pure Go), `golang.org/x/time/rate`, `github.com/oklog/ulid/v2`, `github.com/redis/go-redis/v9`, `github.com/go-redis/redis_rate/v10`; test dep: `github.com/alicebob/miniredis/v2`.

## Quickstart

A three-stage pipeline: retrieve → generate → score (full runnable version in [examples/llm](examples/llm/main.go), `go run ./examples/llm`).

```go
package main

import (
    "context"
    "encoding/json"
    "time"

    "github.com/AmbroseX/taskgate"
    "github.com/AmbroseX/taskgate/memorybroker"
)

func main() {
    g, err := taskgate.New(taskgate.Config{
        Broker: memorybroker.New(), // or sqlitebroker.Open("tasks.db") / redisbroker.New(...)
        Queues: map[string]taskgate.QueueConfig{
            "cpu": {Workers: 4},         // local light work: 4 concurrent, no rate limit
            "llm": {Workers: 2, RPS: 3}, // LLM gateway: 2 concurrent, at most 3 per second
        },
        Routes: map[string]string{ // task type → queue
            "retrieve": "cpu", "generate": "llm", "score": "cpu",
        },
    })
    if err != nil {
        panic(err)
    }

    // Register a handler: the return value is written to Result, the error decides the retry path.
    g.Handle("generate", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
        parent, _ := g.Get(ctx, t.DependsOn[0]) // read the upstream Result
        _ = parent
        // ... call the LLM ...
        return json.Marshal(map[string]string{"answer": "..."})
    })

    ctx := context.Background()

    // Start the consumer loop (blocking, put it in a goroutine).
    go g.Run(ctx)

    // Submit the pipeline: DependsOn chains them, a child wakes up only after its parent completes.
    rid, _ := g.Submit(ctx, "retrieve", nil)
    gid, _ := g.Submit(ctx, "generate", nil, taskgate.DependsOn(rid))
    sid, _ := g.Submit(ctx, "score", nil, taskgate.DependsOn(gid))
    _ = time.Second

    // Wait for the final result; shut down gracefully.
    result, _ := g.Wait(ctx, sid)
    _ = result
    _ = g.Shutdown(ctx)
}
```

## Handler error semantics

What error a handler returns decides which retry path the task takes:

```go
return nil, taskgate.ErrThrottled{RetryAfter: 30 * time.Second} // gateway rate limit: requeue later, doesn't consume retry attempts
return nil, taskgate.ErrSkipRetry{Err: err}                     // hopeless error: go straight to the failed dead-letter state
return nil, err                                                 // ordinary business failure: exponential backoff retry
```

> **Note**: `ErrThrottled` / `ErrSkipRetry` must be returned **by value** (`errors.As` matches by value); do not return a pointer to them.

Submit options: `WithID` (idempotent dedup), `Delay` / `RunAt` (delayed execution), `MaxRetry`, `DependsOn`, `IgnoreParentFailure`.

## Long tasks and manual lease renewal

By default (auto mode) the scheduler starts an auto heartbeat for every running task, renewing the lease every `LeaseTTL/3`, and the handler does nothing. Two situations call for manual renewal:

- **Renew an extra beat inside auto mode**: the handler can call `taskgate.RenewLease(ctx)` on the task ctx at any time; it shares the same lease token as the auto heartbeat, extends idempotently, and the two don't interfere.
- **Sharper poison-task detection**: the auto heartbeat is sent by the scheduler, so a stuck handler still gets its heartbeats and its task is never reclaimed. Set the queue's `ManualHeartbeat` to `true` to turn off the auto heartbeat; the handler renews once per checkpoint it finishes — get stuck and it stops renewing, the lease expires, and the reaper reclaims it to rerun.

```go
Queues: map[string]taskgate.QueueConfig{
    "ocr": {Workers: 2, LeaseTTL: taskgate.Duration(60 * time.Second), ManualHeartbeat: true},
},

g.Handle("ocr", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
    for _, page := range pages {
        if err := taskgate.RenewLease(ctx); err != nil {
            return nil, err // on ErrTaskCanceled / ErrLeaseLost the ctx is already cancelled, exit ASAP
        }
        // ... process one page ...
    }
    return result, nil
})
```

Return values of `RenewLease`:

| Return | Meaning | What the handler should do |
|---|---|---|
| `nil` | renewal succeeded | keep working |
| `ErrTaskCanceled` | task was cancelled externally (renewal still done) | ctx already cancelled, exit ASAP |
| `ErrLeaseLost` | lease already lost (reclaimed by the reaper), the result is doomed | ctx already cancelled, give up now |
| `ErrNoTask` | not a task ctx (called outside a handler) | fix your code |
| other error | network hiccup, lease neither renewed nor lost | retry later |

**Cross-process cancellation in manual mode**: once the auto heartbeat is off, a `Cancel` from another process can only be discovered by the handler's next `RenewLease` (which returns `ErrTaskCanceled`); if the handler never renews, lease expiry is the backstop. A `Cancel` from the same process is unaffected and still cancels the handler's ctx immediately. Manual mode means "the handler is fully responsible for its own lease."

## List pagination

`List` results are stably sorted by `(CreatedAt, ID)` ascending (consistent across all three backends), and `Filter` supports `Offset+Limit` paging: filter → sort → skip `Offset` items → take `Limit` items (0 = unlimited); an out-of-range `Offset` returns an empty list without an error.

```go
page2, _ := g.List(ctx, taskgate.Filter{Type: "ocr", Limit: 20, Offset: 20})
```

Two things to keep in mind:

- **Weakly-consistent paging**: while tasks are being enqueued/transitioning during paging, there is no snapshot-consistency guarantee — only that "unchanged tasks are neither dropped nor duplicated"; strongly-consistent cursors are deferred to M4.
- **Cost on the redis backend**: List goes "index sets → candidate set → fetch each → in-memory sort & slice", so its complexity is O(candidate set), not O(page size); with a large inventory, narrow the candidate set first via `Filter`'s Type/Status/Queue before paging.

## Redis backend (multi-process)

Use it when multiple worker processes connect to the same Redis and race for the same batch of tasks: each task is executed exactly once, and after a `kill -9` running tasks are reclaimed by lease and rerun. Switching backends means changing this one constructor line — everything else stays the same:

```go
b, err := redisbroker.New(redisbroker.Options{
    Addr:      "127.0.0.1:6379",
    Password:  "",    // empty = no auth
    DB:        0,
    KeyPrefix: "tg:", // default "tg:"; use it to isolate when several apps share one Redis
})
if err != nil { ... }
g, err := taskgate.New(taskgate.Config{Broker: b, Queues: ...})
```

Every "multi-step read-write must be atomic" operation (claim, final-state + dependency wake-up, cascade cancel, counter maintenance) runs inside a single Lua script, so there is no crash window where "the task has left the queue but has no lease"; "parent done but child not woken" can never be observed.

### Distributed rate limiting: quota shared across processes

The redis backend additionally implements the `LimiterProvider` capability interface, so a queue's `{Workers, RPS}` quota is shared **across every process connected to the same Redis** — two processes each configured with `{Workers: 2}` run 2 concurrently in total, not 4; RPS goes through GCRA (redis_rate), likewise a global rate. The memory/sqlite backends are unaffected and keep in-process limiting.

Self-healing concurrency slots: each occupied slot records an expiry time (= the queue's `LeaseTTL`), and the holding process renews every `LeaseTTL/3`; process crashes → renewal stops → the slot expires and is reclaimed, so the quota is reusable within at most `2×LeaseTTL` and never leaks permanently.

### Cross-process latency (good to know)

- **A new task noticed by another process**: same-process submissions have an internal wake-up signal and respond immediately; writes from another process are discovered by Dequeue's fallback poll, at worst **≤100ms** (same as sqlite cross-process).
- **Cross-process Cancel**: a running task's cancel flag is discovered by the holding process on its next heartbeat, so at worst **≤ one heartbeat cycle (LeaseTTL/3)** later its handler ctx is cancelled.

### Redis key cheat sheet (for ops)

Default prefix `tg:` (changeable via `Options.KeyPrefix`). No need to go through the app for backlog and in-flight counts — read them straight from `redis-cli`:

| Key | Type | Purpose | Query example |
|---|---|---|---|
| `tg:task:{id}` | hash | all task fields (times stored as unix millis) | `HGETALL tg:task:01J...` |
| `tg:pending:{queue}` | list | ready task ID queue (FIFO) | `LLEN tg:pending:scoring` |
| `tg:delayed:{queue}` | zset | delayed/backoff tasks, score=run_at | `ZCARD tg:delayed:scoring` |
| `tg:inflight` | zset | running tasks, score=lease expiry | `ZCARD tg:inflight` |
| `tg:children:{id}` | set | reverse dependency index (children depending on {id}) | `SMEMBERS tg:children:01J...` |
| `tg:idx:status:{status}` | set | status index (one per each of the seven states) | `SCARD tg:idx:status:failed` |
| `tg:idx:type:{type}` | set | Type index (for List filtering) | `SCARD tg:idx:type:ocr` |
| `tg:stats` | hash | Type×Status counts, field `{type}:{status}` | `HGETALL tg:stats` |
| `tg:types` | set | Types seen so far | `SMEMBERS tg:types` |
| `tg:sem:{queue}` | zset | distributed concurrency slots (limiter-private) | `ZCARD tg:sem:scoring` |
| `rate:tg:{queue}` | string | RPS limiter state (redis_rate GCRA internals, limiter-private). Note the `rate:` prefix is added by redis_rate at the outermost level, so this key is **not** inside the `KeyPrefix` namespace (actual key = `rate:` + KeyPrefix + queue name) — don't miss it when bulk-clearing by prefix | `GET rate:tg:scoring` |

`Counts`/`Overview` just read `tg:stats` (maintained by a Lua `HINCRBY` on each transition), `QueueLen` is just `LLEN + ZCARD` — all counter/length reads, no full scan.

### Testing and limitations

- **Two-tier contract tests**: the miniredis tier runs offline in CI; set `TASKGATE_REDIS_ADDR=127.0.0.1:6379` and the same 18 contract cases run again against real Redis (isolated by random KeyPrefix, cleaned up afterward) to verify Lua script compatibility.
- **Redis Cluster not supported**: keys are built by prefix inside the scripts and carry no hash tag; targets single-instance / primary-replica / sentinel topologies.
- **Limiter keys and task keys live in the same Redis instance**: a flushdb-level failure takes both down together (an honest trade-off).

## Config

The library reads no config files itself. `Config` fields carry `yaml`/`json` tags — unmarshal it in your app and hand it in:

```yaml
# your app's own config file (taskgate doesn't read it; the app unmarshals and injects it)
queues:
  llm:
    workers: 2       # concurrency cap (required, >=1)
    rps: 3           # per-second admissions, 0 = no limit
    burst: 3         # burst allowance, 0 falls back to max(1, int(rps))
    lease_ttl: 60s   # lease duration, 0 fills in the default 60s
  cpu:
    workers: 4
routes:              # task type → queue; unrouted types use the type name as the queue name
  generate: llm
default_queue:       # fallback queue, may be omitted entirely
  workers: 2
lease_lost_max: 3    # lease-loss cap (default 3), goes to failed past this
throttled_max: 100   # throttle-requeue cap (default 100), goes to failed past this
```

```go
var cfg taskgate.Config
_ = yaml.Unmarshal(raw, &cfg)      // your app parses it; Duration fields accept "60s", "10m"
cfg.Broker = memorybroker.New()    // inject runtime objects by hand
cfg.OnStateChange = func(t taskgate.Task) { /* instrument */ }
g, err := taskgate.New(cfg)
```

## Look and feel

Some common small idioms:

```go
// Idempotent submit: submitting the same ID again returns ErrTaskExists, no double-enqueue.
id, err := g.Submit(ctx, "generate", payload, taskgate.WithID("order-42"))
if errors.Is(err, taskgate.ErrTaskExists) { /* already queued */ }

// Delayed execution: relative delay or absolute time, pick one.
g.Submit(ctx, "reminder", payload, taskgate.Delay(30*time.Minute))
g.Submit(ctx, "reminder", payload, taskgate.RunAt(time.Now().Add(time.Hour)))

// Override the retry cap for this one task (defaults to the Config cap).
g.Submit(ctx, "flaky", payload, taskgate.MaxRetry(1))

// Fan-in: one task depends on several parents, wakes only when all complete.
g.Submit(ctx, "merge", nil, taskgate.DependsOn(idA, idB, idC))

// Don't cascade-cancel me if a parent fails (a failed parent cascades to children by default).
g.Submit(ctx, "cleanup", nil, taskgate.DependsOn(job), taskgate.IgnoreParentFailure())

// Block for the final result / cancel / fetch one.
result, err := g.Wait(ctx, id)
err = g.Cancel(ctx, id)
task, err := g.Get(ctx, id)

// Backlog & in-flight for a queue / global per-state counts.
stats, _ := g.Stats(ctx, "llm")
overview, _ := g.Overview(ctx)
```

## Typed errors

All errors are exported; check them with `errors.Is` / `errors.As`:

```go
// Sentinel errors (errors.Is)
taskgate.ErrTaskExists    // a task with the same ID already exists (hit on WithID idempotent dedup)
taskgate.ErrTaskNotFound  // task does not exist (Get/Cancel miss, or a depended-on parent is missing)
taskgate.ErrLeaseLost     // lease token mismatch: task already reclaimed or re-claimed by someone else, result void
taskgate.ErrTaskCanceled  // task was flagged for cancellation, the handler should exit
taskgate.ErrAlreadyFinal  // Cancel on a task already in a final state
taskgate.ErrUnknownType   // Run hit a task type with no registered handler
taskgate.ErrShutdown      // Gate already Shutdown, new submissions rejected
taskgate.ErrNoTask        // RenewLease called on a ctx outside a handler

// Structured errors (errors.As; handlers return them to control the retry path, must be returned by value)
taskgate.ErrThrottled{RetryAfter: d} // gateway rate limit: requeue later, doesn't consume retry attempts
taskgate.ErrSkipRetry{Err: err}      // hopeless error: go straight to failed; Unwrap reaches the original error
```

## Run the tests

The whole suite runs offline (L1 unit → L2 brokertest contract → L3 integration → L4 simulated E2E):

```bash
go test ./... -race
```

To run the 18 contract cases against real Redis as well (optional, verifies Lua script compatibility):

```bash
TASKGATE_REDIS_ADDR=127.0.0.1:6379 go test ./redisbroker/... -race
```

## Test layers and e2e

The `e2e/` directory is L4/L5 simulation:

- **`e2e/mockgw/`**: a fault-injectable mock LLM/OCR gateway (a test component, not part of the library API). Every pit hit in production is turned into a toggle: `Latency` (delay), `BusyAfterConcurrency` (over the concurrency limit it returns HTTP 200 but hides a busy error event in the body — reproducing the "the status code lies" real gateway), `FailRate` (fixed-seed random 500, reproducible in CI), `CrashAfterConcurrency` (drops the connection over the concurrency limit), `BusyFirstN` (targeted busy for the first N requests); exposes atomic observation points `MaxConcurrency/BusyCount/CrashCount/Requests`.
- **`e2e/pipeline_test.go`**: five core cases — rate limiting really blocks busy, busy takes the `ErrThrottled` requeue path with zero failed, disconnects take the ordinary retry path and complete, a three-queue pipeline hits 30/30 with results passed down stage by stage, mid-flight cancellation cascades, and an SSE-hidden error succeeds after requeue.
- **`e2e/realgw_test.go`**: a real-gateway smoke tier, isolated by `//go:build realgw`, **not in CI** (a normal `go vet`/`go test` doesn't even compile it); reads `LLM_GATEWAY_URL`/`LLM_GATEWAY_KEY` (auto-skips if missing), run it manually:

```bash
LLM_GATEWAY_URL=https://your-gateway LLM_GATEWAY_KEY=secret \
  go test -tags realgw ./e2e/ -run RealGW -v
```

## Design docs

- [taskgate design](docs/plans/2026-07-14-任务排队限流库taskgate方案.md) (architecture, Broker contract, state machine)
- [Test plan](docs/plans/2026-07-14-测试方案.md) (layered testing, fault-specific tests, performance baseline)
- spec-kit artifacts: [specs/001-m1-core-queue/](specs/001-m1-core-queue/) (M1), [specs/002-m2-redis/](specs/002-m2-redis/) (M2), [specs/003-m3-polish/](specs/003-m3-polish/) (M3)

## Milestones

- **M1 (done)**: core queueing, rate limiting, retries, dependencies, cancellation, Shutdown; memory / sqlite backends.
- **M2 (done)**: redis backend (atomic Lua transitions, multi-process exactly-once), distributed rate limiting (quota shared across processes), performance baseline.
- **M3 (done)**: L4 simulated E2E (five mockgw fault-injection cases), handler manual renewal (`RenewLease`/`ManualHeartbeat`), List pagination, realgw manual smoke tier.

Explicitly out of scope (YAGNI): task priority, webhook notifications, cursor pagination, Web UI, cron periodic scheduling, DAG workflow engine, standalone server mode.
