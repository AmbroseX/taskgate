# taskgate

[![CI](https://github.com/AmbroseX/taskgate/actions/workflows/ci.yml/badge.svg)](https://github.com/AmbroseX/taskgate/actions/workflows/ci.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/AmbroseX/taskgate)](https://pkg.go.dev/github.com/AmbroseX/taskgate)
[![Go Report Card](https://goreportcard.com/badge/github.com/AmbroseX/taskgate)](https://goreportcard.com/report/github.com/AmbroseX/taskgate)
![Go Version](https://img.shields.io/badge/go-1.25%2B-00ADD8)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

English | [简体中文](README.md)

taskgate is a `go get`-and-go task queueing & rate-limiting library for Go: queueing, rate limiting, retries, dependencies, cancellation, and graceful shutdown — one interface, five backends.

It is a **library, not a service** — there is no Web UI, it reads no environment variables or config files, it only takes the `Config` you hand it. Typical use case: when calling quota-bound external gateways like LLM / OCR, funnel the requests into a queue, isolate and rate-limit them per type, back off and retry on failure, and reclaim tasks via leases when a process crashes.

## Features

- **Per-type rate limiting**: each queue has its own `{Workers, RPS, Burst}`, so a slow queue never drags down a fast one; `Routes` lets multiple task types share a queue (and thus share one gateway's quota).
- **Periodic quota (hard quota)**: `{QuotaLimit, QuotaPeriod}` caps "at most N handler starts per fixed window" — rate and quota are different things (see below); the counter is shared across processes via the backend's medium and **never over-grants**: every failure mode only under-admits. A backend that can't share the counter makes `New()` fail — quota never degrades silently.
- **Five backends, one contract**: `memorybroker` (in-memory, zero deps), `sqlitebroker` (single-file on disk, pure Go, no cgo), `redisbroker` (multi-process shared, atomic Lua transitions), `pgbroker` (PostgreSQL), `mysqlbroker` (MySQL) — the last two are server-database backends: multi-process shared, exclusive claim via `FOR UPDATE SKIP LOCKED` (requires MySQL 8.0+ / PostgreSQL 9.5+); all five pass the same behavioral contract (`brokertest`, 22 cases).
- **Distributed rate limiting** (redis backend): a queue's Workers/RPS quota is shared across every process connected to the same Redis, so adding machines doesn't mean hammering the gateway; concurrency slots held by a crashed process are reclaimed by lease.
- **Lease reclaim**: claiming a task takes a lease, heartbeats renew it automatically; after a worker crash the reaper picks tasks back up to rerun, and poison tasks hit a cap and go to the dead-letter state; long tasks can call `RenewLease` inside the handler, or turn off the auto heartbeat per queue and go fully manual (`ManualHeartbeat`). **The price is at-least-once — crash reclaim reruns your handler, so it must be idempotent**; see [The handler contract](#the-handler-contract-must-be-idempotent).
- **Three retry counters, clear division of labor**: `Attempts` handles business failures (exponential backoff `min(2^n×1s, 10min)±20%`, goes to `failed` past `MaxRetry`), `Throttled` handles gateway rate limiting (`ErrThrottled` doesn't consume retry attempts), `LeaseLost` handles crash reclaim; `ErrSkipRetry` goes straight to dead-letter.
- **Dependency pipelines**: `DependsOn` chains and fans in; the parent's final state and the child's wake-up happen in the same transaction, so no wake-up is lost; a failed parent cascades cancellation by default (opt out with `IgnoreParentFailure`).
- **Cancellation**: pending/blocked tasks are cancelled directly and propagate downward; a running task's handler ctx is cancelled immediately.
- **Graceful shutdown**: `Shutdown(ctx)` lets running tasks finish; on timeout it interrupts and returns the tasks as-is (consuming no counter), so a deploy restart doesn't burn task quota.
- **Observability**: `Get / List / Stats / Overview / Wait` for querying and waiting, `OnStateChange` callback for instrumentation; `List` supports stable `Offset+Limit` pagination.

## Supported backends

All five backends implement the same Broker contract and are verified by the same `brokertest` (22 cases). Switching backends means changing one constructor line — your business code stays untouched. Pick one by deployment shape:

| Backend | Fit | Deps | Persistence | Multi-process sharing |
|---|---|---|---|---|
| `memorybroker` | single process, tests, ephemeral tasks | none | no (lost on exit) | no |
| `sqlitebroker` | single machine, needs disk, no cgo | `modernc.org/sqlite` (pure Go) | single file | same-machine multi-process (file lock) |
| `redisbroker` | multi-process / multi-machine, exclusive claim | Redis + go-redis | Redis | yes (atomic Lua transitions) |
| `pgbroker` | multi-process / multi-machine, existing PG database | `github.com/jackc/pgx/v5` (pure Go) | PostgreSQL | yes (FOR UPDATE SKIP LOCKED) |
| `mysqlbroker` | multi-process / multi-machine, existing MySQL database | `github.com/go-sql-driver/mysql` (pure Go) | MySQL | yes (FOR UPDATE SKIP LOCKED) |

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

Runtime deps: `modernc.org/sqlite` (pure Go), `golang.org/x/time/rate`, `github.com/oklog/ulid/v2`, `github.com/redis/go-redis/v9`, `github.com/go-redis/redis_rate/v10`, `github.com/jackc/pgx/v5` (pure Go, no cgo), `github.com/go-sql-driver/mysql` (pure Go, no cgo); test dep: `github.com/alicebob/miniredis/v2`.

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

## The handler contract: must be idempotent

**taskgate is at-least-once, not exactly-once. The same task's handler may run more than once, and your handler must tolerate reruns.**

This isn't something that only happens "when things go wrong" — it's part of normal operation. Three paths lead to a rerun:

| Path | Trigger | Counter |
|---|---|---|
| Worker process crash (`kill -9`, OOM, power loss) | heartbeat stops → lease expires → reaper picks the task back up | `LeaseLost` |
| Handler hangs with `ManualHeartbeat` enabled on the queue | renewal stops → lease expires → reclaimed | `LeaseLost` |
| `Shutdown(ctx)` times out and interrupts | task is `Requeue`d back to pending as-is | **no counter moves** (the task looks like it never ran) |

For the first two, the crash can happen **halfway through** the first run — the LLM call already went out, the money is already spent, the database may be half-written.

### An example that will bite you

```go
// ❌ Wrong: a rerun double-deducts quota and double-writes
g.Handle("summarize", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
    resp, err := callLLM(ctx, t.Payload) // ← costs money
    if err != nil {
        return nil, err
    }
    billing.Deduct(userID, resp.Tokens)  // ← deducts quota
    return db.Save(ctx, resp)            // ← suppose the process dies here
})
```

The process dies inside `db.Save` → the lease expires → the reaper picks it back up and reruns it → **the LLM is called twice and the quota is deducted twice**, while the database holds only one result.

### How to guard: three separate problems

A rerun hurts three different things. **The remedies differ, and only the first can be fully protected by a local transaction:**

| | Local database side effects (quota deduction, DB writes) | External gateway calls (LLM/OCR) | Async messages/notifications (email, webhook, SMS) |
|---|---|---|---|
| Can it be prevented? | **Yes**, with `Task.ID` as an idempotency key | **Depends on the gateway** — taskgate can't help | **Only up to "the intent is recorded exactly once"** — delivery is still at-least-once |
| How | DB unique constraint / idempotency table, in the same transaction as the business write | If the gateway supports idempotency keys, pass `Task.ID`; if not, you must accept duplicate calls | Transactional outbox: persist "send this notification" within the business transaction; **the sender/receiver must still dedupe by an idempotency key** |

The third category is easy to mistake for the first: an outbox only guarantees that the notification **intent** lives or dies with the business data. The actual send can still happen more than once — recipients of emails and webhooks still see at-least-once delivery, so the idempotency key (e.g. `Task.ID`) must still travel with the message.

#### Local side effects: guard atomically with `Task.ID`

`Task.ID` is stable across reruns — use it as your business idempotency key. **The key point is that the deduction and the write must land in one transaction under one unique key** — doing them in two steps leaves a window.

```go
g.Handle("summarize", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
    resp, err := callLLM(ctx, t.Payload)
    if err != nil {
        return nil, err
    }
    // Deduct + persist in a single transaction keyed by t.ID;
    // on a rerun the unique constraint conflicts, the first result is returned,
    // and the user is not charged twice.
    return db.SaveOnce(ctx, t.ID, userID, resp)
})
```

#### External calls: taskgate cannot close this window

The snippet above **does not stop the LLM from being called twice**. If the process dies after `callLLM` succeeds but before `db.SaveOnce`, the rerun will inevitably hit the gateway again — **that call costs money**. What you get is only that the user isn't double-charged and the database isn't double-written.

This window is the inherent price of at-least-once. There are exactly two ways to deal with it:

- **When the gateway explicitly supports idempotency keys**, pass `t.ID` as the key and let the gateway deduplicate, so a rerun produces no second charge. **This is the only way to truly eliminate the duplicate call.** Payment gateways commonly support this (e.g. Stripe's `Idempotency-Key`); whether an LLM gateway supports it — and on which endpoints — **must be verified against its official documentation**, not assumed.
- **The gateway doesn't**: accept the cost of duplicate calls, or compensate at the business protocol level (reconciliation, dedup by `t.ID`). taskgate can't help here — the "external call already succeeded but nothing landed locally yet" window cannot be closed by any task queue.

> **`WithBusinessKey` makes *submission* idempotent, not *execution*.** Submitting the same key twice returns `ErrTaskExists`, so the same piece of work is enqueued only once; but once enqueued, that single task's handler can still run multiple times due to crash reclaim. These are two different things — don't conflate them. (The old `WithID` is deprecated and is now an alias of `WithBusinessKey` — the value you pass is a business key, no longer the task ID.)

> **Do not put critical side effects in `OnStateChange`.** It is an asynchronous notification: no delivery guarantee, no ordering guarantee, a panic in the callback is swallowed, and there is no retry and no "consumed exactly once globally" guarantee. It suits instrumentation and observation that tolerates loss or duplication — it cannot carry billing, notifications, or business-final writes.

### Why we don't do exactly-once

This is the hard problem of distributed systems: a handler's execution and its side effects don't share a transaction, so no library can give you that guarantee — every "mark it done once it finishes" scheme has a window where the work finished but the mark didn't land. What we can give you is at-least-once plus a `Task.ID` that stays stable across reruns, so you can build idempotency on your side.

## Handler error semantics

What error a handler returns decides which retry path the task takes:

```go
return nil, taskgate.ErrThrottled{RetryAfter: 30 * time.Second} // gateway rate limit: requeue later, doesn't consume retry attempts
return nil, taskgate.ErrSkipRetry{Err: err}                     // hopeless error: go straight to the failed dead-letter state
return nil, err                                                 // ordinary business failure: exponential backoff retry
```

> **Note**: `ErrThrottled` / `ErrSkipRetry` must be returned **by value** (`errors.As` matches by value); do not return a pointer to them.

Submit options: `WithBusinessKey` (business idempotency key; the old `WithID` is deprecated as its alias), `Delay` / `RunAt` (delayed execution), `MaxRetry`, `DependsOn`, `IgnoreParentFailure`.

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

Use it when multiple worker processes connect to the same Redis and race for the same batch of tasks: atomic Lua transitions guarantee that **each task has exactly one valid lease at any instant**, so state transitions are never torn by concurrency; after a `kill -9`, running tasks are reclaimed by lease and **rerun**.

> Note this is not the same as "handlers never overlap": once a lease expires a new worker may re-claim the task, while the old handler may still be running — a network partition, a process pause (STW/paging), or simply not honoring ctx promptly. Two copies of your business code can briefly overlap. So execution is at-least-once and your handler must be idempotent — see [The handler contract](#the-handler-contract-must-be-idempotent).

Switching backends means changing this one constructor line — everything else stays the same:

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
- **A crashed task picked back up**: counting from the last successful renewal, `LeaseTTL` (lease expiry) plus at worst `LeaseTTL/2` (the reaper scans every `min(LeaseTTL across queues)/2`) = **`LeaseTTL` to `1.5×LeaseTTL`** (60s default → 60–90s) before it returns to pending; after that it still has to queue up, take a slot, and wait for a token before it actually runs. Lower `LeaseTTL` to detect crashes sooner, at the cost of more frequent heartbeats (one every `LeaseTTL/3`).

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

- **Two-tier contract tests**: the miniredis tier runs offline in CI; set `TASKGATE_REDIS_ADDR=127.0.0.1:6379` and the same 22 contract cases run again against real Redis (isolated by random KeyPrefix, cleaned up afterward) to verify Lua script compatibility.
- **Redis Cluster not supported**: keys are built by prefix inside the scripts and carry no hash tag; targets single-instance / primary-replica / sentinel topologies.
- **Limiter keys and task keys live in the same Redis instance**: a flushdb-level failure takes both down together (an honest trade-off).

## SQL backends: PostgreSQL / MySQL (multi-process)

Use these when you already run a PostgreSQL or MySQL and don't want to bring in a Redis: multiple worker processes connect to the same database and race for the same batch of tasks, with exclusive claim via `FOR UPDATE SKIP LOCKED` (requires MySQL 8.0+ / PostgreSQL 9.5+), and they pass the same 22 contract cases. Just like redis, switching backends means changing one constructor line — your business code stays untouched:

```go
import "github.com/AmbroseX/taskgate/pgbroker"
b, err := pgbroker.Open("postgres://user:pass@localhost:5432/db?sslmode=disable")
```

```go
import "github.com/AmbroseX/taskgate/mysqlbroker"
b, err := mysqlbroker.Open("user:pass@tcp(localhost:3306)/db")
```

Options: `WithTablePrefix("myapp_")` (default `taskgate_`, use it to isolate when several apps share one database), `WithMaxOpenConns(n)` (default 10), `WithPollInterval(d)` (default 200ms). On the first `Open`, `Init` creates the tables automatically (`CREATE TABLE IF NOT EXISTS`, safe under concurrent cold starts).

### Known limitations

- **Cross-process new-task notice latency = the poll interval** (default 200ms, tunable); there is no PG LISTEN/NOTIFY instant wake-up.
- **No distributed rate limiting**: the SQL backends don't implement `LimiterProvider`, so the scheduler falls back to in-process limiting and each process limits on its own; for precise cross-process rate limiting use the redis backend.
- **Under high-concurrency dependency-propagation conflicts**, transactions go through automatic database-deadlock retries (capped, default 5 times), showing up as higher latency on a few calls; once the retry cap is exceeded the deadlock error is thrown as-is.
- **MySQL-specific**: custom ID/type/queue are at most 255 chars (validated at the `Enqueue` entry point, with a clear error past the limit); payload/result are bounded by the server's `max_allowed_packet` (default 64M); tables use the `utf8mb4_bin` collation (baked into the DDL, so custom IDs are case-sensitive).
- **Contract tests need a real database** (env-gated by `TASKGATE_PG_DSN` / `TASKGATE_MYSQL_DSN`), skipped when no database is available locally — an all-green local run does not mean these two backends were exercised; regression relies on CI.

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
    quota_limit: 5000   # periodic quota: at most 5000 handler starts per window, 0 = disabled
    quota_period: 24h   # window length (>=1s; fixed windows aligned to epoch — 24h means UTC midnight, not your local calendar day)
    quota_key: my-gw    # quota key, empty = queue name; queues sharing a key share one window budget
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

### Rate ≠ quota: the periodic hard quota

`RPS` protects the gateway from bursts ("don't hammer it"); `QuotaLimit` protects your bill ("don't overspend the window budget") — **they are different things**: a gateway allowing 5,000 calls a day does not mean you must spread them one per 17 seconds; you may burn the budget at full speed and then stop. The three dimensions are orthogonal: `Workers` caps concurrency, `RPS` caps starts per second, `QuotaLimit` caps cumulative starts per window.

The honest contract:

- **Hard quota, never over-grants**: the window counter is decremented atomically in the shared medium (summed across every process on that medium); crashes, disconnects, and failed refunds all err on the side of admitting *less*, never more;
- Windows are **fixed-length, aligned to epoch** (`24h` resets at UTC midnight) — not your local calendar day, not a calendar month, and not your gateway's billing cycle; window time comes from the medium's server clock, so application clock skew doesn't matter;
- The unit is **handler starts**, not tasks (a retry's re-claim counts again) and not tokens;
- Two consumption leaks to keep in mind: a handler that starts and then fails or gets canceled, and a task whose type has no registered handler (dead-lettered) — in both cases the quota **was already spent**;
- Exhaustion is **not an error**: the queue stops claiming (without holding a worker slot), tasks sit in pending until the next window, nothing lands in `Throttled` or failed; `Stats(queue)` exposes a `QuotaExhausted` bit;
- If the quota medium is unreachable the queue **fails closed**: claiming pauses, zero admissions, retried with backoff (`QuotaStalled` bit visible) — it never falls back to an in-process counter pretending you're still protected;
- The backend must support shared counting (all five built-ins do: memory = the process, sqlite = the db file, redis/pg/mysql = the server); configuring quota on a backend that doesn't makes `New()` fail.

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

// Idempotent submission + replay (spec 005): the task ID (ExecutionID) is system-generated;
// the business key is a separate concept. A final execution can be replayed into a NEW
// execution — the old record is never mutated, and `ReplayOf` points back for audit.
id, err := g.Submit(ctx, "generate", payload, taskgate.WithBusinessKey("order-42"))
var te *taskgate.TaskExistsError
if errors.As(err, &te) && te.Status == taskgate.StatusFailed {
    newID, _ := g.Replay(ctx, te.ExecutionID) // rerun the failed work under the same key
    _ = newID
}
g.Replay(ctx, id, taskgate.AllowCompleted())        // replaying a completed execution must be explicit
g.Replay(ctx, id, taskgate.WithPayload(newPayload)) // fix the payload before rerunning (copied by default)
history, _ := g.History(ctx, "order-42")            // execution chain under the key (oldest → newest)
```

## Typed errors

All errors are exported; check them with `errors.Is` / `errors.As`:

```go
// Sentinel errors (errors.Is)
taskgate.ErrTaskExists    // the business key already has executions (hit on WithBusinessKey dedup; errors.As gives *TaskExistsError with the chain tail)
taskgate.ErrTaskNotFound  // task does not exist (Get/Cancel miss, or a depended-on parent is missing)
taskgate.ErrLeaseLost     // lease token mismatch: task already reclaimed or re-claimed by someone else, result void
taskgate.ErrTaskCanceled  // task was flagged for cancellation, the handler should exit
taskgate.ErrAlreadyFinal  // Cancel on a task already in a final state
taskgate.ErrUnknownType   // Run hit a task type with no registered handler
taskgate.ErrShutdown      // Gate already Shutdown, new submissions rejected
taskgate.ErrNoTask        // RenewLease called on a ctx outside a handler
taskgate.ErrReplayNotFinal      // Replay target is not in a final state yet
taskgate.ErrAlreadyReplayed     // Replay target was already replayed (history chains never fork)
taskgate.ErrCompletedNotAllowed // replaying a completed execution requires AllowCompleted()

// Structured errors (errors.As; handlers return them to control the retry path, must be returned by value)
taskgate.ErrThrottled{RetryAfter: d} // gateway rate limit: requeue later, doesn't consume retry attempts
taskgate.ErrSkipRetry{Err: err}      // hopeless error: go straight to failed; Unwrap reaches the original error
```

## Run the tests

The whole suite runs offline (L1 unit → L2 brokertest contract → L3 integration → L4 simulated E2E):

```bash
go test ./... -race
```

To run the 22 contract cases against real databases as well (optional; Redis verifies Lua script compatibility, PG/MySQL auto-skip when no database is available locally):

```bash
TASKGATE_REDIS_ADDR=127.0.0.1:6379 go test ./redisbroker/... -race
TASKGATE_PG_DSN="postgres://postgres:pass@localhost:5432/postgres?sslmode=disable" go test ./pgbroker/... -race
TASKGATE_MYSQL_DSN="root:pass@tcp(localhost:3306)/taskgate" go test ./mysqlbroker/... -race
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
- **M2 (done)**: redis backend (atomic Lua transitions, multi-process exclusive claim), distributed rate limiting (quota shared across processes), performance baseline.
- **M3 (done)**: L4 simulated E2E (five mockgw fault-injection cases), handler manual renewal (`RenewLease`/`ManualHeartbeat`), List pagination, realgw manual smoke tier.

Explicitly out of scope (YAGNI): task priority, webhook notifications, cursor pagination, Web UI, cron periodic scheduling, DAG workflow engine, standalone server mode.
