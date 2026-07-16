# taskgate

[![CI](https://github.com/AmbroseX/taskgate/actions/workflows/ci.yml/badge.svg)](https://github.com/AmbroseX/taskgate/actions/workflows/ci.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/AmbroseX/taskgate)](https://pkg.go.dev/github.com/AmbroseX/taskgate)
[![Go Report Card](https://goreportcard.com/badge/github.com/AmbroseX/taskgate)](https://goreportcard.com/report/github.com/AmbroseX/taskgate)
![Go Version](https://img.shields.io/badge/go-1.25%2B-00ADD8)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

[English](README.en.md) | 简体中文

taskgate 是一个 `go get` 即用的 Go 任务排队限流库:排队、限流、重试、依赖、取消、优雅停止,一套接口五种后端。

它是**库不是服务**——没有 Web UI、不读环境变量和配置文件,只吃你传进来的 `Config`。典型场景:调用 LLM / OCR 这类有配额的外部网关时,把请求收进队列,按类型隔离限流,失败自动退避重试,进程崩了靠租约把任务捞回来。

## 特性

- **类型级限流**:每个队列独立的 `{Workers, RPS, Burst}`,慢队列绝不拖累快队列;`Routes` 支持多类型共享同一队列(共享同一个网关配额)。
- **周期配额(硬配额)**:`{QuotaLimit, QuotaPeriod}` 限"每个固定窗口最多启动 N 次 handler"——频率和配额是两码事(见下文);跨进程共享计数、绝不超发,任何故障只少放行不多放;后端不支持共享计数时 `New()` 直接报错,没有静默降级。
- **五种后端一套契约**:`memorybroker`(内存,零依赖)、`sqlitebroker`(单文件落盘,纯 Go 免 cgo)、`redisbroker`(多进程共享,Lua 原子流转)、`pgbroker`(PostgreSQL)、`mysqlbroker`(MySQL)——后两个是服务器型数据库后端,多进程共享,认领互斥靠 `FOR UPDATE SKIP LOCKED`(要求 MySQL 8.0+ / PostgreSQL 9.5+);同一套行为契约(`brokertest` 22 条)五后端验收。
- **分布式限流**(redis 后端):同一队列的 Workers/RPS 配额在所有接同一 Redis 的进程间共享,加机器不等于把网关打爆;进程崩溃占着的并发槽按租约自动回收。
- **租约回收**:认领即加租约,心跳自动续租;worker 崩溃后 reaper 把任务捞回重跑,毒任务封顶进死信;长任务可在 handler 里 `RenewLease` 手动续租,也可按队列关掉自动心跳改全手动(`ManualHeartbeat`)。**代价是 at-least-once——崩溃回收会让 handler 重跑,所以 handler 必须幂等**,见[handler 的执行契约](#handler-的执行契约必须幂等)。
- **重试三计数分工**:`Attempts` 管业务失败(指数退避 `min(2^n×1s,10min)±20%`,超 `MaxRetry` 进 failed)、`Throttled` 管网关限流(`ErrThrottled` 不占重试次数)、`LeaseLost` 管崩溃回收;`ErrSkipRetry` 直接死信。
- **依赖流水线**:`DependsOn` 串联/扇入,父任务终态与子任务唤醒在同一事务内完成,不丢唤醒;父失败默认连锁取消(可 `IgnoreParentFailure`)。
- **取消**:pending/blocked 直接取消并向下传播;running 任务的 handler ctx 被即时 cancel。
- **优雅停止**:`Shutdown(ctx)` 等在跑任务善终;超时则打断并把任务原样归还(不占任何计数),部署重启不消耗任务配额。
- **可观测**:`Get / List / Stats / Overview / Wait` 查询等待,`OnStateChange` 回调埋点;`List` 支持 `Offset+Limit` 稳定分页。

## 支持的后端

五种后端实现同一套 Broker 合同,由同一套 `brokertest`(22 条)验收,换后端只改构造一行、业务代码零改动。按部署形态挑一个:

| 后端 | 适用 | 依赖 | 持久化 | 多进程共享 |
|---|---|---|---|---|
| `memorybroker` | 单进程、测试、临时任务 | 无 | 否(进程退出即丢) | 否 |
| `sqlitebroker` | 单机、要落盘、免 cgo | `modernc.org/sqlite`(纯 Go) | 单文件 | 同机多进程(文件锁) |
| `redisbroker` | 多进程/多机、认领互斥 | Redis + go-redis | Redis | 是(Lua 原子流转) |
| `pgbroker` | 多进程/多机、已有 PG 库 | `github.com/jackc/pgx/v5`(纯 Go) | PostgreSQL | 是(FOR UPDATE SKIP LOCKED) |
| `mysqlbroker` | 多进程/多机、已有 MySQL 库 | `github.com/go-sql-driver/mysql`(纯 Go) | MySQL | 是(FOR UPDATE SKIP LOCKED) |

Redis 后端面向单实例 / 主从 / 哨兵拓扑,**不支持 Redis Cluster**(脚本内自行拼键、键不带 hash tag)。

## 安装

taskgate 需要 Go 1.25+ 并启用 modules。先初始化你的 module:

```bash
go mod init github.com/my/repo
```

再拉取 taskgate:

```bash
go get github.com/AmbroseX/taskgate
```

运行期依赖:`modernc.org/sqlite`(纯 Go)、`golang.org/x/time/rate`、`github.com/oklog/ulid/v2`、`github.com/redis/go-redis/v9`、`github.com/go-redis/redis_rate/v10`、`github.com/jackc/pgx/v5`(纯 Go 免 cgo)、`github.com/go-sql-driver/mysql`(纯 Go 免 cgo);测试依赖 `github.com/alicebob/miniredis/v2`。

## 快速开始

三级流水线:检索 → 生成 → 打分(完整可跑版本见 [examples/llm](examples/llm/main.go),`go run ./examples/llm`)。

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
        Broker: memorybroker.New(), // 或 sqlitebroker.Open("tasks.db") / redisbroker.New(...)
        Queues: map[string]taskgate.QueueConfig{
            "cpu": {Workers: 4},         // 本地轻活:4 并发不限速
            "llm": {Workers: 2, RPS: 3}, // 大模型网关:2 并发,每秒最多 3 个
        },
        Routes: map[string]string{ // 任务类型 → 队列
            "retrieve": "cpu", "generate": "llm", "score": "cpu",
        },
    })
    if err != nil {
        panic(err)
    }

    // 注册 handler:返回值写进 Result,返回错误决定重试路径。
    g.Handle("generate", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
        parent, _ := g.Get(ctx, t.DependsOn[0]) // 读上游的 Result
        _ = parent
        // ... 调大模型 ...
        return json.Marshal(map[string]string{"answer": "..."})
    })

    ctx := context.Background()

    // 起消费循环(阻塞,放 goroutine)。
    go g.Run(ctx)

    // 提交流水线:DependsOn 串联,父任务完成才唤醒子任务。
    rid, _ := g.Submit(ctx, "retrieve", nil)
    gid, _ := g.Submit(ctx, "generate", nil, taskgate.DependsOn(rid))
    sid, _ := g.Submit(ctx, "score", nil, taskgate.DependsOn(gid))
    _ = time.Second

    // 等最终结果;优雅停机。
    result, _ := g.Wait(ctx, sid)
    _ = result
    _ = g.Shutdown(ctx)
}
```

## handler 的执行契约:必须幂等

**taskgate 是 at-least-once(至少执行一次),不是 exactly-once。同一个任务的 handler 可能被执行多次,你的 handler 必须能容忍重跑。**

这不是"出错了才会发生",而是正常运转的一部分。三条路径都会导致重跑:

| 路径 | 触发 | 计数 |
|---|---|---|
| worker 进程崩溃(`kill -9`、OOM、断电) | 心跳停 → 租约到期 → reaper 把任务捞回 | `LeaseLost` |
| handler 卡死 + 队列开了 `ManualHeartbeat` | 停止续租 → 租约到期被回收 | `LeaseLost` |
| `Shutdown(ctx)` 超时打断 | 任务原样 `Requeue` 归还回 pending | **三计数都不动**(任务看起来"没跑过") |

前两条崩溃发生时,第一跑可能已经**跑到一半**——LLM 已经调过了、钱已经花了、库可能已经写了一半。

### 一个会出事的例子

```go
// ❌ 错的:重跑会重复扣额度、重复落库
g.Handle("summarize", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
    resp, err := callLLM(ctx, t.Payload) // ← 花钱了
    if err != nil {
        return nil, err
    }
    billing.Deduct(userID, resp.Tokens)  // ← 扣额度
    return db.Save(ctx, resp)            // ← 假设进程在这里崩了
})
```

进程崩在 `db.Save` 里 → 租约到期 → reaper 捞回重跑 → **LLM 调了两次、额度扣了两次**,而库里只有一份结果。

### 怎么防:三类东西要分开处理

重跑会伤到三类东西,**防护手段各不相同,而且只有第一类能被本地事务彻底防住**:

| | 本地数据库副作用(扣额度、落库) | 外部网关调用(调 LLM/OCR) | 异步消息/通知(邮件、Webhook、短信) |
|---|---|---|---|
| 能不能防住 | **能**,用 `Task.ID` 做幂等键 | **取决于网关**,taskgate 无能为力 | **只能防到"意图不丢不重"**,投递仍是 at-least-once |
| 手段 | 数据库唯一约束 / 幂等表,和业务写入同一事务 | 网关支持 idempotency key 就传 `Task.ID`;不支持只能接受重复调用 | 事务性 outbox:把"要发这条通知"随业务事务落库;**投递端/接收方仍要按幂等键去重** |

第三类容易被误当成第一类:outbox 只保证通知**意图**和业务数据同生共死,通知本身发出去这一步照样可能重复——邮件、Webhook 的接收方看到的还是 at-least-once,该带的幂等键(比如 `Task.ID`)一个都不能省。

#### 本地副作用:用 `Task.ID` 原子保护

`Task.ID` 在重跑时不变,拿它当业务幂等键。**关键是让"扣额度"和"落库"在同一个事务里、用同一个唯一键**——分两步做就还是有窗口。

```go
g.Handle("summarize", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
    resp, err := callLLM(ctx, t.Payload)
    if err != nil {
        return nil, err
    }
    // 扣额度 + 落库在同一事务内完成,以 t.ID 为唯一键;
    // 重跑时唯一约束冲突 → 返回首次的结果,不会二次扣费。
    return db.SaveOnce(ctx, t.ID, userID, resp)
})
```

#### 外部调用:taskgate 消不掉这个窗口

上面这段**不能阻止 LLM 被调用两次**。进程如果崩在 `callLLM` 成功之后、`db.SaveOnce` 之前,重跑必然会再调一次网关——**这一次的钱照花**,只是不会重复扣用户额度、不会重复落库。

这个窗口是 at-least-once 的固有代价,处理方式只有两条:

- **网关明确支持幂等键时**,把 `t.ID` 作为幂等键传入,网关自己去重,重跑不产生第二次计费。**这是唯一能真正消除重复调用的办法。** 支付类网关普遍支持(如 Stripe 的 `Idempotency-Key`);LLM 网关是否支持、哪些端点支持,**以网关官方文档为准**,不要想当然。
- **网关不支持**:接受重复调用的成本,或在业务协议层做补偿(对账、按 `t.ID` 追溯去重)。taskgate 帮不上忙——"外部调用已成功、但本地还没落账"的不确定窗口,任何任务队列都消不掉。

> **`WithBusinessKey` 幂等的是"提交",不是"执行"。** 同键重复 `Submit` 会返回 `ErrTaskExists`,保证同一件事只入队一次;但入队之后,这一个任务的 handler 仍然可能因为崩溃回收而跑多次。这是两件事,别混。(旧的 `WithID` 已弃用,现在是 `WithBusinessKey` 的别名——传入的值是业务键,不再是任务 ID。)

> **不要把关键副作用放进 `OnStateChange`。** 它是异步通知,不保证送达、不保证顺序、回调 panic 会被吞掉、也没有重试和"全局只消费一次"的保证。它只适合埋点和允许丢失/重复的观测逻辑,承载不了扣费、发通知、写业务终态。

### 为什么不做 exactly-once

这是分布式系统的老大难:handler 的执行和它的副作用落地不在同一个事务里,库层面给不了这个保证——任何"跑完就标记完成"的方案,都存在"跑完了、还没来得及标记就崩了"的窗口。能给的只有 at-least-once + 一个重跑时稳定的 `Task.ID`,让你在业务侧自己做幂等。

## handler 的错误语义

handler 返回什么错,决定任务走哪条重试路径:

```go
return nil, taskgate.ErrThrottled{RetryAfter: 30 * time.Second} // 网关限流:延后重排,不占重试次数
return nil, taskgate.ErrSkipRetry{Err: err}                     // 没救的错:直接进 failed 死信
return nil, err                                                 // 普通业务失败:指数退避重试
```

> **注意**:`ErrThrottled` / `ErrSkipRetry` 必须**按值返回**(`errors.As` 按值匹配),不要返回其指针。

提交选项:`WithBusinessKey`(业务幂等键;`WithID` 已弃用为其别名)、`Delay` / `RunAt`(延迟执行)、`MaxRetry`、`DependsOn`、`IgnoreParentFailure`。

## 长任务与手动续租

默认(自动档)scheduler 给每个在跑任务起自动心跳,每 `LeaseTTL/3` 续一次租,handler 什么都不用管。两种情况需要手动续租:

- **自动档里想顺手多续一口**:handler 在任务 ctx 上随时可调 `taskgate.RenewLease(ctx)`,与自动心跳共用同一租约令牌,幂等延长,互不干扰。
- **毒任务检测要更灵敏**:自动心跳是调度器发的,handler 卡死心跳照跳,任务永远不会被回收。把队列的 `ManualHeartbeat` 设为 `true` 关掉自动心跳,handler 每处理完一个检查点自己续一次——卡死就停止续租,租约到期被 reaper 回收重跑。

```go
Queues: map[string]taskgate.QueueConfig{
    "ocr": {Workers: 2, LeaseTTL: taskgate.Duration(60 * time.Second), ManualHeartbeat: true},
},

g.Handle("ocr", func(ctx context.Context, t *taskgate.Task) ([]byte, error) {
    for _, page := range pages {
        if err := taskgate.RenewLease(ctx); err != nil {
            return nil, err // ErrTaskCanceled / ErrLeaseLost 时 ctx 已被 cancel,尽快退出
        }
        // ... 处理一页 ...
    }
    return result, nil
})
```

`RenewLease` 的返回值语义:

| 返回 | 含义 | handler 该做什么 |
|---|---|---|
| `nil` | 续租成功 | 继续干活 |
| `ErrTaskCanceled` | 任务被外部 Cancel(续租照做) | ctx 已被 cancel,尽快退出 |
| `ErrLeaseLost` | 租约已丢(被 reaper 回收),结果注定作废 | ctx 已被 cancel,立即放弃 |
| `ErrNoTask` | ctx 不是任务 ctx(handler 之外调用) | 修代码 |
| 其他错误 | 网络抖动,租约没成也没丢 | 可稍后重试 |

**手动档的跨进程取消语义**:关掉自动心跳后,别的进程发起的 `Cancel` 只能由 handler 的下一次 `RenewLease` 发现(返回 `ErrTaskCanceled`);handler 一直不续租,则由租约过期兜底回收。同进程的 `Cancel` 不受影响,依然即时 cancel handler 的 ctx。手动档的语义就是"handler 对自己的租约负全责"。

## List 分页

`List` 结果按 `(CreatedAt, ID)` 升序稳定排序(三后端一致),`Filter` 支持 `Offset+Limit` 翻页:先过滤 → 排序 → 跳过 `Offset` 条 → 取 `Limit` 条(0 = 不限);`Offset` 越界返回空列表不报错。

```go
page2, _ := g.List(ctx, taskgate.Filter{Type: "ocr", Limit: 20, Offset: 20})
```

两条心里有数:

- **弱一致翻页**:翻页期间有任务入队/流转时不承诺快照一致,只承诺"未变动的任务不丢不重";要强一致游标等 M4 再议。
- **redis 后端的代价**:List 走"索引集合求候选 → 逐个取回 → 内存排序切片",复杂度是 O(候选集) 而不是 O(页大小);大库存时先用 `Filter` 的 Type/Status/Queue 把候选集缩小再翻页。

## Redis 后端(多进程)

多个 worker 进程接同一个 Redis 抢同一批任务时用它:Lua 原子流转保证**每个任务在同一时刻只有一个有效租约**,状态流转不会被并发撕裂;进程 `kill -9` 后在跑任务由租约回收**重跑**。

> 注意这不等于"handler 绝不会并行重叠":租约失效后新 worker 可以重新认领,而旧 handler 可能因网络分区、进程暂停(STW/换页)或没及时响应 ctx 而**还没退出**,两份业务代码可能短暂并行。所以仍然是 at-least-once,handler 必须幂等——见[handler 的执行契约](#handler-的执行契约必须幂等)。

换后端只改构造这一行,其余代码零改动:

```go
b, err := redisbroker.New(redisbroker.Options{
    Addr:      "127.0.0.1:6379",
    Password:  "",    // 空 = 不认证
    DB:        0,
    KeyPrefix: "tg:", // 默认 "tg:";多应用共用一个 Redis 时用它隔离
})
if err != nil { ... }
g, err := taskgate.New(taskgate.Config{Broker: b, Queues: ...})
```

所有"多步读写必须原子"的操作(认领、终态+依赖唤醒、连锁取消、计数维护)都在单段 Lua 脚本内完成,不存在"任务已离队却没有租约"的崩溃窗口;"父完成但子未唤醒"不可被观测到。

### 分布式限流:多进程共享配额

redis 后端额外实现了 `LimiterProvider` 能力接口,同一队列的 `{Workers, RPS}` 配额在**所有接同一 Redis 的进程间共享**——两个进程各配 `{Workers: 2}`,全局同时在跑的也是 2 个,不是 4 个;RPS 走 GCRA(redis_rate),同样是全局速率。memory/sqlite 后端不受影响,维持进程内限流。

并发槽的自愈:每占一个槽记一个过期时刻(= 队列的 `LeaseTTL`),持有进程每 `LeaseTTL/3` 自动续期;进程崩溃 → 续期停 → 槽到期自动回收,最坏 `2×LeaseTTL` 内配额可再用,不会永久泄漏。

### 跨进程延迟(心里有数)

- **新任务被别的进程发现**:同进程提交有内部唤醒信号即时响应;别的进程写入靠 Dequeue 的兜底轮询发现,最坏 **≤100ms**(与 sqlite 跨进程一致)。
- **跨进程 Cancel**:running 任务的取消标记由持有它的进程在下一次心跳发现,最坏 **≤ 一个心跳周期(LeaseTTL/3)** 后其 handler ctx 被 cancel。
- **崩溃任务被捞回**:从最后一次成功续租算起,`LeaseTTL`(租约失效)+ 最坏 `LeaseTTL/2`(reaper 每 `min(各队列 LeaseTTL)/2` 扫一次)= **`LeaseTTL` ~ `1.5×LeaseTTL`**(默认 60s → 60~90s)才会回到 pending;之后还要重新排队、占槽、等令牌才真正开跑。要更快发现崩溃就调小 `LeaseTTL`,代价是心跳更频繁(每 `LeaseTTL/3` 一次)。

### Redis 键名速查(运维直查)

默认前缀 `tg:`(`Options.KeyPrefix` 可改)。积压、在跑数不用走应用,`redis-cli` 直接看:

| 键 | 类型 | 用途 | 直查示例 |
|---|---|---|---|
| `tg:task:{id}` | hash | 任务全字段(时间存 unix 毫秒) | `HGETALL tg:task:01J...` |
| `tg:pending:{queue}` | list | 就绪任务 ID 队列(FIFO) | `LLEN tg:pending:scoring` |
| `tg:delayed:{queue}` | zset | 延迟/重试退避任务,score=run_at | `ZCARD tg:delayed:scoring` |
| `tg:inflight` | zset | 在跑任务,score=租约到期时刻 | `ZCARD tg:inflight` |
| `tg:children:{id}` | set | 反向依赖索引(依赖 {id} 的子任务) | `SMEMBERS tg:children:01J...` |
| `tg:idx:status:{status}` | set | 状态索引(七态各一) | `SCARD tg:idx:status:failed` |
| `tg:idx:type:{type}` | set | Type 索引(List 过滤用) | `SCARD tg:idx:type:ocr` |
| `tg:stats` | hash | Type×Status 计数,字段 `{type}:{status}` | `HGETALL tg:stats` |
| `tg:types` | set | 出现过的 Type | `SMEMBERS tg:types` |
| `tg:sem:{queue}` | zset | 分布式并发槽(限流器私有) | `ZCARD tg:sem:scoring` |
| `rate:tg:{queue}` | string | RPS 限速状态(redis_rate 的 GCRA 内部,限流器私有)。注意 `rate:` 前缀由 redis_rate 加在最外层,该键**不在** `KeyPrefix` 命名空间内(实际键名 = `rate:` + KeyPrefix + 队列名),按前缀批量清理时别漏 | `GET rate:tg:scoring` |

`Counts`/`Overview` 就是读 `tg:stats`(每次流转时 Lua 顺手 HINCRBY 维护),`QueueLen` 就是 `LLEN + ZCARD`,都是计数器/长度读取,不扫全库。

### 测试与限制

- **契约测试双档**:miniredis 档离线进 CI;设 `TASKGATE_REDIS_ADDR=127.0.0.1:6379` 后同一套 22 条契约在真 Redis 上再跑一遍(随机 KeyPrefix 隔离、测后清理),验证 Lua 脚本兼容性。
- **不支持 Redis Cluster**:脚本内用前缀自行拼键、键不带 hash tag,面向单实例/主从/哨兵拓扑。
- **限流键与任务键在同一个 Redis 实例**:flushdb 级故障两者同生共死(诚实的取舍)。

## SQL 后端:PostgreSQL / MySQL(多进程)

已经有一套 PostgreSQL 或 MySQL,不想再引一个 Redis 时用它:多个 worker 进程接同一个库抢同一批任务,认领互斥靠 `FOR UPDATE SKIP LOCKED`(要求 MySQL 8.0+ / PostgreSQL 9.5+),同样过 22 条契约。和 redis 一样,换后端只改构造函数一行,业务代码零改动:

```go
import "github.com/AmbroseX/taskgate/pgbroker"
b, err := pgbroker.Open("postgres://user:pass@localhost:5432/db?sslmode=disable")
```

```go
import "github.com/AmbroseX/taskgate/mysqlbroker"
b, err := mysqlbroker.Open("user:pass@tcp(localhost:3306)/db")
```

可选项:`WithTablePrefix("myapp_")`(默认 `taskgate_`,多应用共享一个库时用它隔离)、`WithMaxOpenConns(n)`(默认 10)、`WithPollInterval(d)`(默认 200ms)。首次 `Open` 后 `Init` 自动建表(`CREATE TABLE IF NOT EXISTS`,冷启动并发安全)。

### 已知限制

- **跨进程新任务感知延迟 = 轮询间隔**(默认 200ms,可调);未实现 PG LISTEN/NOTIFY 即时唤醒。
- **不提供分布式限流**:SQL 后端不实现 `LimiterProvider`,scheduler 自动退回进程内限流,多进程各限各的;需要精确跨进程限流请用 redis 后端。
- **高并发依赖传播冲突时**,事务会经历数据库死锁自动重试(有上限,默认 5 次),表现为个别调用延迟抬高;重试超限会把死锁错误原样抛出。
- **MySQL 专属**:自定义 ID/type/queue 最长 255 字符(`Enqueue` 入口校验,超限清晰报错);payload/result 受服务器 `max_allowed_packet` 限制(默认 64M);表用 `utf8mb4_bin` 排序规则(DDL 内置,自定义 ID 大小写敏感)。
- **契约测试需要真库**(env 门控 `TASKGATE_PG_DSN` / `TASKGATE_MYSQL_DSN`),本地无库时 skip——本地全绿不代表跑过这两个后端,回归靠 CI。

## Config 说明

库自己不读任何配置文件。`Config` 的字段带 `yaml`/`json` tag,应用自己 unmarshal 好再传进来:

```yaml
# 应用自己的配置文件(taskgate 不读它,由应用 unmarshal 后注入)
queues:
  llm:
    workers: 2       # 并发上限(必填,>=1)
    rps: 3           # 每秒放行数,0 = 不限速
    burst: 3         # 突发额度,0 时取 max(1, int(rps))
    lease_ttl: 60s   # 租约时长,0 补默认 60s
    quota_limit: 5000   # 周期配额:每窗口最多启动 5000 次 handler,0 = 不启用
    quota_period: 24h   # 窗口时长(≥1s;固定窗口对齐 epoch,24h 即 UTC 零点,不是当地自然日)
    quota_key: my-gw    # 配额键,空 = 队列名;多队列同键 = 共享同一份窗口预算
  cpu:
    workers: 4
routes:              # 任务类型 → 队列;没配的类型用类型名当队列名
  generate: llm
default_queue:       # 兜底队列,可整个不配
  workers: 2
lease_lost_max: 3    # 租约丢失封顶(默认 3),超过进 failed
throttled_max: 100   # 限流重排封顶(默认 100),超过进 failed
```

```go
var cfg taskgate.Config
_ = yaml.Unmarshal(raw, &cfg)      // 应用自己解;Duration 字段支持 "60s"、"10m" 写法
cfg.Broker = memorybroker.New()    // 运行期对象手动注入
cfg.OnStateChange = func(t taskgate.Task) { /* 埋点 */ }
g, err := taskgate.New(cfg)
```

### 频率 ≠ 配额:周期配额(硬配额)

`RPS` 管"每秒别把网关打爆",`QuotaLimit` 管"每个窗口累计别超预算"——**这是两码事**:网关允许你每天调 5000 次,不代表你必须匀速摊到每 17 秒一次,你可以一口气用完然后歇一天。三个维度正交并存:`Workers` 限同时在跑、`RPS` 限每秒新启动、`QuotaLimit` 限每窗口累计启动。

```go
Queues: map[string]taskgate.QueueConfig{
    "llm": {
        Workers: 2, RPS: 3,                       // 别打爆:2 并发、每秒 3 个
        QuotaLimit: 5000,                          // 别超支:每窗口最多启动 5000 次 handler
        QuotaPeriod: taskgate.Duration(24 * time.Hour),
    },
}
```

**合同(如实版)**:

- **硬配额,绝不超发**:窗口计数在共享介质里原子扣减(所有接同一介质的进程合计),任何故障——进程崩溃、介质断连、退还失败——方向都是**只少放行,不多放行**;
- 窗口是**对齐 epoch 的固定时长**(`24h` = UTC 零点重置),**不是**当地自然日、不是自然月、也对不齐网关的账单周期;窗口时间用介质的服务端钟,应用机器的时钟偏差不影响;
- 配额单位是 **handler 启动次数**,不是任务数(重试的再次认领同样计一次),也不是 token 用量;
- 两个消耗漏口要心里有数:handler 启动后失败/被取消,以及类型没注册 handler 被判死信,**这一次配额都已经消耗**;
- 额度耗尽**不是错误**:队列停止认领(不占 worker 槽),任务老实待在 pending 等下个窗口,不进 `Throttled` 计数、不进 failed;`Stats(queue)` 的 `QuotaExhausted` 位可见;
- 配额介质不可达时 **fail-closed**:暂停认领、零放行、退避重试(`QuotaStalled` 位可见)——绝不退回进程内计数假装还有保护;
- 后端必须支持共享计数(五个内置后端都支持:memory=进程内、sqlite=库文件、redis/pg/mysql=服务器);不支持的后端配了配额,`New()` 直接报错。

## 边角用法

一些常用的小写法:

```go
// 幂等提交:同一业务键重复提交返回 ErrTaskExists,不会重复入队。
// 任务 ID(ExecutionID)由系统生成并从 Submit 返回,业务键与任务 ID 是两个概念。
id, err := g.Submit(ctx, "generate", payload, taskgate.WithBusinessKey("order-42"))
var te *taskgate.TaskExistsError
if errors.As(err, &te) { /* 已排过队;te.ExecutionID/te.Status 是该键下最新执行 */ }

// 失败重跑(Replay):终态执行可以重放成一个新执行,旧记录永远不变。
// 典型 cron 配方:确定性业务键防双触发;失败后从拒绝错误里拿链尾,显式 Replay。
if te != nil && te.Status == taskgate.StatusFailed {
    newID, _ := g.Replay(ctx, te.ExecutionID)          // 新执行,ReplayOf 指回旧执行
    _ = newID
}
g.ReplayByKey(ctx, "order-42")                          // 按键重放,作用于该键最新执行
g.Replay(ctx, id, taskgate.AllowCompleted())            // 重放已成功的执行必须显式允许
g.Replay(ctx, id, taskgate.WithPayload(newPayload))     // 参数修正后重跑(默认复制旧 Payload)
history, _ := g.History(ctx, "order-42")                // 该键下的执行历史链(旧 → 新)

// 延迟执行:相对延迟 or 绝对时刻,二选一。
g.Submit(ctx, "reminder", payload, taskgate.Delay(30*time.Minute))
g.Submit(ctx, "reminder", payload, taskgate.RunAt(time.Now().Add(time.Hour)))

// 覆盖这条任务的重试上限(默认走 Config 的封顶)。
g.Submit(ctx, "flaky", payload, taskgate.MaxRetry(1))

// 扇入:一个任务依赖多个父任务,全部完成才唤醒。
g.Submit(ctx, "merge", nil, taskgate.DependsOn(idA, idB, idC))

// 父失败不连锁取消我(默认父 failed 会连锁取消子)。
g.Submit(ctx, "cleanup", nil, taskgate.DependsOn(job), taskgate.IgnoreParentFailure())

// 阻塞等最终结果 / 主动取消 / 查一条。
result, err := g.Wait(ctx, id)
err = g.Cancel(ctx, id)
task, err := g.Get(ctx, id)

// 看某队列积压与在跑数 / 全局各态计数。
stats, _ := g.Stats(ctx, "llm")
overview, _ := g.Overview(ctx)
```

## 错误类型速查

所有错误都导出,用 `errors.Is` / `errors.As` 判断:

```go
// 哨兵错误(errors.Is)
taskgate.ErrTaskExists    // 业务键下已有执行(WithBusinessKey 幂等时会碰到;errors.As 可解构 *TaskExistsError 拿链尾)
taskgate.ErrTaskNotFound  // 任务不存在(Get/Cancel 找不到,或依赖的父任务缺失)
taskgate.ErrLeaseLost     // 租约令牌不匹配:任务已被回收或被别人重认领,结果作废
taskgate.ErrTaskCanceled  // 任务被打了取消标记,handler 该退出了
taskgate.ErrAlreadyFinal  // 对已进终态的任务再 Cancel
taskgate.ErrUnknownType   // Run 时遇到没注册 handler 的任务类型
taskgate.ErrShutdown      // Gate 已 Shutdown,拒绝新提交
taskgate.ErrNoTask        // 在 handler 之外的 ctx 上调 RenewLease
taskgate.ErrReplayNotFinal      // Replay 目标还没进终态
taskgate.ErrAlreadyReplayed     // Replay 目标已被重放过(历史链不分叉)
taskgate.ErrCompletedNotAllowed // 重放 completed 执行必须显式 AllowCompleted()

// 结构化错误(errors.As;handler 返回它们控制重试路径,必须按值返回)
taskgate.ErrThrottled{RetryAfter: d} // 网关限流:延后重排,不占重试次数
taskgate.ErrSkipRetry{Err: err}      // 没救的错:直接进 failed;Unwrap 可穿透到原错误
```

## 运行测试

全量离线可跑(L1 单元 → L2 brokertest 契约 → L3 集成 → L4 仿真 E2E):

```bash
go test ./... -race
```

想在真库上再跑一遍 22 条契约(可选;Redis 验证 Lua 脚本兼容性,PG/MySQL 本地无库时自动 skip):

```bash
TASKGATE_REDIS_ADDR=127.0.0.1:6379 go test ./redisbroker/... -race
TASKGATE_PG_DSN="postgres://postgres:pass@localhost:5432/postgres?sslmode=disable" go test ./pgbroker/... -race
TASKGATE_MYSQL_DSN="root:pass@tcp(localhost:3306)/taskgate" go test ./mysqlbroker/... -race
```

## 测试分层与 e2e

`e2e/` 目录是 L4/L5 仿真:

- **`e2e/mockgw/`**:可注入故障的 mock LLM/OCR 网关(测试组件,不属库 API)。把生产踩过的坑做成开关:`Latency`(延迟)、`BusyAfterConcurrency`(并发超限返 HTTP 200 但 body 里藏 busy 错误事件——复刻"状态码骗人"的真实网关)、`FailRate`(固定种子随机 500,CI 可复现)、`CrashAfterConcurrency`(并发超限直接断连)、`BusyFirstN`(前 N 个请求定向 busy);暴露 `MaxConcurrency/BusyCount/CrashCount/Requests` 原子观测口。
- **`e2e/pipeline_test.go`**:五个核心用例——限流真的挡住 busy、busy 走 `ErrThrottled` 重排零 failed、断连走普通重试补完、三队列流水线 30/30 且结果逐级传递、中途取消连锁生效、SSE 藏错误重排后成功。
- **`e2e/realgw_test.go`**:真实网关冒烟档,`//go:build realgw` 隔离,**不进 CI**(常规 `go vet`/`go test` 完全不编译它);读 `LLM_GATEWAY_URL`/`LLM_GATEWAY_KEY`(缺失自动 skip),手动执行:

```bash
LLM_GATEWAY_URL=https://网关地址 LLM_GATEWAY_KEY=密钥 \
  go test -tags realgw ./e2e/ -run RealGW -v
```

## 设计文档

- [任务排队限流库 taskgate 方案](docs/plans/2026-07-14-任务排队限流库taskgate方案.md)(架构、Broker 合同、状态机)
- [测试方案](docs/plans/2026-07-14-测试方案.md)(分层测试、故障专项与性能基线)
- spec-kit 产物:[specs/001-m1-core-queue/](specs/001-m1-core-queue/)(M1)、[specs/002-m2-redis/](specs/002-m2-redis/)(M2)、[specs/003-m3-polish/](specs/003-m3-polish/)(M3)

## 里程碑

- **M1(已完成)**:核心排队、限流、重试、依赖、取消、Shutdown,memory / sqlite 双后端。
- **M2(已完成)**:redis 后端(Lua 原子流转、多进程认领互斥)、分布式限流(跨进程共享配额)、性能基线。
- **M3(已完成)**:L4 仿真 E2E(mockgw 故障注入五用例)、handler 手动续租(`RenewLease`/`ManualHeartbeat`)、List 分页、realgw 手动冒烟档。

明确不做(YAGNI):任务优先级、webhook 通知、游标分页、Web UI、cron 周期调度、DAG 工作流引擎、独立 server 模式。
