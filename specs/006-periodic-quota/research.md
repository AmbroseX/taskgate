# Research: 周期配额 Periodic Quota

## 1. 能力接口形态:独立 `QuotaProvider`,fail-fast 不降级

**问题**: 调研方案 5.4 列了四条路(独立能力接口 / 拆三 provider / Broker 原子认领 / SQL 实现整包 LimiterProvider),选哪条?

**研究结果**: `LimiterProvider` 是整包替换 + 静默退化,配额直接复用它会被迫连 Workers/RPS 一起接管(方案 5.4 已论证不成立);拆三 provider 动现有公开接口,回归面大;Broker 原子认领(admission/claim)破坏后端无关性,是基准不过关时的备选路线;SQL 整包实现 LimiterProvider 已被方案排除。

**Decision**: 新增独立能力接口,与 LimiterProvider 平行:

```go
// broker.go
type QuotaProvider interface {
    QueueQuota(queue string, qc QueueConfig) (QuotaGate, error)
}
type QuotaGate interface {
    // 三态:res≠nil 预留成功;res==nil && err==nil 本窗口耗尽(非错误);
    // err≠nil 介质故障,调用方必须 fail-closed。
    Reserve(ctx context.Context) (*QuotaReservation, error)
    // Release 尽力退还,只作用于 r 的窗口;失败即 leaked(保守),调用方不重试。
    Release(ctx context.Context, r *QuotaReservation) error
}
type QuotaReservation struct{ Window int64 } // 窗口起点,unix 秒,介质服务端钟
```

与 LimiterProvider 的关键差异:**没有退化分支**——`New()` 时发现任一队列 `QuotaLimit>0` 而 `cfg.Broker` 未实现 `QuotaProvider`,直接报错(模型裁决 #3"不存在静默降级")。

**Alternatives considered**: 见上;另考虑过 Reserve 返回 `(ok bool, ...)` 三值——reservation 对象反正要携带窗口(退还必须只退预留窗口),用指针 nil 表达耗尽最少形状。

## 2. 预留插在认领链哪一环 + 阻塞 Dequeue 的暴露面

**问题**: 现有顺序"占槽 → 令牌 → Dequeue"(scheduler.go:248 顺序写死);预留插哪?Dequeue 是阻塞式的,预留在手里挂多久?

**研究结果**: 预留越靠后,"预留到认领完成"的窗口越短,崩溃泄漏与白烧暴露面越小;槽和令牌是廉价的进程内资源,瞬时持有后释放无副作用(模型问题 #7 预判)。但 Dequeue 会阻塞到有任务为止——拿着预留无限期阻塞,进程一崩就漏一份。

**Decision**: 顺序 = 占槽 → 令牌 → `QueueLen>0` 启发式 → **Reserve** → **限时 Dequeue**:

- 启发式:队列空则释放槽、按退避等待,不预留(消掉常态白烧,spec Q2);
- 限时:配额启用时 Dequeue 用 `context.WithTimeout(ctx, min(QuotaPeriod, 3s))` 兜底,超时 → 尽力 Release → 释放槽重新循环。启发式已把这条路压成罕见路径,超时值不影响正确性只影响泄漏暴露上限;
- 耗尽(res==nil):释放槽,置 `QuotaExhausted`,按 `min(QuotaPeriod/8, 1s)`(下限 10ms)的间隔经注入 clock 等待后重试;
- 介质故障(err≠nil):释放槽,置 `QuotaStalled`,同样退避重试——fail-closed,零放行。

**Rationale**: 全部等待走注入 clock(测试确定);唯一的真时间是 Dequeue 兜底超时,它是安全界不是行为语义,测试不依赖它。

**Alternatives considered**:
- 预留放占槽之前:预留持有期covers槽等待+令牌等待,泄漏暴露面大好几倍,否决;
- 非阻塞 Dequeue(加 TryDequeue 接口):动 Broker 接口 15 个方法的合同,为一个兜底超时不值,否决。

## 3. 各介质的"检查+扣减"原子形态与服务端时间

**问题**: 五介质怎么在一个原子单位里完成"取服务端时间 → 算窗口 → 检查余额 → 扣减"?

**Decision**(原型已验 sqlite,其余按同构推):

| 介质 | 原子形态 | 服务端时间 |
|---|---|---|
| memory | 单锁临界区内 `quota[key][win]++` | 注入 Clock(介质=本进程,Clock 即介质钟;fakeclock 天然可控) |
| sqlite | 原型同款单语句:`INSERT ... ON CONFLICT(qkey,win) DO UPDATE SET used=used+1 WHERE used<? RETURNING win` | `strftime('%s','now')`(本机钟=文件介质的钟);测试缝:语句写成 `COALESCE(?, strftime(...))`,生产传 NULL |
| PG | 同 sqlite(PG 支持 ON CONFLICT..WHERE + RETURNING) | `EXTRACT(EPOCH FROM now())::bigint`;测试缝同 COALESCE |
| MySQL | **两步**:先 `SELECT UNIX_TIMESTAMP()` 取时算窗,再 `INSERT ... ON DUPLICATE KEY UPDATE used=IF(used<?,used+1,used)`,按 affected rows 判定(1=insert、2=真更新、0=没变即耗尽) | 取时与扣减分两步**不破坏硬配额**:原子性要求在"检查+扣减"上(单条 ODKU 满足);时间读取只决定打在哪个窗口行,边界竞态最多把预留记进上一窗(仍是合法窗口,不多放) |
| redis | 单段 Lua:`TIME` 算窗 → `GET` 检查 → `INCR` + `EXPIRE`(同脚本,无 5.4.1 的崩溃窗口) | `redis.call('TIME')`——quota 脚本是"禁 TIME"约定的唯一豁免(plan 的 Complexity Tracking);测试用 miniredis(已核实 v2.38 支持 `TIME` 命令与 `SetTime()`,介质时间可控) |

MySQL affected-rows 判定依据:go-sql-driver 默认不带 CLIENT_FOUND_ROWS,`IF(used<?,used+1,used)` 没真变更时 affected=0——正是"耗尽"信号。

**Alternatives considered**: MySQL 用 `ROW_COUNT()` 再查——多一次往返且同连接约束,affected rows 由驱动直接返回,更简;redis 用应用侧传时间——违反裁决 #5,否决。

## 4. 窗口行清理:轮换时机会主义删除

**问题**: SQL 介质每窗口每 key 一行,长期堆积。

**Decision**: 每个 QuotaGate 记住上次预留的窗口起点;预留发现窗口轮换(win ≠ last)时,**尽力**执行一次 `DELETE ... WHERE qkey=? AND win < win - 2×period`(独立语句,失败忽略)。redis 用 `EXPIRE 2×period` 自清;memory 在同一临界区顺手删旧窗。

**Rationale**: 每窗口每进程至多一次删除,摊销为零;删的是已作废窗口,与正确性无关。

**Alternatives considered**: 不清理(v1 留债,被否:1s 级测试窗口会堆很快);后台清理 goroutine(新机制,YAGNI)。

## 5. 合规套件与介质时间控制

**问题**: 配额行为要五后端同套题验收,但各介质"推时间"的方式不同。

**Decision**: `brokertest/quota.go` 新增独立入口:

```go
type QuotaFactory func(t *testing.T, opts taskgate.BrokerOptions) (b taskgate.Broker, advance func(time.Duration))
func RunQuota(t *testing.T, factory QuotaFactory)
```

`advance` 由各后端测试文件提供:memory 推 fakeclock;sqlite/pg/mysql 调各包导出的测试钩子(`export_test.go` 风格的包级 offset,叠加在服务端时间上);redis 推 miniredis 的 `SetTime`。套件用例只调 `advance`,不知道介质细节。用例清单见 contracts/。

**Rationale**: 与 brokertest 主套件同一工厂模式;时间控制责任下放到最了解介质的后端测试文件。

**Alternatives considered**: BrokerOptions.Clock 直接当配额时间源——违反裁决 #5(生产不得依赖 Go 层钟),测试钩子必须与生产路径分离,否决。

## 6. scheduler 状态暴露与 New() 校验位置

**Decision**:
- `Config.validate()`(taskgate.go):`QuotaLimit<0`/`QuotaPeriod<0` 报错;`QuotaLimit>0 && QuotaPeriod<=0` 报错;汇总所有队列(含 DefaultQueue)的**解析后 quota key**(空取队列名),同 key 不同 (limit, period) 报错。
- `newGate()`(client.go):任一队列启用配额而 broker 未实现 `QuotaProvider` → 报错(能力断言提早到 New,不等 Run)。
- `QueueQuota` 实例在 `run()` 里与 limiter 一起构造(那时才知道消费哪些队列);同 key 多队列各持一个实例——介质计数按 key 共享,实例无共享状态。
- `scheduler` 每队列一个 `quotaState{exhausted, stalled atomic.Bool}`;`Gate.Stats` 读它填 `QueueStats.QuotaExhausted/QuotaStalled`。

**Alternatives considered**: 状态走回调(`Config.OnQuotaEvent`)——新增回调面、与 Notify 的"不保证送达"合同纠缠,Stats 轮询与现有观测风格一致,否决。
