# Data Model: 周期配额

## 1. Go 类型

### QueueConfig(taskgate.go,扩展)

| 字段 | 类型 | 默认值 | 语义 |
|---|---|---|---|
| `QuotaLimit` | int | 0 = 不启用 | 每窗口最多启动的 handler 次数;`yaml/json:"quota_limit"` |
| `QuotaPeriod` | Duration | 0 | 窗口时长;启用时必须 >0;`yaml/json:"quota_period"`(支持 "24h" 写法) |
| `QuotaKey` | string | "" = 队列名 | 配额键;多队列同 key 共享预算;`yaml/json:"quota_key"` |

校验(`validate()` fail-fast):
- `QuotaLimit < 0` / `QuotaPeriod < 0` → 错;
- `QuotaLimit > 0 && QuotaPeriod <= 0` → 错;
- 解析后同 key(空取队列名;含 DefaultQueue)出现不同 `(QuotaLimit, QuotaPeriod)` → 错。

`newGate()` 追加:任一队列启用配额且 `cfg.Broker` 未实现 `QuotaProvider` → 报错(错误信息含队列名与字段名)。

### 能力接口(broker.go,新增;Broker 接口本体零改动)

```go
type QuotaProvider interface {
    QueueQuota(queue string, qc QueueConfig) (QuotaGate, error)
}
type QuotaGate interface {
    Reserve(ctx context.Context) (*QuotaReservation, error) // 三态,见合同
    Release(ctx context.Context, r *QuotaReservation) error // 尽力退还,只退 r.Window
}
type QuotaReservation struct {
    Window int64 // 窗口起点,unix 秒(介质服务端钟);Release 的定位依据
}
```

### QueueStats(client.go,扩展)

| 字段 | 类型 | 语义 |
|---|---|---|
| `QuotaExhausted` | bool | 本窗口额度已尽,认领暂停等下窗 |
| `QuotaStalled` | bool | 介质不可达,fail-closed 暂停中 |

### scheduler(scheduler.go)

- `quotaState{exhausted, stalled atomic.Bool}`,每消费队列一个,`running` 同款 map;
- claimLoop 常量:`quotaRecheckFloor=10ms`;耗尽/故障退避 = `min(QuotaPeriod/8, 1s)`(经注入 clock);Dequeue 兜底超时 = `min(QuotaPeriod, 3s)`(真时间,安全界)。

## 2. 状态机

任务七状态机**零变更**。配额预留自身的生命周期(非持久化状态机):

```text
Reserve(原子) ──成功── reserved ──认领成功── consumed(额度落定)
     │                    ├──认领扑空/出错,Release──→ released(计数减回;窗口已切走则落空无害)
     │                    └──进程崩溃/Release 失败──→ leaked(视同 consumed,保守)
     ├──res=nil:本窗口耗尽(非错误)→ 释放槽、退避、下窗恢复
     └──err≠nil:介质故障 → fail-closed,零放行,退避重试
```

计数语义:Attempts/LeaseLost/Throttled **全都不动**;耗尽与故障不产生任何任务状态变化。

## 3. sqlite(sqlitebroker/schema.sql + 新 quota.go)

```sql
CREATE TABLE IF NOT EXISTS quota (
    qkey TEXT    NOT NULL,
    win  INTEGER NOT NULL,  -- 窗口起点(unix 秒,对齐 period)
    used INTEGER NOT NULL,
    PRIMARY KEY (qkey, win)
);
```

```sql
-- Reserve:一条原子语句(原型验证过)。?1=qkey ?2=测试时间覆盖(生产 NULL) ?3=period ?4=limit
INSERT INTO quota (qkey, win, used)
VALUES (?, CAST(COALESCE(?, strftime('%s','now')) AS INTEGER) / ? * ?, 1)
ON CONFLICT (qkey, win) DO UPDATE SET used = used + 1 WHERE used < ?
RETURNING win;
-- Release(尽力):
UPDATE quota SET used = used - 1 WHERE qkey = ? AND win = ? AND used > 0;
-- 窗口轮换时的机会主义清理(尽力):
DELETE FROM quota WHERE qkey = ? AND win < ?;
```

- 结果三态:扫到 win = 成功;`sql.ErrNoRows` = 耗尽;其他 error = 介质故障(fail-closed)。
- 测试时间缝:包级 `testQuotaNow func() int64`(export_test.go 设置),非 nil 时其值作 COALESCE 首参,生产恒传 NULL。
- 共用 broker 的单连接池(MaxOpenConns=1),busy_timeout 即"介质不可达"的判定上限。

## 4. PG / MySQL(internal/sqlbroker 新 quota.go + Dialect 差异)

表同 sqlite(前缀 `{{prefix}}quota`;类型映射按各库惯例:PG TEXT/BIGINT,MySQL VARCHAR(255) COLLATE utf8mb4_bin / BIGINT)。

- **PG**:与 sqlite 同构单语句——`ON CONFLICT (qkey, win) DO UPDATE SET used = {{q}}.used + 1 WHERE {{q}}.used < ? RETURNING win`;时间 `CAST(EXTRACT(EPOCH FROM now()) AS BIGINT)`,测试缝 COALESCE。
- **MySQL**:两步(research 决策 3):①`SELECT COALESCE(?, UNIX_TIMESTAMP())` 取时算窗(Go 侧整除对齐);②`INSERT INTO {{q}} (qkey,win,used) VALUES (?,?,1) ON DUPLICATE KEY UPDATE used = IF(used < ?, used + 1, used)`——affected 1(插入)/2(真更新)= 成功,0 = 耗尽。取时与扣减分步不破坏硬性(原子性在②;①只决定窗口归属,边界竞态最多把预留记进上一窗,不多放)。
- 方言差异走 `Dialect` 新方法(如 `QuotaReserveSQL(prefix) string` 与 `QuotaTimeSQL() string`,形态实现时定),两薄壳包各自实现;`IsIdempotentDDLErr` 复用 005 机制建表。

## 5. redis(redisbroker/lua/quota_reserve.lua、quota_release.lua + 新 quota.go)

| 键 | 类型 | 用途 |
|---|---|---|
| `tg:quota:<qkey>:<win>` | STRING(计数) | 该 key 该窗口已用次数;`EXPIRE 2×period` 自清 |

- **quota_reserve.lua**(单段原子):`ARGV = prefix, qkey, period, limit, overrideNow('' = 用 TIME)`。取时(`redis.call('TIME')` 或覆盖值)→ `win = now/period*period` → `GET` 检查 `< limit` → `INCR` + `EXPIRE(2×period)` → 返回 `{1, win}`;耗尽返回 `{0, win}`。**本脚本是 common.lua"禁 TIME"约定的唯一豁免**,头注释写明;不与任何任务键交互,不并进 common.lua 前奏也可(独立小脚本)。
- **quota_release.lua**:`GET > 0` 则 `DECR`(带窗口键定位,窗口过期即落空无害)。
- 三态映射:`{1,win}` 成功;`{0,win}` 耗尽;网络/脚本错误 = 介质故障。
- 测试:miniredis `SetTime` 控制 `TIME` 回音;真 redis 档(TASKGATE_REDIS_ADDR)用真时间短窗口冒烟。

## 6. memory(memorybroker/broker.go)

```go
quota map[string]map[int64]int // qkey → win → used,同一把大锁保护
```

`Reserve`:临界区内 `win = clk.Now().Unix()/period*period`,`used < limit` 则 +1 返回,否则耗尽;顺手删 `< win-2×period` 的旧窗。介质=本进程,注入 Clock 即介质钟——fakeclock 直接可控,是 RunQuota 套件的语义参考实现。

## 7. 配置校验规则汇总(New 时 fail-fast)

1. `QuotaLimit>0` ⇒ `QuotaPeriod>0`;负值一律拒;
2. 同解析后 quota key ⇒ (limit, period) 全等;
3. 启用配额 ⇒ broker 必须实现 `QuotaProvider`(无静默退化);
4. `QuotaKey` 只在 `QuotaLimit>0` 时有意义,单独设置无副作用(文档写明)。
