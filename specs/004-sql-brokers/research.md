# Research: SQL 后端适配关键决策

**Spec**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md) | 事实来源:`docs/plans/2026-07-15-MySQL-PG后端适配方案.md`

按方案第 9 节的头号问题顺序记录:①withTx 死锁重试环 → ②占位符不复用 → ③Dialect 边界 → ④认领 SQL → ⑤建表互斥 → ⑥测试门控。每条给出决策、理由、以及从 sqlitebroker 现有代码摸到的确切迁移点。

## 1. withTx 死锁/序列化失败重试环(蓝本没有的新机制)

**问题**:sqlitebroker 的 `withTx`(sqlitebroker/broker.go:168-178)一行重试都没有——SQLite 单连接串行(`SetMaxOpenConns(1)` + `_txlock=immediate`)天生不死锁。PG/MySQL 是真并发行锁,`propagateTx` 按树形状加锁,两个并发操作(Cancel 取消子树 vs ReapExpired 回收、两个 Fail 传播进同一片子树)加锁顺序不同 → PG 报 40P01、MySQL 报 1213,事务被数据库单方面 kill。SKIP LOCKED 只挡"认领 vs 认领",挡不住"传播 vs 传播"。

**决策**:`sqlbroker.Broker.withTx` 内置重试环:

```go
func (b *Broker) withTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
    var lastErr error
    for attempt := 0; attempt < b.maxRetry; attempt++ {   // maxRetry 默认 5
        tx, err := b.db.BeginTx(ctx, b.txOpts)            // 隔离级别由 dialect/opts 定
        if err != nil { return err }
        err = fn(tx)
        if err != nil {
            _ = tx.Rollback()
            if class := b.dialect.Retryable(err); class != NotRetryable && attempt < b.maxRetry-1 {
                lastErr = err
                b.backoff(ctx, class, attempt)            // 退避走注入 clock
                continue
            }
            return err                                    // 不可重试 / 超限:原样返回
        }
        if err := tx.Commit(); err != nil {
            if class := b.dialect.Retryable(err); class != NotRetryable && attempt < b.maxRetry-1 {
                lastErr = err; b.backoff(ctx, class, attempt); continue
            }
            return err
        }
        return nil
    }
    return lastErr
}
```

**三条配套硬纪律(code review 逐点核对)**:

1. **fn 必须无副作用可重跑**:每轮重新 `getRecForUpdate` 再判状态改状态。sqlite 蓝本骨架本来就是"事务内取行→判→改",天然满足;迁移时只要保证 fn 内不缓存跨轮状态。
2. **Notify/wake 严禁在重试环内触发**——防幻通知。sqlitebroker 已经是"事务内只把状态流转快照收集进切片(propagate.go 的 `notifs *[]Task`),事务外再 wake"的结构。迁移后**必须保持**:`fn` 只往传入的切片里 append 通知快照,`withTx` 整体成功返回后调用方才 `wakeAll()`/发通知。绝不能在 fn 内直接 wake——前几轮 rollback 掉的事务会把通知发出去,等于广播了从未发生的状态流转。
3. **MySQL 1205 ≠ 1213 分档**:1213(死锁)立即返回,重试便宜;1205(lock wait timeout)默认要等 `innodb_lock_wait_timeout=50s` 才报。处理两手:(a)连接初始化 `SET SESSION innodb_lock_wait_timeout=5`(mysqlbroker 连接建立时执行);(b)`Retryable(err)` 返回**三态** `RetryClass`——`NotRetryable` / `RetryImmediate`(死锁,指数退避重试到 5 次)/ `RetryLimited`(1205,只重试 1 次、短退避)。PG 的 40P01/40001 都是 `RetryImmediate`,不受影响。

**取证**:`b.backoff` 用 `b.clk`(注入 clock)而非 `time.Sleep`,契约测试的 fakeclock 才确定;SC-005 的 L3 并发压测是这套机制唯一的自动化证明(契约用例单 goroutine 压不出死锁)。

## 2. 占位符只用不复用(蓝本的 `?N` 复用必须展开)

**问题**:`Rebind` 只能改 SQL 文本、改不了 args 切片;go-sql-driver/mysql 只认位置参数不支持复用,PG 的 `$n` 复用反而是特例。若保留 sqlite 的 `?1`/`?2` 具名复用,共享模型会被 MySQL 击穿。

**摸底结论(迁移点比方案预估更窄)**:grep 全 sqlitebroker,**只有 query.go 的 ReapExpired 两处**用了 `?N` 复用,dequeue/propagate 早已是普通 `?` + 手工顺序 append args:

- `sqlitebroker/query.go:129-133`(取消标记的过期任务):`?1`(nowMS)复用 **2 次** → 展开成 2 个位置 `?`,args 传 `nowMS, nowMS`。
- `sqlitebroker/query.go:160-168`(租约回收):`?1`(LeaseLostMax)复用 **4 次**、`?2`(nowMS)复用 **2 次** → 展开成 6 个位置 `?`,args 按 SQL 中出现顺序传 `LeaseLostMax×(SET 里 3 处), nowMS(finished_at), nowMS(WHERE)`,共 6 个。

**决策**:核心里所有 SQL 一律只用不复用位置 `?`,同值在 args 里重复传;`dialect.Rebind(sql)` 只做"文本改写"这一件事(PG 把第 n 个 `?` 换成 `$n`,MySQL 原样返回)。队列过滤统一用现成的 `placeholders(n)`(sqlitebroker/broker.go:271)展开 `IN (?,...)`,PG 不特开 `ANY(array)` 绑定。这条决定了 Rebind 保持最简形态。ReapExpired 从 RETURNING 改造时(见第 4 节)会顺带重写这两处。

## 3. Dialect 边界(最小差异抽象)

**决策**:Dialect 接口只收 8 个"两库真正不同"的点,放 `internal/sqlbroker/dialect.go`;具体实现放薄壳包(因为要断言驱动私有错误类型)。

```go
package sqlbroker

type RetryClass int
const ( NotRetryable RetryClass = iota; RetryImmediate; RetryLimited )

type Dialect interface {
    Name() string                         // "postgres" / "mysql",日志与错误前缀用
    Rebind(query string) string           // 只改文本:PG ?→$1..$n;MySQL 原样返回
    SchemaSQL(prefix string) []string     // 各自 DDL(建表+索引),按前缀拼名;返回多条按序执行
    Claim(ctx, tx, p ClaimParams) (*Rec, error)  // 认领:PG 一条 RETURNING;MySQL 两步 SELECT FOR UPDATE + UPDATE(第 4 节)
    IsDuplicateKey(err error) bool        // PG SQLSTATE 23505 / MySQL errno 1062 → Enqueue 翻译 ErrTaskExists
    Retryable(err error) RetryClass       // 死锁/序列化/锁等待判定(第 1 节三态)
    Lock(ctx, conn *sql.Conn, key string) error    // 建表期库级互斥(第 5 节)
    Unlock(ctx, conn *sql.Conn, key string) error
}
```

- `Rec`、`ScanRec(RowScanner) (*Rec, error)`、`ClaimParams`、`taskCols` 都由 sqlbroker **导出**,供薄壳包的 dialect 实现使用(internal 包同模块可 import)。
- 标准 SQL(enqueue/lifecycle/propagate/query 除 ReapExpired 认领改造外)全在核心一份,两库逐字相同,只经 `Rebind` 过一道文本。
- **错误判定纪律**(写进 Dialect 注释):`IsDuplicateKey`/`Retryable` 一律 `errors.As` 到 `*pgconn.PgError`/`*mysql.MySQLError` 再看 SQLSTATE/errno,禁止字符串匹配(错误文案随驱动版本/locale 变)。

## 4. 认领语句(RETURNING 差异)

sqlite 靠单连接串行 + BEGIN IMMEDIATE 避认领竞争(dequeue.go:85-92 是 `UPDATE ... WHERE id=(子查询) RETURNING`);SQL 后端必须 `FOR UPDATE SKIP LOCKED` 防两 worker 认领同一行且不互等。lease_until 仍是**第二条语句**补记(认领 SET 那刻多队列下还不知中选队列的 TTL,和 sqlite dequeue.go:116-117 一致)。

**PG**(有 RETURNING,一条完成主认领):
```sql
UPDATE <p>tasks SET status='running', lease_token=?,
       started_at = CASE WHEN started_at=0 THEN ? ELSE started_at END
WHERE id = (SELECT id FROM <p>tasks
            WHERE queue IN (?,...) AND status IN ('pending','retrying') AND run_at <= ?
            ORDER BY run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED)
RETURNING <taskCols>;
-- 之后同事务第二条:UPDATE <p>tasks SET lease_until=? WHERE id=?
```

**MySQL**(无 RETURNING,同事务拆两步,SKIP LOCKED 保第二步不被抢;SELECT 先拿到 queue,lease_until 可在 UPDATE 一步写全):
```sql
SELECT <taskCols> FROM <p>tasks
WHERE queue IN (?,...) AND status IN ('pending','retrying') AND run_at <= ?
ORDER BY run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED;
UPDATE <p>tasks SET status='running', lease_token=?, lease_until=?,
       started_at = CASE WHEN started_at=0 THEN ? ELSE started_at END
WHERE id = ?;
```

两版都由 `dialect.Claim` 各写一份,返回 `*Rec`(用 `sqlbroker.ScanRec` 扫)。核心 `dequeue.go` 只负责:取信号 → 调 `dialect.Claim` → 没中选就 `SELECT MIN(run_at)` 算下次到点(照 dequeue.go:101-103)→ 阻塞等待(三唤醒源)。**ReapExpired 的两条 UPDATE...RETURNING**(query.go)同理:PG 保留 RETURNING;MySQL 改 "SELECT ... FOR UPDATE SKIP LOCKED 拿中选行 id 集 → UPDATE → 按 id 集回读" 或分批,收进 dialect 或核心按 `dialect.Name()` 走两版扫描。ReapExpired 扫描语句也要 `FOR UPDATE SKIP LOCKED`(多进程各跑 reaper 不互踩、不重复计 LeaseLost)。

## 5. 建表冷启动互斥(并发 DDL 竞态)

**问题**:多进程同时首启并发跑 `CREATE TABLE IF NOT EXISTS`,PG 会报 `tuple concurrently updated`/`duplicate key pg_type` 脏错(sqlite 单连接没这事)。

**决策**:建表全过程套库级互斥锁,**锁必须钉在独占连接上**:

```go
conn, _ := db.Conn(ctx)   // 取一个独占连接
defer conn.Close()
dialect.Lock(ctx, conn, lockKey)      // PG: SELECT pg_advisory_lock(hashKey);MySQL: SELECT GET_LOCK(name, timeout)
for _, ddl := range dialect.SchemaSQL(prefix) { conn.ExecContext(ctx, ddl) }  // 建表+索引全在这条连接
dialect.Unlock(ctx, conn, lockKey)    // PG advisory 也可交给会话结束释放;MySQL 必须 RELEASE_LOCK 同连接
```

- MySQL 的 `GET_LOCK` 是会话(连接)级——走连接池会"A 连接拿锁、B 连接放锁"错位,所以整段钉在 `db.Conn` 独占连接上。
- PG 用 `pg_advisory_lock`(会话级)或 `pg_advisory_xact_lock`(事务级、事务结束自动释放),同样锁与 DDL 同连接。
- 对 `tuple concurrently updated` 这类并发 DDL 脏错再兜一层容忍重试(拿不到锁也重试)。
- lockKey 由 TablePrefix 派生(不同前缀不互锁),PG 需把字符串 key hash 成 bigint。

## 6. 测试门控(宪法 V.5 已批准的例外)

- **契约档(L2)**:`TASKGATE_PG_DSN` / `TASKGATE_MYSQL_DSN` 门控,缺失即 `t.Skip`——完全复制真 Redis 档(`TASKGATE_REDIS_ADDR`,redisbroker/broker_test.go:37-54)的模式:随机表前缀隔离(仿 `KeyPrefix="tgtest:"+ulid`)+ `t.Cleanup` DROP 清理。
- **Factory 接线**:`brokertest.Run(t, func(t, opts){ b,_ := pgbroker.Open(dsn, pgbroker.WithTablePrefix(rand)); return b })`,和 sqlite 一行接法一致(brokertest Factory = `func(*testing.T, BrokerOptions) Broker`,返回已 Init 的 broker)。
- **CI**:GitHub Actions service container(postgres:16 / mysql:8)注入 DSN,契约在 CI 必跑;照现有 `test-redis` job 加两个 job。本地开发者不装库也全绿(skip)。
- **L3/多进程**:`multiproc_test.go` 在 DSN 存在时追加两后端;kill -9 / 恰好一次 / 断连专项复用 tcpProxy 对 SQL 后端同样有效。
- **并发 propagate 压测专项(新增,SC-005)**:构造共享子树,N≥8 goroutine 同时打 Cancel/Fail/ReapExpired,断言零错误抛出、全部任务终态一致(死锁被重试环吃掉而非漏给调用方)。env 门控同一开关。这是第 1 节重试环唯一的自动化证明。

## 关键迁移点清单(从 sqlitebroker 摸到的确切位置)

| 迁移点 | sqlite 位置 | SQL 后端处理 |
|---|---|---|
| withTx 加重试环 | broker.go:168-178 | 重试环 + 三态 Retryable(第 1 节) |
| BEGIN IMMEDIATE 来自 DSN | DSN `_txlock=immediate` | 显式 `BeginTx` 选隔离级别 / 靠行锁 |
| `?N` 复用 | query.go:129-133、160-168 | 展开成位置 `?`,args 重复传(第 2 节) |
| 认领 UPDATE...RETURNING | dequeue.go:85-92 | PG 保留;MySQL 拆两步(第 4 节) |
| lease_until 第二条 | dequeue.go:116-117 | 原样保留 |
| 单连接串行 SetMaxOpenConns(1) | broker.go Open | 放开并发,SetMaxOpenConns(10) 默认可调 |
| wake 换代 channel | broker.go:135-144 | 进程内原样复用;跨进程靠轮询 |
| propagate BFS 工作队列 | propagate.go:16 | 原样复用(标准 SQL) |
| validateID 只挡控制字符 | redisbroker/enqueue.go:16-23 | mysqlbroker 参照 + 补 ≤255 长度校验 |
| 建表 DDL | sqlitebroker/schema.sql | 三库类型映射 + 前缀 + 索引名带前缀 + 互斥锁 |
