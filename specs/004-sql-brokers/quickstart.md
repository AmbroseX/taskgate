# Quickstart: PostgreSQL / MySQL 后端

**Spec**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)

## 用 PostgreSQL 后端

```go
import (
    "github.com/AmbroseX/taskgate"
    "github.com/AmbroseX/taskgate/pgbroker"
)

b, err := pgbroker.Open("postgres://user:pass@localhost:5432/mydb?sslmode=disable")
if err != nil { log.Fatal(err) }
defer b.Close()

// 交给 scheduler/client,和 sqlite 后端用法完全一样
sched := taskgate.NewScheduler(b, /* ... */)
```

可选配置(表前缀隔离、连接池、轮询间隔):

```go
b, _ := pgbroker.Open(dsn,
    pgbroker.WithTablePrefix("myapp_"),   // 默认 "taskgate_",多应用共享库时隔离
    pgbroker.WithMaxOpenConns(20),        // 默认 10
    pgbroker.WithPollInterval(200*time.Millisecond), // 默认 200ms
)
```

要求:PostgreSQL 9.5+(需 `FOR UPDATE SKIP LOCKED`)。首次 Open 自动建表(`CREATE TABLE IF NOT EXISTS`,冷启动并发安全)。

## 用 MySQL 后端

```go
import (
    "github.com/AmbroseX/taskgate"
    "github.com/AmbroseX/taskgate/mysqlbroker"
)

b, err := mysqlbroker.Open("user:pass@tcp(localhost:3306)/mydb?parseTime=true")
defer b.Close()
```

要求:MySQL 8.0+(需 `FOR UPDATE SKIP LOCKED`)。DDL 内置 `utf8mb4_bin` 排序规则,自定义 ID 大小写敏感。

**MySQL 独有限制**:

- 自定义 ID / type / queue 最长 255 字符,超限 Enqueue 直接返回清晰错误(不落库)。
- payload / result 受服务器 `max_allowed_packet`(默认 64M)限制,超限由驱动报错。
- 不使用自定义 ID 时(库生成 ulid,26 字符)无需关心长度。

## 已知限制(两后端共通)

- **跨进程新任务感知延迟** = 轮询间隔(默认 200ms,可调);未实现 PG LISTEN/NOTIFY 即时唤醒。
- **不提供分布式限流**:SQL 后端不实现 LimiterProvider,scheduler 自动退回进程内限流,多进程各限各的;需要精确跨进程限流请用 redis 后端。
- **高并发传播冲突**时事务会经历死锁自动重试(有上限),表现为个别调用延迟抬高;重试超限会把数据库死锁错误原样抛出。

## 本地跑契约测试(需要真库)

契约测试用环境变量门控,不设就 skip(本地全绿不代表跑过这两个后端,回归靠 CI):

```bash
# 起临时库(docker)
docker run -d --name tg-pg   -e POSTGRES_PASSWORD=pass -p 5432:5432 postgres:16
docker run -d --name tg-mysql -e MYSQL_ROOT_PASSWORD=pass -e MYSQL_DATABASE=taskgate -p 3306:3306 mysql:8

# 跑 PG 契约档
TASKGATE_PG_DSN="postgres://postgres:pass@localhost:5432/postgres?sslmode=disable" \
  go test -race -count=1 ./pgbroker/...

# 跑 MySQL 契约档
TASKGATE_MYSQL_DSN="root:pass@tcp(localhost:3306)/taskgate?parseTime=true" \
  go test -race -count=1 ./mysqlbroker/...

# 并发 propagate 压测专项(死锁重试环验证)+ 多进程
TASKGATE_PG_DSN=... TASKGATE_MYSQL_DSN=... go test -race -count=1 -run TestConcurrent ./...
```

不设环境变量时 `go test ./... -race` 全绿(两后端档 skip)。

## CI

`.github/workflows/ci.yml` 加两个 job(照现有 `test-redis` 模板):`test-pg`(service `postgres:16`)、`test-mysql`(service `mysql:8`),注入 DSN 后契约必跑。
