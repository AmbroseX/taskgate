package pgbroker

// blank import 注册 database/sql 的 "pgx" 驱动(pgx v5 的 stdlib 模式,纯 Go 免 cgo)。
// 放在薄壳包里:用户只 import pgbroker 时才把 PG 驱动链进二进制,不牵连 MySQL 驱动。
import _ "github.com/jackc/pgx/v5/stdlib"
