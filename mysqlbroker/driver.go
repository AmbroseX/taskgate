package mysqlbroker

// blank import 注册 database/sql 的 "mysql" 驱动(go-sql-driver/mysql,纯 Go 免 cgo)。
// 放在薄壳包里:用户只 import mysqlbroker 时才把 MySQL 驱动链进二进制,不牵连 PG 驱动。
import _ "github.com/go-sql-driver/mysql"
