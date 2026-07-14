# Data Model: taskgate M3

## 1. Go 类型变更(公开 API 只增不改)

| 位置 | 变更 | 默认值/语义 |
|---|---|---|
| taskgate.QueueConfig | 新增 `ManualHeartbeat bool`(yaml:"manual_heartbeat" json:"manual_heartbeat") | false=自动续租(向后兼容);true=不起自动心跳,handler 自己 RenewLease |
| taskgate.Filter(broker.go) | 新增 `Offset int` | 0=不跳过;越界返回空;注释写明排序合同 (CreatedAt,ID) 升序 |
| taskgate(client.go) | 新增 `func RenewLease(ctx context.Context) error` + 非导出 ctx key | 非任务 ctx → ErrNoTask |
| errors.go | 新增哨兵 `ErrNoTask` | RenewLease 在非任务 ctx 调用时返回 |

状态机、计数(Attempts/LeaseLost/Throttled)、Broker 接口签名:**无变更**。

## 2. List 排序与分页合同(修订 001/contracts/broker-contract.md)

- List 结果按 `(CreatedAt, ID)` 升序,三后端一致
- 先按 Filter{Type,Queue,Status} 过滤 → 排序 → 跳过 Offset → 取 Limit(0=不限)
- Offset ≥ 匹配总数 → 空列表,nil error
- 翻页期间数据变动:不承诺快照一致,只承诺"未变动的任务不丢不重"(文档条款)
- brokertest 新增第 18 条契约 ListPagination

## 3. 各后端 List 实现要点

| 后端 | 实现 |
|---|---|
| sqlite | `SELECT ... WHERE 过滤 ORDER BY created_at, id LIMIT ?2 OFFSET ?1`(Limit=0 时用 -1=无限) |
| memory | 过滤后 sort.Slice((CreatedAt,ID)),切片 [Offset:Offset+Limit) |
| redis | 既有候选集(idx 交集)→ 逐个 HGETALL → 内存排序 → 切片;O(候选集) 写进已知限制 |

## 4. scheduler 变更(execute 内)

- handler ctx 注入续租闭包(所有队列都注入,自动/手动模式一致可调)
- 闭包语义:调 Heartbeat(Background,id,token) → nil / ErrTaskCanceled(先 cancel 任务 ctx 再返回)/ ErrLeaseLost(置"租约已丢"标记+cancel ctx)/ 网络错误原样
- `qc.ManualHeartbeat == true` → 不起心跳 goroutine(hbDone/hbExited 通道相应短路,收尾路径保持零泄漏)
- reaper、Shutdown、cancelLocal、三优先级回执判定:零改动

## 5. e2e/mockgw(测试组件,不属库 API)

```go
type Gateway struct{ /* httptest.Server + 原子计数 */ }
func New(opts ...Option) *Gateway
func (g *Gateway) URL() string
func (g *Gateway) Close()
func (g *Gateway) MaxConcurrency() int  // 观测到的最大并发(原子)
func (g *Gateway) BusyCount() int       // busy 响应次数
func (g *Gateway) Requests() int        // 总请求数

func Latency(d time.Duration) Option
func BusyAfterConcurrency(n int) Option  // 并发>n:HTTP 200,SSE 体内 error 事件(复刻讯飞)
func FailRate(p float64, seed int64) Option // 固定种子,p 概率 HTTP 500
func CrashAfterConcurrency(n int) Option // 并发>n:直接断连(复刻 feedoc)
```

响应体约定(极简 JSON/SSE 文本):正常 `data: {"ok":true,"echo":<payload>}`;busy `data: {"error":"busy"}`(HTTP 200);测试 handler 解析体判定 busy → 返回 ErrThrottled{RetryAfter}。

## 6. 配置校验

无新增 fail-fast 条件(ManualHeartbeat 是 bool,零值安全;Offset<0 在 List 入口按 0 处理或报错——取"按 0 处理",宽容读接口)。

## 7. sqlite DDL / redis 键

无变更(排序用的 created_at/id 两列/字段已存在;sqlite 建议顺手加 `(created_at, id)` 复合索引?——不加,List 是管理查询,现有量级全表扫可接受,YAGNI)。
