# Research: taskgate M3

## 1. 手动续租的 API 形态:ctx 注入闭包,不改 Handler 签名

**问题**: handler 怎么拿到"续租"这个能力?改 Handler 签名(加参数)、Task 上挂方法、还是 ctx 注入?

**Decision**: `taskgate.RenewLease(ctx context.Context) error` 导出函数 + 非导出 ctx key。scheduler 在 execute 里把续租闭包(捕获任务 ID/令牌/broker/该任务的 cancel)塞进 handler ctx;RenewLease 从 ctx 取闭包调用,非任务 ctx 返回导出的 `ErrNoTask`(新哨兵)。

**Rationale**: Handler 签名 `func(ctx, *Task) ([]byte, error)` 已定型(M1),改签名破坏全部现有代码;Task 是纯数据模型,挂行为方法违反数据模型纪律。ctx 注入是 Go 生态标准做法(如 http.ServerContextKey)。

**Alternatives considered**:
- Handler 加第三参数:破坏公开 API,排除。
- Task.Renew() 方法:Task 要持 broker 引用,序列化边界被污染,排除。

## 2. 续租闭包的行为:复用心跳的错误语义

**Decision**: 闭包内部调 `broker.Heartbeat(Background, id, token)`:成功返回 nil;ErrTaskCanceled → 先 cancel 该任务 ctx 再原样返回(handler 立刻知道该退);ErrLeaseLost/ErrTaskNotFound → 置"租约已丢"标记、cancel ctx、返回 ErrLeaseLost(与自动心跳的三优先级判定完全同路);网络错误原样返回(handler 自行决定重试)。

**Rationale**: 与 scheduler.heartbeatLoop 的既有语义严格一致,不产生第二套租约状态机;"租约已丢"标记复用后 execute 的回执判定(丢弃结果)自动生效。

## 3. ManualHeartbeat 开关的边界

**问题**: 关掉自动心跳后,跨进程 Cancel 靠什么发现?

**Decision**: `QueueConfig.ManualHeartbeat bool`(yaml:"manual_heartbeat",默认 false=自动续租)。true 时 execute 不起心跳 goroutine,其余(reaper、令牌、Shutdown、本地 Cancel 即时 cancel)全部原样。跨进程 Cancel 在手动模式下由 handler 的下一次 RenewLease 发现(返回 ErrTaskCanceled);handler 一直不续租则由租约过期兜底。这条写进 godoc 与 README。

**Rationale**: 手动模式的语义就是"handler 对自己的租约负全责";本地取消不受影响(cancelLocal 直接 cancel ctx)。

**Alternatives considered**: 手动模式仍保留低频"只查取消不续租"的后台轮询:又造一个半自动模式,语义混浊,排除。

## 4. List 排序合同:(CreatedAt, ID) 升序

**问题**: 分页必须有确定序。ulid 天然可排,但自定义 ID(幂等去重场景)破坏"ID 序=创建序"。

**Decision**: 排序键 = `(CreatedAt, ID)` 升序;Filter 加 `Offset int`(0=不跳过);先过滤后排序再分页;Offset 越界返回空。合同修订进 001 的 broker-contract.md。

**Rationale**: CreatedAt 由 broker 落库时统一写(毫秒),同毫秒内用 ID 定序保证全序;三后端都能实现:sqlite `ORDER BY created_at,id LIMIT ? OFFSET ?`,memory 排序切片,redis 候选集取回后内存排序切片。

**Alternatives considered**:
- 纯 ID 序:自定义 ID 乱序,排除。
- 游标分页(after_id):更稳但三后端实现成本高,YAGNI,M4 再说;Offset 翻页期间数据变动的弱一致写进文档。

## 5. redis List 分页的代价

**Decision**: 照现有实现路子:idx 集合求候选 ID → 逐个 HGETALL → 内存排序 → 切 [Offset, Offset+Limit)。不引入 sorted-set 二级索引。

**Rationale**: List 本来就要 HGETALL 全字段,排序不改变渐进复杂度;加 zset 索引(score=created_at)要在每条流转维护第 4 个结构,为一个管理查询不值。O(候选集) 写进已知限制,大库存用 Filter 缩小候选集。

## 6. mockgw 的形态与故障开关

**Decision**: `e2e/mockgw` 普通包(非 _test.go,pipeline/realgw 测试共用):`New(opts ...Option) *Gateway`,包 httptest.Server;Option:`Latency(d)`、`BusyAfterConcurrency(n)`(并发>n 返回 200+SSE 体内 busy 错误事件,复刻讯飞)、`FailRate(p float64, seed int64)`(固定种子 rand,返回 500)、`CrashAfterConcurrency(n)`(并发>n 直接 conn 断开,复刻 feedoc)、`SSEHideError()`(200 但 body 是错误事件)。Gateway 暴露 `MaxConcurrency()`、`BusyCount()`、`Requests()` 原子观测;`Close()`。

**Rationale**: 测试方案第 4 节原文的四个开关 + 观测口;固定种子保 CI 确定(宪法 V.3);busy 藏在 200 里与真实网关行为一致,测试用例 5 直接复用。

**Alternatives considered**: 放 internal/:e2e 不在 internal 保护范围内的问题不存在(同 module),但放 e2e/mockgw 与测试方案第 8 节目录一致,且不会被使用方 import(路径含 e2e 语义自明)。

## 7. realgw 冒烟档

**Decision**: `e2e/realgw_test.go`,`//go:build realgw`;读 `LLM_GATEWAY_URL`/`LLM_GATEWAY_KEY`,缺失 t.Skip;10 任务 {Workers:2,RPS:1} 用 sqlite 后端;注释写明 reasoning 模型 max_tokens≥600 与 NO_PROXY 两个已知坑(测试方案第 7 节)。CI 取证:常规 `go vet ./...` 不编译它。

## 8. L4 用例与 Gate 的接线

**Decision**: 五用例全用 memory 后端(L4 测的是"限流×故障×流水线"的组合行为,后端矩阵 L2/L3 已盖);handler 是真 http.Client 打 mockgw,busy/SSE 错误判定后返回 ErrThrottled,断连/500 返回普通 error。重试预算:FailRate 档 MaxRetry 给足(如 5),断言最终零 failed。

**Rationale**: 测试方案第 4 节的意图是端到端行为验证,不是后端矩阵;单后端跑五用例控制 CI 时长。
