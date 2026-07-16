# spec 005 Identity 身份模型 完成记录

日期:2026-07-16
状态:**已完成**。spec-kit 全流程(specify → plan → tasks → implement)走完,28/28 任务勾毕。
产物:`specs/005-identity-replay/`(spec/plan/research/data-model/quickstart/contracts/tasks)
上游:《2026-07-16-Identity领域模型.md》(裁决 f 模型)+ 原型验证(`prototype/identity/`)

## 1. 落地内容

- **Task 加两字段**:`BusinessKey`(业务幂等键,可空)、`ReplayOf`(重放来源,可空);`ID` 语义收紧为 ExecutionID(系统 ulid,公开 API 无写入口)。
- **Submit 幂等挪键**:`WithBusinessKey` 新增;`WithID` 弃用为其别名(值是业务键,不再是任务 ID,不能拿去 Get/DependsOn)。键下存在任何执行一律拒,错误 `errors.Is(ErrTaskExists)` 兼容 + `errors.As(*TaskExistsError)` 携带链尾 ID/状态。
- **Broker 接口 15 → 16**:新增 `Replay(ctx, ReplayRequest)`;`Filter` 加 `BusinessKey`;Enqueue 拒非空 ReplayOf。
- **Gate 层**:`Replay` / `ReplayByKey`(AllowCompleted / WithPayload 选项)、`History`(键下链序枚举)。
- **两条不变式落成存储级唯一约束**:链头唯一(同键 `replay_of` 空的行至多一条)+ 重放来源唯一(链不分叉)。sqlite/PG 部分唯一索引,MySQL 生成列 + 唯一索引,redis 单段 Lua,memory 单锁。前置条件②(键下无在途)由①+③推出,不写扫描代码(research 决策 3)。
- **新增哨兵错误**:`ErrReplayNotFinal` / `ErrAlreadyReplayed` / `ErrCompletedNotAllowed`。
- **存量升级零脚本**:sqlite `Open` 时幂等 ALTER;PG `ADD COLUMN IF NOT EXISTS`;MySQL ALTER 撞 1060/1061 由新 Dialect 方法 `IsIdempotentDDLErr` 幂等跳过;旧任务 ID 原样解释为 ExecutionID。
- **brokertest 18 → 22 条**:契约 2 重写为 `BusinessKeyIdempotent`(预置 ID 主键防御保留);新增 `ReplayBasic` / `ReplayChain` / `BusinessKeyQuery` / `IdentityRace`。

## 2. 验收结果

- `go build ./...`、`go vet ./...` 通过;`go test ./... -race` 全绿。
- 五后端契约:memory/sqlite/miniredis 离线全绿;**MySQL 真机(127.0.0.1)22 条全绿**,含存量表(spec 004 旧 schema)升级路径手动验证通过;PG 本地无实例,由 CI service container 验收(spec 004 既有门控口径,PG 的 DDL 全部 IF NOT EXISTS,与 sqlite 同形)。
- 覆盖率:核心包 89.3%(≥85)、memory 89.9 / sqlite 86.6 / redis 85.1 / mysql 84.6(≥80);pgbroker 66.7 是本地无 PG 时只跑方言单测的既有现象。
- 原型五断言全部契约化(SC-001);同键/同目标各 50 并发恰一成功(SC-002,-race);cron 配方 e2e 走通:被拒 → errors.As 拿链尾 → Replay → 跑完(SC-005,`e2e/TestE2ECronReplay`)。

## 3. 关键实现取舍(与文档对应)

- Replay 进 Broker 接口而非 client 组合:校验+创建必须同事务/同 Lua,client 组合在多进程后端有链分叉窗口(research 决策 1)。
- SQL 链尾定位不依赖时间精度:`WHERE business_key=? AND NOT EXISTS(SELECT 1 ... replay_of = id)`。
- redis 目标 hash 只加内部 `replayed` 标记(链元数据,decodeTask 不外露),对外记录零改写。
- MySQL 驱动无结构化约束名字段,`DuplicateKeyConstraint` 返回 1062 原始 message 供子串匹配——Dialect 合同里明写的唯一字符串匹配豁免。

## 4. 后续事项

1. **宪法 IV.2 措辞对齐(建议 MINOR 修宪)**:现文"任务 ID 用 ulid 自动生成,支持自定义 ID 做幂等去重"应改为"任务 ID(ExecutionID)一律系统生成;业务幂等走 BusinessKey;Enqueue 预置 ID 仅为测试/嵌入入口的存储防御"。
2. PG 契约与迁移路径等 CI 首跑确认(push 后看 service container 档)。
3. `docs/plans/2026-07-15-MySQL-PG后端适配方案.md` 已补修订注记("接口一行不改"被 spec 005 按流程扩展)。
4. README 双语已重写 cron/幂等段落与错误清单;`ref/`、旧博文等外部引用如有"恰好执行一次/WithID"措辞不在本仓管辖。
