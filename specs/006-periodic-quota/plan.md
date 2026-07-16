# Implementation Plan: 周期配额 Periodic Quota(硬配额)

**Branch**: `006-periodic-quota` | **Date**: 2026-07-16 | **Spec**: [spec.md](./spec.md)

## Summary

给队列加"每个固定窗口最多启动 N 次 handler"的硬配额。新增**独立能力接口** `QuotaProvider`(与 `LimiterProvider` 平行,但无静默退化:配了配额而后端不支持 → `New()` 报错),后端在共享介质内用**一条原子操作**完成"检查窗口余额 + 扣减"(sqlite/PG 单语句 `INSERT ... ON CONFLICT ... RETURNING`,MySQL 先取服务端时间再 `ON DUPLICATE KEY UPDATE` 看 affected,redis 单段 Lua + `TIME`,memory 单锁 + 注入时钟);窗口时间一律取介质服务端钟。scheduler 的认领链插入第三环:占槽 → 令牌 → **预留** → Dequeue,耗尽/介质故障时释放槽暂停认领(fail-closed),`QueueLen>0` 启发式消掉空队列白烧常态。配额行为由新的 `brokertest.RunQuota` 合规套件五后端统一验收(介质时间经各后端测试缝控制,不真 sleep)。

## Technical Context

**Language/Version**: Go 1.25
**Primary Dependencies**: 零新增(modernc.org/sqlite、go-redis/v9、pgx/v5、go-sql-driver/mysql、x/time/rate 均已有)
**Affected Backends**: 全部五个都要实现 `QuotaProvider`;**不动 Broker 接口本体**(配额是可选能力接口,不是 Broker 方法——调研方案 5.6 的分层裁决)
**Testing**: L1(窗口数学/配置校验/选项)→ L2 新增 `brokertest.RunQuota` 配额合规套件(五后端,介质时间可控)→ L3(scheduler 集成:耗尽暂停/fail-closed/Stats 位/双 Gate 共享 sqlite 文件)→ L4 e2e(`{Workers,RPS,Quota}` 组合打 mockgw)→ 基准(热点行争用对比数)
**Concurrency Semantics**: 预留的原子性由介质保证(单语句/单 Lua/单锁);"检查+扣减"无窗口是硬配额的来源;泄漏方向永远保守(退还失败/崩溃 = 视同消耗)
**Performance Goals**: `QuotaLimit=0` 时认领热路径零新增介质往返;启用时每次认领 +1 次 QueueLen(启发式)+1 次原子预留,损耗以基准数据说话(>30% 触发备选路线重评)
**Constraints**: 向后兼容(新字段零值 = 不启用);零新依赖;生产窗口时间不依赖应用节点钟;所有测试离线、确定、不真 sleep

## Constitution Check

| 原则 | 本计划遵循方式 | 结果 |
|------|---------------|------|
| I 库的边界 | 纯库内能力;配置只走 Config struct(带 yaml/json tag);不读 env | ✅ |
| II.1 接口最小公倍数 | Broker 接口零改动;配额走可选能力接口(LimiterProvider 同款模式),但**无静默退化**——这是模型裁决 #3 对该模式的修正 | ✅ |
| II.2 上层不特判后端 | scheduler 只做一次 `broker.(QuotaProvider)` 能力断言,不 import 后端包 | ✅ |
| II.3 brokertest 合规 | 配额不是 Broker 契约(不进 22 条),但新增 `RunQuota` 独立合规套件,五后端同一套题、先写套件再写实现——纪律同源 | ✅ |
| III 原子性 | "检查+扣减"单语句/单 Lua/单锁;不触碰终态+唤醒路径 | ✅ |
| IV 数据模型 | 无 Task 字段变更;新类型无 interface{};错误经导出符号表达 | ✅ |
| V 测试纪律 | 介质服务端时间与"注入 clock"铁律的调和:**铁律边界收窄为测试**(模型第 4 节)——生产用介质钟,测试经介质侧时间缝(memory=注入 Clock 本身、sqlite/sqlbroker=包内测试钩子、redis=miniredis 的 SetTime/ARGV 缝);不真 sleep | ✅(附说明) |
| V.5 门控例外 | pg/mysql 的 RunQuota 同样走 DSN 门控 + CI service container | ✅ |
| VI 文档流程 | spec-kit 产物归档;完成记录含基准数据 | ✅ |

**宪法 V.3 说明**:"时间相关逻辑走可注入 clock"原条文假设时间只来自 Go 层。硬配额裁决(#5)要求生产窗口用介质服务端时间,这是对该条的**语义细化而非违背**:测试仍然确定、不真 sleep(时间经介质侧注入),生产路径的"钟"从 Go 层挪到介质层。建议随归档在宪法 V.3 补一句"共享介质的服务端时间属介质行为,测试经介质侧时间缝控制"(MINOR)。

**Constitution Check Result**: ✅ 通过(附 V.3 措辞细化建议)

## Project Structure

### Documentation (this feature)

```text
specs/006-periodic-quota/
├── spec.md
├── plan.md
├── research.md          # 6 个关键决策
├── data-model.md        # 类型/DDL/Lua/校验规则
├── quickstart.md
└── contracts/
    └── quota-capability-contract.md  # QuotaProvider/QuotaGate 行为合同 + RunQuota 用例清单
```

### Source Code (repository root)

```text
taskgate/
├── taskgate.go              # QueueConfig 加 QuotaLimit/QuotaPeriod/QuotaKey + validate 校验
├── broker.go                # QuotaProvider/QuotaGate/QuotaReservation(能力接口,非 Broker 方法)
├── client.go                # New() 能力断言 fail-fast;QueueStats 加两个只读位;Stats 接线
├── scheduler.go             # claimLoop 插入预留环节;quotaState(exhausted/stalled);退避与暂停
├── memorybroker/broker.go   # 单锁 + 注入时钟的窗口计数(语义参考实现)
├── sqlitebroker/            # schema.sql 加 quota 表;新 quota.go(Reserve/Release + 测试时间缝)
├── redisbroker/             # 新 lua/quota_reserve.lua + lua/quota_release.lua;新 quota.go
├── internal/sqlbroker/      # 新 quota.go(PG 单语句 RETURNING / MySQL 两步);Dialect 加配额语句差异钩子
├── brokertest/quota.go      # RunQuota 合规套件(新文件;factory 携带"推介质时间"回调)
├── limiter_test.go / taskgate_test.go  # L1
├── integration_test.go      # L3:调度衔接/fail-closed(stub 配额闸)/双 Gate 共享 sqlite
├── e2e/pipeline_test.go     # SC-005 组合场景
└── bench_test.go            # 基准:有/无配额认领吞吐 + 热点行争用
```

**Structure Decision**: 配额是"能力接口 + 每后端一个 quota.go"——与 LimiterProvider(redisbroker/limiter.go)的既有摆法一致;合规套件放 brokertest 包但独立入口(`RunQuota`),不混进 Broker 的 22 条契约。

## Complexity Tracking

| 超标项 | 原因 | 补救措施 |
|--------|------|----------|
| 认领链从两环变四环(槽→令牌→启发式+预留→Dequeue) | 硬配额裁决要求预留在认领前原子完成,且不占槽等待 | 顺序与释放路径在 scheduler 内集中收口;`QuotaLimit=0` 完全绕开新增环节(零开销路径) |
| redis 脚本破例使用 `TIME` | 裁决 #5:生产窗口不得依赖应用节点钟;配额脚本是唯一豁免(common.lua 的"禁 TIME"约定为其余脚本继续有效) | 豁免范围只限 quota_*.lua,脚本头注释写明;测试用 miniredis 的介质时间控制,确定性不受影响 |
