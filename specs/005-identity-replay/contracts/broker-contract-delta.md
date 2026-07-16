# Broker 接口增量合同(005-identity-replay)

基线:`specs/001-m1-core-queue/contracts/broker-contract.md`(15 方法 18 契约)。本文件只记增量。

## 接口变更

### 新增:Replay(15 → 16)

```go
Replay(ctx context.Context, req ReplayRequest) (*Task, error)
```

合同:

- 入参:`ExecutionID` 与 `BusinessKey` 恰好一个非空,否则报参数错误(普通 error,不必哨兵)。
- 原子性:定位 → 校验(终态 / 未被重放 / completed 需 AllowCompleted)→ 创建新执行,**同事务/同 Lua/同临界区**完成;并发同目标恰好一个成功。
- 错误:目标不存在 `ErrTaskNotFound`;未终态 `ErrReplayNotFinal`;已被重放 `ErrAlreadyReplayed`;completed 未显式允许 `ErrCompletedNotAllowed`。
- 成功返回新执行完整快照:新 ulid ID、`ReplayOf=目标`、`BusinessKey` 沿用目标、Type/Queue/MaxRetry/OnParentFailure 沿用、Payload 按 req(nil=复制)、三计数为 0、DependsOn 为空、Status=pending、RunAt=now。
- 目标执行的对外可读记录零改写(redis 的内部 `replayed` 标记不外露)。
- 新执行触发与 Enqueue 相同的队列放置、计数与 Notify;目标不触发任何 Notify。

### 修改:Enqueue

- `t.BusinessKey` 非空时:键下已存在任何执行 → 拒绝,错误满足 `errors.Is(err, ErrTaskExists)` 且 `errors.As` 可取 `*TaskExistsError`(含链尾 ExecutionID 与 Status);并发同键恰好一个成功。
- 预置 `t.ID` 行为保留(测试/嵌入入口):重复 ID 仍返回 `ErrTaskExists`(主键防御)。公开 Submit 路径永不预置 ID。
- `t.ReplayOf` 由 Replay 专属写入;Enqueue 收到非空 `ReplayOf` 直接拒绝(防绕过链约束)。

### 修改:List

- `Filter.BusinessKey` 非空 → 只返回该键下执行;与既有过滤字段是 AND 关系;排序/分页合同不变。

## brokertest 契约变更(18 → 22)

| # | 用例 | 变更 | Given/When/Then 一句话 |
|---|---|---|---|
| 2 | `IdempotentID` → `BusinessKeyIdempotent` | 重写 | 同键二次入队(不论首个执行何状态)→ ErrTaskExists 且可解构链尾;原执行原封不动;预置 ID 重复仍拒(保留原断言) |
| 19 | `ReplayBasic` | 新增 | failed 链尾 Replay → 新执行(ReplayOf/键沿用/计数清零/Payload 复制)入队可跑;目标逐字段不变;completed 无 flag 拒、带 flag 成 |
| 20 | `ReplayChain` | 新增 | E1←E2 链上:对 E1 再 Replay → ErrAlreadyReplayed;按键 Replay 打链尾;在途链尾 Replay → ErrReplayNotFinal;无键执行可 Replay |
| 21 | `BusinessKeyQuery` | 新增 | Filter.BusinessKey 过滤准确;链序 = (CreatedAt,ID) 升序;不存在的键 → 空列表 |
| 22 | `IdentityRace` | 新增 | 同键 N 并发 Enqueue 恰 1 成功;同目标 N 并发 Replay 恰 1 成功(-race 下验) |

依赖溯源断言(spec US2)并入 19:E1 有子 C(DependsOn=[E1]),Replay 后按 C.DependsOn[0] Get 到的是 E1 原结果。
