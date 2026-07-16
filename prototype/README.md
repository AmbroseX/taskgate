# 领域模型原型(裁决第 4 步,先于 spec)

这里是两份领域模型文档的**原型验证代码**,验证的是模型语义,不是存储。
它们不进正式代码:spec 落地后本目录整体删除或按 spec 重写。

| 目录 | 验证什么 | 对应文档 |
|---|---|---|
| `identity/` | BusinessKey / ExecutionID / ReplayOf 四概念:memorybroker 上跑真调度,侧车 map 模拟还不存在的两个字段,过模型第 7 节五断言 + 三条规则性拒绝 | docs/plans/2026-07-16-Identity领域模型.md |
| `quota/` | 硬配额 QuotaReservation:sqlite 共享文件上"检查窗口 + 扣减"单条原子语句(介质钟算窗口键),双进程验证每窗口 ≤N、窗口恢复、kill -9 泄漏只少不多、fail-closed | docs/plans/2026-07-16-Quota领域模型.md |

跑法:`go test ./prototype/...`(quota 的三个多进程场景约 11s,`-short` 跳过)。

验证结果与发现见 docs/plans/2026-07-16-领域模型原型验证记录.md。
