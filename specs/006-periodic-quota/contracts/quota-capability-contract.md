# 配额能力合同(006-periodic-quota)

Broker 接口本体零改动;本合同约束可选能力接口 `QuotaProvider` / `QuotaGate`,由 `brokertest.RunQuota` 独立套件验收(不进 Broker 的 22 条契约)。

## QuotaProvider.QueueQuota(queue, qc) (QuotaGate, error)

- 只在 `qc.QuotaLimit > 0` 时被调用;构造必须廉价、不持有需显式释放的资源(与 LimiterProvider 同约束)。
- quota key = `qc.QuotaKey`,空取 `queue`。同 key 的多个 QuotaGate 实例共享介质计数,实例间无本地共享状态。

## QuotaGate.Reserve(ctx) (*QuotaReservation, error)

- **原子**:取介质服务端时间 → 算窗口(`now/period×period`,unix 秒)→ 检查 `used < limit` → `used+1`,在介质内一个原子单位完成(单语句/单 Lua/单锁);"检查"与"扣减"之间不存在窗口。
- 三态:成功 → 非 nil(携带窗口起点);本窗口耗尽 → `(nil, nil)`(**非错误**);介质故障 → `(nil, err)`,调用方 fail-closed。
- 生产路径不得使用应用进程本地时钟;测试经介质侧时间缝控制(memory=注入 Clock;sqlite/pg/mysql=包级测试钩子作覆盖参数;redis=miniredis SetTime / 真 redis 用 TIME)。
- 窗口轮换时允许(不强制)对旧窗口行做尽力清理。

## QuotaGate.Release(ctx, r) error

- 尽力退还:只作用于 `r.Window` 那个窗口的计数,`used > 0` 才减;窗口已切走/行已清理 → 落空无害(返回 nil)。
- 失败(err≠nil)时调用方**不重试**,该份额度按 leaked(视同消耗)处理——方向永远保守。

## 调度侧合同(scheduler)

- 认领顺序:占槽 → 令牌 → `QueueLen>0` 启发式 → Reserve → 限时 Dequeue;耗尽/故障必须先 ReleaseSlot 再退避,不占 worker 槽等待。
- 认领成功 = consumed(此后 handler 失败/取消/未注册类型均不退);Dequeue 扑空/出错 = 尽力 Release。
- 介质故障期间零 Dequeue;`QueueStats.QuotaStalled=true`;恢复后自动续上。耗尽期间 `QuotaExhausted=true`,下窗自动翻回。
- `QuotaLimit=0`:认领链与本功能引入前完全一致,零新增介质往返。

## RunQuota 套件用例清单

| # | 用例 | Given/When/Then 一句话 |
|---|---|---|
| Q1 | QuotaHardLimit | limit=N 连续预留 N 次成功,第 N+1 次 `(nil,nil)`;Release 一份后恰好能再预留一份 |
| Q2 | QuotaWindowRotate | 耗尽后推介质时间 ≥period → 额度恢复满 N;窗口起点对齐 `now/period×period` |
| Q3 | QuotaStaleRelease | 窗口 1 的预留在窗口 2 Release → 落空无害;窗口 2 额度不受影响 |
| Q4 | QuotaKeyIsolation | 不同 key 各自计数互不占额;同 key 两个 Gate 实例共享计数 |
| Q5 | QuotaConcurrentRace | limit=N,M(>N)并发 Reserve 恰好 N 个成功、M−N 个耗尽态,-race 全绿 |

调度侧(不进 RunQuota,落 L3/L4):耗尽暂停不占槽、fail-closed 零放行、Stats 位、三维度正交组合、双 Gate 共享 sqlite 文件每窗恰好 N。
