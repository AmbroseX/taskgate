-- common.lua 公共前奏:由 Go 侧拼接在每个业务脚本前面,不单独执行。
-- 约定(research 第 2 节铁律):
--   * 所有脚本 ARGV[1] = 键前缀(默认 "tg:"),全部键在脚本内拼出来;
--   * 时间与 ulid 一律由 Go 通过 ARGV 注入,脚本内禁用 redis.call('TIME') 与 math.random,
--     这样 fakeclock 在 miniredis / 真 Redis 上都直接有效,脚本纯确定性;
--   * 业务错误用 "TGERR:<code>:<detail>" 文本经 redis.error_reply 返回,
--     Go 侧翻译成哨兵错误;网络错误没有这个前缀,原样透传(research 第 9 节)。

local P = ARGV[1]

-- ---- 键名工具(键设计照 data-model.md 第 2 节) ----
local function kTask(id) return P .. 'task:' .. id end
local function kPending(q) return P .. 'pending:' .. q end
local function kDelayed(q) return P .. 'delayed:' .. q end
local function kChildren(id) return P .. 'children:' .. id end
local function kIdxStatus(s) return P .. 'idx:status:' .. s end
local function kIdxType(t) return P .. 'idx:type:' .. t end
local kInflight = P .. 'inflight'
local kStats = P .. 'stats'
local kTypes = P .. 'types'

-- ---- 状态机(与 taskgate.go 的 legalTransitions 表逐行一致,不许漂移) ----
--   blocked  → pending | canceled
--   pending  → running | canceled
--   running  → completed | failed | retrying | canceled | pending(Requeue/Reap 归还)
--   retrying → running | canceled
--   三个终态没有出边
local legal = {
  blocked  = { pending = true, canceled = true },
  pending  = { running = true, canceled = true },
  running  = { completed = true, failed = true, retrying = true, canceled = true, pending = true },
  retrying = { running = true, canceled = true },
}
local function canTrans(from, to)
  local m = legal[from]
  return m ~= nil and m[to] == true
end
local function isFinal(s)
  return s == 'completed' or s == 'failed' or s == 'canceled'
end

-- ---- 索引与计数维护:每次状态流转 idx:status 挪窝 + stats 稀疏矩阵旧-1新+1 ----
local function moveStatus(id, typ, from, to)
  redis.call('SREM', kIdxStatus(from), id)
  redis.call('SADD', kIdxStatus(to), id)
  redis.call('HINCRBY', kStats, typ .. ':' .. from, -1)
  redis.call('HINCRBY', kStats, typ .. ':' .. to, 1)
end

-- 从队列结构里摘除(pending list 与 delayed zset 都摘,幂等):
-- QueueLen = LLEN + ZCARD,靠"离开排队状态就立刻摘干净"保证不虚高。
local function removeFromQueue(id, q)
  redis.call('LREM', kPending(q), 0, id)
  redis.call('ZREM', kDelayed(q), id)
end

-- 放进队列结构:到点进 pending list(FIFO),没到点进 delayed zset(score=run_at)。
local function placeInQueue(id, q, runAt, now)
  if tonumber(runAt) > tonumber(now) then
    redis.call('ZADD', kDelayed(q), tonumber(runAt), id)
  else
    redis.call('RPUSH', kPending(q), id)
  end
end

-- 流转后的完整 hash 快照(扁平 k,v 数组),Go 侧解码后逐条异步 Notify。
local function snapshot(id)
  return redis.call('HGETALL', kTask(id))
end

-- 令牌校验三连(与 memorybroker.checkLease 同口径,合同"状态写入通用规则"):
-- 不存在 → TGERR:not_found;非 running / 空令牌 / 令牌不符 → TGERR:lease_lost。
local function checkLease(id, token)
  local k = kTask(id)
  if redis.call('EXISTS', k) == 0 then
    return 'TGERR:not_found:' .. id
  end
  local st = redis.call('HGET', k, 'status')
  if st ~= 'running' or token == '' or redis.call('HGET', k, 'lease_token') ~= token then
    return 'TGERR:lease_lost:task ' .. id .. ' (status=' .. tostring(st) .. ')'
  end
  return nil
end

-- 连锁取消的固定文案(与 taskgate.ParentFailureReason 逐字一致,brokertest 逐字断言)。
local function parentFailureReason(pid, pstatus)
  if pstatus == 'canceled' then
    return 'parent ' .. pid .. ' canceled'
  end
  return 'parent ' .. pid .. ' failed'
end

-- 把任务写成 canceled:写终态字段、清租约、摘队列、摘 inflight、动索引计数。
-- Cancel/FinishCanceled/传播/Reap 取消分支共用。
local function forceCancel(id, reason, now)
  local k = kTask(id)
  local typ = redis.call('HGET', k, 'type')
  local q = redis.call('HGET', k, 'queue')
  local from = redis.call('HGET', k, 'status')
  redis.call('HSET', k, 'status', 'canceled', 'last_error', reason, 'finished_at', now,
    'lease_token', '', 'lease_until', '0')
  removeFromQueue(id, q)
  redis.call('ZREM', kInflight, id)
  moveStatus(id, typ, from, 'canceled')
end

-- DecideOnParentFinal 的 Lua 等价(逐行对照 deps.go DecideOnParentFinal):
-- 返回 newPending, action;action: 0=不动 1=唤醒 2=连锁取消。
--   * 子已终态 → 不动;父没到终态 → 防御性不动;
--   * 父 completed 或策略 ignore_parent_failure → 计数减一(不减穿),
--     减到 0 且子还在 blocked → 唤醒;
--   * 否则(父 failed/canceled 且 fail_fast)→ 连锁取消,计数保持原样。
local function decideOnParentFinal(parentStatus, childStatus, policy, pending)
  if isFinal(childStatus) then return pending, 0 end
  if not isFinal(parentStatus) then return pending, 0 end
  local satisfied = (parentStatus == 'completed') or (policy == 'ignore_parent_failure')
  if not satisfied then return pending, 2 end
  if pending > 0 then pending = pending - 1 end
  if pending == 0 and childStatus == 'blocked' then return 0, 1 end
  return pending, 0
end

-- propagate 终态传播:工作队列在同一段脚本内收敛整棵子树(宪法 III / research 第 3 节),
-- 每层只碰直接子任务,子被连锁取消后再入队处理孙,不递归。
-- 对照 sqlitebroker/propagate.go 的 propagateTx,决策逻辑同 deps.go 两个纯函数。
local function propagate(rootId, rootStatus, now, snaps)
  local work = { { rootId, rootStatus } }
  local i = 1
  while i <= #work do
    local pid, pstatus = work[i][1], work[i][2]
    i = i + 1
    local children = redis.call('SMEMBERS', kChildren(pid))
    table.sort(children) -- 行为确定,便于排查(sqlite 按 child_id 排序)
    for _, cid in ipairs(children) do
      local ck = kTask(cid)
      if redis.call('EXISTS', ck) == 1 then
        local cst = redis.call('HGET', ck, 'status')
        local policy = redis.call('HGET', ck, 'on_parent_fail')
        local pending = tonumber(redis.call('HGET', ck, 'pending_parents')) or 0
        local newPending, action = decideOnParentFinal(pstatus, cst, policy, pending)
        if action == 1 then
          -- 唤醒:blocked → pending,按 run_at 决定进 pending list 还是 delayed zset。
          if canTrans(cst, 'pending') then
            local typ = redis.call('HGET', ck, 'type')
            local q = redis.call('HGET', ck, 'queue')
            local runAt = redis.call('HGET', ck, 'run_at')
            redis.call('HSET', ck, 'status', 'pending', 'pending_parents', newPending)
            placeInQueue(cid, q, runAt, now)
            moveStatus(cid, typ, cst, 'pending')
            snaps[#snaps + 1] = snapshot(cid)
          end
        elseif action == 2 then
          if cst == 'running' then
            -- 防御:正常流程里子等父时不可能在跑;万一出现,打取消标记走 Heartbeat 通道。
            redis.call('HSET', ck, 'cancel_requested', '1')
          elseif canTrans(cst, 'canceled') then
            redis.call('HSET', ck, 'pending_parents', newPending)
            forceCancel(cid, parentFailureReason(pid, pstatus), now)
            snaps[#snaps + 1] = snapshot(cid)
            work[#work + 1] = { cid, 'canceled' } -- 链式:接着处理它的直接子任务
          end
        else
          -- 计数减了但没减到 0,或子已在终态:只落新计数。
          redis.call('HSET', ck, 'pending_parents', newPending)
        end
      end
    end
  end
end
