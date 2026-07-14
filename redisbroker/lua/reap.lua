-- reap.lua 过期租约回收 + blocked 防御修复(合同 ReapExpired 条款,
-- 对照 sqlitebroker/query.go 的 ReapExpired 三步)。
--
-- ARGV: 1 前缀 | 2 now(ms) | 3 LeaseLostMax
--
-- 返回: { 回收条数, { 快照1, 快照2, ... } }

local nowStr = ARGV[2]
local now = tonumber(nowStr)
local maxLost = tonumber(ARGV[3])
local snaps = {}
local count = 0

-- 第一步:扫 inflight 里 lease_until < now 的到期项('(' 开区间:压线不算过期)。
-- 与 sem_acquire.lua 清槽的闭区间(压线即清)口径不同,是有意的口径差异。
-- hash 是权威:zset 里的残留(状态早已不是 running)只做清理,不计入回收。
local expired = redis.call('ZRANGEBYSCORE', kInflight, '-inf', '(' .. nowStr)
for _, id in ipairs(expired) do
  local k = kTask(id)
  if redis.call('EXISTS', k) == 0 then
    redis.call('ZREM', kInflight, id) -- 任务没了,清残留
  else
    local st = redis.call('HGET', k, 'status')
    local leaseUntil = tonumber(redis.call('HGET', k, 'lease_until')) or 0
    if st ~= 'running' or leaseUntil >= now then
      redis.call('ZREM', kInflight, id) -- 与 hash 不符的残留,清掉不处理
    elseif redis.call('HGET', k, 'cancel_requested') == '1' then
      -- 例外条款:用户已请求取消而 worker 崩了(租约过期无人持有)→ 直接 canceled,
      -- 不占 LeaseLost,取消请求不得因 worker 崩溃而丢失;同样计入回收条数并传播。
      redis.call('HSET', k, 'cancel_requested', '0')
      forceCancel(id, 'canceled', nowStr)
      count = count + 1
      snaps[#snaps + 1] = snapshot(id)
      propagate(id, 'canceled', nowStr, snaps)
    else
      local lost = redis.call('HINCRBY', k, 'lease_lost', 1)
      local typ = redis.call('HGET', k, 'type')
      redis.call('ZREM', kInflight, id)
      if lost >= maxLost then
        -- 封顶:failed(固定文案)并传播。
        redis.call('HSET', k, 'status', 'failed',
          'last_error', 'lease expired ' .. lost .. ' times', 'finished_at', nowStr,
          'lease_token', '', 'lease_until', '0', 'cancel_requested', '0')
        moveStatus(id, typ, 'running', 'failed')
        count = count + 1
        snaps[#snaps + 1] = snapshot(id)
        propagate(id, 'failed', nowStr, snaps)
      else
        -- 还有救:清令牌回 pending(认领过说明 run_at 已到点,直接进 pending list)。
        local q = redis.call('HGET', k, 'queue')
        redis.call('HSET', k, 'status', 'pending',
          'lease_token', '', 'lease_until', '0', 'cancel_requested', '0')
        redis.call('RPUSH', kPending(q), id)
        moveStatus(id, typ, 'running', 'pending')
        count = count + 1
        snaps[#snaps + 1] = snapshot(id)
      end
    end
  end
end

-- 第二步:防御修复。blocked 却发现父全是终态 → 按提交时同一套决策(DecideOnSubmit
-- 等价逻辑)补唤醒/补取消;这不是正常路径,是给"唤醒中途崩"这类事故兜底。
local blocked = redis.call('SMEMBERS', kIdxStatus('blocked'))
table.sort(blocked) -- 行为确定(sqlite 按 id 排序)
for _, bid in ipairs(blocked) do
  local bk = kTask(bid)
  -- 可能已被上面的传播顺手处理掉,重查一遍状态。
  if redis.call('EXISTS', bk) == 1 and redis.call('HGET', bk, 'status') == 'blocked' then
    local policy = redis.call('HGET', bk, 'on_parent_fail')
    -- parents 字段:入队时去重后的父列表,\31 拼接(enqueue.lua 写入)。
    local parentsField = redis.call('HGET', bk, 'parents')
    local parents = {}
    if parentsField and parentsField ~= '' then
      for pid in string.gmatch(parentsField, '([^\31]+)') do
        parents[#parents + 1] = pid
      end
    end
    local allExist = true
    local decision, lastError, notFinal = 'pending', '', 0
    for _, pid in ipairs(parents) do
      if redis.call('EXISTS', kTask(pid)) == 0 then
        allExist = false -- 父记录都没了,没法判定,跳过不硬修
        break
      end
      local ps = redis.call('HGET', kTask(pid), 'status')
      if ps == 'failed' or ps == 'canceled' then
        if policy ~= 'ignore_parent_failure' then
          decision = 'canceled'
          lastError = parentFailureReason(pid, ps)
          break
        end
      elseif not isFinal(ps) then
        notFinal = notFinal + 1
      end
    end
    if allExist then
      if decision == 'canceled' then
        forceCancel(bid, lastError, nowStr)
        snaps[#snaps + 1] = snapshot(bid)
        propagate(bid, 'canceled', nowStr, snaps)
      elseif notFinal > 0 then
        redis.call('HSET', bk, 'pending_parents', notFinal) -- 还该等着,顺手校准计数
      else
        -- 补唤醒:blocked → pending。
        local typ = redis.call('HGET', bk, 'type')
        local q = redis.call('HGET', bk, 'queue')
        redis.call('HSET', bk, 'status', 'pending', 'pending_parents', '0')
        placeInQueue(bid, q, redis.call('HGET', bk, 'run_at'), nowStr)
        moveStatus(bid, typ, 'blocked', 'pending')
        snaps[#snaps + 1] = snapshot(bid)
      end
    end
  end
end

return { count, snaps }
