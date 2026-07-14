-- finish.lua 五个写终点共用(op = ack / fail / finish_canceled / cancel / requeue):
-- 令牌校验(cancel 不带令牌)→ 状态机校验 → 写流转 → 同脚本工作队列收敛整棵子树传播
-- → 维护 inflight/pending/delayed/索引/计数 → 返回本次全部流转的 hash 快照列表。
-- 对照 sqlitebroker/lifecycle.go 的五个方法 + propagate.go。
--
-- ARGV: 1 前缀 | 2 op | 3 id | 4 令牌(cancel 传空) | 5 now(ms)
--   op=ack:  6 has_result('0'/'1') | 7 result
--   op=fail: 6 errMsg | 7 kind('business'/'throttled'/'skip') | 8 retryAt(ms,零值已换 now)
--            9 throttledMax
--   其余 op 无额外参数
--
-- 返回: 快照列表 { {k,v,...}, {k,v,...}, ... };running 上的 cancel 只打标记,返回空表

local op = ARGV[2]
local id = ARGV[3]
local token = ARGV[4]
local now = ARGV[5]
local k = kTask(id)
local snaps = {}

if op == 'cancel' then
  -- 合同 Cancel:blocked/pending/retrying → canceled 并同脚本传播;
  -- running → 仅置 cancel_requested,返回成功(终态由 FinishCanceled 落);
  -- 终态 → ErrAlreadyFinal;不存在 → ErrTaskNotFound。
  if redis.call('EXISTS', k) == 0 then
    return redis.error_reply('TGERR:not_found:' .. id)
  end
  local st = redis.call('HGET', k, 'status')
  if isFinal(st) then
    return redis.error_reply('TGERR:already_final:task ' .. id .. ' (status=' .. st .. ')')
  end
  if st == 'running' then
    redis.call('HSET', k, 'cancel_requested', '1') -- worker 下次 Heartbeat 收到 ErrTaskCanceled
    return {}
  end
  if not canTrans(st, 'canceled') then
    return redis.error_reply('TGERR:illegal:' .. st .. ':canceled:' .. id)
  end
  forceCancel(id, 'canceled', now)
  snaps[#snaps + 1] = snapshot(id)
  propagate(id, 'canceled', now, snaps)
  return snaps
end

-- 其余四个 op 都要过令牌校验(校验通过则任务必在 running,后续流转都出自 running 行,
-- 与 taskgate.CanTransition 的 running 出边逐一对应:completed/failed/retrying/canceled/pending)。
local e = checkLease(id, token)
if e then return redis.error_reply(e) end
local typ = redis.call('HGET', k, 'type')
local q = redis.call('HGET', k, 'queue')

if op == 'ack' then
  -- running → completed:写 Result/FinishedAt,清租约与取消标记,唤醒子任务。
  redis.call('HSET', k, 'status', 'completed', 'finished_at', now,
    'lease_token', '', 'lease_until', '0', 'cancel_requested', '0')
  if ARGV[6] == '1' then
    redis.call('HSET', k, 'result', ARGV[7])
  end
  redis.call('ZREM', kInflight, id)
  moveStatus(id, typ, 'running', 'completed')
  snaps[#snaps + 1] = snapshot(id)
  propagate(id, 'completed', now, snaps)
  return snaps
end

if op == 'fail' then
  -- 三种失败语义(合同 Fail 条款,三计数各管一段互不占用):
  --   business:Attempts+1,超 MaxRetry → failed,否则 retrying;
  --   throttled:Throttled+1,≥ThrottledMax → failed(固定文案),否则 retrying,Attempts 不动;
  --   skip:直接 failed,三计数全不动。
  local errMsg = ARGV[6]
  local kind = ARGV[7]
  local retryAt = ARGV[8]
  local toFailed = false
  local lastError = errMsg
  if kind == 'business' then
    local attempts = redis.call('HINCRBY', k, 'attempts', 1)
    toFailed = attempts > tonumber(redis.call('HGET', k, 'max_retry'))
  elseif kind == 'throttled' then
    local thr = redis.call('HINCRBY', k, 'throttled', 1)
    if thr >= tonumber(ARGV[9]) then
      toFailed = true
      lastError = 'throttled ' .. thr .. ' times' -- 封顶用固定文案(brokertest 逐字断言)
    end
  else -- skip
    toFailed = true
  end
  if toFailed then
    redis.call('HSET', k, 'status', 'failed', 'last_error', lastError, 'finished_at', now,
      'lease_token', '', 'lease_until', '0', 'cancel_requested', '0')
    redis.call('ZREM', kInflight, id)
    moveStatus(id, typ, 'running', 'failed')
    snaps[#snaps + 1] = snapshot(id)
    propagate(id, 'failed', now, snaps) -- failed 也要在同一脚本里连锁处理子任务
  else
    -- 还有机会:进 retrying,RunAt=retryAt 到点重跑;
    -- cancel_requested 保持原样(与 sqlite 的 retrying 分支一致,取消请求不丢)。
    redis.call('HSET', k, 'status', 'retrying', 'last_error', lastError, 'run_at', retryAt,
      'lease_token', '', 'lease_until', '0')
    redis.call('ZREM', kInflight, id)
    placeInQueue(id, q, retryAt, now)
    moveStatus(id, typ, 'running', 'retrying')
    snaps[#snaps + 1] = snapshot(id)
  end
  return snaps
end

if op == 'finish_canceled' then
  -- worker 响应取消后收尾:running → canceled 落库并传播。
  redis.call('HSET', k, 'cancel_requested', '0')
  forceCancel(id, 'canceled', now)
  snaps[#snaps + 1] = snapshot(id)
  propagate(id, 'canceled', now, snaps)
  return snaps
end

if op == 'requeue' then
  -- 优雅停机归还:running → pending,三计数与 RunAt 全不动,清租约与取消标记。
  redis.call('HSET', k, 'status', 'pending',
    'lease_token', '', 'lease_until', '0', 'cancel_requested', '0')
  redis.call('ZREM', kInflight, id)
  placeInQueue(id, q, redis.call('HGET', k, 'run_at'), now)
  moveStatus(id, typ, 'running', 'pending')
  snaps[#snaps + 1] = snapshot(id)
  return snaps
end

return redis.error_reply('TGERR:bad_op:' .. op)
