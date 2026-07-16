-- replay.lua 重放一次终态执行(spec 005,合同见 broker-contract-delta.md):
-- 同一段脚本内完成:定位目标(按 ID 或按键取链尾)→ 校验(终态/未被重放/
-- completed 需显式允许)→ 建新 hash → 目标打 replayed 标记(链元数据,Get 不外露)
-- → 链索引 RPUSH → 入队结构/状态索引/计数。并发同目标重放天然串行,恰好一个成功。
-- 目标 hash 除 replayed 标记外零字段改写(终态不可变)。
--
-- ARGV: 1 前缀 | 2 execID('' = 按键) | 3 businessKey | 4 allowCompleted(0/1)
--       5 hasPayload(0/1) | 6 payload(覆盖值) | 7 新执行 ID(Go 生成 ulid) | 8 now(ms)
--
-- 返回: 新执行 hash 的扁平数组(HGETALL 形式,Go 侧 decodeTask 还原)

local execID = ARGV[2]
local bk = ARGV[3]
local allowCompleted = ARGV[4] == '1'
local newID = ARGV[7]
local now = ARGV[8]

-- 定位目标。
local target
if execID ~= '' then
  if redis.call('EXISTS', kTask(execID)) == 0 then
    return redis.error_reply('TGERR:not_found:' .. execID)
  end
  target = execID
else
  local tail = redis.call('LINDEX', kBk(bk), -1)
  if not tail then
    return redis.error_reply('TGERR:not_found:business key ' .. bk)
  end
  target = tail
end

-- 前置校验:终态 → 链尾(未被重放) → completed 显式允许。
local status = redis.call('HGET', kTask(target), 'status')
if not isFinal(status) then
  return redis.error_reply('TGERR:replay_not_final:' .. target .. '\31' .. status)
end
if redis.call('HGET', kTask(target), 'replayed') == '1' then
  return redis.error_reply('TGERR:already_replayed:' .. target)
end
if status == 'completed' and not allowCompleted then
  return redis.error_reply('TGERR:completed_not_allowed:' .. target)
end

-- 创建新执行:沿用目标的 Type/Queue/MaxRetry/OnParentFailure 与业务键;
-- Payload 按 hasPayload 决定复制还是覆盖;三计数清零、无依赖、pending。
local payload = ARGV[6]
if ARGV[5] == '0' then
  payload = redis.call('HGET', kTask(target), 'payload') or ''
end
local typ = redis.call('HGET', kTask(target), 'type')
local q = redis.call('HGET', kTask(target), 'queue')
local maxRetry = redis.call('HGET', kTask(target), 'max_retry')
local policy = redis.call('HGET', kTask(target), 'on_parent_fail')
local tbk = redis.call('HGET', kTask(target), 'business_key') or ''

redis.call('HSET', kTask(newID),
  'id', newID, 'type', typ, 'queue', q, 'payload', payload,
  'status', 'pending', 'result', '', 'last_error', '',
  'attempts', '0', 'max_retry', maxRetry, 'lease_lost', '0', 'throttled', '0',
  'run_at', now, 'depends_on', '[]', 'on_parent_fail', policy,
  'pending_parents', '0', 'parents', '',
  'lease_token', '', 'lease_until', '0', 'cancel_requested', '0',
  'created_at', now, 'started_at', '0', 'finished_at', '0',
  'business_key', tbk, 'replay_of', target)
redis.call('HSET', kTask(target), 'replayed', '1')
if tbk ~= '' then
  redis.call('RPUSH', kBk(tbk), newID)
end
placeInQueue(newID, q, now, now)
redis.call('SADD', kIdxStatus('pending'), newID)
redis.call('SADD', kIdxType(typ), newID)
redis.call('SADD', kTypes, typ)
redis.call('HINCRBY', kStats, typ .. ':pending', 1)

return redis.call('HGETALL', kTask(newID))
