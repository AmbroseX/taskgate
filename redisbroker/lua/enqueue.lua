-- enqueue.lua 入队(合同 Enqueue 条款,对照 sqlitebroker/enqueue.go):
-- 同一段脚本内完成:ID 查重 → 父存在性校验 → DecideOnSubmit 等价判定 → 落 hash
-- → 入 pending/delayed 或直接 canceled → 反向索引/状态索引/计数/types。
--
-- ARGV: 1 前缀 | 2 id | 3 type | 4 queue | 5 payload | 6 result | 7 max_retry
--       8 attempts | 9 lease_lost | 10 throttled | 11 run_at(ms,Go 已把零值换成 now)
--       12 depends_on(原样 JSON 文本,往返一致) | 13 policy | 14 now(ms)
--       15 last_error | 16 started_at(ms) | 17 finished_at(ms)(15~17 调用方预置值,
--       原样落库,与 memory/sqlite 对齐;canceled 判定会覆盖 last_error/finished_at)
--       18 去重后父任务数 n | 19.. n 个父任务 ID(保持 DependsOn 原有顺序)
--
-- 返回: { 初始状态, LastError }(Go 侧据此回填快照;报错时 Go 不回填 t.ID)

local id = ARGV[2]
local typ = ARGV[3]
local q = ARGV[4]
local runAt = ARGV[11]
local policy = ARGV[13]
local now = ARGV[14]
local n = tonumber(ARGV[18])

-- 查重:同 ID 已存在 → 拒收,原任务一个字段都不动。
if redis.call('EXISTS', kTask(id)) == 1 then
  return redis.error_reply('TGERR:exists:' .. id)
end

-- 父任务必须全部已存在(依赖无环靠这条提交校验,不做环检测)。
local parents, pstatuses = {}, {}
for j = 1, n do
  local pid = ARGV[18 + j]
  if redis.call('EXISTS', kTask(pid)) == 0 then
    return redis.error_reply('TGERR:not_found:parent ' .. pid .. ' (child ' .. id .. ')')
  end
  parents[j] = pid
  pstatuses[j] = redis.call('HGET', kTask(pid), 'status')
end

-- DecideOnSubmit 等价逻辑(逐行对照 deps.go DecideOnSubmit;父列表已由 Go 去重):
--   * fail_fast 且任一父 failed/canceled → 直接 canceled(取第一个这样的父拼文案);
--   * ignore_parent_failure 下父进了终态(哪怕失败)就算满足;
--   * 还有父没到终态 → blocked,记未完成父数;否则 pending。
-- lastError 起始值取调用方预置值(通常是空串),只有连锁 canceled 才覆盖成固定文案。
local status, pendingParents, lastError = 'pending', 0, ARGV[15]
local notFinal = 0
for j = 1, n do
  local ps = pstatuses[j]
  if ps == 'failed' or ps == 'canceled' then
    if policy ~= 'ignore_parent_failure' then
      status = 'canceled'
      lastError = parentFailureReason(parents[j], ps)
      break
    end
    -- ignore_parent_failure:失败/取消也算满足,不计入未完成数。
  elseif not isFinal(ps) then
    notFinal = notFinal + 1
  end
end
if status ~= 'canceled' and notFinal > 0 then
  status = 'blocked'
  pendingParents = notFinal
end

-- 落 hash:字段与 sqlite 的 tasks 表列一一对应;parents 字段是去重后父列表
-- 用 \31(单元分隔符)拼接,给 reap.lua 的防御修复用(免依赖 cjson)。
local finishedAt = ARGV[17] -- 预置值原样落库;提交即 canceled 才覆盖成 now
if status == 'canceled' then finishedAt = now end
redis.call('HSET', kTask(id),
  'id', id, 'type', typ, 'queue', q, 'payload', ARGV[5],
  'status', status, 'result', ARGV[6], 'last_error', lastError,
  'attempts', ARGV[8], 'max_retry', ARGV[7], 'lease_lost', ARGV[9], 'throttled', ARGV[10],
  'run_at', runAt, 'depends_on', ARGV[12], 'on_parent_fail', policy,
  'pending_parents', pendingParents, 'parents', table.concat(parents, '\31'),
  'lease_token', '', 'lease_until', '0', 'cancel_requested', '0',
  'created_at', now, 'started_at', ARGV[16], 'finished_at', finishedAt)

-- 反向依赖索引:父到终态时按 children:{pid} 找直接子任务(父已终态的也登记,
-- 传播时由 DecideOnParentFinal 判成不动,语义与 memorybroker 的 children 一致)。
for j = 1, n do
  redis.call('SADD', kChildren(parents[j]), id)
end

-- 只有 pending 进队列结构;blocked 等唤醒,canceled 生来即终态。
-- (新任务不可能有子任务,所以提交即 canceled 无需触发传播,与另两后端一致。)
if status == 'pending' then
  placeInQueue(id, q, runAt, now)
end

redis.call('SADD', kIdxStatus(status), id)
redis.call('SADD', kIdxType(typ), id)
redis.call('SADD', kTypes, typ)
redis.call('HINCRBY', kStats, typ .. ':' .. status, 1)

return { status, lastError }
