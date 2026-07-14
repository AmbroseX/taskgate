-- claim.lua 单段原子认领(research 第 1 节:不用 BLMOVE 两步,消灭崩溃窗口):
-- 搬运到期 delayed → 循环 LPOP pending 校验 → 写 running/新令牌/租约 → ZADD inflight。
-- 对照 sqlitebroker/dequeue.go 的 tryClaim(那边一条 UPDATE 子查询,这边 LPOP 循环)。
--
-- ARGV: 1 前缀 | 2 now(ms) | 3 新租约令牌(Go 预生成的 ulid) | 4 队列数 nq
--       5.. nq 组 (队列名, 该队列租约 TTL 毫秒)
--
-- 返回: 认领到 → { 'task', k1, v1, k2, v2, ... }(完整 hash 快照)
--       没认领到 → { 'none', 最近的 delayed 到期时刻 ms(0=没有) },Go 据此决定挂多久

local nowStr = ARGV[2]
local now = tonumber(nowStr)
local token = ARGV[3]
local nq = tonumber(ARGV[4])

-- 第一步:把各队列 delayed 里 score≤now 的搬回 pending list。
-- 只动队列结构,hash 的状态字段不变(pending/retrying 原样,合同以 hash 为准)。
for j = 0, nq - 1 do
  local q = ARGV[5 + j * 2]
  local due = redis.call('ZRANGEBYSCORE', kDelayed(q), '-inf', now)
  for _, id in ipairs(due) do
    redis.call('ZREM', kDelayed(q), id)
    redis.call('RPUSH', kPending(q), id)
  end
end

-- 第二步:按调用方给的队列顺序逐个 LPOP 校验(多队列无优先级合同,顺序遍历即确定行为):
--   * hash 没了 → 残留脏数据,丢弃继续;
--   * status ∉ {pending, retrying} → 残留(比如已被取消),丢弃继续;
--   * run_at > now → 还没到点,放回 delayed 继续;
--   * 合法 → 认领:running + 新令牌 + lease_until=now+TTL(按队列) + 首次 started_at。
for j = 0, nq - 1 do
  local q = ARGV[5 + j * 2]
  local ttl = tonumber(ARGV[6 + j * 2])
  while true do
    local id = redis.call('LPOP', kPending(q))
    if not id then break end
    local k = kTask(id)
    if redis.call('EXISTS', k) == 1 then
      local st = redis.call('HGET', k, 'status')
      if st == 'pending' or st == 'retrying' then
        local runAt = tonumber(redis.call('HGET', k, 'run_at'))
        if runAt > now then
          redis.call('ZADD', kDelayed(q), runAt, id) -- 没到点:放回 delayed,继续找下一个
        else
          local typ = redis.call('HGET', k, 'type')
          local leaseUntil = now + ttl
          redis.call('HSET', k, 'status', 'running', 'lease_token', token,
            'lease_until', leaseUntil)
          if redis.call('HGET', k, 'started_at') == '0' then
            redis.call('HSET', k, 'started_at', nowStr) -- 只记首次开跑
          end
          redis.call('ZADD', kInflight, leaseUntil, id)
          moveStatus(id, typ, st, 'running')
          local snap = snapshot(id)
          table.insert(snap, 1, 'task')
          return snap
        end
      end
      -- 状态不合法的残留:直接丢弃出列(权威状态在 hash 里)。
    end
  end
end

-- 没认领到:算各队列 delayed 最近到期时刻,Go 侧等 min(100ms, 到期−now)。
local nextDue = 0
for j = 0, nq - 1 do
  local q = ARGV[5 + j * 2]
  local first = redis.call('ZRANGE', kDelayed(q), 0, 0, 'WITHSCORES')
  if first[1] then
    local s = tonumber(first[2])
    if nextDue == 0 or s < nextDue then nextDue = s end
  end
end
return { 'none', tostring(nextDue) }
