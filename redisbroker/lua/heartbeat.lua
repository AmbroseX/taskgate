-- heartbeat.lua 续租(合同 Heartbeat 条款,对照 sqlitebroker/lifecycle.go 的 Heartbeat):
-- 令牌校验 → lease_until = now + TTL(hash 与 inflight zset 的 score 一起续)
-- → 返回 cancel_requested 标记(续租照做;Go 侧看到 '1' 才返回 ErrTaskCanceled)。
--
-- ARGV: 1 前缀 | 2 id | 3 令牌 | 4 now(ms) | 5 缺省 TTL(ms)
--       6 覆写条数 np | 7.. np 组 (队列名, TTL 毫秒)——LeaseTTL 按队列覆写,
--       队列名要读 hash 才知道,所以整张表传进来在脚本里挑(对应 Go 侧 ttlFor)。
--
-- 返回: '0' / '1'(cancel_requested 标记)

local id = ARGV[2]
local e = checkLease(id, ARGV[3])
if e then return redis.error_reply(e) end

local k = kTask(id)
local q = redis.call('HGET', k, 'queue')
local ttl = tonumber(ARGV[5])
local np = tonumber(ARGV[6])
for j = 0, np - 1 do
  if ARGV[7 + j * 2] == q then
    ttl = tonumber(ARGV[8 + j * 2])
  end
end

local leaseUntil = tonumber(ARGV[4]) + ttl
redis.call('HSET', k, 'lease_until', leaseUntil)
redis.call('ZADD', kInflight, leaseUntil, id)
return redis.call('HGET', k, 'cancel_requested')
