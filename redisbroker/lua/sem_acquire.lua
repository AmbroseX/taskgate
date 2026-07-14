-- sem_acquire.lua 分布式并发槽:每队列一个 zset tg:sem:{q},
-- member = 槽 ID(Go 生成的 ulid),score = 过期时刻(now + 槽 TTL,毫秒)。
-- 占槽三步在同一段脚本里原子完成(research 第 6 节):
--   1. 清掉已过期的槽(持有者崩溃 → 续期停 → score 到期,在这里自动回收);
--   2. 数一下活着的槽,少于上限才允许占;
--   3. 占:把自己的槽 ID 写进去,score = 过期时刻。
--
-- ARGV: 1 前缀 | 2 队列名 | 3 并发上限 | 4 now(ms) | 5 槽 TTL(ms) | 6 槽 ID(ulid)
-- 返回: 1 = 占到 | 0 = 满员,Go 侧退避后重试

local key = P .. 'sem:' .. ARGV[2]
local now = tonumber(ARGV[4])

-- 清过期用 score≤now 闭区间(槽压线即清),与 reap.lua 租约的 '(' 开区间
-- (压线不算过期)口径不同,是有意的:槽宁可早一拍让位,租约宁可多留一拍。
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
if redis.call('ZCARD', key) < tonumber(ARGV[3]) then
  redis.call('ZADD', key, now + tonumber(ARGV[5]), ARGV[6])
  return 1
end
return 0
