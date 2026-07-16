-- quota_release.lua 周期配额尽力退还(spec 006):只退指定窗口的计数,
-- 键已过期/窗口已切走则落空无害。不取时间,不碰任务键。
--
-- ARGV: 1 键前缀 | 2 qkey | 3 win(预留时返回的窗口起点)
-- 返回: 1(无条件;退没退成对调用方等价——失败方向永远保守)

local k = ARGV[1] .. 'quota:' .. ARGV[2] .. ':' .. ARGV[3]
local used = tonumber(redis.call('GET', k) or '0')
if used > 0 then
  redis.call('DECR', k)
end
return 1
