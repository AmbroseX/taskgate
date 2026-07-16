-- quota_reserve.lua 周期配额预留(spec 006):单段脚本原子完成
-- "取服务端时间 → 算窗口 → 检查余额 → 扣减 + 续 TTL",检查与扣减之间没有窗口,
-- INCR 与 EXPIRE 同脚本(没有 5.4.1 那种"INCR 后崩、键永不过期"的锁死窗口)。
--
-- ⚠️ TIME 豁免:common.lua 的约定是"脚本内禁 TIME"(fakeclock 铁律),
-- 配额是唯一例外——硬配额裁决 #5 要求窗口用共享介质的服务端钟,不信任应用节点钟。
-- 测试的时间控制走 miniredis.SetTime(介质侧注入),或 ARGV[5] 显式覆盖。
-- 本脚本不与任何任务键交互,不拼接 common.lua 前奏。
--
-- ARGV: 1 键前缀 | 2 qkey | 3 period(秒) | 4 limit | 5 时间覆盖(unix 秒,'' = 用 TIME)
-- 返回: {1, win} 预留成功 | {0, win} 本窗口耗尽(非错误)

if redis.replicate_commands then
  pcall(redis.replicate_commands) -- Redis <7 上 TIME 之后要写命令必须切效果复制;7+ 是空操作
end

local period = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local now
if ARGV[5] ~= '' then
  now = tonumber(ARGV[5])
else
  now = tonumber(redis.call('TIME')[1])
end
local win = math.floor(now / period) * period
local k = ARGV[1] .. 'quota:' .. ARGV[2] .. ':' .. tostring(win)

local used = tonumber(redis.call('GET', k) or '0')
if used >= limit then
  return {0, win}
end
redis.call('INCR', k)
redis.call('EXPIRE', k, period * 2) -- 窗口作废后自清,无需后台回收
return {1, win}
