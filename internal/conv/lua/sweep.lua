-- Closes the conversations of one partition whose deadline is at or before
-- the watermark of that partition, and removes the text of conversations
-- that closed TEXT_KEPT or more before the watermark (decision 0007).
--
-- Script arguments, from ARG: partition, the most entries to handle of
-- each kind.
local part = ARGV[ARG]
local limit = ARGV[ARG + 1]

local wm = redis.call('HGET', K_WATERMARK, part)
if not wm then
  return result()
end

local due = redis.call('ZRANGEBYSCORE', k_deadlines(part), '-inf', wm, 'LIMIT', '0', limit)
for _, key in ipairs(due) do
  local c = load_conv(key)
  if not c or c.st ~= 'o' then
    -- The record expired by its safety lifetime, or the entry is left over.
    redis.call('ZREM', k_deadlines(part), key)
    count('expired')
  else
    local deadline = tonumber(c.last) + TIMEOUT
    if deadline <= tonumber(wm) then
      close(key, c, 'inactivity', deadline)
    else
      redis.call('ZADD', k_deadlines(part), fmt(deadline), key)
    end
  end
end
R.due = #due

local purge = redis.call('ZRANGEBYSCORE', k_purge(part), '-inf', wm, 'LIMIT', '0', limit)
for _, member in ipairs(purge) do
  local sp = string.find(member, ' ', 1, true)
  local bound = string.sub(member, 1, sp - 1)
  local key = string.sub(member, sp + 1)
  local ids = redis.call('ZRANGEBYSCORE', k_msgs(key), '-inf', bound)
  for _, id in ipairs(ids) do
    drop_text(id)
  end
  count('purged', #ids)
  redis.call('ZREM', k_purge(part), member)
end
R.purge_due = #purge

return result()
