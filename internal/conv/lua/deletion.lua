-- Applies one deletion (decisions 0004 and 0007): the text and the index
-- entries of the message go at once, counters stay. A deletion of a message
-- that is not stored is remembered, and the message script applies it if
-- the message arrives within PENDING_KEPT.
--
-- Script arguments, from ARG: event ID, message ID, deletion source time.
local event_id = ARGV[ARG]
local msg_id = ARGV[ARG + 1]
local at = ARGV[ARG + 2]

if not mark_applied(event_id) then
  return result()
end

if redis.call('EXISTS', k_text(msg_id)) == 1 then
  drop_text(msg_id)
  R.result = 'removed'
else
  redis.call('SET', k_pending_deletion(msg_id), at, 'PX', TTL_PENDING)
  R.result = 'pending'
end
return result()
