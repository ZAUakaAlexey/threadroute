-- Applies one author removal (decisions 0004 and 0007):
--   * every stored message of the author is deleted, with one
--     'message_removed' event per message (decision 0011),
--   * open conversations where the author is the customer close with
--     'customer_removed',
--   * open conversations where the author is the company close with
--     'company_removed', and the admission of the company ends,
--   * the removal is remembered for PENDING_KEPT, so a message of the author
--     that is older than the removal and arrives later is handled as if it
--     had come first.
--
-- Script arguments, from ARG: event ID, author ID, removal source time.
local event_id = ARGV[ARG]
local author = ARGV[ARG + 1]
local at = tonumber(ARGV[ARG + 2])

if not mark_applied(event_id) then
  return result()
end

-- Every stored message of the author gets a 'message_removed' event in its
-- own conversation (decision 0011). The work is bounded as before this
-- event existed: one pass over the author index, which holds at most
-- MessagesKept messages per conversation of the author, and only while
-- their text is kept. The events are written and the conversations saved
-- before close_all, which loads the records again.
local ids = redis.call('SMEMBERS', k_author(author))
remove_texts(ids, 'author_removed', at)
redis.call('DEL', k_author(author))
count('messages_removed', #ids)

local function close_all(index, reason)
  local keys = redis.call('SMEMBERS', index)
  for _, key in ipairs(keys) do
    local c = load_conv(key)
    if c and c.st == 'o' then
      close(key, c, reason, at)
    end
  end
  redis.call('DEL', index)
end
close_all(k_as_customer(author), 'customer_removed')
close_all(k_as_company(author), 'company_removed')

local prev = redis.call('ZSCORE', K_ADMITTED, author)
if prev then
  redis.call('ZREM', K_ADMITTED, author)
  count('admission_ended')
end
redis.call('DEL', k_cand(author))

-- Remember the removal. adm keeps the admission as it was before the
-- removal, so an older customer message of this company that arrives later
-- gets the same decision as before. An earlier removal's adm is kept.
local pr = k_pending_removal(author)
redis.call('HSET', pr, 'r', fmt(at))
if prev then
  redis.call('HSET', pr, 'adm', prev)
end
redis.call('PEXPIRE', pr, TTL_PENDING)

R.result = 'removed'
return result()
