-- Applies one message event (decisions 0007 and 0008) in one atomic step:
-- admission, the conversation state, the text, the watermark of the
-- partition and the outbox entries.
--
-- Script arguments, from ARG:
--   event ID, partition, source time, message ID, company ID, customer ID,
--   direction, text, normalised language, channel, conversation key,
--   language tags as sent (joined with ','), '1' for an edit or '0'
local event_id = ARGV[ARG]
local part = ARGV[ARG + 1]
local t = tonumber(ARGV[ARG + 2])
local msg_id = ARGV[ARG + 3]
local co = ARGV[ARG + 4]
local cu = ARGV[ARG + 5]
local dir = ARGV[ARG + 6]
local text = ARGV[ARG + 7]
local lang = ARGV[ARG + 8]
local channel = ARGV[ARG + 9]
local key = ARGV[ARG + 10]
local langs = ARGV[ARG + 11]
local edited = ARGV[ARG + 12] == '1'

if not mark_applied(event_id) then
  return result()
end

-- The watermark of the partition only moves forward. Every record moves
-- it, also one that is dropped later in this script.
local wm = tonumber(redis.call('HGET', K_WATERMARK, part) or '0')
if t > wm then
  wm = t
  redis.call('HSET', K_WATERMARK, part, fmt(t))
end

-- A removal of an author that was processed before this message, and that
-- covers it: the message is older than the removal, and the removal is not
-- older than PENDING_KEPT measured against the watermark of this partition.
local function pending_removal(author)
  local r = redis.call('HMGET', k_pending_removal(author), 'r', 'adm')
  if not r[1] then
    return nil
  end
  local at = tonumber(r[1])
  if t > at or wm > at + PENDING_KEPT then
    return nil
  end
  local adm = nil
  if r[2] then
    adm = tonumber(r[2])
  end
  return { at = at, adm = adm }
end

local rm_customer = pending_removal(cu)
local rm_company = pending_removal(co)
local author = cu
if dir == 'company' then
  author = co
end

-- The text is not stored when the author was removed, or when a deletion of
-- this message came first (decision 0007, "Early deletion").
local text_removed = (dir == 'customer' and rm_customer ~= nil) or (dir == 'company' and rm_company ~= nil)
local pd = redis.call('GET', k_pending_deletion(msg_id))
if pd and wm <= tonumber(pd) + PENDING_KEPT then
  text_removed = true
end

-- Edits (decision 0006): the event says it is an edit. An edit replaces
-- the stored text and language tags and changes nothing else: no counter,
-- sequence number, conversation event, admission or deadline. An edit of a
-- message that is not stored (never admitted, purged or deleted) is dropped
-- and never opens a conversation. A pending early deletion or author
-- removal still wins.
local stored = redis.call('EXISTS', k_text(msg_id)) == 1
if edited then
  if stored and not text_removed then
    redis.call('HSET', k_text(msg_id), 'x', text, 'lg', langs)
    R.result = 'edited'
  else
    R.result = 'edit_dropped'
  end
  return result()
end
-- The same message under another event ID, while its text is stored.
if stored then
  R.result = 'duplicate'
  return result()
end

local c = load_conv(key)
-- The open session ended before this message: close it at its deadline in
-- this step, so the result does not depend on when the sweeper ran.
if c and c.st == 'o' and t >= tonumber(c.last) + TIMEOUT then
  close(key, c, 'inactivity', tonumber(c.last) + TIMEOUT)
end
-- The small session record is forgotten SESSION_KEPT after a close, in
-- source time. The conversation then starts again at session 1.
if c and c.st == 'c' and t > tonumber(c.ca) + SESSION_KEPT then
  redis.call('DEL', k_conv(key))
  c = nil
end
local open = c ~= nil and c.st == 'o'

-- is_admitted reads the admission of the company without changing it.
local function is_admitted()
  if rm_company then
    return rm_company.adm ~= nil and t - rm_company.adm <= IDLE
  end
  local prev = redis.call('ZSCORE', K_ADMITTED, co)
  return prev and t - tonumber(prev) <= IDLE
end

-- admission applies a customer message to the admission state of the
-- company (decision 0008) and reports whether the company is admitted.
local function admission()
  if rm_company then
    -- The company was removed after this message. Decide as the state was
    -- before the removal, and change nothing.
    return rm_company.adm ~= nil and t - rm_company.adm <= IDLE
  end
  -- Clean up admissions that ended long ago. The bound is one margin older
  -- than the idle period, so partitions that are behind in source time do
  -- not lose an admission that is still valid for them.
  local gone = redis.call('ZREMRANGEBYSCORE', K_ADMITTED, '-inf', '(' .. fmt(t - IDLE - MARGIN_MS * 1000))
  if gone > 0 then
    count('admission_ended', gone)
  end

  local prev = redis.call('ZSCORE', K_ADMITTED, co)
  if prev then
    prev = tonumber(prev)
    if t - prev <= IDLE then
      if t > prev then
        redis.call('ZADD', K_ADMITTED, fmt(t), co)
      end
      return true
    end
    -- More than IDLE after the previous customer message: the admission
    -- ends and the company must reach the threshold again.
    redis.call('ZREM', K_ADMITTED, co)
    count('admission_ended')
  end

  local ck = k_cand(co)
  redis.call('ZREMRANGEBYSCORE', ck, '-inf', '(' .. fmt(t - WINDOW))
  local seen = redis.call('ZSCORE', ck, cu)
  if seen then
    if t > tonumber(seen) then
      redis.call('ZADD', ck, fmt(t), cu)
    end
  elseif redis.call('ZCARD', ck) < THRESHOLD then
    -- The set stops growing at THRESHOLD members.
    redis.call('ZADD', ck, fmt(t), cu)
  end
  if redis.call('ZCARD', ck) >= THRESHOLD then
    -- An admission counts while its last customer message is not more than
    -- IDLE before the time of this message.
    if LIMIT > 0 and redis.call('ZCOUNT', K_ADMITTED, fmt(t - IDLE), '+inf') >= LIMIT then
      count('admission_refused')
      redis.call('PEXPIRE', ck, TTL_CAND)
      return false
    end
    redis.call('ZADD', K_ADMITTED, fmt(t), co)
    redis.call('DEL', ck)
    count('admitted')
    return true
  end
  redis.call('PEXPIRE', ck, TTL_CAND)
  return false
end

if dir == 'company' then
  if not open then
    if is_admitted() then
      R.result = 'no_open_conversation'
    else
      R.result = 'not_admitted'
    end
    return result()
  end
else
  local admitted = admission()
  -- A session that is open stays open, also in the rare case that the
  -- admission ended while it was open.
  if not open and not admitted then
    R.result = 'not_admitted'
    return result()
  end
end

local opened = false
if not open then
  -- A new session. The sequence number continues from the record.
  local session, seq = 1, 0
  if c then
    session = tonumber(c.s) + 1
    seq = tonumber(c.q)
  end
  c = {
    co = co, cu = cu, ch = channel, p = part,
    s = fmt(session), q = fmt(seq), st = 'o', o = fmt(t), l = lang,
    nc = '0', nk = '0', last = fmt(t), un = '', la = '0', ca = '',
  }
  redis.call('SADD', k_as_customer(cu), key)
  redis.call('SADD', k_as_company(co), key)
  opened = true
  count('opened')
  if session > 1 then
    count('reopened')
  end
end

-- Counters and the answer.
local response = nil
if dir == 'customer' then
  c.nc = fmt(tonumber(c.nc) + 1)
  -- A customer message older than the last answer counts as answered.
  if t > tonumber(c.la) and (c.un == '' or t < tonumber(c.un)) then
    c.un = fmt(t)
  end
else
  c.nk = fmt(tonumber(c.nk) + 1)
  if c.un ~= '' and t >= tonumber(c.un) then
    response = t - tonumber(c.un)
    c.un = ''
    c.la = fmt(t)
  end
end
if t > tonumber(c.last) then
  c.last = fmt(t)
end

-- Text, ordered by source time, newest KEPT messages only.
if not text_removed then
  redis.call('ZADD', k_msgs(key), fmt(t), msg_id)
  redis.call('HSET', k_text(msg_id), 'k', key, 'a', author, 'd', dir, 't', fmt(t), 'x', text, 'lg', langs)
  redis.call('SADD', k_author(author), msg_id)
  local n = redis.call('ZCARD', k_msgs(key))
  if n > KEPT then
    local old = redis.call('ZRANGE', k_msgs(key), 0, n - KEPT - 1)
    for _, id in ipairs(old) do
      drop_text(id)
    end
    count('trimmed', #old)
  end
else
  count('text_removed')
end
refresh_text(key, c)

redis.call('ZADD', k_deadlines(c.p), fmt(tonumber(c.last) + TIMEOUT), key)

if opened then
  emit(c, 'opened', t, msg_id, dir)
else
  emit(c, 'message_added', t, msg_id, dir)
end
if response then
  emit(c, 'answered', t, msg_id, dir, response)
  R.response_us = response
end
save_conv(key, c)
R.result = 'stored'

-- A removal that came first and covers this message closes the session, as
-- it would have if it came second.
if rm_customer and c.st == 'o' then
  close(key, c, 'customer_removed', rm_customer.at)
end
if rm_company and c.st == 'o' then
  close(key, c, 'company_removed', rm_company.at)
end

return result()
