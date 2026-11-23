-- Shared part of every conv script. Go puts this text in front of each
-- script body, so the body can use the names below.
--
-- Rules for all scripts:
--   * Times are unix microseconds of source time. They arrive as strings in
--     ARGV, are read with tonumber (exact below 2^53) and are written back
--     with fmt(). Never use tostring() or a bare number as a Redis argument:
--     Redis Lua (5.1) turns numbers into strings with 14 significant digits,
--     which rounds a 16-digit microsecond time.
--   * No JSON is built here. Outbox entries are flat string fields, and Go
--     builds the conversation event from them.
--   * Keys are built inside the scripts from a prefix. This works on a
--     single Redis node, which is what the project runs. It is not
--     compatible with Redis Cluster.
--   * Any index entry may point at a key that the wall-clock safety lifetime
--     already removed. Every lookup tolerates that.

local PREFIX = ARGV[1]
local THRESHOLD = tonumber(ARGV[2])
local WINDOW = tonumber(ARGV[3])
local IDLE = tonumber(ARGV[4])
local LIMIT = tonumber(ARGV[5])
local TIMEOUT = tonumber(ARGV[6])
local TEXT_KEPT = tonumber(ARGV[7])
local KEPT = tonumber(ARGV[8])
local SESSION_KEPT = tonumber(ARGV[9])
local PENDING_KEPT = tonumber(ARGV[10])
local APPLIED_TTL_MS = ARGV[11]
local MARGIN_MS = tonumber(ARGV[12])
-- ARG is the index of the first script-specific argument.
local ARG = 13

local function fmt(n)
  return string.format('%.0f', n)
end

-- Wall-clock safety lifetime in milliseconds for a key that holds a source
-- window of us microseconds: the window plus the margin.
local function ttl(us)
  return fmt(us / 1000 + MARGIN_MS)
end

local TTL_CONV = ttl(TIMEOUT + SESSION_KEPT)
local TTL_TEXT = ttl(TIMEOUT + TEXT_KEPT)
local TTL_CAND = ttl(WINDOW)
local TTL_PENDING = ttl(PENDING_KEPT)

local K_WATERMARK = PREFIX .. 'wm'
local K_ADMITTED = PREFIX .. 'adm'
local function k_applied(id) return PREFIX .. 'ap:' .. id end
local function k_cand(company) return PREFIX .. 'cand:' .. company end
local function k_conv(key) return PREFIX .. 'c:' .. key end
local function k_msgs(key) return PREFIX .. 'm:' .. key end
local function k_text(id) return PREFIX .. 'x:' .. id end
local function k_author(a) return PREFIX .. 'au:' .. a end
local function k_as_customer(a) return PREFIX .. 'cc:' .. a end
local function k_as_company(a) return PREFIX .. 'ck:' .. a end
local function k_deadlines(p) return PREFIX .. 'dl:' .. p end
local function k_purge(p) return PREFIX .. 'pg:' .. p end
local function k_outbox(p) return PREFIX .. 'ob:' .. p end
local function k_pending_deletion(id) return PREFIX .. 'pd:' .. id end
local function k_pending_removal(a) return PREFIX .. 'pr:' .. a end

-- R collects the result for Go: counters and labels. OB is the set of
-- outbox partitions that got new entries.
local R = {}
local OB = {}

local function count(name, n)
  R[name] = (R[name] or 0) + (n or 1)
end

local function result()
  local out = {}
  for k, v in pairs(R) do
    out[#out + 1] = k
    if type(v) == 'number' then
      out[#out + 1] = fmt(v)
    else
      out[#out + 1] = v
    end
  end
  local parts = {}
  for p in pairs(OB) do
    parts[#parts + 1] = p
  end
  if #parts > 0 then
    out[#out + 1] = 'ob'
    out[#out + 1] = table.concat(parts, ',')
  end
  return out
end

-- mark_applied sets the event ID marker. It returns false when the event
-- was applied before, and the caller must then change nothing.
local function mark_applied(id)
  local ok = redis.call('SET', k_applied(id), '1', 'NX', 'PX', APPLIED_TTL_MS)
  if not ok then
    R.result = 'duplicate'
    return false
  end
  return true
end

-- Conversation hash fields:
--   co, cu   company and customer ID      ch  channel
--   p        events partition             s   session number
--   q        last event sequence number   st  'o' open or 'c' closed
--   o        opened at (session)          l   language (session)
--   nc, nk   customer and company messages of the session
--   last     newest message time of the session
--   un       first unanswered customer message time, or ''
--   la       time of the last answer, or '0'
--   ca       closed at, or ''
local function load_conv(key)
  local flat = redis.call('HGETALL', k_conv(key))
  if #flat == 0 then
    return nil
  end
  local c = {}
  for i = 1, #flat, 2 do
    c[flat[i]] = flat[i + 1]
  end
  return c
end

local function save_conv(key, c)
  redis.call('HSET', k_conv(key),
    'co', c.co, 'cu', c.cu, 'ch', c.ch, 'p', c.p,
    's', c.s, 'q', c.q, 'st', c.st, 'o', c.o, 'l', c.l,
    'nc', c.nc, 'nk', c.nk, 'last', c.last, 'un', c.un, 'la', c.la,
    'ca', c.ca)
  redis.call('PEXPIRE', k_conv(key), TTL_CONV)
end

-- emit appends one conversation event to the outbox of the conversation's
-- partition and gives it the next sequence number. The caller saves c.
local function emit(c, typ, at, msg_id, dir, response, reason)
  c.q = fmt(tonumber(c.q) + 1)
  redis.call('XADD', k_outbox(c.p), '*',
    'ty', typ, 'ch', c.ch, 'co', c.co, 'cu', c.cu,
    's', c.s, 'q', c.q, 't', fmt(at), 'o', c.o, 'l', c.l,
    'nc', c.nc, 'nk', c.nk,
    'm', msg_id or '', 'd', dir or '',
    'r', response and fmt(response) or '', 'cr', reason or '')
  OB[c.p] = true
  count('ev_' .. typ)
end

-- close ends the open session of c at source time at and schedules the
-- removal of its text at at + TEXT_KEPT. Counters stay in the record.
local function close(key, c, reason, at)
  c.st = 'c'
  c.ca = fmt(at)
  redis.call('ZREM', k_deadlines(c.p), key)
  redis.call('SREM', k_as_customer(c.cu), key)
  redis.call('SREM', k_as_company(c.co), key)
  -- The member holds the newest message time the purge removes, so a later
  -- session of the same conversation gets its own purge entry. A removal
  -- can close a session that already holds messages newer than the removal
  -- (the two input topics are consumed independently), so the bound is the
  -- later of the close time and the newest message.
  local bound = math.max(at, tonumber(c.last))
  redis.call('ZADD', k_purge(c.p), fmt(at + TEXT_KEPT), fmt(bound) .. ' ' .. key)
  emit(c, 'closed', at, nil, nil, nil, reason)
  save_conv(key, c)
  count('closed_' .. reason)
end

-- drop_text removes one stored message: its text hash, its place in the
-- conversation and in the author index.
local function drop_text(id)
  local x = redis.call('HMGET', k_text(id), 'k', 'a')
  if x[1] then
    redis.call('ZREM', k_msgs(x[1]), id)
  end
  if x[2] then
    redis.call('SREM', k_author(x[2]), id)
  end
  redis.call('DEL', k_text(id))
end

-- refresh_text renews the safety lifetime of the text of an open
-- conversation, so a long conversation never loses text by expiry.
local function refresh_text(key, c)
  local ids = redis.call('ZRANGE', k_msgs(key), 0, -1)
  for _, id in ipairs(ids) do
    redis.call('PEXPIRE', k_text(id), TTL_TEXT)
  end
  redis.call('PEXPIRE', k_msgs(key), TTL_TEXT)
  redis.call('PEXPIRE', k_author(c.co), TTL_TEXT)
  redis.call('PEXPIRE', k_author(c.cu), TTL_TEXT)
  redis.call('PEXPIRE', k_as_customer(c.cu), TTL_CONV)
  redis.call('PEXPIRE', k_as_company(c.co), TTL_CONV)
end
