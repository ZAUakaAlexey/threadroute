-- Queries of the router (decisions 0009 and 0010). See the lock order in
-- migrations/00002_routing.sql. Every time is a parameter: the router
-- never uses the database clock for a routing decision.
--
-- # Who can serve a conversation
--
-- One rule decides which operators can serve a conversation (decision
-- 0009, "Assignment", step 2, and "A language that no online operator
-- has"). PickOperator, WaitingConversations and UnskippedOperatorExists
-- each write it out, in the same form, so they cannot disagree. Keep the
-- three in step.
--
-- The operator is online and in the group of the queue: the team of the
-- company for a team queue (dedicated_company_id = company), the shared
-- pool otherwise (dedicated_company_id IS NULL). And the operator has the
-- serving language of the conversation:
--
--   CASE WHEN <shared queue> AND <language> <> ALL (ARRAY(
--            SELECT DISTINCT x.lang FROM operator_languages AS x
--            JOIN operators AS xo ON xo.id = x.operator_id
--            WHERE xo.presence = 'online' AND xo.dedicated_company_id IS NULL))
--        THEN 'und' ELSE <language> END
--
-- So it is the language of the conversation, or 'und' (the general queue)
-- when the queue is shared and no online operator of the shared pool has
-- the language. That test looks only at presence, group and language, not
-- at load, skips or locks: a language whose operators are all busy or all
-- skipped still waits for them. The ARRAY sub-select does not depend on
-- the conversation, so PostgreSQL runs it once per statement and not once
-- per waiting conversation (README, stage 4, has the numbers). The conversation keeps its language and
-- its place in the queue; nothing is stored, and the rule is checked again
-- at every pick. A team queue never uses the general queue: a conversation
-- whose language no team member has is already in the shared queue
-- (decision 0009, "Languages in a team").

-- name: GetConversation :one
SELECT * FROM conversations
WHERE company_id = @company_id AND customer_id = @customer_id;

-- name: LockConversation :one
-- LockConversation locks the conversation row and waits for it.
SELECT * FROM conversations
WHERE company_id = @company_id AND customer_id = @customer_id
FOR UPDATE;

-- name: TryLockConversation :one
-- TryLockConversation locks the conversation row, or returns no row when
-- another transaction holds it. For the timer sweeps.
SELECT * FROM conversations
WHERE company_id = @company_id AND customer_id = @customer_id
FOR UPDATE SKIP LOCKED;

-- name: TryLockWaitingConversation :one
-- TryLockWaitingConversation locks a waiting conversation for assignment,
-- or returns no row when it is locked or no longer waiting (decision
-- 0009, "Assignment", step 3).
SELECT * FROM conversations
WHERE company_id = @company_id AND customer_id = @customer_id AND state = 'waiting'
FOR UPDATE SKIP LOCKED;

-- name: InsertConversation :execrows
-- InsertConversation adds a conversation. It changes nothing when the key
-- exists, so a caller that raced with another insert locks the row next.
INSERT INTO conversations (
    company_id, customer_id, conv_session, conv_seq, lang, state, updated_at
) VALUES (
    @company_id, @customer_id, @conv_session, @conv_seq, @lang, 'closed', @updated_at
)
ON CONFLICT (company_id, customer_id) DO NOTHING;

-- name: SaveConversation :exec
-- SaveConversation writes every changeable column of a locked row.
UPDATE conversations SET
    conv_session = @conv_session,
    conv_seq = @conv_seq,
    lang = @lang,
    state = @state,
    team_queue = @team_queue,
    priority = @priority,
    queued_at = @queued_at,
    preferred_operator_id = @preferred_operator_id,
    last_operator_id = @last_operator_id,
    last_offer_id = @last_offer_id,
    route_seq = @route_seq,
    sla_started_at = @sla_started_at,
    first_accepted_at = @first_accepted_at,
    first_replied_at = @first_replied_at,
    sla_assignment_missed = @sla_assignment_missed,
    sla_response_missed = @sla_response_missed,
    sim_response_us = @sim_response_us,
    real_response_us = @real_response_us,
    close_reason = @close_reason,
    updated_at = @updated_at
WHERE company_id = @company_id AND customer_id = @customer_id;

-- name: TeamServesLang :one
-- TeamServesLang reports whether the company has an active team with a
-- member who has the language (decision 0009, "Languages in a team").
SELECT EXISTS (
    SELECT 1
    FROM companies AS c
    JOIN operators AS o ON o.dedicated_company_id = c.company_id
    JOIN operator_languages AS ol ON ol.operator_id = o.id AND ol.lang = @lang
    WHERE c.company_id = @company_id AND c.team_active
)::boolean AS serves;

-- name: WaitingConversations :many
-- WaitingConversations lists waiting conversations in queue order, after a
-- keyset cursor, without locking them. Only conversations for which some
-- operator who can serve it (see "Who can serve a conversation" above) has
-- a free slot are listed, so a queue that nobody can serve now does not
-- hide the ones behind it. Skipped operators are not filtered here,
-- because assignment clears the skips of a conversation that every
-- operator has skipped.
SELECT cv.company_id, cv.customer_id, cv.priority, cv.queued_at::timestamptz AS queued_at
FROM conversations AS cv
WHERE cv.state = 'waiting'
  AND (
      @first::boolean
      OR cv.priority < @after_priority::integer
      OR (cv.priority = @after_priority::integer
          AND (cv.queued_at, cv.company_id, cv.customer_id)
              > (@after_queued_at::timestamptz, @after_company_id::text, @after_customer_id::text))
  )
  AND EXISTS (
      SELECT 1
      FROM operator_languages AS ol
      JOIN operators AS o ON o.id = ol.operator_id
      WHERE ol.lang = (CASE WHEN NOT cv.team_queue AND cv.lang <> ALL (ARRAY(
                           SELECT DISTINCT x.lang FROM operator_languages AS x
                           JOIN operators AS xo ON xo.id = x.operator_id
                           WHERE xo.presence = 'online' AND xo.dedicated_company_id IS NULL))
                       THEN 'und' ELSE cv.lang END)
        AND o.presence = 'online'
        AND o.dedicated_company_id IS NOT DISTINCT FROM (CASE WHEN cv.team_queue THEN cv.company_id END)
        AND (SELECT count(*) FROM offers AS f
             WHERE f.operator_id = o.id AND f.state IN ('offered', 'accepted')) < o.capacity
  )
ORDER BY cv.priority DESC, cv.queued_at, cv.company_id, cv.customer_id
LIMIT @max_rows;

-- name: PickOperator :one
-- PickOperator picks and locks the operator for a conversation (decision
-- 0009, "Assignment", step 2): an operator who can serve it (see "Who can
-- serve a conversation" above), not skipped, with a free slot. The order
-- is: the preferred operator first, then the lowest load, then the highest
-- priority, then the operator ID. The load in this statement is read from
-- the snapshot of the statement, so the caller counts it again with
-- OperatorSlots after the lock is held. general_queue reports an operator
-- who serves the conversation only through the general queue rule: it
-- does not have the language.
SELECT
    o.id,
    (NOT EXISTS (
        SELECT 1 FROM operator_languages AS ol
        WHERE ol.operator_id = o.id AND ol.lang = @lang
    ))::boolean AS general_queue
FROM operators AS o
WHERE o.presence = 'online'
  AND o.dedicated_company_id IS NOT DISTINCT FROM sqlc.narg(team_company_id)::text
  AND EXISTS (
      SELECT 1 FROM operator_languages AS ol
      WHERE ol.operator_id = o.id
        AND ol.lang = (CASE WHEN sqlc.narg(team_company_id)::text IS NULL AND @lang <> ALL (ARRAY(
                           SELECT DISTINCT x.lang FROM operator_languages AS x
                           JOIN operators AS xo ON xo.id = x.operator_id
                           WHERE xo.presence = 'online' AND xo.dedicated_company_id IS NULL))
                       THEN 'und' ELSE @lang END)
  )
  AND (NOT @use_skips::boolean OR NOT EXISTS (
      SELECT 1 FROM offer_skips AS s
      WHERE s.company_id = @company_id AND s.customer_id = @customer_id
        AND s.session = @session AND s.operator_id = o.id
  ))
  AND NOT (o.id = ANY (@exclude::text[]))
  AND (SELECT count(*) FROM offers AS f
       WHERE f.operator_id = o.id AND f.state IN ('offered', 'accepted')) < o.capacity
ORDER BY
    (o.id = @preferred_operator_id::text) DESC,
    (SELECT count(*) FROM offers AS f
     WHERE f.operator_id = o.id AND f.state IN ('offered', 'accepted')),
    o.priority DESC,
    o.id
LIMIT 1
FOR UPDATE OF o SKIP LOCKED;

-- name: OperatorSlots :one
-- OperatorSlots reads an operator and counts its live slots. Run it in a
-- new statement after the operator row is locked, so the count is exact.
SELECT
    o.capacity,
    o.presence,
    o.dedicated_company_id,
    (SELECT count(*) FROM offers AS f
     WHERE f.operator_id = o.id AND f.state IN ('offered', 'accepted'))::integer AS live
FROM operators AS o
WHERE o.id = @id;

-- name: UnskippedOperatorExists :one
-- UnskippedOperatorExists reports whether an operator who can serve the
-- conversation (see "Who can serve a conversation" above) is not skipped
-- for the session, whatever its load. Without a lock: it only decides
-- whether the skips are cleared.
SELECT EXISTS (
    SELECT 1
    FROM operators AS o
    JOIN operator_languages AS ol ON ol.operator_id = o.id
        AND ol.lang = (CASE WHEN sqlc.narg(team_company_id)::text IS NULL AND @lang <> ALL (ARRAY(
                           SELECT DISTINCT x.lang FROM operator_languages AS x
                           JOIN operators AS xo ON xo.id = x.operator_id
                           WHERE xo.presence = 'online' AND xo.dedicated_company_id IS NULL))
                       THEN 'und' ELSE @lang END)
    WHERE o.presence = 'online'
      AND o.dedicated_company_id IS NOT DISTINCT FROM sqlc.narg(team_company_id)::text
      AND NOT EXISTS (
          SELECT 1 FROM offer_skips AS s
          WHERE s.company_id = @company_id AND s.customer_id = @customer_id
            AND s.session = @session AND s.operator_id = o.id
      )
)::boolean AS found;

-- name: HasSkips :one
SELECT EXISTS (
    SELECT 1 FROM offer_skips
    WHERE company_id = @company_id AND customer_id = @customer_id AND session = @session
)::boolean AS has_skips;

-- name: ClearSkips :execrows
DELETE FROM offer_skips
WHERE company_id = @company_id AND customer_id = @customer_id
  AND (session = @session OR @all_sessions::boolean);

-- name: InsertSkip :exec
INSERT INTO offer_skips (company_id, customer_id, session, operator_id, offer_num)
VALUES (@company_id, @customer_id, @session, @operator_id, @offer_num)
ON CONFLICT DO NOTHING;

-- name: InsertOffer :one
INSERT INTO offers (
    company_id, customer_id, session, operator_id, state, capacity,
    queued_at, offered_at, accept_by
) VALUES (
    @company_id, @customer_id, @session, @operator_id, 'offered', @capacity,
    @queued_at, @offered_at, @accept_by
)
RETURNING num, id;

-- name: GetOfferByID :one
-- GetOfferByID reads an offer without a lock, to find its conversation.
SELECT * FROM offers WHERE id = @id;

-- name: LockOffer :one
-- LockOffer locks an offer. The caller holds the conversation lock.
SELECT * FROM offers WHERE num = @num FOR UPDATE;

-- name: LiveOffer :one
-- LiveOffer returns the live offer of a conversation and locks it. The
-- caller holds the conversation lock.
SELECT * FROM offers
WHERE company_id = @company_id AND customer_id = @customer_id
  AND state IN ('offered', 'accepted')
FOR UPDATE;

-- name: SaveOffer :exec
UPDATE offers SET
    state = @state,
    accepted_at = @accepted_at,
    ended_at = @ended_at,
    end_reason = @end_reason
WHERE num = @num;

-- name: LiveOffersOfOperator :many
-- LiveOffersOfOperator lists the live offers of an operator by
-- conversation key, the order in which Offline locks them.
SELECT num, company_id, customer_id FROM offers
WHERE operator_id = @operator_id AND state IN ('offered', 'accepted')
ORDER BY company_id, customer_id;

-- name: DueOffers :many
-- DueOffers lists offers past their accept timeout, oldest first.
SELECT num, company_id, customer_id FROM offers
WHERE state = 'offered' AND accept_by <= @now
ORDER BY accept_by, num
LIMIT @max_rows;

-- name: InsertReply :execrows
INSERT INTO replies (offer_num, reply_id, replied_at)
VALUES (@offer_num, @reply_id, @replied_at)
ON CONFLICT DO NOTHING;

-- name: ReplyExists :one
SELECT EXISTS (
    SELECT 1 FROM replies WHERE offer_num = @offer_num AND reply_id = @reply_id
)::boolean AS seen;

-- name: LockOperator :one
-- LockOperator locks an operator and waits for it.
SELECT id, capacity, presence, dedicated_company_id FROM operators
WHERE id = @id
FOR UPDATE;

-- name: SetPresence :execrows
UPDATE operators
SET presence = @presence, presence_changed_at = @changed_at, updated_at = @changed_at
WHERE id = @id AND presence <> @presence;

-- name: NextOperatorEvents :many
-- NextOperatorEvents gives each operator of @ids the next operator event
-- sequence number, stores @event_at as the time of its last operator event,
-- and returns the full snapshot of each one in ID order (decision 0011). The
-- caller must already hold the row lock of every operator in @ids, so the
-- statement never waits and the languages it reads are the current ones.
WITH bumped AS (
    UPDATE operators SET op_seq = op_seq + 1, op_event_at = @event_at::timestamptz
    WHERE id = ANY (@ids::text[])
    RETURNING id, capacity, presence, dedicated_company_id, op_seq
)
SELECT
    b.id,
    b.capacity,
    b.presence,
    b.dedicated_company_id,
    b.op_seq,
    COALESCE((SELECT array_agg(ol.lang ORDER BY ol.lang) FROM operator_languages AS ol
              WHERE ol.operator_id = b.id), '{}')::text[] AS languages
FROM bumped AS b
ORDER BY b.id;

-- name: LockOperatorsDueForRefresh :many
-- LockOperatorsDueForRefresh locks up to @max_rows operators whose last
-- operator event is older than @before, or who have none, oldest first. A
-- row that another transaction holds is skipped, not waited for: it stays
-- due and the next call takes it.
SELECT id FROM operators
WHERE op_event_at IS NULL OR op_event_at < @before::timestamptz
ORDER BY op_event_at NULLS FIRST, id
LIMIT @max_rows
FOR UPDATE SKIP LOCKED;

-- name: OperatorExists :one
SELECT EXISTS (SELECT 1 FROM operators WHERE id = @id)::boolean AS found;

-- name: SLADue :many
-- SLADue lists conversations that may have missed an SLA target: the
-- target start is at or before the cutoff of that target.
SELECT company_id, customer_id FROM conversations
WHERE (state IN ('waiting', 'offered') AND first_accepted_at IS NULL AND NOT sla_assignment_missed
       AND sla_started_at <= @assignment_cutoff::timestamptz)
   OR (state IN ('waiting', 'offered', 'assigned') AND first_replied_at IS NULL AND NOT sla_response_missed
       AND sla_started_at <= @response_cutoff::timestamptz)
ORDER BY sla_started_at, company_id, customer_id
LIMIT @max_rows;

-- name: PendingConversations :many
-- PendingConversations locks pending conversations for the end of the
-- catch-up, in key order, and waits for them.
SELECT * FROM conversations
WHERE state = 'pending'
ORDER BY company_id, customer_id
LIMIT @max_rows
FOR UPDATE;

-- name: TeamLock :exec
-- TeamLock serializes the team operations of all routers for the rest of
-- the transaction. They are rare, and one at a time they cannot deadlock
-- on each other's operators.
SELECT pg_advisory_xact_lock(@key::bigint);

-- name: EnsureCompany :exec
INSERT INTO companies (company_id, updated_at)
VALUES (@company_id, @updated_at)
ON CONFLICT (company_id) DO NOTHING;

-- name: LockCompany :one
SELECT * FROM companies WHERE company_id = @company_id FOR UPDATE;

-- name: SaveCompany :exec
UPDATE companies SET
    team_seq = @team_seq,
    team_active = @team_active,
    team_formed_at = @team_formed_at,
    low_since = @low_since,
    updated_at = @updated_at
WHERE company_id = @company_id;

-- name: TeamCompanies :many
-- TeamCompanies lists the companies a team check looks at: those with an
-- active team, those whose active load reaches the form threshold, and
-- those with a waiting conversation in a team queue but no active team.
SELECT c.company_id FROM companies AS c WHERE c.team_active
UNION
SELECT cv.company_id FROM conversations AS cv
WHERE cv.state IN ('waiting', 'offered', 'assigned')
GROUP BY cv.company_id
HAVING count(*) >= @form_load::bigint
UNION
SELECT cv.company_id FROM conversations AS cv
WHERE cv.state = 'waiting' AND cv.team_queue
  AND NOT EXISTS (SELECT 1 FROM companies AS c WHERE c.company_id = cv.company_id AND c.team_active)
ORDER BY 1;

-- name: CompanyLangLoad :many
-- CompanyLangLoad counts the active load of a company by language, most
-- needed language first.
SELECT lang, count(*)::bigint AS n FROM conversations
WHERE company_id = @company_id AND state IN ('waiting', 'offered', 'assigned')
GROUP BY lang
ORDER BY count(*) DESC, lang;

-- name: TeamMembers :many
-- TeamMembers lists the members of a team with their languages and load.
SELECT
    o.id,
    COALESCE((SELECT array_agg(ol.lang ORDER BY ol.lang) FROM operator_languages AS ol
              WHERE ol.operator_id = o.id), '{}')::text[] AS languages,
    (SELECT count(*) FROM offers AS f
     WHERE f.operator_id = o.id AND f.state IN ('offered', 'accepted'))::integer AS live
FROM operators AS o
WHERE o.dedicated_company_id = @company_id::text
ORDER BY o.id;

-- name: PoolCandidates :many
-- PoolCandidates lists online operators of the shared pool that have one
-- of the languages, with their languages and load, lowest load first.
SELECT
    o.id,
    COALESCE((SELECT array_agg(ol.lang ORDER BY ol.lang) FROM operator_languages AS ol
              WHERE ol.operator_id = o.id), '{}')::text[] AS languages,
    (SELECT count(*) FROM offers AS f
     WHERE f.operator_id = o.id AND f.state IN ('offered', 'accepted'))::integer AS live
FROM operators AS o
WHERE o.presence = 'online'
  AND o.dedicated_company_id IS NULL
  AND EXISTS (SELECT 1 FROM operator_languages AS ol
              WHERE ol.operator_id = o.id AND ol.lang = ANY (@langs::text[]))
ORDER BY live, o.id;

-- name: JoinTeam :many
-- JoinTeam moves shared pool operators into the team of a company. An
-- operator that joined another team in the meantime is left out.
UPDATE operators SET dedicated_company_id = @company_id::text, updated_at = @updated_at
WHERE id = ANY (@ids::text[]) AND dedicated_company_id IS NULL
RETURNING id;

-- name: LeaveTeam :many
-- LeaveTeam moves members of the team of a company back to the shared
-- pool. They keep their offers and assignments (decision 0009).
UPDATE operators SET dedicated_company_id = NULL, updated_at = @updated_at
WHERE id = ANY (@ids::text[]) AND dedicated_company_id = @company_id::text
RETURNING id;

-- name: QueueMismatches :many
-- QueueMismatches locks the waiting conversations of a company whose
-- queue (team or shared) differs from the one the team rules give now.
SELECT cv.* FROM conversations AS cv
WHERE cv.company_id = @company_id
  AND cv.state = 'waiting'
  AND cv.team_queue <> (@team_active::boolean AND EXISTS (
      SELECT 1 FROM operators AS o
      JOIN operator_languages AS ol ON ol.operator_id = o.id AND ol.lang = cv.lang
      WHERE o.dedicated_company_id = cv.company_id
  ))
ORDER BY cv.customer_id
FOR UPDATE OF cv;

-- name: InsertOutbox :exec
-- InsertOutbox appends routing events in the order of the arrays.
INSERT INTO outbox (family, record_key, seq, event_type, payload, created_at)
SELECT e.family, e.record_key, e.seq, e.event_type, e.payload, @created_at
FROM ROWS FROM (
    unnest(@families::text[]),
    unnest(@record_keys::bytea[]),
    unnest(@seqs::bigint[]),
    unnest(@event_types::text[]),
    unnest(@payloads::bytea[])
) WITH ORDINALITY AS e (family, record_key, seq, event_type, payload, ord)
ORDER BY e.ord;

-- name: NextOutbox :many
-- NextOutbox returns the oldest unsent routing events in outbox order.
SELECT id, family, record_key, seq, event_type, payload, created_at FROM outbox
WHERE sent_at IS NULL
ORDER BY id
LIMIT @max_rows;

-- name: MarkOutboxSent :execrows
UPDATE outbox SET sent_at = @sent_at::timestamptz
WHERE id = ANY (@ids::bigint[]) AND sent_at IS NULL;

-- name: PruneOutbox :execrows
-- PruneOutbox deletes sent rows that were sent before a time.
DELETE FROM outbox
WHERE id IN (
    SELECT o.id FROM outbox AS o
    WHERE o.sent_at IS NOT NULL AND o.sent_at < @sent_before::timestamptz
    ORDER BY o.id
    LIMIT @max_rows
);

-- name: CountConversations :many
SELECT state, count(*)::bigint AS n FROM conversations GROUP BY state ORDER BY state;

-- name: QueueDepths :many
-- QueueDepths counts waiting conversations per queue, with the oldest
-- queue time.
SELECT
    lang,
    team_queue,
    (CASE WHEN team_queue THEN company_id ELSE '' END)::text AS team_company_id,
    count(*)::bigint AS n,
    min(queued_at)::timestamptz AS oldest
FROM conversations
WHERE state = 'waiting'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- name: CountOperators :many
SELECT
    presence,
    (dedicated_company_id IS NOT NULL)::boolean AS in_team,
    count(*)::bigint AS n,
    COALESCE(sum(capacity), 0)::bigint AS capacity
FROM operators
GROUP BY 1, 2
ORDER BY 1, 2;

-- name: CountLiveSlots :one
SELECT count(*)::bigint AS n FROM offers WHERE state IN ('offered', 'accepted');

-- name: CountUnsentOutbox :one
SELECT count(*)::bigint AS n FROM outbox WHERE sent_at IS NULL;

-- name: OldestUnsentOutbox :one
SELECT created_at FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 1;

-- name: CountActiveTeams :one
SELECT count(*)::bigint AS n FROM companies WHERE team_active;
