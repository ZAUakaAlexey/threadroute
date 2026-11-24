-- Routing state of the router (decisions 0009 and 0010): conversations,
-- offers and assignments, skipped operators, replies, dedicated teams and
-- the outbox of routing events. IDs, times and counters only, no post text
-- (decision 0004).
--
-- # Locking
--
-- The router never uses the database clock for a routing decision. Every
-- time column is written from the clock of the router (machine time).
--
-- Lock order of every operation that waits for a lock: the team advisory
-- lock, then companies, then operators, then conversations (by key), then
-- offers. Assignment and the timer sweeps take conversation rows and
-- operator rows with SKIP LOCKED, so they never wait for one. A row in
-- offers, offer_skips or replies is only changed while its conversation
-- row is locked. No table that is written under a conversation lock has a
-- foreign key to operators, except offers, which only the holder of the
-- operator lock inserts. So nothing waits for an operator row while it
-- holds a conversation row.
--
-- The load of an operator is the number of its live offers (state offered
-- or accepted). It is counted, never stored, so it cannot drift.

-- +goose Up

-- Companies that have, or had, a dedicated team. The row outlives a
-- release, because team_seq counts the team events of the company over
-- every team it had (decision 0009, "Order of routing events").
CREATE TABLE companies (
    company_id text PRIMARY KEY
        CONSTRAINT companies_id_not_empty CHECK (company_id <> ''),
    -- Sequence number of the last team event of this company.
    team_seq bigint NOT NULL DEFAULT 0
        CONSTRAINT companies_team_seq_not_negative CHECK (team_seq >= 0),
    team_active boolean NOT NULL DEFAULT false,
    team_formed_at timestamptz,
    -- While the team is active: since when the active load has been below
    -- the release threshold, or NULL when it is not below.
    low_since timestamptz,
    updated_at timestamptz NOT NULL
);

-- One row per conversation key: the current session as the router sees it.
CREATE TABLE conversations (
    company_id text NOT NULL
        CONSTRAINT conversations_company_not_empty CHECK (company_id <> ''),
    customer_id text NOT NULL
        CONSTRAINT conversations_customer_not_empty CHECK (customer_id <> ''),
    -- Session and sequence number of the last applied conv event.
    conv_session bigint NOT NULL
        CONSTRAINT conversations_session_positive CHECK (conv_session > 0),
    conv_seq bigint NOT NULL
        CONSTRAINT conversations_seq_positive CHECK (conv_seq > 0),
    -- Language of the session, the queue language.
    lang text NOT NULL
        CONSTRAINT conversations_lang_format CHECK (lang ~ '^[a-z]{2,3}$'),
    -- pending: open, applied during the catch-up, not queued yet.
    -- waiting: in a queue, without an offer.
    -- offered: an offer is out (one live row in offers).
    -- assigned: an operator accepted it (one live row in offers).
    -- resolved: the operator resolved it, the session is still open.
    -- closed: conv closed the session.
    state text NOT NULL
        CONSTRAINT conversations_state_known CHECK
            (state IN ('pending', 'waiting', 'offered', 'assigned', 'resolved', 'closed')),
    -- The queue is the team queue of the company (true) or the shared
    -- queue of lang (false).
    team_queue boolean NOT NULL DEFAULT false,
    -- Higher is served first. Raised by one for each missed SLA target.
    priority integer NOT NULL DEFAULT 0
        CONSTRAINT conversations_priority_not_negative CHECK (priority >= 0),
    -- When the conversation entered its queue. Kept when an offer or an
    -- assignment returns it to the queue.
    queued_at timestamptz,
    -- The operator to offer it to first: the previous operator after a
    -- reopen or a customer message after resolve.
    preferred_operator_id text,
    -- The operator who last accepted the conversation, in any session.
    last_operator_id text,
    -- Sequence number of the last routing event of this key. It counts
    -- over every session and is never reset.
    route_seq bigint NOT NULL DEFAULT 0
        CONSTRAINT conversations_route_seq_not_negative CHECK (route_seq >= 0),
    -- SLA of the session: both targets start when the session first
    -- enters a queue (decision 0009, "Where the timers start").
    sla_started_at timestamptz,
    first_accepted_at timestamptz,
    first_replied_at timestamptz,
    sla_assignment_missed boolean NOT NULL DEFAULT false,
    sla_response_missed boolean NOT NULL DEFAULT false,
    -- Simulated time to first response and the real response time of the
    -- thread author (decision 0009, "SLA"), in microseconds of model time
    -- and of source time.
    sim_response_us bigint,
    real_response_us bigint,
    close_reason text,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (company_id, customer_id),
    CONSTRAINT conversations_customer_not_company CHECK (company_id <> customer_id),
    CONSTRAINT conversations_queued_when_routed CHECK
        (state NOT IN ('waiting', 'offered', 'assigned') OR queued_at IS NOT NULL)
);

-- Queue order: priority, then waiting time (decision 0009, "Queues").
CREATE INDEX conversations_queue_idx
    ON conversations (priority DESC, queued_at, company_id, customer_id)
    WHERE state = 'waiting';

-- SLA timers.
CREATE INDEX conversations_sla_assignment_idx
    ON conversations (sla_started_at)
    WHERE state IN ('waiting', 'offered') AND first_accepted_at IS NULL AND NOT sla_assignment_missed;
CREATE INDEX conversations_sla_response_idx
    ON conversations (sla_started_at)
    WHERE state IN ('waiting', 'offered', 'assigned') AND first_replied_at IS NULL AND NOT sla_response_missed;

-- Active load of a company, by language (decision 0009, "Which
-- conversations count for a team").
CREATE INDEX conversations_active_idx
    ON conversations (company_id, lang)
    WHERE state IN ('waiting', 'offered', 'assigned');

CREATE INDEX conversations_pending_idx
    ON conversations (company_id, customer_id)
    WHERE state = 'pending';

-- Offers and assignments. An offer that is accepted becomes the assignment
-- and keeps its ID (decision 0010, "Offer identity").
CREATE SEQUENCE offer_num_seq;

CREATE TABLE offers (
    num bigint PRIMARY KEY DEFAULT nextval('offer_num_seq'),
    -- The offer ID of the operator API and the routing events.
    id text GENERATED ALWAYS AS ('of-' || num::text) STORED NOT NULL
        CONSTRAINT offers_id_unique UNIQUE,
    company_id text NOT NULL,
    customer_id text NOT NULL,
    session bigint NOT NULL,
    operator_id text NOT NULL REFERENCES operators (id),
    -- offered and accepted are live: they use a slot of the operator.
    -- expired, refused and ended (closed or operator offline) end an offer
    -- that was not accepted. resolved and assignment_ended (closed or
    -- operator offline) end an assignment.
    state text NOT NULL
        CONSTRAINT offers_state_known CHECK
            (state IN ('offered', 'accepted', 'expired', 'refused', 'ended', 'resolved', 'assignment_ended')),
    -- The capacity of the operator when the slot was reserved. A later cut
    -- of the capacity does not end slots that were legal before it.
    capacity integer NOT NULL,
    queued_at timestamptz NOT NULL,
    offered_at timestamptz NOT NULL,
    accept_by timestamptz NOT NULL,
    accepted_at timestamptz,
    ended_at timestamptz,
    -- timeout, refused, operator_offline or closed.
    end_reason text,
    FOREIGN KEY (company_id, customer_id) REFERENCES conversations (company_id, customer_id)
);

-- At most one live offer or assignment per conversation.
CREATE UNIQUE INDEX offers_one_live_idx
    ON offers (company_id, customer_id)
    WHERE state IN ('offered', 'accepted');
-- Load of an operator.
CREATE INDEX offers_operator_live_idx
    ON offers (operator_id)
    WHERE state IN ('offered', 'accepted');
-- Accept timeout.
CREATE INDEX offers_accept_by_idx
    ON offers (accept_by)
    WHERE state = 'offered';

-- Operators skipped for one session of a conversation after a timeout or a
-- refusal (decision 0009, "Assignment", step 4). No foreign key to
-- operators, see the lock order above.
CREATE TABLE offer_skips (
    company_id text NOT NULL,
    customer_id text NOT NULL,
    session bigint NOT NULL,
    operator_id text NOT NULL,
    -- The offer that caused the skip.
    offer_num bigint NOT NULL,
    PRIMARY KEY (company_id, customer_id, session, operator_id),
    FOREIGN KEY (company_id, customer_id) REFERENCES conversations (company_id, customer_id)
);

-- Reply markers (decision 0010, "Replies"). The reply ID is chosen by the
-- caller, so a repeated call records nothing new.
CREATE TABLE replies (
    offer_num bigint NOT NULL REFERENCES offers (num),
    reply_id text NOT NULL
        CONSTRAINT replies_id_format CHECK (reply_id ~ '^[A-Za-z0-9._-]{1,64}$'),
    replied_at timestamptz NOT NULL,
    PRIMARY KEY (offer_num, reply_id)
);

-- Outbox of routing events (decision 0009, "Events and state"). A row is
-- written in the transaction of the state change. One publisher sends the
-- unsent rows in id order. Two rows of one key are always written by
-- transactions that hold the same row lock (the conversation or the
-- company), so their id order is their commit order.
CREATE TABLE outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- conversation or team: the two kinds of record key.
    family text NOT NULL
        CONSTRAINT outbox_family_known CHECK (family IN ('conversation', 'team')),
    record_key bytea NOT NULL,
    seq bigint NOT NULL
        CONSTRAINT outbox_seq_positive CHECK (seq > 0),
    event_type text NOT NULL,
    -- The JSON of event.MarshalRouting.
    payload bytea NOT NULL,
    created_at timestamptz NOT NULL,
    sent_at timestamptz,
    CONSTRAINT outbox_key_seq_unique UNIQUE (family, record_key, seq)
);

CREATE INDEX outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;
CREATE INDEX outbox_sent_idx ON outbox (sent_at) WHERE sent_at IS NOT NULL;

-- +goose Down
DROP TABLE outbox;
DROP TABLE replies;
DROP TABLE offer_skips;
DROP TABLE offers;
DROP SEQUENCE offer_num_seq;
DROP TABLE conversations;
DROP TABLE companies;
