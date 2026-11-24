-- Operator events and the previous offer of a requeue (decision 0011,
-- "Contract changes" and "Details fixed before implementation").
--
-- # Operator events
--
-- The router publishes a full snapshot of an operator to the routing topic
-- in the transaction of every change of it, keyed by the operator ID, with
-- a sequence number per operator. The counter lives on the operator row.
-- A transaction gives the next number only while it holds the operator row
-- lock, so the numbers of one operator have no gap and no repeat, and their
-- outbox order is their commit order.
--
-- Lock order (it extends the one in 00002_routing.sql): the team advisory
-- lock, then companies, then operators, then conversations (by key), then
-- offers. An operator event takes its number at the "operators" step: the
-- writer already holds the operator row lock (PutOperator, Online and
-- Offline lock one operator row; a team change locks its members after the
-- company row and under the team lock). The refresh of the snapshots takes
-- operator rows with SKIP LOCKED, so it never waits for a row lock.
--
-- # Previous offer
--
-- A "queued" event after a customer message that follows a resolve names
-- the resolved assignment. The conversation row keeps the offer ID of the
-- last accepted offer next to last_operator_id.

-- +goose Up
ALTER TABLE outbox DROP CONSTRAINT outbox_family_known;
ALTER TABLE outbox ADD CONSTRAINT outbox_family_known
    CHECK (family IN ('conversation', 'team', 'operator'));

ALTER TABLE operators
    -- Sequence number of the last operator event of this operator.
    ADD COLUMN op_seq bigint NOT NULL DEFAULT 0
        CONSTRAINT operators_op_seq_not_negative CHECK (op_seq >= 0),
    -- Router machine time of the last operator event, or NULL when the
    -- operator has none yet. The refresh publishes the snapshot again when
    -- this time is older than the refresh interval.
    ADD COLUMN op_event_at timestamptz;

-- Finds the operators whose snapshot is due for a refresh. NULL (never
-- published) comes first.
CREATE INDEX operators_refresh_idx ON operators (op_event_at NULLS FIRST, id);

ALTER TABLE conversations
    -- The offer ID of the last accepted offer, in any session. It is set
    -- together with last_operator_id.
    ADD COLUMN last_offer_id text;

-- Rows written before this migration: the last accepted offer of each
-- conversation that has a last operator.
UPDATE conversations AS cv
SET last_offer_id = x.id
FROM (
    SELECT DISTINCT ON (company_id, customer_id) company_id, customer_id, id
    FROM offers
    WHERE accepted_at IS NOT NULL
    ORDER BY company_id, customer_id, num DESC
) AS x
WHERE cv.company_id = x.company_id AND cv.customer_id = x.customer_id
  AND cv.last_operator_id IS NOT NULL;

-- +goose Down
ALTER TABLE conversations DROP COLUMN last_offer_id;
DROP INDEX operators_refresh_idx;
ALTER TABLE operators DROP COLUMN op_event_at, DROP COLUMN op_seq;
DELETE FROM outbox WHERE family = 'operator';
ALTER TABLE outbox DROP CONSTRAINT outbox_family_known;
ALTER TABLE outbox ADD CONSTRAINT outbox_family_known
    CHECK (family IN ('conversation', 'team'));
