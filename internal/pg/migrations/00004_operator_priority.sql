-- The priority of an operator (decision 0009, "Changes after the first live
-- run of the console", "An operator priority"). Among the operators that
-- pass every rule and have the same lowest load, the one with the higher
-- priority gets the offer. The caller of the operator API sets it with the
-- languages and the capacity; the gateway gives the operators of people 1.
-- The range is the one of opapi.MaxPriority.
--
-- The rule for a language that no online operator of the shared pool has
-- (same section, "A language that no online operator has") needs no
-- column: the router checks it in the queries at every pick.

-- +goose Up
ALTER TABLE operators
    ADD COLUMN priority integer NOT NULL DEFAULT 0
        CONSTRAINT operators_priority_range CHECK (priority BETWEEN 0 AND 9);

-- +goose Down
ALTER TABLE operators DROP COLUMN priority;
