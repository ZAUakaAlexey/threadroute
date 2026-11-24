-- Stand settings changed from the console (decision 0014, point 1B): one
-- row with the current set and its version, and the change log.
--
-- # The current set
--
-- Version 0 means that nothing is stored: every router then runs with the
-- team values of its own environment, and the sim part is absent. The
-- router writes the row only when a change is applied (a change from the
-- console, or the first report of a simulator). A write transaction first
-- inserts the row with version 0 when it is missing and then locks it, so
-- a test that empties the table cannot break it.
--
-- The sets are JSON objects in the form of opapi.StandSettings. Shares are
-- JSON numbers: Go writes the shortest decimal that reads back as the same
-- double, and jsonb keeps that decimal exactly.
--
-- # Locking
--
-- A change locks the one row with FOR UPDATE and takes no other lock, so
-- it is outside the lock order of 00002_routing.sql. The version check and
-- the "one change per 10 seconds" rule are checked on the locked row, so
-- they hold for several routers. Readers do not lock.
--
-- Every time column is written from the clock of the router (machine
-- time), never from the database clock.

-- +goose Up
CREATE TABLE stand_settings (
    -- Always true: the table has at most one row.
    id boolean PRIMARY KEY DEFAULT true
        CONSTRAINT stand_settings_one_row CHECK (id),
    -- Version of the current set. Each applied change adds 1.
    version bigint NOT NULL
        CONSTRAINT stand_settings_version_not_negative CHECK (version >= 0),
    -- The current set, with the sim part once a simulator reported. NULL at
    -- version 0.
    current_set jsonb,
    -- The values of the last simulator report (the defaults of the sim
    -- part), or NULL before the first report. The defaults of the teams
    -- part are the environment of each router and are not stored.
    defaults_sim jsonb,
    -- Time of the last change applied by PUT /v1/settings, or NULL. A
    -- simulator report does not set it.
    last_put_at timestamptz,
    updated_at timestamptz NOT NULL,
    CONSTRAINT stand_settings_current_at_version
        CHECK ((version = 0) = (current_set IS NULL)),
    CONSTRAINT stand_settings_current_object
        CHECK (current_set IS NULL OR jsonb_typeof(current_set) = 'object'),
    CONSTRAINT stand_settings_defaults_sim_object
        CHECK (defaults_sim IS NULL OR jsonb_typeof(defaults_sim) = 'object')
);

-- The change log. Every applied change adds one row, and rows are never
-- deleted by the router: the answers show the newest ones, the table keeps
-- every change so a measurement can be cut into segments.
CREATE TABLE stand_settings_changes (
    -- Version of the set after the change.
    version bigint PRIMARY KEY
        CONSTRAINT stand_settings_changes_version_positive CHECK (version > 0),
    changed_at timestamptz NOT NULL,
    -- Who made the change: "simulator", or the label the gateway sent
    -- (opapi.CheckBy). It may hold a user ID of the site.
    changed_by text NOT NULL
        CONSTRAINT stand_settings_changes_by_length
            CHECK (octet_length(changed_by) BETWEEN 1 AND 64),
    -- The changed values as a JSON array of opapi.FieldChange.
    fields jsonb NOT NULL
        CONSTRAINT stand_settings_changes_fields_not_empty
            CHECK (jsonb_typeof(fields) = 'array' AND jsonb_array_length(fields) > 0)
);

-- +goose Down
DROP TABLE stand_settings_changes;
DROP TABLE stand_settings;
