-- A trim of the teams after the share of operators in teams was lowered
-- (decision 0009, "A trim after the cap was lowered"), and the end of the
-- old column defaults_sim (decision 0014, "Change on 2026-11-25: the
-- settings live only in the store").
--
-- # The trim request
--
-- A change of the stand settings that lowers teams.max_share stores the
-- request in the same transaction as the change. The team check of a
-- router runs the trim and clears the request in one transaction, so a
-- restart between the change and the check does not lose it, and two
-- routers do not trim twice. Nothing else sets the request: not the seed
-- of the defaults, not a change that raises or keeps the share.
--
-- # Locking
--
-- 00005_stand_settings.sql says that a change of the settings takes no
-- other lock. That still holds for a change and for the seed: they lock
-- the one settings row and nothing else. The trim is the one transaction
-- that holds the settings row together with other locks. Its order is: the
-- team advisory lock, then the settings row, then companies, then
-- operators (the order of 00002_routing.sql with the settings row after
-- the team lock). A change or the seed never waits for the team lock or
-- for any row while it holds the settings row, so no cycle is possible.
-- The trim takes no conversation row: the normal team check that follows
-- it moves the waiting conversations of a released team.

-- +goose Up
-- The version of the change that asked for a trim, or NULL when no trim
-- is pending. A later change that lowers the share again writes its own
-- version; the trim clears it to NULL.
ALTER TABLE stand_settings ADD COLUMN trim_version bigint;
ALTER TABLE stand_settings ADD CONSTRAINT stand_settings_trim_version_stored
    CHECK (trim_version IS NULL OR (trim_version > 0 AND trim_version <= version));

-- The values of the old simulator report. No router reads them since the
-- settings live only in the store.
ALTER TABLE stand_settings DROP CONSTRAINT stand_settings_defaults_sim_object;
ALTER TABLE stand_settings DROP COLUMN defaults_sim;

-- +goose Down
-- The old column comes back empty: its values are gone.
ALTER TABLE stand_settings ADD COLUMN defaults_sim jsonb;
ALTER TABLE stand_settings ADD CONSTRAINT stand_settings_defaults_sim_object
    CHECK (defaults_sim IS NULL OR jsonb_typeof(defaults_sim) = 'object');

ALTER TABLE stand_settings DROP CONSTRAINT stand_settings_trim_version_stored;
ALTER TABLE stand_settings DROP COLUMN trim_version;
