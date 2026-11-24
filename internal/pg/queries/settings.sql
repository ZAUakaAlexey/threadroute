-- name: EnsureStandSettings :exec
-- EnsureStandSettings inserts the row of the stand settings with version 0
-- when it is missing. A write transaction runs it before
-- LockStandSettings.
INSERT INTO stand_settings (id, version, updated_at)
VALUES (true, 0, @updated_at)
ON CONFLICT (id) DO NOTHING;

-- name: LockStandSettings :one
-- LockStandSettings reads the row of the stand settings and locks it until
-- the end of the transaction.
SELECT * FROM stand_settings WHERE id FOR UPDATE;

-- name: GetStandSettings :one
-- GetStandSettings reads the row of the stand settings without a lock. No
-- row is the same as version 0.
SELECT * FROM stand_settings WHERE id;

-- name: SaveStandSettings :exec
-- SaveStandSettings writes the locked row of the stand settings.
UPDATE stand_settings
SET version = @version, current_set = @current_set, defaults_sim = @defaults_sim,
    last_put_at = @last_put_at, updated_at = @updated_at
WHERE id;

-- name: InsertStandSettingsChange :exec
-- InsertStandSettingsChange adds one entry to the change log.
INSERT INTO stand_settings_changes (version, changed_at, changed_by, fields)
VALUES (@version, @changed_at, @changed_by, @fields);

-- name: RecentStandSettingsChanges :many
-- RecentStandSettingsChanges returns the newest entries of the change log
-- up to a version, newest first. The bound keeps the answer consistent
-- with a row read in an earlier statement: a change committed after that
-- read has a higher version and is left out.
SELECT * FROM stand_settings_changes
WHERE version <= @max_version
ORDER BY version DESC
LIMIT @max_rows;
