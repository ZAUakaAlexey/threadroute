-- name: UpsertOperator :one
-- UpsertOperator creates the operator, or sets its capacity and its
-- languages, in one statement. A new operator is offline. The result says
-- what changed, so the operator API can answer "done" or "already done".
-- @languages must not be empty and must hold no duplicates. PostgreSQL
-- runs every data-modifying CTE, even one the final SELECT does not read.
WITH prev AS (
    -- Always one row: n is 0 for a new operator.
    SELECT count(*) AS n, COALESCE(max(o.capacity), 0)::integer AS capacity
    FROM operators AS o
    WHERE o.id = @id
), op AS (
    INSERT INTO operators (id, capacity)
    VALUES (@id, @capacity)
    ON CONFLICT (id) DO UPDATE
        SET capacity = EXCLUDED.capacity, updated_at = now()
    RETURNING operators.id
), removed AS (
    DELETE FROM operator_languages AS ol
    WHERE ol.operator_id = @id AND ol.lang <> ALL (@languages::text[])
    RETURNING ol.lang
), added AS (
    INSERT INTO operator_languages (operator_id, lang)
    SELECT @id, l.lang FROM unnest(@languages::text[]) AS l (lang)
    ON CONFLICT DO NOTHING
    RETURNING operator_languages.lang
), removed_count AS (
    SELECT count(*) AS n FROM removed
), added_count AS (
    SELECT count(*) AS n FROM added
)
SELECT
    prev.n = 0 AS created,
    (prev.n > 0 AND prev.capacity <> @capacity::integer)::boolean AS capacity_changed,
    removed_count.n AS languages_removed,
    added_count.n AS languages_added
FROM prev, removed_count, added_count;

-- name: SetOperatorPresence :execrows
-- SetOperatorPresence sets the presence of an operator. It changes no row,
-- and returns 0, when the operator does not exist or already has this
-- presence. Use GetOperator to tell the two apart.
UPDATE operators
SET presence = @presence, presence_changed_at = now(), updated_at = now()
WHERE id = @id AND presence <> @presence;

-- name: GetOperator :one
-- GetOperator returns an operator with its languages, sorted.
SELECT
    o.id,
    o.capacity,
    o.presence,
    o.dedicated_company_id,
    o.created_at,
    o.updated_at,
    o.presence_changed_at,
    COALESCE(
        array_agg(ol.lang ORDER BY ol.lang) FILTER (WHERE ol.lang IS NOT NULL),
        '{}'
    )::text[] AS languages
FROM operators AS o
LEFT JOIN operator_languages AS ol ON ol.operator_id = o.id
WHERE o.id = @id
GROUP BY o.id;
