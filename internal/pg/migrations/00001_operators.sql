-- Operators and their languages (decisions 0009 and 0010). Operators are
-- simulated, so the IDs are synthetic. The rule of the id column is the
-- same as event.ValidRoutingID. No post text is stored here (decision 0004).
--
-- The load of an operator (how many slots are in use) is not stored here.
-- It follows from the offer and assignment tables of later migrations.

-- +goose Up
CREATE TABLE operators (
    id text PRIMARY KEY
        CONSTRAINT operators_id_format CHECK (id ~ '^[A-Za-z0-9._-]{1,64}$'),
    -- How many conversations the operator handles at once.
    capacity integer NOT NULL
        CONSTRAINT operators_capacity_positive CHECK (capacity > 0),
    -- online or offline. A new operator is offline.
    presence text NOT NULL DEFAULT 'offline'
        CONSTRAINT operators_presence_known CHECK (presence IN ('online', 'offline')),
    -- The company of the dedicated team the operator is in, or NULL in the
    -- shared pool. At most one company (decision 0009).
    dedicated_company_id text
        CONSTRAINT operators_dedicated_company_not_empty CHECK (dedicated_company_id <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    -- The last upsert (also one that changed no value) or change of presence.
    updated_at timestamptz NOT NULL DEFAULT now(),
    -- The last change of presence.
    presence_changed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE operator_languages (
    operator_id text NOT NULL REFERENCES operators (id) ON DELETE CASCADE,
    -- A normalised language (event.NormalizeLang). 'und' is the general
    -- queue.
    lang text NOT NULL
        CONSTRAINT operator_languages_lang_format CHECK (lang ~ '^[a-z]{2,3}$'),
    PRIMARY KEY (operator_id, lang)
);

-- Finds the operators of a language.
CREATE INDEX operator_languages_lang_idx ON operator_languages (lang, operator_id);

-- +goose Down
DROP TABLE operator_languages;
DROP TABLE operators;
