{{ config(
    materialized = 'table',
    unique_key = 'token_address'
) }}

WITH dfk_tokens AS (

    SELECT
        token_address,
        token_name,
        token_symbol,
        decimals
    FROM
        {{ ref('silver__dfk_tokens') }}
),
harmony_explorer_tokens AS (
    SELECT
        token_address,
        token_name,
        token_symbol,
        decimals
    FROM
        {{ ref('silver__harmony_explorer_tokens') }}
),
backfill_tokens AS (
    SELECT
        token_address,
        token_name,
        token_symbol,
        decimals
    FROM
        {{ ref('silver__backfill_tokens_data') }}
    WHERE
        token_address NOT IN (
            SELECT
                token_address
            FROM
                harmony_explorer_tokens
        )
)
SELECT
    token_address,
    token_name,
    token_symbol,
    decimals
FROM
    dfk_tokens
WHERE
    token_address NOT IN (
        SELECT
            token_address
        FROM
            harmony_explorer_tokens
    )
UNION
SELECT
    token_address,
    token_name,
    token_symbol,
    decimals
FROM
    harmony_explorer_tokens
UNION
SELECT
    token_address,
    token_name,
    token_symbol,
    decimals
FROM
    backfill_tokens
