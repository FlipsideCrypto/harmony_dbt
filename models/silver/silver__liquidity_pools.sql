{{ config(
    materialized = 'table',
    unique_key = 'pool_address'
) }}

WITH src_logs_lp AS (

    SELECT
        event_inputs :pair :: STRING AS pool_address,
        '' AS pool_name,
        event_inputs :token0 :: STRING AS token0,
        event_inputs :token1 :: STRING AS token1
    FROM
        {{ ref('silver__logs') }}
    WHERE
        event_name = 'PairCreated'
),
logs_lp AS (
    SELECT
        pool_address,
        t0.token_symbol || '-' || t1.token_symbol || ' LP' AS pool_name,
        token0,
        token1
    FROM
        src_logs_lp p
        INNER JOIN {{ ref('silver__tokens') }} AS t0
        ON p.token0 = t0.token_address
        INNER JOIN {{ ref('silver__tokens') }} AS t1
        ON p.token1 = t1.token_address
),
backfill_from_swaps AS (
    SELECT
        pool_address,
        pool_name,
        token0,
        token1
    FROM
        {{ ref('silver__backfill_pools_data') }}
    WHERE
        pool_address NOT IN (
            SELECT
                pool_address
            FROM
                logs_lp
        )
)
SELECT
    pool_address,
    pool_name,
    token0,
    token1
FROM
    logs_lp
UNION
SELECT
    pool_address,
    pool_name,
    token0,
    token1
FROM
    backfill_from_swaps
