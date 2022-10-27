{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['pool_address']
    ) }} AS dim_liquidity_pool_id,
    pool_address,
    pool_name,
    token0,
    token1
FROM
    {{ ref("silver__liquidity_pools") }}
