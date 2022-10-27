{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['log_id']
    ) }} AS fact_swaps_id,
    block_timestamp,
    block_id,
    log_id,
    tx_hash,
    pool_address,
    event_index,
    evm_origin_from_address,
    evm_origin_to_address,
    native_origin_from_address,
    native_origin_to_address,
    token0_address,
    token0_name,
    token0_symbol,
    amount0In,
    amount1In,
    amount0Out,
    amount1Out,
    token1_address,
    token1_name,
    token1_symbol,
    from_address,
    to_address,
    _inserted_timestamp
FROM
    {{ ref('silver__swaps') }}
