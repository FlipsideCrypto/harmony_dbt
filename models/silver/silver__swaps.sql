{{ config(
    materialized = 'incremental',
    unique_key = 'log_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH logs AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        event_index,
        evm_origin_from_address,
        evm_origin_to_address,
        native_origin_from_address,
        native_origin_to_address,
        native_contract_address,
        evm_contract_address,
        contract_name,
        event_name,
        event_inputs,
        topics,
        DATA,
        event_removed,
        _inserted_timestamp,
        log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        {{ incremental_load_filter("_inserted_timestamp") }}
)
SELECT
    l.block_timestamp,
    l.block_id,
    l.log_id,
    l.tx_hash,
    l.evm_contract_address AS pool_address,
    l.event_index,
    l.evm_origin_from_address,
    l.evm_origin_to_address,
    l.native_origin_from_address,
    l.native_origin_to_address,
    p.token0 AS token0_address,
    t0.token_name AS token0_name,
    t0.token_symbol AS token0_symbol,
    TRY_TO_NUMBER(
        l.event_inputs :amount0In :: STRING
    ) AS amount0In,
    TRY_TO_NUMBER(
        l.event_inputs :amount1In :: STRING
    ) AS amount1In,
    TRY_TO_NUMBER(
        l.event_inputs :amount0Out :: STRING
    ) AS amount0Out,
    TRY_TO_NUMBER(
        l.event_inputs :amount1Out :: STRING
    ) AS amount1Out,
    p.token1 AS token1_address,
    t1.token_name AS token1_name,
    t1.token_symbol AS token1_symbol,
    l.event_inputs :sender :: STRING AS from_address,
    l.event_inputs :to AS to_address,
    l._inserted_timestamp
FROM
    logs AS l
    JOIN {{ ref('silver__liquidity_pools') }} AS p
    ON p.pool_address = l.evm_contract_address
    LEFT JOIN {{ ref('silver__tokens') }} AS t0
    ON t0.token_address = p.token0
    LEFT JOIN {{ ref('silver__tokens') }} AS t1
    ON t1.token_address = p.token1
WHERE
    l.event_name = 'Swap'
