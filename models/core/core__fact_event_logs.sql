{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['log_id']
    ) }} AS fact_event_logs_id,
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
    log_id
FROM
    {{ ref("silver__logs") }}
