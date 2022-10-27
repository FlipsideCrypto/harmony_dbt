{{ config(
    materialized = 'incremental',
    unique_key = 'log_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH logs AS (

    SELECT
        log_id,
        block_id,
        block_timestamp,
        tx_hash,
        evm_contract_address,
        event_name,
        event_inputs,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        {{ incremental_load_filter("_inserted_timestamp") }}
        AND event_name = 'Transfer'
)
SELECT
    log_id,
    block_id,
    tx_hash,
    block_timestamp,
    evm_contract_address :: STRING AS contract_address,
    event_inputs :from :: STRING AS from_address,
    event_inputs :to :: STRING AS to_address,
    event_inputs :value :: FLOAT AS raw_amount,
    _inserted_timestamp
FROM
    logs
WHERE
    raw_amount IS NOT NULL
