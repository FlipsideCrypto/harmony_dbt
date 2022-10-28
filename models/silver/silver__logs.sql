{{ config(
    materialized = 'incremental',
    unique_key = 'log_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH logs_raw AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id AS tx_hash,
        tx :bech32_from :: STRING AS native_origin_from_address,
        tx :bech32_to :: STRING AS native_origin_to_address,
        tx :from :: STRING AS evm_origin_from_address,
        tx :to :: STRING AS evm_origin_to_address,
        tx :receipt :logs AS full_logs,
        _inserted_timestamp
    FROM
        {{ ref("bronze__transactions") }}
    WHERE
        {{ incremental_load_filter("_inserted_timestamp") }}
        qualify ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
logs AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        TO_NUMBER(
            RIGHT(
                VALUE :logIndex :: STRING,
                LENGTH(
                    VALUE :logIndex :: STRING
                ) -2
            ),
            'xxxxx'
        ) AS event_index,
        evm_origin_from_address,
        evm_origin_to_address,
        native_origin_from_address,
        native_origin_to_address,
        VALUE :bech32_address :: STRING AS native_contract_address,
        VALUE :address :: STRING AS evm_contract_address,
        VALUE :decoded :contractName :: STRING AS contract_name,
        VALUE :decoded :eventName :: STRING AS event_name,
        VALUE :decoded :inputs AS event_inputs,
        VALUE :topics AS topics,
        VALUE :data :: STRING AS DATA,
        VALUE :removed AS event_removed,
        _inserted_timestamp
    FROM
        logs_raw,
        LATERAL FLATTEN (
            input => full_logs
        )
)
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
    concat_ws(
        '-',
        tx_hash,
        event_index
    ) AS log_id
FROM
    logs
