{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::date']
) }}

WITH base_tx AS (

    SELECT
        record_id,
        tx_id,
        tx_block_index,
        offset_id,
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx,
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
)
SELECT
    block_timestamp,
    tx :nonce :: STRING AS nonce,
    tx_block_index AS INDEX,
    tx :bech32_from :: STRING AS native_from_address,
    tx :bech32_to :: STRING AS native_to_address,
    tx :from :: STRING AS from_address,
    tx :to :: STRING AS to_address,
    tx :value AS VALUE,
    COALESCE(
        tx :block_number,
        js_hex_to_int(
            tx :blockNumber
        )
    ) AS block_number,
    COALESCE(
        tx :block_hash,
        tx :blockHash
    ) :: STRING AS block_hash,
    COALESCE(
        tx :gas_price,
        js_hex_to_int(
            tx :gasPrice
        )
    ) AS gas_price,
    tx :gas AS gas,
    tx_id AS tx_hash,
    tx :input :: STRING AS DATA,
    tx :receipt :status :: STRING = '0x1' AS status,
    _inserted_timestamp
FROM
    base_tx
