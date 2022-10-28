{{ config(
    materialized = 'view'
) }}

SELECT
    record_id,
    offset_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx_count,
    header,
    _inserted_timestamp
FROM
    {{ source(
        "chainwalkers",
        "harmony_blocks"
    ) }}
