{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['block_id']
    ) }} AS dim_block_id,
    block_id,
    block_timestamp,
    block_hash,
    block_parent_hash,
    gas_limit,
    gas_used,
    miner,
    nonce,
    SIZE,
    tx_count,
    state_root,
    receipts_root,
    _inserted_timestamp
FROM
    {{ ref("silver__blocks") }}
