{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::date']
) }}

WITH base_blocks AS (

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
        {{ ref("bronze__blocks") }}
    WHERE
        {{ incremental_load_filter("_inserted_timestamp") }}
        qualify ROW_NUMBER() over (
            PARTITION BY block_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
)
SELECT
    block_id,
    block_timestamp,
    header :hash :: STRING AS block_hash,
    COALESCE(
        header :parent_hash,
        header :parentHash
    ) :: STRING AS block_parent_hash,
    COALESCE(
        header :gas_limit,
        js_hex_to_int(
            header :gasLimit
        )
    ) AS gas_limit,
    COALESCE(
        header :gas_used,
        js_hex_to_int(
            header :gasUsed
        )
    ) AS gas_used,
    header :miner :: STRING AS miner,
    header :nonce :: STRING AS nonce,
    CASE
        WHEN header :size LIKE '0x%' THEN js_hex_to_int(
            header :size
        )
        ELSE header :size
    END AS SIZE,
    tx_count,
    COALESCE(
        header :state_root,
        header :stateRoot
    ) :: STRING AS state_root,
    COALESCE(
        header :receipts_root,
        header :receiptsRoot
    ) :: STRING AS receipts_root,
    _inserted_timestamp
FROM
    base_blocks
