{{ config(
    materialized = 'incremental',
    unique_key = 'log_id',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH swap_without_prices AS (

    SELECT
        se.block_timestamp,
        se.block_id AS block_number,
        se.tx_hash,
        se.pool_address,
        CASE
            WHEN se.amount0In <> 0
            AND se.amount1In <> 0 THEN amount1In / power(
                10,
                token1.decimals
            ) :: FLOAT
            WHEN se.amount0In <> 0 THEN se.amount0In / power(
                10,
                token0.decimals
            ) :: FLOAT
            WHEN se.amount1In <> 0 THEN se.amount1In / power(
                10,
                token1.decimals
            ) :: FLOAT
        END AS amount_in,
        CASE
            WHEN se.amount0Out <> 0 THEN se.amount0Out / power(
                10,
                token0.decimals
            ) :: FLOAT
            WHEN se.amount1Out <> 0 THEN se.amount1Out / power(
                10,
                token1.decimals
            ) :: FLOAT
        END AS amount_out,
        se.from_address AS sender,
        se.log_id,
        se.event_index,
        se.evm_origin_from_address,
        se.evm_origin_to_address,
        se.native_origin_from_address,
        se.native_origin_to_address,
        CASE
            WHEN se.amount0In <> 0
            AND se.amount1In <> 0 THEN token1_address
            WHEN se.amount0In <> 0 THEN token0_address
            WHEN se.amount1In <> 0 THEN token1_address
        END AS token_In,
        CASE
            WHEN se.amount0Out <> 0 THEN token0_address
            WHEN se.amount1Out <> 0 THEN token1_address
        END AS token_out,
        CASE
            WHEN se.amount0In <> 0
            AND se.amount1In <> 0 THEN token1_symbol
            WHEN se.amount0In <> 0 THEN token0_symbol
            WHEN se.amount1In <> 0 THEN token1_symbol
        END AS symbol_In,
        CASE
            WHEN se.amount0Out <> 0 THEN token0_symbol
            WHEN se.amount1Out <> 0 THEN token1_symbol
        END AS symbol_out,
        CASE
            WHEN amount0In <> 0
            AND amount1In <> 0 THEN token1.decimals
            WHEN amount0In <> 0 THEN token0.decimals
            WHEN amount1In <> 0 THEN token1.decimals
        END AS decimals_in,
        CASE
            WHEN amount0Out <> 0 THEN token0.decimals
            WHEN amount1Out <> 0 THEN token1.decimals
        END AS decimals_out,
        se.to_address :: STRING AS tx_to,
        se._inserted_timestamp
    FROM
        {{ ref('silver__swaps') }}
        se
        LEFT JOIN {{ ref('silver__tokens') }}
        token0
        ON se.token0_address = token0.token_address
        LEFT JOIN {{ ref('silver__tokens') }}
        token1
        ON se.token1_address = token1.token_address

{% if is_incremental() %}
WHERE
    block_timestamp >= (
        SELECT
            MAX(block_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
),
eth_prices AS (
    SELECT
        token_address,
        HOUR,
        symbol,
        AVG(price) AS price
    FROM
        {{ source(
            'ethereum_db_sushi',
            'FACT_HOURLY_TOKEN_PRICES'
        ) }}

{% if is_incremental() %}
WHERE
    HOUR :: DATE IN (
        SELECT
            DISTINCT block_timestamp :: DATE
        FROM
            swap_without_prices
    )
{% else %}
WHERE
    HOUR :: DATE >= '2020-05-05'
{% endif %}
GROUP BY
    token_address,
    HOUR,
    symbol
),
harmony_eth_crosstab AS (
    SELECT
        NAME,
        symbol,
        MAX (
            CASE
                WHEN platform_id = 'harmony-shard-0' THEN token_address
                ELSE ''
            END
        ) AS harmony_address,
        MAX (
            CASE
                WHEN platform = 'ethereum' THEN token_address
                ELSE ''
            END
        ) AS eth_address
    FROM
        {{ source(
            'symbols_cross_tab',
            'MARKET_ASSET_METADATA'
        ) }}
    GROUP BY
        NAME,
        symbol
    HAVING
        harmony_address <> ''
        AND eth_address <> ''
    ORDER BY
        NAME,
        symbol
),
harmony_prices AS (
    SELECT
        ep.token_address,
        ep.hour,
        ep.symbol,
        ep.price,
        hec.harmony_Address AS harmony_address
    FROM
        eth_prices ep
        LEFT JOIN harmony_eth_crosstab hec
        ON ep.token_address = hec.eth_Address
)
SELECT
    wp.block_timestamp,
    wp.block_number,
    wp.tx_hash,
    wp.pool_address,
    'sushiswap' AS platform,
    wp.event_index,
    wp.evm_origin_from_address,
    wp.evm_origin_to_address,
    wp.native_origin_from_address,
    wp.native_origin_to_address,
    wp.amount_in,
    wp.amount_out,
    wp.sender,
    wp.log_id,
    wp.token_In,
    wp.token_out,
    wp.symbol_In,
    wp.symbol_out,
    wp.tx_to,
    CASE
        WHEN wp.decimals_in IS NOT NULL
        AND wp.amount_in * pIn.price <= 5 * wp.amount_out * pOut.price
        AND wp.amount_out * pOut.price <= 5 * wp.amount_in * pIn.price THEN wp.amount_in * pIn.price
        WHEN wp.decimals_in IS NOT NULL
        AND wp.decimals_out IS NULL THEN wp.amount_in * pIn.price
        ELSE NULL
    END AS amount_in_usd,
    CASE
        WHEN wp.decimals_out IS NOT NULL
        AND wp.amount_in * pIn.price <= 5 * wp.amount_out * pOut.price
        AND wp.amount_out * pOut.price <= 5 * wp.amount_in * pIn.price THEN wp.amount_out * pOut.price
        WHEN wp.decimals_out IS NOT NULL
        AND wp.decimals_in IS NULL THEN wp.amount_out * pOut.price
        ELSE NULL
    END AS amount_out_usd,
    wp._inserted_timestamp
FROM
    swap_without_prices wp
    LEFT JOIN harmony_prices pIn
    ON LOWER(token_In) = LOWER(
        pIn.harmony_address
    )
    AND DATE_TRUNC(
        'hour',
        wp.block_timestamp
    ) = pIn.hour
    LEFT JOIN harmony_prices pOut
    ON LOWER(token_out) = LOWER(
        pOut.harmony_address
    )
    AND DATE_TRUNC(
        'hour',
        wp.block_timestamp
    ) = pOut.hour
WHERE
    pool_address IN (
        SELECT
            token_address
        FROM
            mdao_harmony.prod.tokens
        WHERE
            token_name = 'SushiSwap LP Token'
    )
