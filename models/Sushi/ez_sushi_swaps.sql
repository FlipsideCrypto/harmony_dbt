{{ config(
  materialized = 'incremental',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = 'log_id',
  cluster_by = ['block_timestamp::DATE'],
) }}


with 
swap_without_prices as 
(    
select 
se.block_timestamp, 
se.block_id as block_number,
se.tx_hash, 
se.pool_address,
CASE  wHEN se.amount0In <> 0 and se.amount1In <> 0 And token1.decimals is not null 
      THEN amount1In / power(10, token1.decimals ) :: FLOAT
      WHEN se.amount0In <> 0  And token0.decimals is not null 
      THEN se.amount0In / power(10, token0.decimals)::float
      WHEN se.amount1In <> 0 And token1.decimals is not null
      THEN se.amount1In/ power(10, token1.decimals)::float
      when se.amount0In<> 0 and token0.decimals is null 
      then se.amount0In
      when se.amount1In <> 0 and token1.decimals is null
      then se.amount1In
      END AS amount_in,
CASE 
      WHEN se.amount0Out <> 0 and token0.decimals is not null 
      THEN se.amount0Out/ power(10, token0.decimals)::float
      WHEN se.amount1Out <> 0 and token1.decimals is not null 
      THEN se.amount1Out/ power(10, token1.decimals)::float
      when se.amount0Out <> 0 and token0.decimals is null 
      then se.amount0Out
      when se.amount1Out <> 0 and token1.decimals is null
      then se.amount1Out
      END as amount_out,
se.from_address as sender,
se.LOG_ID,
se.event_index,
se.evm_origin_from_address,
se.evm_origin_to_address,
se.native_origin_from_address,
se.native_origin_to_address,
CASE 
    WHEN se.amount0In <> 0 AND se.amount1In <> 0 THEN token1_address
    WHEN se.amount0In <> 0 THEN token0_address
    WHEN se.amount1In <> 0 THEN token1_address
    END AS token_In,
CASE 
    WHEN se.amount0Out <> 0 THEN token0_address
    WHEN se.amount1Out <> 0 THEN TOKEN1_ADDRESS
    END AS token_out,
CASE 
    WHEN se.amount0In <> 0 AND se.amount1In <> 0 THEN token1_symbol
    WHEN se.amount0In <> 0 THEN TOKEN0_SYMBOL
    WHEN se.amount1In <> 0 THEN token1_symbol
    END AS symbol_In,
CASE 
    WHEN se.amount0Out <> 0 THEN token0_symbol
    WHEN se.amount1Out <> 0 THEN token1_symbol
    END AS symbol_out,
case 
    WHEN amount0In <> 0
    AND amount1In <> 0 THEN token1.decimals
    WHEN amount0In <> 0 THEN token0.decimals
    WHEN amount1In <> 0 THEN token1.decimals
    END AS decimals_in,
CASE
    WHEN amount0Out <> 0 THEN token0.decimals
    WHEN amount1Out <> 0 THEN token1.decimals
    END AS decimals_out,
se.TO_ADDRESS::string as tx_to,
se.ingested_at
from {{ ref('swaps') }} se --27,288,348
left join {{ ref('tokens') }} token0
on se.token0_address = token0.token_address
left join {{ ref('tokens') }} token1
on se.TOKEN1_ADDRESS = token1.TOKEN_ADDRESS --27,288,348
where 1 = 1

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(ingested_at) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),


 
    

ETH_prices as
( select token_address,
        hour,
        symbol,
        avg(price) as price  
   from         {{ source(
            'ethereum_db_sushi',
            'FACT_HOURLY_TOKEN_PRICES'
        ) }}

    WHERE
        1 = 1

{% if is_incremental() %}
AND HOUR :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        swap_without_prices
)
{% else %}
    AND HOUR :: DATE >= '2020-05-05'
{% endif %}

group by token_address,
         hour,
         symbol),
         


Harmony_Eth_crosstab as
(select 
 name, 
 symbol, 
max (case 
     when platform_id = 'harmony-shard-0' then token_address 
    else '' end) as harmony_address, 
max (case 
    when platform = 'ethereum' then token_address 
        else '' end) as eth_address
from {{ source(
            'symbols_cross_tab',
            'MARKET_ASSET_METADATA'
        ) }}
group by 1,2
having harmony_address <> '' and eth_address <> ''
order by 1,2 
),

Harmony_prices as 
(select 
ep.token_address,
ep.hour,
ep.symbol,
ep.price,
hec.harmony_Address as harmony_address
from Eth_prices ep
left join Harmony_Eth_crosstab hec
on ep.token_address = hec.eth_Address 
) 

select 
wp.block_timestamp, 
wp.block_number,
wp.tx_hash, 
wp.pool_address,
'sushiswap' as platform,
wp.event_index,
wp.evm_origin_from_address,
wp.evm_origin_to_address,
wp.native_origin_from_address,
wp.native_origin_to_address,
wp.amount_in,
wp.amount_out,
wp.sender,
wp.LOG_ID,
wp.token_In,
wp.token_out,
wp.symbol_In,
wp.symbol_out,  
wp.tx_to,
CASE
    WHEN decimals_in IS NOT NULL
    AND amount_in * pIn.price <= 5 * amount_out * pOut.price
    AND amount_out * pOut.price <= 5 * amount_in * pIn.price THEN amount_in * pIn.price
    ELSE NULL
END AS amount_in_usd,
CASE
    WHEN decimals_out IS NOT NULL
    AND amount_in * pIn.price <= 5 * amount_out * pOut.price
    AND amount_out * pOut.price <= 5 * amount_in * pIn.price THEN amount_out * pOut.price
    ELSE NULL
END AS amount_out_usd,
wp.ingested_at    
from swap_without_prices wp
left join Harmony_prices pIn
    on    lower(token_In) = lower(pIn.harmony_address)
    and   date_trunc('hour',wp.block_timestamp) = pIn.hour
left join Harmony_prices pOut
    on    lower(token_out) = lower(pOut.harmony_address)
    and   date_trunc('hour',wp.block_timestamp) = pOut.hour --27,288,348
where pool_address in (select token_address 
                        from MDAO_HARMONY.PROD.TOKENS
                        where token_name = 'SushiSwap LP Token')
                        











