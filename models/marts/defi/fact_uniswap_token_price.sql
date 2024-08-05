{{
    config(
          materialized='table'
        , cluster_by=['token_address']
    )
}}


WITH l24h_volume AS (
    SELECT 
          token1
        , DATE_TRUNC(DAY, swap_hour)    AS swap_date
        , pool_address
        , SUM(hour_weth_volume)         AS weth_volume
    FROM {{ ref('fact_hourly_pool_volume') }}
    GROUP BY 1,2,3
)
, top_pool AS (
    SELECT
          token1
        , swap_date
        , pool_address
        , weth_volume
    FROM l24h_volume
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY token1, swap_date
        ORDER BY weth_volume DESC
    ) = 1
)
, swaps AS (
    SELECT 
          swap.blk_number
        , swap.blk_timestamp
        , swap.tx_hash
        , swap.tx_index
        , swap.log_index
        , swap.pool_address
        , pool.token0
        , swap.amount0
        , pool.token0_decimals
        , pool.token1
        , swap.amount1
        , pool.token1_decimals
    FROM {{ ref('fact_pair_swap') }} swap
        INNER JOIN top_pool
            ON swap.pool_address = top_pool.pool_address
                AND DATE_TRUNC(DAY, swap.blk_timestamp) = top_pool.swap_date
        LEFT JOIN {{ ref('dim_liquidity_pool') }} pool
            ON top_pool.pool_address = pool.pool_address
    WHERE swap.amount0 != 0
        AND swap.amount1 != 0
        AND pool.token1_decimals != 0
)
SELECT
      blk_number
    , blk_timestamp
    , tx_hash
    , tx_index
    , log_index
    , pool_address
    , token1 AS token_address
    , token1_decimals AS token_decimals
    , amount1 / POWER(10, token1_decimals) AS token_amount
    , amount0 / POWER(10, token0_decimals) AS eth_amount
    , ABS(eth_amount) / ABS(token_amount)  AS price_eth
FROM swaps