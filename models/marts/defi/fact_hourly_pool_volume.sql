{{
    config(
          materialized='incremental'
        , cluster_by=['pool_address']
    )
}}

WITH fact_pair_swap AS (
    SELECT 
        * 
    FROM {{ ref('fact_pair_swap') }}
    WHERE blk_timestamp < (SELECT MAX(DATE_TRUNC(HOUR, blk_timestamp)) from {{ ref('fact_pair_swap') }})  
    {% if is_incremental() %}
        AND blk_timestamp > (SELECT MAX(curr.max_period_blk_timestamp)
                                    FROM {{ this }} AS curr)
    {% endif %}
)
, liquidity_pool AS (
    SELECT 
        * 
    FROM {{ ref('dim_liquidity_pool') }}
)
, fact_swap_weth AS (
    SELECT
        *
      , ABS(AMOUNT0)/POWER(10,18)                       AS absolute_weth_transfered
      , amount0/POWER(10,18)                            AS weth_transfered
      , SQRT(ABS(amount0)) * SQRT(ABS(amount1)) * 0.5   AS geom_mean_volume
      , DIV0(geom_mean_volume, liquidity)               AS vol_pct_of_liquidity
    FROM fact_pair_swap
)
  SELECT
      swap.pool_address
    , swap.token1
    , DATE_TRUNC(HOUR, swap.blk_timestamp)             AS swap_hour
    , MAX(swap.blk_timestamp)                          AS max_period_blk_timestamp
    , SUM(swap.absolute_weth_transfered)               AS hour_weth_volume
    , SUM(swap.vol_pct_of_liquidity)                   AS hour_vol_pct_of_liquidity
    , SUM(swap.weth_transfered)                        AS weth_balance
    , COUNT(1)                                         AS swap_count
  FROM fact_swap_weth swap
    LEFT JOIN liquidity_pool pool
      ON swap.pool_address = pool.pool_address
  GROUP BY 1,2,3

