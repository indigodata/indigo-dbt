{{
    config(
          materialized='incremental'
        , cluster_by=['token_address']
    )
}}

WITH hourly_spot_price AS (
    SELECT
          DATE_TRUNC(HOUR, blk_timestamp) + INTERVAL '1 hour'   AS spot_price_ts
        , token_address
        , token_decimals
        , price_eth
        , log_index
        , LAST_VALUE(tx_hash) OVER(
            PARTITION BY spot_price_ts, token_address
            ORDER BY blk_timestamp
        )                                                     AS latest_swap_tx_hash
        , SUM(ABS(eth_amount)) OVER(
            PARTITION BY spot_price_ts, token_address
        )                                                     AS prev_hour_eth_volume
    FROM {{ ref('fact_uniswap_token_price') }}
    QUALIFY latest_swap_tx_hash = tx_hash
)
SELECT 
      spot_price_ts - INTERVAL '1 hour' AS start_hour
    , spot_price_ts                     AS end_hour
    , token_address
    , token_decimals
    , LAG(price_eth) OVER( 
        PARTITION BY token_address
        ORDER BY spot_price_ts
      )                                 AS start_price_eth
    , price_eth                         AS end_price_eth
    , latest_swap_tx_hash               AS end_price_tx_hash
    , prev_hour_eth_volume              AS hour_volume_eth
FROM hourly_spot_price
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY token_address, start_hour
    ORDER BY log_index DESC
) = 1
