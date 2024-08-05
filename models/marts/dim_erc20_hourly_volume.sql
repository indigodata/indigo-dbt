{{
    config(
        materialized='table'
      , cluster_by=['token_contract']
    )
}}
WITH erc_transfer AS (
    SELECT
          erc.tx_hash
        , erc.token_contract
        , ANY_VALUE(erc.blk_timestamp)                  AS blk_timestamp
        , SUM(
            IFF(
                erc.tx_from = erc.recipient,
                erc.amount / POW(10, price.token_decimals) * price.start_price_eth,
                0
            )
          )                                         AS in_eth
        , SUM(
            IFF(
                erc.tx_from = erc.sender,
                erc.amount / POW(10, price.token_decimals) * price.start_price_eth,
                0
            )
          )                                         AS out_eth
        , in_eth - out_eth                          AS net_eth
    FROM {{ ref('fact_erc20_transfer') }} erc
        LEFT JOIN {{ ref('fact_hourly_price_change') }} price
            ON erc.token_contract = price.token_address
                AND DATE_TRUNC(HOUR, erc.blk_timestamp) = price.start_hour
        WHERE (erc.tx_from = erc.sender 
            OR erc.tx_from = erc.recipient)
        AND blk_timestamp >= '2024-01-01 01:00:00'
    GROUP BY 1,2
)
SELECT 
      erc.token_contract
    , DATE_TRUNC(HOUR, erc.blk_timestamp) AS transfer_hour
    , peer.peer_country
    , SUM(erc.in_eth) AS in_eth
    , SUM(erc.out_eth) AS out_eth
    , SUM(erc.net_eth) AS net_eth
FROM erc_transfer erc
    LEFT JOIN production.fact_hash_win_simple__tx_hash win
        ON erc.tx_hash = '0x' || lower(win.tx_hash::VARCHAR)
    LEFT JOIN production.dim_peer_performance peer
        ON win.peer_id = peer.peer_id
GROUP BY 1,2,3
