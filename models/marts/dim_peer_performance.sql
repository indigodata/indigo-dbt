{{
    config(
        materialized='table'
      , cluster_by=['peer_id']
    )
}}

WITH tx_hour AS (
    SELECT
          DATE_TRUNC(HOUR, BLK_TIMESTAMP) AS blk_hour
        , COUNT(1) AS tx_ct
    FROM production.fact_transaction__tx_hash
    WHERE blk_timestamp > '2024-01-01'
    GROUP BY 1
)
, mempool_performance AS (
    SELECT
          perf.peer_id
        , perf.SESSION_HOUR
        , LEAST(
            perf.confirmed_distinct_tx_count / tx.tx_ct,
            1
          ) AS pct_mempool
    FROM production.peer_session_performance_hourly perf
        LEFT JOIN tx_hour tx
            ON perf.session_hour = tx.blk_hour
    WHERE is_edge_hour = FALSE
)
, performance_metrics AS (
    SELECT
        peer_id
        , COALESCE(
            AVG(
                IFF(
                    session_hour::DATE >= (SYSDATE() - INTERVAL '2 weeks')::DATE,
                    pct_mempool,
                    NULL)
                )
            , 0
        )                               AS avg_propogation_rate_2w
        , COALESCE(
            MAX(
                IFF(
                    session_hour::DATE >= (SYSDATE() - INTERVAL '2 weeks')::DATE,
                    pct_mempool,
                    NULL)
            )
            , 0
        )                               AS max_propogation_rate_2w
        , AVG(pct_mempool)              AS avg_propogation_rate
        , MAX(pct_mempool)              AS max_propogation_rate
    FROM mempool_performance
    GROUP BY 1
)
, latest_dimentions AS (
    SELECT *
    FROM production.fact_peer_session
      WHERE peer_client_type IS NOT NULL
      QUALIFY ROW_NUMBER() OVER(
          PARTITION BY peer_id
          ORDER BY start_time DESC) = 1
)
SELECT
      perf.peer_id
    , ROUND(perf.avg_propogation_rate_2w * 100, 2)  AS avg_propogation_rate_2w
    , ROUND(perf.max_propogation_rate_2w * 100, 2)  AS max_propogation_rate_2w
    , ROUND(perf.avg_propogation_rate * 100, 2)     AS avg_propogation_rate
    , ROUND(perf.max_propogation_rate * 100, 2)     AS max_propogation_rate
    , dim.peer_public_key
    , dim.peer_rlp_protocol_version
    , dim.peer_client_type
    , dim.peer_client_version
    , dim.peer_os
    , dim.peer_run_time_version
    , dim.peer_capabilities
    , dim.peer_ip
    , dim.peer_port
    , dim.peer_region
    , dim.peer_country
    , dim.peer_city
    , dim.peer_subdivision
    , dim.start_time                                AS last_seen_at
FROM performance_metrics perf
    LEFT JOIN latest_dimentions dim
        ON perf.peer_id = dim.peer_id
