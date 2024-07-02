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
    FROM {{ ref('fact_transaction__tx_hash') }}
    WHERE blk_timestamp > '2024-01-01'
    GROUP BY 1
)
, mempool_performance AS (
    SELECT
          perf.peer_id
        , perf.session_hour
        , perf.confirmed_distinct_tx_per_minute
        , LEAST(
            perf.confirmed_distinct_tx_count / tx.tx_ct,
            1
          ) AS pct_mempool
    FROM {{ ref('peer_session_performance_hourly') }} perf
        LEFT JOIN tx_hour tx
            ON perf.session_hour = tx.blk_hour
    WHERE is_edge_hour = FALSE
)
, performance_metrics AS (
    SELECT
        peer_id
        , AVG(confirmed_distinct_tx_per_minute) AS avg_confirmed_distinct_tx_per_minute
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
, latest_dimensions AS (
    SELECT *
    FROM {{ ref('fact_peer_session') }}
    WHERE peer_client_type IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY peer_id
        ORDER BY start_time DESC) = 1
)
, version_changes AS (
    SELECT
          peer_id
        , start_time
        , peer_client_version
        , LAG(peer_client_version) OVER (
            PARTITION BY peer_id 
            ORDER BY start_time
          )                                 AS prev_version,
    FROM {{ ref('fact_peer_session') }}
    WHERE peer_client_type IS NOT NULL
    QUALIFY peer_client_version != prev_version 
        OR prev_version IS NULL
)
, last_upgrade AS (
    SELECT
          peer_id
        , start_time AS upgraded_at
        , peer_client_version
    FROM
        version_changes
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY peer_id 
        ORDER BY start_time DESC) = 1
)
, sync_lag AS (
    SELECT 
          peer_id
        , AVG(sync_block_gap)               AS avg_sync_lag
        , COALESCE(
            AVG(
                IFF(
                    start_time::DATE >= (SYSDATE() - INTERVAL '2 weeks')::DATE,
                    sync_block_gap,
                    NULL)
                )
            , 0
          )                                 AS avg_sync_lag_2w
    FROM {{ ref('fact_peer_session') }}
    GROUP BY 1
)
SELECT
      perf.peer_id
    , ROUND(perf.avg_confirmed_distinct_tx_per_minute) AS avg_confirmed_distinct_tx_per_minute
    , ROUND(perf.avg_propogation_rate_2w * 100, 2)  AS avg_propogation_rate_2w
    , ROUND(perf.max_propogation_rate_2w * 100, 2)  AS max_propogation_rate_2w
    , ROUND(perf.avg_propogation_rate * 100, 2)     AS avg_propogation_rate
    , ROUND(perf.max_propogation_rate * 100, 2)     AS max_propogation_rate
    , up.upgraded_at
    , avg_sync_lag
    , avg_sync_lag_2w
    , COALESCE(dim.in_sync , FALSE)                 AS in_sync
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
    LEFT JOIN latest_dimensions dim
        ON perf.peer_id = dim.peer_id
    LEFT JOIN last_upgrade up
        ON perf.peer_id = up.peer_id
    LEFT JOIN sync_lag sl
        ON perf.peer_id = sl.peer_id
