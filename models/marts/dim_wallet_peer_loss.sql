{{
    config(
        materialized='incremental'
      , cluster_by=['wallet_address']
      , pre_hook=[
            '{{ func_get_array_position() }}', 
            '{{ func_get_item_at_index() }}',
            "{% if is_incremental() %}
             SET START_TIMESTAMP = (SELECT MAX(update_end_time) FROM {{ this }});
             {% else %}
             SET START_TIMESTAMP = '2024-02-01 00:00:00'::timestamp;
             {% endif %}
             SET END_TIMESTAMP = DATEADD(hour, 47, $START_TIMESTAMP);"
        ]
    )
}}

WITH wallet_peer AS (
    SELECT DISTINCT
        wallet_address
      , peer_id
    FROM {{ ref('dim_wallet_peer_win') }}
    WHERE wallet_address IN (
        SELECT DISTINCT tx_from
        FROM {{ ref('fact_hash_win__tx_from') }}
        WHERE first_seen_at >= $START_TIMESTAMP
          AND first_seen_at < $END_TIMESTAMP
    )
)
, session_hours AS (
    SELECT
        wallet_peer.peer_id
      , wallet_peer.wallet_address
      , peer_session.session_hour
      , MAX(peer_session.CONFIRMED_DISTINCT_TX_PER_MINUTE)          AS hash_per_minute
      , MIN(peer_session.start_time)               AS session_start_time
      , MAX(peer_session.end_time)                 AS session_end_time
      , BOOLAND_AGG(peer_session.is_edge_hour)     AS is_edge_hour
    FROM wallet_peer
      INNER JOIN peer_session_performance_hourly peer_session
        ON wallet_peer.peer_id=peer_session.peer_id
    WHERE peer_session.session_hour >= $START_TIMESTAMP
      AND peer_session.session_hour < $END_TIMESTAMP
    GROUP BY 1,2,3
)
, updates_to_make AS (
    SELECT
        sh.peer_id
      , sh.wallet_address
      , sh.session_hour
      , sh.session_start_time
      , sh.session_end_time
      , sh.is_edge_hour
      , sh.hash_per_minute
      , hash_win.first_peer
      , hash_win.first_seen_at
      , hash_win.seen_by_count
      , hash_win.confirmation_lag_sec
      , hash_win.msg_peer_order
      , hash_win.msg_timestamps
    FROM {{ ref('fact_hash_win__tx_from') }} hash_win
        INNER JOIN session_hours sh
            ON hash_win.tx_from=sh.wallet_address
            AND DATE_TRUNC(hour, hash_win.first_seen_at)=sh.session_hour
    WHERE hash_win.first_seen_at >= $START_TIMESTAMP
      AND hash_win.first_seen_at < $END_TIMESTAMP
)
, loss_hashes AS (
    SELECT
        peer_id
      , wallet_address
      , hash_per_minute
      , seen_by_count
      , confirmation_lag_sec
      , CAST(peer_id AS BINARY(32)) AS peer_id_bin
      , get_array_position(peer_id_bin, msg_peer_order)                 AS broadcast_position
      , get_item_at_index(msg_timestamps, broadcast_position)           AS lost_timestamp
      , DATEDIFF('millisecond', first_seen_at, lost_timestamp) / 1000   AS loss_margin
    FROM updates_to_make
    WHERE peer_id!=first_peer -- LOSSES ONLY
      AND (is_edge_hour = FALSE
            OR first_seen_at BETWEEN session_start_time AND session_end_time)
)
, loss_stats AS (
    SELECT
        peer_id
      , wallet_address
      , AVG(hash_per_minute)                 AS avg_hash_per_minute
      , COUNT(1)                             AS loss_count
      , COUNT_IF(broadcast_position IS NULL) AS no_show_count
      , AVG(broadcast_position)              AS avg_loss_position
      , AVG(loss_margin)                     AS avg_loss_margin
      , AVG(seen_by_count)                   AS avg_seen_by_count__loss
      , AVG(confirmation_lag_sec)            AS avg_confirmation_lag__loss
    FROM loss_hashes
    GROUP BY 1,2
)
, get_tx_stats AS (
    SELECT 
        peer_id
      , tx_from                     AS wallet_address
      , COUNT(DISTINCT tx_hash)     AS get_tx_count
    FROM {{ ref('fact_get_tx') }}
    WHERE msg_timestamp >= $START_TIMESTAMP
      AND msg_timestamp < $END_TIMESTAMP
    GROUP BY 1,2
)
SELECT 
    loss.*
  , gts.get_tx_count
  , $START_TIMESTAMP        AS update_start_time
  , $END_TIMESTAMP          AS update_end_time
FROM loss_stats loss
    LEFT JOIN get_tx_stats gts
      ON loss.peer_id=gts.peer_id
      AND loss.wallet_address=gts.wallet_address
