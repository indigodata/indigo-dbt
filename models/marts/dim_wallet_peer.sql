{{
    config(
        materialized='table'
      , cluster_by=['wallet_address']
      , pre_hook=[
            '{{ func_get_array_position() }}', 
            '{{ func_get_item_at_index() }}'
        ]
    )
}}

WITH fact_hash_time_range AS (
    SELECT
        MAX(DATE_TRUNC(min, first_seen_at)) AS max_min
      , MIN(DATE_TRUNC(min, first_seen_at)) AS min_min
    FROM {{ ref('fact_hash__peer_id') }}
)
, session_minute_spine AS (
    SELECT
        min_min
      , max_min
      , ROW_NUMBER() OVER (ORDER BY seq4())  AS rownum
      , DATEADD(minute, rownum, min_min)      AS session_minute
    FROM TABLE(GENERATOR(rowcount => 10000))
    CROSS JOIN fact_hash_time_range
    QUALIFY session_min <= max_min
)
, session_mins AS (
  SELECT
      peer_id
    , session_min
    , MAX(
        COALESCE(confirmed_distinct_tx_per_minute, 0)
      ) AS hash_per_minute
  FROM analytics_slong.peer_session_unique_hash
    CROSS JOIN session_minute_spine
  WHERE session_min BETWEEN start_time AND end_time
  GROUP BY 1,2
)
, win_stats AS (
    SELECT
        first_peer                  AS peer_id
      , tx_from                     AS wallet_address
      , AVG(sm.hash_per_minute)     AS avg_hash_per_minute
      , COUNT(1)                    AS win_count
      , ARRAY_SIZE(
            ARRAY_AGG(
                DISTINCT DATE_TRUNC(hour, first_seen_at::timestamp)
            )
        )                           AS distinct_hours_with_win
      , AVG(win_margin_sec)         AS avg_win_margin
      , AVG(seen_by_count)          AS avg_seen_by_count__win
      , AVG(confirmation_lag_sec)   AS avg_confirmation_lag__win
    FROM {{ ref('fact_hash__peer_id') }}
        LEFT JOIN session_mins sm 
            ON first_peer=sm.peer_id
            AND DATE_TRUNC(minute, first_seen_at)=sm.session_min
    GROUP BY 1,2
)
, winner_wallet_session_mins AS (
    SELECT 
        win.peer_id
      , win.wallet_address
      , sh.session_min
      , sh.hash_per_minute
    FROM win_stats win
        INNER JOIN session_mins sh
            ON win.peer_id=sh.peer_id
)
, loss_hashes AS (
    SELECT
        win.peer_id
      , win.wallet_address
      , win.hash_per_minute
      , fh.first_seen_at
      , fh.seen_by_count
      , fh.confirmation_lag_sec
      , get_array_position(win.peer_id, fh.msg_peer_order)                  AS broadcast_position
      , get_item_at_index(fh.msg_timestamps, broadcast_position)            AS lost_timestamp
      , DATEDIFF('millisecond', fh.first_seen_at, lost_timestamp) / 1000    AS loss_margin
    FROM winner_wallet_session_mins win
        INNER JOIN {{ ref('fact_hash__tx_from') }} fh
            ON win.wallet_address=fh.tx_from
            AND DATE_TRUNC(minute, fh.first_seen_at)=win.session_min
    WHERE win.peer_id!=fh.first_peer
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
        win.peer_id
      , win.wallet_address
      , COUNT(DISTINCT fgt.tx_hash)     AS get_tx_count
    FROM win_stats win
        INNER JOIN {{ ref('fact_get_tx') }} fgt
            ON win.peer_id=fgt.peer_id
            AND win.wallet_address=fgt.tx_from
    GROUP BY 1,2
)
SELECT 
    win.peer_id
  , win.wallet_address
  , SUM(win.win_count) OVER (
      PARTITION BY win.wallet_address
   )                                    AS tx_count
  , win.distinct_hours_with_win
  , win.win_count
  , loss.loss_count
  , loss.no_show_count
  , win.avg_hash_per_minute         AS avg_hash_per_minute__win
  , loss.avg_hash_per_minute        AS avg_hash_per_minute__loss
  , loss.avg_loss_position
  , win.avg_win_margin
  , loss.avg_loss_margin
  , win.avg_seen_by_count__win
  , loss.avg_seen_by_count__loss
  , win.avg_confirmation_lag__win
  , loss.avg_confirmation_lag__loss
FROM win_stats win
    LEFT JOIN loss_stats loss
      ON win.peer_id=loss.peer_id
      AND win.wallet_address=loss.wallet_address
    LEFT JOIN get_tx_stats gts
      ON win.peer_id=gts.peer_id
      AND win.wallet_address=gts.wallet_address
