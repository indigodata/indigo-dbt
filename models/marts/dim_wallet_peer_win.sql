{{
    config(
        materialized='incremental'
      , cluster_by=['wallet_address']
      , pre_hook=[
            "{% if is_incremental() %}
             SET START_TIMESTAMP = (SELECT MAX(update_end_time) FROM {{ this }});
             {% else %}
             SET START_TIMESTAMP = '2024-02-01 00:00:00'::timestamp;
             {% endif %}
             SET END_TIMESTAMP = '2024-03-12 18:00:00'::timestamp;"
      ]
    )
}}

WITH win_stats AS (
    SELECT
        tx_from                     AS wallet_address
      , first_peer                  AS peer_id
      , session_hour
      , AVG(psp.CONFIRMED_DISTINCT_TX_PER_MINUTE)    AS avg_hash_per_minute
      , COUNT(1)                    AS win_count
      , MIN(first_seen_at)          AS first_win_time
      , AVG(win_margin_sec)         AS avg_win_margin
      , AVG(seen_by_count)          AS avg_seen_by_count
      , AVG(confirmation_lag_sec)   AS avg_confirmation_lag
    FROM {{ ref('fact_hash_win__peer_id') }} fhw
        INNER JOIN peer_session_performance_hourly psp
            ON first_peer=psp.peer_id
            AND DATE_TRUNC(hour, first_seen_at)=psp.session_hour
            AND first_node=psp.node_id
    WHERE first_seen_at >= $START_TIMESTAMP
        AND first_seen_at < $END_TIMESTAMP
        -- AND first_node NOT IN ('aws-virginia-1', 'aws-korea-1', 'hetzner-finland-2')
    GROUP BY 1,2,3
)
SELECT
    wallet_address
  , peer_id
  , COUNT(1)                  AS win_hour_count
  , MIN(first_win_time)       AS first_win_time
  , SUM(win_count)            AS win_count
  , AVG(avg_hash_per_minute)  AS avg_hash_per_minute
  , AVG(avg_win_margin)       AS avg_win_margin
  , AVG(avg_seen_by_count)    AS avg_seen_by_count
  , AVG(avg_confirmation_lag) AS avg_confirmation_lag
  , $START_TIMESTAMP          AS update_start_time
  , $END_TIMESTAMP            AS update_end_time
FROM win_stats
GROUP BY 1,2