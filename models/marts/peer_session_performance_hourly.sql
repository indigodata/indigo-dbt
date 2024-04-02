{{
    config(
        materialized='incremental'
      , cluster_by=['peer_id']
      , snowflake_warehouse='compute_large_wh'
      , pre_hook="{% if is_incremental() %}
            SET UPDATE_START_TIME = (SELECT DATEADD(HOUR,1, MAX(session_hour)) FROM {{ this }});
            {% else %}
            SET UPDATE_START_TIME = '2024-02-01 00:00:00'::timestamp;
            {% endif %}
            SET UPDATE_END_TIME = (SELECT DATEADD(hour, -1, DATE_TRUNC(HOUR, MAX(start_time))) FROM production.fact_peer_session);
            "
    )
}}

WITH peer_sessions AS (
    SELECT 
          peer_id
        , node_id
        , start_time
        , LEAST($UPDATE_END_TIME, DATEADD(HOUR, 12, start_time))        AS imputed_end_time
        , COALESCE(end_time, imputed_end_time)                          AS end_time
        , DATE_TRUNC('hour', start_time)                                AS start_hour
        , DATE_TRUNC('hour', COALESCE(end_time, imputed_end_time))      AS end_hour
    FROM {{ ref('fact_peer_session') }}
    WHERE start_time < $UPDATE_END_TIME
        AND start_time >= DATEADD(HOUR, -12, $UPDATE_START_TIME)
        AND (end_time >= $UPDATE_START_TIME
            OR end_time IS NULL)
)
, row_generator AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS idx
    FROM TABLE (GENERATOR(rowcount => 1000))
)
, peer_sessions_hourly AS (
    SELECT
          peer_id
        , node_id
        , DATEADD('hour', idx, start_hour)                                  AS session_hour
        , start_time
        , end_time
        , session_hour = start_hour 
            OR session_hour = end_hour                                      AS is_edge_hour
        , GREATEST(session_hour, start_time)                                AS session_hour_start
        , LEAST(end_time, session_hour + INTERVAL '1 hour')                 AS session_hour_end
        , DATEDIFF('minute', session_hour_start, session_hour_end)          AS session_hour_minutes
    FROM peer_sessions ps
        CROSS JOIN row_generator rg
    WHERE session_hour BETWEEN start_hour AND end_hour 
        AND session_hour >= $UPDATE_START_TIME
        AND session_hour < $UPDATE_END_TIME
        AND session_hour_start != session_hour_end
)
, peer_messages AS (
    SELECT
          *
        , CAST(SUBSTRING(hashes.value, 3) AS BINARY(32)) AS tx_hash
    from KEYSTONE_OFFCHAIN.NETWORK_FEED,
        LATERAL FLATTEN(input => msg_data) hashes
    WHERE msg_timestamp BETWEEN $UPDATE_START_TIME AND $UPDATE_END_TIME
        AND msg_type IN ('new_hash', 'new_hash_66', 'new_hash_68')
)
, confirmed as (
    SELECT
          msg.peer_id
        , msg.node_id
        , msg_timestamp
        , ft.tx_hash
        , ft.tx_hash IS NOT NULL AS is_sent_pre_confirmation
    FROM peer_messages msg
        LEFT JOIN {{ ref('fact_transaction__tx_hash') }} ft
            ON msg.tx_hash=ft.tx_hash
            AND msg.msg_timestamp < ft.blk_timestamp
)
SELECT 
      s.PEER_ID
    , s.NODE_ID
    , s.session_hour
    , s.session_hour_start
    , s.session_hour_end
    , s.start_time
    , s.end_time
    , s.is_edge_hour
    , s.session_hour_minutes
    , COUNT(conf.peer_id)                                       AS total_hash_count
    , DIV0(total_hash_count, session_hour_minutes)              AS hash_per_minute
    , COUNT(DISTINCT conf.tx_hash)                              AS confirmed_distinct_tx_count
    , DIV0(confirmed_distinct_tx_count, session_hour_minutes)   AS confirmed_distinct_tx_per_minute
    , $UPDATE_START_TIME                                        AS update_start_time
    , $UPDATE_END_TIME                                          AS update_end_time
    , '{{run_started_at}}'::timestamp_ntz                       AS updated_at
FROM peer_sessions_hourly s
LEFT JOIN confirmed conf
    ON conf.msg_timestamp BETWEEN s.session_hour_start AND s.session_hour_end
    AND s.peer_id = conf.peer_id
    AND s.node_id = conf.node_id
GROUP BY 1,2,3,4,5,6,7,8,9
