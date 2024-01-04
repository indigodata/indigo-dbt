{{
    config(
        materialized='table'
      , cluster_by=['peer_id']
    )
}}


WITH peer_set_unioned AS (
  SELECT
      msg_timestamp
    , node_id
    , msg_type
    , peer_id
    , NULL            AS msg_timestamp_remove
    , NULL            AS session_duration
  FROM {{ source('keystone_offchain', 'network_feed') }}
  -- TODO: Remove for prod
  WHERE msg_timestamp >= '2023-12-26'
    AND msg_type = 'peer_set_add'

  UNION ALL

  SELECT
      msg_timestamp
    , node_id
    , msg_type
    , peer_id
    , msg_timestamp   AS msg_timestamp_remove
    , msg_data[2]     AS session_duration 
  FROM {{ source('keystone_offchain', 'network_feed') }}
  -- TODO: Remove for prod
  WHERE msg_timestamp >= '2023-12-26'
    AND msg_type = 'peer_set_remove'

  UNION ALL

  SELECT
      msg_timestamp
    , node_id
    , msg_type
    , peer_id
    , msg_timestamp   AS msg_timestamp_remove
    , NULL            AS session_duration
  FROM {{ source('keystone_offchain', 'network_feed') }}
  -- TODO: Remove for prod
  WHERE msg_timestamp >= '2023-12-26'
    AND msg_type = 'indigo_node_start'    
)
, sessions AS (
  SELECT
      msg_timestamp
    , msg_timestamp_remove
    , node_id
    , msg_type
    , peer_id
    , CASE WHEN msg_type = 'peer_set_add'
        THEN LEAD(msg_timestamp_remove) IGNORE NULLS OVER (
          PARTITION BY node_id, peer_id
          ORDER BY msg_timestamp
      )
      END                                                   AS end_timestamp
    , CASE WHEN msg_type = 'peer_set_add'
        THEN LEAD(session_duration) IGNORE NULLS OVER (
          PARTITION BY node_id, peer_id
          ORDER BY msg_timestamp
      )
      END                                                   AS session_duration
  FROM peer_set_unioned
)
, sessions_enriched AS (
  SELECT
        msg_timestamp                                   AS start_time
      , COALESCE(
          sessions.end_timestamp,
          (SELECT MAX(msg_timestamp) FROM SESSIONS)
        )                                               AS end_time
      , session_duration                                
      , DATEDIFF(NANOSECOND, start_time, end_time)      AS session_duration_calculated
      , sessions.end_timestamp IS NULL                  AS end_time_imputed
      , node_id
      , peer_id
  FROM sessions
  WHERE msg_type = 'peer_set_add'
)
