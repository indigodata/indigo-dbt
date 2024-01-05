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
  WHERE msg_timestamp >= SYSDATE() - INTERVAL '1 WEEK'
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
  WHERE msg_timestamp >= SYSDATE() - INTERVAL '1 WEEK'
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
  WHERE msg_timestamp >= SYSDATE() - INTERVAL '1 WEEK'
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
      , session_duration::NUMBER                        AS session_duration                            
      , DATEDIFF(NANOSECOND, start_time, end_time)      AS session_duration_calculated
      , sessions.end_timestamp IS NULL                  AS end_time_imputed
      , node_id
      , peer_id
  FROM sessions
  WHERE msg_type = 'peer_set_add'
)
, node_tracker_feed AS (
  SELECT
      node_id
    , msg_timestamp
    , peer_id
    , msg_data        AS peer_meta_data
  FROM {{ source('keystone_offchain', 'network_feed') }}
    WHERE msg_timestamp >= SYSDATE() - INTERVAL '1 WEEK'
        AND msg_type = 'node_tracker'
)
SELECT 
    s.start_time
  , s.end_time
  , s.session_duration
  , s.session_duration_calculated
  , s.end_time_imputed
  , s.node_id
  , s.peer_id
  , REPLACE(peer_meta_data[0], '"', '')                         AS peer_public_key
  , peer_meta_data[1]::INT                                      AS peer_rlp_protocol_version
  , NULLIF(SPLIT_PART(peer_meta_data[2], '/', 0), '')           AS peer_client_type
  , NULLIF(SPLIT_PART(peer_meta_data[2], '/', 1), '')           AS peer_client_version
  , NULLIF(SPLIT_PART(peer_meta_data[2], '/', 2), '')           AS peer_os
  , NULLIF(SPLIT_PART(peer_meta_data[2], '/', 3), '')           AS peer_run_time_version
  , REPLACE(peer_meta_data[3], '"', '')                         AS peer_capabilities
  -- bug with split_part
  , REPLACE(SPLIT(peer_meta_data[4], ':')[0], '"', '')          AS peer_ip
  , SPLIT(peer_meta_data[4], ':')[1]::INT                       AS peer_port
  , etherscan.first_seen                                        AS etherscan_first_seen
  , etherscan.last_seen                                         AS etherscan_last_seen
  , etherscan.ip                                                AS etherscan_ip
  , etherscan.port                                              AS etherscan_port
  , etherscan.country                                           AS etherscan_country
  , etherscan.client_type                                       AS etherscan_client_type
  , etherscan.run_time_version                                  AS etherscan_run_time_version
  , etherscan.os                                                AS etherscan_os
  , ethernodes.first_seen                                       AS ethernodes_first_seen
  , ethernodes.last_seen                                        AS ethernodes_last_seen
  , ethernodes.ip                                               AS ethernodes_ip
  , ethernodes.isp                                              AS ethernodes_isp
  , ethernodes.country                                          AS ethernodes_country
  , ethernodes.client_type                                      AS ethernodes_client_type
  , ethernodes.client_version                                   AS ethernodes_client_version
  , ethernodes.os                                               AS ethernodes_os
  , ethernodes.in_sync                                          AS ethernodes_in_sync

FROM sessions_enriched s
  LEFT JOIN node_tracker_feed nt
    ON s.node_id = nt.node_id
      AND s.peer_id = nt.peer_id
      AND DATEDIFF(MINUTES, s.start_time, nt.msg_timestamp) BETWEEN 0 AND 1
  LEFT JOIN {{ ref('dim_peers') }} etherscan
    ON s.peer_id = etherscan.node_id
      AND etherscan.source = 'etherscan'
LEFT JOIN {{ ref('dim_peers') }} ethernodes
    ON s.peer_id = ethernodes.node_id
      AND ethernodes.source = 'ethernodes'
