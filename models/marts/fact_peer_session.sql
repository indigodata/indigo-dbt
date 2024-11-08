{{
    config(
        materialized='incremental'
      , cluster_by=['peer_id']
      , unique_key=['peer_id', 'start_time']
      , snowflake_warehouse='compute_large_wh'
      , pre_hook="{% if is_incremental() %}
             SET START_TIMESTAMP = (SELECT GREATEST(MAX(start_time), MAX(end_time)) FROM {{ this }});
             {% else %}
             SET START_TIMESTAMP = '2024-02-01 00:00:00'::timestamp;
             {% endif %}"
    )
}}

WITH country AS (
  SELECT *
  FROM {{ ref('seed_country_code') }}
)
, peer_set_unioned AS (
  SELECT
      msg_timestamp
    , node_id
    , msg_type
    , peer_id
    , NULL            AS msg_timestamp_remove
    , NULL            AS session_duration
  FROM {{ source('keystone_offchain', 'network_feed') }}
  WHERE msg_timestamp > $START_TIMESTAMP
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
  WHERE msg_timestamp > $START_TIMESTAMP
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
  WHERE msg_timestamp > $START_TIMESTAMP
    AND msg_type = 'indigo_node_start'    
  {% if is_incremental() %}
      UNION ALL
      
      SELECT
          start_time      AS msg_timestamp
        , node_id
        , 'peer_set_add'  AS msg_type
        , peer_id
        , NULL            AS msg_timestamp_remove
        , NULL            AS session_duration
      FROM {{ this }}
        WHERE end_time IS NULL
          -- TODO: lookback range filter
    {% endif %}
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
      , sessions.end_timestamp                          AS end_time
      , session_duration::NUMBER                        AS session_duration                            
      , DATEDIFF(
          NANOSECOND,
          start_time,
          COALESCE(
            end_time,
            (SELECT MAX(msg_timestamp) FROM sessions)
          )
        )                                               AS session_duration_calculated
      , session_duration_calculated / 1e9 / 3600.0      AS session_duration_hour
      , sessions.end_timestamp IS NULL                  AS end_time_imputed
      , node_id
      , peer_id
  FROM sessions
  WHERE msg_type = 'peer_set_add'
)
, handshake_feed AS (
    SELECT
          node_id
        , msg_timestamp
        , peer_id
        , msg_data[0]::INT      AS network
        , msg_data[2]::STRING   AS genesis_block
        , msg_data[3]::STRING   AS head_block_hash
        , msg_data[4]::STRING   AS local_block_hash
    FROM {{ source('keystone_offchain', 'network_feed') }}
    WHERE msg_timestamp > $START_TIMESTAMP - INTERVAL '14 hours'
        AND msg_type = 'handshake_eth'
        AND network = 1
        AND genesis_block = '0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3'
)
, sync_gap AS (
    SELECT 
          feed.node_id
        , feed.msg_timestamp
        , feed.peer_id
        , feed.head_block_hash = feed.local_block_hash
            AND feed.head_block_hash IS NOT NULL        AS in_sync
        , b1.blk_number                                 AS head_block
        , b2.blk_number                                 AS local_block
        , GREATEST(head_block - local_block, 0)         AS sync_block_gap
        , ROW_NUMBER() OVER(
            PARTITION BY
                node_id,
                peer_id,
                DATE_TRUNC(MINUTE, msg_timestamp)
            ORDER BY msg_timestamp)                     AS row_num
    FROM handshake_feed feed
        LEFT JOIN {{ source('keystone_ethereum', 'block_header') }} b1
            ON feed.head_block_hash = LOWER('0x' || b1.blk_hash::STRING)
        LEFT JOIN {{ source('keystone_ethereum', 'block_header') }} b2
            ON feed.local_block_hash = LOWER('0x' || b2.blk_hash::STRING)
    QUALIFY row_num = 1
)
, node_tracker_feed AS (
  SELECT
      node_id
    , msg_timestamp
    , peer_id
    , REPLACE(msg_data[0], '"', '')                                                     AS peer_public_key
    , msg_data[1]::INT                                                                  AS peer_rlp_protocol_version
    , NULLIF(SPLIT_PART(msg_data[2], '/', 1), '')                                       AS peer_client_type
    , NULLIF(REGEXP_SUBSTR(MSG_DATA[2], 'v[^/]+', 1, 1, 'e'), '')                       AS peer_client_version
    , NULLIF(REGEXP_SUBSTR(MSG_DATA[2], '/([^/]+)/[^/]+$', 1, 1, 'e'), '')              AS peer_os
    , NULLIF(REGEXP_SUBSTR(MSG_DATA[2], '([^/]+)$', 1, 1, 'e'), '')                     AS peer_run_time_version
    , REPLACE(msg_data[3], '"', '')                                                     AS peer_capabilities
    , REPLACE(SPLIT_PART(msg_data[4], ':', 1), '"', '')                                 AS peer_ip
    , SPLIT_PART(msg_data[4], ':', 2)::INT                                              AS peer_port
    , GEOIP2_COUNTRY(peer_ip)                                                           AS peer_country
    , GEOIP2_CITY(peer_ip)                                                              AS peer_city
    , GEOIP2_SUBDIVISION(peer_ip)                                                       AS peer_subdivision
    , ROW_NUMBER() OVER(PARTITION BY node_id, peer_id, DATE_TRUNC(MINUTE, msg_timestamp) ORDER BY msg_timestamp) AS row_num
  FROM {{ source('keystone_offchain', 'network_feed') }}
  WHERE msg_timestamp > $START_TIMESTAMP - INTERVAL '14 hours'
        AND msg_type = 'node_tracker'
  QUALIFY row_num = 1
)
SELECT 
    s.start_time
  , s.end_time
  , s.session_duration
  , s.session_duration_calculated
  , s.session_duration_hour
  , s.end_time_imputed
  , s.node_id
  , s.peer_id
  , nt.peer_public_key
  , nt.peer_rlp_protocol_version
  , nt.peer_client_type
  , nt.peer_client_version
  , nt.peer_os
  , nt.peer_run_time_version
  , nt.peer_capabilities
  , nt.peer_ip
  , nt.peer_port
  , sg.in_sync
  , sg.head_block
  , sg.local_block
  , sg.sync_block_gap
  , country.region                                              AS peer_region
  , country.country_name                                        AS peer_country
  , nt.peer_city
  , nt.peer_subdivision
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
  , '{{run_started_at}}'::timestamp_ntz                         AS updated_at
FROM sessions_enriched s
  LEFT JOIN node_tracker_feed nt
    ON s.node_id = nt.node_id
      AND s.peer_id = nt.peer_id
      AND date_trunc(MINUTES, s.start_time) = date_trunc(MINUTES, nt.msg_timestamp)
  LEFT JOIN sync_gap sg
    ON s.node_id = sg.node_id
      AND s.peer_id = sg.peer_id
      AND date_trunc(MINUTES, s.start_time) = date_trunc(MINUTES, sg.msg_timestamp)
  LEFT JOIN country
    ON nt.peer_country = country.country_code
  LEFT JOIN {{ ref('dim_peers') }} etherscan
    ON s.peer_id = etherscan.node_id
      AND etherscan.source = 'etherscan'
LEFT JOIN {{ ref('dim_peers') }} ethernodes
    ON s.peer_id = ethernodes.node_id
      AND ethernodes.source = 'ethernodes'
