{{
    config(
        materialized='table'
      , cluster_by=['node_id']
    )
}}

WITH ethernodes AS (
    SELECT
          node_id
        , first_seen
        , last_seen
        , ip
        , NULL              AS port
        , isp
        , country
        , client_type
        , client_version
        , NULL              AS run_time_version
        , os
        , in_sync
        , 'ethernodes'      AS source
    FROM {{ ref('int_ethernodes_tracker') }}
)
, node_tracker AS (
    SELECT
          node_id
        , first_seen
        , last_seen
        , ip
        , port
        , NULL AS isp
        , country
        , client_type
        , NULL AS client_version
        , run_time_version
        , os
        , NULL AS in_sync
        , 'node_tracker' AS source
    FROM {{ ref('int_node_tracker') }}
)

SELECT *
FROM ethernodes

UNION ALL 

SELECT *
FROM node_tracker