{{ config(materialized='ephemeral') }}

WITH seen_at AS (
    SELECT
          node_id
        , MIN(last_seen) AS first_seen
        , MAX(last_seen) AS last_seen
    FROM {{ ref('stg_keystone__node_tracker') }}
    GROUP BY 1
)
, last_seen_dimentions AS (
    SELECT
          node_id
        , ip
        , port
        , country
        , client_type
        , run_time_version
        , os
    FROM {{ ref('stg_keystone__node_tracker') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY node_id ORDER BY last_seen DESC) = 1
)
SELECT
      sa.node_id
    , sa.first_seen
    , sa.last_seen
    , ip
    , port
    , country
    , client_type
    , run_time_version
    , os
FROM seen_at sa
    LEFT JOIN last_seen_dimentions lsd
        ON sa.NODE_ID = lsd.NODE_ID