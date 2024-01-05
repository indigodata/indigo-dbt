{{ config(materialized='ephemeral') }}

WITH seen_at AS (
    SELECT
          node_public_key
        , MIN(last_seen) AS first_seen
        , MAX(last_seen) AS last_seen
    FROM {{ ref('stg_keystone__ethernodes_tracker') }}
    GROUP BY 1
)
, last_seen_dimentions AS (
    SELECT
          node_public_key
        , ip
        , isp
        , country
        , client_type
        , client_version
        , os
        , in_sync
    FROM {{ ref('stg_keystone__ethernodes_tracker') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY node_public_key ORDER BY last_seen DESC) = 1
)
SELECT
      KECCAK256(sa.node_public_key) AS node_id
    , sa.node_public_key
    , sa.first_seen
    , sa.last_seen
    , ip
    , isp
    , country
    , client_type
    , client_version
    , os
    , in_sync
FROM seen_at sa
    LEFT JOIN last_seen_dimentions lsd
        ON sa.node_public_key = lsd.node_public_key
