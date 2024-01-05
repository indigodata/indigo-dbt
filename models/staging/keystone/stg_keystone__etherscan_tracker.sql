{{ config(materialized='ephemeral') }}

WITH etherscan_tracker_base AS (
    SELECT 
          *
    FROM {{ source('keystone_offchain', 'node_tracker') }}
)

SELECT
      node_id                               AS node_public_key
    , host                                  AS ip
    , port
    , country
    , client                                AS client_type
    , type                                  AS run_time_version
    , os
    , last_seen
    , '{{run_started_at}}'::timestamp_ntz   AS run_timestamp
FROM etherscan_tracker_base
