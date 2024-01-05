{{ config(materialized='ephemeral') }}

WITH ethernodes_base AS (
    SELECT *
    FROM {{ source('keystone_offchain', 'ethernodes_tracker') }}
)

SELECT
      node_id                               AS node_public_key
    , host_address                          AS ip
    , isp
    , country
    , client_type
    , client_version
    , os
    , in_sync
    , last_seen
    , '{{run_started_at}}'::timestamp_ntz   AS run_timestamp
FROM ethernodes_base
