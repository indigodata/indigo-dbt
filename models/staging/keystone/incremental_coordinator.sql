{{
    config(
        materialized='incremental'
      , full_refresh = false
      , pre_hook=[
          '{{ create_incremental_coordinator() }}'
        , '{{ assert_no_running_models() }}']
      , post_hook='{{update_run_completed()}}'
    )
}}

{% set blockchains = ['ethereum'] %}
{% set source_tables = [
      'eth_transaction'
    , 'erc20_transfer'
    , 'weth_deposit_withdrawal'
] %}

WITH blockchains AS (
    SELECT
    VALUE::text     AS blockchain
    FROM table(flatten(input=>['ethereum']))
)
, incremental_models AS (
    SELECT 
        blockchain
      , VALUE::text     AS incremental_table
    FROM table(flatten(input=>{{incremental_models_in_selector()}}))
    JOIN blockchains ON TRUE
)
, previous_model_runs AS (
    SELECT 
        im.incremental_table
      , im.blockchain
      , MAX(COALESCE(rc.run_stop_blk_number,0)) AS previous_stop_blk_num
    FROM incremental_models im
        LEFT JOIN {{ this }} rc 
            ON rc.incremental_table=im.incremental_table
            AND rc.blockchain=im.blockchain
            AND rc.run_completed=TRUE
    GROUP BY 1, 2
)
, source_union AS (

    {% for blockchain in blockchains %}
        {% for src_table in source_tables %}
            SELECT
                '{{ blockchain }}' AS blockchain
              , MAX(BLK_NUMBER)   AS source_max_blk_number
            FROM {{ source('keystone_' ~ blockchain, src_table) }}
            WHERE inserted_at < '{{ run_started_at }}'::timestamp_ntz
            {% if not loop.last %} UNION ALL {% endif %}
        {% endfor %}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}

)
, run_start_block AS (
    SELECT
        incremental_table
      , blockchain
      , previous_stop_blk_num + 1           AS run_start_blk_number
    FROM previous_model_runs
)
, run_end_block AS (
    SELECT
        blockchain
    -- for referral_tracker.sql refresh
    {% if flags.FULL_REFRESH %}
      , incremental_table
      , previous_stop_blk_num               AS run_stop_blk_number
    FROM previous_model_runs

    {% else %}
      , MIN(source_max_blk_number)          AS run_stop_blk_number
    FROM source_union
    GROUP BY 1
    {% endif %}
)

SELECT
    '{{ run_started_at }}'::timestamp_ntz   AS run_timestamp
  , im.blockchain                           AS blockchain
  , im.incremental_table                    AS incremental_table
  , FALSE                                   AS run_completed
    {% if flags.FULL_REFRESH %}
    , 1::NUMBER                             AS run_start_blk_number
    {% else %}
    , rsb.run_start_blk_number              AS run_start_blk_number
    {% endif %}
  , reb.run_stop_blk_number                 AS run_stop_blk_number
FROM incremental_models im
   JOIN run_start_block rsb 
    ON rsb.blockchain=im.blockchain
      AND rsb.incremental_table = im.incremental_table
   JOIN run_end_block reb
    ON reb.blockchain=im.blockchain
    {% if flags.FULL_REFRESH %}
      AND reb.incremental_table = im.incremental_table
    {% endif %}