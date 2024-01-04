 {% macro new_blocks_only() %}
     {% if is_incremental() %}
        {% set new_block_only_filter %}
            CASE
                WHEN blockchain='polygon'
                    THEN (blk_number >= ({{ block_range('polygon', 'run_start_blk_number') }})
                        AND blk_number <= ({{ block_range('polygon', 'run_stop_blk_number') }}))
                WHEN blockchain='ethereum'
                    THEN (blk_number >= ({{ block_range('ethereum', 'run_start_blk_number') }})
                        AND blk_number <= ({{ block_range('ethereum', 'run_stop_blk_number') }}))
                WHEN blockchain='binance'
                    THEN (blk_number >= ({{ block_range('binance', 'run_start_blk_number') }})
                        AND blk_number <= ({{ block_range('binance', 'run_stop_blk_number') }}))
                WHEN blockchain='arbitrum'
                    THEN (blk_number >= ({{ block_range('arbitrum', 'run_start_blk_number') }})
                        AND blk_number <= ({{ block_range('arbitrum', 'run_stop_blk_number') }}))
                ELSE FALSE END
        {% endset %}
        {{ return(new_block_only_filter) }}
     {% else %}
        {{ return('TRUE') }}
    {% endif %}
 {% endmacro %}

 {% macro block_range(blockchain, block_field) %}
     {% set max_blk_number_query %}
        SELECT {{ block_field }}
        FROM {{ ref('incremental_coordinator') }}
        WHERE blockchain = '{{ blockchain }}'
            AND run_timestamp = '{{ run_started_at }}'
            AND incremental_table = UPPER('{{ this }}')
     {% endset %}
     {{ return(max_blk_number_query) }}
 {% endmacro %}