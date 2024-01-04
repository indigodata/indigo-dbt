{% macro create_incremental_coordinator() %}
    {% set create_table_sql %}
        CREATE TABLE incremental_coordinator 
        IF NOT EXISTS
        (
            run_timestamp timestamp_ntz,
            blockchain varchar(256),
            incremental_table varchar(256),
            run_completed boolean,
            run_start_blk_number int,
            run_stop_blk_number int
        )
    {% endset %}
    {{ return(create_table_sql) }}
{% endmacro %}

{% macro assert_no_running_models() %}
    {% set run_completion_query %}
        WITH incremental_tables AS (
            SELECT VALUE::text AS incremental_table
            FROM table (flatten(input =>{{ incremental_models_in_selector() }}))
        ),

        previous_run AS (
            SELECT
                blockchain,
                it.incremental_table
            FROM incremental_tables it
                LEFT JOIN {{ target.schema }}.incremental_coordinator rc
                    ON rc.incremental_table = it.incremental_table
            QUALIFY LAST_VALUE(rc.run_completed) OVER (
                PARTITION BY
                    rc.blockchain,
                    rc.incremental_table
                ORDER BY run_timestamp
                ) = FALSE
            )

        SELECT
            COUNT(1) AS running_models
        FROM previous_run;
    {% endset %}

    {% if execute %}
        {% set results = run_query(run_completion_query) %}
        {% set incomplete_models = results.columns[0].values()[0] %}

        {# Assert there are no running models #}
        {% if incomplete_models > 0 %}
            {{ exceptions.raise_compiler_error("Incomplete model(s) found. Aborting until jobs finish") }}
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro incremental_models_in_selector() %}
    {% if execute %}
        {% set incremental_models = [] %}
        {% for m in graph.nodes.values() %}
            {% if m.unique_id in selected_resources 
                and "incremental_coordinator" in m.tags
                and m.config.materialized == "incremental"%}

                {% set model_name = [m.database, m.schema, m.name] | join(".") | upper() %}
                {% do incremental_models.append(model_name) %}
            {% endif %}
        {% endfor %}
    
        {{ return(incremental_models) }}    
    {% endif %}
{% endmacro %}

{% macro get_run_blocks(blockchain, field) %}
    {# Comment out when generating docs in external schema #}
    {% set block_query %}
        SELECT
            {% if field == 'run_start_blk_number' %}
                MIN({{ field }})
            {% else %}
                MAX({{ field }})
            {% endif %}
        FROM {{ ref('incremental_coordinator') }}
        WHERE blockchain = '{{ blockchain }}'
            AND run_timestamp = '{{run_started_at}}'::timestamp_ntz
    {% endset %}

    {% if execute %}
        {% set results = run_query(block_query) %}
        {% set block_num = results.columns[0].values()[0] %}

        {{ return(block_num) }}
    {% endif %}
{% endmacro %}

{% macro update_run_completed() %}
    {% set model_name = [this.database, this.schema, this.name] | join(".") | upper() %}
    {% if this.name == 'incremental_coordinator' %}
        {% set incremental_coordinator %}
            {{this}}
        {% endset %}
    {% else %}
        {% set incremental_coordinator %}
            {{ref('incremental_coordinator')}}
        {% endset %}
    {% endif %}
    {% set update_sql %}
        UPDATE {{incremental_coordinator}}
        SET run_completed=TRUE
        WHERE run_timestamp='{{run_started_at}}'::timestamp_ntz
            AND incremental_table='{{model_name}}'
    {% endset %}
    {{ return(update_sql) }}
{% endmacro %}