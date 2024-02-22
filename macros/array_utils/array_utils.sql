{% macro func_find_duplicate_indices() -%}
CREATE OR REPLACE FUNCTION find_duplicate_indices(NODES_ORDERED ARRAY, PEERS_ORDERED ARRAY)
  RETURNS ARRAY
  LANGUAGE JAVASCRIPT
  AS $$
    var duplicates = [];
    var seen = {};

    for (var i = 0; i < NODES_ORDERED.length; i++) {
        var key = NODES_ORDERED[i] + '-' + PEERS_ORDERED[i];
        if (seen[key]) {
            duplicates.push(i);
        } else {
            seen[key] = true;
        }
    }

    return duplicates;
  $$;
{%- endmacro %}

{% macro func_remove_indices() -%}
CREATE OR REPLACE FUNCTION remove_indices(ARRAY_TO_PRUNE ARRAY, DUPLICATE_INDICES_ASCENDING ARRAY)
  RETURNS ARRAY
  LANGUAGE JAVASCRIPT
  AS $$
    for (var i = DUPLICATE_INDICES_ASCENDING.length - 1; i >= 0; i--) {
        ARRAY_TO_PRUNE.splice(DUPLICATE_INDICES_ASCENDING[i], 1);
    }

    return ARRAY_TO_PRUNE;
  $$;
{%- endmacro %}


{% macro func_get_array_position() -%}
  CREATE OR REPLACE FUNCTION get_array_position(STRING_TO_LOOKUP BINARY, ARRAY_FOR_LOOKUP ARRAY)
    RETURNS DOUBLE
    LANGUAGE JAVASCRIPT
    AS $$
      return ARRAY_FOR_LOOKUP.indexOf(STRING_TO_LOOKUP);
    $$;
{%- endmacro %}

{% macro func_get_item_at_index() -%}
CREATE OR REPLACE FUNCTION get_item_at_index(ARRAY_INPUT ARRAY, INDEX DOUBLE)
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  AS $$
    return ARRAY_INPUT[INDEX];
  $$;
{%- endmacro %}
