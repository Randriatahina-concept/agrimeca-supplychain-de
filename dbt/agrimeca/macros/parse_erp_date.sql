{# Convertit une date ERP en DATE, quel que soit son format :
   jj/mm/aaaa, epoch en millisecondes (nombre) ou ISO aaaa-mm-jj #}
{% macro parse_erp_date(json_path) %}
    case
        when {{ json_path }}::string rlike '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
            then to_date({{ json_path }}::string, 'dd/MM/yyyy')
        when {{ json_path }}::string rlike '^[0-9]+$'
            then to_date(timestamp_millis({{ json_path }}::bigint))
        else to_date({{ json_path }}::string)
    end
{% endmacro %}