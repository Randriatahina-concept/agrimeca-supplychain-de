{# Nom du schéma d'un modèle : celui de son dossier (+schema) s'il en a un,
   sinon celui du profil (silver). Remplace le comportement par défaut de dbt,
   qui produirait « silver_gold ». #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}