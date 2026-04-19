{% macro generate_schema_name(custom_schema_name, node) -%}

    {#-
      Default dbt behaviour: final_schema = profile_schema ~ "_" ~ custom_schema_name
      This produces "public_staging" and "public_marts" — not what we want.

      This macro overrides that behaviour:
      - If the model defines a custom schema (staging, marts), use ONLY that value.
      - If no custom schema is defined, fall back to the profile schema (public).

      Result: dbt writes to exactly "staging" and "marts" with no prefix.
    -#}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}