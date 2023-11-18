{%- set source_model = "v_stg_inventory" -%}
{%- set src_pk = "product_pk" -%}
{%- set src_hashdiff = "product_hashdiff" -%}
{%- set src_payload = ["product_name", "product_picture_url", "product_description", "product_restriction", "product_price"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}