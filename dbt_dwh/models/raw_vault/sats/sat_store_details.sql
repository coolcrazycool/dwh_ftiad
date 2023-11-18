{%- set source_model = "v_stg_purchases" -%}
{%- set src_pk = "store_pk" -%}
{%- set src_hashdiff = "store_hashdiff" -%}
{%- set src_payload = ["store_name", "store_country", "store_city", "store_address"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}