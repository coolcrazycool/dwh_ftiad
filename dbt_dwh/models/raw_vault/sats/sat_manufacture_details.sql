{%- set source_model = "v_stg_inventory" -%}
{%- set src_pk = "manufacturer_pk" -%}
{%- set src_hashdiff = "manufacturer_hashdiff" -%}
{%- set src_payload = ["manufacturer_name", "manufacturer_legal_entity"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}