{%- set source_model = "v_stg_purchases" -%}
{%- set src_pk = "store_pk" -%}
{%- set src_nk = "store_id" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}