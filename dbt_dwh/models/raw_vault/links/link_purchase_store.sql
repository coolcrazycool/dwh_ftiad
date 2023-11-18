{%- set source_model = "v_stg_purchases" -%}
{%- set src_pk = "link_store_purchase_pk" -%}
{%- set src_fk = ["purchase_pk", "store_pk"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}