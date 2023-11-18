{%- set yaml_metadata -%}
source_model: 'raw_purchases'
derived_columns:
  RECORD_SOURCE: '!PURCHASES'
  EFFECTIVE_FROM: 'purchase_date'
hashed_columns:
  purchase_pk: 'purchase_id'
  product_pk: 'product_id'
  store_pk: 'store_id'
  customer_pk: 'customer_id'
  link_product_purchase_pk:
    - 'purchase_id'
    - 'product_id'
  link_customer_purchase_pk:
    - 'purchase_id'
    - 'customer_id'
  link_store_purchase_pk:
    - 'purchase_id'
    - 'store_id'
  store_hashdiff:
    is_hashdiff: true
    columns:
      - 'store_id'
      - 'store_name'
      - 'store_country'
      - 'store_city'
      - 'store_address'
  customer_hashdiff:
    is_hashdiff: true
    columns:
      - 'customer_id'
      - 'customer_fname'
      - 'customer_lname'
      - 'customer_gender'
      - 'customer_phone'
  purchase_hashdiff:
    is_hashdiff: true
    columns:
      - 'purchase_id'
      - 'purchase_payment_type'
      - 'purchase_date'
      - 'product_count'
      - 'product_price'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS (
{{ automate_dv.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }})

SELECT *,
       ('{{ var('load_date') }}')::DATE AS LOAD_DATE
FROM staging