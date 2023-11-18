{%- set yaml_metadata -%}
source_model: 'raw_inventory'
derived_columns:
  RECORD_SOURCE: '!DELIVERIES'
  EFFECTIVE_FROM: 'price_change_ts'
hashed_columns:
  product_pk: 'product_id'
  category_pk: 'category_id'
  manufacturer_pk: 'manufacturer_id'
  link_product_manufacture_pk:
    - 'manufacturer_id'
    - 'product_id'
  link_product_category_pk:
    - 'category_id'
    - 'product_id'
  product_hashdiff:
    is_hashdiff: true
    columns:
      - 'product_id'
      - 'product_name'
      - 'product_picture_url'
      - 'product_description'
      - 'product_restriction'
      - 'product_price'
  manufacturer_hashdiff:
    is_hashdiff: true
    columns:
      - 'manufacturer_id'
      - 'manufacturer_name'
      - 'manufacturer_legal_entity'
  category_hashdiff:
    is_hashdiff: true
    columns:
      - 'category_id'
      - 'category_name'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS ({{ automate_dv.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }})

SELECT *,
       ('{{ var('load_date') }}')::DATE AS LOAD_DATE
FROM staging