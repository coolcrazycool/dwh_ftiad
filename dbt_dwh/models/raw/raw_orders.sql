select del.delivery_id,
       del.product_id,
       del.delivery_date,
       del.product_count,
       st.store_id,
       st.store_name,
       st.store_country,
       st.store_city,
       st.store_address
from {{ source('system', 'deliveries') }} as del
join {{ source('system', 'stores') }} as st
on del.store_id = st.store_id