select pi.product_id,
       pi.product_count,
       pi.product_price,
       pu.purchase_id,
       pu.purchase_date,
       pu.purchase_payment_type,
       st.store_id,
       st.store_name,
       st.store_country,
       st.store_city,
       st.store_address,
       cu.customer_id,
       cu.customer_fname,
       cu.customer_lname,
       cu.customer_gender,
       cu.customer_phone
from {{ source('system', 'purchase_items') }} as pi
join {{ source('system', 'purchases') }} as pu
on pi.purchase_id = pu.purchase_id
    join {{ source('system', 'stores') }} as st
    on pu.store_id = st.store_id
    join {{ source('system', 'customers') }} as cu
    on pu.customer_id = cu.customer_id