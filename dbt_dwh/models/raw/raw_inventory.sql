select pr.product_id,
       pr.product_name,
       pr.product_picture_url,
       pr.product_description,
       pr.product_restriction,
       cat.category_id,
       cat.category_name,
       man.manufacturer_id,
       man.manufacturer_name,
       man.manufacturer_legal_entity,
       prch.new_price as product_price,
       prch.price_change_ts as price_change_ts
from {{ source('system', 'products') }} as pr
join {{ source('system', 'categories') }} as cat
on pr.category_id = cat.category_id
    join {{ source('system', 'manufacturers') }} as man
    on pr.manufacturer_id = man.manufacturer_id
join {{ source('system', 'price_change') }} as prch
on pr.product_id = prch.product_id