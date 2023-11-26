DELETE FROM presentation.categories WHERE business_date = date_sub(DAY, 1, toDate(now()));

with actual_cat_sat as (select *
                        from (
                                 select *,
                                        row_number() over (partition by category_pk order by effective_from desc) as rn
                                 from dbt_detailed.sat_category_details)
                        where rn = 1),
     actual_purch_sat as (select *
                          from (
                                   select *,
                                          row_number() over (partition by purchase_pk order by effective_from desc) as rn
                                   from dbt_detailed.sat_purchase_details
                                   where toDate(purchase_date) == date_sub(DAY, 1, toDate(now())))
                          where rn = 1)

select now()                                                   as created_at,
       date_sub(DAY, 1, toDate(now()))                         as business_date,
       scd.category_name                                       as category_name,
       coalesce(sum(spd.product_price * spd.product_count), 0) as category_gmv
from dbt_detailed.link_product_category lpca
         left join dbt_detailed.link_purchase_product lpp on lpp.product_pk = lpca.product_pk
         left join actual_cat_sat scd on lpca.category_pk = scd.category_pk
         left join actual_purch_sat spd on lpp.purchase_pk = spd.purchase_pk
group by toDate(spd.purchase_date), scd.category_name