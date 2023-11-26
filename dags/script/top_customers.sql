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
                                   from dbt_detailed.sat_purchase_details)
                          where rn = 1),
     best_category as (select innn.customer_id, innn.category_name
                       from (select *,
                                    row_number() over (partition by inn.key order by inn.cat_sum desc) as rn
                             from (select hc.customer_id                                  as customer_id,
                                          scd.category_name                               as category_name,
                                          sum(spd.product_price * spd.product_count)      as cat_sum,
                                          concat(toString(hc.customer_id),
                                                 toString(scd.category_name)) as key
                                   from dbt_detailed.link_purchase_customer lpc
                                            join dbt_detailed.hub_customers hc on lpc.customer_pk = hc.customer_pk
                                            join dbt_detailed.link_purchase_product lpp
                                                 on lpc.purchase_pk = lpp.purchase_pk
                                            join dbt_detailed.link_product_category lpca
                                                 on lpp.product_pk = lpca.product_pk
                                            join actual_cat_sat scd on lpca.category_pk = scd.category_pk
                                            join actual_purch_sat spd on lpc.purchase_pk = spd.purchase_pk
                                   where toDate(spd.purchase_date) != toDate(now())
                                   group by hc.customer_id, scd.category_name) inn) innn
                       where innn.rn = 1)

select now()            as created_at,
       inn.customer_id,
       inn.customer_gmv,
       bc.category_name as customer_category,
       multiIf(customer_gmv / (sum(customer_gmv) over () + 0.001) > 0.95, '5', customer_gmv / (sum(customer_gmv) over () + 0.001) > 0.9, '10',
               customer_gmv / (sum(customer_gmv) over () + 0.001) > 0.75, '25', customer_gmv / (sum(customer_gmv) over () + 0.001) > 0.5, '50',
               '50+')   as customer_group
from (select hc.customer_id                             as customer_id,
             sum(spd.product_price * spd.product_count) as customer_gmv
      from dbt_detailed.link_purchase_customer lpc
               join dbt_detailed.hub_customers hc on lpc.customer_pk = hc.customer_pk
               join dbt_detailed.link_purchase_product lpp on lpc.purchase_pk = lpp.purchase_pk
               join dbt_detailed.link_product_category lpca on lpp.product_pk = lpca.product_pk
               join actual_cat_sat scd on lpca.category_pk = scd.category_pk
               join actual_purch_sat spd on lpc.purchase_pk = spd.purchase_pk
      where toDate(spd.purchase_date) != toDate(now())
      group by hc.customer_id) inn
         join best_category bc on inn.customer_id = bc.customer_id