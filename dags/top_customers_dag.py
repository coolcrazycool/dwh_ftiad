import pendulum
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@postgresql_dwh/postgres')


def top_customers():
    view = pd.read_sql(
        '''
            with actual_cat_sat as (select *
                        from (
                                 select *,
                                        row_number() over (partition by category_pk order by effective_from desc) as rn
                                 from dbt_detailed.sat_category_details) as "*2"
                        where rn = 1),
                 actual_purch_sat as (select *
                                      from (
                                               select *,
                                                      row_number() over (partition by purchase_pk order by effective_from desc) as rn
                                               from dbt_detailed.sat_purchase_details) as "*3"
                                      where rn = 1),
                 best_category as (select innn.customer_id, innn.category_name
                                   from (select *,
                                                row_number() over (partition by inn.key order by inn.cat_sum desc) as rn
                                         from (select hc.customer_id                                  as customer_id,
                                                      scd.category_name                               as category_name,
                                                      sum(spd.product_price * spd.product_count)      as cat_sum,
                                                      concat(hc.customer_id::text,
                                                             scd.category_name::text) as key
                                               from dbt_detailed.link_purchase_customer lpc
                                                        join dbt_detailed.hub_customers hc on lpc.customer_pk = hc.customer_pk
                                                        join dbt_detailed.link_purchase_product lpp
                                                             on lpc.purchase_pk = lpp.purchase_pk
                                                        join dbt_detailed.link_product_category lpca
                                                             on lpp.product_pk = lpca.product_pk
                                                        join actual_cat_sat scd on lpca.category_pk = scd.category_pk
                                                        join actual_purch_sat spd on lpc.purchase_pk = spd.purchase_pk
                                               where DATE(spd.purchase_date) != DATE(now())
                                               group by hc.customer_id, scd.category_name) inn) innn
                                   where innn.rn = 1)

            select now()            as created_at,
                   inn.customer_id,
                   inn.customer_gmv,
                   bc.category_name as customer_category,
                   CASE when customer_gmv / (sum(customer_gmv) over () + 0.001) > 0.95 THEN '5'
                        when customer_gmv / (sum(customer_gmv) over () + 0.001) > 0.9 THEN '10'
                        when customer_gmv / (sum(customer_gmv) over () + 0.001) > 0.75 THEN '25'
                        when customer_gmv / (sum(customer_gmv) over () + 0.001) > 0.5 THEN '50'
                    ELSE '50+' END as customer_group
            from (select hc.customer_id                             as customer_id,
                         sum(spd.product_price * spd.product_count) as customer_gmv
                  from dbt_detailed.link_purchase_customer lpc
                           join dbt_detailed.hub_customers hc on lpc.customer_pk = hc.customer_pk
                           join dbt_detailed.link_purchase_product lpp on lpc.purchase_pk = lpp.purchase_pk
                           join dbt_detailed.link_product_category lpca on lpp.product_pk = lpca.product_pk
                           join actual_cat_sat scd on lpca.category_pk = scd.category_pk
                           join actual_purch_sat spd on lpc.purchase_pk = spd.purchase_pk
                  where DATE(spd.purchase_date) != DATE(now())
                  group by hc.customer_id) inn
                     join best_category bc on inn.customer_id = bc.customer_id
            ''',
        engine
    )
    view.to_sql('categories', engine, if_exists="append", schema='presentation', index=False)

DEFAULT_TASK_ARGS = {
    "owner": "airflow",
    "max_active_runs": 1,
    "start_date": datetime.datetime(2023, 1, 1),
    "retries": 3,
    "retry_delay": 1800
}

dag = DAG(
    max_active_runs=1,
    schedule_interval="5 0 * * *",
    dagrun_timeout=datetime.timedelta(seconds=36000),
    catchup=False,
    dag_id="top_customers",
    default_args=DEFAULT_TASK_ARGS
)

top = PythonOperator(
    python_callable=top_customers,
    dag=dag,
    task_id='top_customers'
)