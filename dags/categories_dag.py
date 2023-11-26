import pendulum
import datetime

from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

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
    dag_id="categories",
    default_args=DEFAULT_TASK_ARGS
)

cat = ClickHouseOperator(
        task_id="categories",
        start_date=(pendulum.datetime(2023, 1, 1, 0, 0)),
        dag=dag,
        sql=(
            '''
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
            ''',
        ),
        query_id='{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}',
        clickhouse_conn_id='CLICKHOUSE_DEFAULT',
    )