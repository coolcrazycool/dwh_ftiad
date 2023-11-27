import pendulum
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@postgresql_dwh/postgres')


def update_categories():
    with engine.connect() as con:
        con.execute("""DELETE FROM presentation.categories WHERE business_date = DATE(now())-1""")
    view = pd.read_sql(
        '''with actual_cat_sat as (select *
                                            from (
                                                     select *,
                                                            row_number() over (partition by category_pk order by effective_from desc) as rn
                                                     from dbt_detailed.sat_category_details) as "*2"
                                            where rn = 1),
                         actual_purch_sat as (select *
                                              from (
                                                       select *,
                                                              row_number() over (partition by purchase_pk order by effective_from desc) as rn
                                                       from dbt_detailed.sat_purchase_details
                                                       where DATE(purchase_date) = DATE(now())-1) as "*3"
                                              where rn = 1)

                    select now()                                                   as created_at,
                           DATE(now())-1                         as business_date,
                           scd.category_name                                       as category_name,
                           coalesce(sum(spd.product_price * spd.product_count), 0) as category_gmv
                    from dbt_detailed.link_product_category lpca
                             left join dbt_detailed.link_purchase_product lpp on lpp.product_pk = lpca.product_pk
                             left join actual_cat_sat scd on lpca.category_pk = scd.category_pk
                             left join actual_purch_sat spd on lpp.purchase_pk = spd.purchase_pk
                    group by DATE(spd.purchase_date), scd.category_name
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
    dag_id="categories",
    default_args=DEFAULT_TASK_ARGS
)

cat = PythonOperator(
    python_callable=update_categories,
    dag=dag,
    task_id='categories'
)
