from confluent_kafka import Consumer
import hashlib
from datetime import date
import json
import pandas as pd
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import create_engine
import time
import datetime

# engine = create_engine('postgresql://postgres:postgres@localhost:5434/postgres')
engine = create_engine('clickhouse://clickhouse:clickhouse@localhost:8123')


def insert_category(before, after):
    df = pd.DataFrame.from_dict({
        "category_pk": [hashlib.md5(str(after["category_id"]).encode()).hexdigest()],
        "category_hashdiff": [
            hashlib.md5(
                (str(after["category_id"]) + str(after["category_name"])).encode()).hexdigest()],
        "category_name": [after["category_name"]],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["3NF"]
    })
    df.to_sql('sat_category_details', engine, if_exists="append", schema='dbt_detailed',
              index=False)
    if before is None:
        df = pd.DataFrame.from_dict({
            "category_pk": [hashlib.md5(str(after["category_id"]).encode()).hexdigest()],
            "category_id": [after["category_id"]],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('hub_categories', engine, if_exists="append", schema='dbt_detailed', index=False)


def insert_manufacture(before, after):
    df = pd.DataFrame.from_dict({
        "manufacturer_pk": [hashlib.md5(str(after["manufacturer_id"]).encode()).hexdigest()],
        "manufacturer_hashdiff": [
            hashlib.md5(
                (str(after["manufacturer_id"]) + str(after["manufacturer_name"]) + str(
                    after["manufacturer_legal_entity"])).encode()).hexdigest()],
        "manufacturer_name": [after["manufacturer_name"]],
        "manufacturer_legal_entity": [after["manufacturer_legal_entity"]],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["3NF"]
    })
    df.to_sql('sat_manufacture_details', engine, if_exists="append", schema='dbt_detailed',
              index=False)
    if before is None:
        df = pd.DataFrame.from_dict({
            "manufacturer_pk": [hashlib.md5(str(after["manufacturer_id"]).encode()).hexdigest()],
            "manufacturer_id": [after["manufacturer_id"]],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('hub_manufacturers', engine, if_exists="append", schema='dbt_detailed', index=False)


def insert_product(before, after):
    df = pd.DataFrame.from_dict({
        "product_pk": [hashlib.md5(str(after["product_id"]).encode()).hexdigest()],
        "product_hashdiff": [
            hashlib.md5(
                (str(after["product_id"]) + str(after["product_name"]) + str(after["product_picture_url"]) + str(
                    after["product_description"]) + str(after["product_restriction"])).encode()).hexdigest()],
        "product_name": [after["product_name"]],
        "product_picture_url": [after["product_picture_url"]],
        "product_description": [after["product_description"]],
        "product_restriction": [after["product_restriction"]],
        "product_price": [None],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["3NF"]
    })
    df.to_sql('sat_product_details', engine, if_exists="append", schema='dbt_detailed',
              index=False)
    if before is None:
        df = pd.DataFrame.from_dict({
            "product_pk": [hashlib.md5(str(after["product_id"]).encode()).hexdigest()],
            "product_id": [after["product_id"]],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('hub_products', engine, if_exists="append", schema='dbt_detailed', index=False)
        df = pd.DataFrame.from_dict({
            "link_product_category_pk": [hashlib.md5((str(after["product_id"]) + str(after["category_id"])).encode()).hexdigest()],
            "product_pk": [hashlib.md5(str(after["product_id"]).encode()).hexdigest()],
            "category_pk": [hashlib.md5(str(after["category_id"]).encode()).hexdigest()],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('link_product_category', engine, if_exists="append", schema='dbt_detailed', index=False)
        df = pd.DataFrame.from_dict({
            "link_product_manufacture_pk": [
                hashlib.md5((str(after["product_id"]) + str(after["category_id"])).encode()).hexdigest()],
            "product_pk": [hashlib.md5(str(after["product_id"]).encode()).hexdigest()],
            "manufacturer_pk": [hashlib.md5(str(after["manufacturer_id"]).encode()).hexdigest()],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('link_product_manufacture', engine, if_exists="append", schema='dbt_detailed', index=False)


def insert_store(before, after):
    df = pd.DataFrame.from_dict({
        "store_pk": [hashlib.md5(str(after["store_id"]).encode()).hexdigest()],
        "store_hashdiff": [
            hashlib.md5(
                (str(after["store_id"]) + str(after["store_name"]) + str(after["store_country"]) + str(
                    after["store_city"]) + str(after["store_address"])).encode()).hexdigest()],
        "store_name": [after["store_name"]],
        "store_country": [after["store_country"]],
        "store_city": [after["store_city"]],
        "store_address": [after["store_address"]],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["3NF"]
    })
    df.to_sql('sat_store_details', engine, if_exists="append", schema='dbt_detailed',
              index=False)
    if before is None:
        df = pd.DataFrame.from_dict({
            "store_pk": [hashlib.md5(str(after["store_id"]).encode()).hexdigest()],
            "store_id": [after["store_id"]],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('hub_stores', engine, if_exists="append", schema='dbt_detailed', index=False)


def insert_customer(before, after):
    df = pd.DataFrame.from_dict({
        "customer_pk": [hashlib.md5(str(after["customer_id"]).encode()).hexdigest()],
        "customer_hashdiff": [
            hashlib.md5(
                (str(after["customer_id"]) + str(after["customer_fname"]) + str(after["customer_lname"]) + str(
                    after["customer_gender"]) + str(after["customer_phone"])).encode()).hexdigest()],
        "customer_fname": [after["customer_fname"]],
        "customer_lname": [after["customer_lname"]],
        "customer_gender": [after["customer_gender"]],
        "customer_phone": [after["customer_phone"]],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["3NF"]
    })
    df.to_sql('sat_customer_details', engine, if_exists="append", schema='dbt_detailed',
              index=False)
    if before is None:
        df = pd.DataFrame.from_dict({
            "customer_pk": [hashlib.md5(str(after["customer_id"]).encode()).hexdigest()],
            "customer_id": [after["customer_id"]],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('hub_customers', engine, if_exists="append", schema='dbt_detailed', index=False)


def insert_purchase(after):
    p_pk = hashlib.md5(str(after["purchase_id"]).encode()).hexdigest()
    old = {
        "purchase_pk": None,
        "purchase_hashdiff": None,
        "purchase_date": None,
        "purchase_payment_type": None,
        "product_count": None,
        "product_price": None,
        "effective_from": None,
        "load_date": None,
        "record_source": None
    } if len(val := pd.read_sql(
        f"select * from dbt_detailed.sat_purchase_details where purchase_pk = '{p_pk}' order by effective_from limit 1",
        engine).to_dict('records')) == 0 else val[0]
    to_create = old["purchase_pk"] is None
    if old["purchase_pk"] is None and "store_id" in after.keys():
        df = pd.DataFrame.from_dict({
            "link_store_purchase_pk": [
                hashlib.md5((str(after["store_id"]) + str(after["purchase_id"])).encode()).hexdigest()],
            "store_pk": [hashlib.md5(str(after["store_id"]).encode()).hexdigest()],
            "purchase_pk": [hashlib.md5(str(after["purchase_id"]).encode()).hexdigest()],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('link_purchase_store', engine, if_exists="append", schema='dbt_detailed', index=False)
        df = pd.DataFrame.from_dict({
            "link_customer_purchase_pk": [
                hashlib.md5((str(after["purchase_id"]) + str(after["customer_id"])).encode()).hexdigest()],
            "purchase_pk": [hashlib.md5(str(after["purchase_id"]).encode()).hexdigest()],
            "customer_pk": [hashlib.md5(str(after["customer_id"]).encode()).hexdigest()],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('link_purchase_customer', engine, if_exists="append", schema='dbt_detailed', index=False)
    if (old["product_count"] is None or old["product_count"] == 0) and "product_id" in after.keys():
        df = pd.DataFrame.from_dict({
            "link_product_purchase_pk": [
                hashlib.md5((str(after["purchase_id"]) + str(after["product_id"])).encode()).hexdigest()],
            "purchase_pk": [hashlib.md5(str(after["purchase_id"]).encode()).hexdigest()],
            "product_pk": [hashlib.md5(str(after["product_id"]).encode()).hexdigest()],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('link_purchase_product', engine, if_exists="append", schema='dbt_detailed', index=False)
    old.update({(k, v) for k, v in after.items() if
                k in ['product_count', 'product_price', 'purchase_id', 'purchase_date', 'purchase_payment_type']})
    df = pd.DataFrame.from_dict({
        "purchase_pk": [p_pk],
        "purchase_hashdiff": [
            hashlib.md5(
                (str(old["purchase_id"]) + str(old["purchase_date"]) + str(old["purchase_payment_type"]) + str(
                    old["product_count"]) + str(old["product_price"])).encode()).hexdigest()],
        "purchase_date": [datetime.datetime.fromtimestamp(val//1000000) if isinstance(val := old["purchase_date"], int) else old["purchase_date"]],
        "purchase_payment_type": [old["purchase_payment_type"]],
        "product_count": [old["product_count"]],
        "product_price": [old["product_price"]],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["3NF"]
    })
    df.to_sql('sat_purchase_details', engine, if_exists="append", schema='dbt_detailed',
              index=False)
    if to_create:
        df = pd.DataFrame.from_dict({
            "purchase_pk": [hashlib.md5(str(after["purchase_id"]).encode()).hexdigest()],
            "purchase_id": [after["purchase_id"]],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('hub_purchases', engine, if_exists="append", schema='dbt_detailed', index=False)


def insert_delivery(before, after):
    df = pd.DataFrame.from_dict({
        "delivery_pk": [hashlib.md5(str(after["delivery_id"]).encode()).hexdigest()],
        "delivery_hashdiff": [
            hashlib.md5(
                (str(after["delivery_id"]) + str(after["delivery_date"]) + str(
                    after["product_count"])).encode()).hexdigest()],
        "delivery_date": [datetime.datetime.utcfromtimestamp(0) + datetime.timedelta(after["delivery_date"])],
        "product_count": [after["product_count"]],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["3NF"]
    })
    df.to_sql('sat_delivery_details', engine, if_exists="append", schema='dbt_detailed',
              index=False)
    if before is None:
        df = pd.DataFrame.from_dict({
            "delivery_pk": [hashlib.md5(str(after["delivery_id"]).encode()).hexdigest()],
            "delivery_id": [after["delivery_id"]],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('hub_deliveries', engine, if_exists="append", schema='dbt_detailed', index=False)
        df = pd.DataFrame.from_dict({
            "link_product_delivery_pk": [
                hashlib.md5((str(after["product_id"]) + str(after["delivery_id"])).encode()).hexdigest()],
            "product_pk": [hashlib.md5(str(after["product_id"]).encode()).hexdigest()],
            "delivery_pk": [hashlib.md5(str(after["delivery_id"]).encode()).hexdigest()],
            "load_date": [date.today()],
            "record_source": ["3NF"]
        })
        df.to_sql('link_product_delivery', engine, if_exists="append", schema='dbt_detailed', index=False)


def insert_price_change(after):
    print(after)
    p_pk = hashlib.md5(str(after["product_id"]).encode()).hexdigest()
    old = pd.read_sql(
        f"select * from dbt_detailed.sat_product_details where product_pk = '{p_pk}' order by effective_from limit 1",
        engine).to_dict('records')[0]
    df = pd.DataFrame.from_dict({
        "product_pk": [p_pk],
        "product_hashdiff": [
            hashlib.md5(
                (str(after["product_id"]) + str(old["product_name"]) + str(old["product_picture_url"]) + str(
                    old["product_description"]) + str(old["product_restriction"]) + str(
                    after["new_price"])).encode()).hexdigest()],
        "product_name": [old["product_name"]],
        "product_picture_url": [old["product_picture_url"]],
        "product_description": [old["product_description"]],
        "product_restriction": [old["product_restriction"]],
        "product_price": [after["new_price"]],
        "effective_from": [datetime.datetime.fromtimestamp(after['price_change_ts']//1000000)],
        "load_date": [date.today()],
        "record_source": ["3NF"]
    })
    df.to_sql('sat_product_details', engine, if_exists="append", schema='dbt_detailed',
              index=False)


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                print(f"ERROR: {msg.value()}")
            else:
                print(msg)
                print(msg.topic())
                print(msg.value())
                print(msg.value().decode('utf-8'))
                print(msg.key())
                value = json.loads(msg.value().decode('utf-8'))['payload']
                before = value["before"]
                after = value["after"]
                if msg.topic() == "postgres.system.categories":
                    insert_category(before, after)
                elif msg.topic() == "postgres.system.manufacturers":
                    insert_manufacture(before, after)
                elif msg.topic() == "postgres.system.products":
                    insert_product(before, after)
                elif msg.topic() == "postgres.system.stores":
                    insert_store(before, after)
                elif msg.topic() == "postgres.system.customers":
                    insert_customer(before, after)
                elif msg.topic() == "postgres.system.purchases":
                    insert_purchase(after)
                elif msg.topic() == "postgres.system.purchase_items":
                    insert_purchase(after)
                elif msg.topic() == "postgres.system.deliveries":
                    insert_delivery(before, after)
                elif msg.topic() == "postgres.system.price_change":
                    insert_price_change(after)
                else:
                    print(msg)
                    print(msg.topic())
                    print(msg.value())
                    print(msg.key())
                    print("unknown")

    finally:
        consumer.close()


if __name__ == "__main__":
    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'dwh_group',
            'enable.auto.commit': 'false',
            'auto.offset.reset': 'latest'}

    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })

    topic_list = [
        "postgres.system.categories",
        "postgres.system.customers",
        "postgres.system.deliveries",
        "postgres.system.manufacturers",
        "postgres.system.price_change",
        "postgres.system.products",
        "postgres.system.purchase_items",
        "postgres.system.purchases",
        "postgres.system.stores"
    ]
    print("Start creating topics")
    admin_client.create_topics([NewTopic(topic, 1, 1) for topic in topic_list])
    time.sleep(5)
    print("Ready to operate")

    consumer = Consumer(conf)
    basic_consume_loop(consumer, topic_list)
