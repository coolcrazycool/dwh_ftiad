from confluent_kafka import Consumer
import hashlib
from datetime import date
import json
import pandas as pd
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import create_engine
import time
import datetime
from functools import reduce

# engine = create_engine('postgresql://postgres:postgres@localhost:5434/postgres')
engine = create_engine('clickhouse://clickhouse:clickhouse@localhost:8123')


def create_pk(data):
    return hashlib.md5(str(data).encode()).hexdigest()


def create_hashdiff(data):
    to_decode = reduce(lambda x, y: x + y, [str(d) for d in data])
    return create_pk(to_decode)


def process_macros(data, after):
    splited = data.split("#")
    print(splited)
    if len(splited) == 1:
        return after[data]
    elif splited[0] == "":
        print("ff")
        return datetime.datetime.utcfromtimestamp(0) + datetime.timedelta(after[splited[1]])
    else:
        return datetime.datetime.fromtimestamp(after[splited[0]] // int(splited[1]))


def compute(before, after, config: dict, schema: str):
    if "hub" in config.keys() and before is None:
        hub_cfg = config["hub"]
        df = pd.DataFrame.from_dict({
            hub_cfg["pk"]["as"]: [create_pk(after[hub_cfg["pk"]["from"]])],
            hub_cfg["id"]["as"]: [after[hub_cfg["id"]["from"]]],
            config["load_date"]["as"]: [
                date.today() if config["load_date"]["from"] is None else config["load_date"]["from"]],
            config["record_source"]["as"]: [config["record_source"]["from"]]
        })
        df.to_sql(hub_cfg["table"], engine, if_exists="append", schema=schema, index=False)
    if "sat" in config.keys():
        sat_cfg = config["sat"]
        p_pk = create_pk(after[sat_cfg["pk"]["from"]])
        if "update" in config["sat"].keys():
            old = pd.read_sql(
                f"select * from {schema}.{sat_cfg['table']} where {sat_cfg['pk']['as']} = '{p_pk}' order by effective_from limit 1",
                engine).to_dict('records')[0]
            old.update({k: after[v] for k, v in sat_cfg["update"].items()})
        else:
            old = {k: process_macros(v, after) if v is not None else v for k, v in sat_cfg["adds"].items()}
        base_dict = {
            sat_cfg["pk"]["as"]: [p_pk],
            sat_cfg["hashdiff"]["as"]:
                [create_hashdiff({k: v for k, v in old.items() if k in sat_cfg["hashdiff"]["from"]})],
            sat_cfg["start_time"]["as"]: [
                date.today() if sat_cfg["start_time"]["from"] is None else process_macros(sat_cfg[
                                                                                              "start_time"]["from"],
                                                                                          after)],
            config["load_date"]["as"]: [
                date.today() if config["load_date"]["from"] is None else config["load_date"]["from"]],
            config["record_source"]["as"]: [config["record_source"]["from"]]
        }
        base_dict.update({k: v for k, v in old.items() if k in list(sat_cfg["adds"].keys())})
        df = pd.DataFrame.from_dict(base_dict)
        df.to_sql(sat_cfg["table"], engine, if_exists="append", schema=schema, index=False)
    if "link" in config.keys() and before is None:
        for link in config["link"]:
            link_cfg = link
            df = pd.DataFrame.from_dict({
                link_cfg["pk"]["as"]: [
                    create_hashdiff({k: v for k, v in after.items() if k in link_cfg["pk"]["from"]})],
                link_cfg["left_pk"]["as"]: [create_pk(after[link_cfg["left_pk"]["from"]])],
                link_cfg["right_pk"]["as"]: [create_pk(after[link_cfg["right_pk"]["from"]])],
                config["load_date"]["as"]: [
                    date.today() if config["load_date"]["from"] is None else config["load_date"]["from"]],
                config["record_source"]["as"]: [config["record_source"]["from"]]
            })
            df.to_sql(link_cfg["table"], engine, if_exists="append", schema=schema, index=False)


def basic_consume_loop(consumer, topics, config):
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                print(f"ERROR: {msg.value()}")
            else:
                value = json.loads(msg.value().decode('utf-8'))['payload']
                before = value["before"]
                after = value["after"]
                compute(before, after, config[msg.topic()], config["schema"])

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
    time.sleep(1)
    print("Ready to operate")
    with open("config.json", "r") as f:
        config = json.loads(f.read())

    print(config["postgres.system.categories"]["sat"]["adds"].keys())

    consumer = Consumer(conf)
    basic_consume_loop(consumer, topic_list, config)
