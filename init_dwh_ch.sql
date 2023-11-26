CREATE DATABASE IF NOT EXISTS "dbt_detailed";
CREATE DATABASE IF NOT EXISTS "presentation";

create table IF NOT EXISTS dbt_detailed.hub_categories
(
    category_pk   bytea,
    category_id   integer,
    load_date     date,
    record_source text
)
    ENGINE = MergeTree
        ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.hub_customers
(
    customer_pk   bytea,
    customer_id   integer,
    load_date     date,
    record_source text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.hub_deliveries
(
    delivery_pk   bytea,
    delivery_id   bigint,
    load_date     date,
    record_source text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.hub_manufacturers
(
    manufacturer_pk bytea,
    manufacturer_id integer,
    load_date       date,
    record_source   text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.hub_products
(
    product_pk    bytea,
    product_id    bigint,
    load_date     date,
    record_source text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.hub_purchases
(
    purchase_pk   bytea,
    purchase_id   integer,
    load_date     date,
    record_source text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.hub_stores
(
    store_pk      bytea,
    store_id      integer,
    load_date     date,
    record_source text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.link_product_category
(
    link_product_category_pk bytea,
    category_pk              bytea,
    product_pk               bytea,
    load_date                date,
    record_source            text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.link_product_delivery
(
    link_product_delivery_pk bytea,
    delivery_pk              bytea,
    product_pk               bytea,
    load_date                date,
    record_source            text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.link_product_manufacture
(
    link_product_manufacture_pk bytea,
    manufacturer_pk             bytea,
    product_pk                  bytea,
    load_date                   date,
    record_source               text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.link_purchase_customer
(
    link_customer_purchase_pk bytea,
    purchase_pk               bytea,
    customer_pk               bytea,
    load_date                 date,
    record_source             text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.link_purchase_product
(
    link_product_purchase_pk bytea,
    purchase_pk              bytea,
    product_pk               bytea,
    load_date                date,
    record_source            text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.link_purchase_store
(
    link_store_purchase_pk bytea,
    purchase_pk            bytea,
    store_pk               bytea,
    load_date              date,
    record_source          text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.sat_category_details
(
    category_pk       bytea,
    category_hashdiff bytea,
    category_name     varchar(100),
    effective_from    timestamp,
    load_date         date,
    record_source     text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.sat_customer_details
(
    customer_pk       bytea,
    customer_hashdiff bytea,
    customer_fname    varchar(100),
    customer_lname    varchar(100),
    customer_gender   varchar(100),
    customer_phone    varchar(100),
    effective_from    timestamp,
    load_date         date,
    record_source     text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.sat_delivery_details
(
    delivery_pk       bytea,
    delivery_hashdiff bytea,
    delivery_date     date,
    product_count     integer,
    effective_from    date,
    load_date         date,
    record_source     text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.sat_manufacture_details
(
    manufacturer_pk           bytea,
    manufacturer_hashdiff     bytea,
    manufacturer_name         varchar(100),
    manufacturer_legal_entity varchar(100),
    effective_from            timestamp,
    load_date                 date,
    record_source             text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.sat_product_details
(
    product_pk          bytea,
    product_hashdiff    bytea,
    product_name        varchar(255),
    product_picture_url varchar(255),
    product_description varchar(255),
    product_restriction integer,
    product_price       numeric(9, 2),
    effective_from      timestamp,
    load_date           date,
    record_source       text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.sat_purchase_details
(
    purchase_pk           bytea,
    purchase_hashdiff     bytea,
    purchase_date         timestamp,
    purchase_payment_type varchar(100),
    product_count         bigint,
    product_price         numeric(9, 2),
    effective_from        timestamp,
    load_date             date,
    record_source         text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS dbt_detailed.sat_store_details
(
    store_pk       bytea,
    store_hashdiff bytea,
    store_name     varchar(255),
    store_country  varchar(255),
    store_city     varchar(255),
    store_address  varchar(255),
    effective_from timestamp,
    load_date      date,
    record_source  text
)
    engine = MergeTree ORDER BY load_date
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS presentation.top_customers
(
    created_at        timestamp,
    customer_id       Int32,
    customer_gmv      DECIMAL(9, 2),
    customer_category varchar(255),
    customer_group    varchar(5)
)
    engine = MergeTree ORDER BY created_at
        SETTINGS index_granularity = 8192;

create table IF NOT EXISTS presentation.categories
(
    created_at        timestamp,
    business_date     DATE,
    customer_category varchar(255),
    customer_gmv      DECIMAL(9, 2)
)
    engine = MergeTree ORDER BY created_at
        SETTINGS index_granularity = 8192;

