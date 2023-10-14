CREATE SCHEMA IF NOT EXISTS "system";

CREATE TABLE IF NOT EXISTS "system"."manufacturers"
(
    "manufacturer_id"   SERIAL,
    "manufacturer_name" varchar(100) NOT NULL,
    CONSTRAINT "pk_manufacturers" PRIMARY KEY ("manufacturer_id")
);

CREATE TABLE IF NOT EXISTS "system"."categories"
(
    "category_id"   SERIAL,
    "category_name" varchar(100) NOT NULL,
    CONSTRAINT "pk_categories" PRIMARY KEY ("category_id")
);

CREATE TABLE IF NOT EXISTS "system"."products"
(
    "category_id"     BIGINT,
    "manufacturer_id" BIGINT,
    "product_id"      SERIAL,
    "product_name"    varchar(255) NOT NULL,
    CONSTRAINT "pk_products" PRIMARY KEY ("product_id"),
    CONSTRAINT "fk_categories" FOREIGN KEY ("category_id") REFERENCES "system"."categories" ("category_id"),
    CONSTRAINT "fk_manufacturers" FOREIGN KEY ("manufacturer_id") REFERENCES "system"."manufacturers" ("manufacturer_id")
);

CREATE TABLE IF NOT EXISTS "system"."stores"
(
    "store_id"   SERIAL,
    "store_name" varchar(255) NOT NULL,
    CONSTRAINT "pk_stores" PRIMARY KEY ("store_id")
);

CREATE TABLE IF NOT EXISTS "system"."customers"
(
    "customer_id"    SERIAL,
    "customer_fname" varchar(100) NOT NULL,
    "customer_lname" varchar(100) NOT NULL,
    CONSTRAINT "pk_customers" PRIMARY KEY ("customer_id")
);

CREATE TABLE IF NOT EXISTS "system"."price_change"
(
    "product_id"      BIGINT        NOT NULL,
    "price_change_ts" TIMESTAMP     NOT NULL,
    "new_price"       NUMERIC(9, 2) NOT NULL,
    CONSTRAINT "fk_products" FOREIGN KEY ("product_id") REFERENCES system.products ("product_id")
);

CREATE TABLE IF NOT EXISTS "system"."deliveries"
(
    "store_id"      BIGINT  NOT NULL,
    "product_id"    BIGINT  NOT NULL,
    "delivery_date" DATE    NOT NULL,
    "product_count" INTEGER NOT NULL,
    CONSTRAINT "fk_stores" FOREIGN KEY ("store_id") REFERENCES "system"."stores" ("store_id"),
    CONSTRAINT "fk_products" FOREIGN KEY ("product_id") REFERENCES "system"."products" ("product_id")
);

CREATE TABLE IF NOT EXISTS "system"."purchases"
(
    "store_id"      BIGINT    NOT NULL,
    "customer_id"   BIGINT    NOT NULL,
    "purchase_id"   SERIAL,
    "purchase_date" TIMESTAMP NOT NULL,
    CONSTRAINT "pk_purchases" PRIMARY KEY ("purchase_id"),
    CONSTRAINT "fk_stores" FOREIGN KEY ("store_id") REFERENCES "system"."stores" ("store_id"),
    CONSTRAINT "fk_customers" FOREIGN KEY ("customer_id") REFERENCES "system"."customers" ("customer_id")
);

CREATE TABLE IF NOT EXISTS "system"."purchase_items"
(
    "product_id"    BIGINT        NOT NULL,
    "purchase_id"   BIGINT        NOT NULL,
    "product_count" BIGINT        NOT NULL,
    "product_price" NUMERIC(9, 2) NOT NULL,
    CONSTRAINT "fk_products" FOREIGN KEY ("product_id") REFERENCES "system"."products" ("product_id"),
    CONSTRAINT "fk_purchases" FOREIGN KEY ("purchase_id") REFERENCES "system"."purchases" ("purchase_id")
);

CREATE VIEW "system"."gmv" as
(with price_change_full as (select product_id,
                                   price_change_ts,
                                   lead(price_change_ts)
                                   over (partition by product_id order by price_change_ts) as next_price_change_ts,
                                   new_price
                            from system.price_change),
      delivery_with_prices as (select d.store_id, pr.category_id, pc.new_price * d.product_count as sum_price
                               from system.deliveries d
                                        join price_change_full pc on d.product_id = pc.product_id and
                                                                     d.delivery_date::timestamp between pc.price_change_ts and pc.next_price_change_ts
                                        join system.products pr on d.product_id = pr.product_id)

select un.store_id, un.category_id, sum(un.sum_price) as sales_sum
from (select pu.store_id, pr.category_id, pi.product_price * pi.product_count as sum_price
      from system.purchase_items pi
               join system.products pr on pi.product_id = pr.product_id
               join system.purchases pu on pi.purchase_id = pu.purchase_id
      union ALL
      select dwp.store_id, dwp.category_id, dwp.sum_price
      from delivery_with_prices dwp) un
group by un.category_id, un.store_id);