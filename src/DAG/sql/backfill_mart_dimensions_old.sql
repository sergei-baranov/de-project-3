-- mart.d_item from staging.user_order_log

-- UPDATE mart.d_item.item_name
WITH
new_data AS (
    SELECT DISTINCT
        "item_id",
        FIRST_VALUE("item_name") OVER (
            PARTITION BY "item_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "item_name"
    FROM
        "staging"."user_order_log"
    ORDER BY "item_id" ASC
)
UPDATE mart.d_item d
SET
    "item_name" = new_data.item_name
FROM new_data
WHERE
    d."item_id" = new_data.item_id
    AND d."item_name" <> new_data.item_name
;
-- INSERT item_id, item_name INTO mart.d_item
WITH
new_data AS (
    SELECT DISTINCT
        "item_id",
        FIRST_VALUE("item_name") OVER (
            PARTITION BY "item_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "item_name"
    FROM
        "staging"."user_order_log"
    ORDER BY "item_id" ASC
)
INSERT INTO mart.d_item ("item_id", "item_name")
SELECT "item_id", "item_name" FROM new_data
WHERE
    new_data."item_id" NOT IN (
        SELECT "item_id" FROM mart.d_item
    )
;

-- mart.d_city from staging.user_order_log

-- UPDATE mart.d_city.city_name
WITH
new_data AS (
    SELECT DISTINCT
        "city_id",
        FIRST_VALUE("city_name") OVER (
            PARTITION BY "city_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "city_name"
    FROM
        "staging"."user_order_log"
    ORDER BY "city_id" ASC
)
UPDATE mart.d_city d
SET
    "city_name" = new_data.city_name
FROM new_data
WHERE
    d."city_id" = new_data.city_id
    AND d."city_name" <> new_data.city_name
;
-- INSERT city_id, city_name INTO mart.d_city
WITH
new_data AS (
    SELECT DISTINCT
        "city_id",
        FIRST_VALUE("city_name") OVER (
            PARTITION BY "city_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "city_name"
    FROM
        "staging"."user_order_log"
    ORDER BY "city_id" ASC
)
INSERT INTO mart.d_city ("city_id", "city_name")
SELECT "city_id", "city_name" FROM new_data
WHERE
    new_data."city_id" NOT IN (
        SELECT "city_id" FROM mart.d_city
    )
;

-- mart.d_category from staging.price_log

-- -- UPDATE mart.d_category.category_name
-- WITH
-- new_data AS (
--     SELECT DISTINCT
--         "category_id",
--         FIRST_VALUE("category_name") OVER (
--             PARTITION BY "category_id" ORDER BY "date_time" ASC
--             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--         ) "category_name"
--     FROM
--         "staging"."price_log"
--     ORDER BY "category_id" ASC
-- )
-- UPDATE mart.d_category d
-- SET
--        "category_name" = new_data.category_name
-- FROM new_data
-- WHERE
--     d."category_id" = new_data.category_id
--     AND d."category_name" <> new_data.category_name
-- ;
-- -- INSERT category_id, category_name INTO mart.d_category
-- WITH
-- new_data AS (
--     SELECT DISTINCT
--         "category_id",
--         FIRST_VALUE("category_name") OVER (
--             PARTITION BY "category_id" ORDER BY "date_time" ASC
--             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--         ) "category_name"
--     FROM
--         "staging"."price_log"
--     ORDER BY "category_id" ASC
-- )
-- INSERT INTO mart.d_category ("category_id", "category_name")
-- SELECT "category_id", "category_name" FROM new_data
-- WHERE
--     new_data."category_id" NOT IN (
--         SELECT "category_id" FROM mart.d_category
--     )
-- ;

-- mart.d_calendar from
-- staging.user_order_log.date_time,
-- staging.user_activity_log.date_time,
-- staging.price_log.date_time,
-- staging.customer_research.date_id.
-- INSERT ONLY

-- INSERT mart.d_calendar.* from staging.user_order_log (.date_time)
WITH
new_data AS (
    SELECT DISTINCT
        cast(EXTRACT(epoch from date_time) as int4) as "date_id",
        EXTRACT('day' FROM "date_time") AS "day_num",
        EXTRACT('month' FROM "date_time") AS "month_num",
        TO_CHAR("date_time", 'Month') AS "month_name",
        EXTRACT('year' FROM "date_time") AS "year_num"
    FROM
        "staging"."user_order_log"
    ORDER BY "date_id" ASC
)
INSERT INTO mart.d_calendar
    ("date_id", "day_num", "month_num", "month_name", "year_num")
SELECT
    "date_id", "day_num", "month_num", "month_name", "year_num"
FROM new_data
WHERE
    new_data."date_id" NOT IN (
        SELECT "date_id" FROM mart.d_calendar
    )
;
-- INSERT mart.d_calendar.* from staging.user_activity_log (.date_time)
WITH
new_data AS (
    SELECT DISTINCT
        cast(EXTRACT(epoch from date_time) as int4) as "date_id",
        EXTRACT('day' FROM "date_time") AS "day_num",
        EXTRACT('month' FROM "date_time") AS "month_num",
        TO_CHAR("date_time", 'Month') AS "month_name",
        EXTRACT('year' FROM "date_time") AS "year_num"
    FROM
        "staging"."user_activity_log"
    ORDER BY "date_id" ASC
)
INSERT INTO mart.d_calendar
    ("date_id", "day_num", "month_num", "month_name", "year_num")
SELECT
    "date_id", "day_num", "month_num", "month_name", "year_num"
FROM new_data
WHERE
    new_data."date_id" NOT IN (
        SELECT "date_id" FROM mart.d_calendar
    )
;
-- INSERT mart.d_calendar.* from staging.price_log (.date_time)
WITH
new_data AS (
    SELECT DISTINCT
        cast(EXTRACT(epoch from date_time) as int4) as "date_id",
        EXTRACT('day' FROM "date_time") AS "day_num",
        EXTRACT('month' FROM "date_time") AS "month_num",
        TO_CHAR("date_time", 'Month') AS "month_name",
        EXTRACT('year' FROM "date_time") AS "year_num"
    FROM
        "staging"."price_log"
    ORDER BY "date_id" ASC
)
INSERT INTO mart.d_calendar
    ("date_id", "day_num", "month_num", "month_name", "year_num")
SELECT
    "date_id", "day_num", "month_num", "month_name", "year_num"
FROM new_data
WHERE
    new_data."date_id" NOT IN (
        SELECT "date_id" FROM mart.d_calendar
    )
;
-- INSERT mart.d_calendar.* from staging.customer_research (.date_id)
WITH
new_data AS (
    SELECT DISTINCT
        cast(EXTRACT(epoch from date_id) as int4) as "date_id",
        EXTRACT('day' FROM "date_id") AS "day_num",
        EXTRACT('month' FROM "date_id") AS "month_num",
        TO_CHAR("date_id", 'Month') AS "month_name",
        EXTRACT('year' FROM "date_id") AS "year_num"
    FROM
        "staging"."customer_research"
    ORDER BY "date_id" ASC
)
INSERT INTO mart.d_calendar
    ("date_id", "day_num", "month_num", "month_name", "year_num")
SELECT
    "date_id", "day_num", "month_num", "month_name", "year_num"
FROM new_data
WHERE
    new_data."date_id" NOT IN (
        SELECT "date_id" FROM mart.d_calendar
    )
;

-- mart.d_customer from staging.user_order_log

-- UPDATE mart.d_customer.first_name, last_name, city_id
WITH
new_data AS (
    SELECT DISTINCT
        "customer_id",
        FIRST_VALUE("first_name") OVER (
            PARTITION BY "customer_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "first_name",
        FIRST_VALUE("last_name") OVER (
            PARTITION BY "customer_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "last_name",
        FIRST_VALUE("city_id") OVER (
            PARTITION BY "customer_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "city_id"
    FROM
        "staging"."user_order_log"
    ORDER BY "customer_id" ASC
)
UPDATE mart.d_customer d
SET
    "first_name" = new_data.first_name,
    "last_name" = new_data.last_name,
    "city_id" = new_data.city_id
FROM new_data
WHERE
    d."customer_id" = new_data.customer_id
    AND (
        d."first_name" <> new_data.first_name
        OR d."last_name" <> new_data.last_name
        OR d."city_id" <> new_data.city_id
    )
;
-- INSERT customer_id, first_name, last_name, city_id INTO mart.d_customer
WITH
new_data AS (
    SELECT DISTINCT
        "customer_id",
        FIRST_VALUE("first_name") OVER (
            PARTITION BY "customer_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "first_name",
        FIRST_VALUE("last_name") OVER (
            PARTITION BY "customer_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "last_name",
        FIRST_VALUE("city_id") OVER (
            PARTITION BY "customer_id" ORDER BY "date_time" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) "city_id"
    FROM
        "staging"."user_order_log"
    ORDER BY "customer_id" ASC
)
INSERT INTO mart.d_customer
    ("customer_id", "first_name", "last_name", "city_id")
SELECT
    "customer_id", "first_name", "last_name", "city_id"
FROM new_data
WHERE
    new_data."customer_id" NOT IN (
        SELECT "customer_id" FROM mart.d_customer
    )
;