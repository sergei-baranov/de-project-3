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
        CAST(EXTRACT('epoch' FROM date_time) AS integer) as "date_id",
        date_time::Date                   as "date_actual",
        EXTRACT('epoch' FROM date_time)   as "epoch",
        TO_CHAR(date_time::Date, 'DDth')  as "day_suffix",
        TO_CHAR(date_time::Date, 'Day')   as "day_name", -- character varying(9)
        EXTRACT(isodow FROM date_time)    as "day_of_week", -- integer
        EXTRACT('day' FROM date_time)     as "day_of_month", -- integer
        1 as "day_of_quarter", -- integer
        EXTRACT(DOY FROM date_time) as "day_of_year", -- integer
        extract('day' from date_trunc('week', current_date) -
        date_trunc('week', date_trunc('month', current_date))) / 7 + 1 as "week_of_month", -- integer
        to_char(date_time, 'WW')::int as "week_of_year", -- integer
        EXTRACT(WEEK FROM date_time) as "week_of_year_iso", -- character(10)
        EXTRACT(MONTH FROM date_time) as "month_actual", -- integer
        TO_CHAR(date_time, 'Month')      as "month_name", -- character varying(9)
        TO_CHAR(date_time, 'MON')        as "month_name_abbreviated", -- character(3)
        EXTRACT(QUARTER FROM date_time)  as "quarter_actual", -- integer
        (CASE
           WHEN EXTRACT(quarter FROM date_time) = 1 THEN 'First'
           WHEN EXTRACT(quarter FROM date_time) = 2 THEN 'Second'
           WHEN EXTRACT(quarter FROM date_time) = 3 THEN 'Third'
           WHEN EXTRACT(quarter FROM date_time) = 4 THEN 'Fourth'
        END) as "quarter_name", -- varying(9)
        EXTRACT(YEAR FROM date_time)     as "year_actual", -- integer
        date_trunc('week', date_time)::Date as "first_day_of_week", -- date
        (date_trunc('week', date_time)+ '6 days'::interval)::date as "last_day_of_week", -- date
        date_trunc('month', date_time)::Date as "first_day_of_month", -- date
        (date_trunc('month', date_time) + interval '1 month' - interval '1 day')::date as "last_day_of_month", -- date
        date_trunc('quarter', date_time) as "first_day_of_quarter", -- date
        CAST(date_trunc('quarter', date_time)  + interval '3 months' - interval '1 day' AS date) "last_day_of_quarter", -- date
        date_trunc('year', date_time) as "first_day_of_year", -- date
        CAST(date_trunc('year', date_time)  + interval '1 year' - interval '1 day' AS date)as "last_day_of_year", -- date
        to_char(date_time, 'MMYYYY') as "mmyyyy", -- character(6)
        to_char(date_time, 'MMDDYYYY') as "mmddyyyy", -- character(10)
        (CASE
           WHEN EXTRACT(isodow FROM date_time) IN (6,7) THEN TRUE
           ELSE FALSE
        END) as "weekend_indr" -- boolean
    FROM
        "staging"."user_order_log"
    ORDER BY "date_id" ASC
)
INSERT INTO mart.d_calendar
    (
        "date_id",                -- integer NOT NULL,
        "date_actual",            -- date NOT NULL,
        "epoch",                  -- bigint NOT NULL,
        "day_suffix",             -- character varying(4) NOT NULL,
        "day_name",               -- character varying(9) NOT NULL,
        "day_of_week",            -- integer NOT NULL,
        "day_of_month",           -- integer NOT NULL,
        "day_of_quarter",         -- integer NOT NULL,
        "day_of_year",            -- integer NOT NULL,
        "week_of_month",          -- integer NOT NULL,
        "week_of_year",           -- integer NOT NULL,
        "week_of_year_iso",       -- character(10) NOT NULL,
        "month_actual",           -- integer NOT NULL,
        "month_name",             -- character varying(9) NOT NULL,
        "month_name_abbreviated", -- character(3) NOT NULL,
        "quarter_actual",         -- integer NOT NULL,
        "quarter_name",           -- varying(9) NOT NULL,
        "year_actual",            -- integer NOT NULL,
        "first_day_of_week",      -- date NOT NULL,
        "last_day_of_week",       -- date NOT NULL,
        "first_day_of_month",     -- date NOT NULL,
        "last_day_of_month",      -- date NOT NULL,
        "first_day_of_quarter",   -- date NOT NULL,
        "last_day_of_quarter",    -- date NOT NULL,
        "first_day_of_year",      -- date NOT NULL,
        "last_day_of_year",       -- date NOT NULL,
        "mmyyyy",                 -- character(6) NOT NULL,
        "mmddyyyy",               -- character(10) NOT NULL,
        "weekend_indr"            -- boolean NOT NULL,
    )
SELECT
    *
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