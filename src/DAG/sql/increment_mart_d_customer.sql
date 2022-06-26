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