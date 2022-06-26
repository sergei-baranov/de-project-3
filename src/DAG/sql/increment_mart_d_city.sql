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