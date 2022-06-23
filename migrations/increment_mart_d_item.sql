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