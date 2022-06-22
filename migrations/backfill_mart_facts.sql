-- mart.f_sales from staging.user_order_log

-- DELETE mart.f_sales
DELETE FROM mart.f_sales
WHERE date_id IN (
    SELECT DISTINCT
        CAST(EXTRACT(epoch FROM date_time) AS int4) AS "date_id"
    FROM staging.user_order_log
)
;
-- INSERT mart.f_sales
INSERT INTO mart.f_sales
    (
        date_id,       -- integer NOT NULL,
        item_id,       -- integer NOT NULL,
        customer_id,   -- integer NOT NULL,
        city_id,       -- integer NOT NULL,
        quantity,      -- bigint,
        payment_amount -- numeric(10,2)
    )
SELECT
    CAST(EXTRACT(epoch FROM date_time) AS int4) AS "date_id",
    item_id,
    customer_id,
    MAX(city_id),
    SUM(quantity) AS "quantity",
    SUM(payment_amount) AS "payment_amount"
FROM
    staging.user_order_log
GROUP BY
    date_time, item_id, customer_id
;