-- mart.f_daily_sales from staging.user_order_log

-- DELETE mart.f_daily_sales
DELETE FROM mart.f_daily_sales
WHERE date_id IN (
    SELECT DISTINCT
        CAST(EXTRACT(epoch FROM date_time) AS int4) AS "date_id"
    FROM staging.user_order_log
)
;
-- INSERT mart.f_daily_sales
INSERT INTO mart.f_daily_sales
    (date_id, item_id, customer_id, quantity, payment_amount)
SELECT
    CAST(EXTRACT(epoch FROM date_time) AS int4) AS "date_id",
    item_id,
    customer_id,
    SUM(quantity) AS "quantity",
    SUM(payment_amount) AS "payment_amount"
FROM
    staging.user_order_log
GROUP BY
    date_time, item_id, customer_id
;

-- mart.f_activity from staging.user_activity_log

-- DELETE mart.f_activity
DELETE FROM mart.f_activity
WHERE
    date_id IN (
        SELECT DISTINCT
            CAST(EXTRACT(epoch FROM date_time) AS int4) AS "date_id"
        FROM staging.user_activity_log
    )
;
-- INSERT mart.f_activity
INSERT INTO mart.f_activity
    (activity_id, date_id, click_number)
SELECT
    action_id as "activity_id",
    CAST(EXTRACT(epoch FROM date_time) AS int4) AS "date_id",
    SUM(quantity) as "click_number"
FROM
    staging.user_activity_log
GROUP BY
    action_id, date_time
;

-- mart.f_research from staging.user_order_log

-- DELETE mart.f_research
DELETE FROM mart.f_research
WHERE
    date_id IN (
        SELECT DISTINCT
            CAST(EXTRACT(epoch FROM date_time) AS int4) AS "date_id"
        FROM staging.user_order_log
    )
;

-- INSERT mart.f_research
INSERT INTO mart.f_research
    (date_id, item_id, customer_id, quantity, amount)
SELECT
    CAST(EXTRACT(epoch FROM date_time) AS int4) AS "date_id",
    item_id,
    customer_id,
    SUM(quantity) AS "quantity",
    SUM(payment_amount) as "amount"
FROM
    staging.user_order_log
GROUP BY
    date_time, item_id, customer_id
;