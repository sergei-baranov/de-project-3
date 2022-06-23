-- mart.f_customer_retention

CREATE OR REPLACE VIEW mart.f_customer_retention AS
WITH
cte_weeks AS (
    SELECT DISTINCT
        DATE_TRUNC('week', to_timestamp(s.date_id))::DATE AS period_id
    FROM
        mart.f_sales "s"
)
,
cte_orders AS (
    SELECT
        DATE_TRUNC('week', to_timestamp(s.date_id))::DATE AS period_id,
        customer_id,
        COUNT(*) AS customer_orders_count,
        SUM(payment_amount) AS customer_revenue
    FROM
        mart.f_sales "s"
    GROUP BY
        period_id,
        customer_id
)
,
cte_refunds AS (
    SELECT
        DATE_TRUNC('week', to_timestamp(s.date_id))::DATE AS period_id,
        COUNT(DISTINCT customer_id) AS refunded_customer_count,
        COUNT(*) AS customers_refunded
    FROM
        mart.f_sales "s"
    WHERE
        payment_amount < 0
    GROUP BY
        period_id
)
,
cte_new AS (
    SELECT
        period_id,
        customer_id,
        customer_revenue
    FROM
        cte_orders
    WHERE
        customer_orders_count = 1
)
,
cte_returning AS (
    SELECT
        period_id,
        customer_id,
        customer_revenue
    FROM
        cte_orders
    WHERE
        customer_orders_count > 1
)
SELECT
    'weekly'                        AS "period_name",
    w.period_id                     AS "period_id",
    COUNT(DISTINCT n.customer_id)   AS "new_customers_count",
    SUM(n.customer_revenue)         AS "new_customers_revenue",
    COUNT(DISTINCT rt.customer_id)  AS "returning_customers_count",
    SUM(rt.customer_revenue)        AS "returning_customers_revenue",
    MAX(rf.refunded_customer_count) AS "refunded_customer_count",
    MAX(rf.customers_refunded)      AS "customers_refunded"
FROM
    cte_weeks as "w"
    LEFT JOIN cte_new "n" ON n.period_id = w.period_id
    LEFT JOIN cte_returning "rt" ON rt.period_id = w.period_id
    LEFT JOIN cte_refunds "rf" ON rf.period_id = w.period_id
GROUP BY
    w.period_id
ORDER BY
    w.period_id ASC
;