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