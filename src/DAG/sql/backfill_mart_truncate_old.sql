-- TRUNCATE
DELETE FROM mart.f_research;
DELETE FROM mart.f_daily_sales;
DELETE FROM mart.f_activity;
TRUNCATE mart.d_item CASCADE;
TRUNCATE mart.d_customer CASCADE;
TRUNCATE mart.d_city CASCADE;
TRUNCATE mart.d_category CASCADE;
TRUNCATE mart.d_calendar CASCADE;