DROP VIEW IF EXISTS mart.f_customer_retention;

DROP TABLE IF EXISTS mart.f_sales;
DROP TABLE IF EXISTS mart.d_item CASCADE;
DROP TABLE IF EXISTS mart.d_customer CASCADE;
DROP TABLE IF EXISTS mart.d_calendar CASCADE;
DROP TABLE IF EXISTS mart.d_city CASCADE;

DROP TABLE IF EXISTS staging.user_order_log;

DROP SEQUENCE IF EXISTS mart.d_city_id_seq;
DROP SEQUENCE IF EXISTS mart.d_customer_id_seq;
DROP SEQUENCE IF EXISTS mart.d_item_id_seq;
DROP SEQUENCE IF EXISTS mart.f_sales_id_seq;
DROP SEQUENCE IF EXISTS staging.user_order_log_id_seq;


CREATE TABLE mart.d_calendar (
    date_id integer NOT NULL,
    date_actual date NOT NULL,
    epoch bigint NOT NULL,
    day_suffix character varying(4) NOT NULL,
    day_name character varying(9) NOT NULL,
    day_of_week integer NOT NULL,
    day_of_month integer NOT NULL,
    day_of_quarter integer NOT NULL,
    day_of_year integer NOT NULL,
    week_of_month integer NOT NULL,
    week_of_year integer NOT NULL,
    week_of_year_iso character(10) NOT NULL,
    month_actual integer NOT NULL,
    month_name character varying(9) NOT NULL,
    month_name_abbreviated character(3) NOT NULL,
    quarter_actual integer NOT NULL,
    quarter_name character varying(9) NOT NULL,
    year_actual integer NOT NULL,
    first_day_of_week date NOT NULL,
    last_day_of_week date NOT NULL,
    first_day_of_month date NOT NULL,
    last_day_of_month date NOT NULL,
    first_day_of_quarter date NOT NULL,
    last_day_of_quarter date NOT NULL,
    first_day_of_year date NOT NULL,
    last_day_of_year date NOT NULL,
    mmyyyy character(6) NOT NULL,
    mmddyyyy character(10) NOT NULL,
    weekend_indr boolean NOT NULL
);

CREATE TABLE mart.d_city (
    id integer NOT NULL,
    city_id integer,
    city_name character varying(50)
);

CREATE SEQUENCE mart.d_city_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE mart.d_city_id_seq OWNED BY mart.d_city.id;

CREATE TABLE mart.d_customer (
    id integer NOT NULL,
    customer_id integer NOT NULL,
    first_name character varying(15),
    last_name character varying(15),
    city_id integer
);

CREATE SEQUENCE mart.d_customer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE mart.d_customer_id_seq OWNED BY mart.d_customer.id;

CREATE TABLE mart.d_item (
    id integer NOT NULL,
    item_id integer NOT NULL,
    item_name character varying(50)
);

CREATE SEQUENCE mart.d_item_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE mart.d_item_id_seq OWNED BY mart.d_item.id;

CREATE TABLE mart.f_sales (
    id integer NOT NULL,
    date_id integer NOT NULL,
    item_id integer NOT NULL,
    customer_id integer NOT NULL,
    city_id integer NOT NULL,
    quantity bigint,
    payment_amount numeric(10,2)
);

CREATE SEQUENCE mart.f_sales_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE mart.f_sales_id_seq OWNED BY mart.f_sales.id;

CREATE TABLE staging.user_order_log (
    id integer NOT NULL,
    date_time timestamp without time zone NOT NULL,
    city_id integer NOT NULL,
    city_name character varying(100),
    customer_id integer NOT NULL,
    first_name character varying(100),
    last_name character varying(100),
    item_id integer NOT NULL,
    item_name character varying(100),
    quantity bigint,
    payment_amount numeric(10,2)
);

CREATE SEQUENCE staging.user_order_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE staging.user_order_log_id_seq OWNED BY staging.user_order_log.id;

ALTER TABLE ONLY mart.d_city ALTER COLUMN id SET DEFAULT nextval('mart.d_city_id_seq'::regclass);
ALTER TABLE ONLY mart.d_customer ALTER COLUMN id SET DEFAULT nextval('mart.d_customer_id_seq'::regclass);
ALTER TABLE ONLY mart.d_item ALTER COLUMN id SET DEFAULT nextval('mart.d_item_id_seq'::regclass);
ALTER TABLE ONLY mart.f_sales ALTER COLUMN id SET DEFAULT nextval('mart.f_sales_id_seq'::regclass);
ALTER TABLE ONLY staging.user_order_log ALTER COLUMN id SET DEFAULT nextval('staging.user_order_log_id_seq'::regclass);

SELECT pg_catalog.setval('mart.d_city_id_seq', 1, false);
SELECT pg_catalog.setval('mart.d_customer_id_seq', 1, false);
SELECT pg_catalog.setval('mart.d_item_id_seq', 1, false);
SELECT pg_catalog.setval('mart.f_sales_id_seq', 1, false);
SELECT pg_catalog.setval('staging.user_order_log_id_seq', 1, false);

ALTER TABLE ONLY mart.d_city
    ADD CONSTRAINT d_city_city_id_key UNIQUE (city_id);

ALTER TABLE ONLY mart.d_city
    ADD CONSTRAINT d_city_pkey PRIMARY KEY (id);

ALTER TABLE ONLY mart.d_customer
    ADD CONSTRAINT d_customer_customer_id_key UNIQUE (customer_id);

ALTER TABLE ONLY mart.d_customer
    ADD CONSTRAINT d_customer_pkey PRIMARY KEY (id);

ALTER TABLE ONLY mart.d_calendar
    ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_id);

ALTER TABLE ONLY mart.d_item
    ADD CONSTRAINT d_item_item_id_key UNIQUE (item_id);

ALTER TABLE ONLY mart.d_item
    ADD CONSTRAINT d_item_pkey PRIMARY KEY (id);

ALTER TABLE ONLY mart.f_sales
    ADD CONSTRAINT f_sales_pkey PRIMARY KEY (id);

ALTER TABLE ONLY staging.user_order_log
    ADD CONSTRAINT user_order_log_pkey PRIMARY KEY (id);

CREATE INDEX d_city1 ON mart.d_city USING btree (city_id);

CREATE INDEX d_cust1 ON mart.d_customer USING btree (customer_id);

CREATE INDEX d_date_date_actual_idx ON mart.d_calendar USING btree (date_actual);

CREATE UNIQUE INDEX d_item1 ON mart.d_item USING btree (item_id);

CREATE INDEX f_ds1 ON mart.f_sales USING btree (date_id);

CREATE INDEX f_ds2 ON mart.f_sales USING btree (item_id);

CREATE INDEX f_ds3 ON mart.f_sales USING btree (customer_id);

CREATE INDEX f_ds4 ON mart.f_sales USING btree (city_id);

CREATE INDEX uo1 ON staging.user_order_log USING btree (customer_id);

CREATE INDEX uo2 ON staging.user_order_log USING btree (item_id);

ALTER TABLE ONLY mart.f_sales
    ADD CONSTRAINT f_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id);

ALTER TABLE ONLY mart.f_sales
    ADD CONSTRAINT f_sales_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id);

ALTER TABLE ONLY mart.f_sales
    ADD CONSTRAINT f_sales_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id);

ALTER TABLE ONLY mart.f_sales
    ADD CONSTRAINT f_sales_item_id_fkey1 FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id);