--
-- PostgreSQL database dump
--

-- Dumped from database version 13.6 (Ubuntu 13.6-1.pgdg20.04+1)
-- Dumped by pg_dump version 14.3 (Ubuntu 14.3-1.pgdg20.04+1)

-- Started on 2022-06-11 01:57:49 MSK

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 6 (class 2615 OID 17048)
-- Name: mart; Type: SCHEMA; Schema: -; Owner: jovyan
--

CREATE SCHEMA mart;


ALTER SCHEMA mart OWNER TO jovyan;

--
-- TOC entry 5 (class 2615 OID 17008)
-- Name: staging; Type: SCHEMA; Schema: -; Owner: jovyan
--

CREATE SCHEMA staging;


ALTER SCHEMA staging OWNER TO jovyan;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 218 (class 1259 OID 17093)
-- Name: d_calendar; Type: TABLE; Schema: mart; Owner: jovyan
--

CREATE TABLE mart.d_calendar (
    date_id integer NOT NULL,
    day_num smallint,
    month_num smallint,
    month_name character varying(8),
    year_num smallint,
    batch_id bigint
);


ALTER TABLE mart.d_calendar OWNER TO jovyan;

--
-- TOC entry 217 (class 1259 OID 17084)
-- Name: d_category; Type: TABLE; Schema: mart; Owner: jovyan
--

CREATE TABLE mart.d_category (
    id integer NOT NULL,
    category_id integer NOT NULL,
    category_name character varying(50),
    batch_id bigint
);


ALTER TABLE mart.d_category OWNER TO jovyan;

--
-- TOC entry 216 (class 1259 OID 17082)
-- Name: d_category_id_seq; Type: SEQUENCE; Schema: mart; Owner: jovyan
--

CREATE SEQUENCE mart.d_category_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mart.d_category_id_seq OWNER TO jovyan;

--
-- TOC entry 3102 (class 0 OID 0)
-- Dependencies: 216
-- Name: d_category_id_seq; Type: SEQUENCE OWNED BY; Schema: mart; Owner: jovyan
--

ALTER SEQUENCE mart.d_category_id_seq OWNED BY mart.d_category.id;


--
-- TOC entry 213 (class 1259 OID 17062)
-- Name: d_city; Type: TABLE; Schema: mart; Owner: jovyan
--

CREATE TABLE mart.d_city (
    id integer NOT NULL,
    city_id integer,
    city_name character varying(50),
    batch_id bigint
);


ALTER TABLE mart.d_city OWNER TO jovyan;

--
-- TOC entry 212 (class 1259 OID 17060)
-- Name: d_city_id_seq; Type: SEQUENCE; Schema: mart; Owner: jovyan
--

CREATE SEQUENCE mart.d_city_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mart.d_city_id_seq OWNER TO jovyan;

--
-- TOC entry 3103 (class 0 OID 0)
-- Dependencies: 212
-- Name: d_city_id_seq; Type: SEQUENCE OWNED BY; Schema: mart; Owner: jovyan
--

ALTER SEQUENCE mart.d_city_id_seq OWNED BY mart.d_city.id;


--
-- TOC entry 211 (class 1259 OID 17051)
-- Name: d_customer; Type: TABLE; Schema: mart; Owner: jovyan
--

CREATE TABLE mart.d_customer (
    id integer NOT NULL,
    customer_id integer NOT NULL,
    first_name character varying(15),
    last_name character varying(15),
    city_id integer,
    batch_id bigint
);


ALTER TABLE mart.d_customer OWNER TO jovyan;

--
-- TOC entry 210 (class 1259 OID 17049)
-- Name: d_customer_id_seq; Type: SEQUENCE; Schema: mart; Owner: jovyan
--

CREATE SEQUENCE mart.d_customer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mart.d_customer_id_seq OWNER TO jovyan;

--
-- TOC entry 3104 (class 0 OID 0)
-- Dependencies: 210
-- Name: d_customer_id_seq; Type: SEQUENCE OWNED BY; Schema: mart; Owner: jovyan
--

ALTER SEQUENCE mart.d_customer_id_seq OWNED BY mart.d_customer.id;


--
-- TOC entry 215 (class 1259 OID 17073)
-- Name: d_item; Type: TABLE; Schema: mart; Owner: jovyan
--

CREATE TABLE mart.d_item (
    id integer NOT NULL,
    item_id integer NOT NULL,
    item_name character varying(50),
    category_id integer NOT NULL,
    batch_id bigint
);


ALTER TABLE mart.d_item OWNER TO jovyan;

--
-- TOC entry 214 (class 1259 OID 17071)
-- Name: d_item_id_seq; Type: SEQUENCE; Schema: mart; Owner: jovyan
--

CREATE SEQUENCE mart.d_item_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE mart.d_item_id_seq OWNER TO jovyan;

--
-- TOC entry 3105 (class 0 OID 0)
-- Dependencies: 214
-- Name: d_item_id_seq; Type: SEQUENCE OWNED BY; Schema: mart; Owner: jovyan
--

ALTER SEQUENCE mart.d_item_id_seq OWNED BY mart.d_item.id;


--
-- TOC entry 219 (class 1259 OID 17099)
-- Name: f_activity; Type: TABLE; Schema: mart; Owner: jovyan
--

CREATE TABLE mart.f_activity (
    activity_id integer NOT NULL,
    date_id integer NOT NULL,
    click_number bigint,
    batch_id bigint
);


ALTER TABLE mart.f_activity OWNER TO jovyan;

--
-- TOC entry 220 (class 1259 OID 17111)
-- Name: f_daily_sales; Type: TABLE; Schema: mart; Owner: jovyan
--

CREATE TABLE mart.f_daily_sales (
    date_id integer NOT NULL,
    item_id integer NOT NULL,
    customer_id integer NOT NULL,
    price numeric(10,2),
    quantity bigint,
    payment_amount numeric(10,2),
    batch_id bigint
);


ALTER TABLE mart.f_daily_sales OWNER TO jovyan;

--
-- TOC entry 221 (class 1259 OID 17134)
-- Name: f_research; Type: TABLE; Schema: mart; Owner: jovyan
--

CREATE TABLE mart.f_research (
    date_id integer NOT NULL,
    item_id integer NOT NULL,
    customer_id integer NOT NULL,
    quantity bigint,
    amount numeric(10,2),
    batch_id bigint
);


ALTER TABLE mart.f_research OWNER TO jovyan;

--
-- TOC entry 203 (class 1259 OID 17011)
-- Name: customer_research; Type: TABLE; Schema: staging; Owner: jovyan
--

CREATE TABLE staging.customer_research (
    id integer NOT NULL,
    date_id integer NOT NULL,
    city_id integer NOT NULL,
    category_id integer NOT NULL,
    geo_id integer NOT NULL,
    sales_qty bigint,
    sales_amt numeric(10,2),
    batch_id bigint
);


ALTER TABLE staging.customer_research OWNER TO jovyan;

--
-- TOC entry 202 (class 1259 OID 17009)
-- Name: customer_research_id_seq; Type: SEQUENCE; Schema: staging; Owner: jovyan
--

CREATE SEQUENCE staging.customer_research_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE staging.customer_research_id_seq OWNER TO jovyan;

--
-- TOC entry 3106 (class 0 OID 0)
-- Dependencies: 202
-- Name: customer_research_id_seq; Type: SEQUENCE OWNED BY; Schema: staging; Owner: jovyan
--

ALTER SEQUENCE staging.customer_research_id_seq OWNED BY staging.customer_research.id;


--
-- TOC entry 209 (class 1259 OID 17040)
-- Name: price_log; Type: TABLE; Schema: staging; Owner: jovyan
--

CREATE TABLE staging.price_log (
    id integer NOT NULL,
    date_time timestamp without time zone NOT NULL,
    category_id integer NOT NULL,
    category_name character varying(50),
    item_id integer NOT NULL,
    price numeric(10,2),
    batch_id bigint
);


ALTER TABLE staging.price_log OWNER TO jovyan;

--
-- TOC entry 208 (class 1259 OID 17038)
-- Name: price_log_id_seq; Type: SEQUENCE; Schema: staging; Owner: jovyan
--

CREATE SEQUENCE staging.price_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE staging.price_log_id_seq OWNER TO jovyan;

--
-- TOC entry 3107 (class 0 OID 0)
-- Dependencies: 208
-- Name: price_log_id_seq; Type: SEQUENCE OWNED BY; Schema: staging; Owner: jovyan
--

ALTER SEQUENCE staging.price_log_id_seq OWNED BY staging.price_log.id;


--
-- TOC entry 207 (class 1259 OID 17030)
-- Name: user_activity_log; Type: TABLE; Schema: staging; Owner: jovyan
--

CREATE TABLE staging.user_activity_log (
    id integer NOT NULL,
    date_time timestamp without time zone NOT NULL,
    action_id integer NOT NULL,
    customer_id integer NOT NULL,
    item_id integer NOT NULL,
    quantity bigint,
    batch_id bigint
);


ALTER TABLE staging.user_activity_log OWNER TO jovyan;

--
-- TOC entry 206 (class 1259 OID 17028)
-- Name: user_activity_log_id_seq; Type: SEQUENCE; Schema: staging; Owner: jovyan
--

CREATE SEQUENCE staging.user_activity_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE staging.user_activity_log_id_seq OWNER TO jovyan;

--
-- TOC entry 3108 (class 0 OID 0)
-- Dependencies: 206
-- Name: user_activity_log_id_seq; Type: SEQUENCE OWNED BY; Schema: staging; Owner: jovyan
--

ALTER SEQUENCE staging.user_activity_log_id_seq OWNED BY staging.user_activity_log.id;


--
-- TOC entry 205 (class 1259 OID 17020)
-- Name: user_order_log; Type: TABLE; Schema: staging; Owner: jovyan
--

CREATE TABLE staging.user_order_log (
    id integer NOT NULL,
    date_time timestamp without time zone NOT NULL,
    city_id integer NOT NULL,
    city_name character varying(15),
    customer_id integer NOT NULL,
    first_name character varying(15),
    last_name character varying(15),
    item_id integer NOT NULL,
    item_name character varying(15),
    quantity bigint,
    payment_amount numeric(10,2),
    batch_id bigint
);


ALTER TABLE staging.user_order_log OWNER TO jovyan;

--
-- TOC entry 204 (class 1259 OID 17018)
-- Name: user_order_log_id_seq; Type: SEQUENCE; Schema: staging; Owner: jovyan
--

CREATE SEQUENCE staging.user_order_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE staging.user_order_log_id_seq OWNER TO jovyan;

--
-- TOC entry 3109 (class 0 OID 0)
-- Dependencies: 204
-- Name: user_order_log_id_seq; Type: SEQUENCE OWNED BY; Schema: staging; Owner: jovyan
--

ALTER SEQUENCE staging.user_order_log_id_seq OWNED BY staging.user_order_log.id;


--
-- TOC entry 2887 (class 2604 OID 17087)
-- Name: d_category id; Type: DEFAULT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_category ALTER COLUMN id SET DEFAULT nextval('mart.d_category_id_seq'::regclass);


--
-- TOC entry 2885 (class 2604 OID 17065)
-- Name: d_city id; Type: DEFAULT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_city ALTER COLUMN id SET DEFAULT nextval('mart.d_city_id_seq'::regclass);


--
-- TOC entry 2884 (class 2604 OID 17054)
-- Name: d_customer id; Type: DEFAULT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_customer ALTER COLUMN id SET DEFAULT nextval('mart.d_customer_id_seq'::regclass);


--
-- TOC entry 2886 (class 2604 OID 17076)
-- Name: d_item id; Type: DEFAULT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_item ALTER COLUMN id SET DEFAULT nextval('mart.d_item_id_seq'::regclass);


--
-- TOC entry 2880 (class 2604 OID 17014)
-- Name: customer_research id; Type: DEFAULT; Schema: staging; Owner: jovyan
--

ALTER TABLE ONLY staging.customer_research ALTER COLUMN id SET DEFAULT nextval('staging.customer_research_id_seq'::regclass);


--
-- TOC entry 2883 (class 2604 OID 17043)
-- Name: price_log id; Type: DEFAULT; Schema: staging; Owner: jovyan
--

ALTER TABLE ONLY staging.price_log ALTER COLUMN id SET DEFAULT nextval('staging.price_log_id_seq'::regclass);


--
-- TOC entry 2882 (class 2604 OID 17033)
-- Name: user_activity_log id; Type: DEFAULT; Schema: staging; Owner: jovyan
--

ALTER TABLE ONLY staging.user_activity_log ALTER COLUMN id SET DEFAULT nextval('staging.user_activity_log_id_seq'::regclass);


--
-- TOC entry 2881 (class 2604 OID 17023)
-- Name: user_order_log id; Type: DEFAULT; Schema: staging; Owner: jovyan
--

ALTER TABLE ONLY staging.user_order_log ALTER COLUMN id SET DEFAULT nextval('staging.user_order_log_id_seq'::regclass);


--
-- TOC entry 3093 (class 0 OID 17093)
-- Dependencies: 218
-- Data for Name: d_calendar; Type: TABLE DATA; Schema: mart; Owner: jovyan
--

COPY mart.d_calendar (date_id, day_num, month_num, month_name, year_num, batch_id) FROM stdin;
\.


--
-- TOC entry 3092 (class 0 OID 17084)
-- Dependencies: 217
-- Data for Name: d_category; Type: TABLE DATA; Schema: mart; Owner: jovyan
--

COPY mart.d_category (id, category_id, category_name, batch_id) FROM stdin;
\.


--
-- TOC entry 3088 (class 0 OID 17062)
-- Dependencies: 213
-- Data for Name: d_city; Type: TABLE DATA; Schema: mart; Owner: jovyan
--

COPY mart.d_city (id, city_id, city_name, batch_id) FROM stdin;
\.


--
-- TOC entry 3086 (class 0 OID 17051)
-- Dependencies: 211
-- Data for Name: d_customer; Type: TABLE DATA; Schema: mart; Owner: jovyan
--

COPY mart.d_customer (id, customer_id, first_name, last_name, city_id, batch_id) FROM stdin;
\.


--
-- TOC entry 3090 (class 0 OID 17073)
-- Dependencies: 215
-- Data for Name: d_item; Type: TABLE DATA; Schema: mart; Owner: jovyan
--

COPY mart.d_item (id, item_id, item_name, category_id, batch_id) FROM stdin;
\.


--
-- TOC entry 3094 (class 0 OID 17099)
-- Dependencies: 219
-- Data for Name: f_activity; Type: TABLE DATA; Schema: mart; Owner: jovyan
--

COPY mart.f_activity (activity_id, date_id, click_number, batch_id) FROM stdin;
\.


--
-- TOC entry 3095 (class 0 OID 17111)
-- Dependencies: 220
-- Data for Name: f_daily_sales; Type: TABLE DATA; Schema: mart; Owner: jovyan
--

COPY mart.f_daily_sales (date_id, item_id, customer_id, price, quantity, payment_amount, batch_id) FROM stdin;
\.


--
-- TOC entry 3096 (class 0 OID 17134)
-- Dependencies: 221
-- Data for Name: f_research; Type: TABLE DATA; Schema: mart; Owner: jovyan
--

COPY mart.f_research (date_id, item_id, customer_id, quantity, amount, batch_id) FROM stdin;
\.


--
-- TOC entry 3078 (class 0 OID 17011)
-- Dependencies: 203
-- Data for Name: customer_research; Type: TABLE DATA; Schema: staging; Owner: jovyan
--

COPY staging.customer_research (id, date_id, city_id, category_id, geo_id, sales_qty, sales_amt, batch_id) FROM stdin;
\.


--
-- TOC entry 3084 (class 0 OID 17040)
-- Dependencies: 209
-- Data for Name: price_log; Type: TABLE DATA; Schema: staging; Owner: jovyan
--

COPY staging.price_log (id, date_time, category_id, category_name, item_id, price, batch_id) FROM stdin;
\.


--
-- TOC entry 3082 (class 0 OID 17030)
-- Dependencies: 207
-- Data for Name: user_activity_log; Type: TABLE DATA; Schema: staging; Owner: jovyan
--

COPY staging.user_activity_log (id, date_time, action_id, customer_id, item_id, quantity, batch_id) FROM stdin;
\.


--
-- TOC entry 3080 (class 0 OID 17020)
-- Dependencies: 205
-- Data for Name: user_order_log; Type: TABLE DATA; Schema: staging; Owner: jovyan
--

COPY staging.user_order_log (id, date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount, batch_id) FROM stdin;
\.


--
-- TOC entry 3110 (class 0 OID 0)
-- Dependencies: 216
-- Name: d_category_id_seq; Type: SEQUENCE SET; Schema: mart; Owner: jovyan
--

SELECT pg_catalog.setval('mart.d_category_id_seq', 1, false);


--
-- TOC entry 3111 (class 0 OID 0)
-- Dependencies: 212
-- Name: d_city_id_seq; Type: SEQUENCE SET; Schema: mart; Owner: jovyan
--

SELECT pg_catalog.setval('mart.d_city_id_seq', 1, false);


--
-- TOC entry 3112 (class 0 OID 0)
-- Dependencies: 210
-- Name: d_customer_id_seq; Type: SEQUENCE SET; Schema: mart; Owner: jovyan
--

SELECT pg_catalog.setval('mart.d_customer_id_seq', 1, false);


--
-- TOC entry 3113 (class 0 OID 0)
-- Dependencies: 214
-- Name: d_item_id_seq; Type: SEQUENCE SET; Schema: mart; Owner: jovyan
--

SELECT pg_catalog.setval('mart.d_item_id_seq', 1, false);


--
-- TOC entry 3114 (class 0 OID 0)
-- Dependencies: 202
-- Name: customer_research_id_seq; Type: SEQUENCE SET; Schema: staging; Owner: jovyan
--

SELECT pg_catalog.setval('staging.customer_research_id_seq', 1, false);


--
-- TOC entry 3115 (class 0 OID 0)
-- Dependencies: 208
-- Name: price_log_id_seq; Type: SEQUENCE SET; Schema: staging; Owner: jovyan
--

SELECT pg_catalog.setval('staging.price_log_id_seq', 1, false);


--
-- TOC entry 3116 (class 0 OID 0)
-- Dependencies: 206
-- Name: user_activity_log_id_seq; Type: SEQUENCE SET; Schema: staging; Owner: jovyan
--

SELECT pg_catalog.setval('staging.user_activity_log_id_seq', 1, false);


--
-- TOC entry 3117 (class 0 OID 0)
-- Dependencies: 204
-- Name: user_order_log_id_seq; Type: SEQUENCE SET; Schema: staging; Owner: jovyan
--

SELECT pg_catalog.setval('staging.user_order_log_id_seq', 1, false);


--
-- TOC entry 2925 (class 2606 OID 17097)
-- Name: d_calendar d_calendar_pkey; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_calendar
    ADD CONSTRAINT d_calendar_pkey PRIMARY KEY (date_id);


--
-- TOC entry 2920 (class 2606 OID 17091)
-- Name: d_category d_category_category_id_key; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_category
    ADD CONSTRAINT d_category_category_id_key UNIQUE (category_id);


--
-- TOC entry 2922 (class 2606 OID 17089)
-- Name: d_category d_category_pkey; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_category
    ADD CONSTRAINT d_category_pkey PRIMARY KEY (id);


--
-- TOC entry 2910 (class 2606 OID 17069)
-- Name: d_city d_city_city_id_key; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_city
    ADD CONSTRAINT d_city_city_id_key UNIQUE (city_id);


--
-- TOC entry 2912 (class 2606 OID 17067)
-- Name: d_city d_city_pkey; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_city
    ADD CONSTRAINT d_city_pkey PRIMARY KEY (id);


--
-- TOC entry 2905 (class 2606 OID 17058)
-- Name: d_customer d_customer_customer_id_key; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_customer
    ADD CONSTRAINT d_customer_customer_id_key UNIQUE (customer_id);


--
-- TOC entry 2907 (class 2606 OID 17056)
-- Name: d_customer d_customer_pkey; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_customer
    ADD CONSTRAINT d_customer_pkey PRIMARY KEY (id);


--
-- TOC entry 2915 (class 2606 OID 17080)
-- Name: d_item d_item_item_id_key; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_item
    ADD CONSTRAINT d_item_item_id_key UNIQUE (item_id);


--
-- TOC entry 2917 (class 2606 OID 17078)
-- Name: d_item d_item_pkey; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.d_item
    ADD CONSTRAINT d_item_pkey PRIMARY KEY (id);


--
-- TOC entry 2929 (class 2606 OID 17103)
-- Name: f_activity f_activity_pkey; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_activity
    ADD CONSTRAINT f_activity_pkey PRIMARY KEY (activity_id, date_id);


--
-- TOC entry 2931 (class 2606 OID 17115)
-- Name: f_daily_sales f_daily_sales_pkey; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_daily_sales
    ADD CONSTRAINT f_daily_sales_pkey PRIMARY KEY (date_id, item_id, customer_id);


--
-- TOC entry 2939 (class 2606 OID 17138)
-- Name: f_research f_research_pkey; Type: CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_research
    ADD CONSTRAINT f_research_pkey PRIMARY KEY (date_id, item_id, customer_id);


--
-- TOC entry 2890 (class 2606 OID 17016)
-- Name: customer_research customer_research_pkey; Type: CONSTRAINT; Schema: staging; Owner: jovyan
--

ALTER TABLE ONLY staging.customer_research
    ADD CONSTRAINT customer_research_pkey PRIMARY KEY (id);


--
-- TOC entry 2902 (class 2606 OID 17045)
-- Name: price_log price_log_pkey; Type: CONSTRAINT; Schema: staging; Owner: jovyan
--

ALTER TABLE ONLY staging.price_log
    ADD CONSTRAINT price_log_pkey PRIMARY KEY (id);


--
-- TOC entry 2898 (class 2606 OID 17035)
-- Name: user_activity_log user_activity_log_pkey; Type: CONSTRAINT; Schema: staging; Owner: jovyan
--

ALTER TABLE ONLY staging.user_activity_log
    ADD CONSTRAINT user_activity_log_pkey PRIMARY KEY (id);


--
-- TOC entry 2894 (class 2606 OID 17025)
-- Name: user_order_log user_order_log_pkey; Type: CONSTRAINT; Schema: staging; Owner: jovyan
--

ALTER TABLE ONLY staging.user_order_log
    ADD CONSTRAINT user_order_log_pkey PRIMARY KEY (id);


--
-- TOC entry 2923 (class 1259 OID 17098)
-- Name: d_calendar1; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX d_calendar1 ON mart.d_calendar USING btree (year_num);


--
-- TOC entry 2918 (class 1259 OID 17092)
-- Name: d_category1; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX d_category1 ON mart.d_category USING btree (category_id);


--
-- TOC entry 2908 (class 1259 OID 17070)
-- Name: d_city1; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX d_city1 ON mart.d_city USING btree (city_id);


--
-- TOC entry 2903 (class 1259 OID 17059)
-- Name: d_cust1; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX d_cust1 ON mart.d_customer USING btree (customer_id);


--
-- TOC entry 2913 (class 1259 OID 17081)
-- Name: d_item1; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE UNIQUE INDEX d_item1 ON mart.d_item USING btree (item_id);


--
-- TOC entry 2926 (class 1259 OID 17109)
-- Name: f_activity1; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX f_activity1 ON mart.f_activity USING btree (date_id);


--
-- TOC entry 2927 (class 1259 OID 17110)
-- Name: f_activity2; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX f_activity2 ON mart.f_activity USING btree (activity_id);


--
-- TOC entry 2932 (class 1259 OID 17131)
-- Name: f_ds1; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX f_ds1 ON mart.f_daily_sales USING btree (date_id);


--
-- TOC entry 2933 (class 1259 OID 17132)
-- Name: f_ds2; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX f_ds2 ON mart.f_daily_sales USING btree (item_id);


--
-- TOC entry 2934 (class 1259 OID 17133)
-- Name: f_ds3; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX f_ds3 ON mart.f_daily_sales USING btree (customer_id);


--
-- TOC entry 2935 (class 1259 OID 17154)
-- Name: f_r1; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX f_r1 ON mart.f_daily_sales USING btree (date_id);


--
-- TOC entry 2936 (class 1259 OID 17155)
-- Name: f_r2; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX f_r2 ON mart.f_daily_sales USING btree (item_id);


--
-- TOC entry 2937 (class 1259 OID 17156)
-- Name: f_r3; Type: INDEX; Schema: mart; Owner: jovyan
--

CREATE INDEX f_r3 ON mart.f_daily_sales USING btree (customer_id);


--
-- TOC entry 2888 (class 1259 OID 17017)
-- Name: cr1; Type: INDEX; Schema: staging; Owner: jovyan
--

CREATE INDEX cr1 ON staging.customer_research USING btree (category_id);


--
-- TOC entry 2899 (class 1259 OID 17046)
-- Name: pr1; Type: INDEX; Schema: staging; Owner: jovyan
--

CREATE INDEX pr1 ON staging.price_log USING btree (date_time);


--
-- TOC entry 2900 (class 1259 OID 17047)
-- Name: pr2; Type: INDEX; Schema: staging; Owner: jovyan
--

CREATE INDEX pr2 ON staging.price_log USING btree (item_id);


--
-- TOC entry 2895 (class 1259 OID 17036)
-- Name: ua1; Type: INDEX; Schema: staging; Owner: jovyan
--

CREATE INDEX ua1 ON staging.user_activity_log USING btree (customer_id);


--
-- TOC entry 2896 (class 1259 OID 17037)
-- Name: ua2; Type: INDEX; Schema: staging; Owner: jovyan
--

CREATE INDEX ua2 ON staging.user_activity_log USING btree (item_id);


--
-- TOC entry 2891 (class 1259 OID 17026)
-- Name: uo1; Type: INDEX; Schema: staging; Owner: jovyan
--

CREATE INDEX uo1 ON staging.user_order_log USING btree (customer_id);


--
-- TOC entry 2892 (class 1259 OID 17027)
-- Name: uo2; Type: INDEX; Schema: staging; Owner: jovyan
--

CREATE INDEX uo2 ON staging.user_order_log USING btree (item_id);


--
-- TOC entry 2940 (class 2606 OID 17104)
-- Name: f_activity f_activity_date_id_fkey; Type: FK CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_activity
    ADD CONSTRAINT f_activity_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id) ON UPDATE CASCADE;


--
-- TOC entry 2943 (class 2606 OID 17126)
-- Name: f_daily_sales f_daily_sales_customer_id_fkey; Type: FK CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_daily_sales
    ADD CONSTRAINT f_daily_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id) ON UPDATE CASCADE;


--
-- TOC entry 2941 (class 2606 OID 17116)
-- Name: f_daily_sales f_daily_sales_date_id_fkey; Type: FK CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_daily_sales
    ADD CONSTRAINT f_daily_sales_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id) ON UPDATE CASCADE;


--
-- TOC entry 2942 (class 2606 OID 17121)
-- Name: f_daily_sales f_daily_sales_item_id_fkey; Type: FK CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_daily_sales
    ADD CONSTRAINT f_daily_sales_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id) ON UPDATE CASCADE;


--
-- TOC entry 2946 (class 2606 OID 17149)
-- Name: f_research f_research_customer_id_fkey; Type: FK CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_research
    ADD CONSTRAINT f_research_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id) ON UPDATE CASCADE;


--
-- TOC entry 2944 (class 2606 OID 17139)
-- Name: f_research f_research_date_id_fkey; Type: FK CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_research
    ADD CONSTRAINT f_research_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id) ON UPDATE CASCADE;


--
-- TOC entry 2945 (class 2606 OID 17144)
-- Name: f_research f_research_item_id_fkey; Type: FK CONSTRAINT; Schema: mart; Owner: jovyan
--

ALTER TABLE ONLY mart.f_research
    ADD CONSTRAINT f_research_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id) ON UPDATE CASCADE;


-- Completed on 2022-06-11 01:57:49 MSK

--
-- PostgreSQL database dump complete
--

