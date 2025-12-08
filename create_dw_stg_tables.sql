drop table js_stg_dim_customer;
CREATE TABLE js_stg_dim_customer
(
  customer_id    NUMBER,
  customer_code  VARCHAR2(20),
  customer_name  VARCHAR2(50),
  segment_name   VARCHAR2(50),
  tbl_last_dt    VARCHAR2(19)
)
ORGANIZATION EXTERNAL
(
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY load_data_dir
  ACCESS PARAMETERS
  (
    RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL
    (
      customer_id,
      customer_code,
      customer_name,
      segment_name,
      tbl_last_dt
    )
  )
  LOCATION ('js_dim_customer.csv')
)
REJECT LIMIT UNLIMITED;

select * from js_stg_dim_customer;
SELECT COUNT(*) FROM js_stg_dim_customer;

DROP TABLE js_stg_dim_product;

DROP TABLE js_stg_dim_product;

CREATE TABLE js_stg_dim_product
(
  product_id       NUMBER,
  product_code     VARCHAR2(30),
  product_name     VARCHAR2(255),
  subcategory_name VARCHAR2(100),
  category_name    VARCHAR2(100),
  tbl_last_dt      VARCHAR2(19)
)
ORGANIZATION EXTERNAL
(
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY load_data_dir
  ACCESS PARAMETERS
  (
    RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL
    (
      product_id,
      product_code,
      product_name,
      subcategory_name,
      category_name,
      tbl_last_dt
    )
  )
  LOCATION ('js_dim_product.csv')
)
REJECT LIMIT UNLIMITED;


select * from js_stg_dim_product;
SELECT COUNT(*) FROM js_stg_dim_product;


CREATE TABLE js_stg_dim_ship_address
(
  address_id   NUMBER,
  region       VARCHAR2(100),
  country      VARCHAR2(100),
  state        VARCHAR2(100),
  city         VARCHAR2(100),
  postal_code  VARCHAR2(20),
  market       VARCHAR2(100),
  tbl_last_dt  VARCHAR2(19)
)
ORGANIZATION EXTERNAL
(
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY load_data_dir
  ACCESS PARAMETERS
  (
    RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    MISSING FIELD VALUES ARE NULL
    (
      address_id,
      region,
      country,
      state,
      city,
      postal_code,
      market,
      tbl_last_dt
    )
  )
  LOCATION ('js_dim_ship_address.csv')
)
REJECT LIMIT UNLIMITED;

select * from js_stg_dim_ship_address;
select count(*) from  js_stg_dim_ship_address;


DROP TABLE JS_STG_FACT_ORDER_LINE;

CREATE TABLE JS_STG_FACT_ORDER_LINE (
  order_code        VARCHAR2(40),
  order_date        DATE,
  ship_date         DATE,
  customer_code     VARCHAR2(20),
  product_code      VARCHAR2(30),
  region            VARCHAR2(100),
  country           VARCHAR2(100),
  state             VARCHAR2(100),
  city              VARCHAR2(100),
  postal_code       VARCHAR2(20),
  market            VARCHAR2(100),
  order_priority    VARCHAR2(20),
  ship_mode         VARCHAR2(50),
  quantity          NUMBER,
  discount          NUMBER(5,2),
  sales             NUMBER(12,2),
  profit            NUMBER(12,2),
  shipping_cost     NUMBER(12,2),
  returned_flag     VARCHAR2(1),
  src_tbl_last_dt   DATE
);

select * from JS_STG_FACT_ORDER_LINE;
select count(*) from  JS_STG_FACT_ORDER_LINE;


CREATE TABLE JS_DIM_DATE (
  DATE_ID           NUMBER(8)      NOT NULL,     -- surrogate key YYYYMMDD
  FULL_DATE         DATE           NOT NULL,     -- actual date
  DAYS              VARCHAR2(2),                 -- day of month
  MONTH_SHORT       VARCHAR2(3),                 -- JAN, FEB, ...
  MONTH_NUM         VARCHAR2(2),                 -- 01..12
  MONTH_LONG        VARCHAR2(15),                -- January, February, ...
  DAY_OF_WEEK_SHORT VARCHAR2(3),                 -- MON, TUE, ...
  DAY_OF_WEEK_LONG  VARCHAR2(10),                -- Monday, Tuesday, ...
  YEAR              VARCHAR2(4),                 -- 4-digit year
  QUARTER           VARCHAR2(2),                 -- Q1, Q2, Q3, Q4
  TBL_LAST_DT       DATE DEFAULT SYSDATE NOT NULL,
  CONSTRAINT PK_JS_DIM_DATE PRIMARY KEY (DATE_ID)
);


-- Populate the date dimension
INSERT /*+ APPEND */ INTO JS_DIM_DATE (
  DATE_ID,
  FULL_DATE,
  DAYS,
  MONTH_SHORT,
  MONTH_NUM,
  MONTH_LONG,
  DAY_OF_WEEK_SHORT,
  DAY_OF_WEEK_LONG,
  YEAR,
  QUARTER,
  TBL_LAST_DT
)
SELECT
  TO_NUMBER(TO_CHAR(d,'YYYYMMDD')) AS DATE_ID,     -- numeric key (YYYYMMDD)
  d AS FULL_DATE,
  TO_CHAR(d,'DD') AS DAYS,
  TO_CHAR(d,'MON') AS MONTH_SHORT,
  TO_CHAR(d,'MM') AS MONTH_NUM,
  TO_CHAR(d,'FMMonth') AS MONTH_LONG,
  TO_CHAR(d,'D') AS DAY_OF_WEEK_SHORT,
  TO_CHAR(d,'FMDay') AS DAY_OF_WEEK_LONG,
  TO_CHAR(d,'YYYY') AS YEAR,
  'Q' || TO_CHAR(d,'Q') AS QUARTER,
  SYSDATE AS TBL_LAST_DT
FROM (
  -- Generate date sequence
  SELECT TO_DATE('31-DEC-1970','DD-MON-YYYY') + LEVEL AS d
  FROM dual
  CONNECT BY LEVEL <= 36600   -- ≈ 100 years of dates
);

SELECT * FROM JS_DIM_DATE;

select table_name from user_tables;



