--full ETL, and then incremental ETL.
--One partition table on data warehouse.
--and one history table into OLTP.




-- incremental etl--

select count(*) from js_stg_fact_order_line a
where a.region = 'Oceania' AND a.order_priority = 'High';

select * from js_stg_fact_order_line a
where a.region = 'Oceania' AND a.order_priority = 'High' and a.city = 'Hamilton';

select * from js_stg_fact_order_line a
where a.region = 'Oceania' AND a.order_priority = 'High' and a.city = 'Geraldton';


-- ensure these two matches--
select * from js_fact_order_line a
join js_dim_ship_address b on a.address_id = b.address_id
where b.region = 'Oceania' AND a.order_priority = 'High' and b.city = 'Geraldton';

select * from js_stg_fact_order_line a
where a.region = 'Oceania' AND a.order_priority = 'High' and a.city = 'Geraldton';

-- partition table--
DROP TABLE JS_FACT_ORDER_LINE CASCADE CONSTRAINTS;

CREATE TABLE JS_FACT_ORDER_LINE
(
    fact_id        NUMBER PRIMARY KEY,
    order_code     VARCHAR2(40),
    order_priority VARCHAR2(10),
    ship_mode      VARCHAR2(50),

    order_date_id  NUMBER NOT NULL,
    ship_date_id   NUMBER,

    customer_id    NUMBER NOT NULL,
    product_id     NUMBER NOT NULL,
    address_id     NUMBER NOT NULL,

    quantity       NUMBER,
    discount       NUMBER(5,2),
    sales          NUMBER(12,2),
    profit         NUMBER(12,2),
    shipping_cost  NUMBER(12,2),
    returned_flag  VARCHAR2(1),
    tbl_last_dt    DATE
)
PARTITION BY RANGE (order_date_id)
(
    PARTITION p_2012 VALUES LESS THAN (20130000),
    PARTITION p_2013 VALUES LESS THAN (20140000),
    PARTITION p_2014 VALUES LESS THAN (20150000),
    PARTITION p_2015 VALUES LESS THAN (20160000),
    PARTITION p_max  VALUES LESS THAN (MAXVALUE)
);


ALTER TABLE JS_FACT_ORDER_LINE
  ADD CONSTRAINT fk_fact_order_date
  FOREIGN KEY (order_date_id)
  REFERENCES JS_DIM_DATE(date_id);

ALTER TABLE JS_FACT_ORDER_LINE
  ADD CONSTRAINT fk_fact_ship_date
  FOREIGN KEY (ship_date_id)
  REFERENCES JS_DIM_DATE(date_id);

ALTER TABLE JS_FACT_ORDER_LINE
  ADD CONSTRAINT fk_fact_customer
  FOREIGN KEY (customer_id)
  REFERENCES JS_DIM_CUSTOMER(customer_id);

ALTER TABLE JS_FACT_ORDER_LINE
  ADD CONSTRAINT fk_fact_product
  FOREIGN KEY (product_id)
  REFERENCES JS_DIM_PRODUCT(product_id);

ALTER TABLE JS_FACT_ORDER_LINE
  ADD CONSTRAINT fk_fact_address
  FOREIGN KEY (address_id)
  REFERENCES JS_DIM_SHIP_ADDRESS(address_id);

select * from js_fact_order_line;







