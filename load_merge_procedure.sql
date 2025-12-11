CREATE OR REPLACE PROCEDURE load_merge_dim_customer IS
    err_code NUMBER;
    err_msg  VARCHAR2(32000);
BEGIN
    MERGE INTO js_dim_customer a
    USING js_stg_dim_customer b
       ON (a.customer_id = b.customer_id)
    WHEN MATCHED THEN
        UPDATE SET
            a.customer_code = b.customer_code,
            a.customer_name = b.customer_name,
            a.segment_name  = b.segment_name,
            a.tbl_last_dt   = TO_DATE(b.tbl_last_dt, 'YYYY-MM-DD HH24:MI:SS')
    WHEN NOT MATCHED THEN
        INSERT (
            customer_id,
            customer_code,
            customer_name,
            segment_name,
            tbl_last_dt
        )
        VALUES (
            b.customer_id,
            b.customer_code,
            b.customer_name,
            b.segment_name,
            TO_DATE(b.tbl_last_dt, 'YYYY-MM-DD HH24:MI:SS')
        );

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        err_code := SQLCODE;
        err_msg  := SQLERRM;
        DBMS_OUTPUT.put_line('Error in load_merge_dim_customer - code ' || err_code || ': ' || err_msg);
END;
/


CREATE OR REPLACE PROCEDURE load_merge_dim_product IS
    err_code NUMBER;
    err_msg  VARCHAR2(32000);
BEGIN
    MERGE INTO js_dim_product a
    USING (
        SELECT
          product_id,
          product_code,
          product_name,
          subcategory_name,
          category_name,
          CASE
            WHEN tbl_last_dt IS NOT NULL THEN
              TO_DATE(tbl_last_dt, 'YYYY-MM-DD HH24:MI:SS')
            ELSE
              SYSDATE
          END AS conv_tbl_last_dt
        FROM js_stg_dim_product
    ) b
       ON (a.product_id = b.product_id)
    WHEN MATCHED THEN
        UPDATE SET
            a.product_code     = b.product_code,
            a.product_name     = b.product_name,
            a.subcategory_name = b.subcategory_name,
            a.category_name    = b.category_name,
            a.tbl_last_dt      = b.conv_tbl_last_dt
    WHEN NOT MATCHED THEN
        INSERT (
            product_id,
            product_code,
            product_name,
            subcategory_name,
            category_name,
            tbl_last_dt
        )
        VALUES (
            b.product_id,
            b.product_code,
            b.product_name,
            b.subcategory_name,
            b.category_name,
            b.conv_tbl_last_dt
        );

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        err_code := SQLCODE;
        err_msg  := SQLERRM;
        DBMS_OUTPUT.put_line('Error in load_merge_dim_product - code ' || err_code || ': ' || err_msg);
END;
/


CREATE OR REPLACE PROCEDURE load_merge_dim_ship_address IS
    err_code NUMBER;
    err_msg  VARCHAR2(32000);
BEGIN
    MERGE INTO js_dim_ship_address a
    USING js_stg_dim_ship_address b
       ON (a.address_id = b.address_id)
    WHEN MATCHED THEN
        UPDATE SET
            a.region      = b.region,
            a.country     = b.country,
            a.state       = b.state,
            a.city        = b.city,
            a.postal_code = b.postal_code,
            a.market      = b.market,
            a.tbl_last_dt = TO_DATE(b.tbl_last_dt, 'YYYY-MM-DD HH24:MI:SS')
    WHEN NOT MATCHED THEN
        INSERT (
            address_id,
            region,
            country,
            state,
            city,
            postal_code,
            market,
            tbl_last_dt
        )
        VALUES (
            b.address_id,
            b.region,
            b.country,
            b.state,
            b.city,
            b.postal_code,
            b.market,
            TO_DATE(b.tbl_last_dt, 'YYYY-MM-DD HH24:MI:SS')
        );

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        err_code := SQLCODE;
        err_msg  := SQLERRM;
        DBMS_OUTPUT.put_line('Error in load_merge_dim_ship_address - code ' || err_code || ': ' || err_msg);
END;
/


select * from js_dim_ship_address;
select count(*) from js_dim_ship_address;


CREATE SEQUENCE js_fact_order_line_seq
  START WITH 1
  INCREMENT BY 1
  NOCACHE;
  
  
  
CREATE OR REPLACE PROCEDURE load_merge_fact_order_line IS
    err_code NUMBER;
    err_msg  VARCHAR2(32000);
BEGIN
    MERGE INTO js_fact_order_line a
    USING (
        SELECT
            s.order_code,
            s.order_priority,
            s.ship_mode,
            -- date keys (YYYYMMDD)
            TO_NUMBER(TO_CHAR(s.order_date, 'YYYYMMDD')) AS order_date_id,
            CASE
              WHEN s.ship_date IS NOT NULL
              THEN TO_NUMBER(TO_CHAR(s.ship_date, 'YYYYMMDD'))
            END AS ship_date_id,

            dc.customer_id,
            dp.product_id,
            da.address_id,

            s.quantity,
            s.discount,
            s.sales,
            s.profit,
            s.shipping_cost,
            s.returned_flag,
            CASE
              WHEN s.src_tbl_last_dt IS NOT NULL THEN
                   TO_DATE(s.src_tbl_last_dt, 'YYYY-MM-DD HH24:MI:SS')
              ELSE SYSDATE
            END AS conv_tbl_last_dt
        FROM js_stg_fact_order_line s
        JOIN js_dim_customer dc
          ON dc.customer_code = s.customer_code
        JOIN js_dim_product dp
          ON dp.product_code  = s.product_code
        JOIN js_dim_ship_address da
          ON da.region      = s.region
         AND da.country     = s.country
         AND da.state       = s.state
         AND da.city        = s.city
         AND NVL(da.postal_code,' ') = NVL(s.postal_code,' ')
         AND da.market      = s.market
    ) b
    ON (
         a.order_code = b.order_code
     AND a.product_id = b.product_id
       )
  WHEN MATCHED THEN
    UPDATE SET
      a.order_priority  = b.order_priority,
      a.ship_mode       = b.ship_mode,
      a.order_date_id   = b.order_date_id,
      a.ship_date_id    = b.ship_date_id,
      a.customer_id     = b.customer_id,
      a.address_id      = b.address_id,
      a.quantity        = b.quantity,
      a.discount        = b.discount,
      a.sales           = b.sales,
      a.profit          = b.profit,
      a.shipping_cost   = b.shipping_cost,
      a.returned_flag   = b.returned_flag,
      a.tbl_last_dt     = b.conv_tbl_last_dt
  WHEN NOT MATCHED THEN
    INSERT (
      fact_id,
      order_code,
      order_priority,
      ship_mode,
      order_date_id,
      ship_date_id,
      customer_id,
      product_id,
      address_id,
      quantity,
      discount,
      sales,
      profit,
      shipping_cost,
      returned_flag,
      tbl_last_dt
    )
    VALUES (
      js_fact_order_line_seq.NEXTVAL,
      b.order_code,
      b.order_priority,
      b.ship_mode,
      b.order_date_id,
      b.ship_date_id,
      b.customer_id,
      b.product_id,
      b.address_id,
      b.quantity,
      b.discount,
      b.sales,
      b.profit,
      b.shipping_cost,
      b.returned_flag,
      b.conv_tbl_last_dt
    );

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
         err_code := SQLCODE;
         err_msg  := SQLERRM;
         DBMS_OUTPUT.put_line('Error in load_merge_fact_order_line - code '
                              || err_code || ': ' || err_msg);
END;
/


  
SET SERVEROUTPUT ON;
EXEC load_merge_fact_order_line;



SELECT 'DIM_CUSTOMER' AS table_name,
       (SELECT COUNT(*) FROM js_stg_dim_customer) AS staging_count,
       (SELECT COUNT(*) FROM js_dim_customer)     AS dw_count
FROM dual
UNION ALL
SELECT 'DIM_PRODUCT',
       (SELECT COUNT(*) FROM js_stg_dim_product),
       (SELECT COUNT(*) FROM js_dim_product)
FROM dual
UNION ALL
SELECT 'DIM_SHIP_ADDRESS',
       (SELECT COUNT(*) FROM js_stg_dim_ship_address),
       (SELECT COUNT(*) FROM js_dim_ship_address)
FROM dual
UNION ALL
SELECT 'DIM_DATE',
       (SELECT COUNT(*) FROM js_dim_date),
       (SELECT COUNT(*) FROM js_dim_date)
FROM dual
UNION ALL
SELECT 'FACT_ORDER_LINE',
       (SELECT COUNT(*) FROM js_stg_fact_order_line),
       (SELECT COUNT(*) FROM js_fact_order_line)
FROM dual;


SELECT * FROM JS_FACT_ORDER_LINE;
SELECT * FROM JS_DIM_CUSTOMER;
SELECT * FROM JS_DIM_PRODUCT;
SELECT * FROM JS_DIM_SHIP_ADDRESS;
SELECT * FROM JS_DIM_DATE;




