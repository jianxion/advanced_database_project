use awesomeinc;

CREATE OR REPLACE VIEW vw_dw_dim_customer AS
SELECT
  c.customer_id,
  c.customer_code,
  c.customer_name,
  s.segment_name,
  NOW() AS tbl_last_dt
FROM js_customer c
JOIN js_segment  s ON s.segment_id = c.segment_id;


CREATE OR REPLACE VIEW vw_dw_dim_product AS
SELECT
  p.product_id,
  p.product_code,
  REPLACE(p.product_name, '"', '') AS product_name,
  sc.subcategory_name,
  c.category_name,
  NOW() AS tbl_last_dt
FROM js_product p
JOIN js_subcategory sc ON sc.subcategory_id = p.subcategory_id
JOIN js_category    c  ON c.category_id  = sc.category_id;


CREATE OR REPLACE VIEW vw_dw_dim_ship_address AS
SELECT
  g.geo_id          AS address_id,
  g.region, g.country, g.state, g.city, g.postal_code, g.market,
  NOW() AS tbl_last_dt
FROM js_geo g;


CREATE OR REPLACE VIEW vw_dw_fact_order_line AS
SELECT
  -- identifiers / degenerate dimensions
  o.order_code,
  DATE_FORMAT(o.order_date, '%Y-%m-%d')    AS order_date,
  DATE_FORMAT(o.ship_date,  '%Y-%m-%d')    AS ship_date,

  -- natural keys for dimensions
  c.customer_code,
  p.product_code,
  g.region,
  g.country,
  g.state,
  g.city,
  g.postal_code,
  g.market,

  -- low-cardinality attributes you put directly in the fact
  o.order_priority,
  sm.ship_mode_name                         AS ship_mode,

  -- measures
  op.quantity,
  op.discount,
  op.sales,
  op.profit,
  op.shipping_cost,

  -- return flag
  COALESCE(r.returned_flag, 'N')            AS returned_flag,

  -- source timestamp for ETL auditing
  NOW()                                     AS src_tbl_last_dt

FROM js_order_product op
JOIN js_order      o  ON o.order_id      = op.order_id
JOIN js_customer   c  ON c.customer_id   = o.customer_id
JOIN js_product    p  ON p.product_id    = op.product_id
JOIN js_geo        g  ON g.geo_id        = o.geo_id
JOIN js_ship_mode  sm ON sm.ship_mode_id = o.ship_mode_id
LEFT JOIN js_return r ON r.order_id      = o.order_id;


SELECT COUNT(*) FROM vw_dw_fact_order_line;

SELECT COUNT(*) FROM vw_dw_dim_customer;

SELECT COUNT(*) FROM vw_dw_dim_product;

SELECT COUNT(*) FROM vw_dw_dim_ship_address;

-- csv file in D:\XAMPP\mysql\data\awesomeinc
SELECT * FROM vw_dw_dim_customer
INTO OUTFILE 'js_dim_customer.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

SELECT * FROM vw_dw_dim_product
INTO OUTFILE 'js_dim_product.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

SELECT * FROM vw_dw_dim_ship_address
INTO OUTFILE 'js_dim_ship_address.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

SELECT * FROM vw_dw_fact_order_line
INTO OUTFILE 'js_fact_order_line.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

-- incremental etl
SELECT COUNT(*) AS rows_before
FROM js_order_product op
JOIN js_order o ON o.order_id = op.order_id
JOIN js_geo   g ON g.geo_id   = o.geo_id
WHERE g.region = 'Oceania'
  AND o.order_priority = 'High';
  
  SELECT o.order_id,
       o.order_code,
       o.order_priority,
       g.region,
       op.product_id,
       op.shipping_cost
FROM js_order_product op
JOIN js_order o ON o.order_id = op.order_id
JOIN js_geo   g ON g.geo_id   = o.geo_id
WHERE g.region = 'Oceania'
  AND o.order_priority = 'High'
ORDER BY o.order_id
LIMIT 20;


UPDATE js_order_product op
JOIN js_order o ON o.order_id = op.order_id
JOIN js_geo   g ON g.geo_id   = o.geo_id
SET op.shipping_cost = ROUND(op.shipping_cost * 1.10, 2)
WHERE g.region = 'Oceania'
  AND o.order_priority = 'High';
  
  
  SELECT o.order_id,
       o.order_code,
       o.order_priority,
       g.region,
       op.product_id,
       op.shipping_cost
FROM js_order_product op
JOIN js_order o ON o.order_id = op.order_id
JOIN js_geo   g ON g.geo_id   = o.geo_id
WHERE g.region = 'Oceania'
  AND o.order_priority = 'High'
ORDER BY o.order_id
LIMIT 20;

SELECT f.*
FROM vw_dw_fact_order_line f
JOIN js_order o ON f.order_code = o.order_code
JOIN js_geo   g ON g.geo_id     = o.geo_id
WHERE g.region = 'Oceania'
  AND o.order_priority = 'High'
ORDER BY o.order_id;


SELECT f.*
FROM vw_dw_fact_order_line f
JOIN js_order o ON f.order_code = o.order_code
JOIN js_geo   g ON g.geo_id     = o.geo_id
WHERE g.region = 'Oceania'
  AND o.order_priority = 'High'
INTO OUTFILE 'js_fact_order_line_inc.csv'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';






