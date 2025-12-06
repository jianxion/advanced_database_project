USE awesomeinc;

CREATE TABLE stg_orders (
  row_id INT,
  `Order ID` VARCHAR(30),
  `Order Date` VARCHAR(20),
  `Ship Date` VARCHAR(20),
  `Ship Mode` VARCHAR(40),
  `Customer ID` VARCHAR(20),
  `Customer Name` VARCHAR(100),
  `Segment` VARCHAR(50),
  `Postal Code` VARCHAR(20),
  `City` VARCHAR(50),
  `State` VARCHAR(50),
  `Country` VARCHAR(50),
  `Region` VARCHAR(50),
  `Market` VARCHAR(50),
  `Product ID` VARCHAR(40),
  `Category` VARCHAR(50),
  `Sub-Category` VARCHAR(100),
  `Product Name` VARCHAR(200),
  `Sales` VARCHAR(30),
  `Quantity` VARCHAR(30),
  `Discount` VARCHAR(30),
  `Profit` VARCHAR(30),
  `Shipping Cost` VARCHAR(30),
  `Order Priority` VARCHAR(30)
);


CREATE TABLE IF NOT EXISTS stg_returns (
  `Returned` VARCHAR(16),
  `Order ID` VARCHAR(40),
  `Region`   VARCHAR(50)
);


TRUNCATE TABLE stg_orders;

LOAD DATA LOCAL INFILE 'C:/Oracle/Awesome_Inc_Superstore_Orders.csv'
INTO TABLE stg_orders
CHARACTER SET latin1           -- matches the file better than utf8mb4 for this dataset
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'     -- Windows CSV, usually \r\n
IGNORE 1 LINES
(row_id, `Order ID`, `Order Date`, `Ship Date`, `Ship Mode`,
 `Customer ID`, `Customer Name`, `Segment`, `Postal Code`,
 `City`, `State`, `Country`, `Region`, `Market`,
 `Product ID`, `Category`, `Sub-Category`, `Product Name`,
 `Sales`, `Quantity`, `Discount`, `Profit`, `Shipping Cost`, `Order Priority`);

 select * from stg_orders;

SELECT COUNT(*) FROM stg_orders;


TRUNCATE TABLE stg_returns;

LOAD DATA LOCAL INFILE 'C:/Oracle/Awesome_Inc_Superstore_Returns.csv'
INTO TABLE stg_returns
CHARACTER SET latin1
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`Returned`, `Order ID`, `Region`);

select * from stg_returns;

-- SEGMENT
INSERT INTO js_segment(segment_name)
SELECT DISTINCT s.`Segment`
FROM stg_orders s
WHERE s.`Segment` IS NOT NULL
  AND s.`Segment` <> ''
ON DUPLICATE KEY UPDATE segment_name = VALUES(segment_name);

SELECT * FROM js_segment;

-- GEO
INSERT INTO js_geo(city, state, country, region, market, postal_code)
SELECT DISTINCT s.`City`, s.`State`, s.`Country`, s.`Region`, s.`Market`, s.`Postal Code`
FROM stg_orders s
ON DUPLICATE KEY UPDATE city = VALUES(city);  -- no-op for uniqueness; keeps tbl_last_dt fresh

-- SHIP MODE
INSERT INTO js_ship_mode(ship_mode_name)
SELECT DISTINCT s.`Ship Mode`
FROM stg_orders s
WHERE s.`Ship Mode` IS NOT NULL AND s.`Ship Mode` <> ''
ON DUPLICATE KEY UPDATE ship_mode_name = VALUES(ship_mode_name);

-- CATEGORY
INSERT INTO js_category(category_name)
SELECT DISTINCT s.`Category`
FROM stg_orders s
WHERE s.`Category` IS NOT NULL AND s.`Category` <> ''
ON DUPLICATE KEY UPDATE category_name = VALUES(category_name);

-- SUBCATEGORY
INSERT INTO js_subcategory(subcategory_name, category_id)
SELECT DISTINCT s.`Sub-Category`,
       c.category_id
FROM stg_orders s
JOIN js_category c ON c.category_name = s.`Category`
WHERE s.`Sub-Category` IS NOT NULL AND s.`Sub-Category` <> ''
ON DUPLICATE KEY UPDATE subcategory_name = VALUES(subcategory_name);

-- PRODUCT
INSERT INTO js_product(product_code, product_name, subcategory_id)
SELECT DISTINCT s.`Product ID`, s.`Product Name`, sc.subcategory_id
FROM stg_orders s
JOIN js_subcategory sc ON sc.subcategory_name = s.`Sub-Category`
ON DUPLICATE KEY UPDATE product_name = VALUES(product_name), subcategory_id = VALUES(subcategory_id);

-- CUSTOMER (requires Segment FK)
INSERT INTO js_customer(customer_code, customer_name, segment_id)
SELECT DISTINCT s.`Customer ID`, s.`Customer Name`, seg.segment_id
FROM stg_orders s
JOIN js_segment seg ON seg.segment_name = s.`Segment`
ON DUPLICATE KEY UPDATE customer_name = VALUES(customer_name), segment_id = VALUES(segment_id);

truncate table js_order;
-- ORDERS
INSERT INTO js_order(order_code, order_date, ship_date, order_priority, customer_id, ship_mode_id, geo_id)
SELECT DISTINCT
  s.`Order ID`,
  STR_TO_DATE(s.`Order Date`, '%m/%d/%Y'),
  STR_TO_DATE(s.`Ship Date`,  '%m/%d/%Y'),
  s.`Order Priority`,
  c.customer_id,
  sm.ship_mode_id,
  g.geo_id
FROM stg_orders s
JOIN js_customer   c  ON c.customer_code = s.`Customer ID`
JOIN js_ship_mode  sm ON sm.ship_mode_name = s.`Ship Mode`
JOIN js_geo        g  ON g.city = s.`City`
                      AND COALESCE(g.state,'')   = COALESCE(s.`State`,'')
                      AND COALESCE(g.country,'') = COALESCE(s.`Country`,'')
                      AND COALESCE(g.region,'')  = COALESCE(s.`Region`,'')
                      AND COALESCE(g.market,'')  = COALESCE(s.`Market`,'')
                      AND COALESCE(g.postal_code,'') = COALESCE(s.`Postal Code`,'')
ON DUPLICATE KEY UPDATE order_priority = VALUES(order_priority),
                        customer_id    = VALUES(customer_id),
                        ship_mode_id   = VALUES(ship_mode_id),
                        geo_id         = VALUES(geo_id),
                        order_date     = VALUES(order_date),
                        ship_date      = VALUES(ship_date);



INSERT INTO js_order_product(
  order_id,
  product_id,
  quantity,
  discount,
  sales,
  profit,
  shipping_cost
)
SELECT
  o.order_id,
  p.product_id,

  -- Quantity: simple integer cast
  COALESCE(CAST(NULLIF(s.`Quantity`, '') AS UNSIGNED), 0) AS quantity,

  -- Discount: already in decimal form (0, 0.1, 0.2, ...)
  COALESCE(CAST(NULLIF(s.`Discount`, '') AS DECIMAL(5,4)), 0) AS discount,

  -- Sales: strip $ and , then cast
  COALESCE(
    CAST(
      REPLACE(
        REPLACE(NULLIF(s.`Sales`, ''), '$', ''),
      ',', '') AS DECIMAL(12,2)
    ),
    0
  ) AS sales,

  -- Profit: strip $ and , then cast (negative values handled by CAST)
  COALESCE(
    CAST(
      REPLACE(
        REPLACE(NULLIF(s.`Profit`, ''), '$', ''),
      ',', '') AS DECIMAL(12,2)
    ),
    0
  ) AS profit,

  -- Shipping Cost: usually plain decimal; still strip $ just in case
  COALESCE(
    CAST(
      REPLACE(
        REPLACE(NULLIF(s.`Shipping Cost`, ''), '$', ''),
      ',', '') AS DECIMAL(10,2)
    ),
    0
  ) AS shipping_cost

FROM stg_orders s
JOIN js_order   o ON o.order_code   = s.`Order ID`
JOIN js_product p ON p.product_code = s.`Product ID`
ON DUPLICATE KEY UPDATE 
  quantity      = VALUES(quantity),
  discount      = VALUES(discount),
  sales         = VALUES(sales),
  profit        = VALUES(profit),
  shipping_cost = VALUES(shipping_cost);


-- RETURNS
INSERT INTO js_return (order_id, returned_flag)
SELECT DISTINCT
  o.order_id,
  'Y' AS returned_flag
FROM stg_returns r
JOIN js_order o
  ON o.order_code = r.`Order ID`
WHERE UPPER(TRIM(r.`Returned`)) IN ('Y','YES','TRUE','T','RETURNED')
ON DUPLICATE KEY UPDATE
  returned_flag = VALUES(returned_flag),
  tbl_last_dt   = CURRENT_TIMESTAMP();


INSERT INTO js_return (order_id, returned_flag)
SELECT o.order_id, 'N'
FROM js_order o
LEFT JOIN js_return r ON r.order_id = o.order_id
WHERE r.order_id IS NULL;

SELECT returned_flag, COUNT(*) AS returns_count FROM js_return GROUP BY returned_flag;

select * from js_order_product;
select * from js_ship_mode;
select * from js_order;
select * from stg_orders;
select * from js_product;
select * from js_category;
select * from js_customer;
select count(*) from js_order;
select * from js_order where order_priority = 'High' order by customer_id desc;


