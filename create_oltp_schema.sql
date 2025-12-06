USE awesomeinc;

-- SEGMENT
CREATE TABLE js_segment (
  segment_id      INT AUTO_INCREMENT PRIMARY KEY,
  segment_name    VARCHAR(50) NOT NULL,
  UNIQUE KEY uk_segment_name (segment_name),
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CUSTOMER
CREATE TABLE js_customer (
  customer_id     INT AUTO_INCREMENT PRIMARY KEY,
  customer_code   VARCHAR(20) NOT NULL,
  customer_name   VARCHAR(100) NOT NULL,
  segment_id      INT NOT NULL,
  UNIQUE KEY uk_customer_code (customer_code),
  KEY fk_cust_segment (segment_id),
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_customer_segment FOREIGN KEY (segment_id) REFERENCES js_segment(segment_id)
);

-- GEO (single table for location normalization)
CREATE TABLE js_geo (
  geo_id          INT AUTO_INCREMENT PRIMARY KEY,
  city            VARCHAR(100),
  state           VARCHAR(100),
  country         VARCHAR(100),
  region          VARCHAR(100),
  market          VARCHAR(100),
  postal_code     VARCHAR(20),
  UNIQUE KEY uk_geo (city, state, country, region, market, postal_code),
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- SHIP MODE
CREATE TABLE js_ship_mode (
  ship_mode_id    INT AUTO_INCREMENT PRIMARY KEY,
  ship_mode_name  VARCHAR(50) NOT NULL,
  UNIQUE KEY uk_ship_mode_name (ship_mode_name),
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CATEGORY / SUBCATEGORY / PRODUCT
CREATE TABLE js_category (
  category_id     INT AUTO_INCREMENT PRIMARY KEY,
  category_name   VARCHAR(100) NOT NULL,
  UNIQUE KEY uk_category_name (category_name),
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE js_subcategory (
  subcategory_id  INT AUTO_INCREMENT PRIMARY KEY,
  subcategory_name VARCHAR(100) NOT NULL,
  category_id     INT NOT NULL,
  UNIQUE KEY uk_subcategory_name (subcategory_name),
  KEY fk_subcat_category (category_id),
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_subcategory_category FOREIGN KEY (category_id) REFERENCES js_category(category_id)
);

CREATE TABLE js_product (
  product_id      INT AUTO_INCREMENT PRIMARY KEY,
  product_code    VARCHAR(30) NOT NULL,
  product_name    VARCHAR(255) NOT NULL,
  subcategory_id  INT NOT NULL,
  UNIQUE KEY uk_product_code (product_code),
  KEY fk_prod_subcat (subcategory_id),
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_product_subcategory FOREIGN KEY (subcategory_id) REFERENCES js_subcategory(subcategory_id)
);

-- ORDER (header)
CREATE TABLE js_order (
  order_id        INT AUTO_INCREMENT PRIMARY KEY,
  order_code      VARCHAR(40) NOT NULL,
  order_date      DATE NOT NULL,
  ship_date       DATE NOT NULL,
  order_priority  VARCHAR(20),
  customer_id     INT NOT NULL,
  ship_mode_id    INT NOT NULL,
  geo_id          INT NOT NULL,
  UNIQUE KEY uk_order_code (order_code),
  KEY fk_order_customer (customer_id),
  KEY fk_order_ship_mode (ship_mode_id),
  KEY fk_order_geo (geo_id),
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_order_customer    FOREIGN KEY (customer_id) REFERENCES js_customer(customer_id),
  CONSTRAINT fk_order_ship_mode   FOREIGN KEY (ship_mode_id) REFERENCES js_ship_mode(ship_mode_id),
  CONSTRAINT fk_order_geo         FOREIGN KEY (geo_id)        REFERENCES js_geo(geo_id)
);

-- ORDER ITEM (detail)
CREATE TABLE js_order_product (
  order_id        INT NOT NULL,
  product_id      INT NOT NULL,
  quantity        INT NOT NULL,
  discount        DECIMAL(5,2) NULL,
  sales           DECIMAL(12,2) NOT NULL,
  profit          DECIMAL(12,2) NOT NULL,
  shipping_cost   DECIMAL(12,2) NOT NULL,
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (order_id, product_id),
  KEY fk_op_product (product_id),
  CONSTRAINT fk_op_order   FOREIGN KEY (order_id)  REFERENCES js_order(order_id),
  CONSTRAINT fk_op_product FOREIGN KEY (product_id) REFERENCES js_product(product_id)
);

-- RETURN (optional per order)
CREATE TABLE js_return (
  order_id        INT PRIMARY KEY,
  returned_flag   ENUM('Y','N') NOT NULL,
  tbl_last_dt     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_return_order FOREIGN KEY (order_id) REFERENCES js_order(order_id)
);

-- CDC triggers (same pattern for all tables)
DELIMITER $$
CREATE TRIGGER ti_js_customer BEFORE INSERT ON js_customer
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); END IF;
END$$
CREATE TRIGGER tu_js_customer BEFORE UPDATE ON js_customer
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;


-- ====================
-- js_segment triggers
-- ====================
DELIMITER $$
CREATE TRIGGER ti_js_segment BEFORE INSERT ON js_segment
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN 
    SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); 
  END IF;
END$$

CREATE TRIGGER tu_js_segment BEFORE UPDATE ON js_segment
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;

-- ====================
-- js_geo triggers
-- ====================
DELIMITER $$
CREATE TRIGGER ti_js_geo BEFORE INSERT ON js_geo
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN 
    SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); 
  END IF;
END$$

CREATE TRIGGER tu_js_geo BEFORE UPDATE ON js_geo
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;

-- ====================
-- js_ship_mode triggers
-- ====================
DELIMITER $$
CREATE TRIGGER ti_js_ship_mode BEFORE INSERT ON js_ship_mode
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN 
    SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); 
  END IF;
END$$

CREATE TRIGGER tu_js_ship_mode BEFORE UPDATE ON js_ship_mode
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;

-- ====================
-- js_category triggers
-- ====================
DELIMITER $$
CREATE TRIGGER ti_js_category BEFORE INSERT ON js_category
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN 
    SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); 
  END IF;
END$$

CREATE TRIGGER tu_js_category BEFORE UPDATE ON js_category
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;

-- ====================
-- js_subcategory triggers
-- ====================
DELIMITER $$
CREATE TRIGGER ti_js_subcategory BEFORE INSERT ON js_subcategory
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN 
    SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); 
  END IF;
END$$

CREATE TRIGGER tu_js_subcategory BEFORE UPDATE ON js_subcategory
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;

-- ====================
-- js_product triggers
-- ====================
DELIMITER $$
CREATE TRIGGER ti_js_product BEFORE INSERT ON js_product
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN 
    SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); 
  END IF;
END$$

CREATE TRIGGER tu_js_product BEFORE UPDATE ON js_product
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;

-- ====================
-- js_order triggers
-- ====================
DELIMITER $$
CREATE TRIGGER ti_js_order BEFORE INSERT ON js_order
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN 
    SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); 
  END IF;
END$$

CREATE TRIGGER tu_js_order BEFORE UPDATE ON js_order
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;

-- ====================
-- js_order_product triggers
-- ====================
DELIMITER $$
CREATE TRIGGER ti_js_order_product BEFORE INSERT ON js_order_product
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN 
    SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); 
  END IF;
END$$

CREATE TRIGGER tu_js_order_product BEFORE UPDATE ON js_order_product
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;

-- ====================
-- js_return triggers
-- ====================
DELIMITER $$
CREATE TRIGGER ti_js_return BEFORE INSERT ON js_return
FOR EACH ROW BEGIN
  IF NEW.tbl_last_dt IS NULL THEN 
    SET NEW.tbl_last_dt = CURRENT_TIMESTAMP(); 
  END IF;
END$$

CREATE TRIGGER tu_js_return BEFORE UPDATE ON js_return
FOR EACH ROW BEGIN
  SET NEW.tbl_last_dt = CURRENT_TIMESTAMP();
END$$
DELIMITER ;
