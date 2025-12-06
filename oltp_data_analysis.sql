-- =========================================
-- DATA ANALYSIS QUERIES
-- Awesome Inc Superstore Analysis
-- =========================================

USE awesomeinc;

-- =========================================
-- 1. SALES OVERVIEW
-- =========================================

SELECT 'js_segment'       AS table_name, COUNT(*) AS row_count FROM js_segment
UNION ALL
SELECT 'js_customer',     COUNT(*) FROM js_customer
UNION ALL
SELECT 'js_geo',          COUNT(*) FROM js_geo
UNION ALL
SELECT 'js_ship_mode',    COUNT(*) FROM js_ship_mode
UNION ALL
SELECT 'js_category',     COUNT(*) FROM js_category
UNION ALL
SELECT 'js_subcategory',  COUNT(*) FROM js_subcategory
UNION ALL
SELECT 'js_product',      COUNT(*) FROM js_product
UNION ALL
SELECT 'js_order',        COUNT(*) FROM js_order
UNION ALL
SELECT 'js_order_product',COUNT(*) FROM js_order_product
UNION ALL
SELECT 'js_return',       COUNT(*) FROM js_return;



-- Total Sales, Profit, and Order Count
SELECT 
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(op.quantity) as total_quantity_sold,
    CONCAT('$', FORMAT(SUM(op.sales), 2)) as total_sales,
    CONCAT('$', FORMAT(SUM(op.profit), 2)) as total_profit,
    CONCAT(FORMAT((SUM(op.profit) / SUM(op.sales) * 100), 2), '%') as profit_margin,
    CONCAT('$', FORMAT(AVG(op.sales), 2)) as avg_order_value
FROM js_order o
JOIN js_order_product op ON o.order_id = op.order_id;

-- =========================================
-- 2. TOP PERFORMING PRODUCTS
-- =========================================

-- Top 10 Products by Sales
SELECT 
    p.product_name,
    sc.subcategory_name,
    c.category_name,
    COUNT(op.order_product_id) as times_ordered,
    SUM(op.quantity) as units_sold,
    CONCAT('$', FORMAT(SUM(op.sales), 2)) as total_sales,
    CONCAT('$', FORMAT(SUM(op.profit), 2)) as total_profit,
    CONCAT(FORMAT((SUM(op.profit) / SUM(op.sales) * 100), 2), '%') as profit_margin
FROM js_order_product op
JOIN js_product p ON op.product_id = p.product_id
JOIN js_subcategory sc ON p.subcategory_id = sc.subcategory_id
JOIN js_category c ON sc.category_id = c.category_id
GROUP BY p.product_id, p.product_name, sc.subcategory_name, c.category_name
ORDER BY SUM(op.sales) DESC
LIMIT 10;

-- =========================================
-- 3. CATEGORY PERFORMANCE
-- =========================================

-- Sales by Category
SELECT 
    c.category_name,
    COUNT(DISTINCT op.order_id) as order_count,
    SUM(op.quantity) as units_sold,
    CONCAT('$', FORMAT(SUM(op.sales), 2)) as total_sales,
    CONCAT('$', FORMAT(SUM(op.profit), 2)) as total_profit,
    CONCAT(FORMAT((SUM(op.profit) / SUM(op.sales) * 100), 2), '%') as profit_margin,
    CONCAT(FORMAT((SUM(op.sales) / (SELECT SUM(sales) FROM js_order_product) * 100), 2), '%') as sales_percentage
FROM js_order_product op
JOIN js_product p ON op.product_id = p.product_id
JOIN js_subcategory sc ON p.subcategory_id = sc.subcategory_id
JOIN js_category c ON sc.category_id = c.category_id
GROUP BY c.category_id, c.category_name
ORDER BY SUM(op.sales) DESC;

-- =========================================
-- 4. CUSTOMER ANALYSIS
-- =========================================

-- Top 10 Customers by Sales
SELECT 
    cu.customer_name,
    s.segment_name,
    COUNT(DISTINCT o.order_id) as order_count,
    SUM(op.quantity) as items_purchased,
    CONCAT('$', FORMAT(SUM(op.sales), 2)) as total_spent,
    CONCAT('$', FORMAT(SUM(op.profit), 2)) as profit_generated,
    CONCAT('$', FORMAT(AVG(op.sales), 2)) as avg_order_value
FROM js_customer cu
JOIN js_segment s ON cu.segment_id = s.segment_id
JOIN js_order o ON cu.customer_id = o.customer_id
JOIN js_order_product op ON o.order_id = op.order_id
GROUP BY cu.customer_id, cu.customer_name, s.segment_name
ORDER BY SUM(op.sales) DESC
LIMIT 10;

-- Customer Segment Analysis
SELECT 
    s.segment_name,
    COUNT(DISTINCT cu.customer_id) as customer_count,
    COUNT(DISTINCT o.order_id) as total_orders,
    CONCAT('$', FORMAT(SUM(op.sales), 2)) as total_sales,
    CONCAT('$', FORMAT(AVG(op.sales), 2)) as avg_order_value,
    CONCAT(FORMAT((SUM(op.sales) / (SELECT SUM(sales) FROM js_order_product) * 100), 2), '%') as sales_percentage
FROM js_segment s
JOIN js_customer cu ON s.segment_id = cu.segment_id
JOIN js_order o ON cu.customer_id = o.customer_id
JOIN js_order_product op ON o.order_id = op.order_id
GROUP BY s.segment_id, s.segment_name
ORDER BY SUM(op.sales) DESC;

-- =========================================
-- 5. GEOGRAPHIC ANALYSIS
-- =========================================

-- Sales by Region
SELECT 
    g.region,
    g.market,
    COUNT(DISTINCT o.order_id) as order_count,
    CONCAT('$', FORMAT(SUM(op.sales), 2)) as total_sales,
    CONCAT('$', FORMAT(SUM(op.profit), 2)) as total_profit,
    CONCAT(FORMAT((SUM(op.profit) / SUM(op.sales) * 100), 2), '%') as profit_margin
FROM js_geo g
JOIN js_order o ON g.geo_id = o.geo_id
JOIN js_order_product op ON o.order_id = op.order_id
GROUP BY g.region, g.market
ORDER BY SUM(op.sales) DESC;

-- Top 10 Cities by Sales
SELECT 
    g.city,
    g.state,
    g.country,
    COUNT(DISTINCT o.order_id) as order_count,
    CONCAT('$', FORMAT(SUM(op.sales), 2)) as total_sales,
    CONCAT('$', FORMAT(SUM(op.profit), 2)) as total_profit
FROM js_geo g
JOIN js_order o ON g.geo_id = o.geo_id
JOIN js_order_product op ON o.order_id = op.order_id
GROUP BY g.geo_id, g.city, g.state, g.country
ORDER BY SUM(op.sales) DESC
LIMIT 10;

-- =========================================
-- 6. TIME-BASED ANALYSIS
-- =========================================

-- Sales by Year
SELECT 
    YEAR(o.order_date) as year,
    COUNT(DISTINCT o.order_id) as order_count,
    CONCAT('$', FORMAT(SUM(op.sales), 2)) as total_sales,
    CONCAT('$', FORMAT(SUM(op.profit), 2)) as total_profit,
    CONCAT(FORMAT((SUM(op.profit) / SUM(op.sales) * 100), 2), '%') as profit_margin
FROM js_order o
JOIN js_order_product op ON o.order_id = op.order_id
GROUP BY YEAR(o.order_date)
ORDER BY year;

-- Record counts by table
SELECT 'Customers' as table_name, COUNT(*) as record_count FROM js_customer
UNION ALL
SELECT 'Products', COUNT(*) FROM js_product
UNION ALL
SELECT 'Orders', COUNT(*) FROM js_order
UNION ALL
SELECT 'Order Items', COUNT(*) FROM js_order_product
UNION ALL
SELECT 'Categories', COUNT(*) FROM js_category
UNION ALL
SELECT 'Subcategories', COUNT(*) FROM js_subcategory
UNION ALL
SELECT 'Segments', COUNT(*) FROM js_segment
UNION ALL
SELECT 'Geo Locations', COUNT(*) FROM js_geo
UNION ALL
SELECT 'Ship Modes', COUNT(*) FROM js_ship_mode;
