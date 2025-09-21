-- queries.sql — Fecom E-commerce analytics (10 topics)

-- Q1. Orders & GMV by month (delivered only)
SELECT date_trunc('month', o.order_purchase_timestamp) AS month,
       COUNT(DISTINCT o.order_id)                      AS orders,
       ROUND(SUM(oi.price), 2)                         AS gmv_items,
       ROUND(SUM(oi.freight_value), 2)                 AS freight
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY 1;

-- Q2. Delivery time (days) for delivered orders: avg/min/max
SELECT ROUND(AVG(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0),2) AS avg_days,
       ROUND(MIN(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0),2) AS min_days,
       ROUND(MAX(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0),2) AS max_days
FROM orders o
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL;

-- Q3. Top-10 product categories by revenue
SELECT p.product_category,
       ROUND(SUM(oi.price),2) AS revenue,
       COUNT(*)               AS items_sold
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY 1
ORDER BY revenue DESC NULLS LAST
LIMIT 10;

-- Q4. Repeat-customer rate (≥2 orders)
WITH c AS (
  SELECT customer_trx_id, COUNT(*) AS orders_cnt
  FROM orders
  GROUP BY 1
)
SELECT SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END)        AS repeat_customers,
       COUNT(*)                                                AS total_customers,
       ROUND(100.0 * SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_share_pct
FROM c;

-- Q5. Avg review score by product category
WITH oi_cat AS (
  SELECT DISTINCT oi.order_id, p.product_category
  FROM order_items oi
  JOIN products p ON p.product_id = oi.product_id
)
SELECT oc.product_category,
       ROUND(AVG(r.review_score)::numeric, 2) AS avg_review_score,
       COUNT(r.review_id)                     AS reviews
FROM oi_cat oc
JOIN order_reviews r ON r.order_id = oc.order_id
GROUP BY 1
ORDER BY avg_review_score DESC NULLS LAST, reviews DESC;

-- Q6. Payment mix (share, avg installments/value)
SELECT payment_type,
       COUNT(*)                                                   AS payments,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)         AS share_pct,
       ROUND(AVG(payment_installments)::numeric, 2)               AS avg_installments,
       ROUND(AVG(payment_value)::numeric, 2)                      AS avg_payment_value
FROM order_payments
GROUP BY payment_type
ORDER BY payments DESC;

-- Q7. AOV by month (items + freight, delivered)
WITH ov AS (
  SELECT order_id,
         SUM(price)         AS items_total,
         SUM(freight_value) AS freight_total
  FROM order_items
  GROUP BY 1
)
SELECT date_trunc('month', o.order_purchase_timestamp) AS month,
       ROUND(AVG(ov.items_total + COALESCE(ov.freight_total,0))::numeric, 2) AS aov
FROM orders o
JOIN ov ON ov.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY 1;

-- Q8. Top-10 sellers by revenue
SELECT oi.seller_id,
       ROUND(SUM(oi.price),2)              AS revenue,
       COUNT(DISTINCT oi.order_id)         AS orders,
       COUNT(*)                            AS items
FROM order_items oi
GROUP BY oi.seller_id
ORDER BY revenue DESC
LIMIT 10;

-- Q9. On-time delivery rate (delivered_on_or_before_estimated)
SELECT ROUND(100.0 * SUM(CASE WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*),0), 2) AS on_time_pct,
       COUNT(*) AS delivered_orders
FROM orders o
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
  AND o.order_estimated_delivery_date IS NOT NULL;

-- Q10. Sales by customer country (delivered)
WITH ov AS (
  SELECT order_id, SUM(price) AS items_total
  FROM order_items
  GROUP BY 1
)
SELECT c.customer_country           AS country,
       COUNT(DISTINCT o.order_id)   AS orders,
       ROUND(SUM(ov.items_total),2) AS revenue
FROM orders o
JOIN customers c ON c.customer_trx_id = o.customer_trx_id
JOIN ov ON ov.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY revenue DESC NULLS LAST
LIMIT 15;
