import os, sys, getpass, argparse
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from tabulate import tabulate
except Exception:
    tabulate = None

# ВАЖНО: порядок выполнения сохраняем явно через список кортежей
QUERIES = [
    # ---- БАЗОВЫЕ ПРОВЕРКИ (4b) ----
    ("B1_LIMIT10",
     """
     SELECT * FROM orders LIMIT 10;
     """
    ),
    ("B2_WHERE_ORDER_BY",
     """
     SELECT order_id, order_status, order_purchase_timestamp
     FROM orders
     WHERE order_status = 'delivered'
     ORDER BY order_purchase_timestamp DESC
     LIMIT 20;
     """
    ),
    ("B3_GROUP_BY_AGG",
     """
     SELECT
       order_status,
       COUNT(*) AS cnt,
       AVG(EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp))/86400.0) AS avg_delivery_days,
       MIN(order_purchase_timestamp) AS min_ts,
       MAX(order_purchase_timestamp) AS max_ts
     FROM orders
     GROUP BY order_status
     ORDER BY cnt DESC;
     """
    ),
    ("B4_JOIN_ITEMS_PRODUCTS",
     """
     SELECT
       oi.order_id,
       p.product_category,
       ROUND(oi.price, 2)         AS price,
       ROUND(oi.freight_value, 2) AS freight
     FROM order_items oi
     JOIN products p ON p.product_id = oi.product_id
     LIMIT 20;
     """
    ),

    # ---- АНАЛИТИКА: 10 тем из queries.sql ----
    ("Q1_ORDERS_GMV_BY_MONTH",
     """
     SELECT date_trunc('month', o.order_purchase_timestamp) AS month,
            COUNT(DISTINCT o.order_id)                      AS orders,
            ROUND(SUM(oi.price), 2)                         AS gmv_items,
            ROUND(SUM(oi.freight_value), 2)                 AS freight
     FROM orders o
     JOIN order_items oi ON oi.order_id = o.order_id
     WHERE o.order_status = 'delivered'
     GROUP BY 1
     ORDER BY 1;
     """
    ),
    ("Q2_DELIVERY_TIME_STATS",
     """
     SELECT ROUND(AVG(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0),2) AS avg_days,
            ROUND(MIN(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0),2) AS min_days,
            ROUND(MAX(EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400.0),2) AS max_days
     FROM orders o
     WHERE o.order_status = 'delivered'
       AND o.order_delivered_customer_date IS NOT NULL;
     """
    ),
    ("Q3_TOP10_CATEGORIES_BY_REVENUE",
     """
     SELECT p.product_category,
            ROUND(SUM(oi.price),2) AS revenue,
            COUNT(*)               AS items_sold
     FROM order_items oi
     JOIN products p ON p.product_id = oi.product_id
     GROUP BY 1
     ORDER BY revenue DESC NULLS LAST
     LIMIT 10;
     """
    ),
    ("Q4_REPEAT_CUSTOMER_RATE",
     """
     WITH c AS (
       SELECT customer_trx_id, COUNT(*) AS orders_cnt
       FROM orders
       GROUP BY 1
     )
     SELECT SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END)        AS repeat_customers,
            COUNT(*)                                                AS total_customers,
            ROUND(100.0 * SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_share_pct
     FROM c;
     """
    ),
    ("Q5_AVG_REVIEW_SCORE_BY_CATEGORY",
     """
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
     """
    ),
    ("Q6_PAYMENT_MIX",
     """
     SELECT payment_type,
            COUNT(*)                                                   AS payments,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)         AS share_pct,
            ROUND(AVG(payment_installments)::numeric, 2)               AS avg_installments,
            ROUND(AVG(payment_value)::numeric, 2)                      AS avg_payment_value
     FROM order_payments
     GROUP BY payment_type
     ORDER BY payments DESC;
     """
    ),
    ("Q7_AOV_BY_MONTH_DELIVERED",
     """
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
     """
    ),
    ("Q8_TOP10_SELLERS_BY_REVENUE",
     """
     SELECT oi.seller_id,
            ROUND(SUM(oi.price),2)      AS revenue,
            COUNT(DISTINCT oi.order_id) AS orders,
            COUNT(*)                    AS items
     FROM order_items oi
     GROUP BY oi.seller_id
     ORDER BY revenue DESC
     LIMIT 10;
     """
    ),
    ("Q9_ON_TIME_DELIVERY_RATE",
     """
     SELECT ROUND(100.0 * SUM(CASE WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(*),0), 2) AS on_time_pct,
            COUNT(*) AS delivered_orders
     FROM orders o
     WHERE o.order_status = 'delivered'
       AND o.order_delivered_customer_date IS NOT NULL
       AND o.order_estimated_delivery_date IS NOT NULL;
     """
    ),
    ("Q10_SALES_BY_CUSTOMER_COUNTRY",
     """
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
     """
    ),
]

def print_table(rows, headers):
    if not rows:
        print("(no rows)\n")
        return
    if tabulate:
        print(tabulate(rows, headers=headers, tablefmt="github"))
    else:
        widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
        fmt = " | ".join(f"{{:{w}}}" for w in widths)
        print(fmt.format(*headers))
        print("-+-".join("-" * w for w in widths))
        for r in rows:
            print(fmt.format(*[str(x) for x in r]))
    print()

def main():
    ap = argparse.ArgumentParser(description="Run SQL checks & analytics against Postgres.")
    ap.add_argument("--host", default=os.getenv("DB_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "5433")))  # у тебя сервер на 5433
    ap.add_argument("--db",   default=os.getenv("DB_NAME", "fecomdb"))
    ap.add_argument("--user", default=os.getenv("DB_USER", "postgres"))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD"))  # можно через env
    ap.add_argument("--only", nargs="*", help="Run only specific query names (e.g. Q1_..., B1_...).")
    args = ap.parse_args()

    pwd = args.password or getpass.getpass(f"Password for {args.user}@{args.host}:{args.port}/{args.db}: ")

    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.db, user=args.user, password=pwd)
    conn.set_client_encoding("UTF8")

    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            for name, sql in QUERIES:
                if args.only and name not in args.only:
                    continue
                print(f"\n=== {name} ===")
                cur.execute(sql)
                rows = cur.fetchall()
                headers = [desc.name for desc in cur.description]
                list_rows = [[row[h] for h in headers] for row in rows]
                print_table(list_rows, headers)
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
