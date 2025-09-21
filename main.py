import os, sys, getpass, argparse
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from tabulate import tabulate
except Exception:
    tabulate = None

QUERIES = {
    "Q1_LIMIT10": """
        SELECT * FROM orders LIMIT 10;
    """,
    "Q2_WHERE_ORDER_BY": """
        SELECT order_id, order_status, order_purchase_timestamp
        FROM orders
        WHERE order_status = 'delivered'
        ORDER BY order_purchase_timestamp DESC
        LIMIT 20;
    """,
    "Q3_GROUP_BY_AGG": """
        SELECT
          order_status,
          COUNT(*) AS cnt,
          AVG(EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp))/86400.0) AS avg_delivery_days,
          MIN(order_purchase_timestamp) AS min_ts,
          MAX(order_purchase_timestamp) AS max_ts
        FROM orders
        GROUP BY order_status
        ORDER BY cnt DESC;
    """,
    "Q4_JOIN_ITEMS_PRODUCTS": """
        SELECT
          oi.order_id,
          p.product_category,
          ROUND(oi.price, 2)   AS price,
          ROUND(oi.freight_value, 2) AS freight
        FROM order_items oi
        JOIN products p ON p.product_id = oi.product_id
        LIMIT 20;
    """,
}

def print_table(rows, headers):
    if not rows:
        print("(no rows)\n")
        return
    if tabulate:
        print(tabulate(rows, headers=headers, tablefmt="github"))
    else:
        # fallback pretty-ish print
        widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
        fmt = " | ".join(f"{{:{w}}}" for w in widths)
        print(fmt.format(*headers))
        print("-+-".join("-" * w for w in widths))
        for r in rows:
            print(fmt.format(*[str(x) for x in r]))
    print()

def main():
    ap = argparse.ArgumentParser(description="Run step-4 SQL checks against Postgres.")
    ap.add_argument("--host", default=os.getenv("DB_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "5433")))  # your server is on 5433
    ap.add_argument("--db",   default=os.getenv("DB_NAME", "fecomdb"))
    ap.add_argument("--user", default=os.getenv("DB_USER", "postgres"))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD"))  # optional env
    args = ap.parse_args()

    pwd = args.password or getpass.getpass(f"Password for {args.user}@{args.host}:{args.port}/{args.db}: ")

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.db, user=args.user, password=pwd
    )
    conn.set_client_encoding("UTF8")
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            for name, sql in QUERIES.items():
                print(f"\n=== {name} ===")
                cur.execute(sql)
                rows = cur.fetchall()
                headers = [desc.name for desc in cur.description]
                # convert dict rows to list rows (for tabulate/fallback)
                list_rows = [[row[h] for h in headers] for row in rows]
                print_table(list_rows, headers)
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
