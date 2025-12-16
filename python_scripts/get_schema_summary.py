import psycopg2
import json
from collections import defaultdict

DB_CONFIG = {
    "host": "10.24.26.80",
    "port": 5432,
    "dbname": "tpc_ds_100gb",
    "user": "himanshu",
    "password": "19011903"
}

SCHEMA = "public"
OUTPUT_FILE = "tpcds_schema_summary.json"

TPCDS_TABLES = {
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics",
    "date_dim", "household_demographics", "income_band",
    "inventory", "item", "promotion", "reason", "ship_mode",
    "store", "store_returns", "store_sales", "time_dim",
    "warehouse", "web_page", "web_returns", "web_sales", "web_site"
}

FACT_TABLES = {
    "store_sales", "store_returns",
    "catalog_sales", "catalog_returns",
    "web_sales", "web_returns",
    "inventory"
}

IGNORE_DATE_SUFFIXES = (
    "rec_start_date", "rec_end_date",
    "open_date_sk", "closed_date_sk"
)

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

schema = defaultdict(lambda: {
    "pk": [],
    "cols": [],
    "fks": []
})

# -----------------------------
# COLUMNS
# -----------------------------
cur.execute("""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = %s
""", (SCHEMA,))

for table, col in cur.fetchall():
    t = table.lower()
    if t not in TPCDS_TABLES:
        continue
    schema[t.upper()]["cols"].append(col.upper())

# -----------------------------
# PRIMARY KEYS (dimensions only)
# -----------------------------
cur.execute("""
    SELECT tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = %s
""", (SCHEMA,))

for table, col in cur.fetchall():
    t = table.lower()
    if t in FACT_TABLES or t not in TPCDS_TABLES:
        continue
    schema[t.upper()]["pk"].append(col.upper())

# -----------------------------
# FOREIGN KEYS (filtered)
# -----------------------------
cur.execute("""
    SELECT
        tc.table_name,
        kcu.column_name,
        ccu.table_name,
        ccu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = %s
""", (SCHEMA,))

for table, col, ref_table, ref_col in cur.fetchall():
    t = table.lower()
    if t not in TPCDS_TABLES:
        continue

    col_lc = col.lower()
    if col_lc.endswith(IGNORE_DATE_SUFFIXES):
        continue

    schema[t.upper()]["fks"].append({
        "col": col.upper(),
        "ref_table": ref_table.upper(),
        "ref_col": ref_col.upper(),
        "inferred": False
    })

with open(OUTPUT_FILE, "w") as f:
    json.dump(schema, f, indent=2, sort_keys=True)

print(f"Clean TPC-DS schema written to {OUTPUT_FILE}")

cur.close()
conn.close()
