import os
import json
from datetime import datetime
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.engine import URL
from pathlib import Path
load_dotenv(Path(__file__).resolve().parents[1] / ".env")


DB_NAME = os.getenv("DB_NAME", "atlas_coop")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
EXCEL_PATH = os.getenv("EXCEL_PATH", "./data/atlas_coop_raw.xlsx")

# Sheet names in the Excel file
SHEET_CUSTOMERS = "Customers"
SHEET_PRODUCTS = "Products"
SHEET_ORDERS = "Orders"

def make_engine():
    url = URL.create(
        drivername="postgresql+psycopg",
        username=DB_USER,
        password=DB_PASSWORD,     # takes raw password safely
        host=DB_HOST,             # use 127.0.0.1
        port=int(DB_PORT),
        database=DB_NAME,
    )
    return create_engine(url, future=True)

def normalize_phone(x: str | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    # Keep plus and digits only
    s = "".join(ch for ch in s if ch.isdigit() or ch == "+")
    return s if s else None

def parse_date_any(x) -> pd.Timestamp | None:
    if x is None or str(x).strip() == "" or str(x).lower() in {"nan", "none"}:
        return None
    # Try flexible parsing (handles 2025-12-01, 2025/12/01, 01-12-2025, etc.)
    dt = pd.to_datetime(x, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        dt = pd.to_datetime(x, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        return None
    return dt.normalize()

def log_rejects(conn, sheet: str, rows: list[dict], reason: str):
    if not rows:
        return 0
    payload = [{"source_sheet": sheet, "row_data": r, "reason": reason} for r in rows]
    conn.execute(
        text("""
            INSERT INTO atlas.rejected_rows (source_sheet, row_data, reason)
            VALUES (:source_sheet, CAST(:row_data AS jsonb), :reason)
        """),
        # ADD default=str RIGHT HERE:
        [{"source_sheet": p["source_sheet"], "row_data": json.dumps(p["row_data"], default=str), "reason": p["reason"]} for p in payload]
    )
    return len(rows)

def main():
    engine = make_engine()

    # --- Read Excel ---
    customers_raw = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_CUSTOMERS, dtype=str)
    products_raw  = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_PRODUCTS, dtype=str)
    orders_raw    = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_ORDERS, dtype=str)

   # ---------------------------------------------------------
    # 1. FIX COLUMN HEADERS (The "Still 0" Fix)
    # ---------------------------------------------------------
    # Force-rename the messy Excel headers to match your Postgres table exactly
    orders_raw = orders_raw.rename(columns={
        "qtv": "qty",                      # Fix typo from Excel
        "payment method": "payment_method", # Fix missing underscore
        "paid amount": "paid_amount",       # Fix missing underscore
        " currency": "currency"             # Fix leading space
    })

    # Optional: Strip whitespace from all other columns just in case
    orders_raw.columns = orders_raw.columns.str.strip()

    # --- Load to staging (raw as-is) ---
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE atlas.stg_customers, atlas.stg_products, atlas.stg_orders;"))

    customers_raw.to_sql('stg_customers', con=engine, schema='atlas', if_exists='append', index=False)
    products_raw.to_sql('stg_products', con=engine, schema='atlas', if_exists='append', index=False)

    # THE TRAP FOR ORDERS
    try:
        print("Attempting to load 10k Orders...")
        orders_raw.to_sql('stg_orders', con=engine, schema='atlas', if_exists='append', index=False)
        print("✅ Orders loaded successfully!")
    except Exception as e:
        print("\n❌ DATABASE REJECTED THE ORDERS. HERE IS THE EXACT REASON:")
        print(e.__cause__)

    # ---------------------------------------------------------
    # 2. CLEAN DATA TYPES
    # ---------------------------------------------------------
    # Clean dates so mixed formats don't crash PostgreSQL
    orders_raw["order_date"] = orders_raw["order_date"].apply(parse_date_any)
    customers_raw["created_at"] = customers_raw["created_at"].apply(parse_date_any)

    # Clean numbers: Turn text/empty cells into proper numbers or NaN
    orders_raw["qty"] = pd.to_numeric(orders_raw["qty"], errors="coerce")
    orders_raw["paid_amount"] = pd.to_numeric(orders_raw["paid_amount"], errors="coerce")

    # ---------------------------------------------------------
    # 3. FIX DBAPI CRASH (The "Float NaN" Fix)
    # ---------------------------------------------------------
    # Convert Pandas NaN (float) to Python None (SQL NULL)
    # We cast to 'object' first to ensure it accepts None without errors
    customers_raw = customers_raw.astype(object).where(pd.notnull(customers_raw), None)
    products_raw  = products_raw.astype(object).where(pd.notnull(products_raw), None)
    orders_raw    = orders_raw.astype(object).where(pd.notnull(orders_raw), None)

    # Debug: Print to confirm columns are finally correct
    print("✅ Fixed Order Columns:", orders_raw.columns.tolist())

    # --- Load to staging (raw as-is) ---
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE atlas.stg_customers, atlas.stg_products, atlas.stg_orders;"))


    customers_raw.to_sql("stg_customers", engine, schema="atlas", if_exists="append", index=False)
    products_raw.to_sql("stg_products", engine, schema="atlas", if_exists="append", index=False)
    orders_raw.to_sql("stg_orders", engine, schema="atlas", if_exists="append", index=False)

    print("✅ staging loaded:",
      "customers", len(customers_raw),
      "products", len(products_raw),
      "orders", len(orders_raw))

    # --- Transform / Clean ---
    # Customers
    c = customers_raw.copy()
    c["customer_code"] = c["customer_code"].astype(str).str.strip()
    c["full_name"] = c["full_name"].astype(str).str.strip()
    c["phone"] = c["phone"].apply(normalize_phone)
    c["city"] = c["city"].astype(str).str.strip()
    c["created_at"] = c["created_at"].apply(parse_date_any)

    # Reject missing required fields
    bad_c = c[c["customer_code"].isna() | (c["customer_code"] == "") | c["created_at"].isna()]
    c_ok = c.drop(bad_c.index)

    # Dedupe by customer_code (keep first)
    dup_mask = c_ok.duplicated(subset=["customer_code"], keep="first")
    dup_c = c_ok[dup_mask]
    c_ok = c_ok[~dup_mask]

    # Products
    p = products_raw.copy()
    p["product_code"] = p["product_code"].astype(str).str.strip()
    p["product_name"] = p["product_name"].astype(str).str.strip()
    p["category"] = p["category"].astype(str).str.strip()

    def to_num(x):
        try:
            return float(str(x).strip())
        except:
            return None

    def to_int(x):
        try:
            return int(float(str(x).strip()))
        except:
            return None

    def to_bool(x):
        s = str(x).strip().lower()
        if s in {"1", "true", "yes", "y"}:
            return True
        if s in {"0", "false", "no", "n"}:
            return False
        return None

    p["unit_price"] = p["unit_price"].apply(to_num)
    p["stock_qty"] = p["stock_qty"].apply(to_int)
    p["active"] = p["active"].apply(to_bool)

    bad_p = p[p["product_code"].isna() | (p["product_code"] == "") | p["unit_price"].isna() | p["stock_qty"].isna() | p["active"].isna()]
    p_ok = p.drop(bad_p.index)

    # Orders (each row = one item)
    o = orders_raw.copy()
    for col in ["order_ref", "customer_code", "product_code", "payment_method", "currency"]:
        o[col] = o[col].astype(str).str.strip()

    o["order_date"] = o["order_date"].apply(parse_date_any)
    o["qty"] = o["qty"].apply(to_int)
    o["paid_amount"] = o["paid_amount"].apply(to_num)

    # Basic rejects
    bad_o_basic = o[
        o["order_ref"].isna() | (o["order_ref"] == "") |
        o["order_date"].isna() |
        o["qty"].isna() | (o["qty"] <= 0) |
        o["paid_amount"].isna() | (o["paid_amount"] <= 0) |
        o["payment_method"].isna() | (o["payment_method"] == "")
    ]
    o_ok = o.drop(bad_o_basic.index)

    # Validate FK-like references using cleaned sets
    valid_customers = set(c_ok["customer_code"].tolist())
    valid_products  = set(p_ok["product_code"].tolist())

    bad_o_fk = o_ok[~o_ok["customer_code"].isin(valid_customers) | ~o_ok["product_code"].isin(valid_products)]
    o_ok = o_ok.drop(bad_o_fk.index)

    # --- Load into final tables ---
    report = {
        "excel_path": EXCEL_PATH,
        "loaded_at": datetime.now().isoformat(timespec="seconds"),
        "counts": {
            "customers_raw": len(customers_raw),
            "products_raw": len(products_raw),
            "orders_raw": len(orders_raw),
        },
        "rejected": {}
    }

    with engine.begin() as conn:
        # Optional: reset final tables for repeatable demo
        conn.execute(text("TRUNCATE atlas.payments, atlas.order_items, atlas.orders RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE atlas.products, atlas.customers RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE atlas.rejected_rows;"))

        # Log rejects
        report["rejected"]["customers_missing_required"] = log_rejects(conn, "Customers", bad_c.fillna("").to_dict("records"), "Missing required fields: customer_code or created_at")
        report["rejected"]["customers_duplicate_code"]   = log_rejects(conn, "Customers", dup_c.fillna("").to_dict("records"), "Duplicate customer_code (kept first)")

        report["rejected"]["products_invalid_fields"]    = log_rejects(conn, "Products", bad_p.fillna("").to_dict("records"), "Invalid product fields (code/price/stock/active)")

        report["rejected"]["orders_invalid_basic"]       = log_rejects(conn, "Orders", bad_o_basic.fillna("").to_dict("records"), "Invalid order fields (date/qty/amount/method/ref)")
        report["rejected"]["orders_invalid_refs"]        = log_rejects(conn, "Orders", bad_o_fk.fillna("").to_dict("records"), "Invalid reference (unknown customer_code or product_code)")

        # Insert customers
        conn.execute(
            text("""
                INSERT INTO atlas.customers (customer_code, full_name, phone, city, created_at)
                VALUES (:customer_code, :full_name, :phone, :city, :created_at)
                ON CONFLICT (customer_code) DO NOTHING;
            """),
            [
                {
                    "customer_code": r["customer_code"],
                    "full_name": r["full_name"],
                    "phone": r["phone"],
                    "city": r["city"],
                    "created_at": r["created_at"].date() if pd.notna(r["created_at"]) else None
                }
                for r in c_ok.to_dict("records")
            ]
        )

        # Insert products
        conn.execute(
            text("""
                INSERT INTO atlas.products (product_code, product_name, category, unit_price, stock_qty, active)
                VALUES (:product_code, :product_name, :category, :unit_price, :stock_qty, :active)
                ON CONFLICT (product_code) DO NOTHING;
            """),
            p_ok.to_dict("records")
        )

        # Build lookup maps
        cust_map = dict(conn.execute(text("SELECT customer_code, id FROM atlas.customers")).all())
        prod_map = dict(conn.execute(text("SELECT product_code, id FROM atlas.products")).all())

        # Insert orders (unique by order_ref)
        order_rows = []
        for r in o_ok.to_dict("records"):
            order_rows.append({
                "order_ref": r["order_ref"],
                "customer_id": cust_map.get(r["customer_code"]),
                "order_date": r["order_date"].date() if pd.notna(r["order_date"]) else None,
            })

        # Deduplicate orders by order_ref (keep first)
        seen = set()
        uniq_orders = []
        for r in order_rows:
            if r["order_ref"] in seen:
                continue
            seen.add(r["order_ref"])
            uniq_orders.append(r)

        conn.execute(
            text("""
                INSERT INTO atlas.orders (order_ref, customer_id, order_date, status)
                VALUES (:order_ref, :customer_id, :order_date, 'PAID')
                ON CONFLICT (order_ref) DO NOTHING;
            """),
            uniq_orders
        )

        # Load order id map
        order_map = dict(conn.execute(text("SELECT order_ref, id FROM atlas.orders")).all())

        # Insert items + payments
        item_rows = []
        payment_rows = []

        for r in o_ok.to_dict("records"):
            o_id = order_map.get(r["order_ref"])
            pr_id = prod_map.get(r["product_code"])
            if not o_id or not pr_id:
                continue

            # Unit price from products table (current)
            unit_price = conn.execute(
                text("SELECT unit_price FROM atlas.products WHERE id = :pid"),
                {"pid": pr_id}
            ).scalar_one()

            item_rows.append({
                "order_id": o_id,
                "product_id": pr_id,
                "qty": int(r["qty"]),
                "unit_price": float(unit_price),
            })

            payment_rows.append({
                "order_id": o_id,
                "method": r["payment_method"],
                "amount": float(r["paid_amount"]),
                "currency": (r.get("currency") or "MAD")[:3]
            })

        conn.execute(
            text("""
                INSERT INTO atlas.order_items (order_id, product_id, qty, unit_price)
                VALUES (:order_id, :product_id, :qty, :unit_price)
                ON CONFLICT (order_id, product_id) DO NOTHING;
            """),
            item_rows
        )

        conn.execute(
            text("""
                INSERT INTO atlas.payments (order_id, method, amount, currency)
                VALUES (:order_id, :method, :amount, :currency);
            """),
            payment_rows
        )

        # Counts
        report["counts"]["customers_inserted"] = conn.execute(text("SELECT COUNT(*) FROM atlas.customers")).scalar_one()
        report["counts"]["products_inserted"]  = conn.execute(text("SELECT COUNT(*) FROM atlas.products")).scalar_one()
        report["counts"]["orders_inserted"]    = conn.execute(text("SELECT COUNT(*) FROM atlas.orders")).scalar_one()
        report["counts"]["items_inserted"]     = conn.execute(text("SELECT COUNT(*) FROM atlas.order_items")).scalar_one()
        report["counts"]["payments_inserted"]  = conn.execute(text("SELECT COUNT(*) FROM atlas.payments")).scalar_one()
        report["counts"]["rejected_total"]     = conn.execute(text("SELECT COUNT(*) FROM atlas.rejected_rows")).scalar_one()

    # Write report
    os.makedirs("./docs", exist_ok=True)
    with open("./docs/import_report.md", "w", encoding="utf-8") as f:
        f.write("# Import Report\n\n")
        f.write(f"- Loaded at: `{report['loaded_at']}`\n")
        f.write(f"- Excel: `{report['excel_path']}`\n\n")
        f.write("## Counts\n\n")
        for k, v in report["counts"].items():
            f.write(f"- **{k}**: {v}\n")
        f.write("\n## Rejected breakdown\n\n")
        for k, v in report["rejected"].items():
            f.write(f"- **{k}**: {v}\n")

    print("✅ ETL finished. Report written to docs/import_report.md")

if __name__ == "__main__":
    main()
