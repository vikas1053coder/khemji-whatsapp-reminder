"""
Khemji Wire - Phase 1: PostgreSQL-backed Operations & Inventory System
- PostgreSQL is the single source of truth (Render Postgres add-on, via DATABASE_URL)
- Google Sheets is a real-time, human-readable MIRROR (best-effort, non-blocking)
- Shared PIN login gates the whole app
- Every form shows a confirmation/review screen before final submit
- New items are discovered from real data, not a hardcoded list - adding a new
  wire size/consumable/etc. never needs a code change again.
"""

import os
import json
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request, render_template_string, abort, g, session, redirect, url_for
import psycopg2
import gspread
from google.oauth2.service_account import Credentials

IST = ZoneInfo("Asia/Kolkata")


def now_ist():
    return datetime.now(IST)


def default_entry_time():
    return now_ist().strftime("%Y-%m-%dT%H:%M")


def parse_entry_datetime(form):
    raw = form.get("entry_time", "").strip()
    if raw:
        try:
            dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M")
            return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
        except ValueError:
            pass
    n = now_ist()
    return n.strftime("%Y-%m-%d"), n.strftime("%H:%M:%S")


def safe_float(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


# ============ CONFIG ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "changeme123")
SHARED_PIN = os.environ.get("SHARED_PIN", "1234")
SECRET_KEY = os.environ.get("SECRET_KEY", "please-change-this-in-render-env-vars")

ALL_PEOPLE_NAMES = ["Vikas", "Prakash", "Subodh", "Rana", "Mahesh", "Monu"]

TEMP_RANGES = {
    "T1": {"low": 445, "high": 455},
}

# Seed lists - just populate dropdowns with sensible defaults from day one.
# Anything typed in via "+ Add Another" becomes real data immediately and will
# appear in dropdowns/stock automatically from then on - no code change needed.
SEED_ITEMS = {
    "Consumables": ["Zinc", "FO", "Lead", "Galva Flux", "Coal", "Charcoal"],
    "Raw Material": ["5.5 mm", "6.00 mm"],
    "Finished Goods": ["1.25 mm", "1.40 mm", "1.60 mm", "1.60 mm S", "1.80 mm", "2.00 mm",
                       "2.25 mm", "2.50 mm", "3.00 mm", "4.00 mm", "Strip 16 Kg", "Strip 23 KG"],
}

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "your_google_sheet_id_here")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
# ==================================================

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=18)


# ---------- Database ----------
def normalize_db_url(url):
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def get_db_connection():
    if not hasattr(g, "_db_conn"):
        g._db_conn = psycopg2.connect(normalize_db_url(DATABASE_URL), sslmode="require")
    return g._db_conn


@app.teardown_appcontext
def close_db_connection(exception=None):
    conn = g.pop("_db_conn", None)
    if conn is not None:
        conn.close()


CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS readings (
    id SERIAL PRIMARY KEY,
    entry_date DATE NOT NULL,
    entry_time TIME NOT NULL,
    operator TEXT NOT NULL,
    t1 NUMERIC, t2 NUMERIC, t3 NUMERIC,
    b1 TEXT, b1_hours NUMERIC,
    b2 TEXT, b2_hours NUMERIC,
    alerts TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS submissions (
    batch_id UUID PRIMARY KEY,
    form_type TEXT NOT NULL,
    entry_date DATE NOT NULL,
    entry_time TIME NOT NULL,
    operator TEXT NOT NULL,
    electricity_units NUMERIC,
    customer TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS line_items (
    id SERIAL PRIMARY KEY,
    batch_id UUID REFERENCES submissions(batch_id) ON DELETE CASCADE,
    category TEXT NOT NULL,
    item_name TEXT NOT NULL,
    quantity NUMERIC NOT NULL,
    price NUMERIC,
    total_amount NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS opening_stock (
    item_name TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    opening_qty NUMERIC NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stock_ledger (
    id SERIAL PRIMARY KEY,
    entry_date DATE NOT NULL,
    category TEXT NOT NULL,
    item_name TEXT NOT NULL,
    opening NUMERIC, in_amt NUMERIC, out_amt NUMERIC, closing NUMERIC,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(entry_date, category, item_name)
);

CREATE INDEX IF NOT EXISTS idx_line_items_batch ON line_items(batch_id);
CREATE INDEX IF NOT EXISTS idx_line_items_category_item ON line_items(category, item_name);
CREATE INDEX IF NOT EXISTS idx_submissions_date ON submissions(entry_date);
CREATE INDEX IF NOT EXISTS idx_readings_date ON readings(entry_date);
CREATE INDEX IF NOT EXISTS idx_stock_ledger_date ON stock_ledger(entry_date);
"""


def init_db():
    """Creates all tables if they don't exist yet. Safe to call every startup."""
    try:
        conn = psycopg2.connect(normalize_db_url(DATABASE_URL), sslmode="require")
        cur = conn.cursor()
        cur.execute(CREATE_TABLES_SQL)
        conn.commit()
        cur.close()
        conn.close()
        print("Database tables ready.")
    except Exception as e:
        print(f"  -> WARNING: could not initialize database tables: {e}")


# ---------- Google Sheets mirror (best-effort, never blocks a submission) ----------
_GSPREAD_CLIENT = None


def get_gspread_client():
    global _GSPREAD_CLIENT
    if _GSPREAD_CLIENT is None:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        _GSPREAD_CLIENT = gspread.authorize(creds)
    return _GSPREAD_CLIENT


def resync_readings_sheet():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT entry_date, entry_time, operator, t1, t2, t3, b1, b1_hours, b2, b2_hours, alerts FROM readings ORDER BY entry_date, entry_time")
    rows = cur.fetchall()
    cur.close()
    header = ["Date", "Time", "Operator", "T1", "T2", "T3", "B1", "B1 Hours", "B2", "B2 Hours", "Alerts"]
    data = [header] + [["" if c is None else str(c) for c in r] for r in rows]
    ws = get_or_create_sheet_tab("Readings", header)
    ws.clear()
    ws.update("A1", data)


def resync_wide_category_sheet(tab_name, form_type):
    from collections import OrderedDict
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.batch_id, s.entry_date, s.entry_time, s.operator, li.item_name, li.quantity
        FROM submissions s JOIN line_items li ON li.batch_id = s.batch_id
        WHERE s.form_type=%s ORDER BY s.entry_date, s.entry_time
    """, (form_type,))
    rows = cur.fetchall()
    cur.close()

    batches = OrderedDict()
    for batch_id, edate, etime, operator, item_name, qty in rows:
        key = str(batch_id)
        if key not in batches:
            batches[key] = {"Date": str(edate), "Time": str(etime), "Operator": operator, "_items": {}}
        batches[key]["_items"][item_name] = float(qty)

    all_items = sorted({item for b in batches.values() for item in b["_items"].keys()})
    header = ["Date", "Time", "Operator"] + all_items
    if form_type == "production":
        header += ["Total Production"]

    data_rows = []
    for b in batches.values():
        row = [b["Date"], b["Time"], b["Operator"]]
        total = 0.0
        for item in all_items:
            v = b["_items"].get(item, 0)
            row.append(v)
            total += v
        if form_type == "production":
            row.append(round(total, 2))
        data_rows.append(row)

    ws = get_or_create_sheet_tab(tab_name, header)
    ws.clear()
    ws.update("A1", [header] + data_rows)


def resync_electricity_sheet():
    from collections import OrderedDict
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.batch_id, s.entry_date, s.entry_time, s.operator, s.electricity_units, li.item_name, li.quantity
        FROM submissions s LEFT JOIN line_items li ON li.batch_id = s.batch_id AND li.category='wire_rod'
        WHERE s.form_type='electricity_wire_rod' ORDER BY s.entry_date, s.entry_time
    """)
    rows = cur.fetchall()
    cur.close()

    batches = OrderedDict()
    for batch_id, edate, etime, operator, elec, item_name, qty in rows:
        key = str(batch_id)
        if key not in batches:
            batches[key] = {"Date": str(edate), "Time": str(etime), "Operator": operator,
                             "Electricity Units": "" if elec is None else float(elec), "_items": {}}
        if item_name:
            batches[key]["_items"][item_name] = float(qty)

    all_items = sorted({item for b in batches.values() for item in b["_items"].keys()})
    header = ["Date", "Time", "Operator", "Electricity Units"] + all_items
    data_rows = []
    for b in batches.values():
        row = [b["Date"], b["Time"], b["Operator"], b["Electricity Units"]]
        for item in all_items:
            row.append(b["_items"].get(item, 0))
        data_rows.append(row)

    ws = get_or_create_sheet_tab("ElectricityWireRod", header)
    ws.clear()
    ws.update("A1", [header] + data_rows)


def resync_receipts_sheet():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.entry_date, s.entry_time, s.operator, li.category, li.item_name, li.quantity
        FROM submissions s JOIN line_items li ON li.batch_id = s.batch_id
        WHERE li.category IN ('receipt_consumables','receipt_raw_material')
        ORDER BY s.entry_date, s.entry_time
    """)
    rows = cur.fetchall()
    cur.close()
    header = ["Date", "Time", "Operator", "Category", "Item", "Quantity"]
    data = [header]
    for edate, etime, operator, category, item, qty in rows:
        cat_label = "Consumables" if category == "receipt_consumables" else "Raw Material"
        data.append([str(edate), str(etime), operator, cat_label, item, float(qty)])
    ws = get_or_create_sheet_tab("Receipts", header)
    ws.clear()
    ws.update("A1", data)


def resync_sales_sheet():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.entry_date, s.entry_time, s.operator, li.item_name, li.quantity, li.price, li.total_amount, s.customer
        FROM submissions s JOIN line_items li ON li.batch_id = s.batch_id
        WHERE li.category='sale' ORDER BY s.entry_date, s.entry_time
    """)
    rows = cur.fetchall()
    cur.close()
    header = ["Date", "Time", "Operator", "Item", "Quantity", "Price (Rs/Kg)", "Total Amount (Rs)", "Customer"]
    data = [header]
    for r in rows:
        data.append([str(r[0]), str(r[1]), r[2], r[3], float(r[4]),
                     "" if r[5] is None else float(r[5]), "" if r[6] is None else float(r[6]), r[7] or ""])
    ws = get_or_create_sheet_tab("Sales", header)
    ws.clear()
    ws.update("A1", data)


def resync_sheet_for_table(table):
    """Rebuilds every affected Sheets tab entirely from the database, so an edit
    or delete always leaves Sheets exactly matching the true data - no drift."""
    try:
        if table == "readings":
            resync_readings_sheet()
        else:
            resync_wide_category_sheet("Consumption", "consumption")
            resync_wide_category_sheet("Production", "production")
            resync_electricity_sheet()
            resync_receipts_sheet()
            resync_sales_sheet()
    except Exception as e:
        print(f"  -> Sheets resync FAILED for {table}: {e}")


def get_or_create_sheet_tab(tab_name, header_row):
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = ss.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=tab_name, rows=2000, cols=max(len(header_row) + 5, 15))
        ws.append_row(header_row)
    return ws


def mirror_ensure_columns(ws, extra_headers):
    header = ws.row_values(1)
    changed = False
    for h in extra_headers:
        if h and h not in header:
            header.append(h)
            changed = True
    if changed:
        ws.update("A1", [header])
    return header


def mirror_append_named_row(tab_name, base_header, values_dict):
    """Best-effort mirror write. Failures are logged, never raised - the database
    write already succeeded and is the source of truth."""
    try:
        ws = get_or_create_sheet_tab(tab_name, base_header)
        header = mirror_ensure_columns(ws, list(values_dict.keys()))
        row = [values_dict.get(col, "") for col in header]
        ws.append_row(row)
    except Exception as e:
        print(f"  -> Sheets mirror FAILED for {tab_name}: {e}")


# ---------- Item discovery (auto-grows from real data, no code changes needed) ----------
def get_all_known_items():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT item_name FROM opening_stock WHERE category='Consumables'")
    consumables = set(r[0] for r in cur.fetchall())
    cur.execute("SELECT DISTINCT item_name FROM line_items WHERE category IN ('consumption','receipt_consumables')")
    consumables |= set(r[0] for r in cur.fetchall())

    cur.execute("SELECT item_name FROM opening_stock WHERE category='Raw Material'")
    raw_material = set(r[0] for r in cur.fetchall())
    cur.execute("SELECT DISTINCT item_name FROM line_items WHERE category IN ('wire_rod','receipt_raw_material')")
    raw_material |= set(r[0] for r in cur.fetchall())

    cur.execute("SELECT item_name FROM opening_stock WHERE category='Finished Goods'")
    finished = set(r[0] for r in cur.fetchall())
    cur.execute("SELECT DISTINCT item_name FROM line_items WHERE category IN ('production','sale')")
    finished |= set(r[0] for r in cur.fetchall())

    cur.close()
    return {
        "Consumables": sorted(consumables),
        "Raw Material": sorted(raw_material),
        "Finished Goods": sorted(finished),
    }


def get_dropdown_items(category_label):
    known = get_all_known_items().get(category_label, [])
    return sorted(set(SEED_ITEMS.get(category_label, [])) | set(known))


# ---------- Stock computation ----------
def get_opening_stock_map():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT item_name, opening_qty FROM opening_stock")
    result = {r[0]: float(r[1]) for r in cur.fetchall()}
    cur.close()
    return result


def ensure_opening_stock_seeded():
    """One-time seed: if opening_stock is empty, populate it with the SEED_ITEMS at 0."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM opening_stock")
    count = cur.fetchone()[0]
    if count == 0:
        for category, items in SEED_ITEMS.items():
            for item in items:
                cur.execute(
                    "INSERT INTO opening_stock (item_name, category, opening_qty) VALUES (%s,%s,0) ON CONFLICT (item_name) DO NOTHING",
                    (item, category),
                )
        conn.commit()
    cur.close()


def sum_qty_for_date(category_db_value, item_name, date_str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(li.quantity),0) FROM line_items li
        JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category=%s AND li.item_name=%s AND s.entry_date=%s
    """, (category_db_value, item_name, date_str))
    result = float(cur.fetchone()[0])
    cur.close()
    return result


def sum_qty_for_month(category_db_value, item_name, month_str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(li.quantity),0) FROM line_items li
        JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category=%s AND li.item_name=%s AND TO_CHAR(s.entry_date,'YYYY-MM')=%s
    """, (category_db_value, item_name, month_str))
    result = float(cur.fetchone()[0])
    cur.close()
    return result


def sum_sales_qty_and_revenue_for_month(item_name, month_str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(li.quantity),0), COALESCE(SUM(li.total_amount),0) FROM line_items li
        JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category='sale' AND li.item_name=%s AND TO_CHAR(s.entry_date,'YYYY-MM')=%s
    """, (item_name, month_str))
    row = cur.fetchone()
    cur.close()
    return float(row[0]), float(row[1])


def get_sales_summary_for_date(date_str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT li.item_name, SUM(li.quantity), SUM(li.total_amount) FROM line_items li
        JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category='sale' AND s.entry_date=%s
        GROUP BY li.item_name
    """, (date_str,))
    rows = cur.fetchall()
    cur.close()
    by_item = {r[0]: float(r[1]) for r in rows}
    total_qty = sum(by_item.values())
    total_revenue = sum(float(r[2]) for r in rows)
    return by_item, round(total_qty, 2), round(total_revenue, 2)


def get_latest_closing_map(before_date):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (item_name) item_name, closing
        FROM stock_ledger
        WHERE entry_date < %s
        ORDER BY item_name, entry_date DESC
    """, (before_date,))
    result = {r[0]: float(r[1]) for r in cur.fetchall()}
    cur.close()
    return result


def get_ledger_rows_for_date(date_str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT item_name, opening, in_amt, out_amt, closing FROM stock_ledger WHERE entry_date=%s", (date_str,))
    result = {}
    for item_name, opening, in_amt, out_amt, closing in cur.fetchall():
        result[item_name] = {"opening": float(opening), "in": float(in_amt), "out": float(out_amt), "balance": float(closing)}
    cur.close()
    return result


def compute_in_out_for_date(category_label, item_name, date_str):
    if category_label == "Consumables":
        return sum_qty_for_date("receipt_consumables", item_name, date_str), sum_qty_for_date("consumption", item_name, date_str)
    elif category_label == "Raw Material":
        return sum_qty_for_date("receipt_raw_material", item_name, date_str), sum_qty_for_date("wire_rod", item_name, date_str)
    else:
        sales_map, _, _ = get_sales_summary_for_date(date_str)
        return sum_qty_for_date("production", item_name, date_str), sales_map.get(item_name, 0.0)


def compute_stock(selected_date=None):
    today = now_ist().strftime("%Y-%m-%d")
    date_str = selected_date or today
    is_today = (date_str == today)

    opening_baseline = get_opening_stock_map()
    known_items = get_all_known_items()
    ledger_rows_for_date = {} if is_today else get_ledger_rows_for_date(date_str)
    latest_closing = get_latest_closing_map(before_date=date_str) if is_today else {}

    def build_category(category_label):
        items = sorted(set(SEED_ITEMS.get(category_label, [])) | set(known_items.get(category_label, [])))
        rows, total = [], 0.0
        for item in items:
            if item in ledger_rows_for_date:
                r = ledger_rows_for_date[item]
                op, in_amt, out_amt, bal = r["opening"], r["in"], r["out"], r["balance"]
            elif is_today:
                op = latest_closing.get(item, opening_baseline.get(item, 0.0))
                in_amt, out_amt = compute_in_out_for_date(category_label, item, date_str)
                bal = round(op + in_amt - out_amt, 2)
            else:
                op = in_amt = out_amt = bal = 0.0
            rows.append({"item": item, "opening": op, "in": in_amt, "out": out_amt, "balance": bal})
            total += bal
        return rows, round(total, 2)

    consumables_stock, consumables_total = build_category("Consumables")
    raw_material_stock, raw_material_total = build_category("Raw Material")
    finished_goods_stock, finished_goods_total = build_category("Finished Goods")

    totals = {"consumables": consumables_total, "raw_material": raw_material_total, "finished_goods": finished_goods_total}
    return consumables_stock, raw_material_stock, finished_goods_stock, totals


def run_daily_stock_close():
    """Runs at 00:00 IST. Closes yesterday for every known item."""
    yesterday = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
    opening_baseline = get_opening_stock_map()
    latest_closing = get_latest_closing_map(before_date=yesterday)
    known_items = get_all_known_items()

    conn = get_db_connection()
    cur = conn.cursor()

    for category_label, items in known_items.items():
        all_items = sorted(set(SEED_ITEMS.get(category_label, [])) | set(items))
        for item in all_items:
            opening = latest_closing.get(item, opening_baseline.get(item, 0.0))
            in_amt, out_amt = compute_in_out_for_date(category_label, item, yesterday)
            closing = round(opening + in_amt - out_amt, 2)
            cur.execute("""
                INSERT INTO stock_ledger (entry_date, category, item_name, opening, in_amt, out_amt, closing)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (entry_date, category, item_name) DO UPDATE
                SET opening=EXCLUDED.opening, in_amt=EXCLUDED.in_amt, out_amt=EXCLUDED.out_amt, closing=EXCLUDED.closing
            """, (yesterday, category_label, item, opening, in_amt, out_amt, closing))
    conn.commit()
    cur.close()
    print(f"[{now_ist()}] Daily stock close completed for {yesterday}")


def check_ranges(values):
    alerts = []
    for field, limits in TEMP_RANGES.items():
        raw_val = values.get(field, "")
        if not raw_val:
            continue
        try:
            num_val = float(raw_val)
        except ValueError:
            continue
        if num_val < limits["low"]:
            alerts.append(f"{field} LOW: {raw_val} (expected {limits['low']}-{limits['high']})")
        elif num_val > limits["high"]:
            alerts.append(f"{field} HIGH: {raw_val} (expected {limits['low']}-{limits['high']})")
    return alerts


# ---------- Styling ----------
BASE_STYLE = """
<style>
  :root {
    --ink: #1F2937; --ink-soft: #4B5563; --paper: #F3F6F9; --card: #FFFFFF;
    --accent: #1B3A5C; --accent-dark: #12283F; --line: #DCE3EC; --ok: #2E7D32; --bad: #C62828;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: linear-gradient(180deg, #DCE6F0 0%, #F3F6F9 280px, #F3F6F9 100%);
    color: var(--ink); font-family: 'Barlow Condensed', 'Segoe UI', Arial, sans-serif;
    min-height: 100vh; display: flex; flex-direction: column; align-items: center; padding: 24px 16px 60px;
  }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; max-width: 480px; width: 100%;
    padding: 28px 24px; box-shadow: 0 4px 16px rgba(27,58,92,0.08); }
  .card.wide { max-width: 1080px; }
  h1 { font-family: 'Barlow Condensed'; font-weight: 700; font-size: 24px; letter-spacing: 0.02em; text-transform: uppercase;
    color: var(--ink); margin: 0 0 4px; border-left: 5px solid var(--accent); padding-left: 12px; }
  h2.section { font-family: 'Barlow Condensed'; font-weight: 700; font-size: 16px; letter-spacing: 0.04em; text-transform: uppercase;
    color: var(--accent-dark); margin: 26px 0 4px; border-bottom: 2px solid var(--line); padding-bottom: 6px; }
  .subtitle { color: var(--ink-soft); font-size: 16px; margin: 0 0 18px; padding-left: 17px; }
  label { display: block; font-size: 14px; font-weight: 600; color: var(--ink-soft); text-transform: uppercase;
    letter-spacing: 0.03em; margin: 14px 0 5px; }
  input[type=number], input[type=text], input[type=datetime-local], input[type=password], select {
    width: 100%; padding: 12px; font-size: 18px; border: 1.5px solid var(--line); border-radius: 8px; background: var(--paper); color: var(--ink); }
  input:focus, select:focus { outline: 2px solid var(--accent); outline-offset: 1px; border-color: var(--accent); }
  .toggle-group { display: flex; gap: 10px; }
  .toggle-btn { flex: 1; padding: 14px 0; text-align: center; font-size: 17px; font-weight: 700; border: 1.5px solid var(--line);
    border-radius: 8px; background: var(--paper); cursor: pointer; user-select: none; }
  .toggle-btn.selected { background: var(--accent); border-color: var(--accent-dark); color: white; }
  button.submit, button.secondary { width: 100%; margin-top: 26px; padding: 15px 0; font-size: 17px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.04em; background: var(--accent); color: white; border: none; border-radius: 8px;
    cursor: pointer; box-shadow: 0 2px 6px rgba(27,58,92,0.18); }
  button.secondary { background: var(--ink); }
  button.add-item { width: 100%; margin-top: 10px; padding: 10px 0; font-size: 14px; font-weight: 700; text-transform: uppercase;
    background: transparent; color: var(--accent-dark); border: 1.5px dashed var(--accent); border-radius: 8px; cursor: pointer; }
  .op-name { color: var(--accent-dark); font-weight: 700; }
  .success h1 { border-left-color: var(--ok); }
  .success .icon { font-size: 44px; margin-bottom: 6px; }
  .error-box { color: var(--bad); font-weight: 600; margin-top: 12px; font-size: 14px; }
  .extra-row { display: flex; gap: 8px; margin-top: 8px; flex-wrap: wrap; }
  .extra-row select, .extra-row input { flex: 1; min-width: 90px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px; }
  th, td { text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--line); }
  th { text-transform: uppercase; letter-spacing: 0.03em; color: var(--ink-soft); font-size: 11px; background: var(--paper); }
  .badge-ok { color: var(--ok); font-weight: 700; }
  .badge-bad { color: var(--bad); font-weight: 700; }
  .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin: 16px 0 6px; }
  .stat-card { background: var(--paper); border: 1px solid var(--line); border-radius: 6px; padding: 14px 16px; }
  .stat-card .label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.03em; color: var(--ink-soft); font-weight: 700; }
  .stat-card .value { font-size: 26px; font-weight: 700; color: var(--ink); margin-top: 4px; }
  .stat-card .value.low { color: var(--bad); }
  details { margin-top: 22px; border: 1px solid var(--line); border-radius: 6px; overflow: hidden; }
  summary { cursor: pointer; padding: 14px 16px; font-family: 'Barlow Condensed'; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.03em; font-size: 15px; color: var(--accent-dark); background: var(--paper); list-style: none; }
  summary::-webkit-details-marker { display: none; }
  summary:before { content: "\\25B8 "; }
  details[open] summary:before { content: "\\25BE "; }
  details .table-wrap { padding: 6px 16px 18px; overflow-x: auto; }
  .home-grid { display: grid; gap: 12px; margin-top: 20px; }
  .home-btn { display: flex; align-items: center; justify-content: center; gap: 8px; text-decoration: none; text-align: center;
    padding: 17px 0; font-family: 'Barlow Condensed'; font-weight: 700; font-size: 16px; text-transform: uppercase;
    letter-spacing: 0.03em; border-radius: 8px; color: white; cursor: pointer; border: none; box-shadow: 0 3px 10px rgba(0,0,0,0.15); }
  .home-btn:active { transform: scale(0.97); }
  .home-btn .icon { font-size: 19px; }
  .nav-top { max-width: 1080px; width: 100%; margin-bottom: 10px; display: flex; justify-content: flex-end; }
  .nav-top a { color: var(--ink-soft); font-size: 13px; text-decoration: none; }
  .review-row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--line); font-size: 15px; }
  .review-row b { color: var(--accent-dark); }
  .review-actions { display: flex; gap: 10px; margin-top: 22px; }
  .review-actions button { flex: 1; margin-top: 0; }
</style>
"""


def confirm_flow_script(form_id, review_id, panel_id):
    """Generic JS: collects visible labeled fields from a form and shows a review
    screen before allowing the actual submit."""
    return f"""
<script>
  (function() {{
    var form = document.getElementById('{form_id}');
    var reviewBtn = form.querySelector('.review-btn');
    reviewBtn.addEventListener('click', function() {{
      var rows = '';
      var labels = form.querySelectorAll('label');
      labels.forEach(function(lab) {{
        var input = lab.nextElementSibling;
        while (input && input.tagName !== 'INPUT' && input.tagName !== 'SELECT') {{
          input = input.nextElementSibling;
        }}
        if (!input) return;
        var val = input.value;
        if (input.type === 'hidden' || !val) return;
        rows += '<div class="review-row"><span>' + lab.textContent + '</span><b>' + val + '</b></div>';
      }});
      document.getElementById('{review_id}').innerHTML = rows;
      form.style.display = 'none';
      document.getElementById('{panel_id}').style.display = 'block';
    }});
  }})();
  function goBackToForm_{form_id}() {{
    document.getElementById('{form_id}').style.display = 'block';
    document.getElementById('{panel_id}').style.display = 'none';
  }}
</script>
"""


# ---------- PIN login ----------
LOGIN_HTML = BASE_STYLE + """
<meta name="viewport" content="width=device-width, initial-scale=1">
<div class="card">
  <div style="text-align:center;margin-bottom:16px;">
    <div style="display:inline-block;background:white;border-radius:10px;padding:10px 20px;box-shadow:0 2px 8px rgba(27,58,92,0.12);border:1px solid var(--line);">
      <img src="https://www.khemjiwire.in/logo.png" alt="Khemji Wire" style="height:56px;display:block;">
    </div>
  </div>
  <h1 style="text-align:center;border-left:none;padding-left:0;">Khemji Wire</h1>
  <p class="subtitle" style="text-align:center;padding-left:0;">Enter the team PIN to continue</p>
  <form method="POST">
    <label>PIN</label>
    <input type="password" name="pin" inputmode="numeric" autofocus required>
    {% if error %}<p class="error-box">{{ error }}</p>{% endif %}
    <button class="submit" type="submit">Enter</button>
  </form>
</div>
"""


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        if request.form.get("pin", "") == SHARED_PIN:
            session.permanent = True
            session["authenticated"] = True
            next_url = request.args.get("next") or "/app-home"
            return redirect(next_url)
        error = "Incorrect PIN. Please try again."
    return render_template_string(LOGIN_HTML, error=error)


@app.before_request
def require_pin():
    if request.path == "/login" or request.path.startswith("/static"):
        return
    if request.path == "/dashboard":
        return  # protected separately by its own admin key
    if not session.get("authenticated"):
        return redirect(f"/login?next={request.path}")


# ---------- Home ----------
HOME_HTML = BASE_STYLE + """
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black">
<meta name="apple-mobile-web-app-title" content="Khemji Wire">
<meta name="viewport" content="width=device-width, initial-scale=1">
<div class="card">
  <div style="text-align:center;margin-bottom:16px;">
    <div style="display:inline-block;background:white;border-radius:10px;padding:10px 20px;box-shadow:0 2px 8px rgba(27,58,92,0.12);border:1px solid var(--line);">
      <img src="https://www.khemjiwire.in/logo.png" alt="Khemji Wire" style="height:56px;display:block;">
    </div>
  </div>
  <h1 style="text-align:center;border-left:none;padding-left:0;">Khemji Wire</h1>
  <p class="subtitle" style="text-align:center;padding-left:0;">Select your name, then choose what to log</p>
  <select id="operatorSelect" style="width:100%;padding:12px;font-size:18px;border:1.5px solid var(--line);border-radius:8px;background:var(--paper);">
    {% for name in all_names %}
    <option value="{{ name }}">{{ name }}</option>
    {% endfor %}
  </select>
  <div class="home-grid">
    <button class="home-btn" style="background:var(--accent);" onclick="goTo('/furnace-form')"><span class="icon">&#128293;</span> Furnace Reading</button>
    <button class="home-btn" style="background:var(--ink);" onclick="goTo('/production-form')"><span class="icon">&#127981;</span> Production &amp; Consumption</button>
    <button class="home-btn" style="background:#3B5C7A;" onclick="goTo('/electricity-form')"><span class="icon">&#9889;</span> Electricity &amp; Wire Rod</button>
    <button class="home-btn" style="background:#2E7D32;" onclick="goTo('/receipt-form')"><span class="icon">&#128230;</span> Log Stock Receipt</button>
    <button class="home-btn" style="background:#8B5E00;" onclick="goTo('/sales-form')"><span class="icon">&#128176;</span> Log a Sale</button>
    <a class="home-btn" style="background:#12283F;" href="/operator-dashboard"><span class="icon">&#128202;</span> View Stock Dashboard</a>
  </div>
</div>
<script>
  function goTo(path) {
    var name = document.getElementById('operatorSelect').value;
    window.location.href = path + '?operator=' + encodeURIComponent(name);
  }
</script>
"""


@app.route("/app-home", methods=["GET"])
@app.route("/", methods=["GET"])
def app_home():
    return render_template_string(HOME_HTML, all_names=sorted(ALL_PEOPLE_NAMES))


SUCCESS_HTML = BASE_STYLE + """
<div class="card success">
  <div class="icon">&#9989;</div>
  <h1>Logged Successfully</h1>
  <p class="subtitle">Thank you, {{ operator }}. Your entry has been recorded.</p>
  {% if alerts %}<p class="error-box">&#128680; {{ alerts }}</p>{% endif %}
  <a href="/app-home" style="display:block;text-align:center;margin-top:18px;color:var(--accent-dark);font-weight:700;text-decoration:none;">&larr; Back to Home</a>
</div>
"""


# ---------- Furnace ----------
FURNACE_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Furnace Temperature</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>

  <form id="furnaceForm" method="POST" action="/submit-furnace">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>
    <label>T1 (&deg;C)</label>
    <input type="number" name="T1" step="0.1" required>
    <label>T2 (&deg;C)</label>
    <input type="number" name="T2" step="0.1" required>
    <label>T3 (&deg;C)</label>
    <input type="number" name="T3" step="0.1" required>

    <label>B1 Status</label>
    <div class="toggle-group">
      <div class="toggle-btn" data-target="B1" data-value="ON">ON</div>
      <div class="toggle-btn" data-target="B1" data-value="OFF">OFF</div>
    </div>
    <input type="hidden" name="B1" id="B1" required>
    <label>B1 Running Hours (today)</label>
    <input type="number" name="B1_HOURS" step="0.1" required>

    <label>B2 Status</label>
    <div class="toggle-group">
      <div class="toggle-btn" data-target="B2" data-value="ON">ON</div>
      <div class="toggle-btn" data-target="B2" data-value="OFF">OFF</div>
    </div>
    <input type="hidden" name="B2" id="B2" required>
    <label>B2 Running Hours (today)</label>
    <input type="number" name="B2_HOURS" step="0.1" required>

    <button class="submit review-btn" type="button">Review Entry</button>
  </form>

  <div id="furnaceReviewPanel" style="display:none;">
    <h2 class="section">Review Your Entry</h2>
    <div id="furnaceReview"></div>
    <div class="review-actions">
      <button class="secondary" type="button" onclick="goBackToForm_furnaceForm()">Go Back</button>
      <button class="submit" type="submit" form="furnaceForm">Confirm &amp; Submit</button>
    </div>
  </div>
</div>
<script>
  document.querySelectorAll('.toggle-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var group = btn.parentElement;
      group.querySelectorAll('.toggle-btn').forEach(function(b) { b.classList.remove('selected'); });
      btn.classList.add('selected');
      document.getElementById(btn.dataset.target).value = btn.dataset.value;
    });
  });
</script>
""" + confirm_flow_script("furnaceForm", "furnaceReview", "furnaceReviewPanel")


@app.route("/furnace-form", methods=["GET"])
def furnace_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(FURNACE_FORM_HTML, operator=operator, default_time=default_entry_time())


@app.route("/submit-furnace", methods=["POST"])
def submit_furnace():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    values = {
        "T1": request.form.get("T1", ""), "T2": request.form.get("T2", ""), "T3": request.form.get("T3", ""),
        "B1": request.form.get("B1", ""), "B2": request.form.get("B2", ""),
    }
    b1_hours = request.form.get("B1_HOURS", "")
    b2_hours = request.form.get("B2_HOURS", "")
    alerts = check_ranges(values)
    alert_text = "; ".join(alerts) if alerts else "OK"

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO readings (entry_date, entry_time, operator, t1, t2, t3, b1, b1_hours, b2, b2_hours, alerts)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (date_str, time_str, operator, safe_float(values["T1"]), safe_float(values["T2"]), safe_float(values["T3"]),
          values["B1"], safe_float(b1_hours), values["B2"], safe_float(b2_hours), alert_text))
    conn.commit()
    cur.close()

    mirror_append_named_row(
        "Readings",
        ["Date", "Time", "Operator", "T1", "T2", "T3", "B1", "B1 Hours", "B2", "B2 Hours", "Alerts"],
        {"Date": date_str, "Time": time_str, "Operator": operator,
         "T1": values["T1"], "T2": values["T2"], "T3": values["T3"],
         "B1": values["B1"], "B1 Hours": b1_hours, "B2": values["B2"], "B2 Hours": b2_hours, "Alerts": alert_text},
    )

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=alert_text if alerts else None)


# ---------- Production & Consumption ----------
PRODUCTION_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Production &amp; Consumption</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>

  <form id="prodForm" method="POST" action="/submit-production">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <h2 class="section">Consumption (kg)</h2>
    {% for item in consumption_items %}
    <label>{{ item }}</label>
    <input type="number" name="cons_{{ loop.index0 }}" step="0.1" value="0">
    {% endfor %}
    <div id="extraConsumption"></div>
    <button class="add-item" type="button" onclick="addExtraConsumptionRow()">+ Add Another Consumable</button>

    <h2 class="section">Production (quantity)</h2>
    {% for item in production_items %}
    <label>{{ item }}</label>
    <input type="number" name="prod_{{ loop.index0 }}" step="0.1" value="0">
    {% endfor %}
    <div id="extraProduction"></div>
    <button class="add-item" type="button" onclick="addExtraProductionRow()">+ Add Another Item</button>

    <button class="submit review-btn" type="button">Review Entry</button>
  </form>

  <div id="prodReviewPanel" style="display:none;">
    <h2 class="section">Review Your Entry</h2>
    <div id="prodReview"></div>
    <div class="review-actions">
      <button class="secondary" type="button" onclick="goBackToForm_prodForm()">Go Back</button>
      <button class="submit" type="submit" form="prodForm">Confirm &amp; Submit</button>
    </div>
  </div>
</div>
<script>
  function addExtraConsumptionRow() {
    var container = document.getElementById('extraConsumption');
    var row = document.createElement('div');
    row.className = 'extra-row';
    row.innerHTML = '<input type="text" name="extra_cons_name[]" placeholder="Consumable name">' +
      '<input type="number" name="extra_cons_qty[]" placeholder="Qty (kg)" step="0.1">';
    container.appendChild(row);
  }
  function addExtraProductionRow() {
    var container = document.getElementById('extraProduction');
    var row = document.createElement('div');
    row.className = 'extra-row';
    row.innerHTML = '<input type="text" name="extra_prod_name[]" placeholder="Item name">' +
      '<input type="number" name="extra_prod_qty[]" placeholder="Qty" step="0.1">';
    container.appendChild(row);
  }
</script>
""" + confirm_flow_script("prodForm", "prodReview", "prodReviewPanel")


@app.route("/production-form", methods=["GET"])
def production_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(
        PRODUCTION_FORM_HTML, operator=operator, default_time=default_entry_time(),
        consumption_items=get_dropdown_items("Consumables"), production_items=get_dropdown_items("Finished Goods"),
    )


@app.route("/submit-production", methods=["POST"])
def submit_production():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    consumption_items = get_dropdown_items("Consumables")
    production_items = get_dropdown_items("Finished Goods")

    batch_id_cons = str(uuid.uuid4())
    batch_id_prod = str(uuid.uuid4())

    conn = get_db_connection()
    cur = conn.cursor()

    # Consumption submission
    cur.execute(
        "INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator) VALUES (%s,%s,%s,%s,%s)",
        (batch_id_cons, "consumption", date_str, time_str, operator),
    )
    cons_mirror = {"Date": date_str, "Time": time_str, "Operator": operator}
    for i, item in enumerate(consumption_items):
        qty = safe_float(request.form.get(f"cons_{i}", "0"))
        if qty != 0:
            cur.execute(
                "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                (batch_id_cons, "consumption", item, qty),
            )
        cons_mirror[item] = request.form.get(f"cons_{i}", "0")

    extra_cons_names = request.form.getlist("extra_cons_name[]")
    extra_cons_qtys = request.form.getlist("extra_cons_qty[]")
    for n, q in zip(extra_cons_names, extra_cons_qtys):
        n = n.strip()
        if n and safe_float(q) != 0:
            cur.execute(
                "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                (batch_id_cons, "consumption", n, safe_float(q)),
            )
            cons_mirror[n] = q

    # Production submission
    cur.execute(
        "INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator) VALUES (%s,%s,%s,%s,%s)",
        (batch_id_prod, "production", date_str, time_str, operator),
    )
    prod_mirror = {"Date": date_str, "Time": time_str, "Operator": operator}
    total = 0.0
    for i, item in enumerate(production_items):
        qty = safe_float(request.form.get(f"prod_{i}", "0"))
        if qty != 0:
            cur.execute(
                "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                (batch_id_prod, "production", item, qty),
            )
        prod_mirror[item] = request.form.get(f"prod_{i}", "0")
        total += qty

    extra_prod_names = request.form.getlist("extra_prod_name[]")
    extra_prod_qtys = request.form.getlist("extra_prod_qty[]")
    for n, q in zip(extra_prod_names, extra_prod_qtys):
        n = n.strip()
        if n and safe_float(q) != 0:
            cur.execute(
                "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                (batch_id_prod, "production", n, safe_float(q)),
            )
            prod_mirror[n] = q
            total += safe_float(q)

    prod_mirror["Total Production"] = round(total, 2)

    conn.commit()
    cur.close()

    mirror_append_named_row("Consumption", ["Date", "Time", "Operator"] + consumption_items, cons_mirror)
    mirror_append_named_row("Production", ["Date", "Time", "Operator"] + production_items + ["Total Production"], prod_mirror)

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Electricity & Wire Rod ----------
ELECTRICITY_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Electricity &amp; Wire Rod</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>

  <form id="elecForm" method="POST" action="/submit-electricity">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <h2 class="section">Electricity</h2>
    <label>Units Consumed</label>
    <input type="number" name="electricity_units" step="0.1" required>

    <h2 class="section">Wire Rod Issued (kg)</h2>
    <div id="wireRodRows"></div>
    <button class="add-item" type="button" onclick="addWireRodRow()">+ Add Wire Rod Entry</button>

    <button class="submit review-btn" type="button">Review Entry</button>
  </form>

  <div id="elecReviewPanel" style="display:none;">
    <h2 class="section">Review Your Entry</h2>
    <div id="elecReview"></div>
    <div class="review-actions">
      <button class="secondary" type="button" onclick="goBackToForm_elecForm()">Go Back</button>
      <button class="submit" type="submit" form="elecForm">Confirm &amp; Submit</button>
    </div>
  </div>
</div>
<script>
  var wireRodSizes = {{ wire_rod_sizes | tojson }};
  function addWireRodRow() {
    var container = document.getElementById('wireRodRows');
    var row = document.createElement('div');
    row.className = 'extra-row';
    var select = document.createElement('select');
    select.name = 'wr_size[]';
    wireRodSizes.forEach(function(size) {
      var opt = document.createElement('option'); opt.value = size; opt.textContent = size; select.appendChild(opt);
    });
    var otherOpt = document.createElement('option'); otherOpt.value = 'Other'; otherOpt.textContent = 'Other';
    select.appendChild(otherOpt);
    var customInput = document.createElement('input');
    customInput.type = 'text'; customInput.name = 'wr_custom_size[]'; customInput.placeholder = 'Specify size'; customInput.style.display = 'none';
    var qtyInput = document.createElement('input');
    qtyInput.type = 'number'; qtyInput.name = 'wr_qty[]'; qtyInput.placeholder = 'Qty (kg)'; qtyInput.step = '0.1';
    select.addEventListener('change', function() { customInput.style.display = (select.value === 'Other') ? 'block' : 'none'; });
    row.appendChild(select); row.appendChild(customInput); row.appendChild(qtyInput);
    container.appendChild(row);
  }
  addWireRodRow();
</script>
""" + confirm_flow_script("elecForm", "elecReview", "elecReviewPanel")


@app.route("/electricity-form", methods=["GET"])
def electricity_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(ELECTRICITY_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   wire_rod_sizes=get_dropdown_items("Raw Material"))


@app.route("/submit-electricity", methods=["POST"])
def submit_electricity():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    units = request.form.get("electricity_units", "")

    wr_sizes = request.form.getlist("wr_size[]")
    wr_customs = request.form.getlist("wr_custom_size[]")
    wr_qtys = request.form.getlist("wr_qty[]")

    batch_id = str(uuid.uuid4())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator, electricity_units) VALUES (%s,%s,%s,%s,%s,%s)",
        (batch_id, "electricity_wire_rod", date_str, time_str, operator, safe_float(units)),
    )

    mirror_row = {"Date": date_str, "Time": time_str, "Operator": operator, "Electricity Units": units}
    for size, custom, qty in zip(wr_sizes, wr_customs, wr_qtys):
        if not qty:
            continue
        final_size = custom.strip() if size == "Other" and custom.strip() else size
        q = safe_float(qty)
        if q != 0:
            cur.execute(
                "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                (batch_id, "wire_rod", final_size, q),
            )
        mirror_row[final_size] = qty

    conn.commit()
    cur.close()

    mirror_append_named_row("ElectricityWireRod", ["Date", "Time", "Operator", "Electricity Units"] + get_dropdown_items("Raw Material"), mirror_row)

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Stock: Receipts ----------
RECEIPT_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Stock Receipt</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>

  <form id="receiptForm" method="POST" action="/submit-receipt">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <label>Category</label>
    <select id="categorySelect" name="category" onchange="updateItems()">
      {% for cat in categories %}<option value="{{ cat }}">{{ cat }}</option>{% endfor %}
    </select>

    <label>Item</label>
    <select id="itemSelect" name="item"></select>
    <input type="text" id="customItem" name="custom_item" placeholder="Specify item" style="display:none;margin-top:8px;">

    <label>Quantity Received</label>
    <input type="number" name="quantity" step="0.1" required>

    <button class="submit review-btn" type="button">Review Entry</button>
  </form>

  <div id="receiptReviewPanel" style="display:none;">
    <h2 class="section">Review Your Entry</h2>
    <div id="receiptReview"></div>
    <div class="review-actions">
      <button class="secondary" type="button" onclick="goBackToForm_receiptForm()">Go Back</button>
      <button class="submit" type="submit" form="receiptForm">Confirm &amp; Submit</button>
    </div>
  </div>
</div>
<script>
  var categoryItems = {{ category_items | tojson }};
  function updateItems() {
    var cat = document.getElementById('categorySelect').value;
    var itemSelect = document.getElementById('itemSelect');
    itemSelect.innerHTML = '';
    categoryItems[cat].forEach(function(item) {
      var opt = document.createElement('option'); opt.value = item; opt.textContent = item; itemSelect.appendChild(opt);
    });
    var otherOpt = document.createElement('option'); otherOpt.value = 'Other'; otherOpt.textContent = 'Other';
    itemSelect.appendChild(otherOpt);
    itemSelect.onchange = function() {
      document.getElementById('customItem').style.display = (itemSelect.value === 'Other') ? 'block' : 'none';
    };
  }
  updateItems();
</script>
""" + confirm_flow_script("receiptForm", "receiptReview", "receiptReviewPanel")


@app.route("/receipt-form", methods=["GET"])
def receipt_form():
    operator = request.args.get("operator", "Operator")
    categories = {"Consumables": get_dropdown_items("Consumables"), "Raw Material": get_dropdown_items("Raw Material")}
    return render_template_string(RECEIPT_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   categories=list(categories.keys()), category_items=categories)


@app.route("/submit-receipt", methods=["POST"])
def submit_receipt():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    category = request.form.get("category", "")
    item = request.form.get("item", "")
    custom_item = request.form.get("custom_item", "").strip()
    final_item = custom_item if item == "Other" and custom_item else item
    quantity = safe_float(request.form.get("quantity", ""))

    category_db = "receipt_consumables" if category == "Consumables" else "receipt_raw_material"

    batch_id = str(uuid.uuid4())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator) VALUES (%s,%s,%s,%s,%s)",
        (batch_id, "receipt", date_str, time_str, operator),
    )
    cur.execute(
        "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
        (batch_id, category_db, final_item, quantity),
    )
    conn.commit()
    cur.close()

    mirror_append_named_row(
        "Receipts", ["Date", "Time", "Operator", "Category", "Item", "Quantity"],
        {"Date": date_str, "Time": time_str, "Operator": operator, "Category": category, "Item": final_item, "Quantity": quantity},
    )

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Stock: Sales ----------
SALES_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Log a Sale</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>

  <form id="saleForm" method="POST" action="/submit-sale">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <label>Item</label>
    <select id="itemSelect" name="item" onchange="toggleCustom()">
      {% for item in finished_goods_items %}<option value="{{ item }}">{{ item }}</option>{% endfor %}
      <option value="Other">Other</option>
    </select>
    <input type="text" id="customItem" name="custom_item" placeholder="Specify item" style="display:none;margin-top:8px;">

    <label>Quantity Sold</label>
    <input type="number" name="quantity" step="0.1" required>

    <label>Sale Price (Rs./Kg)</label>
    <input type="number" name="price" step="0.01" required>

    <label>Customer (optional)</label>
    <input type="text" name="customer">

    <button class="submit review-btn" type="button">Review Entry</button>
  </form>

  <div id="saleReviewPanel" style="display:none;">
    <h2 class="section">Review Your Entry</h2>
    <div id="saleReview"></div>
    <div class="review-actions">
      <button class="secondary" type="button" onclick="goBackToForm_saleForm()">Go Back</button>
      <button class="submit" type="submit" form="saleForm">Confirm &amp; Submit</button>
    </div>
  </div>
</div>
<script>
  function toggleCustom() {
    var sel = document.getElementById('itemSelect');
    document.getElementById('customItem').style.display = (sel.value === 'Other') ? 'block' : 'none';
  }
</script>
""" + confirm_flow_script("saleForm", "saleReview", "saleReviewPanel")


@app.route("/sales-form", methods=["GET"])
def sales_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(SALES_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   finished_goods_items=get_dropdown_items("Finished Goods"))


@app.route("/submit-sale", methods=["POST"])
def submit_sale():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    item = request.form.get("item", "")
    custom_item = request.form.get("custom_item", "").strip()
    final_item = custom_item if item == "Other" and custom_item else item
    quantity = safe_float(request.form.get("quantity", ""))
    price = safe_float(request.form.get("price", ""))
    customer = request.form.get("customer", "")
    total_amount = round(quantity * price, 2)

    batch_id = str(uuid.uuid4())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator, customer) VALUES (%s,%s,%s,%s,%s,%s)",
        (batch_id, "sale", date_str, time_str, operator, customer),
    )
    cur.execute(
        "INSERT INTO line_items (batch_id, category, item_name, quantity, price, total_amount) VALUES (%s,%s,%s,%s,%s,%s)",
        (batch_id, "sale", final_item, quantity, price, total_amount),
    )
    conn.commit()
    cur.close()

    mirror_append_named_row(
        "Sales", ["Date", "Time", "Operator", "Item", "Quantity", "Price (Rs/Kg)", "Total Amount (Rs)", "Customer"],
        {"Date": date_str, "Time": time_str, "Operator": operator, "Item": final_item,
         "Quantity": quantity, "Price (Rs/Kg)": price, "Total Amount (Rs)": total_amount, "Customer": customer},
    )

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Dashboard data helpers ----------
def get_recent_readings(limit=12, operator_filter=None):
    conn = get_db_connection()
    cur = conn.cursor()
    query = "SELECT id, entry_date, entry_time, operator, t1, t2, t3, b1, b1_hours, b2, b2_hours, alerts FROM readings"
    params = []
    if operator_filter:
        query += " WHERE operator=%s"
        params.append(operator_filter)
    query += " ORDER BY entry_date DESC, entry_time DESC LIMIT %s"
    params.append(limit)
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def get_recent_line_items(categories, limit=12, operator_filter=None):
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
        SELECT li.id, s.entry_date, s.entry_time, s.operator, li.category, li.item_name,
               li.quantity, li.price, li.total_amount, s.customer, s.electricity_units
        FROM line_items li JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category = ANY(%s)
    """
    params = [categories]
    if operator_filter:
        query += " AND s.operator=%s"
        params.append(operator_filter)
    query += " ORDER BY s.entry_date DESC, s.entry_time DESC LIMIT %s"
    params.append(limit)
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def get_monthly_item_report(item, month_str, category_label, include_sales):
    report = {"item": item, "category": category_label, "month": month_str}
    if category_label == "Consumables":
        report["consumption"] = sum_qty_for_month("consumption", item, month_str)
        report["receipts"] = sum_qty_for_month("receipt_consumables", item, month_str)
    elif category_label == "Raw Material":
        report["issued"] = sum_qty_for_month("wire_rod", item, month_str)
        report["receipts"] = sum_qty_for_month("receipt_raw_material", item, month_str)
    else:
        report["production"] = sum_qty_for_month("production", item, month_str)
        if include_sales:
            qty, revenue = sum_sales_qty_and_revenue_for_month(item, month_str)
            report["sales_qty"] = qty
            report["sales_revenue"] = revenue
    return report


def find_item_category(item_name, known_items):
    for cat, items in known_items.items():
        if item_name in items:
            return cat
    return None


# ---------- Dashboard template ----------
DASHBOARD_HTML = BASE_STYLE + """
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black">
<meta name="apple-mobile-web-app-title" content="Khemji Dashboard">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { --accent: #1B3A5C; --accent-dark: #12283F; }
  .brand-header { display:flex; flex-direction:column; align-items:center; text-align:center; gap:10px; margin-bottom:18px; }
  .brand-header .logo-box { background:white; border-radius:10px; padding:14px 26px; box-shadow:0 2px 8px rgba(27,58,92,0.12); border:1px solid var(--line); }
  .brand-header img { height:76px; width:auto; display:block; }
  .brand-header .brand-title { font-family:'Barlow Condensed'; font-weight:700; font-size:26px; letter-spacing:0.03em; text-transform:uppercase; color:var(--ink); }
  .brand-header .dash-title { font-family:'Barlow Condensed'; font-weight:700; font-size:20px; letter-spacing:0.04em; text-transform:uppercase; color:var(--accent-dark); margin-top:2px; }
  .date-picker-bar { display:flex; align-items:center; justify-content:center; gap:10px; margin:4px 0 20px; flex-wrap:wrap; }
  .date-picker-bar input[type=date] { max-width:200px; }
  .today-pill { background:var(--accent); color:white; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:0.03em; padding:4px 10px; border-radius:20px; }
  .cat-total-row { display:flex; justify-content:space-between; align-items:center; margin:4px 0 10px; }
  .cat-total-row .cat-total-value { font-size:20px; font-weight:700; color:var(--accent-dark); }
  .kpi-strip { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin:8px 0 22px; }
  .kpi-card { background:linear-gradient(135deg,var(--accent) 0%,var(--accent-dark) 100%); color:white; border-radius:8px; padding:16px 18px; }
  .kpi-card .kpi-label { font-size:11px; text-transform:uppercase; letter-spacing:0.04em; opacity:0.85; font-weight:700; }
  .kpi-card .kpi-value { font-size:24px; font-weight:700; margin-top:4px; }
  .kpi-card.alert { background:linear-gradient(135deg,#C62828 0%,#8E1E1E 100%); }
  .alert-panel { background:#FDECEC; border:1.5px solid #F1B3B3; border-radius:8px; padding:14px 18px; margin-bottom:22px; }
  .alert-panel .alert-title { color:var(--bad); font-weight:700; text-transform:uppercase; font-size:13px; letter-spacing:0.03em; margin-bottom:6px; }
  .alert-panel .alert-item { font-size:14px; color:var(--ink); padding:3px 0; }
  .alert-panel.all-clear { background:#EAF5EA; border-color:#B9DDB9; }
  .alert-panel.all-clear .alert-title { color:var(--ok); }
  .bar-list { margin-top:6px; }
  .bar-row { display:flex; align-items:center; gap:10px; margin:10px 0; }
  .bar-row .bar-label { width:110px; font-size:13px; color:var(--ink-soft); flex-shrink:0; text-align:right; }
  .bar-row .bar-track { flex:1; background:#E2EEEE; border-radius:20px; height:20px; overflow:hidden; }
  .bar-row .bar-fill { height:100%; border-radius:20px; transition:width 0.4s ease; }
  .bar-row .bar-value { width:60px; font-size:13px; font-weight:700; text-align:left; flex-shrink:0; }
  .footer-brand { margin-top:36px; padding-top:20px; border-top:1px solid var(--line); font-size:13px; color:var(--ink-soft); line-height:1.6; text-align:center; }
</style>
<div class="nav-top"><a href="/app-home">&larr; Back to entry forms</a></div>
<div class="card wide">
  <div class="brand-header">
    <div class="logo-box"><img src="https://www.khemjiwire.in/logo.png" alt="Khemji Wire"></div>
    <div class="brand-title">Khemji Wire &amp; Wire Pvt. Ltd.</div>
    <div class="dash-title">Live Dashboard</div>
  </div>

  <div class="date-picker-bar">
    <label style="margin:0;">Viewing</label>
    <input type="date" id="dateSelect" value="{{ selected_date }}" onchange="changeDate()">
    {% if is_today %}<span class="today-pill">Today &mdash; Live</span>{% else %}<span class="today-pill" style="background:var(--ink);">Closed Day Snapshot</span>{% endif %}
  </div>
  <div class="date-picker-bar">
    <label style="margin:0;">My Entries</label>
    <select id="operatorFilterSelect" onchange="changeOperator()">
      <option value="">All Operators</option>
      {% for name in all_operator_names %}
      <option value="{{ name }}" {{ 'selected' if name == operator_filter else '' }}>{{ name }}</option>
      {% endfor %}
    </select>
  </div>

  <div class="kpi-strip">
    <div class="kpi-card {{ 'alert' if low_stock_count > 0 else '' }}">
      <div class="kpi-label">Items Low / Out of Stock</div>
      <div class="kpi-value">{{ low_stock_count }}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Production ({{ selected_date }})</div>
      <div class="kpi-value">{{ day_production }}</div>
    </div>
    {% if show_sales %}
    <div class="kpi-card">
      <div class="kpi-label">Qty Sold ({{ selected_date }})</div>
      <div class="kpi-value">{{ day_sales_qty }}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Revenue ({{ selected_date }})</div>
      <div class="kpi-value">Rs. {{ day_sales_revenue }}</div>
    </div>
    {% if top_seller %}
    <div class="kpi-card" style="background:linear-gradient(135deg,#8B5E00 0%,#5E3F00 100%);">
      <div class="kpi-label">Top Seller</div>
      <div class="kpi-value" style="font-size:17px;">{{ top_seller.item }}<br><span style="font-size:13px;font-weight:400;">{{ top_seller.qty }} units</span></div>
    </div>
    {% endif %}
    {% endif %}
    <div class="kpi-card">
      <div class="kpi-label">Last Updated</div>
      <div class="kpi-value" style="font-size:16px;">{{ last_updated }}</div>
    </div>
  </div>

  <script>
    function changeDate() {
      var d = document.getElementById('dateSelect').value;
      var url = new URL(window.location.href);
      url.searchParams.set('date', d);
      window.location.href = url.toString();
    }
    function changeOperator() {
      var op = document.getElementById('operatorFilterSelect').value;
      var url = new URL(window.location.href);
      if (op) { url.searchParams.set('operator', op); } else { url.searchParams.delete('operator'); }
      window.location.href = url.toString();
    }
    function changeMonthlyReport() {
      var item = document.getElementById('reportItemSelect').value;
      var month = document.getElementById('reportMonthSelect').value;
      var url = new URL(window.location.href);
      url.searchParams.set('report_item', item);
      url.searchParams.set('report_month', month);
      window.location.href = url.toString();
    }
  </script>

  {% if low_stock_items %}
  <div class="alert-panel">
    <div class="alert-title">&#9888; Needs Attention &mdash; Zero or Negative Balance</div>
    {% for item in low_stock_items %}
    <div class="alert-item">{{ item.category }} &mdash; <b>{{ item.item }}</b>: {{ item.balance }}</div>
    {% endfor %}
  </div>
  {% else %}
  <div class="alert-panel all-clear"><div class="alert-title">&#9989; All Stock Levels Healthy</div></div>
  {% endif %}

  <h2 class="section">Stock Overview</h2>
  <div class="cat-selector" style="display:flex;align-items:center;justify-content:center;gap:10px;margin:16px 0 20px;">
    <label style="margin:0;">View Category</label>
    <select id="catSelect" onchange="showCategory()">
      <option value="raw">Raw Material (Wire Rod)</option>
      <option value="finished">Finished Goods</option>
      <option value="consumables">Consumables</option>
    </select>
  </div>

  <div id="bars-raw" class="bar-list cat-block"></div>
  <div id="bars-finished" class="bar-list cat-block" style="display:none;"></div>
  <div id="bars-consumables" class="bar-list cat-block" style="display:none;"></div>

  <div id="stat-raw" class="stat-grid cat-block">
    {% for row in raw_material_stock %}
    <div class="stat-card" style="border-left:4px solid var(--accent);"><div class="label">Wire Rod {{ row.item }}</div><div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div></div>
    {% endfor %}
  </div>
  <div id="stat-finished" class="stat-grid cat-block" style="display:none;">
    {% for row in finished_goods_stock %}
    <div class="stat-card" style="border-left:4px solid var(--ink);"><div class="label">{{ row.item }}</div><div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div></div>
    {% endfor %}
  </div>
  <div id="stat-consumables" class="stat-grid cat-block" style="display:none;">
    {% for row in consumables_stock %}
    <div class="stat-card" style="border-left:4px solid #2E7D32;"><div class="label">{{ row.item }}</div><div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div></div>
    {% endfor %}
  </div>

  <div id="table-raw" class="cat-block">
    <div class="cat-total-row"><span>Total Raw Material Balance</span><span class="cat-total-value">{{ totals.raw_material }}</span></div>
    <div class="table-wrap"><table><tr><th>Size</th><th>Opening</th><th>Received</th><th>Issued</th><th>Balance</th></tr>
    {% for row in raw_material_stock %}<tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td><td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>{% endfor %}
    </table></div>
  </div>
  <div id="table-finished" class="cat-block" style="display:none;">
    <div class="cat-total-row"><span>Total Finished Goods Balance</span><span class="cat-total-value">{{ totals.finished_goods }}</span></div>
    <div class="table-wrap"><table><tr><th>Item</th><th>Opening</th><th>Produced</th><th>Sold</th><th>Balance</th></tr>
    {% for row in finished_goods_stock %}<tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td><td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>{% endfor %}
    </table></div>
  </div>
  <div id="table-consumables" class="cat-block" style="display:none;">
    <div class="cat-total-row"><span>Total Consumables Balance</span><span class="cat-total-value">{{ totals.consumables }}</span></div>
    <div class="table-wrap"><table><tr><th>Item</th><th>Opening</th><th>Received</th><th>Consumed</th><th>Balance</th></tr>
    {% for row in consumables_stock %}<tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td><td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>{% endfor %}
    </table></div>
  </div>

  <h2 class="section">Monthly Item Report</h2>
  <div class="date-picker-bar" style="justify-content:flex-start;">
    <label style="margin:0;">Item</label>
    <select id="reportItemSelect" onchange="changeMonthlyReport()" style="max-width:220px;">
      {% for item in all_report_items %}<option value="{{ item }}" {{ 'selected' if item == report_item else '' }}>{{ item }}</option>{% endfor %}
    </select>
    <label style="margin:0;">Month</label>
    <input type="month" id="reportMonthSelect" value="{{ report_month }}" onchange="changeMonthlyReport()">
  </div>
  {% if monthly_report %}
  <div class="stat-grid" style="margin-bottom:26px;">
    {% if 'consumption' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid #2E7D32;"><div class="label">Consumption ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.consumption }}</div></div>
    <div class="stat-card" style="border-left:4px solid #2E7D32;"><div class="label">Received ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.receipts }}</div></div>
    {% endif %}
    {% if 'issued' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid var(--accent);"><div class="label">Wire Rod Issued ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.issued }}</div></div>
    <div class="stat-card" style="border-left:4px solid var(--accent);"><div class="label">Received ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.receipts }}</div></div>
    {% endif %}
    {% if 'production' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid var(--ink);"><div class="label">Production ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.production }}</div></div>
    {% if show_sales and 'sales_qty' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid #8B5E00;"><div class="label">Qty Sold ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.sales_qty }}</div></div>
    <div class="stat-card" style="border-left:4px solid #8B5E00;"><div class="label">Revenue ({{ monthly_report.month }})</div><div class="value">Rs. {{ monthly_report.sales_revenue }}</div></div>
    {% endif %}
    {% endif %}
  </div>
  {% endif %}

  <h2 class="section">Recent Entries</h2>

  <details>
    <summary>Furnace Readings</summary>
    <div class="table-wrap"><table>
      <tr><th>Date</th><th>Time</th><th>Operator</th><th>T1</th><th>T2</th><th>T3</th><th>B1</th><th>B1 Hrs</th><th>B2</th><th>B2 Hrs</th><th>Alerts</th>{% if show_edit %}<th>Edit</th>{% endif %}</tr>
      {% for r in furnace_rows %}
      <tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[4] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td><td>{{ r[7] }}</td><td>{{ r[8] }}</td><td>{{ r[9] }}</td><td>{{ r[10] }}</td><td>{{ r[11] }}</td>
      {% if show_edit %}<td><a href="/edit-entry?table=readings&id={{ r[0] }}&key={{ admin_key }}">Edit</a><a href="/delete-entry?table=readings&id={{ r[0] }}&key={{ admin_key }}" style="color:var(--bad);margin-left:8px;">Delete</a></td>{% endif %}</tr>
      {% endfor %}
    </table></div>
  </details>

  <details>
    <summary>Production</summary>
    <div class="table-wrap"><table>
      <tr><th>Date</th><th>Time</th><th>Operator</th><th>Item</th><th>Qty</th>{% if show_edit %}<th>Edit</th>{% endif %}</tr>
      {% for r in production_rows %}
      <tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td>
      {% if show_edit %}<td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a><a href="/delete-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}" style="color:var(--bad);margin-left:8px;">Delete</a></td>{% endif %}</tr>
      {% endfor %}
    </table></div>
  </details>

  <details>
    <summary>Consumption</summary>
    <div class="table-wrap"><table>
      <tr><th>Date</th><th>Time</th><th>Operator</th><th>Item</th><th>Qty</th>{% if show_edit %}<th>Edit</th>{% endif %}</tr>
      {% for r in consumption_rows %}
      <tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td>
      {% if show_edit %}<td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a><a href="/delete-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}" style="color:var(--bad);margin-left:8px;">Delete</a></td>{% endif %}</tr>
      {% endfor %}
    </table></div>
  </details>

  <details>
    <summary>Electricity &amp; Wire Rod</summary>
    <div class="table-wrap"><table>
      <tr><th>Date</th><th>Time</th><th>Operator</th><th>Electricity Units</th><th>Wire Rod Size</th><th>Qty</th>{% if show_edit %}<th>Edit</th>{% endif %}</tr>
      {% for r in electricity_rows %}
      <tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[10] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td>
      {% if show_edit %}<td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a><a href="/delete-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}" style="color:var(--bad);margin-left:8px;">Delete</a></td>{% endif %}</tr>
      {% endfor %}
    </table></div>
  </details>

  <details>
    <summary>Receipts</summary>
    <div class="table-wrap"><table>
      <tr><th>Date</th><th>Time</th><th>Operator</th><th>Category</th><th>Item</th><th>Qty</th>{% if show_edit %}<th>Edit</th>{% endif %}</tr>
      {% for r in receipts_rows %}
      <tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ 'Consumables' if r[4] == 'receipt_consumables' else 'Raw Material' }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td>
      {% if show_edit %}<td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a><a href="/delete-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}" style="color:var(--bad);margin-left:8px;">Delete</a></td>{% endif %}</tr>
      {% endfor %}
    </table></div>
  </details>

  {% if show_sales %}
  <details>
    <summary>Sales</summary>
    <div class="table-wrap"><table>
      <tr><th>Date</th><th>Time</th><th>Operator</th><th>Item</th><th>Qty</th><th>Price</th><th>Total</th><th>Customer</th>{% if show_edit %}<th>Edit</th>{% endif %}</tr>
      {% for r in sales_rows %}
      <tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td><td>{{ r[7] }}</td><td>{{ r[8] }}</td><td>{{ r[9] }}</td>
      {% if show_edit %}<td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a><a href="/delete-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}" style="color:var(--bad);margin-left:8px;">Delete</a></td>{% endif %}</tr>
      {% endfor %}
    </table></div>
  </details>
  {% endif %}

  <div class="footer-brand">
    <b>Khemji Wire &amp; Wire Pvt. Ltd.</b> &middot; F-153, Sarna Doongar, RIICO Industrial Area, Jaipur, Rajasthan 302012<br>
    Phone: +91-9829277869 &middot; +91-141-2954144 &middot; Email: info@khemjiwire.in<br>
    GSTIN: 08AAECA7760L1ZA &middot; IS 280 &amp; IS 3975 Certified
  </div>
</div>
<script>
  var stockData = { raw: {{ raw_material_stock | tojson }}, finished: {{ finished_goods_stock | tojson }}, consumables: {{ consumables_stock | tojson }} };
  var colors = { raw: '#1B3A5C', finished: '#1F2937', consumables: '#2E7D32' };
  function renderBars(cat) {
    var container = document.getElementById('bars-' + cat);
    container.innerHTML = '';
    var items = stockData[cat];
    var maxVal = Math.max.apply(null, items.map(function(r) { return Math.abs(r.balance); }).concat([1]));
    items.forEach(function(r) {
      var pct = Math.max(4, Math.min(100, (Math.abs(r.balance) / maxVal) * 100));
      var barColor = r.balance <= 0 ? '#C62828' : colors[cat];
      var row = document.createElement('div');
      row.className = 'bar-row';
      row.innerHTML = '<div class="bar-label">' + r.item + '</div><div class="bar-track"><div class="bar-fill" style="width:' + pct + '%;background:' + barColor + ';"></div></div><div class="bar-value">' + r.balance + '</div>';
      container.appendChild(row);
    });
  }
  ['raw', 'finished', 'consumables'].forEach(renderBars);
  function showCategory() {
    var cat = document.getElementById('catSelect').value;
    ['raw', 'finished', 'consumables'].forEach(function(c) {
      document.getElementById('stat-' + c).style.display = (c === cat) ? 'grid' : 'none';
      document.getElementById('table-' + c).style.display = (c === cat) ? 'block' : 'none';
      document.getElementById('bars-' + c).style.display = (c === cat) ? 'block' : 'none';
    });
  }
</script>
"""


# ---------- Dashboard route ----------
DASHBOARD_ERROR_HTML = """
<div style="font-family:'Segoe UI',Arial,sans-serif;max-width:480px;margin:60px auto;text-align:center;
            background:white;border-radius:10px;padding:32px 24px;box-shadow:0 4px 16px rgba(0,0,0,0.1);">
  <div style="font-size:40px;margin-bottom:10px;">&#9888;</div>
  <h2 style="color:#1B3A5C;margin:0 0 10px;">Dashboard Temporarily Unavailable</h2>
  <p style="color:#4B5563;line-height:1.5;">Please wait a few seconds and reload the page.</p>
  <p style="color:#9CA3AF;font-size:12px;margin-top:18px;">({{ error }})</p>
</div>
"""


def render_dashboard(is_admin, selected_date=None, operator_filter=None, report_item=None, report_month=None):
    today = now_ist().strftime("%Y-%m-%d")
    date_str = selected_date or today
    is_today = (date_str == today)

    consumables_stock, raw_material_stock, finished_goods_stock, totals = compute_stock(date_str)

    low_stock_items = []
    for row in raw_material_stock:
        if row["balance"] <= 0:
            low_stock_items.append({"category": "Raw Material", "item": row["item"], "balance": row["balance"]})
    for row in finished_goods_stock:
        if row["balance"] <= 0:
            low_stock_items.append({"category": "Finished Goods", "item": row["item"], "balance": row["balance"]})
    for row in consumables_stock:
        if row["balance"] <= 0:
            low_stock_items.append({"category": "Consumables", "item": row["item"], "balance": row["balance"]})

    # sum production across ALL items for the day:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(li.quantity),0) FROM line_items li JOIN submissions s ON li.batch_id=s.batch_id
        WHERE li.category='production' AND s.entry_date=%s
    """, (date_str,))
    day_production = round(float(cur.fetchone()[0]), 2)
    cur.close()

    day_sales_qty, day_sales_revenue, top_seller = None, None, None
    if is_admin:
        day_sales_by_item, day_sales_qty, day_sales_revenue = get_sales_summary_for_date(date_str)
        if day_sales_by_item:
            top_item = max(day_sales_by_item, key=day_sales_by_item.get)
            top_seller = {"item": top_item, "qty": round(day_sales_by_item[top_item], 2)}

    last_updated = now_ist().strftime("%d %b %Y, %I:%M %p")

    known_items = get_all_known_items()
    all_report_items = sorted(set(known_items["Consumables"]) | set(known_items["Raw Material"]) | set(known_items["Finished Goods"])
                               | set(SEED_ITEMS["Consumables"]) | set(SEED_ITEMS["Raw Material"]) | set(SEED_ITEMS["Finished Goods"]))
    month_str = report_month or now_ist().strftime("%Y-%m")
    item_for_report = report_item or (all_report_items[0] if all_report_items else "")
    item_category = find_item_category(item_for_report, {
        "Consumables": sorted(set(SEED_ITEMS["Consumables"]) | set(known_items["Consumables"])),
        "Raw Material": sorted(set(SEED_ITEMS["Raw Material"]) | set(known_items["Raw Material"])),
        "Finished Goods": sorted(set(SEED_ITEMS["Finished Goods"]) | set(known_items["Finished Goods"])),
    })
    monthly_report = get_monthly_item_report(item_for_report, month_str, item_category, is_admin) if item_category else None

    furnace_rows = get_recent_readings(operator_filter=operator_filter)
    production_rows = get_recent_line_items(["production"], operator_filter=operator_filter)
    consumption_rows = get_recent_line_items(["consumption"], operator_filter=operator_filter)
    electricity_rows = get_recent_line_items(["wire_rod"], operator_filter=operator_filter)
    receipts_rows = get_recent_line_items(["receipt_consumables", "receipt_raw_material"], operator_filter=operator_filter)
    sales_rows = get_recent_line_items(["sale"], operator_filter=operator_filter) if is_admin else []

    return render_template_string(
        DASHBOARD_HTML,
        admin_key=ADMIN_KEY if is_admin else "",
        show_sales=is_admin, show_edit=is_admin,
        selected_date=date_str, is_today=is_today,
        operator_filter=operator_filter or "", all_operator_names=sorted(ALL_PEOPLE_NAMES),
        low_stock_items=low_stock_items, low_stock_count=len(low_stock_items),
        day_production=day_production, day_sales_qty=day_sales_qty, day_sales_revenue=day_sales_revenue, top_seller=top_seller,
        last_updated=last_updated, totals=totals,
        all_report_items=all_report_items, report_item=item_for_report, report_month=month_str, monthly_report=monthly_report,
        consumables_stock=consumables_stock, raw_material_stock=raw_material_stock, finished_goods_stock=finished_goods_stock,
        furnace_rows=furnace_rows, production_rows=production_rows, consumption_rows=consumption_rows,
        electricity_rows=electricity_rows, receipts_rows=receipts_rows, sales_rows=sales_rows,
    )


@app.route("/dashboard", methods=["GET"])
def dashboard():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    try:
        return render_dashboard(
            is_admin=True, selected_date=request.args.get("date"),
            operator_filter=request.args.get("operator") or None,
            report_item=request.args.get("report_item"), report_month=request.args.get("report_month"),
        )
    except Exception as e:
        print(f"  -> Dashboard render failed: {e}")
        return render_template_string(DASHBOARD_ERROR_HTML, error=str(e)), 503


@app.route("/operator-dashboard", methods=["GET"])
def operator_dashboard():
    try:
        return render_dashboard(
            is_admin=False, selected_date=request.args.get("date"),
            operator_filter=request.args.get("operator") or None,
            report_item=request.args.get("report_item"), report_month=request.args.get("report_month"),
        )
    except Exception as e:
        print(f"  -> Operator dashboard render failed: {e}")
        return render_template_string(DASHBOARD_ERROR_HTML, error=str(e)), 503


# ---------- Admin edit ----------
EDIT_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Edit Entry</h1>
  <p class="subtitle">{{ table }} &mdash; ID {{ row_id }}</p>
  <form method="POST" action="/save-edit">
    <input type="hidden" name="table" value="{{ table }}">
    <input type="hidden" name="row_id" value="{{ row_id }}">
    <input type="hidden" name="key" value="{{ admin_key }}">
    {% for col_name, value in fields %}
    <label>{{ col_name }}{{ ' *' if col_name in protected_columns else '' }}</label>
    <input type="text" name="col_{{ col_name }}" value="{{ value if value is not none else '' }}" {{ 'required' if col_name in protected_columns else '' }}>
    {% endfor %}
    {% if protected_columns %}<p style="font-size:12px;color:var(--ink-soft);">* Required field &mdash; can't be left blank</p>{% endif %}
    <button class="submit" type="submit">Save Changes</button>
  </form>
  <a href="/delete-entry?table={{ table }}&id={{ row_id }}&key={{ admin_key }}"
     style="display:block;text-align:center;margin-top:16px;color:var(--bad);font-weight:700;text-decoration:none;font-size:14px;">
     &#128465; Delete This Entry
  </a>
</div>
"""

DELETE_CONFIRM_HTML = BASE_STYLE + """
<div class="card">
  <h1 style="border-left-color:var(--bad);">Confirm Delete</h1>
  <p class="subtitle">This cannot be undone. Google Sheets will also be updated to remove it.</p>
  <div style="background:var(--paper);border:1px solid var(--line);border-radius:8px;padding:14px 16px;margin:16px 0;">
    {% for col_name, value in fields %}
    <div style="display:flex;justify-content:space-between;padding:4px 0;font-size:14px;">
      <span style="color:var(--ink-soft);">{{ col_name }}</span><b>{{ value if value is not none else '' }}</b>
    </div>
    {% endfor %}
  </div>
  <form method="POST" action="/confirm-delete">
    <input type="hidden" name="table" value="{{ table }}">
    <input type="hidden" name="row_id" value="{{ row_id }}">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <div style="display:flex;gap:10px;margin-top:10px;">
      <a href="/dashboard?key={{ admin_key }}" style="flex:1;text-align:center;padding:15px 0;background:var(--ink);color:white;border-radius:8px;text-decoration:none;font-weight:700;text-transform:uppercase;">Cancel</a>
      <button type="submit" style="flex:1;padding:15px 0;background:var(--bad);color:white;border:none;border-radius:8px;font-weight:700;text-transform:uppercase;cursor:pointer;">Yes, Delete</button>
    </div>
  </form>
</div>
"""

READINGS_COLUMNS = ["entry_date", "entry_time", "operator", "t1", "t2", "t3", "b1", "b1_hours", "b2", "b2_hours", "alerts"]
LINE_ITEMS_COLUMNS = ["item_name", "quantity", "price", "total_amount"]
NOT_NULL_COLUMNS = {
    "line_items": {"item_name", "quantity"},
    "readings": {"entry_date", "entry_time", "operator"},
}


@app.route("/edit-entry", methods=["GET"])
def edit_entry():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    table = request.args.get("table", "")
    row_id = request.args.get("id", "")
    if table not in ("readings", "line_items") or not row_id:
        abort(400)

    columns = READINGS_COLUMNS if table == "readings" else LINE_ITEMS_COLUMNS
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT {', '.join(columns)} FROM {table} WHERE id=%s", (row_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        abort(404)

    fields = list(zip(columns, row))
    return render_template_string(EDIT_FORM_HTML, table=table, row_id=row_id, fields=fields, admin_key=ADMIN_KEY,
                                   protected_columns=NOT_NULL_COLUMNS.get(table, set()))


@app.route("/save-edit", methods=["POST"])
def save_edit():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    table = request.form.get("table", "")
    row_id = request.form.get("row_id", "")
    columns = READINGS_COLUMNS if table == "readings" else LINE_ITEMS_COLUMNS
    protected = NOT_NULL_COLUMNS.get(table, set())

    conn = get_db_connection()
    cur = conn.cursor()

    set_parts = []
    values = []
    for col in columns:
        raw = request.form.get(f"col_{col}", "")
        if col in protected:
            # Never allow a required field to be blanked out - keep the existing
            # value instead if the submitted value is empty.
            set_parts.append(f"{col} = COALESCE(NULLIF(%s, ''), {col})")
            values.append(raw)
        else:
            set_parts.append(f"{col} = %s")
            values.append(raw if raw != "" else None)

    set_clause = ", ".join(set_parts)
    cur.execute(f"UPDATE {table} SET {set_clause} WHERE id=%s", values + [row_id])
    conn.commit()
    cur.close()

    resync_sheet_for_table(table)

    return render_template_string(SUCCESS_HTML, operator="Admin", alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/dashboard?key={ADMIN_KEY}";}}, 1200);</script>'


@app.route("/delete-entry", methods=["GET"])
def delete_entry_confirm():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    table = request.args.get("table", "")
    row_id = request.args.get("id", "")
    if table not in ("readings", "line_items") or not row_id:
        abort(400)

    columns = READINGS_COLUMNS if table == "readings" else LINE_ITEMS_COLUMNS
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT {', '.join(columns)} FROM {table} WHERE id=%s", (row_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        abort(404)

    fields = list(zip(columns, row))
    return render_template_string(DELETE_CONFIRM_HTML, table=table, row_id=row_id, fields=fields, admin_key=ADMIN_KEY)


@app.route("/confirm-delete", methods=["POST"])
def confirm_delete():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    table = request.form.get("table", "")
    row_id = request.form.get("row_id", "")
    if table not in ("readings", "line_items") or not row_id:
        abort(400)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table} WHERE id=%s", (row_id,))
    conn.commit()
    cur.close()

    resync_sheet_for_table(table)

    return render_template_string(SUCCESS_HTML, operator="Admin", alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/dashboard?key={ADMIN_KEY}";}}, 1200);</script>'


# ---------- Admin: set opening stock ----------
OPENING_STOCK_HTML = BASE_STYLE + """
<div class="card wide">
  <h1>Set Opening Stock</h1>
  <p class="subtitle">One-time (or occasional) baseline for each item</p>
  <form method="POST">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <h2 class="section">Consumables</h2>
    {% for item, qty in consumables %}
    <label>{{ item }}</label><input type="number" step="0.1" name="qty_{{ item }}" value="{{ qty }}">
    {% endfor %}
    <h2 class="section">Raw Material</h2>
    {% for item, qty in raw_material %}
    <label>{{ item }}</label><input type="number" step="0.1" name="qty_{{ item }}" value="{{ qty }}">
    {% endfor %}
    <h2 class="section">Finished Goods</h2>
    {% for item, qty in finished_goods %}
    <label>{{ item }}</label><input type="number" step="0.1" name="qty_{{ item }}" value="{{ qty }}">
    {% endfor %}
    <button class="submit" type="submit">Save Opening Stock</button>
  </form>
</div>
"""


@app.route("/opening-stock", methods=["GET", "POST"])
def opening_stock_admin():
    if request.args.get("key") != ADMIN_KEY and request.form.get("key") != ADMIN_KEY:
        abort(403)

    ensure_opening_stock_seeded()
    known = get_all_known_items()

    if request.method == "POST":
        conn = get_db_connection()
        cur = conn.cursor()
        for category, items in [("Consumables", set(SEED_ITEMS["Consumables"]) | set(known["Consumables"])),
                                 ("Raw Material", set(SEED_ITEMS["Raw Material"]) | set(known["Raw Material"])),
                                 ("Finished Goods", set(SEED_ITEMS["Finished Goods"]) | set(known["Finished Goods"]))]:
            for item in items:
                val = request.form.get(f"qty_{item}")
                if val is not None:
                    cur.execute("""
                        INSERT INTO opening_stock (item_name, category, opening_qty) VALUES (%s,%s,%s)
                        ON CONFLICT (item_name) DO UPDATE SET opening_qty=EXCLUDED.opening_qty, updated_at=NOW()
                    """, (item, category, safe_float(val)))
        conn.commit()
        cur.close()

    opening = get_opening_stock_map()
    consumables = sorted((i, opening.get(i, 0.0)) for i in set(SEED_ITEMS["Consumables"]) | set(known["Consumables"]))
    raw_material = sorted((i, opening.get(i, 0.0)) for i in set(SEED_ITEMS["Raw Material"]) | set(known["Raw Material"]))
    finished_goods = sorted((i, opening.get(i, 0.0)) for i in set(SEED_ITEMS["Finished Goods"]) | set(known["Finished Goods"]))

    return render_template_string(OPENING_STOCK_HTML, admin_key=ADMIN_KEY,
                                   consumables=consumables, raw_material=raw_material, finished_goods=finished_goods)


# ---------- Manual test trigger ----------
# ---------- One-time migration: old Google Sheets -> new database ----------
MIGRATION_RESULT_HTML = BASE_STYLE + """
<div class="card wide">
  <h1>Migration Results</h1>
  <div class="stat-grid">
    {% for tab, count in counts.items() %}
    <div class="stat-card"><div class="label">{{ tab }}</div><div class="value">{{ count }} rows</div></div>
    {% endfor %}
  </div>
  {% if errors %}
  <h2 class="section">Skipped / Problem Rows ({{ errors|length }})</h2>
  <div class="table-wrap"><table><tr><th>Detail</th></tr>
    {% for e in errors %}<tr><td>{{ e }}</td></tr>{% endfor %}
  </table></div>
  {% else %}
  <p style="color:var(--ok);font-weight:700;">No problem rows - everything migrated cleanly.</p>
  {% endif %}
  <a href="/dashboard?key={{ admin_key }}" style="display:block;text-align:center;margin-top:20px;color:var(--accent-dark);font-weight:700;text-decoration:none;">&larr; Go to Dashboard</a>
</div>
"""


def _cell(row, idx_map, name):
    i = idx_map.get(name)
    if i is None or i >= len(row):
        return None
    val = row[i]
    return val if val != "" else None


def _split_date_time(row, idx_map):
    """Supports both old combined 'Timestamp' column and newer split Date/Time columns."""
    if idx_map.get("Date") is not None and idx_map.get("Time") is not None:
        return _cell(row, idx_map, "Date"), _cell(row, idx_map, "Time")
    ts = _cell(row, idx_map, "Timestamp")
    if ts:
        parts = ts.split(" ")
        return parts[0], (parts[1] if len(parts) > 1 else "00:00:00")
    return None, None


def migrate_readings():
    ws = get_or_create_sheet_tab("Readings", ["Date", "Time", "Operator", "T1", "T2", "T3", "B1", "B1 Hours", "B2", "B2 Hours", "Alerts"])
    all_values = ws.get_all_values()
    if len(all_values) <= 1:
        return 0, []
    header, rows = all_values[0], all_values[1:]
    idx_map = {name: i for i, name in enumerate(header)}
    conn = get_db_connection()
    cur = conn.cursor()
    count, errors = 0, []
    for rn, row in enumerate(rows, start=2):
        try:
            d, t = _split_date_time(row, idx_map)
            if not d:
                continue
            operator = _cell(row, idx_map, "Operator") or "Unknown"
            cur.execute("""INSERT INTO readings (entry_date, entry_time, operator, t1, t2, t3, b1, b1_hours, b2, b2_hours, alerts)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (d, t, operator, _cell(row, idx_map, "T1"), _cell(row, idx_map, "T2"), _cell(row, idx_map, "T3"),
                         _cell(row, idx_map, "B1"), _cell(row, idx_map, "B1 Hours"), _cell(row, idx_map, "B2"),
                         _cell(row, idx_map, "B2 Hours"), _cell(row, idx_map, "Alerts")))
            count += 1
        except Exception as e:
            errors.append(f"Readings row {rn}: {e}")
    conn.commit()
    cur.close()
    return count, errors


def migrate_wide_category(tab_name, form_type, category_db_value, skip_columns):
    ws = get_or_create_sheet_tab(tab_name, ["Date", "Time", "Operator"])
    all_values = ws.get_all_values()
    if len(all_values) <= 1:
        return 0, []
    header, rows = all_values[0], all_values[1:]
    idx_map = {name: i for i, name in enumerate(header)}
    item_columns = [h for h in header if h and h not in skip_columns]

    conn = get_db_connection()
    cur = conn.cursor()
    count, errors = 0, []
    for rn, row in enumerate(rows, start=2):
        try:
            d, t = _split_date_time(row, idx_map)
            if not d:
                continue
            operator = _cell(row, idx_map, "Operator") or "Unknown"
            batch_id = str(uuid.uuid4())
            cur.execute("INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator) VALUES (%s,%s,%s,%s,%s)",
                        (batch_id, form_type, d, t, operator))
            for col in item_columns:
                val = _cell(row, idx_map, col)
                qty = safe_float(val)
                if qty != 0:
                    cur.execute("INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                                (batch_id, category_db_value, col, qty))
            count += 1
        except Exception as e:
            errors.append(f"{tab_name} row {rn}: {e}")
    conn.commit()
    cur.close()
    return count, errors


def migrate_electricity_wire_rod():
    ws = get_or_create_sheet_tab("ElectricityWireRod", ["Date", "Time", "Operator", "Electricity Units"])
    all_values = ws.get_all_values()
    if len(all_values) <= 1:
        return 0, []
    header, rows = all_values[0], all_values[1:]
    idx_map = {name: i for i, name in enumerate(header)}
    skip = {"Date", "Time", "Timestamp", "Operator", "Electricity Units", "Wire Rod Issued"}
    item_columns = [h for h in header if h and h not in skip]

    conn = get_db_connection()
    cur = conn.cursor()
    count, errors = 0, []
    for rn, row in enumerate(rows, start=2):
        try:
            d, t = _split_date_time(row, idx_map)
            if not d:
                continue
            operator = _cell(row, idx_map, "Operator") or "Unknown"
            units = safe_float(_cell(row, idx_map, "Electricity Units"))
            batch_id = str(uuid.uuid4())
            cur.execute("INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator, electricity_units) VALUES (%s,%s,%s,%s,%s,%s)",
                        (batch_id, "electricity_wire_rod", d, t, operator, units))

            # newer combined "Wire Rod Issued" text column (e.g. "5.5 mm=120; 6.00 mm=40")
            combined = _cell(row, idx_map, "Wire Rod Issued")
            if combined:
                for pair in combined.split(";"):
                    pair = pair.strip()
                    if "=" in pair:
                        size, qty = pair.split("=", 1)
                        q = safe_float(qty)
                        if q != 0:
                            cur.execute("INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                                        (batch_id, "wire_rod", size.strip(), q))

            for col in item_columns:
                qty = safe_float(_cell(row, idx_map, col))
                if qty != 0:
                    cur.execute("INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                                (batch_id, "wire_rod", col, qty))
            count += 1
        except Exception as e:
            errors.append(f"ElectricityWireRod row {rn}: {e}")
    conn.commit()
    cur.close()
    return count, errors


def migrate_receipts():
    ws = get_or_create_sheet_tab("Receipts", ["Date", "Time", "Operator", "Category", "Item", "Quantity"])
    all_values = ws.get_all_values()
    if len(all_values) <= 1:
        return 0, []
    header, rows = all_values[0], all_values[1:]
    idx_map = {name: i for i, name in enumerate(header)}

    conn = get_db_connection()
    cur = conn.cursor()
    count, errors = 0, []
    for rn, row in enumerate(rows, start=2):
        try:
            d, t = _split_date_time(row, idx_map)
            if not d:
                continue
            operator = _cell(row, idx_map, "Operator") or "Unknown"
            category = _cell(row, idx_map, "Category") or ""
            item = _cell(row, idx_map, "Item") or ""
            qty = safe_float(_cell(row, idx_map, "Quantity"))
            if not item:
                continue
            category_db = "receipt_consumables" if category == "Consumables" else "receipt_raw_material"
            batch_id = str(uuid.uuid4())
            cur.execute("INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator) VALUES (%s,%s,%s,%s,%s)",
                        (batch_id, "receipt", d, t, operator))
            cur.execute("INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                        (batch_id, category_db, item, qty))
            count += 1
        except Exception as e:
            errors.append(f"Receipts row {rn}: {e}")
    conn.commit()
    cur.close()
    return count, errors


def migrate_sales():
    ws = get_or_create_sheet_tab("Sales", ["Date", "Time", "Operator", "Item", "Quantity"])
    all_values = ws.get_all_values()
    if len(all_values) <= 1:
        return 0, []
    header, rows = all_values[0], all_values[1:]
    idx_map = {name: i for i, name in enumerate(header)}

    conn = get_db_connection()
    cur = conn.cursor()
    count, errors = 0, []
    for rn, row in enumerate(rows, start=2):
        try:
            d, t = _split_date_time(row, idx_map)
            if not d:
                continue
            operator = _cell(row, idx_map, "Operator") or "Unknown"
            item = _cell(row, idx_map, "Item") or ""
            qty = safe_float(_cell(row, idx_map, "Quantity"))
            price_val = _cell(row, idx_map, "Price (Rs/Kg)")
            total_val = _cell(row, idx_map, "Total Amount (Rs)")
            customer = _cell(row, idx_map, "Customer") or ""
            if not item:
                continue
            price = safe_float(price_val) if price_val else None
            total = safe_float(total_val) if total_val else (round(qty * (price or 0), 2))
            batch_id = str(uuid.uuid4())
            cur.execute("INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator, customer) VALUES (%s,%s,%s,%s,%s,%s)",
                        (batch_id, "sale", d, t, operator, customer))
            cur.execute("INSERT INTO line_items (batch_id, category, item_name, quantity, price, total_amount) VALUES (%s,%s,%s,%s,%s,%s)",
                        (batch_id, "sale", item, qty, price, total))
            count += 1
        except Exception as e:
            errors.append(f"Sales row {rn}: {e}")
    conn.commit()
    cur.close()
    return count, errors


def migrate_opening_stock():
    ws = get_or_create_sheet_tab("OpeningStock", ["Item", "Category", "Opening Qty"])
    all_values = ws.get_all_values()
    if len(all_values) <= 1:
        return 0, []
    header, rows = all_values[0], all_values[1:]
    idx_map = {name: i for i, name in enumerate(header)}

    conn = get_db_connection()
    cur = conn.cursor()
    count, errors = 0, []
    for rn, row in enumerate(rows, start=2):
        try:
            item = _cell(row, idx_map, "Item")
            category = _cell(row, idx_map, "Category")
            qty = safe_float(_cell(row, idx_map, "Opening Qty"))
            if not item or not category:
                continue
            cur.execute("""
                INSERT INTO opening_stock (item_name, category, opening_qty) VALUES (%s,%s,%s)
                ON CONFLICT (item_name) DO UPDATE SET opening_qty=EXCLUDED.opening_qty, updated_at=NOW()
            """, (item, category, qty))
            count += 1
        except Exception as e:
            errors.append(f"OpeningStock row {rn}: {e}")
    conn.commit()
    cur.close()
    return count, errors


@app.route("/migrate-from-sheets", methods=["GET"])
def migrate_from_sheets():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    if request.args.get("confirm") != "yes":
        return """
        <div style="font-family:sans-serif;max-width:500px;margin:60px auto;text-align:center;">
        <h2>Run Historical Data Migration?</h2>
        <p>This reads your old Google Sheets tabs and imports everything into the new database.
        Only run this ONCE - running it twice will create duplicate entries.</p>
        <a href="?key=""" + ADMIN_KEY + """&confirm=yes" style="display:inline-block;padding:14px 28px;background:#1B3A5C;color:white;border-radius:8px;text-decoration:none;font-weight:bold;">Yes, Run Migration Now</a>
        </div>
        """

    counts = {}
    all_errors = []

    counts["Readings"], errs = migrate_readings()
    all_errors += errs
    counts["Production"], errs = migrate_wide_category("Production", "production", "production", {"Date", "Time", "Timestamp", "Operator", "Total Production"})
    all_errors += errs
    counts["Consumption"], errs = migrate_wide_category("Consumption", "consumption", "consumption", {"Date", "Time", "Timestamp", "Operator", "Additional Consumables"})
    all_errors += errs
    counts["ElectricityWireRod"], errs = migrate_electricity_wire_rod()
    all_errors += errs
    counts["Receipts"], errs = migrate_receipts()
    all_errors += errs
    counts["Sales"], errs = migrate_sales()
    all_errors += errs
    counts["OpeningStock"], errs = migrate_opening_stock()
    all_errors += errs

    # Rebuild Sheets mirrors from the newly-migrated database so both are perfectly aligned
    resync_sheet_for_table("readings")
    resync_sheet_for_table("line_items")

    return render_template_string(MIGRATION_RESULT_HTML, counts=counts, errors=all_errors, admin_key=ADMIN_KEY)


@app.route("/test-daily-close", methods=["GET"])
def test_daily_close():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    run_daily_stock_close()
    return "Daily stock close run manually for yesterday's date."


if __name__ == "__main__":
    init_db()
    try:
        with app.app_context():
            ensure_opening_stock_seeded()
    except Exception as e:
        print(f"  -> WARNING: could not seed opening stock at startup (will retry when DB is reachable): {e}")

    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger

    scheduler = BackgroundScheduler()

    def run_daily_stock_close_job():
        with app.app_context():
            run_daily_stock_close()

    scheduler.add_job(run_daily_stock_close_job, CronTrigger(hour=0, minute=0, timezone=IST))
    scheduler.start()

    print("Khemji Wire Inventory App (PostgreSQL) - running.")
    print("Daily stock close runs automatically at 00:00 IST.")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
