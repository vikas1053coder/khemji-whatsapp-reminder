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
from urllib.parse import quote
import psycopg2
import gspread
from google.oauth2.service_account import Credentials

IST = ZoneInfo("Asia/Kolkata")


def now_ist():
    return datetime.now(IST)


def default_entry_time():
    return now_ist().strftime("%Y-%m-%dT%H:%M")


def default_entry_time_yesterday():
    """For forms that report the PREVIOUS day's shift (8am-8am), entered the
    next morning - Production/Consumption and Electricity & Wire Rod. This
    only changes what pre-fills in the date field; operators can still
    override it, and nothing about already-submitted data is touched."""
    return (now_ist() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")


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


def is_likely_duplicate_submission(form_type, operator, entry_date, window_seconds=90):
    """Server-side guard against duplicate submissions - catches cases the
    client-side 'disable button on click' protection can miss (slow page
    loads causing a second tap, network retries, etc). If the same operator
    submitted the same form for the same date within the last window_seconds,
    treat this new one as a likely accidental duplicate."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM submissions
        WHERE form_type=%s AND operator=%s AND entry_date=%s
              AND created_at > NOW() - (%s || ' seconds')::INTERVAL
        LIMIT 1
    """, (form_type, operator, entry_date, window_seconds))
    row = cur.fetchone()
    cur.close()
    return row is not None


def safe_float(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


# ============ CONFIG ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "changeme123")
SHARED_PIN = os.environ.get("SHARED_PIN", "1234")
MAINTENANCE_PIN = os.environ.get("MAINTENANCE_PIN", "5678")
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
    "Semi-Finished": ["MS Wire", "Scrap"],
    "Finished Goods": ["1.25 mm", "1.40 mm", "1.60 mm", "1.60 mm S", "1.70 mm", "1.80 mm", "2.00 mm",
                       "2.25 mm", "2.50 mm", "2.95 mm", "3.00 mm", "3.35 mm", "3.80 mm", "4.00 mm", "Strip 16 Kg", "Strip 23 KG"],
}

SEED_MACHINES = ["Furnace", "Wire Drawing Machine 1", "Wire Drawing Machine 2", "Galvanizing Line", "Coiling Machine"]
SEED_SPARES = ["Bearing 6205", "V-Belt A47", "Motor 5HP", "Contactor 32A"]
EXPENSE_CATEGORIES = ["Office", "Petty Cash", "Travel", "Other"]

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


_DB_POOL = None


def get_db_pool():
    """A small pool of already-open connections, reused across requests instead
    of opening a brand new connection (TCP + SSL handshake + auth) on every
    single page load - this was a major hidden cost, especially with the
    database in a different region than the web service."""
    global _DB_POOL
    if _DB_POOL is None:
        from psycopg2 import pool as pg_pool
        _DB_POOL = pg_pool.SimpleConnectionPool(1, 20, normalize_db_url(DATABASE_URL), sslmode="require")
    return _DB_POOL


def get_db_connection():
    if not hasattr(g, "_db_conn"):
        try:
            g._db_conn = get_db_pool().getconn()
            # A pooled connection might have gone stale (e.g., DB restarted) or
            # carry leftover transaction state from a previous request - reset
            # and health-check it before trusting it.
            g._db_conn.rollback()
            with g._db_conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            try:
                get_db_pool().putconn(g._db_conn, close=True)
            except Exception:
                pass
            g._db_conn = psycopg2.connect(normalize_db_url(DATABASE_URL), sslmode="require")
    return g._db_conn


@app.teardown_appcontext
def close_db_connection(exception=None):
    conn = g.pop("_db_conn", None)
    if conn is not None:
        try:
            conn.rollback()  # leave it clean for whoever borrows it next
            get_db_pool().putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass


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
    opening_value NUMERIC,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS category_monthly_rates (
    category TEXT NOT NULL,
    month TEXT NOT NULL,
    weighted_avg_rate NUMERIC NOT NULL,
    opening_qty NUMERIC,
    opening_value NUMERIC,
    closing_qty NUMERIC,
    closing_value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (category, month)
);

CREATE TABLE IF NOT EXISTS pnl_opening_bootstrap (
    category TEXT PRIMARY KEY,
    for_month TEXT,
    opening_qty NUMERIC,
    opening_value NUMERIC,
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

CREATE TABLE IF NOT EXISTS monthly_costs (
    month TEXT PRIMARY KEY,
    electricity_cost NUMERIC NOT NULL DEFAULT 0,
    salary NUMERIC NOT NULL DEFAULT 0,
    interest NUMERIC NOT NULL DEFAULT 0,
    logistics NUMERIC NOT NULL DEFAULT 0,
    director_remuneration NUMERIC NOT NULL DEFAULT 0,
    other_costs NUMERIC NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monthly_rates (
    month TEXT PRIMARY KEY,
    cost_per_kg NUMERIC NOT NULL,
    opening_fg_value NUMERIC,
    closing_fg_value NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS spares_opening_stock (
    item_name TEXT PRIMARY KEY,
    opening_qty NUMERIC NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS spare_transactions (
    id SERIAL PRIMARY KEY,
    entry_date DATE NOT NULL,
    entry_time TIME NOT NULL,
    operator TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    item_name TEXT NOT NULL,
    quantity NUMERIC NOT NULL,
    rate NUMERIC,
    total_amount NUMERIC,
    machine TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS preventive_maintenance_schedule (
    id SERIAL PRIMARY KEY,
    machine TEXT NOT NULL,
    task TEXT NOT NULL,
    frequency_days INTEGER NOT NULL,
    last_done_date DATE,
    next_due_date DATE,
    last_cost NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS breakdown_maintenance (
    id SERIAL PRIMARY KEY,
    machine TEXT NOT NULL,
    issue_description TEXT NOT NULL,
    reported_date DATE NOT NULL,
    reported_time TIME NOT NULL,
    resolved_date DATE,
    resolved_time TIME,
    operator TEXT NOT NULL,
    repair_cost NUMERIC,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS general_expenses (
    id SERIAL PRIMARY KEY,
    entry_date DATE NOT NULL,
    category TEXT NOT NULL,
    description TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    operator TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS maintenance_expenses (
    id SERIAL PRIMARY KEY,
    entry_date DATE NOT NULL,
    machine TEXT,
    description TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    operator TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
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

        # CREATE TABLE IF NOT EXISTS does NOT add new columns to a table that
        # already exists - these patch columns added after the table was
        # already live in production. Safe to run every startup.
        cur.execute("ALTER TABLE monthly_costs ADD COLUMN IF NOT EXISTS director_remuneration NUMERIC NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE preventive_maintenance_schedule ADD COLUMN IF NOT EXISTS last_cost NUMERIC")
        cur.execute("ALTER TABLE opening_stock ADD COLUMN IF NOT EXISTS opening_value NUMERIC")
        cur.execute("ALTER TABLE pnl_opening_bootstrap ADD COLUMN IF NOT EXISTS for_month TEXT")

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
    ws.update(values=data, range_name="A1")


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
    ws.update(values=[header] + data_rows, range_name="A1")


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
    ws.update(values=[header] + data_rows, range_name="A1")


def resync_receipts_sheet():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.entry_date, s.entry_time, s.operator, li.category, li.item_name, li.quantity, li.price, li.total_amount
        FROM submissions s JOIN line_items li ON li.batch_id = s.batch_id
        WHERE li.category IN ('receipt_consumables','receipt_raw_material','receipt_semi_finished','receipt_finished_goods')
        ORDER BY s.entry_date, s.entry_time
    """)
    rows = cur.fetchall()
    cur.close()
    header = ["Date", "Time", "Operator", "Category", "Item", "Quantity", "Rate (Rs/Kg)", "Value (Rs)"]
    data = [header]
    receipt_cat_labels = {"receipt_consumables": "Consumables", "receipt_raw_material": "Raw Material",
                          "receipt_semi_finished": "Semi-Finished", "receipt_finished_goods": "Finished Goods"}
    for edate, etime, operator, category, item, qty, price, total_amount in rows:
        cat_label = receipt_cat_labels.get(category, category)
        data.append([str(edate), str(etime), operator, cat_label, item, float(qty),
                    float(price) if price is not None else "", float(total_amount) if total_amount is not None else ""])
    ws = get_or_create_sheet_tab("Receipts", header)
    ws.clear()
    ws.update(values=data, range_name="A1")


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
    ws.update(values=data, range_name="A1")


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
        ws.update(values=[header], range_name="A1")
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


def request_cache(func):
    """Caches a function's result for the lifetime of ONE request (using Flask's
    request-scoped `g`). Several functions here get called many times per page
    load with identical arguments - this ensures the expensive DB work behind
    them only actually runs once per visit, not once per call."""
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        cache_attr = f"_cache_{func.__name__}"
        if not hasattr(g, cache_attr):
            setattr(g, cache_attr, {})
        cache = getattr(g, cache_attr)
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]
    return wrapper


# ---------- Item discovery (auto-grows from real data, no code changes needed) ----------
@request_cache
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

    cur.execute("SELECT item_name FROM opening_stock WHERE category='Semi-Finished'")
    semi_finished = set(r[0] for r in cur.fetchall())
    cur.execute("SELECT DISTINCT item_name FROM line_items WHERE category IN ('ms_wire_produced','ms_wire_consumed','scrap_produced','receipt_semi_finished')")
    semi_finished |= set(r[0] for r in cur.fetchall())
    semi_finished |= set(SEED_ITEMS.get("Semi-Finished", []))

    cur.execute("SELECT item_name FROM opening_stock WHERE category='Finished Goods'")
    finished = set(r[0] for r in cur.fetchall())
    cur.execute("SELECT DISTINCT item_name FROM line_items WHERE category IN ('production','sale','receipt_finished_goods')")
    finished |= set(r[0] for r in cur.fetchall())
    finished -= semi_finished  # a semi-finished item sold directly shouldn't also count as finished goods
    finished -= raw_material   # a raw material item (e.g. Wire Rod) sold directly shouldn't count as finished goods either

    cur.close()
    return {
        "Consumables": sorted(consumables),
        "Raw Material": sorted(raw_material),
        "Semi-Finished": sorted(semi_finished),
        "Finished Goods": sorted(finished),
    }


def get_dropdown_items(category_label):
    known = get_all_known_items().get(category_label, [])
    return sorted(set(SEED_ITEMS.get(category_label, [])) | set(known))


@request_cache
def get_known_machines():
    conn = get_db_connection()
    cur = conn.cursor()
    machines = set(SEED_MACHINES)
    cur.execute("SELECT DISTINCT machine FROM spare_transactions WHERE machine IS NOT NULL")
    machines |= set(r[0] for r in cur.fetchall())
    cur.execute("SELECT DISTINCT machine FROM preventive_maintenance_schedule")
    machines |= set(r[0] for r in cur.fetchall())
    cur.execute("SELECT DISTINCT machine FROM breakdown_maintenance")
    machines |= set(r[0] for r in cur.fetchall())
    cur.close()
    return sorted(machines)


@request_cache
def get_known_spares():
    conn = get_db_connection()
    cur = conn.cursor()
    spares = set(SEED_SPARES)
    cur.execute("SELECT DISTINCT item_name FROM spare_transactions")
    spares |= set(r[0] for r in cur.fetchall())
    cur.close()
    return sorted(spares)


def sum_general_expenses_for_month(month_str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(amount),0) FROM general_expenses WHERE TO_CHAR(entry_date,'YYYY-MM')=%s", (month_str,))
    result = float(cur.fetchone()[0])
    cur.close()
    return result


def sum_maintenance_cost_for_month(month_str):
    """Breakdown repair costs + standalone maintenance expenses for the month -
    this is what feeds into the P&L as 'Maintenance Cost'."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(repair_cost),0) FROM breakdown_maintenance WHERE TO_CHAR(reported_date,'YYYY-MM')=%s", (month_str,))
    breakdown_total = float(cur.fetchone()[0])
    cur.execute("SELECT COALESCE(SUM(amount),0) FROM maintenance_expenses WHERE TO_CHAR(entry_date,'YYYY-MM')=%s", (month_str,))
    expense_total = float(cur.fetchone()[0])
    cur.close()
    return round(breakdown_total + expense_total, 2)


def get_spares_opening_map():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT item_name, opening_qty FROM spares_opening_stock")
    result = {r[0]: float(r[1]) for r in cur.fetchall()}
    cur.close()
    return result


def get_spare_rate(item_name):
    """Most recent rate on file for a spare part, from when it was last
    received - used to auto-calculate cost when the same spare is issued.
    Returns None if never received with a rate on file (admin can fill in
    manually afterward)."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT rate FROM spare_transactions
        WHERE item_name=%s AND transaction_type='receipt' AND rate IS NOT NULL
        ORDER BY entry_date DESC, entry_time DESC LIMIT 1
    """, (item_name,))
    row = cur.fetchone()
    cur.close()
    return float(row[0]) if row else None


def compute_spares_stock():
    """Simple live stock for spares - no daily closing chain, just current
    totals (spares don't need the same day-by-day ledger as production stock)."""
    opening = get_spares_opening_map()
    items = get_known_spares()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT item_name, transaction_type, COALESCE(SUM(quantity),0)
        FROM spare_transactions GROUP BY item_name, transaction_type
    """)
    totals = {}
    for item_name, ttype, qty in cur.fetchall():
        totals.setdefault(item_name, {"received": 0.0, "issued": 0.0})
        totals[item_name]["received" if ttype == "receipt" else "issued"] = float(qty)
    cur.close()

    rows = []
    for item in items:
        op = opening.get(item, 0.0)
        received = totals.get(item, {}).get("received", 0.0)
        issued = totals.get(item, {}).get("issued", 0.0)
        balance = round(op + received - issued, 2)
        rows.append({"item": item, "opening": op, "received": received, "issued": issued, "balance": balance})
    return rows


# ---------- Stock computation ----------
@request_cache
def get_opening_stock_map():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT item_name, opening_qty FROM opening_stock")
    result = {r[0]: float(r[1]) for r in cur.fetchall()}
    cur.close()
    return result


@request_cache
def get_opening_stock_map_with_dates():
    """Returns {item: (qty, updated_at_date_str)} - used to tell whether a manual
    Opening Stock correction is MORE RECENT than the last automatic day-close,
    which means it should override the ledger chain going forward."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT item_name, opening_qty, updated_at FROM opening_stock")
    result = {r[0]: (float(r[1]), r[2].strftime("%Y-%m-%d") if r[2] else None) for r in cur.fetchall()}
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


def sum_category_qty_for_date(category_db_value, date_str):
    """Sums a line_item category across ALL items (not just one) for a date."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(li.quantity),0) FROM line_items li
        JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category=%s AND s.entry_date=%s
    """, (category_db_value, date_str))
    result = float(cur.fetchone()[0])
    cur.close()
    return result


def sum_category_qty_for_month(category_db_value, month_str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(li.quantity),0) FROM line_items li
        JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category=%s AND TO_CHAR(s.entry_date,'YYYY-MM')=%s
    """, (category_db_value, month_str))
    result = float(cur.fetchone()[0])
    cur.close()
    return result


def compute_yield_report(period_type, period_value):
    """Compares theoretical Finished Goods weight (from Wire Rod + Zinc, after
    known process losses) against what was actually reported as produced.
    period_type: 'date' or 'month'.

    Note: Production and Electricity & Wire Rod forms now default their date
    to the actual shift day (the 8am-8am shift, entered the next morning) -
    so Wire Rod/Zinc and Production entries for the same shift already share
    the same entry_date. No additional day-shift is needed here anymore;
    doing so would double-shift and produce a nonsensical mismatch.
    """
    if period_type == "date":
        wire_rod_total = sum_category_qty_for_date("wire_rod", period_value)
        zinc_total = sum_qty_for_date("consumption", "Zinc", period_value)
        actual_fg = sum_category_qty_for_date("production", period_value)
    else:
        wire_rod_total = sum_category_qty_for_month("wire_rod", period_value)
        zinc_total = sum_qty_for_month("consumption", "Zinc", period_value)
        actual_fg = sum_category_qty_for_month("production", period_value)

    calculated_fg = round(wire_rod_total * MS_WIRE_CONVERSION_FACTOR + zinc_total * ZINC_YIELD_FACTOR, 2)
    losses = round(actual_fg - calculated_fg, 2)
    return {
        "wire_rod_total": round(wire_rod_total, 2),
        "zinc_total": round(zinc_total, 2),
        "calculated_fg": calculated_fg,
        "actual_fg": round(actual_fg, 2),
        "losses": losses,
    }


# ---------- Profit & Loss engine (ADMIN ONLY - never shown to operators) ----------
def get_latest_rate(item_name):
    """Most recent Rs./Kg rate on file for an item, from its receipt history
    (admin adds this via the Edit feature after logging a receipt)."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT li.price FROM line_items li JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.item_name=%s AND li.category IN
              ('receipt_consumables','receipt_raw_material','receipt_semi_finished','receipt_finished_goods')
              AND li.price IS NOT NULL
        ORDER BY s.entry_date DESC, s.entry_time DESC LIMIT 1
    """, (item_name,))
    row = cur.fetchone()
    cur.close()
    return float(row[0]) if row else None


def get_latest_wire_rod_rate():
    """Most recent Wire Rod rate on file, regardless of specific size - used to
    value MS Wire (which isn't itself purchased, so has no rate of its own)."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT li.price FROM line_items li JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category='receipt_raw_material' AND li.price IS NOT NULL
        ORDER BY s.entry_date DESC, s.entry_time DESC LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    return float(row[0]) if row else 0.0


def get_month_boundaries(month_str):
    """Returns (first_day, last_day, prev_month_last_day) as 'YYYY-MM-DD' strings."""
    import calendar
    year, month = map(int, month_str.split("-"))
    first_day = f"{month_str}-01"
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = f"{month_str}-{last_day_num:02d}"
    if month == 1:
        prev_year, prev_month = year - 1, 12
    else:
        prev_year, prev_month = year, month - 1
    prev_last_day_num = calendar.monthrange(prev_year, prev_month)[1]
    prev_last_day = f"{prev_year:04d}-{prev_month:02d}-{prev_last_day_num:02d}"
    return first_day, last_day, prev_last_day


def get_item_balance_on_date(category_label, item_name, date_str):
    """Reuses compute_stock's already-correct (ledger-frozen vs live) logic to
    get a single item's balance on a specific date."""
    consumables_stock, raw_material_stock, semi_finished_stock, finished_goods_stock, _ = compute_stock(date_str)
    category_map = {"Consumables": consumables_stock, "Raw Material": raw_material_stock,
                    "Semi-Finished": semi_finished_stock, "Finished Goods": finished_goods_stock}
    for row in category_map.get(category_label, []):
        if row["item"] == item_name:
            return row["balance"]
    return 0.0


def get_category_total_balance_on_date(category_label, date_str):
    _, _, _, _, totals = compute_stock(date_str)
    key_map = {"Consumables": "consumables", "Raw Material": "raw_material",
              "Semi-Finished": "semi_finished", "Finished Goods": "finished_goods"}
    return totals.get(key_map.get(category_label), 0.0)


def get_category_value_consumed(category_label, consumption_category_db, month_str):
    """Sum of (qty consumed this month x that item's latest rate), across every
    item in a category - since different items may have different rates."""
    known_items = get_all_known_items()
    items = sorted(set(SEED_ITEMS.get(category_label, [])) | set(known_items.get(category_label, [])))
    total_value = 0.0
    for item in items:
        qty = sum_qty_for_month(consumption_category_db, item, month_str)
        if qty:
            rate = get_latest_rate(item) or 0.0
            total_value += qty * rate
    return round(total_value, 2)


def get_stored_monthly_rate(month_str):
    """Looks up a previously-computed month's Cost of Production per Kg -
    used as 'last month's rate' for valuing Opening Finished Goods stock."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT cost_per_kg FROM monthly_rates WHERE month=%s", (month_str,))
    row = cur.fetchone()
    cur.close()
    return float(row[0]) if row else None


def store_monthly_rate(month_str, cost_per_kg, opening_fg_value, closing_fg_value):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO monthly_rates (month, cost_per_kg, opening_fg_value, closing_fg_value)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (month) DO UPDATE SET cost_per_kg=EXCLUDED.cost_per_kg,
            opening_fg_value=EXCLUDED.opening_fg_value, closing_fg_value=EXCLUDED.closing_fg_value
    """, (month_str, cost_per_kg, opening_fg_value, closing_fg_value))
    conn.commit()
    cur.close()


def get_manual_opening_bootstrap(category_label, month_str):
    """Reads the manually-entered P&L-only opening bootstrap for a category -
    completely separate table from opening_stock, so it can never interact
    with or affect live stock tracking/correction logic. Only returns a value
    when it was explicitly set FOR this exact month - so it applies once, to
    the genuine first tracked month, and never silently leaks into later
    months once a real carried-forward rate exists."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT opening_qty, opening_value, for_month FROM pnl_opening_bootstrap WHERE category=%s", (category_label,))
    row = cur.fetchone()
    cur.close()
    if not row or row[1] is None or row[2] != month_str:
        return None, None
    return (float(row[0]) if row[0] is not None else None), float(row[1])


def get_stored_category_rate(category_label, month_str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT weighted_avg_rate FROM category_monthly_rates WHERE category=%s AND month=%s", (category_label, month_str))
    row = cur.fetchone()
    cur.close()
    return float(row[0]) if row else None


def store_category_rate(category_label, month_str, rate, opening_qty, opening_value, closing_qty, closing_value):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO category_monthly_rates (category, month, weighted_avg_rate, opening_qty, opening_value, closing_qty, closing_value)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (category, month) DO UPDATE SET weighted_avg_rate=EXCLUDED.weighted_avg_rate,
            opening_qty=EXCLUDED.opening_qty, opening_value=EXCLUDED.opening_value,
            closing_qty=EXCLUDED.closing_qty, closing_value=EXCLUDED.closing_value
    """, (category_label, month_str, rate, opening_qty, opening_value, closing_qty, closing_value))
    conn.commit()
    cur.close()


def compute_raw_material_weighted_avg(month_str):
    """Weighted-average rate for Raw Material (Wire Rod), carried forward
    monthly. Opening value/qty comes from last month's stored rate, UNLESS a
    manual bootstrap (Opening Value page) was explicitly set - that always
    takes priority, since it's a deliberate human override for a month where
    no reliable prior data exists (e.g. the system's first tracked month)."""
    first_day, last_day, prev_last_day = get_month_boundaries(month_str)
    prev_month_str = prev_last_day[:7]

    bootstrap_qty, bootstrap_value = get_manual_opening_bootstrap("Raw Material", month_str)
    if bootstrap_value is not None:
        opening_qty = bootstrap_qty if bootstrap_qty else get_category_total_balance_on_date("Raw Material", prev_last_day)
        opening_value = bootstrap_value
    else:
        opening_qty = get_category_total_balance_on_date("Raw Material", prev_last_day)
        prev_rate = get_stored_category_rate("Raw Material", prev_month_str)
        opening_value = round(opening_qty * prev_rate, 2) if prev_rate is not None else 0.0

    # This month's additions: value actually paid for Wire Rod received (from Receipts)
    added_qty = sum_category_qty_for_month("receipt_raw_material", month_str)
    added_value = get_category_value_consumed("Raw Material", "receipt_raw_material", month_str)

    total_qty = opening_qty + added_qty
    total_value = opening_value + added_value
    weighted_avg_rate = round(total_value / total_qty, 4) if total_qty > 0 else 0.0

    closing_qty = get_category_total_balance_on_date("Raw Material", last_day)
    closing_value = round(closing_qty * weighted_avg_rate, 2)

    store_category_rate("Raw Material", month_str, weighted_avg_rate, opening_qty, opening_value, closing_qty, closing_value)
    return {"opening_qty": round(opening_qty, 2), "opening_value": round(opening_value, 2),
            "weighted_avg_rate": weighted_avg_rate, "closing_qty": round(closing_qty, 2), "closing_value": closing_value,
            "added_qty": round(added_qty, 2), "added_value": round(added_value, 2)}


def compute_semi_finished_weighted_avg(month_str, raw_material_rate):
    """Weighted-average rate for Semi-Finished (MS Wire) - its own independent
    chain, separate from Raw Material's. MS Wire isn't purchased, it's
    produced, so its 'addition value' comes from the Wire Rod that went into
    it, valued at THIS SAME month's Raw Material weighted-average rate."""
    first_day, last_day, prev_last_day = get_month_boundaries(month_str)
    prev_month_str = prev_last_day[:7]

    bootstrap_qty, bootstrap_value = get_manual_opening_bootstrap("Semi-Finished", month_str)
    if bootstrap_value is not None:
        opening_qty = bootstrap_qty if bootstrap_qty else get_item_balance_on_date("Semi-Finished", "MS Wire", prev_last_day)
        opening_value = bootstrap_value
    else:
        opening_qty = get_item_balance_on_date("Semi-Finished", "MS Wire", prev_last_day)
        prev_rate = get_stored_category_rate("Semi-Finished", prev_month_str)
        opening_value = round(opening_qty * prev_rate, 2) if prev_rate is not None else 0.0

    added_qty = sum_qty_for_month("ms_wire_produced", "MS Wire", month_str)
    ms_wire_input_rate = (raw_material_rate / MS_WIRE_CONVERSION_FACTOR) if raw_material_rate else 0.0
    added_value = round(added_qty * ms_wire_input_rate, 2)

    total_qty = opening_qty + added_qty
    total_value = opening_value + added_value
    weighted_avg_rate = round(total_value / total_qty, 4) if total_qty > 0 else 0.0

    closing_qty = get_item_balance_on_date("Semi-Finished", "MS Wire", last_day)
    closing_value = round(closing_qty * weighted_avg_rate, 2)

    store_category_rate("Semi-Finished", month_str, weighted_avg_rate, opening_qty, opening_value, closing_qty, closing_value)
    return {"opening_qty": round(opening_qty, 2), "opening_value": round(opening_value, 2),
            "weighted_avg_rate": weighted_avg_rate, "closing_qty": round(closing_qty, 2), "closing_value": closing_value,
            "added_qty": round(added_qty, 2), "added_value": round(added_value, 2)}


def compute_pnl(month_str, manual_opening_fg_value=None):
    """Full monthly Profit & Loss statement, per the agreed design. ADMIN ONLY -
    the caller must ensure this is never rendered on an operator-facing page.
    manual_opening_fg_value: only needed for the very first month ever used,
    where no prior stored rate exists to value Opening Finished Goods with."""
    first_day, last_day, prev_last_day = get_month_boundaries(month_str)

    # --- Raw Material (Wire Rod): weighted-average, stock-adjusted consumption value ---
    rm = compute_raw_material_weighted_avg(month_str)
    raw_material_consumed_value = round(rm["opening_value"] + rm["added_value"] - rm["closing_value"], 2)
    consumables_value = get_category_value_consumed("Consumables", "consumption", month_str)

    # --- Monthly fixed costs (Salary/Interest/Logistics/Electricity/Director Remuneration
    #     stay manual lump sums; Other Costs and Maintenance Cost are now auto-calculated) ---
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT electricity_cost, salary, interest, logistics, director_remuneration FROM monthly_costs WHERE month=%s", (month_str,))
    row = cur.fetchone()
    cur.close()
    electricity_cost, salary, interest, logistics, director_remuneration = (
        (float(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4])) if row else (0.0, 0.0, 0.0, 0.0, 0.0)
    )
    other_costs = sum_general_expenses_for_month(month_str)
    maintenance_cost = sum_maintenance_cost_for_month(month_str)

    # --- Per-item Consumables breakdown, for full transparency in the P&L display ---
    known_items = get_all_known_items()
    consumables_breakdown = []
    for item in sorted(set(SEED_ITEMS.get("Consumables", [])) | set(known_items.get("Consumables", []))):
        qty = sum_qty_for_month("consumption", item, month_str)
        if qty:
            rate = get_latest_rate(item) or 0.0
            consumables_breakdown.append({"item": item, "qty": round(qty, 2), "rate": rate, "value": round(qty * rate, 2)})

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT entry_date, category, description, amount FROM general_expenses WHERE TO_CHAR(entry_date,'YYYY-MM')=%s ORDER BY entry_date", (month_str,))
    general_expense_rows = [{"entry_date": r[0], "category": r[1], "description": r[2], "amount": float(r[3])} for r in cur.fetchall()]
    cur.execute("SELECT entry_date, machine, description, amount FROM maintenance_expenses WHERE TO_CHAR(entry_date,'YYYY-MM')=%s ORDER BY entry_date", (month_str,))
    maintenance_expense_rows = [{"entry_date": r[0], "machine": r[1], "description": r[2], "amount": float(r[3])} for r in cur.fetchall()]
    cur.execute("SELECT reported_date, machine, issue_description, repair_cost FROM breakdown_maintenance WHERE TO_CHAR(reported_date,'YYYY-MM')=%s AND repair_cost IS NOT NULL ORDER BY reported_date", (month_str,))
    breakdown_cost_rows = [{"reported_date": r[0], "machine": r[1], "issue_description": r[2], "repair_cost": float(r[3])} for r in cur.fetchall()]
    cur.close()

    total_manufacturing_cost = round(raw_material_consumed_value + consumables_value + electricity_cost + salary + interest
                                     + logistics + director_remuneration + other_costs + maintenance_cost, 2)

    # --- MS Wire (Semi-Finished): its own independent weighted-average chain ---
    sf = compute_semi_finished_weighted_avg(month_str, rm["weighted_avg_rate"])
    ms_wire_opening_qty, ms_wire_closing_qty = sf["opening_qty"], sf["closing_qty"]
    ms_wire_opening_value, ms_wire_closing_value = sf["opening_value"], sf["closing_value"]

    # --- COGM: Cost of Goods Manufactured (accounts for MS Wire carried over between months) ---
    cogm = round(ms_wire_opening_value + total_manufacturing_cost - ms_wire_closing_value, 2)

    # --- Finished Goods produced this month (Kg), for the Cost/Kg rate ---
    fg_produced_kg = sum_category_qty_for_month("production", month_str)
    cost_per_kg_this_month = round(cogm / fg_produced_kg, 2) if fg_produced_kg else 0.0

    # --- Finished Goods stock valuation (Opening at LAST month's rate, Closing at THIS month's) ---
    fg_closing_qty = get_category_total_balance_on_date("Finished Goods", last_day)
    fg_opening_qty = get_category_total_balance_on_date("Finished Goods", prev_last_day)

    last_month_str = prev_last_day[:7]
    last_month_rate = get_stored_monthly_rate(last_month_str)
    used_manual_bootstrap = False
    if last_month_rate is None:
        if manual_opening_fg_value is not None:
            fg_opening_value = round(manual_opening_fg_value, 2)
            used_manual_bootstrap = True
        else:
            fg_opening_value = 0.0
    else:
        fg_opening_value = round(fg_opening_qty * last_month_rate, 2)

    fg_closing_value = round(fg_closing_qty * cost_per_kg_this_month, 2)

    # --- COGS: Cost of Goods Sold ---
    cogs = round(fg_opening_value + cogm - fg_closing_value, 2)

    # --- Revenue ---
    def category_sales_revenue(category_label):
        known_items = get_all_known_items()
        items = sorted(set(SEED_ITEMS.get(category_label, [])) | set(known_items.get(category_label, [])))
        total = 0.0
        for item in items:
            _, revenue = sum_sales_qty_and_revenue_for_month(item, month_str)
            total += revenue
        return round(total, 2)

    fg_revenue = category_sales_revenue("Finished Goods")
    ms_wire_revenue, _ = sum_sales_qty_and_revenue_for_month("MS Wire", month_str)
    scrap_revenue, _ = sum_sales_qty_and_revenue_for_month("Scrap", month_str)
    wire_rod_revenue = category_sales_revenue("Raw Material")
    total_revenue = round(fg_revenue + ms_wire_revenue + scrap_revenue + wire_rod_revenue, 2)

    gross_profit = round(total_revenue - cogs, 2)

    # --- Conversion Cost per Kg (everything except raw material itself) ---
    conversion_cost_total = round(consumables_value + electricity_cost + salary + interest + logistics + director_remuneration + other_costs + maintenance_cost, 2)
    conversion_cost_per_kg = round(conversion_cost_total / fg_produced_kg, 2) if fg_produced_kg else 0.0

    # --- Process losses (diagnostic only, already embedded in the costs above - not added again) ---
    wire_rod_issued_kg = sum_category_qty_for_month("wire_rod", month_str)
    scale_loss_kg = round(wire_rod_issued_kg * (MS_WIRE_SCALING_PERCENT / 100), 2)
    scale_loss_value = round(scale_loss_kg * rm["weighted_avg_rate"], 2) if rm["weighted_avg_rate"] else 0.0
    zinc_consumed_kg = sum_qty_for_month("consumption", "Zinc", month_str)
    zinc_rate = get_latest_rate("Zinc") or 0.0
    zinc_burning_loss_kg = round(zinc_consumed_kg * (ZINC_BURNING_LOSS_PERCENT / 100), 2)
    zinc_burning_loss_value = round(zinc_burning_loss_kg * zinc_rate, 2)

    result = {
        "month": month_str,
        "wire_rod_value": raw_material_consumed_value, "consumables_value": consumables_value,
        "consumables_breakdown": consumables_breakdown,
        "rm_opening_qty": rm["opening_qty"], "rm_opening_value": rm["opening_value"],
        "rm_added_qty": rm["added_qty"], "rm_added_value": rm["added_value"],
        "rm_closing_qty": rm["closing_qty"], "rm_closing_value": rm["closing_value"],
        "rm_weighted_avg_rate": rm["weighted_avg_rate"],
        "sf_weighted_avg_rate": sf["weighted_avg_rate"],
        "electricity_cost": electricity_cost, "salary": salary, "interest": interest,
        "logistics": logistics, "director_remuneration": director_remuneration, "other_costs": other_costs, "maintenance_cost": maintenance_cost,
        "general_expense_rows": general_expense_rows, "maintenance_expense_rows": maintenance_expense_rows, "breakdown_cost_rows": breakdown_cost_rows,
        "total_manufacturing_cost": total_manufacturing_cost,
        "ms_wire_opening_qty": round(ms_wire_opening_qty, 2), "ms_wire_closing_qty": round(ms_wire_closing_qty, 2),
        "ms_wire_opening_value": ms_wire_opening_value, "ms_wire_closing_value": ms_wire_closing_value,
        "cogm": cogm, "fg_produced_kg": round(fg_produced_kg, 2), "cost_per_kg_this_month": cost_per_kg_this_month,
        "fg_opening_qty": round(fg_opening_qty, 2), "fg_closing_qty": round(fg_closing_qty, 2),
        "fg_opening_value": fg_opening_value, "fg_closing_value": fg_closing_value,
        "used_manual_bootstrap": used_manual_bootstrap, "last_month_rate": last_month_rate,
        "cogs": cogs,
        "fg_revenue": fg_revenue, "ms_wire_revenue": ms_wire_revenue, "scrap_revenue": scrap_revenue,
        "wire_rod_revenue": wire_rod_revenue, "total_revenue": total_revenue,
        "gross_profit": gross_profit,
        "conversion_cost_per_kg": conversion_cost_per_kg,
        "scale_loss_kg": scale_loss_kg, "scale_loss_value": scale_loss_value,
        "zinc_burning_loss_kg": zinc_burning_loss_kg, "zinc_burning_loss_value": zinc_burning_loss_value,
    }

    # Store this month's rate so NEXT month can use it as "last month's rate"
    store_monthly_rate(month_str, cost_per_kg_this_month, fg_opening_value, fg_closing_value)

    return result


def get_sales_insights(category_label, from_date, to_date):
    """For a date range, returns per-item {qty_sold, revenue, avg_rate}, plus
    the blended total across the whole category."""
    known_items = get_all_known_items()
    items = sorted(set(SEED_ITEMS.get(category_label, [])) | set(known_items.get(category_label, [])))
    conn = get_db_connection()
    cur = conn.cursor()
    rows = []
    total_qty, total_revenue = 0.0, 0.0
    for item in items:
        cur.execute("""
            SELECT COALESCE(SUM(li.quantity),0), COALESCE(SUM(li.total_amount),0)
            FROM line_items li JOIN submissions s ON li.batch_id = s.batch_id
            WHERE li.category='sale' AND li.item_name=%s AND s.entry_date BETWEEN %s AND %s
        """, (item, from_date, to_date))
        qty, revenue = cur.fetchone()
        qty, revenue = float(qty), float(revenue)
        if qty > 0:
            rows.append({"item": item, "qty_sold": round(qty, 2), "revenue": round(revenue, 2),
                        "avg_rate": round(revenue / qty, 2) if qty else 0.0})
            total_qty += qty
            total_revenue += revenue
    cur.close()
    overall_avg = round(total_revenue / total_qty, 2) if total_qty else 0.0
    return rows, round(total_qty, 2), round(total_revenue, 2), overall_avg


def get_purchase_insights(category_label, receipt_category_db, from_date, to_date):
    """Same idea as get_sales_insights, for the purchase/receipt side."""
    known_items = get_all_known_items()
    items = sorted(set(SEED_ITEMS.get(category_label, [])) | set(known_items.get(category_label, [])))
    conn = get_db_connection()
    cur = conn.cursor()
    rows = []
    total_qty, total_amount = 0.0, 0.0
    for item in items:
        cur.execute("""
            SELECT COALESCE(SUM(li.quantity),0), COALESCE(SUM(li.total_amount),0)
            FROM line_items li JOIN submissions s ON li.batch_id = s.batch_id
            WHERE li.category=%s AND li.item_name=%s AND s.entry_date BETWEEN %s AND %s
        """, (receipt_category_db, item, from_date, to_date))
        qty, amount = cur.fetchone()
        qty, amount = float(qty), float(amount)
        if qty > 0:
            rows.append({"item": item, "qty_received": round(qty, 2), "amount_paid": round(amount, 2),
                        "avg_rate": round(amount / qty, 2) if amount else 0.0})
            total_qty += qty
            total_amount += amount
    cur.close()
    overall_avg = round(total_amount / total_qty, 2) if total_qty else 0.0
    return rows, round(total_qty, 2), round(total_amount, 2), overall_avg


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


@request_cache
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
    """Returns {item_name: (entry_date_str, closing_value)} for the most recent
    closed day per item, strictly before `before_date`."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (item_name) item_name, entry_date, closing
        FROM stock_ledger
        WHERE entry_date < %s
        ORDER BY item_name, entry_date DESC
    """, (before_date,))
    result = {r[0]: (r[1].strftime("%Y-%m-%d"), float(r[2])) for r in cur.fetchall()}
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


@request_cache
def sum_qty_by_item_for_date(category_db_value, date_str):
    """Returns {item_name: qty} for an ENTIRE category on one date - one query
    instead of one query per item."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT li.item_name, COALESCE(SUM(li.quantity),0) FROM line_items li
        JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category=%s AND s.entry_date=%s
        GROUP BY li.item_name
    """, (category_db_value, date_str))
    result = {r[0]: float(r[1]) for r in cur.fetchall()}
    cur.close()
    return result


def compute_in_out_for_date(category_label, item_name, date_str):
    if category_label == "Consumables":
        return sum_qty_for_date("receipt_consumables", item_name, date_str), sum_qty_for_date("consumption", item_name, date_str)
    elif category_label == "Raw Material":
        sales_map, _, _ = get_sales_summary_for_date(date_str)
        out_amt = sum_qty_for_date("wire_rod", item_name, date_str) + sales_map.get(item_name, 0.0)
        return sum_qty_for_date("receipt_raw_material", item_name, date_str), out_amt
    elif category_label == "Semi-Finished":
        sales_map, _, _ = get_sales_summary_for_date(date_str)
        in_amt = (sum_qty_for_date("receipt_semi_finished", item_name, date_str)
                  + sum_qty_for_date("ms_wire_produced", item_name, date_str)
                  + sum_qty_for_date("scrap_produced", item_name, date_str))
        out_amt = sum_qty_for_date("ms_wire_consumed", item_name, date_str) + sales_map.get(item_name, 0.0)
        return in_amt, out_amt
    else:
        sales_map, _, _ = get_sales_summary_for_date(date_str)
        in_amt = sum_qty_for_date("production", item_name, date_str) + sum_qty_for_date("receipt_finished_goods", item_name, date_str)
        return in_amt, sales_map.get(item_name, 0.0)


def compute_stock(selected_date=None):
    today = now_ist().strftime("%Y-%m-%d")
    date_str = selected_date or today

    opening_baseline_with_dates = get_opening_stock_map_with_dates()
    known_items = get_all_known_items()
    # Always check both: if a ledger row exists for this date, that's the frozen
    # closed snapshot. Otherwise, ALWAYS compute live from real transactions -
    # regardless of whether the date is today, an unclosed past day, or even a
    # mistakenly future-dated entry. Previously this only worked for exactly
    # "today", so any other undosed date silently showed zero.
    ledger_rows_for_date = get_ledger_rows_for_date(date_str)
    latest_closing = get_latest_closing_map(before_date=date_str)

    def get_live_opening(item):
        """The base to compute from: whichever is MORE RECENT - the last
        automatic close, or a manual Opening Stock correction. A correction
        made after the last close intentionally overrides the chain going
        forward; it never touches already-closed historical days."""
        closing_info = latest_closing.get(item)  # (date_str, closing_value) or None
        baseline_qty, baseline_updated = opening_baseline_with_dates.get(item, (0.0, None))

        if closing_info is None:
            return baseline_qty
        closing_date, closing_value = closing_info
        if baseline_updated and baseline_updated > closing_date:
            return baseline_qty
        return closing_value

    def build_category(category_label):
        items = sorted(set(SEED_ITEMS.get(category_label, [])) | set(known_items.get(category_label, [])))

        # Fetch this category's In/Out maps ONCE (covers every item), instead of
        # one query per item - this is the main fix for slow dashboard loads.
        if category_label == "Consumables":
            in_map = sum_qty_by_item_for_date("receipt_consumables", date_str)
            out_map = sum_qty_by_item_for_date("consumption", date_str)
        elif category_label == "Raw Material":
            sales_map, _, _ = get_sales_summary_for_date(date_str)
            in_map = sum_qty_by_item_for_date("receipt_raw_material", date_str)
            wire_rod_out_map = sum_qty_by_item_for_date("wire_rod", date_str)
            out_map = {i: wire_rod_out_map.get(i, 0.0) + sales_map.get(i, 0.0) for i in items}
        elif category_label == "Semi-Finished":
            sales_map, _, _ = get_sales_summary_for_date(date_str)
            receipts_map = sum_qty_by_item_for_date("receipt_semi_finished", date_str)
            produced_map = sum_qty_by_item_for_date("ms_wire_produced", date_str)
            scrap_map = sum_qty_by_item_for_date("scrap_produced", date_str)
            consumed_map = sum_qty_by_item_for_date("ms_wire_consumed", date_str)
            in_map = {i: receipts_map.get(i, 0.0) + produced_map.get(i, 0.0) + scrap_map.get(i, 0.0) for i in items}
            out_map = {i: consumed_map.get(i, 0.0) + sales_map.get(i, 0.0) for i in items}
        else:
            sales_map, _, _ = get_sales_summary_for_date(date_str)
            produced_fg_map = sum_qty_by_item_for_date("production", date_str)
            receipts_fg_map = sum_qty_by_item_for_date("receipt_finished_goods", date_str)
            in_map = {i: produced_fg_map.get(i, 0.0) + receipts_fg_map.get(i, 0.0) for i in items}
            out_map = {i: sales_map.get(i, 0.0) for i in items}

        rows, total = [], 0.0
        for item in items:
            if item in ledger_rows_for_date:
                r = ledger_rows_for_date[item]
                op, in_amt, out_amt, bal = r["opening"], r["in"], r["out"], r["balance"]
            else:
                op = get_live_opening(item)
                in_amt, out_amt = in_map.get(item, 0.0), out_map.get(item, 0.0)
                bal = round(op + in_amt - out_amt, 2)
            rows.append({"item": item, "opening": op, "in": in_amt, "out": out_amt, "balance": bal})
            total += bal
        return rows, round(total, 2)

    consumables_stock, consumables_total = build_category("Consumables")
    raw_material_stock, raw_material_total = build_category("Raw Material")
    semi_finished_stock, semi_finished_total = build_category("Semi-Finished")
    finished_goods_stock, finished_goods_total = build_category("Finished Goods")

    totals = {"consumables": consumables_total, "raw_material": raw_material_total,
              "semi_finished": semi_finished_total, "finished_goods": finished_goods_total}
    return consumables_stock, raw_material_stock, semi_finished_stock, finished_goods_stock, totals


CATEGORY_TAB_SUFFIX = {
    "Consumables": "Consumables",
    "Raw Material": "RawMaterial",
    "Semi-Finished": "SemiFinished",
    "Finished Goods": "FinishedGoods",
}


def resync_stock_ledger_sheet():
    """Full detail: one row per item per closed day - split into a separate
    tab per category so each stays focused and easy to scan."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT entry_date, category, item_name, opening, in_amt, out_amt, closing FROM stock_ledger ORDER BY entry_date, item_name")
    rows = cur.fetchall()
    cur.close()

    header = ["Date", "Item", "Opening", "In", "Out", "Closing"]
    by_category = {cat: [header] for cat in CATEGORY_TAB_SUFFIX}
    for entry_date, category, item_name, opening, in_amt, out_amt, closing in rows:
        if category in by_category:
            by_category[category].append([str(entry_date), item_name, float(opening), float(in_amt), float(out_amt), float(closing)])

    for category, suffix in CATEGORY_TAB_SUFFIX.items():
        tab_name = f"StockLedger-{suffix}"
        ws = get_or_create_sheet_tab(tab_name, header)
        ws.clear()
        ws.update(values=by_category[category], range_name="A1")


def resync_stock_history_sheet():
    """One row per DATE, one column per item - a compact trend view, split
    into a separate tab per category."""
    from collections import OrderedDict
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT entry_date, category, item_name, closing FROM stock_ledger ORDER BY entry_date")
    rows = cur.fetchall()
    cur.close()

    by_category_dates = {cat: OrderedDict() for cat in CATEGORY_TAB_SUFFIX}
    by_category_items = {cat: set() for cat in CATEGORY_TAB_SUFFIX}

    for entry_date, category, item_name, closing in rows:
        if category not in by_category_dates:
            continue
        d = str(entry_date)
        by_category_dates[category].setdefault(d, {})[item_name] = float(closing)
        by_category_items[category].add(item_name)

    for category, suffix in CATEGORY_TAB_SUFFIX.items():
        items = sorted(by_category_items[category])
        header = ["Date"] + items
        data = [header]
        for d, item_vals in by_category_dates[category].items():
            data.append([d] + [item_vals.get(item, "") for item in items])
        tab_name = f"StockHistory-{suffix}"
        ws = get_or_create_sheet_tab(tab_name, header)
        ws.clear()
        ws.update(values=data, range_name="A1")


def run_daily_stock_close(target_date=None, skip_resync=False):
    """Closes a specific date for every known item. Defaults to yesterday (the
    normal nightly behavior); pass target_date='YYYY-MM-DD' to backfill any
    specific missing day on demand."""
    close_date = target_date or (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
    opening_baseline_with_dates = get_opening_stock_map_with_dates()
    latest_closing = get_latest_closing_map(before_date=close_date)
    known_items = get_all_known_items()

    def get_opening_for_close(item):
        closing_info = latest_closing.get(item)
        baseline_qty, baseline_updated = opening_baseline_with_dates.get(item, (0.0, None))
        if closing_info is None:
            return baseline_qty
        closing_date, closing_value = closing_info
        if baseline_updated and baseline_updated > closing_date:
            return baseline_qty
        return closing_value

    conn = get_db_connection()
    cur = conn.cursor()

    for category_label, items in known_items.items():
        all_items = sorted(set(SEED_ITEMS.get(category_label, [])) | set(items))
        for item in all_items:
            opening = get_opening_for_close(item)
            in_amt, out_amt = compute_in_out_for_date(category_label, item, close_date)
            closing = round(opening + in_amt - out_amt, 2)
            cur.execute("""
                INSERT INTO stock_ledger (entry_date, category, item_name, opening, in_amt, out_amt, closing)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (entry_date, category, item_name) DO UPDATE
                SET opening=EXCLUDED.opening, in_amt=EXCLUDED.in_amt, out_amt=EXCLUDED.out_amt, closing=EXCLUDED.closing
            """, (close_date, category_label, item, opening, in_amt, out_amt, closing))
    conn.commit()
    cur.close()

    if not skip_resync:
        try:
            resync_stock_ledger_sheet()
            resync_stock_history_sheet()
        except Exception as e:
            print(f"  -> Sheets mirror FAILED for StockLedger/StockHistory: {e}")

    print(f"[{now_ist()}] Daily stock close completed for {close_date}")


def is_date_already_closed(date_str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM stock_ledger WHERE entry_date=%s LIMIT 1", (date_str,))
    result = cur.fetchone() is not None
    cur.close()
    return result


def recalc_ms_wire_and_scrap_for_batch(batch_id):
    """MS Wire Produced and Scrap are only calculated once, at the moment Wire
    Rod is originally submitted - not a live formula. If a Wire Rod entry in
    this submission is later edited or deleted, this recalculates the derived
    MS Wire Produced and Scrap entries to match the CURRENT Wire Rod total for
    that same submission, so they never go stale."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(quantity),0) FROM line_items WHERE batch_id=%s AND category='wire_rod'", (batch_id,))
    total_wire_rod = float(cur.fetchone()[0])

    new_ms_wire = round(total_wire_rod * MS_WIRE_CONVERSION_FACTOR, 2)
    new_scrap = round(total_wire_rod * (MS_WIRE_SCRAP_PERCENT / 100), 2)

    for category, item_name, new_qty in [("ms_wire_produced", "MS Wire", new_ms_wire), ("scrap_produced", "Scrap", new_scrap)]:
        cur.execute("SELECT id FROM line_items WHERE batch_id=%s AND category=%s", (batch_id, category))
        existing = cur.fetchone()
        if existing:
            if new_qty > 0:
                cur.execute("UPDATE line_items SET quantity=%s WHERE id=%s", (new_qty, existing[0]))
            else:
                cur.execute("DELETE FROM line_items WHERE id=%s", (existing[0],))
        elif new_qty > 0:
            cur.execute("INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                        (batch_id, category, item_name, new_qty))
    conn.commit()
    cur.close()


def cascade_reclose_from_date(from_date_str):
    """When a backdated entry (or an Opening Stock correction) lands on or before
    an already-closed day, this re-closes that day and every day after it, in
    order, so the correction ripples forward through the whole chain
    automatically - no manual re-closing needed."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT entry_date FROM stock_ledger WHERE entry_date >= %s ORDER BY entry_date", (from_date_str,))
    dates_to_reclose = [r[0].strftime("%Y-%m-%d") for r in cur.fetchall()]
    cur.close()

    if not dates_to_reclose:
        return

    print(f"[{now_ist()}] Cascading re-close from {from_date_str}: {dates_to_reclose}")
    for d in dates_to_reclose:
        run_daily_stock_close(target_date=d, skip_resync=True)

    try:
        resync_stock_ledger_sheet()
        resync_stock_history_sheet()
    except Exception as e:
        print(f"  -> Sheets mirror FAILED after cascade reclose: {e}")


def maybe_recascade_for_date(date_str):
    """Call this after ANY edit or delete that touches a date - if that date
    was already closed, the frozen StockLedger snapshot is now stale and needs
    recalculating, same as when a backdated entry is newly submitted."""
    if date_str and is_date_already_closed(date_str):
        cascade_reclose_from_date(date_str)


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
    --ink: #2C2C2C; --ink-soft: #6B6E73; --paper: #F7F7F8; --card: #FFFFFF;
    --accent: #2C2C2C; --accent-dark: #1A1A1A;
    --brand: #F26A04; --brand-dark: #C85600;
    --steel: #8B9096; --line: #E3E3E5; --ok: #2E7D32; --bad: #D64545;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: linear-gradient(180deg, #F0F0F1 0%, #F7F7F8 240px, #F7F7F8 100%);
    color: var(--ink); font-family: 'Barlow Condensed', 'Segoe UI', Arial, sans-serif;
    min-height: 100vh; display: flex; flex-direction: column; align-items: center; padding: 24px 16px 60px;
  }
  @media (max-width: 480px) {
    body { padding: 10px 6px 40px; }
  }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; max-width: 480px; width: 100%;
    padding: 28px 24px; box-shadow: 0 2px 12px rgba(44,44,44,0.06); }
  .card.wide { max-width: 1080px; }
  h1 { font-family: 'Barlow Condensed'; font-weight: 700; font-size: 24px; letter-spacing: 0.02em; text-transform: uppercase;
    color: var(--ink); margin: 0 0 4px; border-left: 5px solid var(--brand); padding-left: 12px; }
  h2.section { font-family: 'Barlow Condensed'; font-weight: 700; font-size: 16px; letter-spacing: 0.04em; text-transform: uppercase;
    color: var(--accent-dark); margin: 26px 0 4px; border-bottom: 2px solid var(--line); padding-bottom: 6px; }
  .subtitle { color: var(--ink-soft); font-size: 16px; margin: 0 0 18px; padding-left: 17px; }
  label { display: block; font-size: 14px; font-weight: 600; color: var(--ink-soft); text-transform: uppercase;
    letter-spacing: 0.03em; margin: 14px 0 5px; }
  input[type=number], input[type=text], input[type=datetime-local], input[type=password], select {
    width: 100%; padding: 12px; font-size: 18px; border: 1.5px solid var(--line); border-radius: 8px; background: var(--paper); color: var(--ink); }
  input:focus, select:focus { outline: 2px solid var(--steel); outline-offset: 1px; border-color: var(--steel); }
  .toggle-group { display: flex; gap: 10px; }
  .toggle-btn { flex: 1; padding: 14px 0; text-align: center; font-size: 17px; font-weight: 700; border: 1.5px solid var(--line);
    border-radius: 8px; background: var(--paper); cursor: pointer; user-select: none; }
  .toggle-btn.selected { background: var(--accent); border-color: var(--accent-dark); color: white; }
  button.submit, button.secondary { width: 100%; margin-top: 26px; padding: 15px 0; font-size: 17px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.04em; background: var(--brand); color: white; border: none; border-radius: 8px;
    cursor: pointer; box-shadow: 0 2px 8px rgba(242,106,4,0.25); }
  button.secondary { background: var(--ink); box-shadow: none; }
  button.add-item { width: 100%; margin-top: 10px; padding: 10px 0; font-size: 14px; font-weight: 700; text-transform: uppercase;
    background: transparent; color: var(--accent-dark); border: 1.5px dashed var(--steel); border-radius: 8px; cursor: pointer; }
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
    letter-spacing: 0.03em; border-radius: 8px; color: white; cursor: pointer; border: none; box-shadow: 0 3px 10px rgba(0,0,0,0.12); }
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
      if (!form.checkValidity()) {{
        form.reportValidity();
        return;
      }}
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

  // Prevent double/triple submissions if the operator taps Confirm & Submit
  // more than once (e.g., while waiting for a slow response).
  (function() {{
    var form = document.getElementById('{form_id}');
    form.addEventListener('submit', function() {{
      var btns = form.querySelectorAll('button[type="submit"]');
      btns.forEach(function(b) {{
        b.disabled = true;
        b.textContent = 'Submitting...';
      }});
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
    <div style="display:inline-block;background:white;border-radius:10px;padding:10px 20px;box-shadow:0 2px 8px rgba(44,44,44,0.10);border:1px solid var(--line);">
      <img src="/static/logo.png" alt="Khemji Wire" style="height:56px;display:block;">
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
        entered_pin = request.form.get("pin", "")
        if entered_pin == SHARED_PIN:
            session.permanent = True
            session["authenticated"] = True
            session["role"] = "full"
            next_url = request.args.get("next") or "/app-home"
            return redirect(next_url)
        elif entered_pin == MAINTENANCE_PIN:
            session.permanent = True
            session["authenticated"] = True
            session["role"] = "maintenance"
            return redirect("/maintenance-home")
        error = "Incorrect PIN. Please try again."
    return render_template_string(LOGIN_HTML, error=error)


MAINTENANCE_ALLOWED_PATHS = {
    "/maintenance-home", "/spares-form", "/submit-spares", "/breakdown-form", "/submit-breakdown",
    "/resolve-breakdown", "/confirm-resolve-breakdown", "/preventive-maintenance", "/add-preventive-schedule",
    "/mark-preventive-done", "/set-preventive-cost", "/confirm-preventive-cost", "/maintenance-dashboard",
}


@app.before_request
def require_pin():
    if request.path == "/login" or request.path.startswith("/static"):
        return
    if request.path == "/dashboard":
        return  # protected separately by its own admin key
    if not session.get("authenticated"):
        # Preserve the full path AND query string (e.g. ?key=...) - losing
        # this would strip the admin key on any admin-only page visited
        # before logging in, causing a confusing 403 after login.
        target = request.full_path if request.query_string else request.path
        return redirect(f"/login?next={quote(target, safe='')}")
    if session.get("role") == "maintenance" and request.path not in MAINTENANCE_ALLOWED_PATHS:
        return redirect("/maintenance-home")


# ---------- Home ----------
HOME_HTML = BASE_STYLE + """
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black">
<meta name="apple-mobile-web-app-title" content="Khemji Wire">
<meta name="viewport" content="width=device-width, initial-scale=1">
<div class="card">
  <div style="text-align:center;margin-bottom:16px;">
    <div style="display:inline-block;background:white;border-radius:10px;padding:10px 20px;box-shadow:0 2px 8px rgba(44,44,44,0.10);border:1px solid var(--line);">
      <img src="/static/logo.png" alt="Khemji Wire" style="height:56px;display:block;">
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
    <a class="home-btn" style="background:#1A1A1A;" href="/operator-dashboard"><span class="icon">&#128202;</span> View Stock Dashboard</a>
    <button class="home-btn" style="background:#5C6B78;" onclick="goTo('/spares-form')"><span class="icon">&#128295;</span> Spares Stock</button>
    <button class="home-btn" style="background:#D64545;" onclick="goTo('/breakdown-form')"><span class="icon">&#9888;</span> Report Breakdown</button>
    <button class="home-btn" style="background:#B8860B;" onclick="goTo('/preventive-maintenance')"><span class="icon">&#128197;</span> Preventive Maintenance</button>
    <a class="home-btn" style="background:#2C2C2C;" href="/maintenance-dashboard"><span class="icon">&#128736;</span> Maintenance Dashboard</a>
    <button class="home-btn" style="background:#8B5E00;" onclick="goTo('/log-expense')"><span class="icon">&#128179;</span> Log Expense</button>
    <button class="home-btn" style="background:#A0522D;" onclick="goTo('/log-maintenance-expense')"><span class="icon">&#128176;</span> Log Maintenance Expense</button>
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


MAINTENANCE_HOME_HTML = BASE_STYLE + """
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black">
<meta name="apple-mobile-web-app-title" content="Khemji Maintenance">
<meta name="viewport" content="width=device-width, initial-scale=1">
<div class="card">
  <div style="text-align:center;margin-bottom:16px;">
    <div style="display:inline-block;background:white;border-radius:10px;padding:10px 20px;box-shadow:0 2px 10px rgba(44,44,44,0.10);border:1px solid var(--line);">
      <img src="/static/logo.png" alt="Khemji Wire" style="height:56px;display:block;">
    </div>
  </div>
  <h1 style="text-align:center;border-left:none;padding-left:0;">Maintenance</h1>
  <p class="subtitle" style="text-align:center;padding-left:0;">Select your name, then choose what to log</p>
  <select id="operatorSelect" style="width:100%;padding:12px;font-size:18px;border:1.5px solid var(--line);border-radius:8px;background:var(--paper);">
    {% for name in all_names %}
    <option value="{{ name }}">{{ name }}</option>
    {% endfor %}
  </select>
  <div class="home-grid">
    <button class="home-btn" style="background:#5C6B78;" onclick="goTo('/spares-form')"><span class="icon">&#128295;</span> Spares Stock</button>
    <button class="home-btn" style="background:#D64545;" onclick="goTo('/breakdown-form')"><span class="icon">&#9888;</span> Report Breakdown</button>
    <button class="home-btn" style="background:#B8860B;" onclick="goTo('/preventive-maintenance')"><span class="icon">&#128197;</span> Preventive Maintenance</button>
    <a class="home-btn" style="background:#2C2C2C;" href="/maintenance-dashboard"><span class="icon">&#128736;</span> Maintenance Dashboard</a>
  </div>
</div>
<script>
  function goTo(path) {
    var name = document.getElementById('operatorSelect').value;
    window.location.href = path + '?operator=' + encodeURIComponent(name);
  }
</script>
"""


@app.route("/maintenance-home", methods=["GET"])
def maintenance_home():
    return render_template_string(MAINTENANCE_HOME_HTML, all_names=sorted(ALL_PEOPLE_NAMES))


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
    <input type="number" name="T1" step="any" required>
    <label>T2 (&deg;C)</label>
    <input type="number" name="T2" step="any" required>
    <label>T3 (&deg;C)</label>
    <input type="number" name="T3" step="any" required>

    <label>B1 Status</label>
    <div class="toggle-group">
      <div class="toggle-btn" data-target="B1" data-value="ON">ON</div>
      <div class="toggle-btn" data-target="B1" data-value="OFF">OFF</div>
    </div>
    <input type="hidden" name="B1" id="B1" required>
    <label>B1 Running Hours (today)</label>
    <input type="number" name="B1_HOURS" step="any" required>

    <label>B2 Status</label>
    <div class="toggle-group">
      <div class="toggle-btn" data-target="B2" data-value="ON">ON</div>
      <div class="toggle-btn" data-target="B2" data-value="OFF">OFF</div>
    </div>
    <input type="hidden" name="B2" id="B2" required>
    <label>B2 Running Hours (today)</label>
    <input type="number" name="B2_HOURS" step="any" required>

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
    <input type="number" name="cons_{{ loop.index0 }}" step="any" value="0">
    {% endfor %}
    <div id="extraConsumption"></div>
    <button class="add-item" type="button" onclick="addExtraConsumptionRow()">+ Add Another Consumable</button>

    <h2 class="section">Production (quantity)</h2>
    {% for item in production_items %}
    <label>{{ item }}</label>
    <input type="number" name="prod_{{ loop.index0 }}" step="any" value="0">
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
    row.style.flexDirection = 'column';
    row.innerHTML = '<label>Additional Consumable Name</label>' +
      '<input type="text" name="extra_cons_name[]" placeholder="Consumable name">' +
      '<label>Additional Consumable Qty</label>' +
      '<input type="number" name="extra_cons_qty[]" placeholder="Qty (kg)" step="any">';
    container.appendChild(row);
  }
  function addExtraProductionRow() {
    var container = document.getElementById('extraProduction');
    var row = document.createElement('div');
    row.className = 'extra-row';
    row.style.flexDirection = 'column';
    row.innerHTML = '<label>Additional Item Name</label>' +
      '<input type="text" name="extra_prod_name[]" placeholder="Item name">' +
      '<label>Additional Item Qty</label>' +
      '<input type="number" name="extra_prod_qty[]" placeholder="Qty" step="any">';
    container.appendChild(row);
  }
</script>
""" + confirm_flow_script("prodForm", "prodReview", "prodReviewPanel")


@app.route("/production-form", methods=["GET"])
def production_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(
        PRODUCTION_FORM_HTML, operator=operator, default_time=default_entry_time_yesterday(),
        consumption_items=get_dropdown_items("Consumables"), production_items=get_dropdown_items("Finished Goods"),
    )


DUPLICATE_BLOCKED_HTML = BASE_STYLE + """
<div class="card success">
  <div class="icon">&#9888;</div>
  <h1>Not Saved - Possible Duplicate</h1>
  <p class="subtitle">Hi {{ operator }}, this looks like the same entry you just submitted a moment ago for {{ entry_date }} - so it was NOT saved again, to avoid a duplicate.</p>
  <p style="font-size:14px;color:var(--ink-soft);">If this is genuinely a second, different entry for today (not a duplicate), please wait about a minute and submit again.</p>
  <a href="/app-home" style="display:block;text-align:center;margin-top:18px;color:var(--accent-dark);font-weight:700;text-decoration:none;">&larr; Back to Home</a>
</div>
"""


@app.route("/submit-production", methods=["POST"])
def submit_production():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)

    if is_likely_duplicate_submission("production", operator, date_str):
        return render_template_string(DUPLICATE_BLOCKED_HTML, operator=operator, entry_date=date_str)

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

    if is_date_already_closed(date_str):
        cascade_reclose_from_date(date_str)

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Electricity & Wire Rod ----------
MS_WIRE_SCALING_PERCENT = 2.0
MS_WIRE_SCRAP_PERCENT = 2.0
MS_WIRE_CONVERSION_FACTOR = round(1 - (MS_WIRE_SCALING_PERCENT + MS_WIRE_SCRAP_PERCENT) / 100, 4)  # 0.96

ZINC_BURNING_LOSS_PERCENT = 10.0
ZINC_YIELD_FACTOR = round(1 - ZINC_BURNING_LOSS_PERCENT / 100, 4)  # 0.90

ELECTRICITY_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Electricity &amp; Wire Rod</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>

  <form id="elecForm" method="POST" action="/submit-electricity">
    <input type="hidden" name="operator" value="{{ operator }}">
    <div id="elecFormFields">
      <label>Date &amp; Time</label>
      <input type="datetime-local" name="entry_time" id="elecEntryTime" value="{{ default_time }}">

      <h2 class="section">Electricity</h2>
      <label>Units Consumed</label>
      <input type="number" name="electricity_units" id="elecUnits" step="any">

      <h2 class="section">Wire Rod Issued (Kg)</h2>
      <p style="font-size:12px;color:var(--ink-soft);margin-top:-8px;">MS Wire produced is calculated automatically ({{ conversion_pct }}% of what's issued, after scaling &amp; scrap loss)</p>
      <div id="wireRodRows"></div>
      <button class="add-item" type="button" onclick="addWireRodRow()">+ Add Wire Rod Entry</button>

      <h2 class="section">MS Wire Issued for Conversion (kg)</h2>
      <p style="font-size:12px;color:var(--ink-soft);margin-top:-8px;">MS Wire sent for galvanizing into finished wire</p>
      <div id="msIssuedRows"></div>
      <button class="add-item" type="button" onclick="addMsIssuedRow()">+ Add MS Wire Entry</button>

      <button class="submit" type="button" onclick="showElecReview()">Review Entry</button>
    </div>

    <div id="elecReviewPanel" style="display:none;">
      <h2 class="section">Review Your Entry</h2>
      <div id="elecReview"></div>
      <div class="review-actions">
        <button class="secondary" type="button" onclick="goBackToElecForm()">Go Back</button>
        <button class="submit" type="submit">Confirm &amp; Submit</button>
      </div>
    </div>
  </form>
</div>
<script>
  var wireRodSizes = {{ wire_rod_sizes | tojson }};
  var msWireItems = {{ ms_wire_items | tojson }};
  var conversionFactor = {{ conversion_factor }};
  var wireRodRowCount = 0;
  var msIssuedRowCount = 0;

  function addWireRodRow() {
    wireRodRowCount++;
    var container = document.getElementById('wireRodRows');
    var row = document.createElement('div');
    row.className = 'extra-row';
    row.id = 'wrRow_' + wireRodRowCount;

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
    qtyInput.type = 'number'; qtyInput.name = 'wr_qty[]'; qtyInput.placeholder = 'Qty (Kg)'; qtyInput.step = 'any';

    select.addEventListener('change', function() { customInput.style.display = (select.value === 'Other') ? 'block' : 'none'; });

    row.appendChild(select); row.appendChild(customInput); row.appendChild(qtyInput);
    container.appendChild(row);
  }
  addWireRodRow();

  function addMsIssuedRow() {
    msIssuedRowCount++;
    var container = document.getElementById('msIssuedRows');
    var row = document.createElement('div');
    row.className = 'extra-row';
    row.id = 'msRow_' + msIssuedRowCount;

    var select = document.createElement('select');
    select.name = 'ms_item[]';
    msWireItems.forEach(function(item) {
      var opt = document.createElement('option'); opt.value = item; opt.textContent = item; select.appendChild(opt);
    });
    var otherOpt = document.createElement('option'); otherOpt.value = 'Other'; otherOpt.textContent = 'Other';
    select.appendChild(otherOpt);

    var customInput = document.createElement('input');
    customInput.type = 'text'; customInput.name = 'ms_custom_item[]'; customInput.placeholder = 'Specify item'; customInput.style.display = 'none';

    var qtyInput = document.createElement('input');
    qtyInput.type = 'number'; qtyInput.name = 'ms_qty[]'; qtyInput.placeholder = 'Qty (kg)'; qtyInput.step = 'any';

    select.addEventListener('change', function() { customInput.style.display = (select.value === 'Other') ? 'block' : 'none'; });

    row.appendChild(select); row.appendChild(customInput); row.appendChild(qtyInput);
    container.appendChild(row);
  }
  addMsIssuedRow();

  function showElecReview() {
    var entryTime = document.getElementById('elecEntryTime').value;
    var units = document.getElementById('elecUnits').value;
    if (!entryTime) { alert('Please set the Date & Time.'); return; }
    if (!units) { alert('Please enter Units Consumed.'); return; }

    var rows = '';
    rows += '<div class="review-row"><span>Date &amp; Time</span><b>' + entryTime + '</b></div>';
    rows += '<div class="review-row"><span>Units Consumed</span><b>' + units + '</b></div>';

    var sizes = document.getElementsByName('wr_size[]');
    var customs = document.getElementsByName('wr_custom_size[]');
    var qtys = document.getElementsByName('wr_qty[]');
    var totalMsWire = 0;
    for (var i = 0; i < sizes.length; i++) {
      var qty = qtys[i].value;
      if (!qty) continue;
      var sizeLabel = (sizes[i].value === 'Other' && customs[i].value) ? customs[i].value : sizes[i].value;
      rows += '<div class="review-row"><span>Wire Rod - ' + sizeLabel + '</span><b>' + qty + ' Kg</b></div>';
      totalMsWire += parseFloat(qty) * 1000 * conversionFactor;
    }
    if (totalMsWire > 0) {
      rows += '<div class="review-row"><span>MS Wire Produced (auto, in Kg)</span><b>' + totalMsWire.toFixed(2) + ' kg</b></div>';
    }

    var msItems = document.getElementsByName('ms_item[]');
    var msCustoms = document.getElementsByName('ms_custom_item[]');
    var msQtys = document.getElementsByName('ms_qty[]');
    for (var j = 0; j < msItems.length; j++) {
      var msQty = msQtys[j].value;
      if (!msQty) continue;
      var msLabel = (msItems[j].value === 'Other' && msCustoms[j].value) ? msCustoms[j].value : msItems[j].value;
      rows += '<div class="review-row"><span>MS Wire Issued - ' + msLabel + '</span><b>' + msQty + ' kg</b></div>';
    }

    document.getElementById('elecReview').innerHTML = rows;
    document.getElementById('elecFormFields').style.display = 'none';
    document.getElementById('elecReviewPanel').style.display = 'block';
  }

  function goBackToElecForm() {
    document.getElementById('elecFormFields').style.display = 'block';
    document.getElementById('elecReviewPanel').style.display = 'none';
  }

  document.getElementById('elecForm').addEventListener('submit', function() {
    var btns = document.querySelectorAll('#elecReviewPanel button[type="submit"]');
    btns.forEach(function(b) { b.disabled = true; b.textContent = 'Submitting...'; });
  });
</script>
"""


@app.route("/electricity-form", methods=["GET"])
def electricity_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(ELECTRICITY_FORM_HTML, operator=operator, default_time=default_entry_time_yesterday(),
                                   wire_rod_sizes=get_dropdown_items("Raw Material"),
                                   ms_wire_items=get_dropdown_items("Semi-Finished"),
                                   conversion_factor=MS_WIRE_CONVERSION_FACTOR,
                                   conversion_pct=round(MS_WIRE_CONVERSION_FACTOR * 100, 1))


@app.route("/submit-electricity", methods=["POST"])
def submit_electricity():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    units = request.form.get("electricity_units", "")

    wr_sizes = request.form.getlist("wr_size[]")
    wr_customs = request.form.getlist("wr_custom_size[]")
    wr_qtys = request.form.getlist("wr_qty[]")

    ms_items = request.form.getlist("ms_item[]")
    ms_customs = request.form.getlist("ms_custom_item[]")
    ms_qtys = request.form.getlist("ms_qty[]")

    batch_id = str(uuid.uuid4())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator, electricity_units) VALUES (%s,%s,%s,%s,%s,%s)",
        (batch_id, "electricity_wire_rod", date_str, time_str, operator, safe_float(units)),
    )

    mirror_row = {"Date": date_str, "Time": time_str, "Operator": operator, "Electricity Units": units}
    total_ms_wire_produced = 0.0
    total_scrap_produced = 0.0
    for size, custom, qty in zip(wr_sizes, wr_customs, wr_qtys):
        if not qty:
            continue
        final_size = custom.strip() if size == "Other" and custom.strip() else size
        q_kg = safe_float(qty)  # Wire Rod is entered directly in Kg now, same as everything else
        if q_kg != 0:
            cur.execute(
                "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                (batch_id, "wire_rod", final_size, q_kg),
            )
            total_ms_wire_produced += q_kg * MS_WIRE_CONVERSION_FACTOR
            total_scrap_produced += q_kg * (MS_WIRE_SCRAP_PERCENT / 100)
            # Scale (also MS_WIRE_SCALING_PERCENT of Wire Rod) is a pure process
            # loss - not saleable, so it's never stored as inventory. It's
            # calculated on the fly wherever needed (Yield Report, P&L) directly
            # from Wire Rod Issued, since it's just Wire Rod x scaling%.
        mirror_row[final_size] = q_kg

    # Auto-calculate MS Wire produced from wire rod issued, accounting for
    # scaling + scrap loss - no manual entry needed, always consistent with
    # the real conversion rate.
    if total_ms_wire_produced > 0:
        cur.execute(
            "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
            (batch_id, "ms_wire_produced", "MS Wire", round(total_ms_wire_produced, 2)),
        )

    # Scrap - real, saleable Semi-Finished stock, auto-generated the same way.
    if total_scrap_produced > 0:
        cur.execute(
            "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
            (batch_id, "scrap_produced", "Scrap", round(total_scrap_produced, 2)),
        )

    # MS Wire issued for conversion (galvanizing) - input to Stage 2, logged
    # here alongside Wire Rod Issued since both represent material moving OUT
    # of a stage in the same wire-processing workflow.
    for item, custom, qty in zip(ms_items, ms_customs, ms_qtys):
        if not qty:
            continue
        final_item = custom.strip() if item == "Other" and custom.strip() else item
        q = safe_float(qty)
        if q != 0:
            cur.execute(
                "INSERT INTO line_items (batch_id, category, item_name, quantity) VALUES (%s,%s,%s,%s)",
                (batch_id, "ms_wire_consumed", final_item, q),
            )

    conn.commit()
    cur.close()

    mirror_append_named_row("ElectricityWireRod", ["Date", "Time", "Operator", "Electricity Units"] + get_dropdown_items("Raw Material"), mirror_row)

    if is_date_already_closed(date_str):
        cascade_reclose_from_date(date_str)

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
    <input type="number" name="quantity" step="any" required>

    <label>Rate (Rs./Kg)</label>
    <input type="number" name="rate" step="any">

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
    categories = {"Consumables": get_dropdown_items("Consumables"), "Raw Material": get_dropdown_items("Raw Material"),
                  "Semi-Finished": get_dropdown_items("Semi-Finished"), "Finished Goods": get_dropdown_items("Finished Goods")}
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
    rate_raw = request.form.get("rate", "")
    rate = safe_float(rate_raw) if rate_raw else None
    total_value = round(quantity * rate, 2) if rate else None

    category_db_map = {"Consumables": "receipt_consumables", "Raw Material": "receipt_raw_material",
                       "Semi-Finished": "receipt_semi_finished", "Finished Goods": "receipt_finished_goods"}
    category_db = category_db_map.get(category, "receipt_consumables")

    batch_id = str(uuid.uuid4())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator) VALUES (%s,%s,%s,%s,%s)",
        (batch_id, "receipt", date_str, time_str, operator),
    )
    cur.execute(
        "INSERT INTO line_items (batch_id, category, item_name, quantity, price, total_amount) VALUES (%s,%s,%s,%s,%s,%s)",
        (batch_id, category_db, final_item, quantity, rate, total_value),
    )
    conn.commit()
    cur.close()

    mirror_append_named_row(
        "Receipts", ["Date", "Time", "Operator", "Category", "Item", "Quantity", "Rate (Rs/Kg)", "Value (Rs)"],
        {"Date": date_str, "Time": time_str, "Operator": operator, "Category": category, "Item": final_item,
         "Quantity": quantity, "Rate (Rs/Kg)": rate_raw, "Value (Rs)": total_value or ""},
    )

    if is_date_already_closed(date_str):
        cascade_reclose_from_date(date_str)

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Maintenance: Spares Stock (Receipt / Issued combined) ----------
SPARES_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Spares Stock</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>

  <form id="sparesForm" method="POST" action="/submit-spares">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <label>Transaction Type</label>
    <div class="toggle-group">
      <div class="toggle-btn selected" data-target="txnType" data-value="receipt">Received</div>
      <div class="toggle-btn" data-target="txnType" data-value="issued">Issued</div>
    </div>
    <input type="hidden" name="transaction_type" id="txnType" value="receipt">

    <label>Item</label>
    <select id="itemSelect" name="item" onchange="toggleCustomItem()">
      {% for item in spares_items %}<option value="{{ item }}">{{ item }}</option>{% endfor %}
      <option value="Other">Other</option>
    </select>
    <input type="text" id="customItem" name="custom_item" placeholder="Specify item" style="display:none;margin-top:8px;">

    <label>Quantity</label>
    <input type="number" name="quantity" step="any" required>

    <div id="rateField">
      <label>Rate (Rs.)</label>
      <input type="number" name="rate" step="any">
    </div>

    <div id="machineField" style="display:none;">
      <label>Machine</label>
      <select id="machineSelect" name="machine" onchange="toggleCustomMachine()">
        {% for m in machines %}<option value="{{ m }}">{{ m }}</option>{% endfor %}
        <option value="Other">Other</option>
      </select>
      <input type="text" id="customMachine" name="custom_machine" placeholder="Specify machine" style="display:none;margin-top:8px;">
    </div>

    <button class="submit review-btn" type="button">Review Entry</button>
  </form>

  <div id="sparesReviewPanel" style="display:none;">
    <h2 class="section">Review Your Entry</h2>
    <div id="sparesReview"></div>
    <div class="review-actions">
      <button class="secondary" type="button" onclick="goBackToForm_sparesForm()">Go Back</button>
      <button class="submit" type="submit" form="sparesForm">Confirm &amp; Submit</button>
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
      var isIssued = btn.dataset.value === 'issued';
      document.getElementById('machineField').style.display = isIssued ? 'block' : 'none';
      document.getElementById('rateField').style.display = isIssued ? 'none' : 'block';
    });
  });
  function toggleCustomItem() {
    document.getElementById('customItem').style.display = (document.getElementById('itemSelect').value === 'Other') ? 'block' : 'none';
  }
  function toggleCustomMachine() {
    document.getElementById('customMachine').style.display = (document.getElementById('machineSelect').value === 'Other') ? 'block' : 'none';
  }
</script>
""" + confirm_flow_script("sparesForm", "sparesReview", "sparesReviewPanel")


@app.route("/spares-form", methods=["GET"])
def spares_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(SPARES_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   spares_items=get_known_spares(), machines=get_known_machines())


@app.route("/submit-spares", methods=["POST"])
def submit_spares():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    txn_type = request.form.get("transaction_type", "receipt")
    item = request.form.get("item", "")
    custom_item = request.form.get("custom_item", "").strip()
    final_item = custom_item if item == "Other" and custom_item else item
    quantity = safe_float(request.form.get("quantity", ""))
    rate_raw = request.form.get("rate", "")
    rate = safe_float(rate_raw) if rate_raw else None
    total_amount = round(quantity * rate, 2) if rate else None

    machine = None
    if txn_type == "issued":
        m = request.form.get("machine", "")
        custom_machine = request.form.get("custom_machine", "").strip()
        machine = custom_machine if m == "Other" and custom_machine else m
        # No rate is entered when issuing - auto-calculate cost from the
        # item's most recent known rate (set when it was last received).
        # If no rate is on file, this stays blank - admin can fill it in later.
        auto_rate = get_spare_rate(final_item)
        if auto_rate:
            rate = auto_rate
            total_amount = round(quantity * rate, 2)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO spare_transactions (entry_date, entry_time, operator, transaction_type, item_name, quantity, rate, total_amount, machine)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (date_str, time_str, operator, txn_type, final_item, quantity, rate, total_amount, machine))
    conn.commit()
    cur.close()

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Maintenance: Breakdown Report ----------
BREAKDOWN_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Report Breakdown</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>

  <form id="breakdownForm" method="POST" action="/submit-breakdown">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Machine</label>
    <select id="machineSelect" name="machine" onchange="toggleCustomMachine()">
      {% for m in machines %}<option value="{{ m }}">{{ m }}</option>{% endfor %}
      <option value="Other">Other</option>
    </select>
    <input type="text" id="customMachine" name="custom_machine" placeholder="Specify machine" style="display:none;margin-top:8px;">

    <label>Issue Description</label>
    <input type="text" name="issue_description" required>

    <label>Reported Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <button class="submit review-btn" type="button">Review Entry</button>
  </form>

  <div id="breakdownReviewPanel" style="display:none;">
    <h2 class="section">Review Your Entry</h2>
    <div id="breakdownReview"></div>
    <div class="review-actions">
      <button class="secondary" type="button" onclick="goBackToForm_breakdownForm()">Go Back</button>
      <button class="submit" type="submit" form="breakdownForm">Confirm &amp; Submit</button>
    </div>
  </div>
</div>
<script>
  function toggleCustomMachine() {
    document.getElementById('customMachine').style.display = (document.getElementById('machineSelect').value === 'Other') ? 'block' : 'none';
  }
</script>
""" + confirm_flow_script("breakdownForm", "breakdownReview", "breakdownReviewPanel")


@app.route("/breakdown-form", methods=["GET"])
def breakdown_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(BREAKDOWN_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   machines=get_known_machines())


@app.route("/submit-breakdown", methods=["POST"])
def submit_breakdown():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    m = request.form.get("machine", "")
    custom_machine = request.form.get("custom_machine", "").strip()
    machine = custom_machine if m == "Other" and custom_machine else m
    issue_description = request.form.get("issue_description", "")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO breakdown_maintenance (machine, issue_description, reported_date, reported_time, operator)
        VALUES (%s,%s,%s,%s,%s)
    """, (machine, issue_description, date_str, time_str, operator))
    conn.commit()
    cur.close()

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


RESOLVE_BREAKDOWN_HTML = BASE_STYLE + """
<div class="card">
  <h1>Resolve Breakdown</h1>
  <p class="subtitle">{{ breakdown.machine }} &mdash; {{ breakdown.issue_description }}</p>
  <form method="POST" action="/confirm-resolve-breakdown">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <input type="hidden" name="breakdown_id" value="{{ breakdown.id }}">
    <label>Resolved Date &amp; Time</label>
    <input type="datetime-local" name="resolved_time" value="{{ default_time }}" required>
    <label>Repair Cost (Rs.)</label>
    <input type="number" step="any" name="repair_cost">
    <label>Notes</label>
    <input type="text" name="notes">
    <button class="submit" type="submit">Mark Resolved</button>
  </form>
</div>
"""


@app.route("/resolve-breakdown", methods=["GET"])
def resolve_breakdown_form():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    breakdown_id = request.args.get("id", "")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, machine, issue_description FROM breakdown_maintenance WHERE id=%s", (breakdown_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        abort(404)
    breakdown = {"id": row[0], "machine": row[1], "issue_description": row[2]}
    return render_template_string(RESOLVE_BREAKDOWN_HTML, admin_key=ADMIN_KEY, breakdown=breakdown, default_time=default_entry_time())


@app.route("/confirm-resolve-breakdown", methods=["POST"])
def confirm_resolve_breakdown():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    breakdown_id = request.form.get("breakdown_id", "")
    raw_resolved = request.form.get("resolved_time", "").strip()
    if raw_resolved:
        try:
            dt = datetime.strptime(raw_resolved, "%Y-%m-%dT%H:%M")
            resolved_date, resolved_time = dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
        except ValueError:
            n = now_ist()
            resolved_date, resolved_time = n.strftime("%Y-%m-%d"), n.strftime("%H:%M:%S")
    else:
        n = now_ist()
        resolved_date, resolved_time = n.strftime("%Y-%m-%d"), n.strftime("%H:%M:%S")
    repair_cost = request.form.get("repair_cost", "")
    notes = request.form.get("notes", "")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE breakdown_maintenance SET resolved_date=%s, resolved_time=%s, repair_cost=%s, notes=%s WHERE id=%s
    """, (resolved_date, resolved_time, safe_float(repair_cost) if repair_cost else None, notes, breakdown_id))
    conn.commit()
    cur.close()

    return render_template_string(SUCCESS_HTML, operator="Admin", alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/maintenance-dashboard?key={ADMIN_KEY}";}}, 1200);</script>'


# ---------- Maintenance: Preventive Maintenance ----------
def get_preventive_schedule():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, machine, task, frequency_days, last_done_date, next_due_date, last_cost FROM preventive_maintenance_schedule ORDER BY next_due_date")
    rows = []
    today = now_ist().date()
    for r in cur.fetchall():
        next_due = r[5]
        status = "overdue" if next_due and next_due < today else ("due_soon" if next_due and (next_due - today).days <= 7 else "ok")
        rows.append({"id": r[0], "machine": r[1], "task": r[2], "frequency_days": r[3],
                     "last_done_date": r[4], "next_due_date": next_due, "status": status,
                     "last_cost": float(r[6]) if r[6] is not None else None})
    cur.close()
    return rows


PREVENTIVE_LIST_HTML = BASE_STYLE + """
<div class="card wide">
  <h1>Preventive Maintenance</h1>
  <p class="subtitle">Scheduled servicing - mark as done when completed</p>

  {% if show_edit %}
  <h2 class="section">Add New Scheduled Task</h2>
  <form method="POST" action="/add-preventive-schedule">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <label>Machine</label>
    <select name="machine">
      {% for m in machines %}<option value="{{ m }}">{{ m }}</option>{% endfor %}
    </select>
    <label>Task</label>
    <input type="text" name="task" required placeholder="e.g. Grease bearings">
    <label>Frequency (days)</label>
    <input type="number" name="frequency_days" required min="1">
    <button class="submit" type="submit">Add Schedule</button>
  </form>
  {% endif %}

  <h2 class="section">Schedule</h2>
  <div class="table-wrap"><table>
    <tr><th>Machine</th><th>Task</th><th>Frequency</th><th>Last Done</th><th>Next Due</th><th>Status</th>{% if show_edit %}<th>Cost</th>{% endif %}<th>Action</th></tr>
    {% for s in schedule %}
    <tr>
      <td>{{ s.machine }}</td><td>{{ s.task }}</td><td>Every {{ s.frequency_days }} days</td>
      <td>{{ s.last_done_date or '-' }}</td><td>{{ s.next_due_date or '-' }}</td>
      <td class="{{ 'badge-bad' if s.status == 'overdue' else '' }}">{{ 'OVERDUE' if s.status == 'overdue' else ('Due Soon' if s.status == 'due_soon' else 'OK') }}</td>
      {% if show_edit %}<td>{{ 'Rs. ' + s.last_cost|string if s.last_cost else '-' }}</td>{% endif %}
      <td><a href="/mark-preventive-done?id={{ s.id }}&operator={{ operator }}">Mark Done</a>{% if show_edit %} <a href="/set-preventive-cost?id={{ s.id }}&key={{ admin_key }}">Set Cost</a>{% endif %}</td>
    </tr>
    {% endfor %}
  </table></div>
</div>
"""


@app.route("/preventive-maintenance", methods=["GET"])
def preventive_maintenance_list():
    operator = request.args.get("operator", "Operator")
    admin_key_param = request.args.get("key", "")
    show_edit = (admin_key_param == ADMIN_KEY)
    return render_template_string(PREVENTIVE_LIST_HTML, schedule=get_preventive_schedule(), machines=get_known_machines(),
                                   show_edit=show_edit, admin_key=ADMIN_KEY, operator=operator)


@app.route("/add-preventive-schedule", methods=["POST"])
def add_preventive_schedule():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    machine = request.form.get("machine", "")
    task = request.form.get("task", "")
    frequency_days = int(safe_float(request.form.get("frequency_days", "30")))

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO preventive_maintenance_schedule (machine, task, frequency_days, next_due_date)
        VALUES (%s,%s,%s, CURRENT_DATE + %s)
    """, (machine, task, frequency_days, frequency_days))
    conn.commit()
    cur.close()

    return render_template_string(SUCCESS_HTML, operator="Admin", alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/preventive-maintenance?key={ADMIN_KEY}";}}, 1200);</script>'


@app.route("/mark-preventive-done", methods=["GET"])
def mark_preventive_done():
    schedule_id = request.args.get("id", "")
    operator = request.args.get("operator", "Operator")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT frequency_days FROM preventive_maintenance_schedule WHERE id=%s", (schedule_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        abort(404)
    frequency_days = row[0]
    cur.execute("""
        UPDATE preventive_maintenance_schedule
        SET last_done_date=CURRENT_DATE, next_due_date=CURRENT_DATE + %s, last_cost=NULL
        WHERE id=%s
    """, (frequency_days, schedule_id))
    conn.commit()
    cur.close()

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/preventive-maintenance?operator={operator}";}}, 1200);</script>'


SET_PREVENTIVE_COST_HTML = BASE_STYLE + """
<div class="card">
  <h1>Set Maintenance Cost</h1>
  <p class="subtitle">{{ machine }} &mdash; {{ task }}</p>
  <form method="POST" action="/confirm-preventive-cost">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <input type="hidden" name="schedule_id" value="{{ schedule_id }}">
    <label>Cost (Rs.)</label>
    <input type="number" step="any" name="cost" value="{{ current_cost }}">
    <button class="submit" type="submit">Save Cost</button>
  </form>
</div>
"""


@app.route("/set-preventive-cost", methods=["GET"])
def set_preventive_cost_form():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    schedule_id = request.args.get("id", "")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT machine, task, last_cost FROM preventive_maintenance_schedule WHERE id=%s", (schedule_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        abort(404)
    return render_template_string(SET_PREVENTIVE_COST_HTML, admin_key=ADMIN_KEY, schedule_id=schedule_id,
                                   machine=row[0], task=row[1], current_cost=row[2] if row[2] is not None else "")


@app.route("/confirm-preventive-cost", methods=["POST"])
def confirm_preventive_cost():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    schedule_id = request.form.get("schedule_id", "")
    cost = request.form.get("cost", "")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE preventive_maintenance_schedule SET last_cost=%s WHERE id=%s",
                (safe_float(cost) if cost else None, schedule_id))
    conn.commit()
    cur.close()

    return render_template_string(SUCCESS_HTML, operator="Admin", alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/maintenance-dashboard?key={ADMIN_KEY}";}}, 1200);</script>'


MAINTENANCE_DASHBOARD_HTML = BASE_STYLE + """
<meta name="viewport" content="width=device-width, initial-scale=1">
<div class="nav-top"><a href="/app-home">&larr; Back to entry forms</a></div>
<div class="card wide">
  <h1>Maintenance Dashboard</h1>
  <p class="subtitle">Spares stock, preventive schedule, and breakdown history</p>

  {% if show_edit %}
  <a href="/log-maintenance-expense?key={{ admin_key }}" style="display:inline-block;margin-bottom:16px;padding:10px 20px;background:var(--brand);color:white;border-radius:8px;text-decoration:none;font-weight:700;text-transform:uppercase;font-size:13px;">Log Maintenance Expense</a>
  {% endif %}

  {% if overdue_count > 0 %}
  <div class="alert-panel">
    <div class="alert-title">&#9888; {{ overdue_count }} Preventive Task(s) Overdue</div>
    {% for s in schedule if s.status == 'overdue' %}
    <div class="alert-item">{{ s.machine }} &mdash; {{ s.task }} (was due {{ s.next_due_date }})</div>
    {% endfor %}
  </div>
  {% else %}
  <div class="alert-panel all-clear"><div class="alert-title">&#9989; No Overdue Preventive Maintenance</div></div>
  {% endif %}

  {% if unresolved_count > 0 %}
  <div class="alert-panel">
    <div class="alert-title">&#128295; {{ unresolved_count }} Unresolved Breakdown(s)</div>
    {% for b in breakdowns if not b.resolved_date %}
    <div class="alert-item">{{ b.machine }} &mdash; {{ b.issue_description }} (reported {{ b.reported_date }})
      {% if show_edit %}<a href="/resolve-breakdown?id={{ b.id }}&key={{ admin_key }}" style="margin-left:8px;">Resolve</a>{% endif %}
    </div>
    {% endfor %}
  </div>
  {% endif %}

  <h2 class="section">Spares Stock</h2>
  <div class="table-wrap"><table>
    <tr><th>Item</th><th>Opening</th><th>Received</th><th>Issued</th><th>Balance</th></tr>
    {% for row in spares_stock %}
    <tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.received }}</td><td>{{ row.issued }}</td>
    <td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>
    {% endfor %}
  </table></div>

  <h2 class="section">Preventive Maintenance Schedule</h2>
  <div class="table-wrap"><table>
    <tr><th>Machine</th><th>Task</th><th>Next Due</th><th>Status</th>{% if show_edit %}<th>Cost</th>{% endif %}</tr>
    {% for s in schedule %}
    <tr><td>{{ s.machine }}</td><td>{{ s.task }}</td><td>{{ s.next_due_date or '-' }}</td>
    <td class="{{ 'badge-bad' if s.status == 'overdue' else '' }}">{{ 'OVERDUE' if s.status == 'overdue' else ('Due Soon' if s.status == 'due_soon' else 'OK') }}</td>
    {% if show_edit %}<td>{{ 'Rs. ' + s.last_cost|string if s.last_cost else '-' }}</td>{% endif %}</tr>
    {% endfor %}
  </table></div>

  <h2 class="section">Breakdown History</h2>
  <div class="table-wrap"><table>
    <tr><th>Machine</th><th>Issue</th><th>Reported</th><th>Resolved</th>{% if show_edit %}<th>Repair Cost</th>{% endif %}</tr>
    {% for b in breakdowns %}
    <tr><td>{{ b.machine }}</td><td>{{ b.issue_description }}</td><td>{{ b.reported_date }} {{ b.reported_time }}</td>
    <td>{{ b.resolved_date or 'Not resolved' }}</td>
    {% if show_edit %}<td>{{ 'Rs. ' + b.repair_cost|string if b.repair_cost else '-' }}</td>{% endif %}</tr>
    {% endfor %}
  </table></div>
</div>
"""


MAINTENANCE_EXPENSE_HTML = BASE_STYLE + """
<div class="card wide">
  <div class="nav-top" style="justify-content:flex-start;margin-bottom:10px;"><a href="/maintenance-dashboard">&larr; Back to Maintenance Dashboard</a></div>
  <h1>Log Maintenance Expense</h1>
  <p class="subtitle">Outside repair work and other maintenance costs not tied to Spares or a specific breakdown - counts toward P&amp;L</p>

  <form id="maintExpenseForm" method="POST" action="/submit-maintenance-expense">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date</label>
    <input type="date" name="entry_date" value="{{ today }}" required>
    <label>Machine (optional)</label>
    <select name="machine">
      <option value="">-- Not machine-specific --</option>
      {% for m in machines %}<option value="{{ m }}">{{ m }}</option>{% endfor %}
    </select>
    <label>Description</label>
    <input type="text" name="description" required placeholder="e.g. AMC payment, outside contractor visit">
    <label>Amount (Rs.)</label>
    <input type="number" step="any" name="amount" required>
    <button class="submit review-btn" type="button">Review Entry</button>
  </form>

  <div id="maintExpenseReviewPanel" style="display:none;">
    <h2 class="section">Review Your Entry</h2>
    <div id="maintExpenseReview"></div>
    <div class="review-actions">
      <button class="secondary" type="button" onclick="goBackToForm_maintExpenseForm()">Go Back</button>
      <button class="submit" type="submit" form="maintExpenseForm">Confirm &amp; Submit</button>
    </div>
  </div>

  <h2 class="section">Maintenance Expenses</h2>
  <form method="GET" style="display:flex;gap:10px;align-items:center;margin-bottom:10px;">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label style="margin:0;">Viewing Month</label>
    <input type="month" name="month" value="{{ view_month }}" onchange="this.form.submit()">
  </form>
  <div class="table-wrap"><table>
    <tr><th>Date</th><th>Machine</th><th>Description</th><th>Amount</th><th>Action</th></tr>
    {% for e in expenses %}
    <tr><td>{{ e.entry_date }}</td><td>{{ e.machine or '-' }}</td><td>{{ e.description }}</td><td>Rs. {{ e.amount }}</td>
    <td><a href="/edit-maintenance-expense?id={{ e.id }}&operator={{ operator }}&month={{ view_month }}">Edit</a> | <a href="/delete-maintenance-expense?id={{ e.id }}&operator={{ operator }}&month={{ view_month }}" style="color:var(--bad);" onclick="return confirm('Delete this expense?');">Delete</a></td></tr>
    {% endfor %}
    <tr style="font-weight:700;"><td colspan="3">Total</td><td>Rs. {{ month_total }}</td><td></td></tr>
  </table></div>
</div>
""" + confirm_flow_script("maintExpenseForm", "maintExpenseReview", "maintExpenseReviewPanel")


@app.route("/log-maintenance-expense", methods=["GET"])
def log_maintenance_expense_form():
    operator = request.args.get("operator", "Operator")
    month_str = request.args.get("month") or now_ist().strftime("%Y-%m")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, entry_date, machine, description, amount FROM maintenance_expenses
        WHERE TO_CHAR(entry_date,'YYYY-MM')=%s ORDER BY entry_date DESC
    """, (month_str,))
    expenses = [{"id": r[0], "entry_date": r[1], "machine": r[2], "description": r[3], "amount": float(r[4])} for r in cur.fetchall()]
    cur.close()
    month_total = round(sum(e["amount"] for e in expenses), 2)

    return render_template_string(MAINTENANCE_EXPENSE_HTML, admin_key=ADMIN_KEY, operator=operator, today=now_ist().strftime("%Y-%m-%d"),
                                   machines=get_known_machines(), expenses=expenses, month_total=month_total, view_month=month_str)


@app.route("/delete-maintenance-expense", methods=["GET"])
def delete_maintenance_expense():
    expense_id = request.args.get("id", "")
    operator = request.args.get("operator", "Operator")
    month = request.args.get("month", "")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM maintenance_expenses WHERE id=%s", (expense_id,))
    conn.commit()
    cur.close()
    return redirect(f"/log-maintenance-expense?operator={operator}&month={month}")


EDIT_MAINTENANCE_EXPENSE_HTML = BASE_STYLE + """
<div class="card">
  <h1>Edit Maintenance Expense</h1>
  <form method="POST" action="/save-maintenance-expense">
    <input type="hidden" name="id" value="{{ e.id }}">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date</label>
    <input type="date" name="entry_date" value="{{ e.entry_date }}" required>
    <label>Machine (optional)</label>
    <select name="machine">
      <option value="">-- Not machine-specific --</option>
      {% for m in machines %}<option value="{{ m }}" {{ 'selected' if m == e.machine else '' }}>{{ m }}</option>{% endfor %}
    </select>
    <label>Description</label>
    <input type="text" name="description" value="{{ e.description }}" required>
    <label>Amount (Rs.)</label>
    <input type="number" step="any" name="amount" value="{{ e.amount }}" required>
    <button class="submit" type="submit">Save Changes</button>
  </form>
</div>
"""


@app.route("/edit-maintenance-expense", methods=["GET"])
def edit_maintenance_expense():
    expense_id = request.args.get("id", "")
    operator = request.args.get("operator", "Operator")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, entry_date, machine, description, amount FROM maintenance_expenses WHERE id=%s", (expense_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        abort(404)
    e = {"id": row[0], "entry_date": row[1], "machine": row[2], "description": row[3], "amount": row[4]}
    return render_template_string(EDIT_MAINTENANCE_EXPENSE_HTML, e=e, operator=operator, machines=get_known_machines())


@app.route("/save-maintenance-expense", methods=["POST"])
def save_maintenance_expense():
    expense_id = request.form.get("id", "")
    operator = request.form.get("operator", "Operator")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE maintenance_expenses SET entry_date=%s, machine=%s, description=%s, amount=%s WHERE id=%s
    """, (request.form.get("entry_date", ""), request.form.get("machine", "") or None,
          request.form.get("description", ""), safe_float(request.form.get("amount", "0")), expense_id))
    conn.commit()
    cur.close()
    new_month = request.form.get("entry_date", "")[:7]
    return redirect(f"/log-maintenance-expense?operator={operator}&month={new_month}")


@app.route("/submit-maintenance-expense", methods=["POST"])
def submit_maintenance_expense():
    operator = request.form.get("operator", "Operator")
    entry_date = request.form.get("entry_date", "")
    machine = request.form.get("machine", "") or None
    description = request.form.get("description", "")
    amount = safe_float(request.form.get("amount", "0"))

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO maintenance_expenses (entry_date, machine, description, amount, operator)
        VALUES (%s,%s,%s,%s,%s)
    """, (entry_date, machine, description, amount, operator))
    conn.commit()
    cur.close()

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/log-maintenance-expense?operator={operator}";}}, 1200);</script>'


@app.route("/maintenance-dashboard", methods=["GET"])
def maintenance_dashboard():
    admin_key_param = request.args.get("key", "")
    show_edit = (admin_key_param == ADMIN_KEY)

    schedule = get_preventive_schedule()
    overdue_count = sum(1 for s in schedule if s["status"] == "overdue")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, machine, issue_description, reported_date, reported_time, resolved_date, resolved_time, repair_cost, notes
        FROM breakdown_maintenance ORDER BY reported_date DESC, reported_time DESC LIMIT 50
    """)
    breakdowns = [{"id": r[0], "machine": r[1], "issue_description": r[2], "reported_date": r[3], "reported_time": r[4],
                   "resolved_date": r[5], "resolved_time": r[6], "repair_cost": float(r[7]) if r[7] is not None else None,
                   "notes": r[8]} for r in cur.fetchall()]
    cur.close()
    unresolved_count = sum(1 for b in breakdowns if not b["resolved_date"])

    return render_template_string(
        MAINTENANCE_DASHBOARD_HTML, schedule=schedule, overdue_count=overdue_count,
        breakdowns=breakdowns, unresolved_count=unresolved_count,
        spares_stock=compute_spares_stock(), show_edit=show_edit, admin_key=ADMIN_KEY,
    )


# ---------- Stock: Sales ----------
SALES_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Log a Sale</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>

  <form id="saleForm" method="POST" action="/submit-sale">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <label>Customer (optional)</label>
    <input type="text" name="customer">

    <h2 class="section">Items Sold</h2>
    <div id="saleItemRows"></div>
    <button class="add-item" type="button" onclick="addSaleItemRow()">+ Add Another Item</button>

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
  var financeItems = {{ finished_goods_items | tojson }};
  function addSaleItemRow() {
    var container = document.getElementById('saleItemRows');
    var row = document.createElement('div');
    row.style.border = '1px solid var(--line)';
    row.style.borderRadius = '8px';
    row.style.padding = '12px';
    row.style.marginBottom = '10px';

    var select = document.createElement('select');
    select.name = 'item[]';
    financeItems.forEach(function(it) {
      var opt = document.createElement('option'); opt.value = it; opt.textContent = it; select.appendChild(opt);
    });
    var otherOpt = document.createElement('option'); otherOpt.value = 'Other'; otherOpt.textContent = 'Other';
    select.appendChild(otherOpt);

    var customInput = document.createElement('input');
    customInput.type = 'text'; customInput.name = 'custom_item[]'; customInput.placeholder = 'Specify item';
    customInput.style.display = 'none'; customInput.style.marginTop = '8px';
    select.addEventListener('change', function() { customInput.style.display = (select.value === 'Other') ? 'block' : 'none'; });

    var qtyLabel = document.createElement('label'); qtyLabel.textContent = 'Quantity Sold';
    var qtyInput = document.createElement('input');
    qtyInput.type = 'number'; qtyInput.name = 'quantity[]'; qtyInput.step = 'any';

    var priceLabel = document.createElement('label'); priceLabel.textContent = 'Sale Price (Rs./Kg)';
    var priceInput = document.createElement('input');
    priceInput.type = 'number'; priceInput.name = 'price[]'; priceInput.step = 'any';

    var removeBtn = document.createElement('button');
    removeBtn.type = 'button'; removeBtn.textContent = 'Remove This Item';
    removeBtn.style.cssText = 'margin-top:10px;background:transparent;border:none;color:var(--bad);font-size:13px;cursor:pointer;padding:0;';
    removeBtn.onclick = function() { row.remove(); };

    var itemLabel = document.createElement('label'); itemLabel.textContent = 'Item';
    row.appendChild(itemLabel); row.appendChild(select); row.appendChild(customInput);
    row.appendChild(qtyLabel); row.appendChild(qtyInput);
    row.appendChild(priceLabel); row.appendChild(priceInput);
    row.appendChild(removeBtn);
    container.appendChild(row);
  }
  addSaleItemRow();
</script>
""" + confirm_flow_script("saleForm", "saleReview", "saleReviewPanel")


@app.route("/sales-form", methods=["GET"])
def sales_form():
    operator = request.args.get("operator", "Operator")
    sellable_items = get_dropdown_items("Finished Goods") + get_dropdown_items("Semi-Finished") + get_dropdown_items("Raw Material")
    return render_template_string(SALES_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   finished_goods_items=sellable_items)


@app.route("/submit-sale", methods=["POST"])
def submit_sale():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    customer = request.form.get("customer", "")

    if is_likely_duplicate_submission("sale", operator, date_str):
        return render_template_string(DUPLICATE_BLOCKED_HTML, operator=operator, entry_date=date_str)

    items = request.form.getlist("item[]")
    customs = request.form.getlist("custom_item[]")
    quantities = request.form.getlist("quantity[]")
    prices = request.form.getlist("price[]")

    batch_id = str(uuid.uuid4())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO submissions (batch_id, form_type, entry_date, entry_time, operator, customer) VALUES (%s,%s,%s,%s,%s,%s)",
        (batch_id, "sale", date_str, time_str, operator, customer),
    )

    line_summaries = []
    for item, custom, qty_raw, price_raw in zip(items, customs, quantities, prices):
        final_item = custom.strip() if item == "Other" and custom.strip() else item
        qty = safe_float(qty_raw)
        price = safe_float(price_raw)
        if not final_item or qty == 0:
            continue
        total_amount = round(qty * price, 2)
        cur.execute(
            "INSERT INTO line_items (batch_id, category, item_name, quantity, price, total_amount) VALUES (%s,%s,%s,%s,%s,%s)",
            (batch_id, "sale", final_item, qty, price, total_amount),
        )
        line_summaries.append((final_item, qty, price, total_amount))

    conn.commit()
    cur.close()

    resync_sales_sheet()

    if is_date_already_closed(date_str):
        cascade_reclose_from_date(date_str)

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Dashboard data helpers ----------
def get_recent_readings(limit=12, operator_filter=None, date_filter=None):
    conn = get_db_connection()
    cur = conn.cursor()
    query = "SELECT id, entry_date, entry_time, operator, t1, t2, t3, b1, b1_hours, b2, b2_hours, alerts FROM readings"
    where_parts, params = [], []
    if operator_filter:
        where_parts.append("operator=%s")
        params.append(operator_filter)
    if date_filter:
        where_parts.append("entry_date=%s")
        params.append(date_filter)
    if where_parts:
        query += " WHERE " + " AND ".join(where_parts)
    query += " ORDER BY entry_date DESC, entry_time DESC LIMIT %s"
    params.append(limit)
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def get_recent_line_items(categories, limit=12, operator_filter=None, date_filter=None):
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
        SELECT li.id, s.entry_date, s.entry_time, s.operator, li.category, li.item_name,
               li.quantity, li.price, li.total_amount, s.customer, s.electricity_units, li.batch_id
        FROM line_items li JOIN submissions s ON li.batch_id = s.batch_id
        WHERE li.category = ANY(%s)
    """
    params = [categories]
    if operator_filter:
        query += " AND s.operator=%s"
        params.append(operator_filter)
    if date_filter:
        query += " AND s.entry_date=%s"
        params.append(date_filter)
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
        if include_sales:
            qty, revenue = sum_sales_qty_and_revenue_for_month(item, month_str)
            report["sales_qty"] = qty
            report["sales_revenue"] = revenue
    elif category_label == "Semi-Finished":
        report["produced"] = sum_qty_for_month("ms_wire_produced", item, month_str) + sum_qty_for_month("scrap_produced", item, month_str)
        report["consumed"] = sum_qty_for_month("ms_wire_consumed", item, month_str)
        report["receipts"] = sum_qty_for_month("receipt_semi_finished", item, month_str)
        if include_sales:
            qty, revenue = sum_sales_qty_and_revenue_for_month(item, month_str)
            report["sales_qty"] = qty
            report["sales_revenue"] = revenue
    else:
        report["production"] = sum_qty_for_month("production", item, month_str)
        if include_sales:
            qty, revenue = sum_sales_qty_and_revenue_for_month(item, month_str)
            report["sales_qty"] = qty
            report["sales_revenue"] = revenue
    return report


def get_monthly_category_report(category_label, month_str, include_sales):
    known_items = get_all_known_items()
    items = sorted(set(SEED_ITEMS.get(category_label, [])) | set(known_items.get(category_label, [])))
    report = {"category": category_label, "month": month_str, "item_count": len(items)}

    if category_label == "Consumables":
        report["consumption"] = round(sum(sum_qty_for_month("consumption", i, month_str) for i in items), 2)
        report["receipts"] = round(sum(sum_qty_for_month("receipt_consumables", i, month_str) for i in items), 2)
    elif category_label == "Raw Material":
        report["issued"] = round(sum(sum_qty_for_month("wire_rod", i, month_str) for i in items), 2)
        report["receipts"] = round(sum(sum_qty_for_month("receipt_raw_material", i, month_str) for i in items), 2)
        if include_sales:
            tq, tr = 0.0, 0.0
            for i in items:
                q, r = sum_sales_qty_and_revenue_for_month(i, month_str)
                tq += q
                tr += r
            report["sales_qty"] = round(tq, 2)
            report["sales_revenue"] = round(tr, 2)
    elif category_label == "Semi-Finished":
        report["produced"] = round(sum(sum_qty_for_month("ms_wire_produced", i, month_str) + sum_qty_for_month("scrap_produced", i, month_str) for i in items), 2)
        report["consumed"] = round(sum(sum_qty_for_month("ms_wire_consumed", i, month_str) for i in items), 2)
        report["receipts"] = round(sum(sum_qty_for_month("receipt_semi_finished", i, month_str) for i in items), 2)
        if include_sales:
            tq, tr = 0.0, 0.0
            for i in items:
                q, r = sum_sales_qty_and_revenue_for_month(i, month_str)
                tq += q
                tr += r
            report["sales_qty"] = round(tq, 2)
            report["sales_revenue"] = round(tr, 2)
    else:
        report["production"] = round(sum(sum_qty_for_month("production", i, month_str) for i in items), 2)
        if include_sales:
            tq, tr = 0.0, 0.0
            for i in items:
                q, r = sum_sales_qty_and_revenue_for_month(i, month_str)
                tq += q
                tr += r
            report["sales_qty"] = round(tq, 2)
            report["sales_revenue"] = round(tr, 2)
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
  :root { --accent: #2C2C2C; --accent-dark: #1A1A1A; }
  .brand-header { display:flex; flex-direction:column; align-items:center; text-align:center; gap:10px; margin-bottom:18px; }
  .brand-header .logo-box { background:white; border-radius:10px; padding:14px 26px; box-shadow:0 2px 10px rgba(44,44,44,0.10); border:1px solid var(--line); }
  .brand-header img { height:76px; width:auto; display:block; }
  .brand-header .brand-title { font-family:'Barlow Condensed'; font-weight:700; font-size:26px; letter-spacing:0.03em; text-transform:uppercase; color:var(--ink); }
  .brand-header .dash-title { font-family:'Barlow Condensed'; font-weight:700; font-size:20px; letter-spacing:0.04em; text-transform:uppercase; color:var(--accent-dark); margin-top:2px; }
  .date-picker-bar { display:flex; align-items:center; justify-content:center; gap:10px; margin:4px 0 20px; flex-wrap:wrap; }
  .date-picker-bar input[type=date] { max-width:200px; }
  .today-pill { background:var(--brand); color:white; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:0.03em; padding:4px 10px; border-radius:20px; }
  .cat-total-row { display:flex; justify-content:space-between; align-items:center; margin:4px 0 10px; }
  .cat-total-row .cat-total-value { font-size:20px; font-weight:700; color:var(--accent-dark); }
  .kpi-strip { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin:8px 0 22px; }
  .kpi-card { background:linear-gradient(135deg,var(--accent) 0%,var(--accent-dark) 100%); color:white; border-radius:8px; padding:16px 18px; }
  .kpi-card .kpi-label { font-size:11px; text-transform:uppercase; letter-spacing:0.04em; opacity:0.85; font-weight:700; }
  .kpi-card .kpi-value { font-size:24px; font-weight:700; margin-top:4px; }
  .kpi-card.alert { background:linear-gradient(135deg,#D64545 0%,#A83636 100%); }
  .alert-panel { background:#FDECEC; border:1.5px solid #F1B3B3; border-radius:8px; padding:14px 18px; margin-bottom:22px; }
  .alert-panel .alert-title { color:var(--bad); font-weight:700; text-transform:uppercase; font-size:13px; letter-spacing:0.03em; margin-bottom:6px; }
  .alert-panel .alert-item { font-size:14px; color:var(--ink); padding:3px 0; }
  .alert-panel.all-clear { background:#EAF5EA; border-color:#B9DDB9; }
  .alert-panel.all-clear .alert-title { color:var(--ok); }
  .bar-list { margin-top:6px; }
  .bar-row { display:flex; align-items:center; gap:10px; margin:10px 0; }
  .bar-row .bar-label { width:110px; font-size:13px; color:var(--ink-soft); flex-shrink:0; text-align:right; }
  .bar-row .bar-track { flex:1; background:#ECECED; border-radius:20px; height:20px; overflow:hidden; }
  .bar-row .bar-fill { height:100%; border-radius:20px; transition:width 0.4s ease; }
  .bar-row .bar-value { width:60px; font-size:13px; font-weight:700; text-align:left; flex-shrink:0; }
  .footer-brand { margin-top:36px; padding-top:20px; border-top:1px solid var(--line); font-size:13px; color:var(--ink-soft); line-height:1.6; text-align:center; }

  /* ---- Admin dark sidebar shell (admin dashboard only) ---- */
  .admin-shell { display:flex; width:100%; max-width:1400px; margin:0 auto; background:#F7F7F8; border-radius:14px;
    overflow:hidden; box-shadow:0 4px 24px rgba(44,44,44,0.10); min-height:600px; border:1px solid #E3E3E5; }
  .admin-sidebar { width:220px; flex-shrink:0; background:#FFFFFF; padding:22px 14px; display:flex; flex-direction:column; gap:4px;
    border-right:1px solid #E3E3E5; }
  .admin-sidebar .side-logo-box { background:white; border-radius:8px; padding:10px 12px; margin-bottom:20px; text-align:center; }
  .admin-sidebar .side-logo-box img { height:34px; width:auto; }
  .admin-sidebar .side-link { display:flex; align-items:center; gap:10px; padding:12px 14px; border-radius:8px; color:#6B6E73;
    text-decoration:none; font-size:14px; font-weight:700; font-family:'Barlow Condensed'; text-transform:uppercase; letter-spacing:0.03em; }
  .admin-sidebar .side-link:hover { background:#F7F7F8; color:#2C2C2C; }
  .admin-sidebar .side-link.active { background:var(--brand); color:white; }
  .admin-sidebar .side-divider { height:1px; background:#E3E3E5; margin:12px 6px; }
  .admin-main { flex:1; padding:28px 32px; min-width:0; }
  .admin-shell .card.wide { max-width:100%; background:transparent; border:none; box-shadow:none; padding:0; }
  .admin-shell .brand-header { display:none; }
  .admin-shell .nav-top { display:none; }

  /* Portrait phones (like iPhone held normally): stack the sidebar above the
     content instead of side-by-side, and turn it into a horizontal scrolling
     row of links instead of a tall vertical column - this is what was forcing
     landscape rotation before. */
  @media (max-width: 820px) {
    .admin-shell { flex-direction: column; border-radius: 10px; }
    .admin-sidebar { width: 100%; flex-direction: row; overflow-x: auto; padding: 12px; gap: 8px;
      border-right: none; border-bottom: 1px solid #E3E3E5; -webkit-overflow-scrolling: touch; }
    .admin-sidebar .side-logo-box { display: none; }
    .admin-sidebar .side-link { flex-shrink: 0; padding: 10px 14px; font-size: 12px; white-space: nowrap; }
    .admin-sidebar .side-divider { display: none; }
    .admin-main { padding: 16px; }
    .kpi-strip, .stat-grid { grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); }
    table { font-size: 12px; }
    .table-wrap { overflow-x: auto; }
  }
  .kpi-card.c1 { background:linear-gradient(135deg,#FF6B9D 0%,#C44569 100%); }
  .kpi-card.c2 { background:linear-gradient(135deg,#4FACFE 0%,#0072B1 100%); }
  .kpi-card.c3 { background:linear-gradient(135deg,#43E97B 0%,#2C9E5C 100%); }
  .kpi-card.c4 { background:linear-gradient(135deg,#F2994A 0%,#C2691E 100%); }
  .kpi-card.c5 { background:linear-gradient(135deg,#A78BFA 0%,#7C5CD6 100%); }
  .chart-card { background:#FFFFFF; border:1px solid #E3E3E5; border-radius:10px; padding:18px 20px; margin:16px 0 22px; }
  .chart-card h3 { font-family:'Barlow Condensed'; text-transform:uppercase; font-size:14px; letter-spacing:0.03em; color:#6B6E73; margin:0 0 12px; }
</style>
{% if show_edit %}
<div class="admin-shell">
  <div class="admin-sidebar">
    <div class="side-logo-box"><img src="/static/logo.png" alt="Khemji Wire"></div>
    <a class="side-link active" href="/dashboard?key={{ admin_key }}">&#128202; Dashboard</a>
    <a class="side-link" href="/monthly-costs?key={{ admin_key }}">&#128181; Monthly Costs</a>
    <a class="side-link" href="/pnl-report?key={{ admin_key }}">&#128200; P&amp;L Report</a>
    <a class="side-link" href="/insights?key={{ admin_key }}">&#128269; Insights</a>
    <a class="side-link" href="/check-duplicates?key={{ admin_key }}">&#128203; Check Duplicates</a>
    <a class="side-link" href="/opening-stock?key={{ admin_key }}">&#128230; Opening Stock</a>
    <a class="side-link" href="/set-opening-value?key={{ admin_key }}">&#128176; Opening Value (P&amp;L)</a>
    <a class="side-link" href="/maintenance-dashboard?key={{ admin_key }}">&#128736; Maintenance</a>
    <a class="side-link" href="/log-expense?key={{ admin_key }}">&#128179; Log Expense</a>
    <div class="side-divider"></div>
    <a class="side-link" href="/app-home">&larr; Entry Forms</a>
  </div>
  <div class="admin-main">
{% else %}
<div class="nav-top"><a href="/app-home">&larr; Back to entry forms</a></div>
{% endif %}
<div class="card wide">
  <div class="brand-header">
    <div class="logo-box"><img src="/static/logo.png" alt="Khemji Wire"></div>
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
    <div class="kpi-card {{ 'alert' if low_stock_count > 0 else 'c3' }}">
      <div class="kpi-label">Items Low / Out of Stock</div>
      <div class="kpi-value">{{ low_stock_count }}</div>
    </div>
    <div class="kpi-card c2">
      <div class="kpi-label">Production ({{ selected_date }})</div>
      <div class="kpi-value">{{ day_production }}</div>
    </div>
    {% if show_sales %}
    <div class="kpi-card c1">
      <div class="kpi-label">Qty Sold ({{ selected_date }})</div>
      <div class="kpi-value">{{ day_sales_qty }}</div>
    </div>
    <div class="kpi-card c4">
      <div class="kpi-label">Revenue ({{ selected_date }})</div>
      <div class="kpi-value">Rs. {{ day_sales_revenue }}</div>
    </div>
    {% if top_seller %}
    <div class="kpi-card c5">
      <div class="kpi-label">Top Seller</div>
      <div class="kpi-value" style="font-size:17px;">{{ top_seller.item }}<br><span style="font-size:13px;font-weight:400;">{{ top_seller.qty }} units</span></div>
    </div>
    {% endif %}
    {% if show_sales %}
    <div class="kpi-card" style="background:linear-gradient(135deg,#8B5E00 0%,#5E3F00 100%);">
      <div class="kpi-label">This Month's Expenses</div>
      <div class="kpi-value">Rs. {{ month_expenses_total }}</div>
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

  {% if show_edit %}
  <div class="chart-card">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:12px;">
      <h3 style="margin:0;">{{ trend_label }} &mdash; Last 7 Days</h3>
      <select id="chartMetricSelect" onchange="changeChartMetric()" style="max-width:220px;padding:8px 10px;font-size:13px;">
        {% for key, cfg in chart_metric_options.items() %}
        <option value="{{ key }}" {{ 'selected' if key == chart_metric else '' }}>{{ cfg.label }}</option>
        {% endfor %}
      </select>
    </div>
    <canvas id="productionTrendChart" height="80"></canvas>
  </div>
  <script>
    function changeChartMetric() {
      var metric = document.getElementById('chartMetricSelect').value;
      var url = new URL(window.location.href);
      url.searchParams.set('chart_metric', metric);
      window.location.href = url.toString();
    }
  </script>
  {% endif %}

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
      <option value="semifinished">Semi-Finished (MS Wire)</option>
      <option value="finished">Finished Goods</option>
      <option value="consumables">Consumables</option>
    </select>
  </div>

  <div id="bars-raw" class="bar-list cat-block"></div>
  <div id="bars-semifinished" class="bar-list cat-block" style="display:none;"></div>
  <div id="bars-finished" class="bar-list cat-block" style="display:none;"></div>
  <div id="bars-consumables" class="bar-list cat-block" style="display:none;"></div>

  <div id="stat-raw" class="stat-grid cat-block">
    {% for row in raw_material_stock %}
    <div class="stat-card" style="border-left:4px solid #5C6B78;"><div class="label">Wire Rod {{ row.item }}</div><div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div></div>
    {% endfor %}
  </div>
  <div id="stat-semifinished" class="stat-grid cat-block" style="display:none;">
    {% for row in semi_finished_stock %}
    <div class="stat-card" style="border-left:4px solid #B8860B;"><div class="label">{{ row.item }}</div><div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div></div>
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
  <div id="table-semifinished" class="cat-block" style="display:none;">
    <div class="cat-total-row"><span>Total Semi-Finished Balance</span><span class="cat-total-value">{{ totals.semi_finished }}</span></div>
    <div class="table-wrap"><table><tr><th>Item</th><th>Opening</th><th>Produced/Received</th><th>Consumed/Sold</th><th>Balance</th></tr>
    {% for row in semi_finished_stock %}<tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td><td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>{% endfor %}
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

  {% if show_sales %}
  <div style="display:flex;gap:12px;margin:20px 0;flex-wrap:wrap;">
    <a href="/pnl-report?key={{ admin_key }}" style="flex:1;min-width:200px;text-align:center;padding:14px 0;background:var(--accent-dark);color:white;border-radius:8px;text-decoration:none;font-weight:700;text-transform:uppercase;font-size:14px;">View Profit &amp; Loss Statement</a>
    <a href="/monthly-costs?key={{ admin_key }}" style="flex:1;min-width:200px;text-align:center;padding:14px 0;background:var(--ink);color:white;border-radius:8px;text-decoration:none;font-weight:700;text-transform:uppercase;font-size:14px;">Set Monthly Fixed Costs</a>
  </div>
  {% endif %}

  <h2 class="section">Process Yield Report</h2>
  <p class="subtitle" style="padding-left:0;">Calculated Finished Goods = (Wire Rod Issued &times; {{ ms_wire_pct }}%) + (Zinc Consumed &times; {{ zinc_pct }}%) &mdash; compared against what was actually reported as produced</p>

  <h3 style="font-family:'Barlow Condensed';text-transform:uppercase;font-size:14px;color:var(--ink-soft);margin:16px 0 6px;">For {{ selected_date }} (Selected Date Above)</h3>
  <p style="font-size:12px;color:var(--ink-soft);margin-top:-4px;">Wire Rod/Zinc and Production shown here are for the same shift day (entered the next morning, dated to when the shift actually happened)</p>
  <div class="stat-grid" style="margin-bottom:20px;">
    <div class="stat-card"><div class="label">Wire Rod Issued</div><div class="value">{{ daily_yield.wire_rod_total }}</div></div>
    <div class="stat-card"><div class="label">Zinc Consumed</div><div class="value">{{ daily_yield.zinc_total }}</div></div>
    <div class="stat-card"><div class="label">Calculated Finished Goods</div><div class="value">{{ daily_yield.calculated_fg }}</div></div>
    <div class="stat-card"><div class="label">Actual Finished Goods</div><div class="value">{{ daily_yield.actual_fg }}</div></div>
    <div class="stat-card" style="border-left:4px solid {{ 'var(--bad)' if daily_yield.losses < 0 else 'var(--ok)' }};">
      <div class="label">Losses / Variance</div><div class="value {{ 'low' if daily_yield.losses < 0 else '' }}">{{ daily_yield.losses }}</div>
    </div>
  </div>

  <h3 style="font-family:'Barlow Condensed';text-transform:uppercase;font-size:14px;color:var(--ink-soft);margin:16px 0 6px;">For {{ report_month }} (Selected Month Below)</h3>
  <div class="stat-grid" style="margin-bottom:20px;">
    <div class="stat-card"><div class="label">Wire Rod Issued</div><div class="value">{{ monthly_yield.wire_rod_total }}</div></div>
    <div class="stat-card"><div class="label">Zinc Consumed</div><div class="value">{{ monthly_yield.zinc_total }}</div></div>
    <div class="stat-card"><div class="label">Calculated Finished Goods</div><div class="value">{{ monthly_yield.calculated_fg }}</div></div>
    <div class="stat-card"><div class="label">Actual Finished Goods</div><div class="value">{{ monthly_yield.actual_fg }}</div></div>
    <div class="stat-card" style="border-left:4px solid {{ 'var(--bad)' if monthly_yield.losses < 0 else 'var(--ok)' }};">
      <div class="label">Losses / Variance</div><div class="value {{ 'low' if monthly_yield.losses < 0 else '' }}">{{ monthly_yield.losses }}</div>
    </div>
  </div>

  <h2 class="section">Monthly Item Report</h2>
  <div class="date-picker-bar" style="justify-content:flex-start;">
    <label style="margin:0;">Item</label>
    <select id="reportItemSelect" onchange="changeMonthlyReport()" style="max-width:260px;">
      <optgroup label="Whole Category Totals">
        {% for opt in category_totals_options %}
        <option value="{{ opt }}" {{ 'selected' if opt == report_item else '' }}>Total {{ opt.replace('__TOTAL__', '') }}</option>
        {% endfor %}
      </optgroup>
      <optgroup label="Individual Items">
        {% for item in all_report_items %}<option value="{{ item }}" {{ 'selected' if item == report_item else '' }}>{{ item }}</option>{% endfor %}
      </optgroup>
    </select>
    <label style="margin:0;">Month</label>
    <input type="month" id="reportMonthSelect" value="{{ report_month }}" onchange="changeMonthlyReport()">
  </div>
  {% if monthly_report %}
  <p class="subtitle" style="padding-left:0;margin-top:10px;">
    {% if monthly_report.is_category_total %}Showing totals across all {{ monthly_report.category }} items ({{ monthly_report.item_count }} items){% else %}Showing {{ monthly_report.item }}{% endif %}
  </p>
  <div class="stat-grid" style="margin-bottom:26px;">
    {% if 'consumption' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid #2E7D32;"><div class="label">Consumption ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.consumption }}</div></div>
    <div class="stat-card" style="border-left:4px solid #2E7D32;"><div class="label">Received ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.receipts }}</div></div>
    {% endif %}
    {% if 'issued' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid #5C6B78;"><div class="label">Wire Rod Issued ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.issued }}</div></div>
    <div class="stat-card" style="border-left:4px solid var(--accent);"><div class="label">Received ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.receipts }}</div></div>
    {% if show_sales and 'sales_qty' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid #8B5E00;"><div class="label">Qty Sold ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.sales_qty }}</div></div>
    <div class="stat-card" style="border-left:4px solid #8B5E00;"><div class="label">Revenue ({{ monthly_report.month }})</div><div class="value">Rs. {{ monthly_report.sales_revenue }}</div></div>
    {% endif %}
    {% endif %}
    {% if 'produced' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid #B8860B;"><div class="label">MS Wire Produced ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.produced }}</div></div>
    <div class="stat-card" style="border-left:4px solid #B8860B;"><div class="label">MS Wire Consumed ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.consumed }}</div></div>
    <div class="stat-card" style="border-left:4px solid #B8860B;"><div class="label">Received ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.receipts }}</div></div>
    {% if show_sales and 'sales_qty' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid #8B5E00;"><div class="label">Qty Sold ({{ monthly_report.month }})</div><div class="value">{{ monthly_report.sales_qty }}</div></div>
    <div class="stat-card" style="border-left:4px solid #8B5E00;"><div class="label">Revenue ({{ monthly_report.month }})</div><div class="value">Rs. {{ monthly_report.sales_revenue }}</div></div>
    {% endif %}
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

  <h2 class="section">Entries for {{ selected_date }}</h2>

  <details>
    <summary>Furnace Readings</summary>
    <div class="table-wrap">
    {% if show_edit %}
    <form method="POST" action="/bulk-delete-readings" onsubmit="return confirm('Delete all selected readings? This cannot be undone.');">
      <input type="hidden" name="key" value="{{ admin_key }}">
      <table>
        <tr><th><input type="checkbox" onclick="document.querySelectorAll('.readingChk').forEach(function(c){c.checked=this.checked;}.bind(this))"></th><th>Date</th><th>Time</th><th>Operator</th><th>T1</th><th>T2</th><th>T3</th><th>B1</th><th>B1 Hrs</th><th>B2</th><th>B2 Hrs</th><th>Alerts</th><th>Edit</th></tr>
        {% for r in furnace_rows %}
        <tr><td><input type="checkbox" class="readingChk" name="delete_ids[]" value="{{ r[0] }}"></td><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[4] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td><td>{{ r[7] }}</td><td>{{ r[8] }}</td><td>{{ r[9] }}</td><td>{{ r[10] }}</td><td>{{ r[11] }}</td>
        <td><a href="/edit-entry?table=readings&id={{ r[0] }}&key={{ admin_key }}">Edit</a></td></tr>
        {% endfor %}
      </table>
      <button type="submit" style="margin-top:12px;padding:10px 20px;background:var(--bad);color:white;border:none;border-radius:6px;font-weight:700;text-transform:uppercase;cursor:pointer;font-size:13px;">Delete Selected</button>
    </form>
    {% else %}
      <table>
        <tr><th>Date</th><th>Time</th><th>Operator</th><th>T1</th><th>T2</th><th>T3</th><th>B1</th><th>B1 Hrs</th><th>B2</th><th>B2 Hrs</th><th>Alerts</th></tr>
        {% for r in furnace_rows %}
        <tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[4] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td><td>{{ r[7] }}</td><td>{{ r[8] }}</td><td>{{ r[9] }}</td><td>{{ r[10] }}</td><td>{{ r[11] }}</td></tr>
        {% endfor %}
      </table>
    {% endif %}
    </div>
  </details>

  <details>
    <summary>Production</summary>
    <div class="table-wrap">
    {% if show_edit %}
    <form method="POST" action="/bulk-delete-line-items" onsubmit="return confirm('Delete all selected entries? This cannot be undone.');">
      <input type="hidden" name="key" value="{{ admin_key }}">
      <table>
        <tr><th><input type="checkbox" onclick="this.closest('table').querySelectorAll('.li-chk').forEach(function(c){c.checked=this.checked;}.bind(this))"></th><th>Date</th><th>Time</th><th>Operator</th><th>Item</th><th>Qty</th><th>Edit</th></tr>
        {% for r in production_rows %}
        <tr><td><input type="checkbox" class="li-chk" name="delete_ids[]" value="{{ r[0] }}"></td><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td>
        <td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a></td></tr>
        {% endfor %}
      </table>
      <button type="submit" style="margin-top:12px;padding:10px 20px;background:var(--bad);color:white;border:none;border-radius:6px;font-weight:700;text-transform:uppercase;cursor:pointer;font-size:13px;">Delete Selected</button>
    </form>
    {% else %}
      <table>
        <tr><th>Date</th><th>Time</th><th>Operator</th><th>Item</th><th>Qty</th></tr>
        {% for r in production_rows %}<tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td></tr>{% endfor %}
      </table>
    {% endif %}
    </div>
  </details>

  <details>
    <summary>Consumption</summary>
    <div class="table-wrap">
    {% if show_edit %}
    <form method="POST" action="/bulk-delete-line-items" onsubmit="return confirm('Delete all selected entries? This cannot be undone.');">
      <input type="hidden" name="key" value="{{ admin_key }}">
      <table>
        <tr><th><input type="checkbox" onclick="this.closest('table').querySelectorAll('.li-chk').forEach(function(c){c.checked=this.checked;}.bind(this))"></th><th>Date</th><th>Time</th><th>Operator</th><th>Item</th><th>Qty</th><th>Edit</th></tr>
        {% for r in consumption_rows %}
        <tr><td><input type="checkbox" class="li-chk" name="delete_ids[]" value="{{ r[0] }}"></td><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td>
        <td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a></td></tr>
        {% endfor %}
      </table>
      <button type="submit" style="margin-top:12px;padding:10px 20px;background:var(--bad);color:white;border:none;border-radius:6px;font-weight:700;text-transform:uppercase;cursor:pointer;font-size:13px;">Delete Selected</button>
    </form>
    {% else %}
      <table>
        <tr><th>Date</th><th>Time</th><th>Operator</th><th>Item</th><th>Qty</th></tr>
        {% for r in consumption_rows %}<tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td></tr>{% endfor %}
      </table>
    {% endif %}
    </div>
  </details>

  <details>
    <summary>Electricity &amp; Wire Rod</summary>
    <div class="table-wrap">
    {% if show_edit %}
    <form method="POST" action="/bulk-delete-line-items" onsubmit="return confirm('Delete all selected entries? This cannot be undone.');">
      <input type="hidden" name="key" value="{{ admin_key }}">
      <table>
        <tr><th><input type="checkbox" onclick="this.closest('table').querySelectorAll('.li-chk').forEach(function(c){c.checked=this.checked;}.bind(this))"></th><th>Date</th><th>Time</th><th>Operator</th><th>Electricity Units</th><th>Type</th><th>Item</th><th>Qty</th><th>Edit</th></tr>
        {% for r in electricity_rows %}
        <tr><td><input type="checkbox" class="li-chk" name="delete_ids[]" value="{{ r[0] }}"></td><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[10] }}</td><td>{{ 'Wire Rod Issued' if r[4] == 'wire_rod' else 'MS Wire Issued' }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td>
        <td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a></td></tr>
        {% endfor %}
      </table>
      <button type="submit" style="margin-top:12px;padding:10px 20px;background:var(--bad);color:white;border:none;border-radius:6px;font-weight:700;text-transform:uppercase;cursor:pointer;font-size:13px;">Delete Selected</button>
    </form>
    {% else %}
      <table>
        <tr><th>Date</th><th>Time</th><th>Operator</th><th>Electricity Units</th><th>Type</th><th>Item</th><th>Qty</th></tr>
        {% for r in electricity_rows %}<tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[10] }}</td><td>{{ 'Wire Rod Issued' if r[4] == 'wire_rod' else 'MS Wire Issued' }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td></tr>{% endfor %}
      </table>
    {% endif %}
    </div>
  </details>

  <details>
    <summary>Receipts</summary>
    <div class="table-wrap">
    {% if show_edit %}
    <form method="POST" action="/bulk-delete-line-items" onsubmit="return confirm('Delete all selected entries? This cannot be undone.');">
      <input type="hidden" name="key" value="{{ admin_key }}">
      <table>
        <tr><th><input type="checkbox" onclick="this.closest('table').querySelectorAll('.li-chk').forEach(function(c){c.checked=this.checked;}.bind(this))"></th><th>Date</th><th>Time</th><th>Operator</th><th>Category</th><th>Item</th><th>Qty</th><th>Rate</th><th>Value</th><th>Edit</th></tr>
        {% for r in receipts_rows %}
        <tr><td><input type="checkbox" class="li-chk" name="delete_ids[]" value="{{ r[0] }}"></td><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ {'receipt_consumables':'Consumables','receipt_raw_material':'Raw Material','receipt_semi_finished':'Semi-Finished','receipt_finished_goods':'Finished Goods'}.get(r[4], r[4]) }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td><td>{{ r[7] if r[7] is not none else '' }}</td><td>{{ r[8] if r[8] is not none else '' }}</td>
        <td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a></td></tr>
        {% endfor %}
      </table>
      <button type="submit" style="margin-top:12px;padding:10px 20px;background:var(--bad);color:white;border:none;border-radius:6px;font-weight:700;text-transform:uppercase;cursor:pointer;font-size:13px;">Delete Selected</button>
    </form>
    {% else %}
      <table>
        <tr><th>Date</th><th>Time</th><th>Operator</th><th>Category</th><th>Item</th><th>Qty</th><th>Rate</th><th>Value</th></tr>
        {% for r in receipts_rows %}<tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ {'receipt_consumables':'Consumables','receipt_raw_material':'Raw Material','receipt_semi_finished':'Semi-Finished','receipt_finished_goods':'Finished Goods'}.get(r[4], r[4]) }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td><td>{{ r[7] if r[7] is not none else '' }}</td><td>{{ r[8] if r[8] is not none else '' }}</td></tr>{% endfor %}
      </table>
    {% endif %}
    </div>
  </details>

  {% if show_sales %}
  <details>
    <summary>Sales</summary>
    <div class="table-wrap">
    {% if show_edit %}
    <form method="POST" action="/bulk-delete-line-items" onsubmit="return confirm('Delete all selected entries? This cannot be undone.');">
      <input type="hidden" name="key" value="{{ admin_key }}">
      <table>
        <tr><th><input type="checkbox" onclick="this.closest('table').querySelectorAll('.li-chk').forEach(function(c){c.checked=this.checked;}.bind(this))"></th><th>Date</th><th>Time</th><th>Operator</th><th>Item</th><th>Qty</th><th>Price</th><th>Total</th><th>Customer</th><th>Edit</th></tr>
        {% for r in sales_rows %}
        <tr><td><input type="checkbox" class="li-chk" name="delete_ids[]" value="{{ r[0] }}"></td><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td><td>{{ r[7] }}</td><td>{{ r[8] }}</td><td>{{ r[9] }}</td>
        <td><a href="/edit-entry?table=line_items&id={{ r[0] }}&key={{ admin_key }}">Edit</a></td></tr>
        {% endfor %}
      </table>
      <button type="submit" style="margin-top:12px;padding:10px 20px;background:var(--bad);color:white;border:none;border-radius:6px;font-weight:700;text-transform:uppercase;cursor:pointer;font-size:13px;">Delete Selected</button>
    </form>
    {% else %}
      <table>
        <tr><th>Date</th><th>Time</th><th>Operator</th><th>Item</th><th>Qty</th><th>Price</th><th>Total</th><th>Customer</th></tr>
        {% for r in sales_rows %}<tr><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td><td>{{ r[5] }}</td><td>{{ r[6] }}</td><td>{{ r[7] }}</td><td>{{ r[8] }}</td><td>{{ r[9] }}</td></tr>{% endfor %}
      </table>
    {% endif %}
    </div>
  </details>
  {% endif %}

  <div class="footer-brand">
    <b>Khemji Wire &amp; Wire Pvt. Ltd.</b> &middot; F-153, Sarna Doongar, RIICO Industrial Area, Jaipur, Rajasthan 302012<br>
    Phone: +91-9829277869 &middot; +91-141-2954144 &middot; Email: info@khemjiwire.in<br>
    GSTIN: 08AAECA7760L1ZA &middot; IS 280 &amp; IS 3975 Certified
  </div>
</div>
{% if show_edit %}
  </div>
</div>
{% endif %}
<script>
  var stockData = { raw: {{ raw_material_stock | tojson }}, semifinished: {{ semi_finished_stock | tojson }}, finished: {{ finished_goods_stock | tojson }}, consumables: {{ consumables_stock | tojson }} };
  var colors = { raw: '#5C6B78', semifinished: '#B8860B', finished: '#2C2C2C', consumables: '#2E7D32' };
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
  ['raw', 'semifinished', 'finished', 'consumables'].forEach(renderBars);
  function showCategory() {
    var cat = document.getElementById('catSelect').value;
    ['raw', 'semifinished', 'finished', 'consumables'].forEach(function(c) {
      document.getElementById('stat-' + c).style.display = (c === cat) ? 'grid' : 'none';
      document.getElementById('table-' + c).style.display = (c === cat) ? 'block' : 'none';
      document.getElementById('bars-' + c).style.display = (c === cat) ? 'block' : 'none';
    });
  }
</script>
{% if show_edit %}
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<script>
  var trendCanvas = document.getElementById('productionTrendChart');
  if (trendCanvas) {
    new Chart(trendCanvas, {
      type: 'line',
      data: {
        labels: {{ trend_labels | tojson }},
        datasets: [{
          label: {{ trend_label | tojson }},
          data: {{ trend_values | tojson }},
          borderColor: '#F26A04',
          backgroundColor: 'rgba(242,106,4,0.12)',
          fill: true,
          tension: 0.35,
          pointRadius: 3,
          pointBackgroundColor: '#F26A04',
        }]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#6B6E73' }, grid: { color: '#E3E3E5' } },
          y: { ticks: { color: '#6B6E73' }, grid: { color: '#E3E3E5' }, beginAtZero: true }
        }
      }
    });
  }
</script>
{% endif %}
"""


# ---------- Dashboard route ----------
DASHBOARD_ERROR_HTML = """
<div style="font-family:'Segoe UI',Arial,sans-serif;max-width:480px;margin:60px auto;text-align:center;
            background:white;border-radius:10px;padding:32px 24px;box-shadow:0 4px 16px rgba(0,0,0,0.1);">
  <div style="font-size:40px;margin-bottom:10px;">&#9888;</div>
  <h2 style="color:#2C2C2C;margin:0 0 10px;">Dashboard Temporarily Unavailable</h2>
  <p style="color:#4B5563;line-height:1.5;">Please wait a few seconds and reload the page.</p>
  <p style="color:#9CA3AF;font-size:12px;margin-top:18px;">({{ error }})</p>
</div>
"""


CHART_METRIC_OPTIONS = {
    "production": {"label": "Production (kg)", "category": "production", "value_field": "quantity"},
    "sales_qty": {"label": "Sales Quantity", "category": "sale", "value_field": "quantity"},
    "sales_revenue": {"label": "Sales Revenue (Rs.)", "category": "sale", "value_field": "total_amount"},
    "wire_rod": {"label": "Wire Rod Issued (kg)", "category": "wire_rod", "value_field": "quantity"},
    "zinc": {"label": "Zinc Consumed (kg)", "category": "consumption", "value_field": "quantity", "item_filter": "Zinc"},
    "receipts_consumables": {"label": "Consumables Received (kg)", "category": "receipt_consumables", "value_field": "quantity"},
    "general_expenses": {"label": "General Expenses (Rs.)"},
    "maintenance_cost": {"label": "Maintenance Cost (Rs.)"},
}


def get_metric_trend(metric_key, days=7):
    """Returns (labels, values, label) for the last N days of whichever metric
    is selected - powers the admin dashboard's dynamic trend chart."""
    start_date = (now_ist() - timedelta(days=days - 1)).strftime("%Y-%m-%d")

    if metric_key == "general_expenses":
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT entry_date, COALESCE(SUM(amount),0) FROM general_expenses WHERE entry_date >= %s GROUP BY entry_date", (start_date,))
        by_date = {r[0].strftime("%Y-%m-%d"): float(r[1]) for r in cur.fetchall()}
        cur.close()
        label = "General Expenses (Rs.)"
    elif metric_key == "maintenance_cost":
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT entry_date, COALESCE(SUM(amount),0) FROM maintenance_expenses WHERE entry_date >= %s GROUP BY entry_date", (start_date,))
        by_date = {r[0].strftime("%Y-%m-%d"): float(r[1]) for r in cur.fetchall()}
        cur.execute("SELECT reported_date, COALESCE(SUM(repair_cost),0) FROM breakdown_maintenance WHERE reported_date >= %s AND repair_cost IS NOT NULL GROUP BY reported_date", (start_date,))
        for r in cur.fetchall():
            d = r[0].strftime("%Y-%m-%d")
            by_date[d] = by_date.get(d, 0.0) + float(r[1])
        cur.close()
        label = "Maintenance Cost (Rs.)"
    else:
        config = CHART_METRIC_OPTIONS.get(metric_key, CHART_METRIC_OPTIONS["production"])
        conn = get_db_connection()
        cur = conn.cursor()
        query = f"""
            SELECT s.entry_date, COALESCE(SUM(li.{config['value_field']}),0)
            FROM submissions s JOIN line_items li ON li.batch_id = s.batch_id
            WHERE li.category=%s AND s.entry_date >= %s
        """
        params = [config["category"], start_date]
        if "item_filter" in config:
            query += " AND li.item_name=%s"
            params.append(config["item_filter"])
        query += " GROUP BY s.entry_date ORDER BY s.entry_date"
        cur.execute(query, params)
        by_date = {r[0].strftime("%Y-%m-%d"): float(r[1]) for r in cur.fetchall()}
        cur.close()
        label = config["label"]

    labels, values = [], []
    for i in range(days - 1, -1, -1):
        d = (now_ist() - timedelta(days=i)).strftime("%Y-%m-%d")
        labels.append(d[5:])  # MM-DD, compact for chart labels
        values.append(round(by_date.get(d, 0.0), 2))
    return labels, values, label


def render_dashboard(is_admin, selected_date=None, operator_filter=None, report_item=None, report_month=None, chart_metric=None):
    today = now_ist().strftime("%Y-%m-%d")
    date_str = selected_date or today
    is_today = (date_str == today)

    consumables_stock, raw_material_stock, semi_finished_stock, finished_goods_stock, totals = compute_stock(date_str)

    low_stock_items = []
    for row in raw_material_stock:
        if row["balance"] <= 0:
            low_stock_items.append({"category": "Raw Material", "item": row["item"], "balance": row["balance"]})
    for row in semi_finished_stock:
        if row["balance"] <= 0:
            low_stock_items.append({"category": "Semi-Finished", "item": row["item"], "balance": row["balance"]})
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
    all_report_items = sorted(set(known_items["Consumables"]) | set(known_items["Raw Material"])
                               | set(known_items["Semi-Finished"]) | set(known_items["Finished Goods"])
                               | set(SEED_ITEMS["Consumables"]) | set(SEED_ITEMS["Raw Material"])
                               | set(SEED_ITEMS["Semi-Finished"]) | set(SEED_ITEMS["Finished Goods"]))
    category_totals_options = ["__TOTAL__Consumables", "__TOTAL__Raw Material", "__TOTAL__Semi-Finished", "__TOTAL__Finished Goods"]
    month_str = report_month or now_ist().strftime("%Y-%m")
    item_for_report = report_item or (all_report_items[0] if all_report_items else "")

    daily_yield = compute_yield_report("date", date_str)
    monthly_yield = compute_yield_report("month", month_str)
    chart_metric = chart_metric or "production"
    trend_labels, trend_values, trend_label = get_metric_trend(chart_metric, 7) if is_admin else ([], [], "")
    month_expenses_total = round(sum_general_expenses_for_month(today[:7]) + sum_maintenance_cost_for_month(today[:7]), 2) if is_admin else 0

    if item_for_report.startswith("__TOTAL__"):
        cat_for_total = item_for_report.replace("__TOTAL__", "")
        monthly_report = get_monthly_category_report(cat_for_total, month_str, is_admin)
        monthly_report["is_category_total"] = True
    else:
        item_category = find_item_category(item_for_report, {
            "Consumables": sorted(set(SEED_ITEMS["Consumables"]) | set(known_items["Consumables"])),
            "Raw Material": sorted(set(SEED_ITEMS["Raw Material"]) | set(known_items["Raw Material"])),
            "Semi-Finished": sorted(set(SEED_ITEMS["Semi-Finished"]) | set(known_items["Semi-Finished"])),
            "Finished Goods": sorted(set(SEED_ITEMS["Finished Goods"]) | set(known_items["Finished Goods"])),
        })
        monthly_report = get_monthly_item_report(item_for_report, month_str, item_category, is_admin) if item_category else None
        if monthly_report:
            monthly_report["is_category_total"] = False

    furnace_rows = get_recent_readings(limit=50, operator_filter=operator_filter, date_filter=date_str)
    production_rows = get_recent_line_items(["production"], limit=50, operator_filter=operator_filter, date_filter=date_str)
    consumption_rows = get_recent_line_items(["consumption"], limit=50, operator_filter=operator_filter, date_filter=date_str)
    electricity_rows_raw = get_recent_line_items(["wire_rod", "ms_wire_consumed"], limit=50, operator_filter=operator_filter, date_filter=date_str)

    def dedupe_electricity_units(rows):
        """Rows sharing the same submission (date+time+operator) share one
        Electricity Units value - only show it on the first row, blank the rest,
        so it doesn't visually repeat once a submission has multiple items."""
        seen = set()
        result = []
        for r in rows:
            key = (r[1], r[2], r[3])
            r_list = list(r)
            if key in seen:
                r_list[10] = ""
            else:
                seen.add(key)
            result.append(r_list)
        return result

    electricity_rows = dedupe_electricity_units(electricity_rows_raw)
    receipts_rows = get_recent_line_items(["receipt_consumables", "receipt_raw_material", "receipt_semi_finished", "receipt_finished_goods"], limit=50, operator_filter=operator_filter, date_filter=date_str)
    sales_rows = get_recent_line_items(["sale"], limit=50, operator_filter=operator_filter, date_filter=date_str) if is_admin else []

    return render_template_string(
        DASHBOARD_HTML,
        admin_key=ADMIN_KEY if is_admin else "",
        show_sales=is_admin, show_edit=is_admin,
        selected_date=date_str, is_today=is_today,
        operator_filter=operator_filter or "", all_operator_names=sorted(ALL_PEOPLE_NAMES),
        low_stock_items=low_stock_items, low_stock_count=len(low_stock_items),
        day_production=day_production, day_sales_qty=day_sales_qty, day_sales_revenue=day_sales_revenue, top_seller=top_seller,
        last_updated=last_updated, totals=totals,
        all_report_items=all_report_items, category_totals_options=category_totals_options,
        daily_yield=daily_yield, monthly_yield=monthly_yield,
        trend_labels=trend_labels, trend_values=trend_values, trend_label=trend_label,
        chart_metric=chart_metric, chart_metric_options=CHART_METRIC_OPTIONS, month_expenses_total=month_expenses_total,
        ms_wire_pct=round(MS_WIRE_CONVERSION_FACTOR * 100), zinc_pct=round(ZINC_YIELD_FACTOR * 100),
        report_item=item_for_report, report_month=month_str, monthly_report=monthly_report,
        consumables_stock=consumables_stock, raw_material_stock=raw_material_stock,
        semi_finished_stock=semi_finished_stock, finished_goods_stock=finished_goods_stock,
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
            chart_metric=request.args.get("chart_metric"),
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
SUBMISSION_COLUMNS = ["entry_date", "entry_time", "operator"]
NOT_NULL_COLUMNS = {
    "line_items": {"item_name", "quantity"},
    "readings": {"entry_date", "entry_time", "operator"},
}
NOT_NULL_SUBMISSION_COLUMNS = {"entry_date", "entry_time", "operator"}


@app.route("/edit-entry", methods=["GET"])
def edit_entry():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    table = request.args.get("table", "")
    row_id = request.args.get("id", "")
    if table not in ("readings", "line_items") or not row_id:
        abort(400)

    conn = get_db_connection()
    cur = conn.cursor()

    if table == "readings":
        cur.execute(f"SELECT {', '.join(READINGS_COLUMNS)} FROM readings WHERE id=%s", (row_id,))
        row = cur.fetchone()
        cur.close()
        if not row:
            abort(404)
        fields = list(zip(READINGS_COLUMNS, row))
        protected = NOT_NULL_COLUMNS["readings"]
    else:
        # line_items needs its own columns PLUS the date/time/operator from the linked submission
        cur.execute(f"SELECT {', '.join(LINE_ITEMS_COLUMNS)}, batch_id, category FROM line_items WHERE id=%s", (row_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            abort(404)
        item_values, batch_id, category = row[:-2], row[-2], row[-1]

        is_electricity_related = category in ("wire_rod", "ms_wire_produced", "ms_wire_consumed", "scrap_produced")
        sub_columns = list(SUBMISSION_COLUMNS) + (["electricity_units"] if is_electricity_related else [])
        cur.execute(f"SELECT {', '.join(sub_columns)} FROM submissions WHERE batch_id=%s", (batch_id,))
        sub_row = cur.fetchone()
        cur.close()

        fields = list(zip(sub_columns, sub_row)) + list(zip(LINE_ITEMS_COLUMNS, item_values))
        protected = NOT_NULL_COLUMNS["line_items"] | NOT_NULL_SUBMISSION_COLUMNS

    return render_template_string(EDIT_FORM_HTML, table=table, row_id=row_id, fields=fields, admin_key=ADMIN_KEY,
                                   protected_columns=protected)


@app.route("/save-edit", methods=["POST"])
def save_edit():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    table = request.form.get("table", "")
    row_id = request.form.get("row_id", "")

    conn = get_db_connection()
    cur = conn.cursor()
    edited_category = None
    batch_id = None

    COLUMN_SQL_TYPES = {
        "item_name": "text", "quantity": "numeric",
        "entry_date": "date", "entry_time": "time", "operator": "text",
        "electricity_units": "numeric",
    }

    def build_update(columns, protected_cols):
        set_parts, values = [], []
        for col in columns:
            raw = request.form.get(f"col_{col}", "")
            if col in protected_cols:
                col_type = COLUMN_SQL_TYPES.get(col, "text")
                if col_type == "text":
                    set_parts.append(f"{col} = COALESCE(NULLIF(%s, ''), {col})")
                else:
                    set_parts.append(f"{col} = COALESCE(NULLIF(%s, '')::{col_type}, {col})")
                values.append(raw)
            else:
                set_parts.append(f"{col} = %s")
                values.append(raw if raw != "" else None)
        return ", ".join(set_parts), values

    if table == "readings":
        set_clause, values = build_update(READINGS_COLUMNS, NOT_NULL_COLUMNS["readings"])
        cur.execute(f"UPDATE readings SET {set_clause} WHERE id=%s", values + [row_id])
    else:
        cur.execute("SELECT batch_id, category FROM line_items WHERE id=%s", (row_id,))
        batch_id, edited_category = cur.fetchone()
        cur.execute("SELECT entry_date FROM submissions WHERE batch_id=%s", (batch_id,))
        old_date = cur.fetchone()[0].strftime("%Y-%m-%d")

        # Update the line_items row itself
        set_clause, values = build_update(LINE_ITEMS_COLUMNS, NOT_NULL_COLUMNS["line_items"])
        if edited_category and (edited_category.startswith("receipt_") or edited_category == "sale"):
            # Total Value doesn't auto-calculate on its own in this generic edit
            # form - if Rate or Quantity changed, recompute it here so editing
            # in a Rate later (e.g. adding it after the fact) actually reflects
            # everywhere that depends on it (Insights, P&L).
            qty_idx = LINE_ITEMS_COLUMNS.index("quantity")
            price_idx = LINE_ITEMS_COLUMNS.index("price")
            total_idx = LINE_ITEMS_COLUMNS.index("total_amount")
            raw_qty, raw_price = values[qty_idx], values[price_idx]
            try:
                qty_val = float(raw_qty) if raw_qty not in (None, "") else None
                price_val = float(raw_price) if raw_price not in (None, "") else None
                if qty_val is not None and price_val is not None:
                    values[total_idx] = str(round(qty_val * price_val, 2))
            except ValueError:
                pass
        cur.execute(f"UPDATE line_items SET {set_clause} WHERE id=%s", values + [row_id])

        # Also update the linked submission's date/time/operator (and Electricity
        # Units, if this entry is part of an Electricity & Wire Rod submission -
        # only present in the submitted form when that field was actually shown).
        sub_columns = list(SUBMISSION_COLUMNS)
        if request.form.get("col_electricity_units") is not None:
            sub_columns.append("electricity_units")
        sub_set_clause, sub_values = build_update(sub_columns, NOT_NULL_SUBMISSION_COLUMNS)
        cur.execute(f"UPDATE submissions SET {sub_set_clause} WHERE batch_id=%s", sub_values + [batch_id])

        cur.execute("SELECT entry_date FROM submissions WHERE batch_id=%s", (batch_id,))
        new_date = cur.fetchone()[0].strftime("%Y-%m-%d")

    conn.commit()
    cur.close()

    if table == "line_items":
        if edited_category == "wire_rod":
            recalc_ms_wire_and_scrap_for_batch(batch_id)
        # If this entry lands on (or used to be on) an already-closed day, that
        # day's frozen StockLedger snapshot is now stale - recalculate it.
        maybe_recascade_for_date(old_date)
        if new_date != old_date:
            maybe_recascade_for_date(new_date)

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
    batch_id, was_wire_rod, entry_date_str = None, False, None

    if table == "readings":
        cur.execute("SELECT entry_date FROM readings WHERE id=%s", (row_id,))
        row = cur.fetchone()
        if row:
            entry_date_str = row[0].strftime("%Y-%m-%d")
    else:
        cur.execute("SELECT batch_id, category FROM line_items WHERE id=%s", (row_id,))
        row = cur.fetchone()
        if row:
            batch_id, category = row
            was_wire_rod = (category == "wire_rod")
            cur.execute("SELECT entry_date FROM submissions WHERE batch_id=%s", (batch_id,))
            sub_row = cur.fetchone()
            if sub_row:
                entry_date_str = sub_row[0].strftime("%Y-%m-%d")

    cur.execute(f"DELETE FROM {table} WHERE id=%s", (row_id,))
    conn.commit()
    cur.close()

    if was_wire_rod:
        recalc_ms_wire_and_scrap_for_batch(batch_id)
    maybe_recascade_for_date(entry_date_str)

    resync_sheet_for_table(table)

    return render_template_string(SUCCESS_HTML, operator="Admin", alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/dashboard?key={ADMIN_KEY}";}}, 1200);</script>'


DELETE_SUBMISSION_CONFIRM_HTML = BASE_STYLE + """
<div class="card">
  <h1 style="border-left-color:var(--bad);">Delete Entire Entry</h1>
  <p class="subtitle">This removes ALL {{ items|length }} item(s) from this single submission at once. Cannot be undone.</p>
  <div style="background:var(--paper);border:1px solid var(--line);border-radius:8px;padding:14px 16px;margin:16px 0;">
    <div style="display:flex;justify-content:space-between;padding:4px 0;font-size:14px;"><span style="color:var(--ink-soft);">Date / Time / Operator</span><b>{{ date }} {{ time }} - {{ operator }}</b></div>
    {% for item_name, qty in items %}
    <div style="display:flex;justify-content:space-between;padding:4px 0;font-size:14px;"><span style="color:var(--ink-soft);">{{ item_name }}</span><b>{{ qty }}</b></div>
    {% endfor %}
  </div>
  <form method="POST" action="/confirm-delete-submission">
    <input type="hidden" name="batch_id" value="{{ batch_id }}">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <div style="display:flex;gap:10px;margin-top:10px;">
      <a href="/dashboard?key={{ admin_key }}" style="flex:1;text-align:center;padding:15px 0;background:var(--ink);color:white;border-radius:8px;text-decoration:none;font-weight:700;text-transform:uppercase;">Cancel</a>
      <button type="submit" style="flex:1;padding:15px 0;background:var(--bad);color:white;border:none;border-radius:8px;font-weight:700;text-transform:uppercase;cursor:pointer;">Yes, Delete All {{ items|length }} Items</button>
    </div>
  </form>
</div>
"""


@app.route("/bulk-delete-readings", methods=["POST"])
def bulk_delete_readings():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    ids = request.form.getlist("delete_ids[]")
    if ids:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT entry_date FROM readings WHERE id = ANY(%s)", (ids,))
        affected_dates = [r[0].strftime("%Y-%m-%d") for r in cur.fetchall()]

        cur.execute("DELETE FROM readings WHERE id = ANY(%s)", (ids,))
        conn.commit()
        cur.close()

        for d in affected_dates:
            maybe_recascade_for_date(d)

        resync_sheet_for_table("readings")

    return render_template_string(SUCCESS_HTML, operator="Admin", alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/dashboard?key={ADMIN_KEY}";}}, 1200);</script>'


@app.route("/bulk-delete-line-items", methods=["POST"])
def bulk_delete_line_items():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    ids = request.form.getlist("delete_ids[]")
    if ids:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT batch_id FROM line_items WHERE id = ANY(%s) AND category='wire_rod'", (ids,))
        affected_wire_rod_batches = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT s.entry_date FROM line_items li JOIN submissions s ON li.batch_id = s.batch_id
            WHERE li.id = ANY(%s)
        """, (ids,))
        affected_dates = [r[0].strftime("%Y-%m-%d") for r in cur.fetchall()]

        cur.execute("DELETE FROM line_items WHERE id = ANY(%s)", (ids,))
        conn.commit()
        cur.close()

        for batch_id in affected_wire_rod_batches:
            recalc_ms_wire_and_scrap_for_batch(batch_id)
        for d in affected_dates:
            maybe_recascade_for_date(d)

        resync_sheet_for_table("line_items")

    return render_template_string(SUCCESS_HTML, operator="Admin", alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/dashboard?key={ADMIN_KEY}";}}, 1200);</script>'


@app.route("/delete-submission", methods=["GET"])
def delete_submission_confirm():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    batch_id = request.args.get("batch_id", "")
    if not batch_id:
        abort(400)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT entry_date, entry_time, operator FROM submissions WHERE batch_id=%s", (batch_id,))
    sub = cur.fetchone()
    if not sub:
        cur.close()
        abort(404)
    cur.execute("SELECT item_name, quantity FROM line_items WHERE batch_id=%s ORDER BY id", (batch_id,))
    items = [(r[0], float(r[1])) for r in cur.fetchall()]
    cur.close()

    return render_template_string(
        DELETE_SUBMISSION_CONFIRM_HTML, batch_id=batch_id, admin_key=ADMIN_KEY,
        date=sub[0], time=sub[1], operator=sub[2], items=items,
    )


@app.route("/confirm-delete-submission", methods=["POST"])
def confirm_delete_submission():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    batch_id = request.form.get("batch_id", "")
    if not batch_id:
        abort(400)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT entry_date FROM submissions WHERE batch_id=%s", (batch_id,))
    row = cur.fetchone()
    entry_date_str = row[0].strftime("%Y-%m-%d") if row else None

    # line_items has ON DELETE CASCADE tied to submissions.batch_id, so removing
    # the submission removes every item in it in one go.
    cur.execute("DELETE FROM submissions WHERE batch_id=%s", (batch_id,))
    conn.commit()
    cur.close()

    maybe_recascade_for_date(entry_date_str)

    resync_sheet_for_table("line_items")

    return render_template_string(SUCCESS_HTML, operator="Admin", alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/dashboard?key={ADMIN_KEY}";}}, 1200);</script>'


# ---------- Admin: set opening stock ----------
OPENING_STOCK_HTML = BASE_STYLE + """
<div class="card wide">
  <h1>Set Opening Stock</h1>
  <p class="subtitle">The stock as it stood at the START of the date you choose below</p>
  <form method="POST">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <label>As Of Date</label>
    <input type="date" name="as_of_date" value="{{ today }}" required>
    <p style="font-size:12px;color:var(--ink-soft);margin-top:-4px;">
      Enter the stock as it was at the start of this date - not including that day's own production/sales.
      The system automatically adds/subtracts everything already logged for that date onward.
    </p>
    <h2 class="section">Consumables</h2>
    {% for item, qty, value in consumables %}
    <label>{{ item }} - Qty</label><input type="number" step="any" name="qty_{{ item }}" value="{{ qty }}">
    {% endfor %}
    <h2 class="section">Raw Material</h2>
    <p style="font-size:12px;color:var(--ink-soft);">Value (Rs.) is optional - only needed for the very first month, as a starting cost basis. After that it carries forward automatically.</p>
    {% for item, qty, value in raw_material %}
    <div class="extra-row">
      <div style="flex:1;"><label>{{ item }} - Qty</label><input type="number" step="any" name="qty_{{ item }}" value="{{ qty }}"></div>
      <div style="flex:1;"><label>{{ item }} - Value (Rs.)</label><input type="number" step="any" name="value_{{ item }}" value="{{ value if value is not none else '' }}"></div>
    </div>
    {% endfor %}
    <h2 class="section">Semi-Finished (MS Wire, Scrap)</h2>
    <p style="font-size:12px;color:var(--ink-soft);">Value (Rs.) is optional - same as above, only needed for the first month.</p>
    {% for item, qty, value in semi_finished %}
    <div class="extra-row">
      <div style="flex:1;"><label>{{ item }} - Qty</label><input type="number" step="any" name="qty_{{ item }}" value="{{ qty }}"></div>
      <div style="flex:1;"><label>{{ item }} - Value (Rs.)</label><input type="number" step="any" name="value_{{ item }}" value="{{ value if value is not none else '' }}"></div>
    </div>
    {% endfor %}
    <h2 class="section">Finished Goods</h2>
    {% for item, qty, value in finished_goods %}
    <label>{{ item }} - Qty</label><input type="number" step="any" name="qty_{{ item }}" value="{{ qty }}">
    {% endfor %}
    <button class="submit" type="submit">Save Opening Stock</button>
  </form>
</div>
"""


def resync_opening_stock_sheet():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT item_name, category, opening_qty, updated_at FROM opening_stock ORDER BY category, item_name")
    rows = cur.fetchall()
    cur.close()
    header = ["Item", "Category", "Opening Qty", "As Of Date"]
    data = [header] + [[r[0], r[1], float(r[2]), r[3].strftime("%Y-%m-%d") if r[3] else ""] for r in rows]
    ws = get_or_create_sheet_tab("OpeningStock", header)
    ws.clear()
    ws.update(values=data, range_name="A1")


# ---------- Admin: monthly fixed costs (Salary, Interest, Logistics, Other) ----------
MONTHLY_COSTS_HTML = BASE_STYLE + """
<div class="card">
  <h1>Monthly Fixed Costs</h1>
  <p class="subtitle">One simple number each, for the month selected below - used in the P&amp;L calculation</p>
  <form method="POST">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <label>Month</label>
    <input type="month" name="month" value="{{ month }}" required onchange="this.form.submit()">
    <label>Electricity Cost (Rs.)</label>
    <input type="number" step="any" name="electricity_cost" value="{{ costs.electricity_cost }}">
    <label>Salary (Rs.)</label>
    <input type="number" step="any" name="salary" value="{{ costs.salary }}">
    <label>Interest (Rs.)</label>
    <input type="number" step="any" name="interest" value="{{ costs.interest }}">
    <label>Logistics Cost (Rs.)</label>
    <input type="number" step="any" name="logistics" value="{{ costs.logistics }}">
    <label>Director Remuneration (Rs.)</label>
    <input type="number" step="any" name="director_remuneration" value="{{ costs.director_remuneration }}">
    <button class="submit" type="submit">Save</button>
  </form>

  <h2 class="section">Auto-Calculated This Month</h2>
  <div class="stat-grid">
    <div class="stat-card"><div class="label">Other Costs (from Log Expense)</div><div class="value">Rs. {{ other_costs_this_month }}</div></div>
    <div class="stat-card"><div class="label">Maintenance Cost</div><div class="value">Rs. {{ maintenance_cost_this_month }}</div></div>
  </div>
  <p style="font-size:12px;color:var(--ink-soft);">These are no longer entered manually - they total up automatically from every entry logged via <a href="/log-expense?key={{ admin_key }}">Log Expense</a> and <a href="/log-maintenance-expense?key={{ admin_key }}">Log Maintenance Expense</a>.</p>
  <a href="/dashboard?key={{ admin_key }}" style="display:block;text-align:center;margin-top:20px;color:var(--accent-dark);font-weight:700;text-decoration:none;">&larr; Back to Dashboard</a>
</div>
"""


PNL_REPORT_HTML = BASE_STYLE + """
<div class="card wide">
  <h1>Profit &amp; Loss Statement</h1>
  <p class="subtitle">Admin only &mdash; never visible to operators</p>

  <form method="GET" style="display:flex;gap:10px;align-items:center;margin-bottom:20px;">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <label style="margin:0;">Month</label>
    <input type="month" name="month" value="{{ pnl.month }}" onchange="this.form.submit()">
  </form>

  {% if pnl.last_month_rate is none %}
  <div class="alert-panel">
    <div class="alert-title">&#9888; No prior month's rate on file</div>
    <div class="alert-item">This looks like the first month using P&amp;L. Enter a starting Opening Finished Goods value manually below, then re-submit.</div>
    <form method="POST" style="margin-top:10px;">
      <input type="hidden" name="key" value="{{ admin_key }}">
      <input type="hidden" name="month" value="{{ pnl.month }}">
      <label>Opening Finished Goods Value (Rs.) &mdash; one-time manual entry</label>
      <input type="number" step="any" name="manual_opening_fg_value" value="{{ pnl.fg_opening_value }}">
      <button class="submit" type="submit">Recalculate With This Value</button>
    </form>
  </div>
  {% endif %}

  <h2 class="section">Cost</h2>
  <div class="table-wrap"><table>
    <tr><td>Wire Rod Consumed (stock-adjusted, weighted-average)</td><td>Rs. {{ pnl.wire_rod_value }}</td></tr>
    <tr><td colspan="2" style="padding-left:24px;font-size:13px;">
      <div>Opening: {{ pnl.rm_opening_qty }} kg &mdash; Rs. {{ pnl.rm_opening_value }}</div>
      <div>Received this month: {{ pnl.rm_added_qty }} kg &mdash; Rs. {{ pnl.rm_added_value }}</div>
      <div>Closing: {{ pnl.rm_closing_qty }} kg &mdash; Rs. {{ pnl.rm_closing_value }}</div>
      <div>Weighted-average rate: Rs. {{ pnl.rm_weighted_avg_rate }}/kg (carries forward to next month automatically)</div>
    </td></tr>
    <tr><td>Consumables Consumed</td><td>Rs. {{ pnl.consumables_value }}</td></tr>
    {% if pnl.consumables_breakdown %}
    <tr><td colspan="2" style="padding-left:24px;font-size:13px;">
      {% for c in pnl.consumables_breakdown %}<div>{{ c.item }}: {{ c.qty }} kg &times; Rs. {{ c.rate }}/kg = Rs. {{ c.value }}</div>{% endfor %}
    </td></tr>
    {% endif %}
    <tr><td>Electricity</td><td>Rs. {{ pnl.electricity_cost }}</td></tr>
    <tr><td>Salary</td><td>Rs. {{ pnl.salary }}</td></tr>
    <tr><td>Interest</td><td>Rs. {{ pnl.interest }}</td></tr>
    <tr><td>Logistics</td><td>Rs. {{ pnl.logistics }}</td></tr>
    <tr><td>Director Remuneration</td><td>Rs. {{ pnl.director_remuneration }}</td></tr>
    <tr><td>Other Costs</td><td>Rs. {{ pnl.other_costs }}</td></tr>
    {% if pnl.general_expense_rows %}
    <tr><td colspan="2" style="padding-left:24px;font-size:13px;">
      {% for e in pnl.general_expense_rows %}<div>{{ e.entry_date }} &mdash; {{ e.category }}: {{ e.description }} (Rs. {{ e.amount }})</div>{% endfor %}
    </td></tr>
    {% endif %}
    <tr><td>Maintenance Cost</td><td>Rs. {{ pnl.maintenance_cost }}</td></tr>
    {% if pnl.maintenance_expense_rows or pnl.breakdown_cost_rows %}
    <tr><td colspan="2" style="padding-left:24px;font-size:13px;">
      {% for e in pnl.maintenance_expense_rows %}<div>{{ e.entry_date }} &mdash; {{ e.machine or 'General' }}: {{ e.description }} (Rs. {{ e.amount }})</div>{% endfor %}
      {% for b in pnl.breakdown_cost_rows %}<div>{{ b.reported_date }} &mdash; {{ b.machine }} repair: {{ b.issue_description }} (Rs. {{ b.repair_cost }})</div>{% endfor %}
    </td></tr>
    {% endif %}
    <tr style="font-weight:700;"><td>Total Manufacturing Cost</td><td>Rs. {{ pnl.total_manufacturing_cost }}</td></tr>
  </table></div>

  {% if general_expense_details %}
  <h3 style="font-family:'Barlow Condensed';text-transform:uppercase;font-size:14px;color:var(--ink-soft);margin:16px 0 6px;">Breakdown - Other Costs (Log Expense)</h3>
  <div class="table-wrap"><table>
    <tr><th>Date</th><th>Category</th><th>Description</th><th>Amount</th></tr>
    {% for e in general_expense_details %}
    <tr><td>{{ e.entry_date }}</td><td>{{ e.category }}</td><td>{{ e.description }}</td><td>Rs. {{ e.amount }}</td></tr>
    {% endfor %}
  </table></div>
  {% endif %}

  {% if maintenance_expense_details or breakdown_cost_details %}
  <h3 style="font-family:'Barlow Condensed';text-transform:uppercase;font-size:14px;color:var(--ink-soft);margin:16px 0 6px;">Breakdown - Maintenance Cost</h3>
  <div class="table-wrap"><table>
    <tr><th>Date</th><th>Machine</th><th>Description</th><th>Amount</th></tr>
    {% for e in maintenance_expense_details %}
    <tr><td>{{ e.entry_date }}</td><td>{{ e.machine or '-' }}</td><td>{{ e.description }}</td><td>Rs. {{ e.amount }}</td></tr>
    {% endfor %}
    {% for b in breakdown_cost_details %}
    <tr><td>{{ b.reported_date }}</td><td>{{ b.machine }}</td><td>Breakdown: {{ b.issue_description }}</td><td>Rs. {{ b.repair_cost }}</td></tr>
    {% endfor %}
  </table></div>
  {% endif %}

  <h2 class="section">MS Wire Stock Adjustment</h2>
  <div class="table-wrap"><table>
    <tr><td>Opening MS Wire ({{ pnl.ms_wire_opening_qty }} kg)</td><td>Rs. {{ pnl.ms_wire_opening_value }}</td></tr>
    <tr><td>Closing MS Wire ({{ pnl.ms_wire_closing_qty }} kg)</td><td>Rs. {{ pnl.ms_wire_closing_value }}</td></tr>
    <tr><td colspan="2" style="font-size:13px;color:var(--ink-soft);">MS Wire's own weighted-average rate this month: Rs. {{ pnl.sf_weighted_avg_rate }}/kg (independent from Wire Rod's rate, carries forward automatically)</td></tr>
  </table></div>

  <div class="stat-grid" style="margin:16px 0;">
    <div class="stat-card" style="border-left:4px solid var(--ink);"><div class="label">Cost of Goods Manufactured (COGM)</div><div class="value">Rs. {{ pnl.cogm }}</div></div>
    <div class="stat-card"><div class="label">Finished Goods Produced</div><div class="value">{{ pnl.fg_produced_kg }} kg</div></div>
    <div class="stat-card"><div class="label">Cost of Production / Kg</div><div class="value">Rs. {{ pnl.cost_per_kg_this_month }}</div></div>
  </div>

  <h2 class="section">Finished Goods Stock Adjustment</h2>
  <div class="table-wrap"><table>
    <tr><td>Opening FG ({{ pnl.fg_opening_qty }} kg, at last month's rate)</td><td>Rs. {{ pnl.fg_opening_value }}</td></tr>
    <tr><td>Closing FG ({{ pnl.fg_closing_qty }} kg, at this month's rate)</td><td>Rs. {{ pnl.fg_closing_value }}</td></tr>
  </table></div>

  <div class="stat-grid" style="margin:16px 0;">
    <div class="stat-card" style="border-left:4px solid var(--accent);"><div class="label">Cost of Goods Sold (COGS)</div><div class="value">Rs. {{ pnl.cogs }}</div></div>
  </div>

  <h2 class="section">Revenue</h2>
  <div class="table-wrap"><table>
    <tr><td>Finished Goods Sales</td><td>Rs. {{ pnl.fg_revenue }}</td></tr>
    <tr><td>MS Wire Sales</td><td>Rs. {{ pnl.ms_wire_revenue }}</td></tr>
    <tr><td>Scrap Sales</td><td>Rs. {{ pnl.scrap_revenue }}</td></tr>
    <tr><td>Wire Rod Sales</td><td>Rs. {{ pnl.wire_rod_revenue }}</td></tr>
    <tr style="font-weight:700;"><td>Total Revenue</td><td>Rs. {{ pnl.total_revenue }}</td></tr>
  </table></div>

  <div class="stat-grid" style="margin:16px 0;">
    <div class="stat-card" style="border-left:4px solid {{ 'var(--bad)' if pnl.gross_profit < 0 else 'var(--ok)' }};">
      <div class="label">Gross Profit</div><div class="value {{ 'low' if pnl.gross_profit < 0 else '' }}">Rs. {{ pnl.gross_profit }}</div>
    </div>
    <div class="stat-card"><div class="label">Conversion Cost / Kg</div><div class="value">Rs. {{ pnl.conversion_cost_per_kg }}</div></div>
  </div>

  <h2 class="section">Process Losses (Diagnostic Only &mdash; Already Included in Costs Above)</h2>
  <div class="table-wrap"><table>
    <tr><td>Scale Loss</td><td>{{ pnl.scale_loss_kg }} kg &mdash; Rs. {{ pnl.scale_loss_value }}</td></tr>
    <tr><td>Zinc Burning Loss</td><td>{{ pnl.zinc_burning_loss_kg }} kg &mdash; Rs. {{ pnl.zinc_burning_loss_value }}</td></tr>
  </table></div>
  <p style="font-size:12px;color:var(--ink-soft);">These are shown for visibility into where cost is going - they are NOT added again on top of the Total Manufacturing Cost above, since that cost already covers all material regardless of whether it became product or loss.</p>

  <a href="/dashboard?key={{ admin_key }}" style="display:block;text-align:center;margin-top:20px;color:var(--accent-dark);font-weight:700;text-decoration:none;">&larr; Back to Dashboard</a>
</div>
"""


INSIGHTS_HTML = BASE_STYLE + """
<div class="card wide">
  <h1>Insights</h1>
  <p class="subtitle">Average rates and totals from your actual Sales &amp; Receipt data - not for P&amp;L, for understanding the real story</p>

  <form method="GET" style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:20px;">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <label style="margin:0;">From</label>
    <input type="date" name="from_date" value="{{ from_date }}">
    <label style="margin:0;">To</label>
    <input type="date" name="to_date" value="{{ to_date }}">
    <button class="submit" type="submit" style="margin-top:0;width:auto;padding:10px 24px;">View</button>
  </form>

  <h2 class="section">Finished Goods Sold</h2>
  <div class="table-wrap"><table>
    <tr><th>Size</th><th>Qty Sold</th><th>Revenue</th><th>Avg Rate/Kg</th></tr>
    {% for r in fg_sales_rows %}
    <tr><td>{{ r.item }}</td><td>{{ r.qty_sold }}</td><td>Rs. {{ r.revenue }}</td><td>Rs. {{ r.avg_rate }}</td></tr>
    {% endfor %}
    <tr style="font-weight:700;"><td>TOTAL (All Sizes Blended)</td><td>{{ fg_total_qty }}</td><td>Rs. {{ fg_total_revenue }}</td><td>Rs. {{ fg_overall_avg }}</td></tr>
  </table></div>

  <h2 class="section">Semi-Finished Sold (MS Wire / Scrap)</h2>
  <div class="table-wrap"><table>
    <tr><th>Item</th><th>Qty Sold</th><th>Revenue</th><th>Avg Rate/Kg</th></tr>
    {% for r in sf_sales_rows %}
    <tr><td>{{ r.item }}</td><td>{{ r.qty_sold }}</td><td>Rs. {{ r.revenue }}</td><td>Rs. {{ r.avg_rate }}</td></tr>
    {% endfor %}
    <tr style="font-weight:700;"><td>TOTAL</td><td>{{ sf_total_qty }}</td><td>Rs. {{ sf_total_revenue }}</td><td>Rs. {{ sf_overall_avg }}</td></tr>
  </table></div>

  <h2 class="section">Wire Rod Sold (if any)</h2>
  <div class="table-wrap"><table>
    <tr><th>Size</th><th>Qty Sold</th><th>Revenue</th><th>Avg Rate/Kg</th></tr>
    {% for r in rm_sales_rows %}
    <tr><td>{{ r.item }}</td><td>{{ r.qty_sold }}</td><td>Rs. {{ r.revenue }}</td><td>Rs. {{ r.avg_rate }}</td></tr>
    {% endfor %}
    <tr style="font-weight:700;"><td>TOTAL</td><td>{{ rm_total_qty }}</td><td>Rs. {{ rm_total_revenue }}</td><td>Rs. {{ rm_overall_avg }}</td></tr>
  </table></div>

  <h2 class="section">Raw Material Purchased</h2>
  <div class="table-wrap"><table>
    <tr><th>Size</th><th>Qty Received</th><th>Amount Paid</th><th>Avg Rate/Kg</th></tr>
    {% for r in rm_purchase_rows %}
    <tr><td>{{ r.item }}</td><td>{{ r.qty_received }}</td><td>Rs. {{ r.amount_paid }}</td><td>Rs. {{ r.avg_rate }}</td></tr>
    {% endfor %}
    <tr style="font-weight:700;"><td>TOTAL</td><td>{{ rm_purchase_total_qty }}</td><td>Rs. {{ rm_purchase_total_amount }}</td><td>Rs. {{ rm_purchase_overall_avg }}</td></tr>
  </table></div>

  <h2 class="section">Consumables Purchased</h2>
  <div class="table-wrap"><table>
    <tr><th>Item</th><th>Qty Received</th><th>Amount Paid</th><th>Avg Rate/Kg</th></tr>
    {% for r in cons_purchase_rows %}
    <tr><td>{{ r.item }}</td><td>{{ r.qty_received }}</td><td>Rs. {{ r.amount_paid }}</td><td>Rs. {{ r.avg_rate }}</td></tr>
    {% endfor %}
    <tr style="font-weight:700;"><td>TOTAL</td><td>{{ cons_purchase_total_qty }}</td><td>Rs. {{ cons_purchase_total_amount }}</td><td>Rs. {{ cons_purchase_overall_avg }}</td></tr>
  </table></div>

  <a href="/dashboard?key={{ admin_key }}" style="display:block;text-align:center;margin-top:20px;color:var(--accent-dark);font-weight:700;text-decoration:none;">&larr; Back to Dashboard</a>
</div>
"""


@app.route("/insights", methods=["GET"])
def insights_page():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)

    today = now_ist()
    default_from = today.replace(day=1).strftime("%Y-%m-%d")
    default_to = today.strftime("%Y-%m-%d")
    from_date = request.args.get("from_date") or default_from
    to_date = request.args.get("to_date") or default_to

    fg_sales_rows, fg_total_qty, fg_total_revenue, fg_overall_avg = get_sales_insights("Finished Goods", from_date, to_date)
    sf_sales_rows, sf_total_qty, sf_total_revenue, sf_overall_avg = get_sales_insights("Semi-Finished", from_date, to_date)
    rm_sales_rows, rm_total_qty, rm_total_revenue, rm_overall_avg = get_sales_insights("Raw Material", from_date, to_date)

    rm_purchase_rows, rm_purchase_total_qty, rm_purchase_total_amount, rm_purchase_overall_avg = \
        get_purchase_insights("Raw Material", "receipt_raw_material", from_date, to_date)
    cons_purchase_rows, cons_purchase_total_qty, cons_purchase_total_amount, cons_purchase_overall_avg = \
        get_purchase_insights("Consumables", "receipt_consumables", from_date, to_date)

    return render_template_string(
        INSIGHTS_HTML, admin_key=ADMIN_KEY, from_date=from_date, to_date=to_date,
        fg_sales_rows=fg_sales_rows, fg_total_qty=fg_total_qty, fg_total_revenue=fg_total_revenue, fg_overall_avg=fg_overall_avg,
        sf_sales_rows=sf_sales_rows, sf_total_qty=sf_total_qty, sf_total_revenue=sf_total_revenue, sf_overall_avg=sf_overall_avg,
        rm_sales_rows=rm_sales_rows, rm_total_qty=rm_total_qty, rm_total_revenue=rm_total_revenue, rm_overall_avg=rm_overall_avg,
        rm_purchase_rows=rm_purchase_rows, rm_purchase_total_qty=rm_purchase_total_qty,
        rm_purchase_total_amount=rm_purchase_total_amount, rm_purchase_overall_avg=rm_purchase_overall_avg,
        cons_purchase_rows=cons_purchase_rows, cons_purchase_total_qty=cons_purchase_total_qty,
        cons_purchase_total_amount=cons_purchase_total_amount, cons_purchase_overall_avg=cons_purchase_overall_avg,
    )


@app.route("/pnl-report", methods=["GET", "POST"])
def pnl_report():
    if request.args.get("key") != ADMIN_KEY and request.form.get("key") != ADMIN_KEY:
        abort(403)
    month = request.values.get("month") or now_ist().strftime("%Y-%m")
    manual_opening = request.form.get("manual_opening_fg_value")
    manual_opening_val = safe_float(manual_opening) if manual_opening not in (None, "") else None

    pnl = compute_pnl(month, manual_opening_fg_value=manual_opening_val)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT entry_date, category, description, amount FROM general_expenses
        WHERE TO_CHAR(entry_date,'YYYY-MM')=%s ORDER BY entry_date
    """, (month,))
    general_expense_details = [{"entry_date": r[0], "category": r[1], "description": r[2], "amount": float(r[3])} for r in cur.fetchall()]

    cur.execute("""
        SELECT entry_date, machine, description, amount FROM maintenance_expenses
        WHERE TO_CHAR(entry_date,'YYYY-MM')=%s ORDER BY entry_date
    """, (month,))
    maintenance_expense_details = [{"entry_date": r[0], "machine": r[1], "description": r[2], "amount": float(r[3])} for r in cur.fetchall()]

    cur.execute("""
        SELECT machine, issue_description, reported_date, repair_cost FROM breakdown_maintenance
        WHERE TO_CHAR(reported_date,'YYYY-MM')=%s AND repair_cost IS NOT NULL ORDER BY reported_date
    """, (month,))
    breakdown_cost_details = [{"machine": r[0], "issue_description": r[1], "reported_date": r[2], "repair_cost": float(r[3])} for r in cur.fetchall()]
    cur.close()

    return render_template_string(PNL_REPORT_HTML, admin_key=ADMIN_KEY, pnl=pnl,
                                   general_expense_details=general_expense_details,
                                   maintenance_expense_details=maintenance_expense_details,
                                   breakdown_cost_details=breakdown_cost_details)


GENERAL_EXPENSE_HTML = BASE_STYLE + """
<div class="card wide">
  <div class="nav-top" style="justify-content:flex-start;margin-bottom:10px;"><a href="/app-home">&larr; Back to Home</a></div>
  <h1>Log Expense</h1>
  <p class="subtitle">Office, petty cash, and other indirect expenses - feeds automatically into P&amp;L "Other Costs"</p>

  <form id="expenseForm" method="POST" action="/submit-general-expense">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date</label>
    <input type="date" name="entry_date" value="{{ today }}" required>
    <label>Category</label>
    <select name="category">
      {% for c in categories %}<option value="{{ c }}">{{ c }}</option>{% endfor %}
    </select>
    <label>Description</label>
    <input type="text" name="description" required placeholder="e.g. Stationery purchase">
    <label>Amount (Rs.)</label>
    <input type="number" step="any" name="amount" required>
    <button class="submit review-btn" type="button">Review Entry</button>
  </form>

  <div id="expenseReviewPanel" style="display:none;">
    <h2 class="section">Review Your Entry</h2>
    <div id="expenseReview"></div>
    <div class="review-actions">
      <button class="secondary" type="button" onclick="goBackToForm_expenseForm()">Go Back</button>
      <button class="submit" type="submit" form="expenseForm">Confirm &amp; Submit</button>
    </div>
  </div>

  <h2 class="section">Expenses</h2>
  <form method="GET" style="display:flex;gap:10px;align-items:center;margin-bottom:10px;">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label style="margin:0;">Viewing Month</label>
    <input type="month" name="month" value="{{ view_month }}" onchange="this.form.submit()">
  </form>
  <div class="table-wrap"><table>
    <tr><th>Date</th><th>Category</th><th>Description</th><th>Amount</th><th>Action</th></tr>
    {% for e in expenses %}
    <tr><td>{{ e.entry_date }}</td><td>{{ e.category }}</td><td>{{ e.description }}</td><td>Rs. {{ e.amount }}</td>
    <td><a href="/edit-general-expense?id={{ e.id }}&operator={{ operator }}&month={{ view_month }}">Edit</a> | <a href="/delete-general-expense?id={{ e.id }}&operator={{ operator }}&month={{ view_month }}" style="color:var(--bad);" onclick="return confirm('Delete this expense?');">Delete</a></td></tr>
    {% endfor %}
    <tr style="font-weight:700;"><td colspan="3">Total</td><td>Rs. {{ month_total }}</td><td></td></tr>
  </table></div>
</div>
""" + confirm_flow_script("expenseForm", "expenseReview", "expenseReviewPanel")


@app.route("/log-expense", methods=["GET"])
def log_expense_form():
    operator = request.args.get("operator", "Operator")
    month_str = request.args.get("month") or now_ist().strftime("%Y-%m")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, entry_date, category, description, amount FROM general_expenses
        WHERE TO_CHAR(entry_date,'YYYY-MM')=%s ORDER BY entry_date DESC
    """, (month_str,))
    expenses = [{"id": r[0], "entry_date": r[1], "category": r[2], "description": r[3], "amount": float(r[4])} for r in cur.fetchall()]
    cur.close()
    month_total = round(sum(e["amount"] for e in expenses), 2)

    return render_template_string(GENERAL_EXPENSE_HTML, admin_key=ADMIN_KEY, operator=operator, today=now_ist().strftime("%Y-%m-%d"),
                                   categories=EXPENSE_CATEGORIES, expenses=expenses, month_total=month_total, view_month=month_str)


@app.route("/delete-general-expense", methods=["GET"])
def delete_general_expense():
    expense_id = request.args.get("id", "")
    operator = request.args.get("operator", "Operator")
    month = request.args.get("month", "")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM general_expenses WHERE id=%s", (expense_id,))
    conn.commit()
    cur.close()
    return redirect(f"/log-expense?operator={operator}&month={month}")


EDIT_GENERAL_EXPENSE_HTML = BASE_STYLE + """
<div class="card">
  <h1>Edit Expense</h1>
  <form method="POST" action="/save-general-expense">
    <input type="hidden" name="id" value="{{ e.id }}">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date</label>
    <input type="date" name="entry_date" value="{{ e.entry_date }}" required>
    <label>Category</label>
    <select name="category">
      {% for c in categories %}<option value="{{ c }}" {{ 'selected' if c == e.category else '' }}>{{ c }}</option>{% endfor %}
    </select>
    <label>Description</label>
    <input type="text" name="description" value="{{ e.description }}" required>
    <label>Amount (Rs.)</label>
    <input type="number" step="any" name="amount" value="{{ e.amount }}" required>
    <button class="submit" type="submit">Save Changes</button>
  </form>
</div>
"""


@app.route("/edit-general-expense", methods=["GET"])
def edit_general_expense():
    expense_id = request.args.get("id", "")
    operator = request.args.get("operator", "Operator")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, entry_date, category, description, amount FROM general_expenses WHERE id=%s", (expense_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        abort(404)
    e = {"id": row[0], "entry_date": row[1], "category": row[2], "description": row[3], "amount": row[4]}
    return render_template_string(EDIT_GENERAL_EXPENSE_HTML, e=e, operator=operator, categories=EXPENSE_CATEGORIES)


@app.route("/save-general-expense", methods=["POST"])
def save_general_expense():
    expense_id = request.form.get("id", "")
    operator = request.form.get("operator", "Operator")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE general_expenses SET entry_date=%s, category=%s, description=%s, amount=%s WHERE id=%s
    """, (request.form.get("entry_date", ""), request.form.get("category", "Other"),
          request.form.get("description", ""), safe_float(request.form.get("amount", "0")), expense_id))
    conn.commit()
    cur.close()
    new_month = request.form.get("entry_date", "")[:7]
    return redirect(f"/log-expense?operator={operator}&month={new_month}")


@app.route("/submit-general-expense", methods=["POST"])
def submit_general_expense():
    operator = request.form.get("operator", "Operator")
    entry_date = request.form.get("entry_date", "")
    category = request.form.get("category", "Other")
    description = request.form.get("description", "")
    amount = safe_float(request.form.get("amount", "0"))

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO general_expenses (entry_date, category, description, amount, operator)
        VALUES (%s,%s,%s,%s,%s)
    """, (entry_date, category, description, amount, operator))
    conn.commit()
    cur.close()

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None) + \
        f'<script>setTimeout(function(){{window.location.href="/log-expense?operator={operator}";}}, 1200);</script>'


@app.route("/monthly-costs", methods=["GET", "POST"])
def monthly_costs_admin():
    if request.args.get("key") != ADMIN_KEY and request.form.get("key") != ADMIN_KEY:
        abort(403)

    month = request.values.get("month") or now_ist().strftime("%Y-%m")

    if request.method == "POST":
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO monthly_costs (month, electricity_cost, salary, interest, logistics, director_remuneration, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (month) DO UPDATE SET electricity_cost=EXCLUDED.electricity_cost, salary=EXCLUDED.salary,
                interest=EXCLUDED.interest, logistics=EXCLUDED.logistics, director_remuneration=EXCLUDED.director_remuneration, updated_at=NOW()
        """, (month, safe_float(request.form.get("electricity_cost", "0")), safe_float(request.form.get("salary", "0")),
              safe_float(request.form.get("interest", "0")), safe_float(request.form.get("logistics", "0")),
              safe_float(request.form.get("director_remuneration", "0"))))
        conn.commit()
        cur.close()

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT electricity_cost, salary, interest, logistics, director_remuneration FROM monthly_costs WHERE month=%s", (month,))
    row = cur.fetchone()
    cur.close()
    costs = {"electricity_cost": float(row[0]), "salary": float(row[1]), "interest": float(row[2]),
             "logistics": float(row[3]), "director_remuneration": float(row[4])} if row \
        else {"electricity_cost": 0, "salary": 0, "interest": 0, "logistics": 0, "director_remuneration": 0}

    return render_template_string(MONTHLY_COSTS_HTML, admin_key=ADMIN_KEY, month=month, costs=costs,
                                   other_costs_this_month=sum_general_expenses_for_month(month),
                                   maintenance_cost_this_month=sum_maintenance_cost_for_month(month))


OPENING_VALUE_HTML = BASE_STYLE + """
<div class="card">
  <h1>Set Opening Value (P&amp;L Only)</h1>
  <p class="subtitle">A one-time starting cost basis for Wire Rod and MS Wire - this ONLY affects P&amp;L calculations. It never touches your live stock tracking, never triggers any recalculation, and your existing correct closing balances stay exactly as they are.</p>
  <form method="GET" style="display:flex;gap:10px;align-items:center;margin-bottom:16px;">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <label style="margin:0;">For Month</label>
    <input type="month" name="for_month" value="{{ for_month }}" onchange="this.form.submit()">
  </form>
  <form method="POST">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <input type="hidden" name="for_month" value="{{ for_month }}">
    <h2 class="section">Wire Rod</h2>
    <label>Opening Quantity as of {{ for_month }}-01 (kg)</label>
    <input type="number" step="any" name="wire_rod_qty" value="{{ wire_rod_manual_qty if wire_rod_manual_qty is not none else '' }}" placeholder="e.g. 203105">
    <label>Opening Value (Rs.)</label>
    <input type="number" step="any" name="wire_rod_value" value="{{ wire_rod_value if wire_rod_value is not none else '' }}" placeholder="e.g. 10155250">
    <p style="font-size:12px;color:var(--ink-soft);margin-top:-8px;">For reference, your live tracked Wire Rod quantity today is {{ wire_rod_qty }} kg - only used for comparison, not linked to what you enter above.</p>

    <h2 class="section">MS Wire</h2>
    <label>Opening Quantity as of {{ for_month }}-01 (kg)</label>
    <input type="number" step="any" name="ms_wire_qty" value="{{ ms_wire_manual_qty if ms_wire_manual_qty is not none else '' }}" placeholder="e.g. 13000">
    <label>Opening Value (Rs.)</label>
    <input type="number" step="any" name="ms_wire_value" value="{{ ms_wire_value if ms_wire_value is not none else '' }}" placeholder="e.g. 676000">
    <p style="font-size:12px;color:var(--ink-soft);margin-top:-8px;">For reference, your live tracked MS Wire quantity today is {{ ms_wire_qty }} kg - only used for comparison, not linked to what you enter above.</p>

    <p style="font-size:13px;color:var(--brand);font-weight:600;">This applies specifically to {{ for_month }} only - it will NOT carry over or reapply to any later month once that month's own rate is calculated.</p>
    <button class="submit" type="submit">Save (P&amp;L Only)</button>
  </form>
  <a href="/pnl-report?key={{ admin_key }}" style="display:block;text-align:center;margin-top:20px;color:var(--accent-dark);font-weight:700;text-decoration:none;">&larr; Back to P&amp;L Report</a>
</div>
"""


@app.route("/set-opening-value", methods=["GET", "POST"])
def set_opening_value():
    if request.args.get("key") != ADMIN_KEY and request.form.get("key") != ADMIN_KEY:
        abort(403)

    # Default to the earliest month with any real data, not "today's month" -
    # this bootstrap is meant for whichever month tracking genuinely began.
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT MIN(entry_date) FROM submissions")
    earliest = cur.fetchone()[0]
    cur.close()
    default_month = earliest.strftime("%Y-%m") if earliest else now_ist().strftime("%Y-%m")
    for_month = request.values.get("for_month") or default_month

    if request.method == "POST":
        conn = get_db_connection()
        cur = conn.cursor()
        wire_rod_qty_raw = request.form.get("wire_rod_qty", "")
        wire_rod_val_raw = request.form.get("wire_rod_value", "")
        ms_wire_qty_raw = request.form.get("ms_wire_qty", "")
        ms_wire_val_raw = request.form.get("ms_wire_value", "")

        if wire_rod_val_raw:
            cur.execute("""
                INSERT INTO pnl_opening_bootstrap (category, for_month, opening_qty, opening_value, updated_at)
                VALUES ('Raw Material', %s, %s, %s, NOW())
                ON CONFLICT (category) DO UPDATE SET for_month=EXCLUDED.for_month, opening_qty=EXCLUDED.opening_qty, opening_value=EXCLUDED.opening_value, updated_at=NOW()
            """, (for_month, safe_float(wire_rod_qty_raw) if wire_rod_qty_raw else None, safe_float(wire_rod_val_raw)))
        if ms_wire_val_raw:
            cur.execute("""
                INSERT INTO pnl_opening_bootstrap (category, for_month, opening_qty, opening_value, updated_at)
                VALUES ('Semi-Finished', %s, %s, %s, NOW())
                ON CONFLICT (category) DO UPDATE SET for_month=EXCLUDED.for_month, opening_qty=EXCLUDED.opening_qty, opening_value=EXCLUDED.opening_value, updated_at=NOW()
            """, (for_month, safe_float(ms_wire_qty_raw) if ms_wire_qty_raw else None, safe_float(ms_wire_val_raw)))
        conn.commit()
        cur.close()
        # This table is never read by any stock/cascade logic - completely
        # isolated, so nothing about live tracking is ever touched here.

    wire_rod_manual_qty, wire_rod_value = get_manual_opening_bootstrap("Raw Material", for_month)
    ms_wire_manual_qty, ms_wire_value = get_manual_opening_bootstrap("Semi-Finished", for_month)

    wire_rod_qty = round(get_category_total_balance_on_date("Raw Material", now_ist().strftime("%Y-%m-%d")), 2)
    ms_wire_qty = round(get_item_balance_on_date("Semi-Finished", "MS Wire", now_ist().strftime("%Y-%m-%d")), 2)

    return render_template_string(OPENING_VALUE_HTML, admin_key=ADMIN_KEY, wire_rod_value=wire_rod_value,
                                   ms_wire_value=ms_wire_value, wire_rod_qty=wire_rod_qty, ms_wire_qty=ms_wire_qty,
                                   wire_rod_manual_qty=wire_rod_manual_qty, ms_wire_manual_qty=ms_wire_manual_qty,
                                   for_month=for_month)


@app.route("/opening-stock", methods=["GET", "POST"])
def opening_stock_admin():
    if request.args.get("key") != ADMIN_KEY and request.form.get("key") != ADMIN_KEY:
        abort(403)

    ensure_opening_stock_seeded()
    known = get_all_known_items()

    if request.method == "POST":
        as_of_date = request.form.get("as_of_date") or now_ist().strftime("%Y-%m-%d")
        conn = get_db_connection()
        cur = conn.cursor()
        for category, items in [("Consumables", set(SEED_ITEMS["Consumables"]) | set(known["Consumables"])),
                                 ("Raw Material", set(SEED_ITEMS["Raw Material"]) | set(known["Raw Material"])),
                                 ("Semi-Finished", set(SEED_ITEMS["Semi-Finished"]) | set(known["Semi-Finished"])),
                                 ("Finished Goods", set(SEED_ITEMS["Finished Goods"]) | set(known["Finished Goods"]))]:
            for item in items:
                val = request.form.get(f"qty_{item}")
                value_raw = request.form.get(f"value_{item}", "")
                value_val = safe_float(value_raw) if value_raw else None
                if val is not None:
                    cur.execute("""
                        INSERT INTO opening_stock (item_name, category, opening_qty, opening_value, updated_at) VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (item_name) DO UPDATE SET opening_qty=EXCLUDED.opening_qty, opening_value=EXCLUDED.opening_value, updated_at=EXCLUDED.updated_at
                    """, (item, category, safe_float(val), value_val, as_of_date))
        conn.commit()
        cur.close()

        try:
            resync_opening_stock_sheet()
        except Exception as e:
            print(f"  -> Sheets mirror FAILED for OpeningStock: {e}")

        if is_date_already_closed(as_of_date):
            cascade_reclose_from_date(as_of_date)

    opening = get_opening_stock_map()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT item_name, opening_value FROM opening_stock")
    opening_values = {r[0]: float(r[1]) if r[1] is not None else None for r in cur.fetchall()}
    cur.close()
    consumables = sorted((i, opening.get(i, 0.0), None) for i in set(SEED_ITEMS["Consumables"]) | set(known["Consumables"]))
    raw_material = sorted((i, opening.get(i, 0.0), opening_values.get(i)) for i in set(SEED_ITEMS["Raw Material"]) | set(known["Raw Material"]))
    semi_finished = sorted((i, opening.get(i, 0.0), opening_values.get(i)) for i in set(SEED_ITEMS["Semi-Finished"]) | set(known["Semi-Finished"]))
    finished_goods = sorted((i, opening.get(i, 0.0), None) for i in set(SEED_ITEMS["Finished Goods"]) | set(known["Finished Goods"]))

    return render_template_string(OPENING_STOCK_HTML, admin_key=ADMIN_KEY, today=now_ist().strftime("%Y-%m-%d"),
                                   consumables=consumables, raw_material=raw_material, semi_finished=semi_finished, finished_goods=finished_goods)


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
        <a href="?key=""" + ADMIN_KEY + """&confirm=yes" style="display:inline-block;padding:14px 28px;background:#2C2C2C;color:white;border-radius:8px;text-decoration:none;font-weight:bold;">Yes, Run Migration Now</a>
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


DUPLICATE_CHECK_HTML = BASE_STYLE + """
<div class="card wide">
  <h1>Check for Duplicate Entries</h1>
  <p class="subtitle">Groups below share the same operator, form, and date - review each one and decide for yourself; nothing is deleted automatically, since some could be genuinely separate entries.</p>

  {% if not groups %}
  <div class="alert-panel all-clear"><div class="alert-title">&#9989; No matching groups found for this date range</div></div>
  {% endif %}

  {% for g in groups %}
  <h2 class="section">{{ g.form_type }} &mdash; {{ g.operator }} &mdash; {{ g.entry_date }} ({{ g.submissions|length }} submissions)</h2>
  {% for sub in g.submissions %}
  <div style="border:1px solid var(--line);border-radius:8px;padding:12px 16px;margin-bottom:10px;">
    <div style="font-size:13px;color:var(--ink-soft);margin-bottom:6px;">Submitted at {{ sub.entry_time }} &mdash; batch {{ sub.batch_id[:8] }}</div>
    <table style="margin-top:0;">
      <tr><th>Item</th><th>Qty</th></tr>
      {% for item in sub.line_items %}<tr><td>{{ item.item_name }}</td><td>{{ item.quantity }}</td></tr>{% endfor %}
    </table>
    <div style="margin-top:8px;">
      <a href="/delete-submission?batch_id={{ sub.batch_id }}&key={{ admin_key }}" style="color:var(--bad);font-weight:700;">Delete This Whole Entry</a>
    </div>
  </div>
  {% endfor %}
  {% endfor %}

  <form method="GET" style="margin-top:20px;">
    <input type="hidden" name="key" value="{{ admin_key }}">
    <label>From Date</label>
    <input type="date" name="from_date" value="{{ from_date }}">
    <label>To Date</label>
    <input type="date" name="to_date" value="{{ to_date }}">
    <button class="submit" type="submit">Check This Range</button>
  </form>
</div>
"""


@app.route("/check-duplicates", methods=["GET"])
def check_duplicates():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)

    today = now_ist().strftime("%Y-%m-%d")
    from_date = request.args.get("from_date") or (now_ist() - timedelta(days=14)).strftime("%Y-%m-%d")
    to_date = request.args.get("to_date") or today

    conn = get_db_connection()
    cur = conn.cursor()
    # Same operator, same form, same date, more than one submission - a
    # candidate worth a human look, not proof of an actual duplicate.
    cur.execute("""
        SELECT form_type, operator, entry_date, COUNT(*) as cnt
        FROM submissions
        WHERE entry_date BETWEEN %s AND %s
        GROUP BY form_type, operator, entry_date
        HAVING COUNT(*) > 1
        ORDER BY entry_date DESC, form_type
    """, (from_date, to_date))
    candidate_groups = cur.fetchall()

    groups = []
    for form_type, operator, entry_date, cnt in candidate_groups:
        cur.execute("""
            SELECT batch_id, entry_time FROM submissions
            WHERE form_type=%s AND operator=%s AND entry_date=%s
            ORDER BY entry_time
        """, (form_type, operator, entry_date))
        subs = []
        for batch_id, entry_time in cur.fetchall():
            cur.execute("SELECT item_name, quantity FROM line_items WHERE batch_id=%s", (batch_id,))
            items = [{"item_name": r[0], "quantity": float(r[1])} for r in cur.fetchall()]
            subs.append({"batch_id": str(batch_id), "entry_time": str(entry_time), "line_items": items})
        groups.append({"form_type": form_type, "operator": operator, "entry_date": entry_date, "submissions": subs})
    cur.close()

    return render_template_string(DUPLICATE_CHECK_HTML, admin_key=ADMIN_KEY, groups=groups, from_date=from_date, to_date=to_date)


@app.route("/shift-entry-dates", methods=["GET"])
def shift_entry_dates():
    """Corrects the previous-day reporting issue - shifts Production,
    Consumption, and Electricity & Wire Rod entries back by one day each,
    for a given date range. Furnace, Receipts, and Sales are untouched, since
    those are genuinely entered same-day. Preview-then-confirm, same safe
    pattern as the other correction tools."""
    if request.args.get("key") != ADMIN_KEY:
        abort(403)

    from_date = request.args.get("from_date", "")
    to_date = request.args.get("to_date", "")
    if not from_date or not to_date:
        return """
        <div style="font-family:sans-serif;max-width:600px;margin:40px auto;">
        <h2>Shift Entry Dates</h2>
        <p>Corrects Production, Consumption, and Electricity &amp; Wire Rod entries
        that were dated as "today" instead of the shift day they actually
        report - shifts each one back by exactly one day. Furnace, Receipts,
        and Sales are never touched.</p>
        <form method="GET">
          <input type="hidden" name="key" value="%s">
          <label>From Date</label><br>
          <input type="date" name="from_date" required><br><br>
          <label>To Date</label><br>
          <input type="date" name="to_date" required><br><br>
          <button type="submit" style="padding:12px 24px;background:#1B3A5C;color:white;border:none;border-radius:8px;">Preview</button>
        </form>
        </div>
        """ % ADMIN_KEY

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT batch_id, form_type, entry_date, operator FROM submissions
        WHERE form_type IN ('production','consumption','electricity_wire_rod')
              AND entry_date BETWEEN %s AND %s
        ORDER BY entry_date, form_type
    """, (from_date, to_date))
    rows = cur.fetchall()
    cur.close()

    if request.args.get("confirm") != "yes":
        preview_rows = "".join(
            f"<li>{r[1]} by {r[3]} &mdash; {r[2]} &rarr; {r[2] - timedelta(days=1)}</li>" for r in rows
        )
        return f"""
        <div style="font-family:sans-serif;max-width:600px;margin:40px auto;">
        <h2>Shift Entry Dates - Preview</h2>
        <p>{len(rows)} submissions will be shifted back by one day each:</p>
        <ul>{preview_rows}</ul>
        <p>Furnace, Receipts, and Sales in this range are NOT affected.</p>
        <a href="?key={ADMIN_KEY}&from_date={from_date}&to_date={to_date}&confirm=yes"
           style="display:inline-block;padding:14px 28px;background:#1B3A5C;color:white;border-radius:8px;text-decoration:none;font-weight:bold;">
           Yes, Shift These {len(rows)} Entries Back One Day</a>
        </div>
        """

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE submissions SET entry_date = entry_date - INTERVAL '1 day'
        WHERE form_type IN ('production','consumption','electricity_wire_rod')
              AND entry_date BETWEEN %s AND %s
    """, (from_date, to_date))
    conn.commit()
    cur.close()

    earliest_affected = (datetime.strptime(from_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    cascade_reclose_from_date(earliest_affected)

    return f"""
    <div style="font-family:sans-serif;max-width:600px;margin:40px auto;">
    <h2>Done</h2>
    <p>{len(rows)} entries shifted back one day. Stock recalculated from {earliest_affected} forward.</p>
    <a href="/dashboard?key={ADMIN_KEY}">Go to Dashboard</a>
    </div>
    """


@app.route("/fix-wire-rod-receipts", methods=["GET"])
def fix_wire_rod_receipts():
    """Separate, one-time correction for Wire Rod RECEIPTS specifically - the
    original /fix-wire-rod-units tool only corrected Wire Rod ISSUED (and its
    derived MS Wire), which was already run once. Receipts never got the same
    fix, so this is scoped to touch ONLY receipt_raw_material entries for
    actual Wire Rod items - never re-touching what's already been corrected."""
    if request.args.get("key") != ADMIN_KEY:
        abort(403)

    known_items = get_all_known_items()
    wire_rod_items = sorted(set(SEED_ITEMS.get("Raw Material", [])) | set(known_items.get("Raw Material", [])))

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, item_name, quantity FROM line_items
        WHERE category='receipt_raw_material' AND item_name = ANY(%s)
        ORDER BY id
    """, (wire_rod_items,))
    receipt_rows = cur.fetchall()
    cur.close()

    if request.args.get("confirm") != "yes":
        preview_html = """
        <div style="font-family:sans-serif;max-width:600px;margin:40px auto;">
        <h2>Fix Wire Rod Receipts (MT &rarr; Kg correction)</h2>
        <p>This multiplies every historical Wire Rod RECEIPT quantity by 1000 -
        separate from the earlier correction, which only covered Wire Rod Issued.
        Only run this ONCE.</p>
        <h3>Wire Rod receipt entries to be corrected: %d</h3>
        <ul>%s</ul>
        <a href="?key=%s&confirm=yes" style="display:inline-block;padding:14px 28px;background:#2C2C2C;color:white;border-radius:8px;text-decoration:none;font-weight:bold;">Yes, Apply &times;1000 Correction Now</a>
        </div>
        """ % (
            len(receipt_rows),
            "".join(f"<li>{r[1]}: {r[2]} &rarr; {round(float(r[2]) * 1000, 2)}</li>" for r in receipt_rows),
            ADMIN_KEY,
        )
        return preview_html

    conn = get_db_connection()
    for row_id, item_name, qty in receipt_rows:
        cur = conn.cursor()
        cur.execute("UPDATE line_items SET quantity=%s WHERE id=%s", (round(float(qty) * 1000, 2), row_id))
        cur.close()
    conn.commit()

    cur = conn.cursor()
    cur.execute("SELECT MIN(entry_date) FROM stock_ledger")
    earliest = cur.fetchone()[0]
    cur.close()
    if earliest:
        cascade_reclose_from_date(earliest.strftime("%Y-%m-%d"))

    return f"""
    <div style="font-family:sans-serif;max-width:600px;margin:40px auto;">
    <h2>Correction Applied</h2>
    <p>{len(receipt_rows)} Wire Rod receipt entries corrected (&times;1000).</p>
    <p>StockLedger and StockHistory have been recalculated from {earliest} forward.</p>
    <a href="/dashboard?key={ADMIN_KEY}">Go to Dashboard</a>
    </div>
    """


@app.route("/fix-wire-rod-units", methods=["GET"])
def fix_wire_rod_units():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, item_name, quantity FROM line_items WHERE category='wire_rod' ORDER BY id")
    wire_rod_rows = cur.fetchall()
    cur.execute("SELECT id, item_name, quantity FROM line_items WHERE category='ms_wire_produced' ORDER BY id")
    ms_wire_rows = cur.fetchall()
    cur.execute("SELECT item_name, opening_qty FROM opening_stock WHERE category='Raw Material' ORDER BY item_name")
    opening_rows = cur.fetchall()
    cur.close()

    if request.args.get("confirm") != "yes":
        preview_html = """
        <div style="font-family:sans-serif;max-width:600px;margin:40px auto;">
        <h2>Fix Wire Rod Units (MT &rarr; Kg correction)</h2>
        <p>This multiplies every historical Wire Rod, MS Wire Produced, and Raw Material
        Opening Stock value by 1000, correcting entries made before the unit conversion
        was implemented. Only run this ONCE.</p>
        <h3>Wire Rod entries to be corrected: %d</h3>
        <h3>MS Wire Produced entries to be corrected: %d</h3>
        <h3>Opening Stock (Raw Material) items to be corrected:</h3>
        <ul>%s</ul>
        <a href="?key=%s&confirm=yes" style="display:inline-block;padding:14px 28px;background:#2C2C2C;color:white;border-radius:8px;text-decoration:none;font-weight:bold;">Yes, Apply &times;1000 Correction Now</a>
        </div>
        """ % (
            len(wire_rod_rows), len(ms_wire_rows),
            "".join(f"<li>{r[0]}: {r[1]} &rarr; {round(r[1] * 1000, 2)}</li>" for r in opening_rows),
            ADMIN_KEY,
        )
        return preview_html

    for row_id, item_name, qty in wire_rod_rows:
        cur = conn.cursor()
        cur.execute("UPDATE line_items SET quantity=%s WHERE id=%s", (round(float(qty) * 1000, 2), row_id))
        cur.close()
    for row_id, item_name, qty in ms_wire_rows:
        cur = conn.cursor()
        cur.execute("UPDATE line_items SET quantity=%s WHERE id=%s", (round(float(qty) * 1000, 2), row_id))
        cur.close()
    for item_name, qty in opening_rows:
        cur = conn.cursor()
        cur.execute("UPDATE opening_stock SET opening_qty=%s WHERE item_name=%s AND category='Raw Material'",
                     (round(float(qty) * 1000, 2), item_name))
        cur.close()
    conn.commit()

    # Recompute every closed day from the earliest one, so StockLedger/StockHistory
    # reflect the corrected values throughout the whole chain.
    cur = conn.cursor()
    cur.execute("SELECT MIN(entry_date) FROM stock_ledger")
    earliest = cur.fetchone()[0]
    cur.close()

    if earliest:
        cascade_reclose_from_date(earliest.strftime("%Y-%m-%d"))

    return f"""
    <div style="font-family:sans-serif;max-width:600px;margin:40px auto;">
    <h2>Correction Applied</h2>
    <p>{len(wire_rod_rows)} Wire Rod entries, {len(ms_wire_rows)} MS Wire Produced entries,
    and {len(opening_rows)} Opening Stock items corrected (&times;1000).</p>
    <p>StockLedger and StockHistory have been recalculated from {earliest} forward.</p>
    <a href="/dashboard?key={ADMIN_KEY}">Go to Dashboard</a>
    </div>
    """


@app.route("/test-daily-close", methods=["GET"])
def test_daily_close():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    target_date = request.args.get("date")  # optional: YYYY-MM-DD to backfill a specific day
    run_daily_stock_close(target_date=target_date)
    closed = target_date or "yesterday"
    return f"Daily stock close run manually for {closed}. Check the StockLedger/StockHistory tabs."


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
