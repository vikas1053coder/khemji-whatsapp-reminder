"""
Khemji Wire - Reminder + Mobile Entry Forms + Admin Dashboard + Inventory
Runs 24/7 on Render. Twilio WhatsApp for reminders (link-based), Google
Sheets for logging with separate Date/Time columns and dynamically-growing
item columns (any new item typed in gets its own titled column automatically).
"""

import os
import time
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
from flask import Flask, request, render_template_string, abort, g
from twilio.rest import Client
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import gspread
from google.oauth2.service_account import Credentials

IST = ZoneInfo("Asia/Kolkata")


def now_ist():
    return datetime.now(IST)


def default_entry_time():
    return now_ist().strftime("%Y-%m-%dT%H:%M")


def parse_entry_datetime(form):
    """Returns (date_str, time_str) using the operator-supplied date/time if given,
    otherwise falls back to the current server time (IST)."""
    raw = form.get("entry_time", "").strip()
    if raw:
        try:
            dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M")
            return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
        except ValueError:
            pass
    n = now_ist()
    return n.strftime("%Y-%m-%d"), n.strftime("%H:%M:%S")


# ============ CONFIG ============
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "your_account_sid_here")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "your_auth_token_here")
TWILIO_WHATSAPP_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")

APP_BASE_URL = os.environ.get("APP_BASE_URL", "https://khemji-whatsapp-reminder.onrender.com")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "changeme123")

ALL_PEOPLE = {
    "Vikas": "whatsapp:+919402168373",
    "Prakash": "whatsapp:+919829945873",
    "Subodh": "whatsapp:+918302822703",
    "Rana": "whatsapp:+919667807024",
    "Mahesh": "whatsapp:+919829277869",
    "Monu": "whatsapp:+919983709386",
}

# ---- 1. FURNACE reminder ----
FURNACE_OPERATOR_NAMES = ["Vikas", "Prakash", "Subodh"]
FURNACE_REMINDER_TIMES = ["08:00", "20:00"]
TEMP_RANGES = {
    "T1": {"low": 445, "high": 455},
}

# ---- 2. PRODUCTION & CONSUMPTION reminder (one form, two sheet tabs) ----
PROD_CONSUMPTION_OPERATOR_NAMES = ["Subodh", "Prakash"]
PROD_CONSUMPTION_REMINDER_TIMES = ["09:00"]

CONSUMPTION_ITEMS = ["Zinc", "FO", "Lead", "Galva Flux", "Coal", "Charcoal"]
PRODUCTION_ITEMS = ["1.25 mm", "1.40 mm", "1.60 mm", "1.60 mm S", "1.80 mm", "2.00 mm", "2.25 mm", "2.50 mm", "4.00 mm", "Strip 16 Kg", "Strip 23 KG"]

# ---- 3. ELECTRICITY & WIRE ROD reminder ----
ELECTRICITY_OPERATOR_NAMES = ["Rana", "Prakash"]
ELECTRICITY_REMINDER_TIMES = ["10:00"]
WIRE_ROD_SIZES = ["5.5 mm", "6.00 mm"]

# ---- 4. STOCK: Receipts (anyone can log) + Sales (finished goods, anyone incl. Monu) ----
# "Consumables" = furnace/production inputs consumed daily. "Raw Material" = wire rod, the
# core input converted into finished wire.
RECEIPT_CATEGORIES = {
    "Consumables": CONSUMPTION_ITEMS,
    "Raw Material": WIRE_ROD_SIZES,
}
FINISHED_GOODS_ITEMS = PRODUCTION_ITEMS

ADMIN_NUMBERS = [ALL_PEOPLE["Vikas"], ALL_PEOPLE["Mahesh"]]

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "your_google_sheet_id_here")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

TAB_READINGS = "Readings"
TAB_PRODUCTION = "Production"
TAB_CONSUMPTION = "Consumption"
TAB_ELECTRICITY_WIRE_ROD = "ElectricityWireRod"
TAB_RECEIPTS = "Receipts"
TAB_SALES = "Sales"
TAB_OPENING_STOCK = "OpeningStock"
TAB_STOCK_LEDGER = "StockLedger"
TAB_STOCK_HISTORY = "StockHistory"
STOCK_LEDGER_HEADER = ["Date", "Category", "Item", "Opening", "In", "Out", "Closing"]
# ==================================================

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
app = Flask(__name__)


# ---------- Google Sheets core helpers ----------
_GSPREAD_CLIENT = None  # cached across the whole process - auth doesn't change


def get_gspread_client():
    global _GSPREAD_CLIENT
    if _GSPREAD_CLIENT is None:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        _GSPREAD_CLIENT = gspread.authorize(creds)
    return _GSPREAD_CLIENT


def get_spreadsheet():
    """Opens the spreadsheet ONCE per request (cached on `g`) instead of re-fetching
    its metadata every time a tab is read - this was doubling our API call count."""
    if hasattr(g, "_spreadsheet_obj"):
        return g._spreadsheet_obj
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)
    g._spreadsheet_obj = ss
    return ss


def with_retries(fn, *args, retries=3, delay=1.2, **kwargs):
    """Retries a Google Sheets call a few times before giving up, since transient
    rate-limit or timeout errors from Google's API are common and usually pass
    within a second or two."""
    last_exc = None
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            print(f"  -> Sheets call failed (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise last_exc


def get_or_create_tab(tab_name, header_row):
    ss = get_spreadsheet()
    try:
        ws = ss.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=tab_name, rows=2000, cols=max(len(header_row) + 5, 15))
        ws.append_row(header_row)
    return ws


def ensure_columns(ws, extra_headers):
    """Ensures row 1 includes every name in extra_headers, appending new columns
    (with that exact title) if they don't exist yet. Returns the up-to-date header list."""
    header = ws.row_values(1)
    changed = False
    for h in extra_headers:
        if h and h not in header:
            header.append(h)
            changed = True
    if changed:
        ws.update('A1', [header])
    return header


def append_named_row(tab_name, base_header, values_dict):
    """values_dict maps COLUMN NAME -> value. Any name not already a column gets
    its own new titled column automatically. Row is aligned to the header order."""
    ws = get_or_create_tab(tab_name, base_header)
    header = ensure_columns(ws, list(values_dict.keys()))
    row = [values_dict.get(col, "") for col in header]
    ws.append_row(row)


def get_all_rows_with_header(tab_name, base_header):
    """Reads a tab's full data ONCE per request (cached on Flask's request-scoped `g`),
    since many stock/report calculations need the same tab repeatedly."""
    if not hasattr(g, "_sheet_cache"):
        g._sheet_cache = {}
    if tab_name in g._sheet_cache:
        return g._sheet_cache[tab_name]

    def _fetch():
        ws = get_or_create_tab(tab_name, base_header)
        return ws.get_all_values()

    all_values = with_retries(_fetch)
    result = (base_header, []) if not all_values else (all_values[0], all_values[1:])
    g._sheet_cache[tab_name] = result
    return result


def get_recent_rows(tab_name, base_header, limit=12, operator_filter=None):
    """Returns (header, [(sheet_row_number, row_values), ...]) newest first.
    If operator_filter is given, searches the FULL history for that operator's
    rows (not just the most recent ones) before taking the last `limit`."""
    header, rows = get_all_rows_with_header(tab_name, base_header)
    if not rows:
        return header, []
    indexed = [(i + 2, rows[i]) for i in range(len(rows))]  # row 1 is header
    if operator_filter and "Operator" in header:
        idx = header.index("Operator")
        indexed = [(rn, r) for rn, r in indexed if len(r) > idx and r[idx] == operator_filter]
    return header, list(reversed(indexed[-limit:]))


def safe_float(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def sum_column_by_name(tab_name, base_header, column_names):
    """Looks up each column by its header NAME (robust to dynamically-added
    columns / changing order) and sums it across every row."""
    header, rows = get_all_rows_with_header(tab_name, base_header)
    totals = {name: 0.0 for name in column_names}
    for name in column_names:
        if name in header:
            idx = header.index(name)
            for row in rows:
                if idx < len(row):
                    totals[name] += safe_float(row[idx])
    return totals


# ---------- Stock ----------
def get_opening_stock_map():
    header = ["Item", "Category", "Opening Qty"]
    ws = get_or_create_tab(TAB_OPENING_STOCK, header)
    all_values = ws.get_all_values()

    if len(all_values) <= 1:
        today = now_ist().strftime("%Y-%m-%d")
        seed_rows = []
        for item in CONSUMPTION_ITEMS:
            seed_rows.append([item, "Consumables", 0])
        for size in WIRE_ROD_SIZES:
            seed_rows.append([size, "Raw Material", 0])
        for item in FINISHED_GOODS_ITEMS:
            seed_rows.append([item, "Finished Goods", 0])
        for row in seed_rows:
            ws.append_row(row)
        try:
            ws.update_acell('E1', 'As Of Date')
            ws.update_acell('E2', today)
        except Exception as e:
            print(f"  -> Could not set OpeningStock date note: {e}")
        all_values = ws.get_all_values()

    result = {}
    for row in all_values[1:]:
        if len(row) >= 3 and row[0]:
            result[row[0]] = safe_float(row[2])
    return result


def sum_receipts_by_item(category):
    header = ["Date", "Time", "Operator", "Category", "Item", "Quantity"]
    _, rows = get_all_rows_with_header(TAB_RECEIPTS, header)
    totals = {}
    for row in rows:
        if len(row) >= 6 and row[3] == category:
            item = row[4]
            totals[item] = totals.get(item, 0.0) + safe_float(row[5])
    return totals


def sum_receipts_by_item_for_date(category, date_str):
    header = ["Date", "Time", "Operator", "Category", "Item", "Quantity"]
    _, rows = get_all_rows_with_header(TAB_RECEIPTS, header)
    totals = {}
    for row in rows:
        if len(row) >= 6 and row[0] == date_str and row[3] == category:
            item = row[4]
            totals[item] = totals.get(item, 0.0) + safe_float(row[5])
    return totals


def sum_column_by_name_for_date(tab_name, base_header, column_name, date_str):
    header, rows = get_all_rows_with_header(tab_name, base_header)
    if column_name not in header or "Date" not in header:
        return 0.0
    col_idx = header.index(column_name)
    date_idx = header.index("Date")
    total = 0.0
    for row in rows:
        if len(row) > max(col_idx, date_idx) and row[date_idx] == date_str:
            total += safe_float(row[col_idx])
    return round(total, 2)


def get_sales_summary_for_date(date_str):
    """Returns ({item: qty_sold}, total_qty, total_revenue) for one date."""
    header = ["Date", "Time", "Operator", "Item", "Quantity", "Price (Rs/Kg)", "Total Amount (Rs)", "Customer"]
    _, rows = get_all_rows_with_header(TAB_SALES, header)
    by_item = {}
    total_qty, total_revenue = 0.0, 0.0
    for row in rows:
        if len(row) >= 7 and row[0] == date_str and row[3]:
            item, qty, revenue = row[3], safe_float(row[4]), safe_float(row[6])
            by_item[item] = by_item.get(item, 0.0) + qty
            total_qty += qty
            total_revenue += revenue
    return by_item, round(total_qty, 2), round(total_revenue, 2)


def get_sales_rows_for_date(date_str):
    header, rows = get_all_rows_with_header(TAB_SALES, ["Date", "Time", "Operator", "Item", "Quantity", "Price (Rs/Kg)", "Total Amount (Rs)", "Customer"])
    matched = [(i + 2, row) for i, row in enumerate(rows) if row and row[0] == date_str]
    return header, list(reversed(matched))


CATEGORY_ITEMS = {
    "Consumables": CONSUMPTION_ITEMS,
    "Raw Material": WIRE_ROD_SIZES,
    "Finished Goods": FINISHED_GOODS_ITEMS,
}


def sum_column_by_name_for_month(tab_name, base_header, column_name, month_str):
    header, rows = get_all_rows_with_header(tab_name, base_header)
    if column_name not in header or "Date" not in header:
        return 0.0
    col_idx = header.index(column_name)
    date_idx = header.index("Date")
    total = 0.0
    for row in rows:
        if len(row) > max(col_idx, date_idx) and row[date_idx].startswith(month_str):
            total += safe_float(row[col_idx])
    return round(total, 2)


def sum_receipts_by_item_for_month(category, item, month_str):
    header = ["Date", "Time", "Operator", "Category", "Item", "Quantity"]
    _, rows = get_all_rows_with_header(TAB_RECEIPTS, header)
    total = 0.0
    for row in rows:
        if len(row) >= 6 and row[0].startswith(month_str) and row[3] == category and row[4] == item:
            total += safe_float(row[5])
    return round(total, 2)


def sum_sales_for_item_month(item, month_str):
    header = ["Date", "Time", "Operator", "Item", "Quantity", "Price (Rs/Kg)", "Total Amount (Rs)", "Customer"]
    _, rows = get_all_rows_with_header(TAB_SALES, header)
    qty, revenue = 0.0, 0.0
    for row in rows:
        if len(row) >= 7 and row[0].startswith(month_str) and row[3] == item:
            qty += safe_float(row[4])
            revenue += safe_float(row[6])
    return round(qty, 2), round(revenue, 2)


def get_monthly_item_report(item, month_str, include_sales):
    category = None
    for cat, items in CATEGORY_ITEMS.items():
        if item in items:
            category = cat
            break
    if not category:
        return None

    report = {"item": item, "category": category, "month": month_str}
    if category == "Consumables":
        report["consumption"] = sum_column_by_name_for_month(TAB_CONSUMPTION, ["Date", "Time", "Operator"] + CONSUMPTION_ITEMS, item, month_str)
        report["receipts"] = sum_receipts_by_item_for_month("Consumables", item, month_str)
    elif category == "Raw Material":
        report["issued"] = sum_column_by_name_for_month(TAB_ELECTRICITY_WIRE_ROD, ["Date", "Time", "Operator", "Electricity Units"] + WIRE_ROD_SIZES, item, month_str)
        report["receipts"] = sum_receipts_by_item_for_month("Raw Material", item, month_str)
    else:
        report["production"] = sum_column_by_name_for_month(TAB_PRODUCTION, ["Date", "Time", "Operator"] + PRODUCTION_ITEMS + ["Total Production"], item, month_str)
        if include_sales:
            qty, revenue = sum_sales_for_item_month(item, month_str)
            report["sales_qty"] = qty
            report["sales_revenue"] = revenue
    return report


def filter_rows_by_operator(header, rows, operator_name):
    """rows is a list of (sheet_row_number, row_values). Filters to only that operator's entries."""
    if not operator_name or "Operator" not in header:
        return rows
    idx = header.index("Operator")
    return [(rn, r) for rn, r in rows if len(r) > idx and r[idx] == operator_name]


def compute_in_out_for_date(category, item, date_str):
    if category == "Consumables":
        in_amt = sum_receipts_by_item_for_date("Consumables", date_str).get(item, 0.0)
        out_amt = sum_column_by_name_for_date(TAB_CONSUMPTION, ["Date", "Time", "Operator"] + CONSUMPTION_ITEMS, item, date_str)
    elif category == "Raw Material":
        in_amt = sum_receipts_by_item_for_date("Raw Material", date_str).get(item, 0.0)
        out_amt = sum_column_by_name_for_date(TAB_ELECTRICITY_WIRE_ROD, ["Date", "Time", "Operator", "Electricity Units"] + WIRE_ROD_SIZES, item, date_str)
    else:
        in_amt = sum_column_by_name_for_date(TAB_PRODUCTION, ["Date", "Time", "Operator"] + PRODUCTION_ITEMS + ["Total Production"], item, date_str)
        sales_map, _, _ = get_sales_summary_for_date(date_str)
        out_amt = sales_map.get(item, 0.0)
    return round(in_amt, 2), round(out_amt, 2)


def get_latest_closing_map(before_date):
    """Latest StockLedger closing per item, strictly before `before_date`."""
    header, rows = get_all_rows_with_header(TAB_STOCK_LEDGER, STOCK_LEDGER_HEADER)
    latest = {}
    for row in rows:
        if len(row) < 7:
            continue
        date_str, category, item, opening, in_amt, out_amt, closing = row[:7]
        if date_str >= before_date:
            continue
        if item not in latest or date_str > latest[item][0]:
            latest[item] = (date_str, safe_float(closing))
    return latest


def get_ledger_rows_for_date(date_str):
    """{item: {opening,in,out,balance}} for every item already closed on this exact date."""
    header, rows = get_all_rows_with_header(TAB_STOCK_LEDGER, STOCK_LEDGER_HEADER)
    result = {}
    for row in rows:
        if len(row) >= 7 and row[0] == date_str:
            result[row[2]] = {"opening": safe_float(row[3]), "in": safe_float(row[4]), "out": safe_float(row[5]), "balance": safe_float(row[6])}
    return result


def run_daily_stock_close():
    """Runs at 00:00 IST. Closes out the day that just ended (yesterday's date) for every item.
    Writes two things:
      - StockLedger: detailed row per item per day (Opening/In/Out/Closing) - source of truth
      - StockHistory: one row per DATE, one column per item, closing balance only - easy to scan
    """
    yesterday = (now_ist() - timedelta(days=1)).strftime("%Y-%m-%d")
    opening_baseline = get_opening_stock_map()
    latest_closing = get_latest_closing_map(before_date=yesterday)

    history_row = {"Date": yesterday}
    all_item_names = []

    for category, items in CATEGORY_ITEMS.items():
        for item in items:
            opening = latest_closing.get(item, (None, opening_baseline.get(item, 0.0)))[1]
            in_amt, out_amt = compute_in_out_for_date(category, item, yesterday)
            closing = round(opening + in_amt - out_amt, 2)
            try:
                log_row_simple(TAB_STOCK_LEDGER, STOCK_LEDGER_HEADER, [yesterday, category, item, opening, in_amt, out_amt, closing])
            except Exception as e:
                print(f"  -> FAILED to close stock for {item} on {yesterday}: {e}")
            history_row[item] = closing
            all_item_names.append(item)

    try:
        append_named_row(TAB_STOCK_HISTORY, ["Date"] + all_item_names, history_row)
    except Exception as e:
        print(f"  -> FAILED to write StockHistory row for {yesterday}: {e}")

    print(f"[{now_ist()}] Daily stock close completed for {yesterday}")


def compute_stock(selected_date=None):
    """Returns (consumables_stock, raw_material_stock, finished_goods_stock, totals) for a date.
    TODAY: live balance = last closed balance + today's in/out so far.
    PAST DATE already closed by the midnight job: returns that exact closed snapshot.
    PAST DATE never closed: returns zeros (no data) rather than guessing.
    """
    today = now_ist().strftime("%Y-%m-%d")
    date_str = selected_date or today
    is_today = (date_str == today)

    opening_baseline = get_opening_stock_map()
    ledger_rows_for_date = {} if is_today else get_ledger_rows_for_date(date_str)
    latest_closing = get_latest_closing_map(before_date=date_str) if is_today else {}

    def build_category(category, items):
        rows, total = [], 0.0
        for item in items:
            if item in ledger_rows_for_date:
                r = ledger_rows_for_date[item]
                op, in_amt, out_amt, bal = r["opening"], r["in"], r["out"], r["balance"]
            elif is_today:
                op = latest_closing.get(item, (None, opening_baseline.get(item, 0.0)))[1]
                in_amt, out_amt = compute_in_out_for_date(category, item, date_str)
                bal = round(op + in_amt - out_amt, 2)
            else:
                op = in_amt = out_amt = bal = 0.0
            rows.append({"item": item, "opening": op, "in": in_amt, "out": out_amt, "balance": bal})
            total += bal
        return rows, round(total, 2)

    consumables_stock, consumables_total = build_category("Consumables", CONSUMPTION_ITEMS)
    raw_material_stock, raw_material_total = build_category("Raw Material", WIRE_ROD_SIZES)
    finished_goods_stock, finished_goods_total = build_category("Finished Goods", FINISHED_GOODS_ITEMS)

    totals = {"consumables": consumables_total, "raw_material": raw_material_total, "finished_goods": finished_goods_total}
    return consumables_stock, raw_material_stock, finished_goods_stock, totals


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


def notify_admins(message):
    for admin in ADMIN_NUMBERS:
        try:
            client.messages.create(from_=TWILIO_WHATSAPP_NUMBER, to=admin, body=message)
        except Exception as e:
            print(f"  -> Could not notify admin {admin}: {e}")


def send_to_group(operator_names, label, link_builder):
    print(f"[{now_ist()}] Sending {label} reminders...")
    for name in operator_names:
        number = ALL_PEOPLE.get(name)
        if not number:
            continue
        link = link_builder(name)
        body = f"{label.upper()}\nTap the link below to enter your reading:\n{link}"
        try:
            client.messages.create(from_=TWILIO_WHATSAPP_NUMBER, to=number, body=body)
            print(f"  -> Sent to {name} ({number})")
        except Exception as e:
            print(f"  -> FAILED for {name}: {e}")


def send_furnace_reminders():
    send_to_group(FURNACE_OPERATOR_NAMES, "Furnace Temperature Log",
                  lambda name: f"{APP_BASE_URL}/furnace-form?operator={quote(name)}")


def send_prod_consumption_reminders():
    send_to_group(PROD_CONSUMPTION_OPERATOR_NAMES, "Production & Consumption Log",
                  lambda name: f"{APP_BASE_URL}/production-form?operator={quote(name)}")


def send_electricity_reminders():
    send_to_group(ELECTRICITY_OPERATOR_NAMES, "Electricity & Wire Rod Log",
                  lambda name: f"{APP_BASE_URL}/electricity-form?operator={quote(name)}")


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
    color: var(--ink);
    font-family: 'Barlow Condensed', 'Segoe UI', Arial, sans-serif;
    min-height: 100vh; display: flex; flex-direction: column; align-items: center;
    padding: 24px 16px 60px;
  }
  .card {
    background: var(--card); border: 1px solid var(--line); border-radius: 10px;
    max-width: 480px; width: 100%; padding: 28px 24px; box-shadow: 0 4px 16px rgba(27,58,92,0.08);
  }
  .card.wide { max-width: 1080px; }
  h1 {
    font-family: 'Barlow Condensed', sans-serif; font-weight: 700; font-size: 24px;
    letter-spacing: 0.02em; text-transform: uppercase; color: var(--ink); margin: 0 0 4px;
    border-left: 5px solid var(--accent); padding-left: 12px;
  }
  h2.section {
    font-family: 'Barlow Condensed', sans-serif; font-weight: 700; font-size: 16px;
    letter-spacing: 0.04em; text-transform: uppercase; color: var(--accent-dark);
    margin: 26px 0 4px; border-bottom: 2px solid var(--line); padding-bottom: 6px;
  }
  .subtitle { color: var(--ink-soft); font-size: 16px; margin: 0 0 18px; padding-left: 17px; }
  label {
    display: block; font-size: 14px; font-weight: 600; color: var(--ink-soft);
    text-transform: uppercase; letter-spacing: 0.03em; margin: 14px 0 5px;
  }
  input[type=number], input[type=text], input[type=datetime-local], select {
    width: 100%; padding: 12px 12px; font-size: 18px; border: 1.5px solid var(--line);
    border-radius: 4px; background: var(--paper); color: var(--ink);
  }
  input:focus, select:focus { outline: 2px solid var(--accent); outline-offset: 1px; border-color: var(--accent); }
  .toggle-group { display: flex; gap: 10px; }
  .toggle-btn {
    flex: 1; padding: 14px 0; text-align: center; font-size: 17px; font-weight: 700;
    border: 1.5px solid var(--line); border-radius: 4px; background: var(--paper); cursor: pointer; user-select: none;
  }
  .toggle-btn.selected { background: var(--accent); border-color: var(--accent-dark); color: white; }
  button.submit, button.secondary {
    width: 100%; margin-top: 26px; padding: 15px 0; font-size: 17px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.04em; background: var(--accent); color: white;
    border: none; border-radius: 8px; cursor: pointer; box-shadow: 0 2px 6px rgba(27,58,92,0.18);
  }
  button.secondary { background: var(--ink); margin-top: 12px; }
  button.add-item {
    width: 100%; margin-top: 10px; padding: 10px 0; font-size: 14px; font-weight: 700;
    text-transform: uppercase; background: transparent; color: var(--accent-dark);
    border: 1.5px dashed var(--accent); border-radius: 4px; cursor: pointer;
  }
  button.submit:active, button.secondary:active { opacity: 0.85; }
  .op-name { color: var(--accent-dark); font-weight: 700; }
  .success h1 { border-left-color: var(--ok); }
  .success .icon { font-size: 44px; margin-bottom: 6px; }
  .error-box { color: var(--bad); font-weight: 600; margin-top: 12px; font-size: 14px; }
  .extra-row { display: flex; gap: 8px; margin-top: 8px; flex-wrap: wrap; }
  .extra-row select, .extra-row input { flex: 1; min-width: 90px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px; }
  th, td { text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--line); }
  th { text-transform: uppercase; letter-spacing: 0.03em; color: var(--ink-soft); font-size: 11px; background: var(--paper); }
  tr:hover td { background: #FBF8F3; }
  .badge-ok { color: var(--ok); font-weight: 700; }
  .badge-bad { color: var(--bad); font-weight: 700; }
  .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin: 16px 0 6px; }
  .stat-card {
    background: var(--paper); border: 1px solid var(--line); border-radius: 6px; padding: 14px 16px;
  }
  .stat-card .label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.03em; color: var(--ink-soft); font-weight: 700; }
  .stat-card .value { font-size: 26px; font-weight: 700; color: var(--ink); margin-top: 4px; }
  .stat-card .value.low { color: var(--bad); }
  details { margin-top: 22px; border: 1px solid var(--line); border-radius: 6px; overflow: hidden; }
  summary {
    cursor: pointer; padding: 14px 16px; font-family: 'Barlow Condensed'; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.03em; font-size: 15px; color: var(--accent-dark);
    background: var(--paper); list-style: none;
  }
  summary::-webkit-details-marker { display: none; }
  summary:before { content: "▸ "; }
  details[open] summary:before { content: "▾ "; }
  details .table-wrap { padding: 6px 16px 18px; overflow-x: auto; }
  .home-grid { display: grid; gap: 12px; margin-top: 20px; }
  .home-btn {
    display: flex; align-items: center; justify-content: center; gap: 8px;
    text-decoration: none; text-align: center; padding: 17px 0;
    font-family: 'Barlow Condensed'; font-weight: 700; font-size: 16px; text-transform: uppercase;
    letter-spacing: 0.03em; border-radius: 8px; color: white; cursor: pointer; border: none;
    box-shadow: 0 3px 10px rgba(0,0,0,0.15); transition: transform 0.08s ease;
  }
  .home-btn:active { transform: scale(0.97); }
  .home-btn .icon { font-size: 19px; }
  .nav-top { max-width: 1080px; width: 100%; margin-bottom: 10px; display: flex; justify-content: flex-end; }
  .nav-top a { color: var(--ink-soft); font-size: 13px; text-decoration: none; }
</style>
"""

# ---------- Home page ----------
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
    <button class="home-btn" style="background:var(--accent);" onclick="goTo('/furnace-form')"><span class="icon">🔥</span> Furnace Reading</button>
    <button class="home-btn" style="background:var(--ink);" onclick="goTo('/production-form')"><span class="icon">🏭</span> Production &amp; Consumption</button>
    <button class="home-btn" style="background:#3B5C7A;" onclick="goTo('/electricity-form')"><span class="icon">⚡</span> Electricity &amp; Wire Rod</button>
    <button class="home-btn" style="background:#2E7D32;" onclick="goTo('/receipt-form')"><span class="icon">📦</span> Log Stock Receipt</button>
    <button class="home-btn" style="background:#8B5E00;" onclick="goTo('/sales-form')"><span class="icon">💰</span> Log a Sale</button>
    <a class="home-btn" style="background:#12283F;" href="/operator-dashboard"><span class="icon">📊</span> View Stock Dashboard</a>
  </div>
</div>
<script>
  function goTo(path) {
    var name = document.getElementById('operatorSelect').value;
    window.location.href = path + '?operator=' + encodeURIComponent(name);
  }
</script>
"""

# ---------- Furnace form ----------
FURNACE_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Furnace Temperature</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>
  <form method="POST" action="/submit-furnace">
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
    <div class="toggle-group" id="b1-group">
      <div class="toggle-btn" data-target="B1" data-value="ON">ON</div>
      <div class="toggle-btn" data-target="B1" data-value="OFF">OFF</div>
    </div>
    <input type="hidden" name="B1" id="B1" required>
    <label>B1 Running Hours (today)</label>
    <input type="number" name="B1_HOURS" step="0.1" required>

    <label>B2 Status</label>
    <div class="toggle-group" id="b2-group">
      <div class="toggle-btn" data-target="B2" data-value="ON">ON</div>
      <div class="toggle-btn" data-target="B2" data-value="OFF">OFF</div>
    </div>
    <input type="hidden" name="B2" id="B2" required>
    <label>B2 Running Hours (today)</label>
    <input type="number" name="B2_HOURS" step="0.1" required>

    <button class="submit" type="submit">Submit Reading</button>
  </form>
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
"""

# ---------- Production & Consumption form ----------
PRODUCTION_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Production &amp; Consumption</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>
  <form method="POST" action="/submit-production">
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

    <button class="submit" type="submit">Submit Log</button>
  </form>
</div>
<script>
  function addExtraConsumptionRow() {
    var container = document.getElementById('extraConsumption');
    var row = document.createElement('div');
    row.className = 'extra-row';
    row.innerHTML =
      '<input type="text" name="extra_cons_name[]" placeholder="Consumable name">' +
      '<input type="number" name="extra_cons_qty[]" placeholder="Qty (kg)" step="0.1">';
    container.appendChild(row);
  }
  function addExtraProductionRow() {
    var container = document.getElementById('extraProduction');
    var row = document.createElement('div');
    row.className = 'extra-row';
    row.innerHTML =
      '<input type="text" name="extra_prod_name[]" placeholder="Item name">' +
      '<input type="number" name="extra_prod_qty[]" placeholder="Qty" step="0.1">';
    container.appendChild(row);
  }
</script>
"""

# ---------- Electricity & Wire Rod form ----------
ELECTRICITY_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Electricity &amp; Wire Rod</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>
  <form method="POST" action="/submit-electricity">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <h2 class="section">Electricity</h2>
    <label>Units Consumed</label>
    <input type="number" name="electricity_units" step="0.1" required>

    <h2 class="section">Wire Rod Issued (kg)</h2>
    {% for size in wire_rod_sizes %}
    <label>{{ size }}</label>
    <input type="number" name="wr_fixed_{{ loop.index0 }}" step="0.1" value="0">
    {% endfor %}
    <div id="extraWireRod"></div>
    <button class="add-item" type="button" onclick="addExtraWireRodRow()">+ Add Another Size</button>

    <button class="submit" type="submit">Submit Log</button>
  </form>
</div>
<script>
  function addExtraWireRodRow() {
    var container = document.getElementById('extraWireRod');
    var row = document.createElement('div');
    row.className = 'extra-row';
    row.innerHTML =
      '<input type="text" name="extra_wr_size[]" placeholder="Size (e.g. 7.00 mm)">' +
      '<input type="number" name="extra_wr_qty[]" placeholder="Qty (kg)" step="0.1">';
    container.appendChild(row);
  }
</script>
"""

# ---------- Receipt form ----------
RECEIPT_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Stock Receipt</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>
  <form method="POST" action="/submit-receipt">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <label>Category</label>
    <select id="categorySelect" name="category" onchange="updateItems()">
      {% for cat in categories %}
      <option value="{{ cat }}">{{ cat }}</option>
      {% endfor %}
    </select>

    <label>Item</label>
    <select id="itemSelect" name="item"></select>
    <input type="text" id="customItem" name="custom_item" placeholder="Specify item" style="display:none;margin-top:8px;">

    <label>Quantity Received</label>
    <input type="number" name="quantity" step="0.1" required>

    <button class="submit" type="submit">Submit Receipt</button>
  </form>
</div>
<script>
  var categoryItems = {{ category_items | tojson }};
  function updateItems() {
    var cat = document.getElementById('categorySelect').value;
    var itemSelect = document.getElementById('itemSelect');
    itemSelect.innerHTML = '';
    categoryItems[cat].forEach(function(item) {
      var opt = document.createElement('option');
      opt.value = item; opt.textContent = item;
      itemSelect.appendChild(opt);
    });
    var otherOpt = document.createElement('option');
    otherOpt.value = 'Other'; otherOpt.textContent = 'Other';
    itemSelect.appendChild(otherOpt);
    itemSelect.onchange = function() {
      document.getElementById('customItem').style.display = (itemSelect.value === 'Other') ? 'block' : 'none';
    };
  }
  updateItems();
</script>
"""

# ---------- Sales form ----------
SALES_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Log a Sale</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>
  <form method="POST" action="/submit-sale">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>Date &amp; Time</label>
    <input type="datetime-local" name="entry_time" value="{{ default_time }}" required>

    <label>Item</label>
    <select id="itemSelect" name="item" onchange="toggleCustom()">
      {% for item in finished_goods_items %}
      <option value="{{ item }}">{{ item }}</option>
      {% endfor %}
      <option value="Other">Other</option>
    </select>
    <input type="text" id="customItem" name="custom_item" placeholder="Specify item" style="display:none;margin-top:8px;">

    <label>Quantity Sold</label>
    <input type="number" name="quantity" step="0.1" required>

    <label>Sale Price (Rs./Kg)</label>
    <input type="number" name="price" step="0.01" required>

    <label>Customer (optional)</label>
    <input type="text" name="customer">

    <button class="submit" type="submit">Submit Sale</button>
  </form>
</div>
<script>
  function toggleCustom() {
    var sel = document.getElementById('itemSelect');
    document.getElementById('customItem').style.display = (sel.value === 'Other') ? 'block' : 'none';
  }
</script>
"""

SUCCESS_HTML = BASE_STYLE + """
<div class="card success">
  <div class="icon">&#9989;</div>
  <h1>Logged Successfully</h1>
  <p class="subtitle">Thank you, {{ operator }}. Your entry has been recorded.</p>
  {% if alerts %}
    <p class="error-box">&#128680; {{ alerts }}</p>
  {% endif %}
  <a href="/app-home" style="display:block;text-align:center;margin-top:18px;color:var(--accent-dark);font-weight:700;text-decoration:none;">&larr; Back to Home</a>
</div>
"""

# ---------- Dashboard ----------
DASHBOARD_HTML = BASE_STYLE + """
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black">
<meta name="apple-mobile-web-app-title" content="Khemji Dashboard">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { --accent: #1B3A5C; --accent-dark: #12283F; }
  body { background: linear-gradient(180deg, #DCE6F0 0%, #F3F6F9 320px, #F3F6F9 100%); }
  .brand-header {
    display: flex; flex-direction: column; align-items: center; text-align: center;
    gap: 10px; margin-bottom: 18px;
  }
  .brand-header .logo-box {
    background: white; border-radius: 10px; padding: 14px 26px;
    box-shadow: 0 2px 8px rgba(14,124,134,0.12); border: 1px solid var(--line);
  }
  .brand-header img { height: 76px; width: auto; display: block; }
  .brand-header .brand-title {
    font-family:'Barlow Condensed'; font-weight:700; font-size:26px; letter-spacing:0.03em;
    text-transform:uppercase; color: var(--ink);
  }
  .brand-header .dash-title {
    font-family:'Barlow Condensed'; font-weight:700; font-size:20px; letter-spacing:0.04em;
    text-transform:uppercase; color: var(--accent-dark); margin-top: 2px;
  }
  .cat-selector { display:flex; align-items:center; justify-content:center; gap:10px; margin: 16px 0 20px; }
  .cat-selector select { max-width: 260px; }
  .footer-brand {
    margin-top: 36px; padding-top: 20px; border-top: 1px solid var(--line);
    font-size: 13px; color: var(--ink-soft); line-height: 1.6; text-align: center;
  }
  .footer-brand b { color: var(--ink); }

  .kpi-strip { display:grid; grid-template-columns: repeat(auto-fit, minmax(160px,1fr)); gap: 12px; margin: 8px 0 22px; }
  .kpi-card {
    background: linear-gradient(135deg, var(--accent) 0%, var(--accent-dark) 100%);
    color: white; border-radius: 8px; padding: 16px 18px;
  }
  .kpi-card .kpi-label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; opacity: 0.85; font-weight: 700; }
  .kpi-card .kpi-value { font-size: 24px; font-weight: 700; margin-top: 4px; }
  .kpi-card.alert { background: linear-gradient(135deg, #C62828 0%, #8E1E1E 100%); }

  .alert-panel {
    background: #FDECEC; border: 1.5px solid #F1B3B3; border-radius: 8px; padding: 14px 18px; margin-bottom: 22px;
  }
  .alert-panel .alert-title { color: var(--bad); font-weight: 700; text-transform: uppercase; font-size: 13px; letter-spacing: 0.03em; margin-bottom: 6px; }
  .alert-panel .alert-item { font-size: 14px; color: var(--ink); padding: 3px 0; }
  .alert-panel.all-clear { background: #EAF5EA; border-color: #B9DDB9; }
  .alert-panel.all-clear .alert-title { color: var(--ok); }

  .bar-list { margin-top: 6px; }
  .bar-row { display:flex; align-items:center; gap: 10px; margin: 10px 0; }
  .bar-row .bar-label { width: 110px; font-size: 13px; color: var(--ink-soft); flex-shrink: 0; text-align: right; }
  .bar-row .bar-track { flex: 1; background: #E2EEEE; border-radius: 20px; height: 20px; overflow: hidden; position: relative; }
  .bar-row .bar-fill { height: 100%; border-radius: 20px; transition: width 0.4s ease; }
  .bar-row .bar-value { width: 60px; font-size: 13px; font-weight: 700; text-align: left; flex-shrink: 0; }
  .date-picker-bar {
    display:flex; align-items:center; justify-content:center; gap:10px; margin: 4px 0 20px; flex-wrap: wrap;
  }
  .date-picker-bar input[type=date] { max-width: 200px; }
  .date-picker-bar .today-pill {
    background: var(--accent); color: white; font-size: 12px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.03em; padding: 4px 10px; border-radius: 20px;
  }
  .cat-total-row { display:flex; justify-content: space-between; align-items:center; margin: 4px 0 10px; }
  .cat-total-row .cat-total-value { font-size: 20px; font-weight: 700; color: var(--accent-dark); }
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
      <div class="kpi-value">{{ day_sales_qty if day_sales_qty is not none else 0 }}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Revenue ({{ selected_date }})</div>
      <div class="kpi-value">Rs. {{ day_sales_revenue if day_sales_revenue is not none else 0 }}</div>
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
  <div class="alert-panel all-clear">
    <div class="alert-title">&#9989; All Stock Levels Healthy</div>
  </div>
  {% endif %}

  <h2 class="section">Stock Overview</h2>
  <div class="cat-selector">
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
    <div class="stat-card" style="border-left:4px solid var(--accent);">
      <div class="label">Wire Rod {{ row.item }}</div>
      <div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div>
    </div>
    {% endfor %}
  </div>
  <div id="stat-finished" class="stat-grid cat-block" style="display:none;">
    {% for row in finished_goods_stock %}
    <div class="stat-card" style="border-left:4px solid var(--ink);">
      <div class="label">{{ row.item }}</div>
      <div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div>
    </div>
    {% endfor %}
  </div>
  <div id="stat-consumables" class="stat-grid cat-block" style="display:none;">
    {% for row in consumables_stock %}
    <div class="stat-card" style="border-left:4px solid #2E7D32;">
      <div class="label">{{ row.item }}</div>
      <div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div>
    </div>
    {% endfor %}
  </div>

  <div id="table-raw" class="cat-block">
    <div class="cat-total-row"><span>Total Raw Material Balance</span><span class="cat-total-value">{{ totals.raw_material }}</span></div>
    <div class="table-wrap">
      <table>
        <tr><th>Size</th><th>Opening</th><th>Received</th><th>Issued</th><th>Balance</th></tr>
        {% for row in raw_material_stock %}
        <tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td>
        <td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>
        {% endfor %}
      </table>
    </div>
  </div>
  <div id="table-finished" class="cat-block" style="display:none;">
    <div class="cat-total-row"><span>Total Finished Goods Balance</span><span class="cat-total-value">{{ totals.finished_goods }}</span></div>
    <div class="table-wrap">
      <table>
        <tr><th>Item</th><th>Opening</th><th>Produced</th><th>Sold</th><th>Balance</th></tr>
        {% for row in finished_goods_stock %}
        <tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td>
        <td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>
        {% endfor %}
      </table>
    </div>
  </div>
  <div id="table-consumables" class="cat-block" style="display:none;">
    <div class="cat-total-row"><span>Total Consumables Balance</span><span class="cat-total-value">{{ totals.consumables }}</span></div>
    <div class="table-wrap">
      <table>
        <tr><th>Item</th><th>Opening</th><th>Received</th><th>Consumed</th><th>Balance</th></tr>
        {% for row in consumables_stock %}
        <tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td>
        <td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>
        {% endfor %}
      </table>
    </div>
  </div>

  <h2 class="section">Monthly Item Report</h2>
  <div class="date-picker-bar" style="justify-content:flex-start;">
    <label style="margin:0;">Item</label>
    <select id="reportItemSelect" onchange="changeMonthlyReport()" style="max-width:220px;">
      {% for item in all_report_items %}
      <option value="{{ item }}" {{ 'selected' if item == report_item else '' }}>{{ item }}</option>
      {% endfor %}
    </select>
    <label style="margin:0;">Month</label>
    <input type="month" id="reportMonthSelect" value="{{ report_month }}" onchange="changeMonthlyReport()">
  </div>

  {% if monthly_report %}
  <div class="stat-grid" style="margin-bottom:26px;">
    {% if 'consumption' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid #2E7D32;">
      <div class="label">Consumption ({{ monthly_report.month }})</div>
      <div class="value">{{ monthly_report.consumption }}</div>
    </div>
    <div class="stat-card" style="border-left:4px solid #2E7D32;">
      <div class="label">Received ({{ monthly_report.month }})</div>
      <div class="value">{{ monthly_report.receipts }}</div>
    </div>
    {% endif %}
    {% if 'issued' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid var(--accent);">
      <div class="label">Wire Rod Issued ({{ monthly_report.month }})</div>
      <div class="value">{{ monthly_report.issued }}</div>
    </div>
    <div class="stat-card" style="border-left:4px solid var(--accent);">
      <div class="label">Received ({{ monthly_report.month }})</div>
      <div class="value">{{ monthly_report.receipts }}</div>
    </div>
    {% endif %}
    {% if 'production' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid var(--ink);">
      <div class="label">Production ({{ monthly_report.month }})</div>
      <div class="value">{{ monthly_report.production }}</div>
    </div>
    {% if show_sales and 'sales_qty' in monthly_report %}
    <div class="stat-card" style="border-left:4px solid #8B5E00;">
      <div class="label">Qty Sold ({{ monthly_report.month }})</div>
      <div class="value">{{ monthly_report.sales_qty }}</div>
    </div>
    <div class="stat-card" style="border-left:4px solid #8B5E00;">
      <div class="label">Revenue ({{ monthly_report.month }})</div>
      <div class="value">Rs. {{ monthly_report.sales_revenue }}</div>
    </div>
    {% endif %}
    {% endif %}
  </div>
  {% endif %}

  <h2 class="section">Recent Entries</h2>

  <details>
    <summary>Furnace Readings</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in furnace_header %}<th>{{ h }}</th>{% endfor %}{% if show_edit %}<th>Edit</th>{% endif %}</tr>
        {% for rownum, row in furnace_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}
        {% if show_edit %}<td><a href="/edit-entry?tab=Readings&row={{ rownum }}&key={{ admin_key }}">Edit</a></td>{% endif %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  <details>
    <summary>Production</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in production_header %}<th>{{ h }}</th>{% endfor %}{% if show_edit %}<th>Edit</th>{% endif %}</tr>
        {% for rownum, row in production_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}
        {% if show_edit %}<td><a href="/edit-entry?tab=Production&row={{ rownum }}&key={{ admin_key }}">Edit</a></td>{% endif %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  <details>
    <summary>Consumption</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in consumption_header %}<th>{{ h }}</th>{% endfor %}{% if show_edit %}<th>Edit</th>{% endif %}</tr>
        {% for rownum, row in consumption_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}
        {% if show_edit %}<td><a href="/edit-entry?tab=Consumption&row={{ rownum }}&key={{ admin_key }}">Edit</a></td>{% endif %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  <details>
    <summary>Electricity &amp; Wire Rod</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in electricity_header %}<th>{{ h }}</th>{% endfor %}{% if show_edit %}<th>Edit</th>{% endif %}</tr>
        {% for rownum, row in electricity_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}
        {% if show_edit %}<td><a href="/edit-entry?tab=ElectricityWireRod&row={{ rownum }}&key={{ admin_key }}">Edit</a></td>{% endif %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  <details>
    <summary>Receipts</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in receipts_header %}<th>{{ h }}</th>{% endfor %}{% if show_edit %}<th>Edit</th>{% endif %}</tr>
        {% for rownum, row in receipts_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}
        {% if show_edit %}<td><a href="/edit-entry?tab=Receipts&row={{ rownum }}&key={{ admin_key }}">Edit</a></td>{% endif %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  {% if show_sales %}
  <details>
    <summary>Sales</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in sales_header %}<th>{{ h }}</th>{% endfor %}{% if show_edit %}<th>Edit</th>{% endif %}</tr>
        {% for rownum, row in sales_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}
        {% if show_edit %}<td><a href="/edit-entry?tab=Sales&row={{ rownum }}&key={{ admin_key }}">Edit</a></td>{% endif %}</tr>{% endfor %}
      </table>
    </div>
  </details>
  {% endif %}

  <div class="footer-brand">
    <b>Khemji Wire &amp; Wire Pvt. Ltd.</b> &middot; F-153, Sarna Doongar, RIICO Industrial Area, Jaipur, Rajasthan 302012<br>
    Phone: +91-9829277869 &middot; +91-141-2954144 &middot; Email: info@khemjiwire.in<br>
    GSTIN: 08AAECA7760L1ZA &middot; IS 280 &amp; IS 3975 Certified
  </div>
</div>
<script>
  var stockData = {
    raw: {{ raw_material_stock | tojson }},
    finished: {{ finished_goods_stock | tojson }},
    consumables: {{ consumables_stock | tojson }}
  };
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
      row.innerHTML =
        '<div class="bar-label">' + r.item + '</div>' +
        '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%;background:' + barColor + ';"></div></div>' +
        '<div class="bar-value">' + r.balance + '</div>';
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

EDIT_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Edit Entry</h1>
  <p class="subtitle">{{ tab }} &mdash; Row {{ row_number }}</p>
  <form method="POST" action="/save-edit">
    <input type="hidden" name="tab" value="{{ tab }}">
    <input type="hidden" name="row" value="{{ row_number }}">
    <input type="hidden" name="key" value="{{ admin_key }}">
    {% for col_name, value in fields %}
    <label>{{ col_name }}</label>
    <input type="text" name="col_{{ loop.index0 }}" value="{{ value }}">
    {% endfor %}
    <button class="submit" type="submit">Save Changes</button>
  </form>
</div>
"""


# ---------- Home ----------
@app.route("/app-home", methods=["GET"])
@app.route("/", methods=["GET"])
def app_home():
    return render_template_string(HOME_HTML, all_names=sorted(ALL_PEOPLE.keys()))


# ---------- Furnace ----------
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

    base_header = ["Date", "Time", "Operator", "T1", "T2", "T3", "B1", "B1 Hours", "B2", "B2 Hours", "Alerts"]
    row_dict = {
        "Date": date_str, "Time": time_str, "Operator": operator,
        "T1": values["T1"], "T2": values["T2"], "T3": values["T3"],
        "B1": values["B1"], "B1 Hours": b1_hours, "B2": values["B2"], "B2 Hours": b2_hours,
        "Alerts": alert_text,
    }
    try:
        append_named_row(TAB_READINGS, base_header, row_dict)
    except Exception as e:
        print(f"  -> FAILED to log furnace reading: {e}")

    notify_admins(
        f"📋 Furnace reading from {operator}\n"
        f"T1={values['T1']} T2={values['T2']} T3={values['T3']}\n"
        f"B1={values['B1']} ({b1_hours} hrs)  B2={values['B2']} ({b2_hours} hrs)\n"
        f"Alerts: {alert_text}"
    )
    if alerts:
        notify_admins(f"🚨 ALARM - Furnace reading out of range from {operator}:\n" + "\n".join(alerts))

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=alert_text if alerts else None)


# ---------- Production & Consumption ----------
@app.route("/production-form", methods=["GET"])
def production_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(PRODUCTION_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   consumption_items=CONSUMPTION_ITEMS, production_items=PRODUCTION_ITEMS)


@app.route("/submit-production", methods=["POST"])
def submit_production():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)

    # ---- Consumption ----
    cons_dict = {"Date": date_str, "Time": time_str, "Operator": operator}
    for i, item in enumerate(CONSUMPTION_ITEMS):
        cons_dict[item] = request.form.get(f"cons_{i}", "0")

    extra_cons_names = request.form.getlist("extra_cons_name[]")
    extra_cons_qtys = request.form.getlist("extra_cons_qty[]")
    for n, q in zip(extra_cons_names, extra_cons_qtys):
        n = n.strip()
        if n:
            cons_dict[n] = q

    try:
        append_named_row(TAB_CONSUMPTION, ["Date", "Time", "Operator"] + CONSUMPTION_ITEMS, cons_dict)
    except Exception as e:
        print(f"  -> FAILED to log consumption: {e}")

    # ---- Production ----
    prod_dict = {"Date": date_str, "Time": time_str, "Operator": operator}
    total = 0.0
    for i, item in enumerate(PRODUCTION_ITEMS):
        v = request.form.get(f"prod_{i}", "0")
        prod_dict[item] = v
        total += safe_float(v)

    extra_prod_names = request.form.getlist("extra_prod_name[]")
    extra_prod_qtys = request.form.getlist("extra_prod_qty[]")
    for n, q in zip(extra_prod_names, extra_prod_qtys):
        n = n.strip()
        if n:
            prod_dict[n] = q
            total += safe_float(q)

    prod_dict["Total Production"] = round(total, 2)

    try:
        append_named_row(TAB_PRODUCTION, ["Date", "Time", "Operator"] + PRODUCTION_ITEMS + ["Total Production"], prod_dict)
    except Exception as e:
        print(f"  -> FAILED to log production: {e}")

    notify_admins(f"🏭 Production & Consumption from {operator}\nTotal Production: {round(total, 2)}\n(Full breakdown in Google Sheet)")

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Electricity & Wire Rod ----------
@app.route("/electricity-form", methods=["GET"])
def electricity_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(ELECTRICITY_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   wire_rod_sizes=WIRE_ROD_SIZES)


@app.route("/submit-electricity", methods=["POST"])
def submit_electricity():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    units = request.form.get("electricity_units", "")

    row_dict = {"Date": date_str, "Time": time_str, "Operator": operator, "Electricity Units": units}
    for i, size in enumerate(WIRE_ROD_SIZES):
        row_dict[size] = request.form.get(f"wr_fixed_{i}", "0")

    extra_sizes = request.form.getlist("extra_wr_size[]")
    extra_qtys = request.form.getlist("extra_wr_qty[]")
    for s, q in zip(extra_sizes, extra_qtys):
        s = s.strip()
        if s:
            row_dict[s] = q

    try:
        append_named_row(TAB_ELECTRICITY_WIRE_ROD, ["Date", "Time", "Operator", "Electricity Units"] + WIRE_ROD_SIZES, row_dict)
    except Exception as e:
        print(f"  -> FAILED to log electricity/wire rod: {e}")

    wr_summary = ", ".join(f"{k}={v}" for k, v in row_dict.items() if k not in ("Date", "Time", "Operator", "Electricity Units") and v not in ("", "0"))
    notify_admins(f"⚡ Electricity & Wire Rod from {operator}\nUnits={units}\n{wr_summary if wr_summary else 'No wire rod entered'}")

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Stock: Receipts ----------
@app.route("/receipt-form", methods=["GET"])
def receipt_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(RECEIPT_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   categories=list(RECEIPT_CATEGORIES.keys()), category_items=RECEIPT_CATEGORIES)


@app.route("/submit-receipt", methods=["POST"])
def submit_receipt():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    category = request.form.get("category", "")
    item = request.form.get("item", "")
    custom_item = request.form.get("custom_item", "").strip()
    final_item = custom_item if item == "Other" and custom_item else item
    quantity = request.form.get("quantity", "")

    row = [date_str, time_str, operator, category, final_item, quantity]
    try:
        log_row_simple(TAB_RECEIPTS, ["Date", "Time", "Operator", "Category", "Item", "Quantity"], row)
    except Exception as e:
        print(f"  -> FAILED to log receipt: {e}")

    notify_admins(f"📦 Stock Receipt from {operator}\n{category} - {final_item}: +{quantity}")

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Stock: Sales ----------
@app.route("/sales-form", methods=["GET"])
def sales_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(SALES_FORM_HTML, operator=operator, default_time=default_entry_time(),
                                   finished_goods_items=FINISHED_GOODS_ITEMS)


@app.route("/submit-sale", methods=["POST"])
def submit_sale():
    operator = request.form.get("operator", "Unknown")
    date_str, time_str = parse_entry_datetime(request.form)
    item = request.form.get("item", "")
    custom_item = request.form.get("custom_item", "").strip()
    final_item = custom_item if item == "Other" and custom_item else item
    quantity = request.form.get("quantity", "")
    price = request.form.get("price", "")
    customer = request.form.get("customer", "")
    total_amount = round(safe_float(quantity) * safe_float(price), 2)

    row = [date_str, time_str, operator, final_item, quantity, price, total_amount, customer]
    try:
        log_row_simple(TAB_SALES, ["Date", "Time", "Operator", "Item", "Quantity", "Price (Rs/Kg)", "Total Amount (Rs)", "Customer"], row)
    except Exception as e:
        print(f"  -> FAILED to log sale: {e}")

    notify_admins(
        f"💰 Sale logged by {operator}\n{final_item}: -{quantity} @ Rs.{price}/kg = Rs.{total_amount}"
        + (f" (Customer: {customer})" if customer else "")
    )

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


def log_row_simple(tab_name, header, row):
    ws = get_or_create_tab(tab_name, header)
    ws.append_row(row)


# ---------- Admin dashboard ----------
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

    day_production = sum_column_by_name_for_date(TAB_PRODUCTION, ["Date", "Time", "Operator"] + PRODUCTION_ITEMS + ["Total Production"], "Total Production", date_str)

    day_sales_by_item, day_sales_qty, day_sales_revenue = ({}, None, None)
    top_seller = None
    if is_admin:
        day_sales_by_item, day_sales_qty, day_sales_revenue = get_sales_summary_for_date(date_str)
        if day_sales_by_item:
            top_item = max(day_sales_by_item, key=day_sales_by_item.get)
            top_seller = {"item": top_item, "qty": round(day_sales_by_item[top_item], 2)}

    last_updated = now_ist().strftime("%d %b %Y, %I:%M %p")

    # Monthly item report
    all_report_items = CONSUMPTION_ITEMS + WIRE_ROD_SIZES + FINISHED_GOODS_ITEMS
    month_str = report_month or now_ist().strftime("%Y-%m")
    item_for_report = report_item or all_report_items[0]
    monthly_report = get_monthly_item_report(item_for_report, month_str, include_sales=is_admin)

    furnace_header_base = ["Date", "Time", "Operator", "T1", "T2", "T3", "B1", "B1 Hours", "B2", "B2 Hours", "Alerts"]
    production_header_base = ["Date", "Time", "Operator"] + PRODUCTION_ITEMS + ["Total Production"]
    consumption_header_base = ["Date", "Time", "Operator"] + CONSUMPTION_ITEMS
    electricity_header_base = ["Date", "Time", "Operator", "Electricity Units"] + WIRE_ROD_SIZES
    receipts_header = ["Date", "Time", "Operator", "Category", "Item", "Quantity"]
    sales_header = ["Date", "Time", "Operator", "Item", "Quantity", "Price (Rs/Kg)", "Total Amount (Rs)", "Customer"]

    furnace_header, furnace_rows = get_recent_rows(TAB_READINGS, furnace_header_base, operator_filter=operator_filter)
    production_header, production_rows = get_recent_rows(TAB_PRODUCTION, production_header_base, operator_filter=operator_filter)
    consumption_header, consumption_rows = get_recent_rows(TAB_CONSUMPTION, consumption_header_base, operator_filter=operator_filter)
    electricity_header, electricity_rows = get_recent_rows(TAB_ELECTRICITY_WIRE_ROD, electricity_header_base, operator_filter=operator_filter)
    _, receipts_rows = get_recent_rows(TAB_RECEIPTS, receipts_header, operator_filter=operator_filter)

    if is_admin:
        _, sales_rows = get_sales_rows_for_date(date_str)
        if operator_filter:
            sales_rows = [(rn, r) for rn, r in sales_rows if len(r) > 2 and r[2] == operator_filter]
    else:
        sales_rows = []

    return render_template_string(
        DASHBOARD_HTML,
        admin_key=ADMIN_KEY if is_admin else "",
        show_sales=is_admin,
        show_edit=is_admin,
        selected_date=date_str,
        is_today=is_today,
        operator_filter=operator_filter or "",
        all_operator_names=sorted(ALL_PEOPLE.keys()),
        low_stock_items=low_stock_items,
        low_stock_count=len(low_stock_items),
        day_production=day_production,
        day_sales_qty=day_sales_qty,
        day_sales_revenue=day_sales_revenue,
        top_seller=top_seller,
        last_updated=last_updated,
        totals=totals,
        all_report_items=all_report_items,
        report_item=item_for_report,
        report_month=month_str,
        monthly_report=monthly_report,
        consumables_stock=consumables_stock,
        raw_material_stock=raw_material_stock,
        finished_goods_stock=finished_goods_stock,
        furnace_header=furnace_header, furnace_rows=furnace_rows,
        production_header=production_header, production_rows=production_rows,
        consumption_header=consumption_header, consumption_rows=consumption_rows,
        electricity_header=electricity_header, electricity_rows=electricity_rows,
        receipts_header=receipts_header, receipts_rows=receipts_rows,
        sales_header=sales_header, sales_rows=sales_rows,
    )


DASHBOARD_ERROR_HTML = """
<div style="font-family:'Segoe UI',Arial,sans-serif;max-width:480px;margin:60px auto;text-align:center;
            background:white;border-radius:10px;padding:32px 24px;box-shadow:0 4px 16px rgba(0,0,0,0.1);">
  <div style="font-size:40px;margin-bottom:10px;">&#9888;</div>
  <h2 style="color:#1B3A5C;margin:0 0 10px;">Dashboard Temporarily Unavailable</h2>
  <p style="color:#4B5563;line-height:1.5;">
    Google Sheets didn't respond in time \u2014 this usually clears up on its own.
    Please wait a few seconds and reload the page.
  </p>
  <p style="color:#9CA3AF;font-size:12px;margin-top:18px;">({{ error }})</p>
</div>
"""


@app.route("/dashboard", methods=["GET"])
def dashboard():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    try:
        return render_dashboard(
            is_admin=True,
            selected_date=request.args.get("date"),
            operator_filter=request.args.get("operator") or None,
            report_item=request.args.get("report_item"),
            report_month=request.args.get("report_month"),
        )
    except Exception as e:
        print(f"  -> Dashboard render failed: {e}")
        return render_template_string(DASHBOARD_ERROR_HTML, error=str(e)), 503


@app.route("/operator-dashboard", methods=["GET"])
def operator_dashboard():
    try:
        return render_dashboard(
            is_admin=False,
            selected_date=request.args.get("date"),
            operator_filter=request.args.get("operator") or None,
            report_item=request.args.get("report_item"),
            report_month=request.args.get("report_month"),
        )
    except Exception as e:
        print(f"  -> Operator dashboard render failed: {e}")
        return render_template_string(DASHBOARD_ERROR_HTML, error=str(e)), 503


# ---------- Admin edit ----------
@app.route("/edit-entry", methods=["GET"])
def edit_entry():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    tab = request.args.get("tab", "")
    row_number = request.args.get("row", "")
    if not tab or not row_number:
        abort(400)

    ss = get_spreadsheet()
    try:
        ws = ss.worksheet(tab)
    except gspread.exceptions.WorksheetNotFound:
        abort(404)

    header = ws.row_values(1)
    row_values = ws.row_values(int(row_number))
    while len(row_values) < len(header):
        row_values.append("")

    fields = list(zip(header, row_values))
    return render_template_string(EDIT_FORM_HTML, tab=tab, row_number=row_number, fields=fields, admin_key=ADMIN_KEY)


@app.route("/save-edit", methods=["POST"])
def save_edit():
    if request.form.get("key") != ADMIN_KEY:
        abort(403)
    tab = request.form.get("tab", "")
    row_number = int(request.form.get("row", "0"))

    ss = get_spreadsheet()
    try:
        ws = ss.worksheet(tab)
    except gspread.exceptions.WorksheetNotFound:
        abort(404)

    header = ws.row_values(1)
    new_values = [request.form.get(f"col_{i}", "") for i in range(len(header))]
    ws.update(f"A{row_number}", [new_values])

    return render_template_string(
        SUCCESS_HTML, operator="Admin",
        alerts=None
    ) + f'<script>setTimeout(function(){{window.location.href="/dashboard?key={ADMIN_KEY}";}}, 1200);</script>'


# ---------- Manual test triggers ----------
@app.route("/test-send-furnace", methods=["GET"])
def test_send_furnace():
    send_furnace_reminders()
    return "Furnace reminder sent!"


@app.route("/test-send-production", methods=["GET"])
def test_send_production():
    send_prod_consumption_reminders()
    return "Production & Consumption reminder sent!"


@app.route("/test-send-electricity", methods=["GET"])
def test_send_electricity():
    send_electricity_reminders()
    return "Electricity & Wire Rod reminder sent!"


@app.route("/test-daily-close", methods=["GET"])
def test_daily_close():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    run_daily_stock_close()
    return "Daily stock close run manually for yesterday's date. Check the StockLedger tab."


if __name__ == "__main__":
    scheduler = BackgroundScheduler()

    for t in FURNACE_REMINDER_TIMES:
        hour, minute = t.split(":")
        scheduler.add_job(send_furnace_reminders, CronTrigger(hour=int(hour), minute=int(minute), timezone=IST))

    for t in PROD_CONSUMPTION_REMINDER_TIMES:
        hour, minute = t.split(":")
        scheduler.add_job(send_prod_consumption_reminders, CronTrigger(hour=int(hour), minute=int(minute), timezone=IST))

    for t in ELECTRICITY_REMINDER_TIMES:
        hour, minute = t.split(":")
        scheduler.add_job(send_electricity_reminders, CronTrigger(hour=int(hour), minute=int(minute), timezone=IST))

    def run_daily_stock_close_job():
        with app.app_context():
            run_daily_stock_close()

    scheduler.add_job(run_daily_stock_close_job, CronTrigger(hour=0, minute=0, timezone=IST))

    scheduler.start()

    print("Khemji Wire Reminder + Inventory App - running.")
    print(f"Furnace reminders daily at: {', '.join(FURNACE_REMINDER_TIMES)}")
    print(f"Production & Consumption reminders daily at: {', '.join(PROD_CONSUMPTION_REMINDER_TIMES)}")
    print(f"Electricity & Wire Rod reminders daily at: {', '.join(ELECTRICITY_REMINDER_TIMES)}")
    print("Daily stock close runs automatically at 00:00 IST.")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
