"""
Khemji Wire - Reminder + Mobile Entry Forms + Admin Dashboard + Inventory
Runs 24/7 on Render. Twilio WhatsApp for reminders (link-based), Google
Sheets for logging with separate Date/Time columns and dynamically-growing
item columns (any new item typed in gets its own titled column automatically).
"""

import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote
from flask import Flask, request, render_template_string, abort
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
PRODUCTION_ITEMS = ["1.25 mm", "1.40 mm", "1.60 mm", "1.80 mm", "2.00 mm", "2.50 mm", "4.00 mm", "Strip 16 Kg", "Strip 23 KG"]

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
# ==================================================

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
app = Flask(__name__)


# ---------- Google Sheets core helpers ----------
def get_spreadsheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(GOOGLE_SHEET_ID)


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
    ws = get_or_create_tab(tab_name, base_header)
    all_values = ws.get_all_values()
    if not all_values:
        return base_header, []
    return all_values[0], all_values[1:]


def get_recent_rows(tab_name, base_header, limit=12):
    header, rows = get_all_rows_with_header(tab_name, base_header)
    return header, list(reversed(rows[-limit:])) if rows else []


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
    header = ["Item", "Category", "Opening Qty", "As Of Date"]
    ws = get_or_create_tab(TAB_OPENING_STOCK, header)
    all_values = ws.get_all_values()

    if len(all_values) <= 1:
        today = now_ist().strftime("%Y-%m-%d")
        seed_rows = []
        for item in CONSUMPTION_ITEMS:
            seed_rows.append([item, "Consumables", 0, today])
        for size in WIRE_ROD_SIZES:
            seed_rows.append([size, "Raw Material", 0, today])
        for item in FINISHED_GOODS_ITEMS:
            seed_rows.append([item, "Finished Goods", 0, today])
        for row in seed_rows:
            ws.append_row(row)
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


def sum_sales_by_item():
    header = ["Date", "Time", "Operator", "Item", "Quantity", "Customer"]
    _, rows = get_all_rows_with_header(TAB_SALES, header)
    totals = {}
    for row in rows:
        if len(row) >= 5 and row[3]:
            item = row[3]
            totals[item] = totals.get(item, 0.0) + safe_float(row[4])
    return totals


def compute_stock():
    opening = get_opening_stock_map()

    consumption_base = ["Date", "Time", "Operator"] + CONSUMPTION_ITEMS
    consumed = sum_column_by_name(TAB_CONSUMPTION, consumption_base, CONSUMPTION_ITEMS)
    receipts_consumables = sum_receipts_by_item("Consumables")
    consumables_stock = []
    for item in CONSUMPTION_ITEMS:
        op, rec, used = opening.get(item, 0.0), receipts_consumables.get(item, 0.0), consumed.get(item, 0.0)
        consumables_stock.append({"item": item, "opening": op, "in": rec, "out": used, "balance": round(op + rec - used, 2)})

    wr_base = ["Date", "Time", "Operator", "Electricity Units"] + WIRE_ROD_SIZES
    issued = sum_column_by_name(TAB_ELECTRICITY_WIRE_ROD, wr_base, WIRE_ROD_SIZES)
    receipts_raw = sum_receipts_by_item("Raw Material")
    raw_material_stock = []
    for size in WIRE_ROD_SIZES:
        op, rec, used = opening.get(size, 0.0), receipts_raw.get(size, 0.0), issued.get(size, 0.0)
        raw_material_stock.append({"item": size, "opening": op, "in": rec, "out": used, "balance": round(op + rec - used, 2)})

    prod_base = ["Date", "Time", "Operator"] + PRODUCTION_ITEMS + ["Total Production"]
    produced = sum_column_by_name(TAB_PRODUCTION, prod_base, PRODUCTION_ITEMS)
    sold = sum_sales_by_item()
    finished_goods_stock = []
    for item in FINISHED_GOODS_ITEMS:
        op, made, sld = opening.get(item, 0.0), produced.get(item, 0.0), sold.get(item, 0.0)
        finished_goods_stock.append({"item": item, "opening": op, "in": made, "out": sld, "balance": round(op + made - sld, 2)})

    return consumables_stock, raw_material_stock, finished_goods_stock


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
    --ink: #1F2937; --ink-soft: #4B5563; --paper: #F5F4F1; --card: #FFFFFF;
    --accent: #E85D04; --accent-dark: #C24A00; --line: #E2DFD8; --ok: #2E7D32; --bad: #C62828;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--paper); color: var(--ink);
    font-family: 'Barlow Condensed', 'Segoe UI', Arial, sans-serif;
    min-height: 100vh; display: flex; flex-direction: column; align-items: center;
    padding: 24px 16px 60px;
  }
  .card {
    background: var(--card); border: 1px solid var(--line); border-radius: 6px;
    max-width: 480px; width: 100%; padding: 28px 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
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
    border: none; border-radius: 4px; cursor: pointer;
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
    display: block; text-decoration: none; text-align: center; padding: 16px 0;
    font-family: 'Barlow Condensed'; font-weight: 700; font-size: 16px; text-transform: uppercase;
    letter-spacing: 0.03em; border-radius: 4px; color: white; cursor: pointer; border: none;
  }
  .nav-top { max-width: 1080px; width: 100%; margin-bottom: 10px; display: flex; justify-content: flex-end; }
  .nav-top a { color: var(--ink-soft); font-size: 13px; text-decoration: none; }
</style>
"""

# ---------- Home page ----------
HOME_HTML = BASE_STYLE + """
<div class="card">
  <h1>Khemji Wire</h1>
  <p class="subtitle">Select your name, then choose what to log</p>
  <select id="operatorSelect" style="width:100%;padding:12px;font-size:18px;border:1.5px solid var(--line);border-radius:4px;background:var(--paper);">
    {% for name in all_names %}
    <option value="{{ name }}">{{ name }}</option>
    {% endfor %}
  </select>
  <div class="home-grid">
    <button class="home-btn" style="background:var(--accent);" onclick="goTo('/furnace-form')">Furnace Reading</button>
    <button class="home-btn" style="background:var(--ink);" onclick="goTo('/production-form')">Production &amp; Consumption</button>
    <button class="home-btn" style="background:#3B5C7A;" onclick="goTo('/electricity-form')">Electricity &amp; Wire Rod</button>
    <button class="home-btn" style="background:#2E7D32;" onclick="goTo('/receipt-form')">Log Stock Receipt</button>
    <button class="home-btn" style="background:#8B5E00;" onclick="goTo('/sales-form')">Log a Sale</button>
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
<div class="nav-top"><a href="/app-home">&larr; Back to entry forms</a></div>
<div class="card wide">
  <h1>Khemji Wire &mdash; Live Dashboard</h1>
  <p class="subtitle">Inventory balances update automatically as entries come in</p>

  <h2 class="section">Stock Overview</h2>
  <div class="stat-grid">
    {% for row in consumables_stock %}
    <div class="stat-card">
      <div class="label">{{ row.item }}</div>
      <div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div>
    </div>
    {% endfor %}
    {% for row in raw_material_stock %}
    <div class="stat-card">
      <div class="label">Wire Rod {{ row.item }}</div>
      <div class="value {{ 'low' if row.balance <= 0 else '' }}">{{ row.balance }}</div>
    </div>
    {% endfor %}
  </div>

  <details open>
    <summary>Consumables Stock (Opening + Received &minus; Consumed)</summary>
    <div class="table-wrap">
      <table>
        <tr><th>Item</th><th>Opening</th><th>Received</th><th>Consumed</th><th>Balance</th></tr>
        {% for row in consumables_stock %}
        <tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td>
        <td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>
        {% endfor %}
      </table>
    </div>
  </details>

  <details open>
    <summary>Raw Material Stock &mdash; Wire Rod (Opening + Received &minus; Issued)</summary>
    <div class="table-wrap">
      <table>
        <tr><th>Size</th><th>Opening</th><th>Received</th><th>Issued</th><th>Balance</th></tr>
        {% for row in raw_material_stock %}
        <tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td>
        <td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>
        {% endfor %}
      </table>
    </div>
  </details>

  <details open>
    <summary>Finished Goods Stock (Opening + Produced &minus; Sold)</summary>
    <div class="table-wrap">
      <table>
        <tr><th>Item</th><th>Opening</th><th>Produced</th><th>Sold</th><th>Balance</th></tr>
        {% for row in finished_goods_stock %}
        <tr><td>{{ row.item }}</td><td>{{ row.opening }}</td><td>{{ row.in }}</td><td>{{ row.out }}</td>
        <td class="{{ 'badge-bad' if row.balance <= 0 else 'badge-ok' }}">{{ row.balance }}</td></tr>
        {% endfor %}
      </table>
    </div>
  </details>

  <h2 class="section">Recent Entries</h2>

  <details>
    <summary>Furnace Readings</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in furnace_header %}<th>{{ h }}</th>{% endfor %}</tr>
        {% for row in furnace_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  <details>
    <summary>Production</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in production_header %}<th>{{ h }}</th>{% endfor %}</tr>
        {% for row in production_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  <details>
    <summary>Consumption</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in consumption_header %}<th>{{ h }}</th>{% endfor %}</tr>
        {% for row in consumption_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  <details>
    <summary>Electricity &amp; Wire Rod</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in electricity_header %}<th>{{ h }}</th>{% endfor %}</tr>
        {% for row in electricity_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  <details>
    <summary>Receipts</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in receipts_header %}<th>{{ h }}</th>{% endfor %}</tr>
        {% for row in receipts_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
      </table>
    </div>
  </details>

  <details>
    <summary>Sales</summary>
    <div class="table-wrap">
      <table>
        <tr>{% for h in sales_header %}<th>{{ h }}</th>{% endfor %}</tr>
        {% for row in sales_rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
      </table>
    </div>
  </details>
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
    customer = request.form.get("customer", "")

    row = [date_str, time_str, operator, final_item, quantity, customer]
    try:
        log_row_simple(TAB_SALES, ["Date", "Time", "Operator", "Item", "Quantity", "Customer"], row)
    except Exception as e:
        print(f"  -> FAILED to log sale: {e}")

    notify_admins(f"💰 Sale logged by {operator}\n{final_item}: -{quantity}" + (f" (Customer: {customer})" if customer else ""))

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


def log_row_simple(tab_name, header, row):
    ws = get_or_create_tab(tab_name, header)
    ws.append_row(row)


# ---------- Admin dashboard ----------
@app.route("/dashboard", methods=["GET"])
def dashboard():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)

    consumables_stock, raw_material_stock, finished_goods_stock = compute_stock()

    furnace_header_base = ["Date", "Time", "Operator", "T1", "T2", "T3", "B1", "B1 Hours", "B2", "B2 Hours", "Alerts"]
    production_header_base = ["Date", "Time", "Operator"] + PRODUCTION_ITEMS + ["Total Production"]
    consumption_header_base = ["Date", "Time", "Operator"] + CONSUMPTION_ITEMS
    electricity_header_base = ["Date", "Time", "Operator", "Electricity Units"] + WIRE_ROD_SIZES
    receipts_header = ["Date", "Time", "Operator", "Category", "Item", "Quantity"]
    sales_header = ["Date", "Time", "Operator", "Item", "Quantity", "Customer"]

    furnace_header, furnace_rows = get_recent_rows(TAB_READINGS, furnace_header_base)
    production_header, production_rows = get_recent_rows(TAB_PRODUCTION, production_header_base)
    consumption_header, consumption_rows = get_recent_rows(TAB_CONSUMPTION, consumption_header_base)
    electricity_header, electricity_rows = get_recent_rows(TAB_ELECTRICITY_WIRE_ROD, electricity_header_base)
    _, receipts_rows = get_recent_rows(TAB_RECEIPTS, receipts_header)
    _, sales_rows = get_recent_rows(TAB_SALES, sales_header)

    return render_template_string(
        DASHBOARD_HTML,
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

    scheduler.start()

    print("Khemji Wire Reminder + Inventory App - running.")
    print(f"Furnace reminders daily at: {', '.join(FURNACE_REMINDER_TIMES)}")
    print(f"Production & Consumption reminders daily at: {', '.join(PROD_CONSUMPTION_REMINDER_TIMES)}")
    print(f"Electricity & Wire Rod reminders daily at: {', '.join(ELECTRICITY_REMINDER_TIMES)}")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
