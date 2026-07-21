"""
Khemji Wire - Reminder + Mobile Entry Forms + Admin Dashboard
Runs 24/7 on Render. Uses Twilio WhatsApp for reminders (link-based), Google
Sheets for logging, separate tab per data category.

FOUR data categories, each with its own reminder schedule, recipients, and
Google Sheet tab:
  1. Furnace Reading         - 08:00 & 20:00 - Vikas, Prakash, Subodh   -> "Readings"
  2. Production & Consumption- 09:00          - Subodh, Prakash         -> "Production" + "Consumption"
  3. Electricity & Wire Rod  - 10:00          - Rana, Prakash           -> "ElectricityWireRod"
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
PRODUCTION_ITEMS = ["1.25 mm", "1.40 mm", "1.60 mm", "1.80 mm", "2.00 mm", "2.50 mm", "4.00 mm", "Strip 16 Kg"]

# ---- 3. ELECTRICITY & WIRE ROD reminder ----
ELECTRICITY_OPERATOR_NAMES = ["Rana", "Prakash"]
ELECTRICITY_REMINDER_TIMES = ["10:00"]
WIRE_ROD_SIZES = ["5.5 mm", "6.00 mm"]

ADMIN_NUMBERS = [ALL_PEOPLE["Vikas"]]

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "your_google_sheet_id_here")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

TAB_READINGS = "Readings"
TAB_PRODUCTION = "Production"
TAB_CONSUMPTION = "Consumption"
TAB_ELECTRICITY_WIRE_ROD = "ElectricityWireRod"
# ==================================================

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
app = Flask(__name__)


# ---------- Google Sheets helpers ----------
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
        ws = ss.add_worksheet(title=tab_name, rows=1000, cols=max(len(header_row), 10))
        ws.append_row(header_row)
    return ws


def log_row(tab_name, header_row, row):
    ws = get_or_create_tab(tab_name, header_row)
    ws.append_row(row)


def get_recent_rows(tab_name, header_row, limit=10):
    try:
        ws = get_or_create_tab(tab_name, header_row)
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return []
        return list(reversed(all_values[1:][-limit:]))
    except Exception as e:
        print(f"  -> Could not fetch rows from {tab_name}: {e}")
        return []


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


# ---------- Shared styling ----------
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
    background: var(--card); border: 1px solid var(--line); border-radius: 4px;
    max-width: 480px; width: 100%; padding: 28px 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
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
  input[type=number], input[type=text], select {
    width: 100%; padding: 12px 12px; font-size: 18px; border: 1.5px solid var(--line);
    border-radius: 3px; background: var(--paper); color: var(--ink);
  }
  input:focus, select:focus { outline: 2px solid var(--accent); outline-offset: 1px; border-color: var(--accent); }
  .toggle-group { display: flex; gap: 10px; }
  .toggle-btn {
    flex: 1; padding: 14px 0; text-align: center; font-size: 17px; font-weight: 700;
    border: 1.5px solid var(--line); border-radius: 3px; background: var(--paper); cursor: pointer; user-select: none;
  }
  .toggle-btn.selected { background: var(--accent); border-color: var(--accent-dark); color: white; }
  button.submit, button.secondary {
    width: 100%; margin-top: 26px; padding: 15px 0; font-size: 17px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.04em; background: var(--accent); color: white;
    border: none; border-radius: 3px; cursor: pointer;
  }
  button.secondary { background: var(--ink); margin-top: 12px; }
  button.add-item {
    width: 100%; margin-top: 10px; padding: 10px 0; font-size: 14px; font-weight: 700;
    text-transform: uppercase; background: transparent; color: var(--accent-dark);
    border: 1.5px dashed var(--accent); border-radius: 3px; cursor: pointer;
  }
  button.submit:active, button.secondary:active { opacity: 0.85; }
  .op-name { color: var(--accent-dark); font-weight: 700; }
  .success h1 { border-left-color: var(--ok); }
  .success .icon { font-size: 44px; margin-bottom: 6px; }
  .error-box { color: var(--bad); font-weight: 600; margin-top: 12px; font-size: 14px; }
  .extra-row { display: flex; gap: 8px; margin-top: 8px; flex-wrap: wrap; }
  .extra-row select, .extra-row input { flex: 1; min-width: 90px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px; }
  th, td { text-align: left; padding: 7px 5px; border-bottom: 1px solid var(--line); }
  th { text-transform: uppercase; letter-spacing: 0.03em; color: var(--ink-soft); font-size: 11px; }
  .dash-section { margin-top: 28px; }
  .badge-ok { color: var(--ok); font-weight: 700; }
  .badge-bad { color: var(--bad); font-weight: 700; }
  .home-btn { text-decoration: none; }
</style>
"""

# ---------- Home page ----------
HOME_HTML = BASE_STYLE + """
<div class="card">
  <h1>Khemji Wire</h1>
  <p class="subtitle">Select your name, then choose what to log</p>
  <select id="operatorSelect" style="width:100%;padding:12px;font-size:18px;border:1.5px solid var(--line);border-radius:3px;background:var(--paper);">
    {% for name in all_names %}
    <option value="{{ name }}">{{ name }}</option>
    {% endfor %}
  </select>
  <button class="submit" type="button" onclick="goTo('/furnace-form')">Furnace Reading</button>
  <button class="secondary" type="button" onclick="goTo('/production-form')">Production &amp; Consumption</button>
  <button class="secondary" type="button" onclick="goTo('/electricity-form')">Electricity &amp; Wire Rod</button>
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

    <h2 class="section">Additional Items (optional)</h2>
    <div id="extraItems"></div>
    <button class="add-item" type="button" onclick="addExtraRow()">+ Add Another Item</button>

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

  function addExtraRow() {
    var container = document.getElementById('extraItems');
    var row = document.createElement('div');
    row.className = 'extra-row';
    row.innerHTML =
      '<input type="text" name="extra_name[]" placeholder="Item name">' +
      '<input type="number" name="extra_qty[]" placeholder="Qty" step="0.1">';
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

    <h2 class="section">Electricity</h2>
    <label>Units Consumed</label>
    <input type="number" name="electricity_units" step="0.1" required>

    <h2 class="section">Wire Rod Issued (kg)</h2>
    <div id="wireRodRows"></div>
    <button class="add-item" type="button" onclick="addWireRodRow()">+ Add Wire Rod Entry</button>

    <button class="submit" type="submit">Submit Log</button>
  </form>
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
      var opt = document.createElement('option');
      opt.value = size;
      opt.textContent = size;
      select.appendChild(opt);
    });
    var otherOpt = document.createElement('option');
    otherOpt.value = 'Other';
    otherOpt.textContent = 'Other';
    select.appendChild(otherOpt);

    var customInput = document.createElement('input');
    customInput.type = 'text';
    customInput.name = 'wr_custom_size[]';
    customInput.placeholder = 'Specify size';
    customInput.style.display = 'none';

    var qtyInput = document.createElement('input');
    qtyInput.type = 'number';
    qtyInput.name = 'wr_qty[]';
    qtyInput.placeholder = 'Qty (kg)';
    qtyInput.step = '0.1';

    select.addEventListener('change', function() {
      customInput.style.display = (select.value === 'Other') ? 'block' : 'none';
    });

    row.appendChild(select);
    row.appendChild(customInput);
    row.appendChild(qtyInput);
    container.appendChild(row);
  }

  // Start with one row visible by default
  addWireRodRow();
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
</div>
"""

DASHBOARD_HTML = BASE_STYLE + """
<div class="card" style="max-width:960px;">
  <h1>Khemji Wire &mdash; Live Dashboard</h1>
  <p class="subtitle">Most recent entries, newest first</p>

  <div class="dash-section">
    <h2 class="section">Furnace Readings</h2>
    <table>
      <tr><th>Time</th><th>Operator</th><th>T1</th><th>T2</th><th>T3</th><th>B1</th><th>B1 Hrs</th><th>B2</th><th>B2 Hrs</th><th>Alerts</th></tr>
      {% for row in furnace_rows %}
      <tr>
        {% for cell in row %}<td class="{{ 'badge-bad' if loop.last and cell != 'OK' else '' }}">{{ cell }}</td>{% endfor %}
      </tr>
      {% endfor %}
    </table>
  </div>

  <div class="dash-section">
    <h2 class="section">Production</h2>
    <table>
      <tr>{% for h in production_header %}<th>{{ h }}</th>{% endfor %}</tr>
      {% for row in production_rows %}
      <tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>
      {% endfor %}
    </table>
  </div>

  <div class="dash-section">
    <h2 class="section">Consumption</h2>
    <table>
      <tr>{% for h in consumption_header %}<th>{{ h }}</th>{% endfor %}</tr>
      {% for row in consumption_rows %}
      <tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>
      {% endfor %}
    </table>
  </div>

  <div class="dash-section">
    <h2 class="section">Electricity &amp; Wire Rod</h2>
    <table>
      <tr>{% for h in electricity_header %}<th>{{ h }}</th>{% endfor %}</tr>
      {% for row in electricity_rows %}
      <tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>
      {% endfor %}
    </table>
  </div>
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
    return render_template_string(FURNACE_FORM_HTML, operator=operator)


@app.route("/submit-furnace", methods=["POST"])
def submit_furnace():
    operator = request.form.get("operator", "Unknown")
    values = {
        "T1": request.form.get("T1", ""), "T2": request.form.get("T2", ""), "T3": request.form.get("T3", ""),
        "B1": request.form.get("B1", ""), "B2": request.form.get("B2", ""),
    }
    b1_hours = request.form.get("B1_HOURS", "")
    b2_hours = request.form.get("B2_HOURS", "")

    alerts = check_ranges(values)
    alert_text = "; ".join(alerts) if alerts else "OK"

    row = [
        now_ist().strftime("%Y-%m-%d %H:%M:%S"), operator,
        values["T1"], values["T2"], values["T3"],
        values["B1"], b1_hours, values["B2"], b2_hours,
        alert_text,
    ]
    try:
        log_row(TAB_READINGS,
                ["Timestamp", "Operator", "T1", "T2", "T3", "B1", "B1 Hours", "B2", "B2 Hours", "Alerts"],
                row)
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
    return render_template_string(PRODUCTION_FORM_HTML, operator=operator,
                                   consumption_items=CONSUMPTION_ITEMS, production_items=PRODUCTION_ITEMS)


@app.route("/submit-production", methods=["POST"])
def submit_production():
    operator = request.form.get("operator", "Unknown")
    ts = now_ist().strftime("%Y-%m-%d %H:%M:%S")

    # Consumption
    cons_values = [request.form.get(f"cons_{i}", "0") for i in range(len(CONSUMPTION_ITEMS))]
    extra_cons_names = request.form.getlist("extra_cons_name[]")
    extra_cons_qtys = request.form.getlist("extra_cons_qty[]")
    extra_cons_pairs = [(n.strip(), q.strip()) for n, q in zip(extra_cons_names, extra_cons_qtys) if n.strip()]
    extra_cons_summary = "; ".join(f"{n}={q}" for n, q in extra_cons_pairs) if extra_cons_pairs else ""

    cons_row = [ts, operator] + cons_values + [extra_cons_summary]
    try:
        log_row(TAB_CONSUMPTION, ["Timestamp", "Operator"] + CONSUMPTION_ITEMS + ["Additional Consumables"], cons_row)
    except Exception as e:
        print(f"  -> FAILED to log consumption: {e}")

    # Production (fixed items)
    prod_values = [request.form.get(f"prod_{i}", "0") for i in range(len(PRODUCTION_ITEMS))]

    # Production (dynamic extra items)
    extra_names = request.form.getlist("extra_name[]")
    extra_qtys = request.form.getlist("extra_qty[]")
    extra_pairs = [(n.strip(), q.strip()) for n, q in zip(extra_names, extra_qtys) if n.strip()]

    total = 0.0
    for v in prod_values:
        try:
            total += float(v)
        except ValueError:
            pass
    for _, q in extra_pairs:
        try:
            total += float(q)
        except ValueError:
            pass

    extra_summary = "; ".join(f"{n}={q}" for n, q in extra_pairs) if extra_pairs else ""
    prod_row = [ts, operator] + prod_values + [extra_summary, round(total, 2)]
    try:
        log_row(TAB_PRODUCTION,
                ["Timestamp", "Operator"] + PRODUCTION_ITEMS + ["Additional Items", "Total Production"],
                prod_row)
    except Exception as e:
        print(f"  -> FAILED to log production: {e}")

    notify_admins(
        f"🏭 Production & Consumption from {operator}\n"
        f"Total Production: {round(total, 2)}\n"
        f"(Full breakdown logged to Google Sheet)"
    )

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Electricity & Wire Rod ----------
@app.route("/electricity-form", methods=["GET"])
def electricity_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(ELECTRICITY_FORM_HTML, operator=operator, wire_rod_sizes=WIRE_ROD_SIZES)


@app.route("/submit-electricity", methods=["POST"])
def submit_electricity():
    operator = request.form.get("operator", "Unknown")
    units = request.form.get("electricity_units", "")

    wr_sizes = request.form.getlist("wr_size[]")
    wr_customs = request.form.getlist("wr_custom_size[]")
    wr_qtys = request.form.getlist("wr_qty[]")

    wire_rod_entries = []
    for size, custom, qty in zip(wr_sizes, wr_customs, wr_qtys):
        if not qty:
            continue
        final_size = custom.strip() if size == "Other" and custom.strip() else size
        wire_rod_entries.append((final_size, qty))

    wire_rod_summary = "; ".join(f"{s}={q}" for s, q in wire_rod_entries) if wire_rod_entries else ""

    row = [now_ist().strftime("%Y-%m-%d %H:%M:%S"), operator, units, wire_rod_summary]
    try:
        log_row(TAB_ELECTRICITY_WIRE_ROD,
                ["Timestamp", "Operator", "Electricity Units", "Wire Rod Issued"],
                row)
    except Exception as e:
        print(f"  -> FAILED to log electricity/wire rod: {e}")

    notify_admins(
        f"⚡ Electricity & Wire Rod from {operator}\n"
        f"Units={units}\n"
        f"Wire Rod: {wire_rod_summary if wire_rod_summary else 'None entered'}"
    )

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Admin dashboard ----------
@app.route("/dashboard", methods=["GET"])
def dashboard():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)

    furnace_header = ["Timestamp", "Operator", "T1", "T2", "T3", "B1", "B1 Hours", "B2", "B2 Hours", "Alerts"]
    production_header = ["Timestamp", "Operator"] + PRODUCTION_ITEMS + ["Additional Items", "Total Production"]
    consumption_header = ["Timestamp", "Operator"] + CONSUMPTION_ITEMS + ["Additional Consumables"]
    electricity_header = ["Timestamp", "Operator", "Electricity Units", "Wire Rod Issued"]

    return render_template_string(
        DASHBOARD_HTML,
        furnace_rows=get_recent_rows(TAB_READINGS, furnace_header),
        production_rows=get_recent_rows(TAB_PRODUCTION, production_header),
        production_header=production_header,
        consumption_rows=get_recent_rows(TAB_CONSUMPTION, consumption_header),
        consumption_header=consumption_header,
        electricity_rows=get_recent_rows(TAB_ELECTRICITY_WIRE_ROD, electricity_header),
        electricity_header=electricity_header,
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

    print("Khemji Wire Reminder App - running.")
    print(f"Furnace reminders daily at: {', '.join(FURNACE_REMINDER_TIMES)}")
    print(f"Production & Consumption reminders daily at: {', '.join(PROD_CONSUMPTION_REMINDER_TIMES)}")
    print(f"Electricity & Wire Rod reminders daily at: {', '.join(ELECTRICITY_REMINDER_TIMES)}")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
