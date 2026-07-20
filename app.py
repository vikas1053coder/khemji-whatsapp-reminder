"""
Khemji Wire - Reminder + Mobile Entry Form + Admin Dashboard
Runs 24/7 on Render. Uses Twilio WhatsApp for reminders (with a link to a mobile
form, not typed replies), Google Sheets for logging.
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

# Base URL of THIS deployed app (used to build the link sent in reminders)
APP_BASE_URL = os.environ.get("APP_BASE_URL", "https://khemji-whatsapp-reminder.onrender.com")

# Simple admin key to protect the dashboard (not high security, just keeps it private)
ADMIN_KEY = os.environ.get("ADMIN_KEY", "changeme123")

FURNACE_OPERATORS = [
    {"name": "Vikas", "number": "whatsapp:+919402168373"},
    {"name": "Prakash", "number": "whatsapp:+919829945873"},
    {"name": "Subodh", "number": "whatsapp:+918302822703"},
]
FURNACE_REMINDER_TIMES = ["08:00", "20:00"]
TEMP_RANGES = {
    "T1": {"low": 445, "high": 455},
}

PRODUCTION_OPERATORS = [
    {"name": "Rana", "number": "whatsapp:+919667807024"},
]
PRODUCTION_REMINDER_TIMES = ["10:00"]

ADMIN_NUMBERS = [
    "whatsapp:+919402168373",
]

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "your_google_sheet_id_here")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

FURNACE_TAB_NAME = "Readings"
PRODUCTION_TAB_NAME = "Production"
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
        ws = ss.add_worksheet(title=tab_name, rows=1000, cols=len(header_row))
        ws.append_row(header_row)
    return ws


def log_row(tab_name, header_row, row):
    ws = get_or_create_tab(tab_name, header_row)
    ws.append_row(row)


def get_recent_rows(tab_name, header_row, limit=15):
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


def send_to_group(operators, label, link_builder):
    print(f"[{now_ist()}] Sending {label} reminders...")
    for op in operators:
        link = link_builder(op["name"])
        body = (
            f"{label.upper()} LOG\n"
            f"Tap the link below to enter your reading:\n{link}"
        )
        try:
            client.messages.create(from_=TWILIO_WHATSAPP_NUMBER, to=op["number"], body=body)
            print(f"  -> Sent to {op['name']} ({op['number']})")
        except Exception as e:
            print(f"  -> FAILED for {op['name']}: {e}")


def send_furnace_reminders():
    send_to_group(
        FURNACE_OPERATORS, "furnace temperature",
        lambda name: f"{APP_BASE_URL}/furnace-form?operator={quote(name)}"
    )


def send_production_reminders():
    send_to_group(
        PRODUCTION_OPERATORS, "production & electricity",
        lambda name: f"{APP_BASE_URL}/production-form?operator={quote(name)}"
    )


# ---------- Shared page styling (industrial, mobile-first) ----------
BASE_STYLE = """
<style>
  :root {
    --ink: #1F2937;
    --ink-soft: #4B5563;
    --paper: #F5F4F1;
    --card: #FFFFFF;
    --accent: #E85D04;
    --accent-dark: #C24A00;
    --line: #E2DFD8;
    --ok: #2E7D32;
    --bad: #C62828;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--paper);
    color: var(--ink);
    font-family: 'Barlow Condensed', 'Segoe UI', Arial, sans-serif;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 24px 16px 60px;
  }
  .card {
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 4px;
    max-width: 460px;
    width: 100%;
    padding: 28px 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  h1 {
    font-family: 'Barlow Condensed', sans-serif;
    font-weight: 700;
    font-size: 26px;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    color: var(--ink);
    margin: 0 0 4px;
    border-left: 5px solid var(--accent);
    padding-left: 12px;
  }
  .subtitle {
    color: var(--ink-soft);
    font-size: 16px;
    margin: 0 0 24px;
    padding-left: 17px;
  }
  label {
    display: block;
    font-size: 15px;
    font-weight: 600;
    color: var(--ink-soft);
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin: 18px 0 6px;
  }
  input[type=number], input[type=text] {
    width: 100%;
    padding: 14px 12px;
    font-size: 20px;
    border: 1.5px solid var(--line);
    border-radius: 3px;
    background: var(--paper);
    color: var(--ink);
  }
  input:focus { outline: 2px solid var(--accent); outline-offset: 1px; border-color: var(--accent); }
  .toggle-group { display: flex; gap: 10px; }
  .toggle-btn {
    flex: 1;
    padding: 14px 0;
    text-align: center;
    font-size: 17px;
    font-weight: 700;
    border: 1.5px solid var(--line);
    border-radius: 3px;
    background: var(--paper);
    cursor: pointer;
    user-select: none;
  }
  .toggle-btn.selected { background: var(--accent); border-color: var(--accent-dark); color: white; }
  button.submit {
    width: 100%;
    margin-top: 28px;
    padding: 16px 0;
    font-size: 18px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    background: var(--accent);
    color: white;
    border: none;
    border-radius: 3px;
    cursor: pointer;
  }
  button.submit:active { background: var(--accent-dark); }
  .op-name { color: var(--accent-dark); font-weight: 700; }
  .success h1 { border-left-color: var(--ok); }
  .success .icon { font-size: 44px; margin-bottom: 6px; }
  .error-box { color: var(--bad); font-weight: 600; margin-top: 12px; font-size: 14px; }
  table { width: 100%; border-collapse: collapse; font-size: 14px; margin-top: 14px; }
  th, td { text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--line); }
  th { text-transform: uppercase; letter-spacing: 0.03em; color: var(--ink-soft); font-size: 12px; }
  .dash-section { margin-top: 30px; }
  .badge-ok { color: var(--ok); font-weight: 700; }
  .badge-bad { color: var(--bad); font-weight: 700; }
</style>
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
    <label>B1</label>
    <div class="toggle-group" id="b1-group">
      <div class="toggle-btn" data-target="B1" data-value="ON">ON</div>
      <div class="toggle-btn" data-target="B1" data-value="OFF">OFF</div>
    </div>
    <input type="hidden" name="B1" id="B1" required>
    <label>B2</label>
    <div class="toggle-group" id="b2-group">
      <div class="toggle-btn" data-target="B2" data-value="ON">ON</div>
      <div class="toggle-btn" data-target="B2" data-value="OFF">OFF</div>
    </div>
    <input type="hidden" name="B2" id="B2" required>
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

PRODUCTION_FORM_HTML = BASE_STYLE + """
<div class="card">
  <h1>Production &amp; Electricity</h1>
  <p class="subtitle">Logging as <span class="op-name">{{ operator }}</span></p>
  <form method="POST" action="/submit-production">
    <input type="hidden" name="operator" value="{{ operator }}">
    <label>E1 &mdash; Units Consumed</label>
    <input type="number" name="E1" step="0.1" required>
    <label>P1 &mdash; Production Quantity</label>
    <input type="number" name="P1" step="0.1" required>
    <button class="submit" type="submit">Submit Reading</button>
  </form>
</div>
"""

SUCCESS_HTML = BASE_STYLE + """
<div class="card success">
  <div class="icon">&#9989;</div>
  <h1>Reading Logged</h1>
  <p class="subtitle">Thank you, {{ operator }}. Your reading has been recorded.</p>
  {% if alerts %}
    <p class="error-box">&#128680; {{ alerts }}</p>
  {% endif %}
</div>
"""

HOME_HTML = BASE_STYLE + """
<div class="card">
  <h1>Khemji Wire</h1>
  <p class="subtitle">Select your name, then choose what to log</p>
  <form id="homeForm">
    <label>Your Name</label>
    <select name="operator" id="operatorSelect" style="width:100%;padding:14px 12px;font-size:18px;border:1.5px solid var(--line);border-radius:3px;background:var(--paper);">
      {% for name in all_names %}
      <option value="{{ name }}">{{ name }}</option>
      {% endfor %}
    </select>
    <button class="submit" type="button" onclick="goTo('/furnace-form')">Furnace Reading</button>
    <button class="submit" type="button" style="margin-top:12px;background:var(--ink);" onclick="goTo('/production-form')">Production &amp; Electricity</button>
  </form>
</div>
<script>
  function goTo(path) {
    var name = document.getElementById('operatorSelect').value;
    window.location.href = path + '?operator=' + encodeURIComponent(name);
  }
</script>
"""

DASHBOARD_HTML = BASE_STYLE + """
<div class="card" style="max-width:900px;">
  <h1>Khemji Wire &mdash; Live Dashboard</h1>
  <p class="subtitle">Most recent entries, newest first</p>

  <div class="dash-section">
    <h2 style="font-family:'Barlow Condensed';text-transform:uppercase;">Furnace Readings</h2>
    <table>
      <tr><th>Time</th><th>Operator</th><th>T1</th><th>T2</th><th>T3</th><th>B1</th><th>B2</th><th>Alerts</th></tr>
      {% for row in furnace_rows %}
      <tr>
        <td>{{ row[0] }}</td><td>{{ row[1] }}</td><td>{{ row[2] }}</td><td>{{ row[3] }}</td>
        <td>{{ row[4] }}</td><td>{{ row[5] }}</td><td>{{ row[6] }}</td>
        <td class="{{ 'badge-ok' if row[9] == 'OK' else 'badge-bad' }}">{{ row[9] }}</td>
      </tr>
      {% endfor %}
    </table>
  </div>

  <div class="dash-section">
    <h2 style="font-family:'Barlow Condensed';text-transform:uppercase;">Production &amp; Electricity</h2>
    <table>
      <tr><th>Time</th><th>Operator</th><th>E1 (Units)</th><th>P1 (Production)</th></tr>
      {% for row in production_rows %}
      <tr><td>{{ row[0] }}</td><td>{{ row[1] }}</td><td>{{ row[2] }}</td><td>{{ row[3] }}</td></tr>
      {% endfor %}
    </table>
  </div>
</div>
"""


# ---------- App home (operator picker) ----------
@app.route("/app-home", methods=["GET"])
@app.route("/", methods=["GET"])
def app_home():
    all_names = sorted(set(
        [op["name"] for op in FURNACE_OPERATORS] + [op["name"] for op in PRODUCTION_OPERATORS]
    ))
    return render_template_string(HOME_HTML, all_names=all_names)


# ---------- Routes: Furnace ----------
@app.route("/furnace-form", methods=["GET"])
def furnace_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(FURNACE_FORM_HTML, operator=operator)


@app.route("/submit-furnace", methods=["POST"])
def submit_furnace():
    operator = request.form.get("operator", "Unknown")
    values = {
        "T1": request.form.get("T1", ""),
        "T2": request.form.get("T2", ""),
        "T3": request.form.get("T3", ""),
        "B1": request.form.get("B1", ""),
        "B2": request.form.get("B2", ""),
    }
    alerts = check_ranges(values)
    alert_text = "; ".join(alerts) if alerts else "OK"

    row = [
        now_ist().strftime("%Y-%m-%d %H:%M:%S"), operator,
        values["T1"], values["T2"], values["T3"], values["B1"], values["B2"],
        "(submitted via app)", "OK", alert_text,
    ]
    try:
        log_row(FURNACE_TAB_NAME,
                ["Timestamp", "Operator", "T1", "T2", "T3", "B1", "B2", "Raw Reply", "Parse Status", "Alerts"],
                row)
    except Exception as e:
        print(f"  -> FAILED to log furnace reading: {e}")

    notify_admins(
        f"📋 Furnace reading from {operator}\n"
        f"T1={values['T1']}  T2={values['T2']}  T3={values['T3']}  B1={values['B1']}  B2={values['B2']}\n"
        f"Alerts: {alert_text}"
    )
    if alerts:
        notify_admins(f"🚨 ALARM - Furnace reading out of range from {operator}:\n" + "\n".join(alerts))

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=alert_text if alerts else None)


# ---------- Routes: Production ----------
@app.route("/production-form", methods=["GET"])
def production_form():
    operator = request.args.get("operator", "Operator")
    return render_template_string(PRODUCTION_FORM_HTML, operator=operator)


@app.route("/submit-production", methods=["POST"])
def submit_production():
    operator = request.form.get("operator", "Unknown")
    e1 = request.form.get("E1", "")
    p1 = request.form.get("P1", "")

    row = [now_ist().strftime("%Y-%m-%d %H:%M:%S"), operator, e1, p1, "(submitted via app)", "OK"]
    try:
        log_row(PRODUCTION_TAB_NAME,
                ["Timestamp", "Operator", "E1 (Units Consumed)", "P1 (Production Qty)", "Raw Reply", "Parse Status"],
                row)
    except Exception as e:
        print(f"  -> FAILED to log production reading: {e}")

    notify_admins(f"⚡ Production/Electricity reading from {operator}\nE1 (units)={e1}  P1 (production)={p1}")

    return render_template_string(SUCCESS_HTML, operator=operator, alerts=None)


# ---------- Admin dashboard ----------
@app.route("/dashboard", methods=["GET"])
def dashboard():
    if request.args.get("key") != ADMIN_KEY:
        abort(403)
    furnace_rows = get_recent_rows(
        FURNACE_TAB_NAME,
        ["Timestamp", "Operator", "T1", "T2", "T3", "B1", "B2", "Raw Reply", "Parse Status", "Alerts"],
    )
    production_rows = get_recent_rows(
        PRODUCTION_TAB_NAME,
        ["Timestamp", "Operator", "E1 (Units Consumed)", "P1 (Production Qty)", "Raw Reply", "Parse Status"],
    )
    return render_template_string(DASHBOARD_HTML, furnace_rows=furnace_rows, production_rows=production_rows)


# ---------- Manual test triggers ----------
@app.route("/test-send-furnace", methods=["GET"])
def test_send_furnace():
    send_furnace_reminders()
    return "Furnace reminder sent! Check WhatsApp."


@app.route("/test-send-production", methods=["GET"])
def test_send_production():
    send_production_reminders()
    return "Production reminder sent! Check WhatsApp."


if __name__ == "__main__":
    scheduler = BackgroundScheduler()

    for t in FURNACE_REMINDER_TIMES:
        hour, minute = t.split(":")
        scheduler.add_job(send_furnace_reminders, CronTrigger(hour=int(hour), minute=int(minute), timezone=IST))

    for t in PRODUCTION_REMINDER_TIMES:
        hour, minute = t.split(":")
        scheduler.add_job(send_production_reminders, CronTrigger(hour=int(hour), minute=int(minute), timezone=IST))

    scheduler.start()

    print("Khemji Wire Reminder + Mobile Form App - running.")
    print(f"Furnace reminders daily at: {', '.join(FURNACE_REMINDER_TIMES)}")
    print(f"Production reminders daily at: {', '.join(PRODUCTION_REMINDER_TIMES)}")
    print(f"Admin dashboard: {APP_BASE_URL}/dashboard?key=YOUR_ADMIN_KEY")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
