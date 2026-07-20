"""
Khemji Wire - WhatsApp Reminder Automation
Runs on Render (24/7, no laptop needed). Uses Twilio WhatsApp Sandbox.

Handles TWO independent reminder flows:
1. FURNACE TEMPERATURE MONITORING - sent at 08:00 & 20:00 IST to furnace operators
2. PRODUCTION & ELECTRICITY LOG - sent at 10:00 IST to production operator

Replies are auto-detected by which fields they contain (T1-B2 = furnace,
E1/P1 = production) and logged to separate tabs in the same Google Sheet.
"""

import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request
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

# ---- FURNACE reminder (unchanged from before) ----
FURNACE_OPERATORS = [
    {"name": "Vikas", "number": "whatsapp:+919402168373"},
    {"name": "Prakash", "number": "whatsapp:+919829945873"},
    {"name": "Subodh", "number": "whatsapp:+918302822703"},
]
FURNACE_REMINDER_TIMES = ["08:00", "20:00"]
FURNACE_FIELDS = ["T1", "T2", "T3", "B1", "B2"]
FURNACE_REMINDER_MESSAGE = (
    "FURNACE TEMPERATURE MONITORING\n"
    "Please send the readings in the format\n"
    "T1,T2,T3,B1,B2\n\n"
    "Example\n"
    "T1=451\n"
    "T2=649\n"
    "T3=505\n"
    "B1=ON\n"
    "B2=OFF"
)
TEMP_RANGES = {
    "T1": {"low": 445, "high": 455},
    # "T2": {"low": 600, "high": 660},
    # "T3": {"low": 490, "high": 520},
}

# ---- PRODUCTION & ELECTRICITY reminder (new) ----
PRODUCTION_OPERATORS = [
    {"name": "Rana", "number": "whatsapp:+919667807024"},
]
PRODUCTION_REMINDER_TIMES = ["10:00"]
PRODUCTION_FIELDS = ["E1", "P1"]
PRODUCTION_REMINDER_MESSAGE = (
    "PRODUCTION & ELECTRICITY LOG\n"
    "Please send the readings in the format\n"
    "E1,P1\n\n"
    "Example\n"
    "E1=1250   (units consumed)\n"
    "P1=3400   (production quantity)"
)

# Number(s) to notify for every reply + alarms
ADMIN_NUMBERS = [
    "whatsapp:+919402168373",
]

# Google Sheets config
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "your_google_sheet_id_here")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

FURNACE_TAB_NAME = "Readings"
PRODUCTION_TAB_NAME = "Production"
# ==================================================

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
app = Flask(__name__)


def get_spreadsheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(GOOGLE_SHEET_ID)


def get_or_create_tab(tab_name, header_row):
    """Gets a worksheet tab by name, creating it with headers if it doesn't exist yet."""
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


def extract_key_values(message_body):
    """
    Generic parser: pulls out all KEY=VALUE (or KEY:VALUE) pairs from a message,
    regardless of line breaks / commas / semicolons used as separators.
    Returns a dict of UPPERCASE_KEY -> UPPERCASE_VALUE.
    """
    found = {}
    normalized = message_body.replace("\r\n", "\n").replace("\r", "\n")
    normalized = normalized.replace(",", "\n").replace(";", "\n")

    for part in normalized.splitlines():
        part = part.strip()
        if not part:
            continue
        sep = "=" if "=" in part else (":" if ":" in part else None)
        if not sep:
            continue
        key, _, val = part.partition(sep)
        found[key.strip().upper()] = val.strip().upper()

    return found


def build_values_and_status(all_kv, expected_fields):
    values = {f: all_kv.get(f, "") for f in expected_fields}
    missing = [f for f in expected_fields if not values[f]]
    status = "OK" if not missing else f"Missing: {', '.join(missing)}"
    return values, status


def check_ranges(values):
    """Compares numeric fields against TEMP_RANGES. Returns list of alert strings."""
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


def send_to_group(operators, message, label):
    print(f"[{now_ist()}] Sending {label} reminders...")
    for op in operators:
        try:
            client.messages.create(from_=TWILIO_WHATSAPP_NUMBER, to=op["number"], body=message)
            print(f"  -> Sent to {op['name']} ({op['number']})")
        except Exception as e:
            print(f"  -> FAILED for {op['name']}: {e}")


def send_furnace_reminders():
    send_to_group(FURNACE_OPERATORS, FURNACE_REMINDER_MESSAGE, "furnace")


def send_production_reminders():
    send_to_group(PRODUCTION_OPERATORS, PRODUCTION_REMINDER_MESSAGE, "production")


def notify_admins(message):
    for admin in ADMIN_NUMBERS:
        try:
            client.messages.create(from_=TWILIO_WHATSAPP_NUMBER, to=admin, body=message)
        except Exception as e:
            print(f"  -> Could not notify admin {admin}: {e}")


@app.route("/reply", methods=["POST"])
def receive_reply():
    """Twilio calls this webhook whenever anyone replies on WhatsApp. Auto-detects reading type."""
    from_number = request.form.get("From", "unknown")
    message_body = request.form.get("Body", "")

    all_kv = extract_key_values(message_body)
    is_furnace = any(k in all_kv for k in FURNACE_FIELDS)
    is_production = any(k in all_kv for k in PRODUCTION_FIELDS)

    if is_furnace:
        values, status = build_values_and_status(all_kv, FURNACE_FIELDS)
        alerts = check_ranges(values)
        alert_text = "; ".join(alerts) if alerts else "OK"

        row = [
            now_ist().strftime("%Y-%m-%d %H:%M:%S"), from_number,
            values["T1"], values["T2"], values["T3"], values["B1"], values["B2"],
            message_body, status, alert_text,
        ]
        try:
            log_row(FURNACE_TAB_NAME,
                    ["Timestamp", "Operator Number", "T1", "T2", "T3", "B1", "B2", "Raw Reply", "Parse Status", "Alerts"],
                    row)
        except Exception as e:
            print(f"  -> FAILED to log furnace reading: {e}")

        print(f"[{now_ist()}] Furnace reply from {from_number}: {values} ({status}) Alerts: {alert_text}")

        notify_admins(
            f"📋 Furnace reading from {from_number}\n"
            f"T1={values['T1']}  T2={values['T2']}  T3={values['T3']}  B1={values['B1']}  B2={values['B2']}\n"
            f"Status: {status}\nAlerts: {alert_text}"
        )
        if alerts:
            notify_admins(f"🚨 ALARM - Furnace reading out of range from {from_number}:\n" + "\n".join(alerts))
        if status != "OK":
            try:
                client.messages.create(
                    from_=TWILIO_WHATSAPP_NUMBER, to=from_number,
                    body=f"⚠️ Reading not fully understood. {status}. Please resend in the format:\nT1=451\nT2=649\nT3=505\nB1=ON\nB2=OFF",
                )
            except Exception as e:
                print(f"  -> Could not send correction notice: {e}")

    elif is_production:
        values, status = build_values_and_status(all_kv, PRODUCTION_FIELDS)

        row = [
            now_ist().strftime("%Y-%m-%d %H:%M:%S"), from_number,
            values["E1"], values["P1"], message_body, status,
        ]
        try:
            log_row(PRODUCTION_TAB_NAME,
                    ["Timestamp", "Operator Number", "E1 (Units Consumed)", "P1 (Production Qty)", "Raw Reply", "Parse Status"],
                    row)
        except Exception as e:
            print(f"  -> FAILED to log production reading: {e}")

        print(f"[{now_ist()}] Production reply from {from_number}: {values} ({status})")

        notify_admins(
            f"⚡ Production/Electricity reading from {from_number}\n"
            f"E1 (units)={values['E1']}  P1 (production)={values['P1']}\n"
            f"Status: {status}"
        )
        if status != "OK":
            try:
                client.messages.create(
                    from_=TWILIO_WHATSAPP_NUMBER, to=from_number,
                    body=f"⚠️ Reading not fully understood. {status}. Please resend in the format:\nE1=1250\nP1=3400",
                )
            except Exception as e:
                print(f"  -> Could not send correction notice: {e}")

    else:
        print(f"[{now_ist()}] Unrecognized reply from {from_number}: {message_body}")
        notify_admins(f"❓ Unrecognized reply from {from_number}:\n{message_body}")

    return ("", 204)


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

    print("Khemji Wire WhatsApp Reminder - running.")
    print(f"Furnace reminders daily at: {', '.join(FURNACE_REMINDER_TIMES)}")
    print(f"Production reminders daily at: {', '.join(PRODUCTION_REMINDER_TIMES)}")
    print("Replies will be logged to the configured Google Sheet (separate tabs).")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
