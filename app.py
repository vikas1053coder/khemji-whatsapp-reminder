"""
Khemji Wire - WhatsApp Temperature Reading Reminder (TRIAL VERSION)
Uses Twilio WhatsApp Sandbox - free, works with your own personal number, no Meta approval needed.

WHAT THIS DOES:
1. Sends a scheduled WhatsApp reminder to operator(s) asking for a temperature reading
2. Listens for their reply and logs it into an Excel file with operator name + timestamp
3. Runs continuously on your laptop while active

SETUP STEPS (one-time, ~10 minutes):
1. Create a free Twilio account: https://www.twilio.com/try-twilio
2. Go to Console > Messaging > Try it out > Send a WhatsApp message
   -> This gives you a Sandbox number and a join code (e.g. "join happy-tiger")
3. From YOUR phone's WhatsApp, send that join code to the Sandbox number shown.
   Do this from EVERY phone/operator number you want to test with.
4. Copy your Account SID and Auth Token from the Twilio Console dashboard.
5. Fill in the CONFIG section below.
6. Install requirements:  pip install flask twilio apscheduler openpyxl --break-system-packages
7. Run:  python app.py
8. For Twilio to reach your laptop with replies, expose port 5000 using ngrok:
      ngrok http 5000
   Copy the https ngrok URL, and paste it into Twilio Console > WhatsApp Sandbox Settings
   -> "WHEN A MESSAGE COMES IN" field, as:  https://<your-ngrok-id>.ngrok-free.app/reply
   (This step is only needed to auto-capture replies. Sending reminders works without it.)
"""

import os
import json
from datetime import datetime
from flask import Flask, request
from twilio.rest import Client
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import gspread
from google.oauth2.service_account import Credentials

# ============ CONFIG ============
# On Render, these come from Environment Variables (set in the dashboard), not hardcoded here.
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "your_account_sid_here")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "your_auth_token_here")
TWILIO_WHATSAPP_NUMBER = os.environ.get("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")

# Add each operator here: name + their WhatsApp number (must have joined the sandbox)
OPERATORS = [
    {"name": "Operator1", "number": "whatsapp:+91XXXXXXXXXX"},
    # {"name": "Operator2", "number": "whatsapp:+91XXXXXXXXXX"},
]

REMINDER_TIMES = ["08:00", "20:00"]   # 24-hour format, sends exactly at these times daily
REMINDER_MESSAGE = (
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

# Number(s) to notify when a reading is out of range (e.g. your own number)
ADMIN_NUMBERS = [
    "whatsapp:+91XXXXXXXXXX",  # replace with your number
]

# Google Sheets config
GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "your_google_sheet_id_here")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")  # paste full JSON content as env var on Render

EXPECTED_FIELDS = ["T1", "T2", "T3", "B1", "B2"]

# Acceptable ranges for numeric fields. Add more fields here if needed later.
TEMP_RANGES = {
    "T1": {"low": 445, "high": 455},
    # "T2": {"low": 600, "high": 660},
    # "T3": {"low": 490, "high": 520},
}
# ==================================================

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
app = Flask(__name__)


def get_sheet():
    """Connects to the Google Sheet using the service account credentials."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(GOOGLE_SHEET_ID).sheet1


def log_reading_to_sheet(row):
    """Appends one row of data to the Google Sheet."""
    sheet = get_sheet()
    sheet.append_row(row)


def parse_reply(message_body):
    """
    Parses replies like:
        T1=451
        T2=649
        T3=505
        B1=ON
        B2=OFF
    or comma-separated on one line: T1=451,T2=649,T3=505,B1=ON,B2=OFF
    Returns (values_dict, status_string)
    """
    values = {field: "" for field in EXPECTED_FIELDS}

    # normalize: treat commas, semicolons, and any line-break style as separators
    normalized = message_body.replace("\r\n", "\n").replace("\r", "\n")
    normalized = normalized.replace(",", "\n").replace(";", "\n")
    raw_parts = normalized.splitlines()

    for part in raw_parts:
        part = part.strip()
        if not part:
            continue
        # accept "=" or ":" as the separator between key and value
        sep = "=" if "=" in part else (":" if ":" in part else None)
        if not sep:
            continue
        key, _, val = part.partition(sep)
        key = key.strip().upper()
        val = val.strip().upper()
        if key in values:
            values[key] = val

    missing = [f for f in EXPECTED_FIELDS if not values[f]]
    status = "OK" if not missing else f"Missing: {', '.join(missing)}"
    return values, status


def check_ranges(values):
    """
    Compares numeric fields against TEMP_RANGES.
    Returns a list of alert strings, e.g. ["T1 HIGH: 462 (expected 445-455)"]
    Non-numeric or unconfigured fields are skipped.
    """
    alerts = []
    for field, limits in TEMP_RANGES.items():
        raw_val = values.get(field, "")
        if not raw_val:
            continue
        try:
            num_val = float(raw_val)
        except ValueError:
            continue  # not a number, skip range check (e.g. B1=ON is fine)

        if num_val < limits["low"]:
            alerts.append(f"{field} LOW: {raw_val} (expected {limits['low']}-{limits['high']})")
        elif num_val > limits["high"]:
            alerts.append(f"{field} HIGH: {raw_val} (expected {limits['low']}-{limits['high']})")
    return alerts


def send_reminders():
    print(f"[{datetime.now()}] Sending reminders...")
    for op in OPERATORS:
        try:
            client.messages.create(
                from_=TWILIO_WHATSAPP_NUMBER,
                to=op["number"],
                body=REMINDER_MESSAGE,
            )
            print(f"  -> Sent to {op['name']} ({op['number']})")
        except Exception as e:
            print(f"  -> FAILED for {op['name']}: {e}")


@app.route("/reply", methods=["POST"])
def receive_reply():
    """Twilio calls this webhook whenever an operator replies on WhatsApp."""
    from_number = request.form.get("From", "unknown")
    message_body = request.form.get("Body", "")

    values, status = parse_reply(message_body)
    alerts = check_ranges(values)
    alert_text = "; ".join(alerts) if alerts else "OK"

    row = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        from_number,
        values["T1"], values["T2"], values["T3"], values["B1"], values["B2"],
        message_body,
        status,
        alert_text,
    ]
    try:
        log_reading_to_sheet(row)
    except Exception as e:
        print(f"  -> FAILED to log to Google Sheet: {e}")

    print(f"[{datetime.now()}] Logged reply from {from_number}: {values} ({status}) Alerts: {alert_text}")

    # Forward every reply to admin(s) so you can see it directly on WhatsApp too
    forward_msg = (
        f"📋 New reading from {from_number}\n"
        f"T1={values['T1']}  T2={values['T2']}  T3={values['T3']}  "
        f"B1={values['B1']}  B2={values['B2']}\n"
        f"Status: {status}\n"
        f"Alerts: {alert_text}"
    )
    for admin in ADMIN_NUMBERS:
        try:
            client.messages.create(from_=TWILIO_WHATSAPP_NUMBER, to=admin, body=forward_msg)
        except Exception as e:
            print(f"  -> Could not forward reading to {admin}: {e}")

    # Send a separate, more urgent alarm message if anything is out of range
    if alerts:
        alarm_msg = f"🚨 ALARM - Reading out of range from {from_number}:\n" + "\n".join(alerts)
        for admin in ADMIN_NUMBERS:
            try:
                client.messages.create(from_=TWILIO_WHATSAPP_NUMBER, to=admin, body=alarm_msg)
            except Exception as e:
                print(f"  -> Could not send alarm to {admin}: {e}")

    # Let the operator know if something was missing, so they can resend
    if status != "OK":
        try:
            client.messages.create(
                from_=TWILIO_WHATSAPP_NUMBER,
                to=from_number,
                body=f"⚠️ Reading not fully understood. {status}. Please resend in the format:\nT1=451\nT2=649\nT3=505\nB1=ON\nB2=OFF",
            )
        except Exception as e:
            print(f"  -> Could not send correction notice: {e}")

    return ("", 204)


@app.route("/test-send", methods=["GET"])
def test_send():
    """Visit http://localhost:5000/test-send in a browser to manually trigger a reminder now."""
    send_reminders()
    return "Reminder sent! Check WhatsApp."


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    for t in REMINDER_TIMES:
        hour, minute = t.split(":")
        scheduler.add_job(send_reminders, CronTrigger(hour=int(hour), minute=int(minute)))
    scheduler.start()

    print("Khemji Wire WhatsApp Reminder - running.")
    print(f"Reminders will be sent daily at: {', '.join(REMINDER_TIMES)}")
    print("Replies will be logged to the configured Google Sheet.")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
