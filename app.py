
from flask import Flask, request, jsonify
import os
from datetime import datetime
import requests

app = Flask(__name__)

CRON_SECRET = os.getenv("CRON_SECRET", "")
LINE_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
SHEET_ID = os.getenv("SHEET_ID", "")
SHEET_NAME = os.getenv("SHEET_NAME", "data")

# ===== MOCK DATA (replace with real sheet later) =====
mock_rows = []

# ===== UTIL =====
def safe_get_owner(row):
    try:
        return list(row.values())[12]
    except:
        return ""

def map_owner_to_province(owner):
    mapping = {
        "CMI": "เชียงใหม่",
        "CRI": "เชียงราย",
        "LPG": "ลำปาง",
        "NAN": "น่าน",
        "PYO": "พะเยา",
        "MHS": "แม่ฮ่องสอน",
        "LPN": "ลำพูน",
        "UTR": "อุตรดิตถ์",
        "UTT": "อุตรดิตถ์",
        "PRE": "แพร่",
        "PHE": "เพชรบูรณ์",
        "PSN": "พิษณุโลก",
        "PLK": "พิษณุโลก"
    }
    if not owner:
        return None
    owner = owner.upper()
    for k, v in mapping.items():
        if k in owner:
            return v
    return None

def push_line(text):
    if not LINE_TOKEN:
        return {"error": "no token"}

    headers = {
        "Authorization": f"Bearer {LINE_TOKEN}",
        "Content-Type": "application/json"
    }
    body = {
        "to": "C8887bdaae58b6d65a9bfa7a01c6aba62",
        "messages": [
            {"type": "text", "text": text}
        ]
    }
    res = requests.post("https://api.line.me/v2/bot/message/push", headers=headers, json=body)
    return {"status": res.status_code, "text": res.text}

# ===== ROUTES =====
@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "has_line_token": bool(LINE_TOKEN),
        "sheet_id_set": bool(SHEET_ID),
        "sheet_name": SHEET_NAME
    })

@app.route("/debug-secret")
def debug_secret():
    return jsonify({
        "cron_secret_loaded": CRON_SECRET,
        "has_line_token": bool(LINE_TOKEN),
        "sheet_id_set": bool(SHEET_ID),
        "sheet_name": SHEET_NAME
    })

@app.route("/test-push")
def test_push():
    secret = request.args.get("secret")
    province = request.args.get("province", "")

    if secret != CRON_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"})

    text = f"✅ ทดสอบ LINE NOC Bot\nห้อง: {province}\nเวลา: {datetime.now().strftime('%H:%M:%S')}"
    result = push_line(text)
    return jsonify({"ok": True, "result": result})

@app.route("/run-by-insert-time")
def run_job():
    secret = request.args.get("secret")
    if secret != CRON_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"})

    pushed = 0

    for row in mock_rows:
        owner = safe_get_owner(row)
        province = map_owner_to_province(owner)

        if province:
            text = f"🔥 Ticket ใหม่\nจังหวัด: {province}"
            push_line(text)
            pushed += 1

    return jsonify({
        "ok": True,
        "pushed_count": pushed
    })

@app.route("/debug-last-rows")
def debug_rows():
    return jsonify({
        "ok": True,
        "rows": mock_rows
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
