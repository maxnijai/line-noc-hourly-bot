
# FULL UPDATED VERSION (Telegram + Maps + Aging Color)

import os, requests
from flask import Flask

app = Flask(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_CMI", "")

def aging_label(v):
    try: v=float(v)
    except: return "Unknown"
    if v>30: return "🔴🔴 OverSLA > 30 days"
    elif v>7: return "🔴 OverSLA < 30 days"
    elif v>3: return "🟠 OverSLA < 7 days"
    elif v>1: return "🟡 OverSLA < 3 days"
    elif v>0: return "🟢 OverSLA < 1 day"
    else: return "🟢🟢 Within SLA"

def push(text):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url,json={"chat_id":CHAT_ID,"text":text,"parse_mode":"HTML"})

@app.route("/")
def home():
    return {"ok":True}

@app.route("/test")
def test():
    msg = "🚨 TEST\n"
    msg += "📍 <a href='https://maps.google.com/?q=18.5,98.2'>Open Map</a>\n"
    msg += "🚦 " + aging_label(35)
    push(msg)
    return {"ok":True}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
