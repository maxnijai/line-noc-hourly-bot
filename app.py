
"""
Telegram NOC Ticket Notifier (Summary + Each Ticket)
- Google Sheet source
- New batch detection by max(insert_time)
- New ticket filter by CREATIONDATE > last_creation_time
- Duplicate protection with seen_tickets.json
- Sends:
  1) one summary message per province per batch
  2) then one message per ticket
- Allowed severities: SA1 SA2 SA3 SA4 NSA1 NSA2 NSA3 NSA4

Updated:
- /health stays lightweight for Railway
- ticket message removes SLA Status text
- adds LATITUDE, LONGITUDE, Aging_Flag_Group
- adds clickable Google Maps link
- highlights aging by OVER_SLA_Day color band
"""

import os, re, json, logging, requests, gspread
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
TZ = ZoneInfo("Asia/Bangkok")

CRON_SECRET = os.getenv("CRON_SECRET", "change-me")
SHEET_ID = os.getenv("SHEET_ID", "")
SHEET_NAME = os.getenv("SHEET_NAME", "data")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_PARSE_MODE = os.getenv("TELEGRAM_PARSE_MODE", "HTML")

COL_TICKET_ID = os.getenv("COL_TICKET_ID", "TICKETID")
COL_OWNER_GROUP = os.getenv("COL_OWNER_GROUP", "TRUEOWNERGROUP")
COL_CREATION = os.getenv("COL_CREATION", "CREATIONDATE")
COL_INSERT_TIME = os.getenv("COL_INSERT_TIME", "insert_time")
COL_TARGET_FINISH = os.getenv("COL_TARGET_FINISH", "TARGETFINISH")
COL_SEVERITY = os.getenv("COL_SEVERITY", "SEVERITY")
COL_SUBJECT = os.getenv("COL_SUBJECT", "SUBJECT")
COL_CINAME = os.getenv("COL_CINAME", "CINAME")
COL_CATEGORIES = os.getenv("COL_CATEGORIES", "CATEGORIES")
COL_REGION = os.getenv("COL_REGION", "Region")
COL_PROVINCE = os.getenv("COL_PROVINCE", "Province")
COL_OVER_SLA = os.getenv("COL_OVER_SLA", "OVER_SLA_Day")
COL_LATITUDE = os.getenv("COL_LATITUDE", "LATITUDE")
COL_LONGITUDE = os.getenv("COL_LONGITUDE", "LONGITUDE")
COL_AGING_GROUP = os.getenv("COL_AGING_GROUP", "Aging_Flag_Group")

OWNER_GROUP_FALLBACK_INDEX = int(os.getenv("OWNER_GROUP_FALLBACK_INDEX", "12"))
STATE_DIR = os.getenv("STATE_DIR", "/data")
STATE_FILE = os.path.join(STATE_DIR, "seen_tickets.json")
LAST_BATCH_FILE = os.path.join(STATE_DIR, "last_batch_state.json")

ALLOWED_SEVERITIES = {"SA1","SA2","SA3","SA4","NSA1","NSA2","NSA3","NSA4"}
SEVERITY_PRIORITY = {"SA1":1,"NSA1":2,"SA2":3,"NSA2":4,"SA3":5,"NSA3":6,"SA4":7,"NSA4":8}

CODE_TO_PROVINCE: Dict[str, Tuple[str, str]] = {
    "CMI": ("เชียงใหม่", "NOR1"), "CMI1": ("เชียงใหม่", "NOR1"), "CMI2": ("เชียงใหม่", "NOR1"),
    "CRI": ("เชียงราย", "NOR1"), "LPG": ("ลำปาง", "NOR1"), "LPN": ("ลำพูน", "NOR1"),
    "MHS": ("แม่ฮ่องสอน", "NOR1"), "NAN": ("น่าน", "NOR1"), "PHE": ("แพร่", "NOR1"),
    "PRE": ("แพร่", "NOR1"), "PYO": ("พะเยา", "NOR1"),
    "KPP": ("กำแพงเพชร", "NOR2"), "PCB": ("เพชรบูรณ์", "NOR2"), "PBN": ("เพชรบูรณ์", "NOR2"),
    "PCT": ("พิจิตร", "NOR2"), "PSN": ("พิษณุโลก", "NOR2"), "PHS": ("พิษณุโลก", "NOR2"),
    "PLK": ("พิษณุโลก", "NOR2"), "SKT": ("สุโขทัย", "NOR2"), "STI": ("สุโขทัย", "NOR2"),
    "TAK": ("ตาก", "NOR2"), "UTR": ("อุตรดิตถ์", "NOR2"), "UTT": ("อุตรดิตถ์", "NOR2"),
}
PROVINCE_NAMES = ["เชียงใหม่","เชียงราย","ลำปาง","ลำพูน","แม่ฮ่องสอน","น่าน","แพร่","พะเยา","กำแพงเพชร","เพชรบูรณ์","พิจิตร","พิษณุโลก","สุโขทัย","ตาก","อุตรดิตถ์"]
PROVINCE_TO_ENV = {
    "เชียงใหม่":"TELEGRAM_CHAT_ID_CMI","เชียงราย":"TELEGRAM_CHAT_ID_CRI","ลำปาง":"TELEGRAM_CHAT_ID_LPG","ลำพูน":"TELEGRAM_CHAT_ID_LPN",
    "แม่ฮ่องสอน":"TELEGRAM_CHAT_ID_MHS","น่าน":"TELEGRAM_CHAT_ID_NAN","แพร่":"TELEGRAM_CHAT_ID_PHE","พะเยา":"TELEGRAM_CHAT_ID_PYO",
    "กำแพงเพชร":"TELEGRAM_CHAT_ID_KPP","เพชรบูรณ์":"TELEGRAM_CHAT_ID_PCB","พิจิตร":"TELEGRAM_CHAT_ID_PCT","พิษณุโลก":"TELEGRAM_CHAT_ID_PSN",
    "สุโขทัย":"TELEGRAM_CHAT_ID_SKT","ตาก":"TELEGRAM_CHAT_ID_TAK","อุตรดิตถ์":"TELEGRAM_CHAT_ID_UTR"
}
CODE_TO_PROVINCE_ONLY = {k: v[0] for k, v in CODE_TO_PROVINCE.items()}
OWNER_RE = re.compile(r"TRUE-TH-[A-Z]+-(NOR[12])-([A-Z0-9]+)-", re.IGNORECASE)
OWNER_HINT_TO_PROVINCE = {
    "CMI":"เชียงใหม่","CRI":"เชียงราย","LPG":"ลำปาง","LPN":"ลำพูน","MHS":"แม่ฮ่องสอน","MSH":"แม่ฮ่องสอน",
    "NAN":"น่าน","PRE":"แพร่","PHE":"แพร่","PYO":"พะเยา","PHY":"พะเยา","KPP":"กำแพงเพชร","PCB":"เพชรบูรณ์",
    "PBN":"เพชรบูรณ์","PCT":"พิจิตร","PSN":"พิษณุโลก","PHS":"พิษณุโลก","PLK":"พิษณุโลก","SKT":"สุโขทัย",
    "STI":"สุโขทัย","TAK":"ตาก","UTR":"อุตรดิตถ์","UTT":"อุตรดิตถ์"
}
REGION_TO_DEFAULTS = {
    "NOR1": {"เชียงใหม่","เชียงราย","ลำปาง","ลำพูน","แม่ฮ่องสอน","น่าน","แพร่","พะเยา"},
    "NOR2": {"กำแพงเพชร","เพชรบูรณ์","พิจิตร","พิษณุโลก","สุโขทัย","ตาก","อุตรดิตถ์"},
}

app = Flask(__name__)

def _authorized():
    return request.args.get("secret", "") == CRON_SECRET

def _ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)

def _load_targets() -> Dict[str, str]:
    out = {}
    raw = os.getenv("TELEGRAM_TARGETS_JSON", "").strip()
    if raw:
        try:
            d = json.loads(raw)
            return {k: str(v).strip() for k, v in d.items() if str(v).strip()}
        except Exception as e:
            log.error(f"TELEGRAM_TARGETS_JSON invalid: {e}")
    for province, env_name in PROVINCE_TO_ENV.items():
        val = os.getenv(env_name, "").strip()
        if val:
            out[province] = val
    return out

def load_seen() -> Set[str]:
    try:
        _ensure_state_dir()
        if not os.path.exists(STATE_FILE):
            return set()
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        cutoff = (datetime.now(TZ) - timedelta(days=7)).isoformat()
        return {k for k, v in data.items() if v >= cutoff}
    except Exception as e:
        log.error(f"load_seen failed: {e}")
        return set()

def save_seen(ticket_ids: List[str]):
    try:
        _ensure_state_dir()
        data = {}
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        now_iso = datetime.now(TZ).isoformat()
        for tid in ticket_ids:
            data[tid] = now_iso
        cutoff = (datetime.now(TZ) - timedelta(days=7)).isoformat()
        data = {k: v for k, v in data.items() if v >= cutoff}
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"save_seen failed: {e}")

def load_last_batch_state() -> Tuple[Optional[datetime], Optional[datetime]]:
    try:
        _ensure_state_dir()
        if not os.path.exists(LAST_BATCH_FILE):
            return None, None
        with open(LAST_BATCH_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return (
            datetime.fromisoformat(data["last_insert_time"]) if data.get("last_insert_time") else None,
            datetime.fromisoformat(data["last_creation_time"]) if data.get("last_creation_time") else None,
        )
    except Exception as e:
        log.error(f"load_last_batch_state failed: {e}")
        return None, None

def save_last_batch_state(last_insert_time: Optional[datetime], last_creation_time: Optional[datetime]):
    try:
        _ensure_state_dir()
        with open(LAST_BATCH_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "last_insert_time": last_insert_time.isoformat() if last_insert_time else None,
                "last_creation_time": last_creation_time.isoformat() if last_creation_time else None,
                "updated_at": datetime.now(TZ).isoformat(),
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"save_last_batch_state failed: {e}")

def fetch_tickets() -> List[dict]:
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly","https://www.googleapis.com/auth/drive.readonly"]
    info = json.loads(GOOGLE_CREDS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME).get_all_records()

def parse_datetime(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=TZ)
    text = str(val).strip()
    if not text:
        return None
    for fmt in ["%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%d/%m/%Y %H:%M:%S","%d/%m/%Y %H:%M","%m/%d/%Y %H:%M:%S","%m/%d/%Y %H:%M","%Y/%m/%d %H:%M:%S","%Y/%m/%d %H:%M","%d-%m-%Y %H:%M:%S","%d-%m-%Y %H:%M"]:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=TZ)
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=TZ)
    except ValueError:
        return None

def normalize_severity(val) -> str:
    return str(val).strip().upper() if val is not None else ""

def safe_get_owner(row: dict) -> str:
    value = row.get(COL_OWNER_GROUP)
    if value not in (None, ""):
        return str(value).strip()
    for key in ["TRUEOWNERGROUP","TRUEOWNERGROUP ","TRUE OWNER GROUP","trueownnergroup","TRUEOWNNERGROUP"]:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    try:
        values = list(row.values())
        if OWNER_GROUP_FALLBACK_INDEX < len(values):
            value = values[OWNER_GROUP_FALLBACK_INDEX]
            return str(value).strip() if value not in (None, "") else ""
    except Exception:
        pass
    return ""

def normalize_requested_province(raw: str) -> Optional[str]:
    if not raw:
        return None
    text = str(raw).strip()
    if text in PROVINCE_NAMES:
        return text
    return CODE_TO_PROVINCE_ONLY.get(text.upper())

def parse_owner_group(owner: str, region_val: str = "", province_col_val: str = "") -> Tuple[Optional[str], Optional[str]]:
    pcol = str(province_col_val or "").strip()
    if pcol in PROVINCE_NAMES:
        region = str(region_val or "").strip().upper() or None
        return pcol, region
    if not owner:
        return None, None
    text = str(owner).strip().upper()
    region = str(region_val or "").strip().upper() or None
    m = OWNER_RE.search(text)
    if m:
        rg = m.group(1).upper()
        code = m.group(2).upper()
        if code in CODE_TO_PROVINCE:
            province, _ = CODE_TO_PROVINCE[code]
            return province, rg
    for code, (province, rg) in CODE_TO_PROVINCE.items():
        if code in text:
            return province, region or rg
    hits = []
    for hint, province in OWNER_HINT_TO_PROVINCE.items():
        if hint in text:
            hits.append(province)
    hits = list(dict.fromkeys(hits))
    if len(hits) == 1:
        return hits[0], region
    if region in REGION_TO_DEFAULTS and hits:
        region_hits = [p for p in hits if p in REGION_TO_DEFAULTS[region]]
        region_hits = list(dict.fromkeys(region_hits))
        if len(region_hits) == 1:
            return region_hits[0], region
    return None, region

def get_max_insert_time(rows: List[dict]) -> Optional[datetime]:
    mx = None
    for row in rows:
        ins = parse_datetime(row.get(COL_INSERT_TIME, ""))
        if ins and (mx is None or ins > mx):
            mx = ins
    return mx

def parse_over_sla(val) -> float:
    try:
        if val is None or str(val).strip() == "":
            return 0.0
        return float(str(val).strip())
    except Exception:
        return 0.0

def aging_label_from_over_sla(value) -> str:
    try:
        v = float(str(value).strip())
    except Exception:
        return "⚪ Unknown"
    if v > 30:
        return "🔴🔴 OverSLA > 30 days"
    elif v > 7:
        return "🔴 OverSLA < 30 days"
    elif v > 3:
        return "🟠 OverSLA < 7 days"
    elif v > 1:
        return "🟡 OverSLA < 3 days"
    elif v > 0:
        return "🟢 OverSLA < 1 day"
    else:
        return "🟢🟢 Within SLA"

def filter_new_tickets(rows: List[dict], seen: Set[str], last_creation_time: Optional[datetime]):
    result = []
    seen_in_batch = set()
    max_creation = last_creation_time
    for row in rows:
        tid = str(row.get(COL_TICKET_ID, "")).strip()
        if not tid or tid in seen or tid in seen_in_batch:
            continue
        sev = normalize_severity(row.get(COL_SEVERITY, ""))
        if sev not in ALLOWED_SEVERITIES:
            continue
        owner = safe_get_owner(row)
        province, region = parse_owner_group(owner, row.get(COL_REGION, ""), row.get(COL_PROVINCE, ""))
        if not province:
            continue
        created = parse_datetime(row.get(COL_CREATION, ""))
        if created is None:
            continue
        if last_creation_time is not None and created <= last_creation_time:
            continue
        row["_ticket_id"] = tid
        row["_province"] = province
        row["_region"] = region or "-"
        row["_created_dt"] = created
        row["_severity_norm"] = sev
        row["_owner_raw"] = owner
        result.append(row)
        seen_in_batch.add(tid)
        if max_creation is None or created > max_creation:
            max_creation = created
    return result, max_creation

def group_by_province(tickets: List[dict]) -> Dict[str, List[dict]]:
    buckets = {p: [] for p in PROVINCE_NAMES}
    for t in tickets:
        buckets[t["_province"]].append(t)
    return {p: v for p, v in buckets.items() if v}

def severity_icon(sev: str) -> str:
    return {"SA1":"🚨","NSA1":"🚨","SA2":"🔴","NSA2":"🔴","SA3":"🟠","NSA3":"🟠","SA4":"🟡","NSA4":"🟡"}.get(normalize_severity(sev), "📌")

def tg_escape(text: str) -> str:
    return str(text or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def shorten(text: str, limit: int = 120) -> str:
    text = str(text or "").strip()
    return text if len(text) <= limit else text[:limit-3] + "..."

def build_summary_message(province: str, region: str, tickets: List[dict], batch_insert_time: str) -> str:
    now = datetime.now(TZ).strftime("%d/%m/%Y %H:%M")
    total = len(tickets)
    sev_order = sorted(ALLOWED_SEVERITIES, key=lambda s: SEVERITY_PRIORITY.get(s, 999))
    counts = {sev: 0 for sev in sev_order}
    aging_counts = {
        "🔴🔴 OverSLA > 30 days": 0,
        "🔴 OverSLA < 30 days": 0,
        "🟠 OverSLA < 7 days": 0,
        "🟡 OverSLA < 3 days": 0,
        "🟢 OverSLA < 1 day": 0,
        "🟢🟢 Within SLA": 0,
        "⚪ Unknown": 0,
    }
    for t in tickets:
        counts[t["_severity_norm"]] = counts.get(t["_severity_norm"], 0) + 1
        aging_counts[aging_label_from_over_sla(t.get(COL_OVER_SLA, 0))] += 1
    summary_line = " | ".join([f"{sev}:{counts[sev]}" for sev in sev_order if counts[sev] > 0]) or "-"
    aging_line = " | ".join([f"{k}={v}" for k, v in aging_counts.items() if v > 0]) or "-"
    lines = [
        "🔔 <b>แจ้งเตือน Ticket รอบใหม่</b>",
        f"📍 จังหวัด: <b>{tg_escape(province)}</b> ({tg_escape(region)})",
        f"⏰ ส่งเมื่อ: {tg_escape(now)}",
        f"🕒 insert_time รอบนี้: <b>{tg_escape(batch_insert_time)}</b>",
        f"🎫 จำนวนที่ส่ง: <b>{total}</b> รายการ",
        f"📊 Severity: {tg_escape(summary_line)}",
        f"🎯 Aging: {tg_escape(aging_line)}",
    ]
    return "\n".join(lines)[:4000]

def build_ticket_message(t: dict, idx: int, total: int) -> str:
    tid = tg_escape(str(t.get(COL_TICKET_ID, "-")).strip() or "-")
    sev = t["_severity_norm"]
    created = tg_escape(str(t.get(COL_CREATION, "-")).strip() or "-")
    target = tg_escape(str(t.get(COL_TARGET_FINISH, "-")).strip() or "-")
    ci = tg_escape(shorten(t.get(COL_CINAME, "-"), 60))
    cat = tg_escape(shorten(t.get(COL_CATEGORIES, "-"), 60))
    subj = tg_escape(shorten(t.get(COL_SUBJECT, "-"), 200))
    lat_raw = str(t.get(COL_LATITUDE, "-")).strip() or "-"
    lon_raw = str(t.get(COL_LONGITUDE, "-")).strip() or "-"
    aging_group = tg_escape(str(t.get(COL_AGING_GROUP, "-")).strip() or "-")
    aging_label = aging_label_from_over_sla(t.get(COL_OVER_SLA, 0))
    lat = tg_escape(lat_raw)
    lon = tg_escape(lon_raw)
    icon = severity_icon(sev)

    if lat_raw not in {"", "-"} and lon_raw not in {"", "-"}:
        map_line = f"📌 <a href='https://maps.google.com/?q={lat_raw},{lon_raw}'>Open Google Maps</a>"
    else:
        map_line = "📌 Open Google Maps: -"

    lines = [
        f"{icon} <b>{idx}/{total}</b> | <b>{sev}</b> | <code>{tid}</code>",
        f"🕒 Create: {created}",
        f"⏳ Target: {target}",
        f"📍 Site: {ci}",
        f"🗂 {cat}",
        f"📝 {subj}",
        f"🧭 LATITUDE: {lat}",
        f"🧭 LONGITUDE: {lon}",
        map_line,
        f"🎯 Aging Group: {aging_group}",
        f"🚦 <b>{tg_escape(aging_label)}</b>",
    ]
    return "\n".join(lines)[:4000]

def push_telegram(chat_id: str, text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN missing")
        return False
    if not chat_id:
        log.error("push skipped: empty chat id")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": TELEGRAM_PARSE_MODE, "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code == 200:
            return True
        log.error(f"telegram push failed {r.status_code} → {str(chat_id)[:12]}...: {r.text}")
        return False
    except Exception as e:
        log.error(f"telegram push exception → {str(chat_id)[:12]}...: {e}")
        return False

def run_job() -> dict:
    targets = _load_targets()
    seen = load_seen()
    last_insert_time, last_creation_time = load_last_batch_state()
    rows = fetch_tickets()
    max_insert_time = get_max_insert_time(rows)
    if max_insert_time is None:
        return {"ok": False, "error": f"cannot find valid {COL_INSERT_TIME}"}
    if last_insert_time and max_insert_time <= last_insert_time:
        return {"ok": True, "mode": "insert_then_creation_summary_plus_each_telegram", "new_batch": False, "new": 0, "message": "insert_time not changed", "last_insert_time": last_insert_time.isoformat(), "current_max_insert_time": max_insert_time.isoformat(), "last_creation_time": last_creation_time.isoformat() if last_creation_time else None, "pushed": {}}
    new_tickets, max_creation_time = filter_new_tickets(rows, seen, last_creation_time)
    detail = {}
    pushed_ids = []
    batch_insert_text = max_insert_time.strftime("%Y-%m-%d %H:%M:%S")
    if new_tickets:
        by_province = group_by_province(new_tickets)
        for province, lst in by_province.items():
            target = targets.get(province)
            if not target:
                detail[province] = {"count": len(lst), "pushed": False, "reason": "no target configured"}
                continue
            region = lst[0]["_region"]
            ok_summary = push_telegram(target, build_summary_message(province, region, lst, batch_insert_text))
            ok_all = ok_summary
            sorted_tickets = sorted(lst, key=lambda x: (SEVERITY_PRIORITY.get(x["_severity_norm"], 999), x["_created_dt"]))
            for idx, t in enumerate(sorted_tickets, 1):
                ok_one = push_telegram(target, build_ticket_message(t, idx, len(sorted_tickets)))
                ok_all = ok_all and ok_one
            detail[province] = {"count": len(lst), "pushed": ok_all}
            if ok_all:
                pushed_ids.extend([t["_ticket_id"] for t in lst])
        if pushed_ids:
            save_seen(pushed_ids)
    save_last_batch_state(max_insert_time, max_creation_time if max_creation_time else last_creation_time)
    final_last_creation = max_creation_time if max_creation_time else last_creation_time
    return {"ok": True, "mode": "insert_then_creation_summary_plus_each_telegram", "new_batch": True, "new": len(new_tickets), "pushed_count": len(pushed_ids), "last_insert_time": max_insert_time.isoformat(), "last_creation_time": final_last_creation.isoformat() if final_last_creation else None, "allowed_severities": sorted(list(ALLOWED_SEVERITIES), key=lambda s: SEVERITY_PRIORITY.get(s, 999)), "detail": detail}

@app.route("/")
def home():
    return jsonify({"service":"Telegram NOC Ticket Notifier","status":"running","mode":"insert_then_creation_summary_plus_each_telegram","time":datetime.now(TZ).isoformat(),"summary_mode":True,"each_ticket_mode":True})

@app.route("/health")
def health():
    # lightweight for Railway
    targets = _load_targets()
    last_insert_time, last_creation_time = load_last_batch_state()
    return jsonify({
        "ok":True,
        "status":"healthy",
        "has_telegram_token":bool(TELEGRAM_BOT_TOKEN),
        "has_sheet_creds":bool(GOOGLE_CREDS_JSON),
        "sheet_id_set":bool(SHEET_ID),
        "sheet_name":SHEET_NAME,
        "insert_time_column":COL_INSERT_TIME,
        "creation_column":COL_CREATION,
        "targets_configured":len(targets),
        "targets_missing":[p for p in PROVINCE_NAMES if p not in targets],
        "last_insert_time":last_insert_time.isoformat() if last_insert_time else None,
        "last_creation_time":last_creation_time.isoformat() if last_creation_time else None,
    })

@app.route("/run-by-insert-time", methods=["GET","POST"])
def run_by_insert_time():
    if not _authorized():
        return jsonify({"ok":False,"error":"unauthorized"}), 401
    return jsonify(run_job())

@app.route("/test-push", methods=["GET"])
def test_push():
    if not _authorized():
        return jsonify({"ok":False,"error":"unauthorized"}), 401
    targets = _load_targets()
    only_raw = request.args.get("province", "").strip()
    requested = only_raw
    normalized = normalize_requested_province(only_raw) if only_raw else None
    if normalized:
        targets = {normalized: targets[normalized]} if normalized in targets else {}
    elif only_raw:
        targets = {}
    if not targets:
        return jsonify({"ok":False,"error":"no targets","requested":requested,"normalized":normalized,"available_targets":sorted(list(_load_targets().keys()))}), 400
    result = {}
    for province, chat_id in targets.items():
        result[province] = push_telegram(chat_id, f"✅ ทดสอบ Telegram NOC Bot\nห้อง: {province}\nเวลา: {datetime.now(TZ).strftime('%H:%M:%S')}")
    return jsonify({"ok":True,"requested":requested,"normalized":normalized,"result":result})

@app.route("/debug-last-rows", methods=["GET"])
def debug_last_rows():
    if not _authorized():
        return jsonify({"ok":False,"error":"unauthorized"}), 401
    limit = int(request.args.get("limit", "5"))
    rows = fetch_tickets()
    rows = rows[-limit:] if limit > 0 else rows
    out = []
    for row in rows:
        owner = safe_get_owner(row)
        province, region = parse_owner_group(owner, row.get(COL_REGION, ""), row.get(COL_PROVINCE, ""))
        sev = normalize_severity(row.get(COL_SEVERITY, ""))
        out.append({
            "TICKETID":row.get(COL_TICKET_ID,""),
            "CREATIONDATE":row.get(COL_CREATION,""),
            "insert_time":row.get(COL_INSERT_TIME,""),
            "SEVERITY":row.get(COL_SEVERITY,""),
            "severity_normalized":sev,
            "severity_allowed":sev in ALLOWED_SEVERITIES,
            "TRUEOWNERGROUP":owner,
            "Region":row.get(COL_REGION,""),
            "Province":row.get(COL_PROVINCE,""),
            "LATITUDE":row.get(COL_LATITUDE,""),
            "LONGITUDE":row.get(COL_LONGITUDE,""),
            "Aging_Flag_Group":row.get(COL_AGING_GROUP,""),
            "Aging_Status":aging_label_from_over_sla(row.get(COL_OVER_SLA, 0)),
            "creation_parsed":parse_datetime(row.get(COL_CREATION,"")).isoformat() if parse_datetime(row.get(COL_CREATION,"")) else None,
            "insert_parsed":parse_datetime(row.get(COL_INSERT_TIME,"")).isoformat() if parse_datetime(row.get(COL_INSERT_TIME,"")) else None,
            "parsed_province":province,
            "parsed_region":region
        })
    return jsonify({"ok":True,"count":len(out),"rows":out})

@app.route("/reset-state", methods=["GET","POST"])
def reset_state():
    if not _authorized():
        return jsonify({"ok":False,"error":"unauthorized"}), 401
    try:
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
        if os.path.exists(LAST_BATCH_FILE):
            os.remove(LAST_BATCH_FILE)
        return jsonify({"ok":True})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
