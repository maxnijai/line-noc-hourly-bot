"""
LINE NOC Ticket Notifier (Multi-room)
Logic:
1) ใช้ max(insert_time) เป็นตัวเช็คว่าชีตมี batch ใหม่เข้ามาหรือยัง
2) ถ้า batch ใหม่เข้ามา ค่อยคัด ticket ด้วย CREATIONDATE ที่ใหม่กว่ารอบก่อน
3) กันส่งซ้ำด้วย seen_tickets.json
"""

import os
import re
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, request, jsonify
import requests

# ---------- CONFIG ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

TZ = ZoneInfo("Asia/Bangkok")

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
CRON_SECRET = os.getenv("CRON_SECRET", "change-me")

SHEET_ID = os.getenv("SHEET_ID", "")
SHEET_NAME = os.getenv("SHEET_NAME", "Sheet1")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "")

# ชื่อคอลัมน์ใน sheet (ปรับตาม header จริง)
COL_TICKET_ID = os.getenv("COL_TICKET_ID", "TICKETID")
COL_OWNER_GROUP = os.getenv("COL_OWNER_GROUP", "trueownnergroup")
COL_CREATION = os.getenv("COL_CREATION", "CREATIONDATE")
COL_INSERT_TIME = os.getenv("COL_INSERT_TIME", "insert_time")
COL_TARGET_FINISH = os.getenv("COL_TARGET_FINISH", "TARGETFINISH")
COL_SEVERITY = os.getenv("COL_SEVERITY", "SEVERITY")
COL_SUBJECT = os.getenv("COL_SUBJECT", "SUBJECT")
COL_CINAME = os.getenv("COL_CINAME", "CINAME")
COL_CATEGORIES = os.getenv("COL_CATEGORIES", "CATEGORIES")
COL_LATITUDE = os.getenv("COL_LATITUDE", "LATITUDE")
COL_LONGITUDE = os.getenv("COL_LONGITUDE", "LONGITUDE")

STATE_DIR = os.getenv("STATE_DIR", "/data")
STATE_FILE = os.path.join(STATE_DIR, "seen_tickets.json")
LAST_BATCH_FILE = os.path.join(STATE_DIR, "last_batch_state.json")

# ---------- 15 PROVINCE CONFIG ----------
CODE_TO_PROVINCE: Dict[str, Tuple[str, str]] = {
    "CMI1": ("เชียงใหม่", "NOR1"),
    "CMI2": ("เชียงใหม่", "NOR1"),
    "CRI":  ("เชียงราย", "NOR1"),
    "LPG":  ("ลำปาง", "NOR1"),
    "LPN":  ("ลำพูน", "NOR1"),
    "MHS":  ("แม่ฮ่องสอน", "NOR1"),
    "NAN":  ("น่าน", "NOR1"),
    "PHE":  ("แพร่", "NOR1"),
    "PYO":  ("พะเยา", "NOR1"),
    "KPP":  ("กำแพงเพชร", "NOR2"),
    "PCB":  ("เพชรบูรณ์", "NOR2"),
    "PCT":  ("พิจิตร", "NOR2"),
    "PSN":  ("พิษณุโลก", "NOR2"),
    "SKT":  ("สุโขทัย", "NOR2"),
    "TAK":  ("ตาก", "NOR2"),
    "UTR":  ("อุตรดิตถ์", "NOR2"),
}

PROVINCE_NAMES_NOR1 = [
    "เชียงใหม่", "เชียงราย", "ลำปาง", "ลำพูน",
    "แม่ฮ่องสอน", "น่าน", "แพร่", "พะเยา"
]
PROVINCE_NAMES_NOR2 = [
    "กำแพงเพชร", "เพชรบูรณ์", "พิจิตร", "พิษณุโลก",
    "สุโขทัย", "ตาก", "อุตรดิตถ์"
]
PROVINCE_NAMES = PROVINCE_NAMES_NOR1 + PROVINCE_NAMES_NOR2

OWNER_RE = re.compile(r"TRUE-TH-BBT-(NOR[12])-([A-Z]+[0-9]*)-", re.IGNORECASE)


# ---------- CONFIG HELPERS ----------
def _load_line_targets() -> Dict[str, str]:
    raw = os.getenv("LINE_TARGETS_JSON", "").strip()
    if raw:
        try:
            d = json.loads(raw)
            return {k: v for k, v in d.items() if v}
        except Exception as e:
            log.error(f"LINE_TARGETS_JSON invalid: {e}")

    name_map = {
        "เชียงใหม่": "LINE_TARGET_CMI",
        "เชียงราย": "LINE_TARGET_CRI",
        "ลำปาง": "LINE_TARGET_LPG",
        "ลำพูน": "LINE_TARGET_LPN",
        "แม่ฮ่องสอน": "LINE_TARGET_MHS",
        "น่าน": "LINE_TARGET_NAN",
        "แพร่": "LINE_TARGET_PHE",
        "พะเยา": "LINE_TARGET_PYO",
        "กำแพงเพชร": "LINE_TARGET_KPP",
        "เพชรบูรณ์": "LINE_TARGET_PCB",
        "พิจิตร": "LINE_TARGET_PCT",
        "พิษณุโลก": "LINE_TARGET_PSN",
        "สุโขทัย": "LINE_TARGET_SKT",
        "ตาก": "LINE_TARGET_TAK",
        "อุตรดิตถ์": "LINE_TARGET_UTR",
    }
    out = {}
    for province, env_name in name_map.items():
        val = os.getenv(env_name, "").strip()
        if val:
            out[province] = val
    return out


# ---------- STATE ----------
def _ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)


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


def save_seen(new_ids: List[str]) -> None:
    try:
        _ensure_state_dir()
        data = {}
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        now_iso = datetime.now(TZ).isoformat()
        for tid in new_ids:
            data[tid] = now_iso
        cutoff = (datetime.now(TZ) - timedelta(days=7)).isoformat()
        data = {k: v for k, v in data.items() if v >= cutoff}
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"save_seen failed: {e}")


def load_last_batch_state() -> Tuple[Optional[datetime], Optional[datetime]]:
    """คืน (last_insert_time, last_creation_time)"""
    try:
        _ensure_state_dir()
        if not os.path.exists(LAST_BATCH_FILE):
            return None, None
        with open(LAST_BATCH_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        last_insert = data.get("last_insert_time")
        last_creation = data.get("last_creation_time")
        return (
            datetime.fromisoformat(last_insert) if last_insert else None,
            datetime.fromisoformat(last_creation) if last_creation else None,
        )
    except Exception as e:
        log.error(f"load_last_batch_state failed: {e}")
        return None, None


def save_last_batch_state(last_insert_time: Optional[datetime], last_creation_time: Optional[datetime]) -> None:
    try:
        _ensure_state_dir()
        payload = {
            "last_insert_time": last_insert_time.isoformat() if last_insert_time else None,
            "last_creation_time": last_creation_time.isoformat() if last_creation_time else None,
            "updated_at": datetime.now(TZ).isoformat(),
        }
        with open(LAST_BATCH_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"save_last_batch_state failed: {e}")


# ---------- GOOGLE SHEET ----------
def fetch_tickets() -> List[dict]:
    if not GOOGLE_CREDS_JSON:
        raise RuntimeError("GOOGLE_CREDS_JSON env var not set")
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID env var not set")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    info = json.loads(GOOGLE_CREDS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(SHEET_NAME)
    records = ws.get_all_records()
    log.info(f"fetched {len(records)} rows")
    return records


# ---------- PARSING ----------
def parse_owner_group(val: str) -> Tuple[Optional[str], Optional[str]]:
    if not val:
        return None, None
    m = OWNER_RE.search(str(val))
    if not m:
        return None, None
    code = m.group(2).upper()
    if code not in CODE_TO_PROVINCE:
        return None, None
    return CODE_TO_PROVINCE[code]


def parse_datetime(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=TZ)

    text = str(val).strip()
    if not text:
        return None

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=TZ)
        except ValueError:
            continue

    try:
        dt = datetime.fromisoformat(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=TZ)
    except ValueError:
        return None


# ---------- FILTERING ----------
def get_max_insert_time(rows: List[dict]) -> Optional[datetime]:
    max_insert: Optional[datetime] = None
    for row in rows:
        ins = parse_datetime(row.get(COL_INSERT_TIME, ""))
        if ins is None:
            continue
        if max_insert is None or ins > max_insert:
            max_insert = ins
    return max_insert


def filter_new_tickets_for_new_batch(
    rows: List[dict],
    seen: Set[str],
    last_creation_time: Optional[datetime],
) -> Tuple[List[dict], Optional[datetime]]:
    """
    เมื่อรู้แล้วว่า insert_time เปลี่ยน (เข้ารอบใหม่)
    ให้คัด ticket ที่:
      1) มี TICKETID และยังไม่เคยแจ้ง
      2) trueownnergroup ตรง 15 จังหวัด
      3) CREATIONDATE ใหม่กว่ารอบก่อน
    """
    result = []
    seen_in_batch: Set[str] = set()
    max_creation: Optional[datetime] = last_creation_time

    for row in rows:
        tid = str(row.get(COL_TICKET_ID, "")).strip()
        if not tid or tid in seen or tid in seen_in_batch:
            continue

        province, region = parse_owner_group(row.get(COL_OWNER_GROUP, ""))
        if not province:
            continue

        created = parse_datetime(row.get(COL_CREATION, ""))
        if created is None:
            continue

        if last_creation_time and created <= last_creation_time:
            continue

        row["_ticket_id"] = tid
        row["_province"] = province
        row["_region"] = region
        row["_created_dt"] = created
        result.append(row)
        seen_in_batch.add(tid)

        if max_creation is None or created > max_creation:
            max_creation = created

    return result, max_creation


def group_by_province(tickets: List[dict]) -> Dict[str, List[dict]]:
    buckets: Dict[str, List[dict]] = {p: [] for p in PROVINCE_NAMES}
    for t in tickets:
        buckets[t["_province"]].append(t)
    return {p: v for p, v in buckets.items() if v}


# ---------- MESSAGE FORMAT ----------
def format_ticket_text(t: dict, index: int, total: int) -> str:
    def g(key: str) -> str:
        v = t.get(key, "")
        return str(v).strip() if v not in (None, "") else "-"

    lat = g(COL_LATITUDE)
    lon = g(COL_LONGITUDE)
    maps_line = ""
    if lat != "-" and lon != "-":
        try:
            float(lat)
            float(lon)
            maps_line = f"\n🗺 https://maps.google.com/?q={lat},{lon}"
        except ValueError:
            pass

    severity = g(COL_SEVERITY)
    header = f"📌 {index}/{total} | {severity}"

    return (
        f"{header}\n"
        f"TICKETID\n{g(COL_TICKET_ID)}\n"
        f"CREATIONDATE\n{g(COL_CREATION)}\n"
        f"INSERT_TIME\n{g(COL_INSERT_TIME)}\n"
        f"TARGETFINISH\n{g(COL_TARGET_FINISH)}\n"
        f"SEVERITY\n{severity}\n"
        f"SUBJECT\n{g(COL_SUBJECT)}\n"
        f"CINAME\n{g(COL_CINAME)}\n"
        f"CATEGORIES\n{g(COL_CATEGORIES)}\n"
        f"LATITUDE\n{lat}\n"
        f"LONGITUDE\n{lon}"
        f"{maps_line}"
    )


def build_messages_for_province(province: str, region: str, tickets: List[dict]) -> List[dict]:
    now = datetime.now(TZ).strftime("%d/%m/%Y %H:%M")
    total = len(tickets)
    header = (
        f"🔔 Ticket ใหม่ — {province} ({region})\n"
        f"⏰ {now}  |  🎫 {total} รายการ"
    )
    msgs = [{"type": "text", "text": header}]
    sorted_tickets = sorted(tickets, key=lambda x: x.get("_created_dt", datetime.min.replace(tzinfo=TZ)))
    for i, t in enumerate(sorted_tickets, 1):
        txt = format_ticket_text(t, i, total)
        if len(txt) > 4900:
            txt = txt[:4900] + "\n...(truncated)"
        msgs.append({"type": "text", "text": txt})
    return msgs


# ---------- LINE PUSH ----------
def push_line(target_id: str, messages: List[dict]) -> bool:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        log.error("LINE_CHANNEL_ACCESS_TOKEN missing")
        return False
    if not target_id:
        return False

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }
    ok_all = True

    for i in range(0, len(messages), 5):
        batch = messages[i:i + 5]
        try:
            r = requests.post(
                url,
                headers=headers,
                json={"to": target_id, "messages": batch},
                timeout=15,
            )
            if r.status_code == 200:
                log.info(f"push ok → {target_id[:10]}... ({len(batch)} msgs)")
            else:
                log.error(f"push failed {r.status_code} → {target_id[:10]}...: {r.text}")
                ok_all = False
        except Exception as e:
            log.error(f"push exception → {target_id[:10]}...: {e}")
            ok_all = False
    return ok_all


# ---------- MAIN JOB ----------
def run_insert_creation_job() -> dict:
    log.info("=== insert+creation job start ===")
    targets = _load_line_targets()
    log.info(f"configured targets: {list(targets.keys())}")

    seen = load_seen()
    last_insert_time, last_creation_time = load_last_batch_state()

    try:
        rows = fetch_tickets()
    except Exception as e:
        log.error(f"fetch_tickets failed: {e}")
        return {"ok": False, "error": str(e)}

    max_insert_time = get_max_insert_time(rows)
    if max_insert_time is None:
        return {
            "ok": False,
            "error": f"cannot find valid {COL_INSERT_TIME}",
        }

    if last_insert_time and max_insert_time <= last_insert_time:
        return {
            "ok": True,
            "mode": "insert_then_creation",
            "new_batch": False,
            "new": 0,
            "message": "insert_time not changed",
            "last_insert_time": last_insert_time.isoformat(),
            "current_max_insert_time": max_insert_time.isoformat(),
            "last_creation_time": last_creation_time.isoformat() if last_creation_time else None,
            "pushed": {},
        }

    new_tickets, max_creation_time = filter_new_tickets_for_new_batch(
        rows=rows,
        seen=seen,
        last_creation_time=last_creation_time,
    )
    log.info(f"new batch detected, new tickets by CREATIONDATE: {len(new_tickets)}")

    detail: Dict[str, dict] = {}
    pushed_ids: List[str] = []

    if new_tickets:
        by_province = group_by_province(new_tickets)
        for province, lst in by_province.items():
            target = targets.get(province)
            if not target:
                log.warning(f"no LINE target for {province} → skip {len(lst)} tickets")
                detail[province] = {
                    "count": len(lst),
                    "pushed": False,
                    "reason": "no target configured",
                }
                continue

            region = lst[0]["_region"]
            msgs = build_messages_for_province(province, region, lst)
            ok = push_line(target, msgs)
            detail[province] = {"count": len(lst), "pushed": ok}
            if ok:
                pushed_ids.extend(t["_ticket_id"] for t in lst)

        if pushed_ids:
            save_seen(pushed_ids)

    new_last_creation = max_creation_time if max_creation_time else last_creation_time
    save_last_batch_state(max_insert_time, new_last_creation)

    return {
        "ok": True,
        "mode": "insert_then_creation",
        "new_batch": True,
        "new": len(new_tickets),
        "pushed_count": len(pushed_ids),
        "last_insert_time": max_insert_time.isoformat(),
        "last_creation_time": new_last_creation.isoformat() if new_last_creation else None,
        "detail": detail,
    }


# ---------- FLASK ----------
app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({
        "service": "LINE NOC Ticket Notifier",
        "status": "running",
        "mode": "insert_then_creation",
        "time": datetime.now(TZ).isoformat(),
    })


@app.route("/health")
def health():
    targets = _load_line_targets()
    seen = load_seen()
    last_insert_time, last_creation_time = load_last_batch_state()
    return jsonify({
        "ok": True,
        "seen_count": len(seen),
        "has_line_token": bool(LINE_CHANNEL_ACCESS_TOKEN),
        "has_sheet_creds": bool(GOOGLE_CREDS_JSON),
        "sheet_id_set": bool(SHEET_ID),
        "sheet_name": SHEET_NAME,
        "insert_time_column": COL_INSERT_TIME,
        "creation_column": COL_CREATION,
        "targets_configured": len(targets),
        "targets_missing": [p for p in PROVINCE_NAMES if p not in targets],
        "last_insert_time": last_insert_time.isoformat() if last_insert_time else None,
        "last_creation_time": last_creation_time.isoformat() if last_creation_time else None,
    })


@app.route("/run", methods=["GET", "POST"])
def run_endpoint():
    if request.args.get("secret", "") != CRON_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    return jsonify(run_insert_creation_job())


@app.route("/run-by-insert-time", methods=["GET", "POST"])
def run_by_insert_time():
    if request.args.get("secret", "") != CRON_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    return jsonify(run_insert_creation_job())


@app.route("/debug-state", methods=["GET"])
def debug_state():
    if request.args.get("secret", "") != CRON_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    last_insert_time, last_creation_time = load_last_batch_state()
    return jsonify({
        "ok": True,
        "last_insert_time": last_insert_time.isoformat() if last_insert_time else None,
        "last_creation_time": last_creation_time.isoformat() if last_creation_time else None,
        "seen_count": len(load_seen()),
    })


@app.route("/test-push", methods=["GET"])
def test_push():
    if request.args.get("secret", "") != CRON_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    targets = _load_line_targets()
    only = request.args.get("province", "").strip()
    if only:
        targets = {only: targets[only]} if only in targets else {}
    if not targets:
        return jsonify({"ok": False, "error": "no targets"}), 400
    result = {}
    for province, tid in targets.items():
        ok = push_line(tid, [{
            "type": "text",
            "text": f"✅ ทดสอบ LINE NOC Bot\nห้อง: {province}\nเวลา: {datetime.now(TZ).strftime('%H:%M:%S')}",
        }])
        result[province] = ok
    return jsonify({"ok": True, "result": result})


@app.route("/reset-seen", methods=["POST"])
def reset_seen():
    if request.args.get("secret", "") != CRON_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    try:
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/reset-state", methods=["POST"])
def reset_state():
    if request.args.get("secret", "") != CRON_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    try:
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
        if os.path.exists(LAST_BATCH_FILE):
            os.remove(LAST_BATCH_FILE)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json(silent=True) or {}
    for ev in body.get("events", []):
        src = ev.get("source", {})
        stype = src.get("type", "")
        sid = src.get("groupId") or src.get("roomId") or src.get("userId")
        msg = (ev.get("message") or {}).get("text", "")
        log.info(f"[webhook] type={stype} id={sid} msg={msg!r}")
        if msg.strip().lower() == "/id" and ev.get("replyToken"):
            try:
                requests.post(
                    "https://api.line.me/v2/bot/message/reply",
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                    },
                    json={
                        "replyToken": ev["replyToken"],
                        "messages": [{
                            "type": "text",
                            "text": f"type: {stype}\nid: {sid}",
                        }],
                    },
                    timeout=10,
                )
            except Exception as e:
                log.error(f"reply failed: {e}")
    return "OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
