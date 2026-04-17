import json
import os
import re
import sqlite3
import time
import hashlib
from typing import Any, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request, render_template_string

app = Flask(__name__)

DB_PATH = os.environ.get("DB_PATH", "/data/bot_database.db")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "realupilootbot")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SECRET_SALT = os.environ.get("SECRET_SALT", "change_me_in_production")
ADMIN_ID = int(str(os.environ.get("ADMIN_ID", "0") or "0") or 0)

MAX_ATTEMPTS = 5
RATE_WINDOW = 3600

HTML_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{{ title }}</title>
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center;
      font-family: Arial, sans-serif; background: #0b1020; color: #eef2ff; padding: 16px;
    }
    .card {
      width: 100%; max-width: 520px; background: #121933; border: 1px solid rgba(255,255,255,.08);
      border-radius: 22px; padding: 28px; box-shadow: 0 20px 60px rgba(0,0,0,.35);
    }
    .icon { font-size: 52px; margin-bottom: 8px; }
    h1 { margin: 0 0 10px; font-size: 28px; }
    p { line-height: 1.55; color: #dbe3ff; }
    .meta {
      margin-top: 16px; padding: 14px; border-radius: 14px; background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.06); font-size: 14px;
    }
    .row { margin: 8px 0; }
    .label { color: #9fb0ff; }
    .btn {
      display: inline-block; margin-top: 18px; padding: 12px 18px; border-radius: 12px;
      background: #5b7cff; color: white; text-decoration: none; font-weight: bold;
    }
    .muted { color: #aab4da; font-size: 14px; }
    code { background: rgba(255,255,255,.06); padding: 2px 6px; border-radius: 8px; }
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{{ icon }}</div>
    <h1>{{ title }}</h1>
    <p>{{ message }}</p>
    <div class="meta">
      <div class="row"><span class="label">User ID:</span> <code>{{ user_id }}</code></div>
      <div class="row"><span class="label">Status:</span> <code>{{ status }}</code></div>
      {% if verified_at %}<div class="row"><span class="label">Verified At:</span> <code>{{ verified_at }}</code></div>{% endif %}
      {% if details %}<div class="row"><span class="label">Details:</span> <code>{{ details }}</code></div>{% endif %}
    </div>
    <p class="muted">Return to Telegram. The reward flow and welcome message are handled automatically after successful verification.</p>
    {% if redirect_url %}<a class="btn" href="{{ redirect_url }}">Open Telegram Bot</a>{% endif %}
  </div>
</body>
</html>
"""


def ensure_db_parent_dir(path: str) -> str:
    path = (path or "").strip() or "/data/bot_database.db"
    parent = os.path.dirname(path)
    try:
        if parent:
            os.makedirs(parent, exist_ok=True)
        return path
    except Exception:
        fallback = os.path.join(os.getcwd(), os.path.basename(path) or "bot_database.db")
        fallback_parent = os.path.dirname(fallback)
        if fallback_parent:
            os.makedirs(fallback_parent, exist_ok=True)
        return fallback


DB_PATH = ensure_db_parent_dir(DB_PATH)


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def table_has_column(cur: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    cur.execute(f"PRAGMA table_info({table_name})")
    return any(str(row[1]) == column_name for row in cur.fetchall())


def ensure_schema() -> None:
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            balance REAL DEFAULT 0,
            total_earned REAL DEFAULT 0,
            total_withdrawn REAL DEFAULT 0,
            referral_count INTEGER DEFAULT 0,
            referred_by INTEGER DEFAULT 0,
            upi_id TEXT DEFAULT '',
            banned INTEGER DEFAULT 0,
            joined_at TEXT DEFAULT '',
            last_daily TEXT DEFAULT '',
            is_premium INTEGER DEFAULT 0,
            referral_paid INTEGER DEFAULT 0,
            ip_address TEXT DEFAULT '',
            ip_verified INTEGER DEFAULT 0,
            verify_attempts INTEGER DEFAULT 0,
            last_attempt_at REAL DEFAULT 0,
            verified_at REAL DEFAULT 0,
            session_hash TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            device_type TEXT DEFAULT ''
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS verify_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            ip TEXT,
            result TEXT,
            reason TEXT,
            user_agent TEXT,
            ts REAL,
            session_hash TEXT DEFAULT ''
        )
        """
    )

    extra_columns = [
        ("referral_paid", "INTEGER DEFAULT 0"),
        ("ip_address", "TEXT DEFAULT ''"),
        ("ip_verified", "INTEGER DEFAULT 0"),
        ("verify_attempts", "INTEGER DEFAULT 0"),
        ("last_attempt_at", "REAL DEFAULT 0"),
        ("verified_at", "REAL DEFAULT 0"),
        ("session_hash", "TEXT DEFAULT ''"),
        ("user_agent", "TEXT DEFAULT ''"),
        ("device_type", "TEXT DEFAULT ''"),
        ("total_referral_earnings", "REAL DEFAULT 0"),
        ("bonus_balance", "REAL DEFAULT 0"),
        ("last_active_at", "TEXT DEFAULT ''"),
        ("verification_status", "TEXT DEFAULT 'pending'"),
        ("flagged_for_review", "INTEGER DEFAULT 0"),
        ("verification_note", "TEXT DEFAULT ''"),
        ("first_verified_ip", "TEXT DEFAULT ''"),
        ("latest_ip", "TEXT DEFAULT ''"),
        ("fingerprint_hash", "TEXT DEFAULT ''"),
        ("fraud_score", "INTEGER DEFAULT 0"),
        ("referral_hold_until", "TEXT DEFAULT ''"),
        ("last_verification_at", "TEXT DEFAULT ''"),
    ]

    for col_name, col_type in extra_columns:
        if table_has_column(cur, "users", col_name):
            continue
        cur.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")

    conn.commit()
    conn.close()


def parse_setting(raw: Any, default: Any = None) -> Any:
    if raw is None:
        return default
    if isinstance(raw, (dict, list, int, float, bool)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return raw if raw != "" else default


def get_setting_value(key: str, default: Any = None) -> Any:
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row:
            return default
        return parse_setting(row["value"], default)
    finally:
        conn.close()


def get_user(user_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def telegram_api(method: str, payload: dict) -> bool:
    if not BOT_TOKEN:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
            json=payload,
            timeout=15,
        )
        return resp.ok
    except Exception:
        return False


def build_main_keyboard(user_id: int) -> dict:
    keyboard = [
        [{"text": "💰 Balance"}, {"text": "👥 Refer"}],
        [{"text": "🏧 Withdraw"}, {"text": "🎁 Gift"}],
        [{"text": "📋 Tasks"}],
    ]
    if ADMIN_ID and int(user_id) == ADMIN_ID:
        keyboard.append([{"text": "👑 Admin Panel"}])
    return {"keyboard": keyboard, "resize_keyboard": True}


def send_welcome_via_bot(user_id: int) -> bool:
    user = get_user(user_id)
    if not user:
        return False

    first_name = (user["first_name"] or "User").strip() or "User"
    balance = to_float(user["balance"], 0.0)
    per_refer = to_float(get_setting_value("per_refer", get_setting_value("welcome_bonus", 0.0)), 0.0)
    min_withdraw = to_float(get_setting_value("min_withdraw", 0.0), 0.0)
    welcome_image = str(get_setting_value("welcome_image", "") or "").strip()
    refer_link = f"https://t.me/{BOT_USERNAME}?start={user_id}" if BOT_USERNAME else ""

    caption = (
        "👑 <b>Welcome to UPI Loot Pay!</b> 🔥\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"😄 Hello, <b>{first_name}</b>!\n\n"
        f"💸 <b>Your Balance:</b> ₹{balance:.2f}\n"
        f"⭐ <b>Per Refer:</b> ₹{per_refer:g}\n"
        f"⬇️ <b>Min Withdraw:</b> ₹{min_withdraw:g}\n\n"
        "⚡ <b>How to Earn?</b>\n"
        "  ▶️ Share your referral link\n"
        "  ▶️ Friends complete verification or auto approval → You earn referral rewards\n"
        "  ▶️ Complete Tasks & earn more!\n"
        "  ▶️ Withdraw to UPI instantly!\n\n"
        f"🔗 <b>Your Refer Link:</b>\n<code>{refer_link}</code>\n\n"
        "✨ <i>No limit! Earn unlimited!</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )

    common = {
        "chat_id": user_id,
        "parse_mode": "HTML",
        "reply_markup": build_main_keyboard(user_id),
    }
    if welcome_image:
        if telegram_api("sendPhoto", {**common, "photo": welcome_image, "caption": caption}):
            return True
    return telegram_api("sendMessage", {**common, "text": caption})


def get_referral_reward(level: int, base_amount: float = 0.0) -> float:
    if not bool(get_setting_value("referral_system_enabled", True)):
        return 0.0
    reward_type = str(get_setting_value(f"referral_level_{level}_type", "fixed") or "fixed").lower()
    reward_value = to_float(get_setting_value(f"referral_level_{level}_value", 0), 0.0)
    if reward_type == "percent":
        return round(base_amount * reward_value / 100.0, 2)
    return round(reward_value, 2)


def get_referral_chain(user_id: int, max_levels: int = 3) -> List[Tuple[int, sqlite3.Row]]:
    chain: List[Tuple[int, sqlite3.Row]] = []
    current = get_user(user_id)
    for level in range(1, max_levels + 1):
        if not current:
            break
        ref_id = int(current["referred_by"] or 0)
        if not ref_id or ref_id == user_id:
            break
        parent = get_user(ref_id)
        if not parent:
            break
        chain.append((level, parent))
        current = parent
    return chain


def process_referral_bonus(user_id: int) -> bool:
    user = get_user(user_id)
    if not user:
        return False
    if int(user["referral_paid"] or 0) == 1:
        return False
    if int(user["ip_verified"] or 0) != 1:
        return False

    chain = get_referral_chain(user_id, 3)
    conn = get_db()
    cur = conn.cursor()
    paid_any = False
    try:
        if not chain:
            cur.execute("UPDATE users SET referral_paid = 1, referral_hold_until = '' WHERE user_id = ?", (user_id,))
            conn.commit()
            return False

        base_amount = get_referral_reward(1, 0.0)
        for level, parent in chain:
            reward = get_referral_reward(level, base_amount)
            if reward <= 0:
                continue
            cur.execute(
                """
                UPDATE users
                SET balance = balance + ?,
                    total_earned = total_earned + ?,
                    total_referral_earnings = COALESCE(total_referral_earnings, 0) + ?,
                    referral_count = referral_count + ?
                WHERE user_id = ?
                """,
                (reward, reward, reward, 1 if level == 1 else 0, int(parent["user_id"])),
            )
            paid_any = True
            telegram_api(
                "sendMessage",
                {
                    "chat_id": int(parent["user_id"]),
                    "parse_mode": "HTML",
                    "text": (
                        f"🎉 <b>Referral Level {level} Bonus Claimed!</b>\n\n"
                        f"💰 You earned <b>₹{reward:.2f}</b>\n"
                        f"👥 User: <code>{user_id}</code> completed verification automatically."
                    ),
                },
            )
        cur.execute("UPDATE users SET referral_paid = 1, referral_hold_until = '' WHERE user_id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()
    return paid_any


def get_real_ip() -> str:
    for header in ("CF-Connecting-IP", "X-Real-IP", "X-Forwarded-For"):
        value = request.headers.get(header, "")
        if value:
            return value.split(",")[0].strip()[:128]
    return (request.remote_addr or "").strip()[:128]


def detect_device(user_agent: str) -> str:
    ua = user_agent or ""
    if re.search(r"iPad|Tablet", ua, re.IGNORECASE):
        return "Tablet"
    if re.search(r"Mobi|Android|iPhone|iPod", ua, re.IGNORECASE):
        return "Mobile"
    return "Desktop"


def make_session_hash(user_id: int, ip: str, user_agent: str) -> str:
    raw = f"{user_id}|{ip}|{user_agent}|{SECRET_SALT}|{time.time()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


def format_ts(ts_value: Any) -> str:
    try:
        ts_value = float(ts_value or 0)
        if ts_value <= 0:
            return "—"
        return time.strftime("%d %b %Y • %I:%M %p", time.localtime(ts_value))
    except Exception:
        return "—"


def log_verification(cur: sqlite3.Cursor, user_id: int, ip: str, result: str, reason: str, user_agent: str, session_hash: str = "") -> None:
    cur.execute(
        """
        INSERT INTO verify_log (user_id, ip, result, reason, user_agent, ts, session_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (user_id, ip, result, reason, user_agent, time.time(), session_hash),
    )


def ip_taken_by_other_account(ip: str, user_id: int) -> bool:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users WHERE ip_address = ? AND user_id != ? LIMIT 1", (ip, user_id))
    row = cur.fetchone()
    conn.close()
    return row is not None


def verify_user(user_id: int, ip: str, user_agent: str) -> Tuple[bool, dict]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = cur.fetchone()

    if not user:
        log_verification(cur, user_id, ip, "fail", "user_not_found", user_agent)
        conn.commit()
        conn.close()
        return False, {"message": "User not found. Please start the bot first.", "code": "ERR_USER_404"}

    if int(user["banned"] or 0) == 1:
        log_verification(cur, user_id, ip, "fail", "account_banned", user_agent)
        conn.commit()
        conn.close()
        return False, {"message": "Your account is banned.", "code": "ERR_ACCT_BAN"}

    now = time.time()
    attempts = int(user["verify_attempts"] or 0)
    last_attempt_at = float(user["last_attempt_at"] or 0)
    if now - last_attempt_at >= RATE_WINDOW:
        attempts = 0
    if attempts >= MAX_ATTEMPTS:
        log_verification(cur, user_id, ip, "fail", "rate_limited", user_agent)
        conn.commit()
        conn.close()
        mins = max(1, int(max(60, RATE_WINDOW - (now - last_attempt_at))) // 60)
        return False, {"message": f"Too many attempts. Try again in {mins} minute(s).", "code": "ERR_RATE_LIMIT"}

    if not ip:
        log_verification(cur, user_id, ip, "fail", "ip_missing", user_agent)
        conn.commit()
        conn.close()
        return False, {"message": "Could not detect your IP address.", "code": "ERR_IP_DETECT"}

    if int(user["ip_verified"] or 0) == 1:
        conn.close()
        return True, {
            "message": "Already verified.",
            "status": "already_verified",
            "user_id": user_id,
            "session_hash": user["session_hash"] or "",
            "verified_at": format_ts(user["verified_at"]),
            "device_type": user["device_type"] or detect_device(user_agent),
        }

    if bool(get_setting_value("ip_verification_enabled", True)) and ip_taken_by_other_account(ip, user_id):
        cur.execute("UPDATE users SET verify_attempts = ?, last_attempt_at = ? WHERE user_id = ?", (attempts + 1, now, user_id))
        log_verification(cur, user_id, ip, "fail", "ip_conflict", user_agent)
        conn.commit()
        conn.close()
        return False, {
            "message": "This IP is already linked to another account. You can still use the bot, but referral bonus is blocked for this verification.",
            "code": "ERR_IP_CONFLICT",
        }

    session_hash = make_session_hash(user_id, ip, user_agent)
    device_type = detect_device(user_agent)

    cur.execute(
        """
        UPDATE users
        SET ip_address = ?,
            ip_verified = 1,
            verify_attempts = ?,
            last_attempt_at = ?,
            verified_at = ?,
            session_hash = ?,
            user_agent = ?,
            device_type = ?,
            first_verified_ip = CASE WHEN COALESCE(first_verified_ip,'') = '' THEN ? ELSE first_verified_ip END,
            latest_ip = ?,
            verification_status = 'verified',
            verification_note = '',
            flagged_for_review = 0,
            last_verification_at = ?
        WHERE user_id = ?
        """,
        (
            ip,
            attempts + 1,
            now,
            now,
            session_hash,
            user_agent,
            device_type,
            ip,
            ip,
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
            user_id,
        ),
    )
    log_verification(cur, user_id, ip, "success", "verified", user_agent, session_hash)
    conn.commit()
    conn.close()

    process_referral_bonus(user_id)
    send_welcome_via_bot(user_id)

    return True, {
        "message": "Verification successful. Welcome message and referral flow completed automatically.",
        "status": "verified",
        "user_id": user_id,
        "session_hash": session_hash,
        "verified_at": format_ts(now),
        "device_type": device_type,
    }


@app.route("/")
def home():
    return jsonify({"status": "running", "service": "web_verify", "version": "6.2"})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "timestamp": int(time.time()), "db_path": DB_PATH})


@app.route("/ip-verify")
@app.route("/ip-verify/")
def ip_verify():
    uid = request.args.get("uid", "").strip()
    if not uid or not uid.isdigit():
        return render_template_string(
            HTML_PAGE,
            icon="❌",
            title="Verification Failed",
            message="Invalid or missing user ID. Use the correct link from the bot.",
            user_id="—",
            status="ERR_INVALID_UID",
            verified_at="",
            details="",
            redirect_url=f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "",
        ), 400

    user_id = int(uid)
    ip = get_real_ip()
    user_agent = request.headers.get("User-Agent", "")

    try:
        ok, data = verify_user(user_id, ip, user_agent)
    except Exception as exc:
        return render_template_string(
            HTML_PAGE,
            icon="❌",
            title="Verification Error",
            message="The verification page hit a server-side error. The bug has been handled more safely now.",
            user_id=user_id,
            status="ERR_INTERNAL",
            verified_at="",
            details=str(exc)[:180],
            redirect_url=f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "",
        ), 500

    if not ok:
        return render_template_string(
            HTML_PAGE,
            icon="❌",
            title="Verification Failed",
            message=data["message"],
            user_id=user_id,
            status=data.get("code", "ERR_VERIFY"),
            verified_at="",
            details=detect_device(user_agent),
            redirect_url=f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "",
        ), 400

    return render_template_string(
        HTML_PAGE,
        icon="✅",
        title="Verified Successfully",
        message=data["message"],
        user_id=data["user_id"],
        status=data["status"],
        verified_at=data.get("verified_at", ""),
        details=data.get("device_type", ""),
        redirect_url=f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "",
    )


@app.route("/api/verify-status/<int:user_id>")
def verify_status(user_id: int):
    user = get_user(user_id)
    if not user:
        return jsonify({"verified": False, "error": "user_not_found"}), 404
    return jsonify(
        {
            "verified": bool(int(user["ip_verified"] or 0)),
            "ip_address": user["ip_address"] or "",
            "verified_at": user["verified_at"] or 0,
            "device_type": user["device_type"] or "",
            "session_hash": user["session_hash"] or "",
            "referral_paid": bool(int(user["referral_paid"] or 0)),
        }
    )


@app.route("/api/verify-log/<int:user_id>")
def verify_log_api(user_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT result, reason, ts, ip FROM verify_log WHERE user_id = ? ORDER BY ts DESC LIMIT 20",
        (user_id,),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify({"user_id": user_id, "logs": rows})


@app.route("/api/stats")
def stats():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS total FROM users")
    total_users = cur.fetchone()["total"]
    cur.execute("SELECT COUNT(*) AS total FROM users WHERE ip_verified = 1")
    total_verified = cur.fetchone()["total"]
    cur.execute("SELECT COUNT(*) AS total FROM verify_log WHERE result = 'fail'")
    total_failed = cur.fetchone()["total"]
    conn.close()
    return jsonify(
        {
            "total_users": total_users,
            "total_verified": total_verified,
            "total_failed_attempts": total_failed,
        }
    )


ensure_schema()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
