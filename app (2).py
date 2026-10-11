"""
Table review agent.

Flow:
  POS "check paid" webhook -> schedule text (+2h, never in quiet hours)
  -> scheduler sends opening SMS via Twilio
  -> guest replies hit /webhook/twilio-inbound -> Claude answers using prompt.txt
  -> FLAG / OPTOUT control lines are parsed out, alerts go to Slack / manager phone.
"""
import hmac
import json
import logging
import os
import re
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import anthropic
import httpx
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel

try:  # optional: load a local .env file (hosts set real env vars instead)
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
from twilio.request_validator import RequestValidator
from twilio.rest import Client as TwilioClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("review-agent")

# ----------------------------------------------------------------- config
def env(name, default=""):
    return os.environ.get(name, default)

RESTAURANT_NAME = env("RESTAURANT_NAME", "Our Restaurant")
MANAGER_NAME = env("MANAGER_NAME", "the manager")
RESTAURANT_PHONE = env("RESTAURANT_PHONE")
REVIEW_LINK = env("REVIEW_LINK")
TZ = ZoneInfo(env("RESTAURANT_TZ", "America/Chicago"))
DEFAULT_CC = env("DEFAULT_COUNTRY_CODE", "+1")

SEND_DELAY = timedelta(hours=float(env("SEND_DELAY_HOURS", "2")))
QUIET_START = int(env("QUIET_START_HOUR", "0"))
QUIET_END = int(env("QUIET_END_HOUR", "8"))
FOLLOWUP_DELAY = timedelta(days=float(env("FOLLOWUP_DELAY_DAYS", "2")))
READ_RECEIPTS = env("READ_RECEIPTS", "false").lower() == "true"
MAX_LATE = timedelta(hours=float(env("MAX_LATE_HOURS", "24")))  # skip texts older than this
DEDUPE_WINDOW = timedelta(hours=12)  # ignore repeat webhooks for the same guest

WEBHOOK_SECRET = env("WEBHOOK_SECRET")
PUBLIC_URL = env("PUBLIC_URL").rstrip("/")
TWILIO_FROM = env("TWILIO_FROM")
TWILIO_VALIDATE = env("TWILIO_VALIDATE", "true").lower() != "false"
SLACK_WEBHOOK_URL = env("SLACK_WEBHOOK_URL")
MANAGER_PHONE = env("MANAGER_PHONE")
MODEL = env("CLAUDE_MODEL", "claude-sonnet-5-5")
DB_PATH = env("DB_PATH", "agent.db")

STOP_WORDS = re.compile(
    r"^\s*(stop|stopall|unsubscribe|cancel|end|quit|optout|opt out)\s*[.!]*\s*$", re.I
)
PROMPT_TEMPLATE = open(os.path.join(os.path.dirname(__file__), "prompt.txt"), encoding="utf-8").read()

FIRST_MSG = (
    "Hi {first_name}, thanks for dining with us at {restaurant}! "
    "How was everything, 1 to 5? {link} Reply STOP to opt out."
)
FOLLOWUP_MSG = (
    "Hi {first_name}, {restaurant} again. If you have 10 seconds, a quick rating "
    "really helps a small place like ours. {link}"
)

_twilio = None
_claude = None

def twilio():
    global _twilio
    if _twilio is None:
        _twilio = TwilioClient(env("TWILIO_ACCOUNT_SID"), env("TWILIO_AUTH_TOKEN"))
    return _twilio

def claude():
    global _claude
    if _claude is None:
        _claude = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY
    return _claude

# --------------------------------------------------------------------- db
LOCK = threading.Lock()

def db(sql, args=(), fetch=None):
    """fetch: None (write), 'one', or 'all'."""
    with LOCK:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        try:
            cur = con.execute(sql, args)
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
            con.commit()
        finally:
            con.close()

def check_config():
    missing = [n for n in ("REVIEW_LINK", "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_FROM",
                           "ANTHROPIC_API_KEY", "WEBHOOK_SECRET", "PUBLIC_URL") if not env(n)]
    if missing:
        log.warning("Missing settings: %s", ", ".join(missing))
    if not TWILIO_VALIDATE:
        log.warning("TWILIO_VALIDATE=false: inbound webhooks are NOT verified. Testing only.")


def init_db():
    db("""CREATE TABLE IF NOT EXISTS visits(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT NOT NULL,
        first_name TEXT NOT NULL,
        consent_at TEXT NOT NULL,
        send_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'scheduled',
        first_sid TEXT,
        first_sent_at TEXT,
        delivery_status TEXT,
        follow_up_sent INTEGER NOT NULL DEFAULT 0,
        replied INTEGER NOT NULL DEFAULT 0,
        human_only INTEGER NOT NULL DEFAULT 0,
        flagged INTEGER NOT NULL DEFAULT 0)""")
    db("""CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT NOT NULL, role TEXT NOT NULL, content TEXT NOT NULL, ts TEXT NOT NULL)""")
    db("CREATE TABLE IF NOT EXISTS optouts(phone TEXT PRIMARY KEY, ts TEXT NOT NULL)")

# ---------------------------------------------------------------- helpers
def now_utc():
    return datetime.now(timezone.utc)

def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()

def parse_iso(s):
    return datetime.fromisoformat(s.strip().replace("Z", "+00:00"))

def norm_phone(p):
    d = re.sub(r"[^\d+]", "", p or "")
    if d.startswith("+"):
        return d
    if len(d) == 10:
        return DEFAULT_CC + d
    return "+" + d

def in_quiet_hours(dt):
    if QUIET_START == QUIET_END:
        return False  # same value = quiet hours switched off
    h = dt.astimezone(TZ).hour
    if QUIET_START < QUIET_END:
        return QUIET_START <= h < QUIET_END
    return h >= QUIET_START or h < QUIET_END  # window wraps midnight

def next_allowed(dt):
    """Return dt, or the moment quiet hours end if dt lands inside them."""
    if not in_quiet_hours(dt):
        return dt
    local = dt.astimezone(TZ)
    end = local.replace(hour=QUIET_END, minute=0, second=0, microsecond=0)
    if end <= local:
        end += timedelta(days=1)
    return end.astimezone(timezone.utc)

def is_opted_out(phone):
    return db("SELECT 1 FROM optouts WHERE phone=?", (phone,), "one") is not None

def add_optout(phone):
    db("INSERT OR IGNORE INTO optouts(phone, ts) VALUES(?,?)", (phone, iso(now_utc())))

def log_msg(phone, role, content):
    db("INSERT INTO messages(phone, role, content, ts) VALUES(?,?,?,?)",
       (phone, role, content, iso(now_utc())))

def send_sms(to, body):
    if is_opted_out(to):
        log.info("blocked send to opted-out number %s", to)
        return None
    kwargs = dict(to=to, from_=TWILIO_FROM, body=body)
    if PUBLIC_URL:
        kwargs["status_callback"] = PUBLIC_URL + "/webhook/twilio-status"
    msg = twilio().messages.create(**kwargs)
    return msg.sid

# ------------------------------------------------------------------ alerts
def alert_team(reason, priority, summary, guest, phone):
    text = (f"{'URGENT ' if priority == 'urgent' else ''}Review agent flag [{reason}] "
            f"from {guest} ({phone}): {summary}")
    log.warning(text)
    if SLACK_WEBHOOK_URL:
        try:
            httpx.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        except Exception:
            log.exception("slack alert failed")
    if MANAGER_PHONE:
        try:
            twilio().messages.create(to=MANAGER_PHONE, from_=TWILIO_FROM, body=text[:300])
        except Exception:
            log.exception("manager sms alert failed")

# ------------------------------------------------------------------- agent
FLAG_RE = re.compile(r"^FLAG:\s*(\{.*\})\s*$", re.M)
OPTOUT_RE = re.compile(r"^OPTOUT\s*$", re.M)

def build_system_prompt(first_name, phone):
    return (PROMPT_TEMPLATE
            .replace("{restaurant name}", RESTAURANT_NAME)
            .replace("{manager name}", MANAGER_NAME)
            .replace("{restaurant phone}", RESTAURANT_PHONE)
            .replace("{first name}", first_name)
            .replace("{phone}", phone)
            .replace("{link}", REVIEW_LINK))

def history_for(phone):
    rows = db("SELECT role, content FROM messages WHERE phone=? ORDER BY id DESC LIMIT 30",
              (phone,), "all")
    msgs = [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]
    # The API wants the conversation to start with a user turn.
    if not msgs or msgs[0]["role"] != "user":
        msgs.insert(0, {"role": "user", "content": "(Visit finished. Opening text was sent.)"})
    return msgs

def handle_inbound(phone, body):
    visit = db("SELECT * FROM visits WHERE phone=? ORDER BY id DESC LIMIT 1", (phone,), "one")
    first_name = visit["first_name"] if visit else "there"
    log_msg(phone, "user", body)

    if visit:
        db("UPDATE visits SET replied=1 WHERE id=?", (visit["id"],))

    if STOP_WORDS.match(body):
        add_optout(phone)
        log.info("opt-out via keyword: %s", phone)
        return  # Twilio sends its own STOP confirmation on its numbers

    if is_opted_out(phone):
        return
    if visit and visit["human_only"]:
        alert_team("other", "normal", f"Guest wrote again after handoff: {body[:200]}", first_name, phone)
        return

    try:
        resp = claude().messages.create(
            model=MODEL,
            max_tokens=500,
            system=build_system_prompt(first_name, phone),
            messages=history_for(phone),
        )
        raw = "".join(b.text for b in resp.content if b.type == "text")
    except Exception:
        log.exception("claude call failed")
        alert_team("other", "normal", f"Agent error, guest said: {body[:200]}", first_name, phone)
        return

    flag = None
    m = FLAG_RE.search(raw)
    if m:
        try:
            flag = json.loads(m.group(1))
        except json.JSONDecodeError:
            flag = {"reason": "other", "priority": "normal", "summary": "Unparseable flag from agent"}
    optout = bool(OPTOUT_RE.search(raw))
    reply = OPTOUT_RE.sub("", FLAG_RE.sub("", raw)).strip()

    if reply:
        try:
            if send_sms(phone, reply):
                log_msg(phone, "assistant", reply)
        except Exception:
            log.exception("failed sending reply")

    if optout:
        add_optout(phone)

    if flag:
        reason = flag.get("reason", "other")
        priority = flag.get("priority", "normal")
        already = visit["flagged"] if visit else 0
        if priority == "urgent" or not already:
            alert_team(reason, priority, flag.get("summary", ""), first_name, phone)
            if visit:
                db("UPDATE visits SET flagged=1 WHERE id=?", (visit["id"],))
        if reason == "wants_human" and visit:
            db("UPDATE visits SET human_only=1 WHERE id=?", (visit["id"],))

# --------------------------------------------------------------- scheduler
def run_due_jobs():
    now = now_utc()
    # 1) opening messages
    for v in db("SELECT * FROM visits WHERE status='scheduled' AND send_at<=?", (iso(now),), "all"):
        if is_opted_out(v["phone"]):
            db("UPDATE visits SET status='skipped' WHERE id=?", (v["id"],))
            continue
        if now - parse_iso(v["send_at"]) > MAX_LATE:
            db("UPDATE visits SET status='expired' WHERE id=?", (v["id"],))
            continue  # too late to be a natural follow-up
        if in_quiet_hours(now):
            continue  # hard stop: nothing leaves during quiet hours
        body = FIRST_MSG.format(first_name=v["first_name"], restaurant=RESTAURANT_NAME, link=REVIEW_LINK)
        try:
            sid = send_sms(v["phone"], body)
            if not sid:
                db("UPDATE visits SET status='skipped' WHERE id=?", (v["id"],))
                continue
            log_msg(v["phone"], "assistant", body)
            db("UPDATE visits SET status='sent', first_sid=?, first_sent_at=? WHERE id=?",
               (sid, iso(now), v["id"]))
        except Exception:
            log.exception("opening send failed for visit %s", v["id"])
            db("UPDATE visits SET status='error' WHERE id=?", (v["id"],))

    # 2) single follow-up: only if the channel proves it was delivered but NOT read
    if READ_RECEIPTS and not in_quiet_hours(now):
        cutoff = iso(now - FOLLOWUP_DELAY)
        rows = db("""SELECT * FROM visits WHERE status='sent' AND follow_up_sent=0 AND replied=0
                     AND delivery_status='delivered' AND first_sent_at<=?""", (cutoff,), "all")
        for v in rows:
            if is_opted_out(v["phone"]):
                continue
            body = FOLLOWUP_MSG.format(first_name=v["first_name"], restaurant=RESTAURANT_NAME, link=REVIEW_LINK)
            try:
                send_sms(v["phone"], body)
                log_msg(v["phone"], "assistant", body)
                db("UPDATE visits SET follow_up_sent=1 WHERE id=?", (v["id"],))
            except Exception:
                log.exception("follow-up failed for visit %s", v["id"])

def scheduler_loop(stop_event):
    while not stop_event.is_set():
        try:
            run_due_jobs()
        except Exception:
            log.exception("scheduler tick failed")
        stop_event.wait(30)

# --------------------------------------------------------------------- app
@asynccontextmanager
async def lifespan(app):
    init_db()
    check_config()
    stop = threading.Event()
    t = threading.Thread(target=scheduler_loop, args=(stop,), daemon=True)
    t.start()
    yield
    stop.set()

app = FastAPI(title="Table review agent", lifespan=lifespan)

class TableDone(BaseModel):
    first_name: str
    phone: str
    consent: bool = False          # guest agreed to a follow-up text
    paid_at: str | None = None     # ISO time check closed; defaults to now

@app.post("/webhook/table-done")
def table_done(payload: TableDone, x_webhook_secret: str = Header(default="")):
    if not WEBHOOK_SECRET or not hmac.compare_digest(x_webhook_secret, WEBHOOK_SECRET):
        raise HTTPException(401, "bad secret")
    if not payload.consent:
        return {"status": "skipped", "reason": "no consent"}
    phone = norm_phone(payload.phone)
    if is_opted_out(phone):
        return {"status": "skipped", "reason": "opted out"}
    recent = db("SELECT 1 FROM visits WHERE phone=? AND consent_at>?",
                (phone, iso(now_utc() - DEDUPE_WINDOW)), "one")
    if recent:
        return {"status": "skipped", "reason": "duplicate (already scheduled in last 12h)"}
    paid = parse_iso(payload.paid_at) if payload.paid_at else now_utc()
    if paid.tzinfo is None:
        paid = paid.replace(tzinfo=TZ)
    send_at = next_allowed(paid + SEND_DELAY)
    db("INSERT INTO visits(phone, first_name, consent_at, send_at) VALUES(?,?,?,?)",
       (phone, payload.first_name.strip() or "there", iso(now_utc()), iso(send_at)))
    log.info("scheduled %s for %s", phone, send_at.astimezone(TZ).isoformat())
    return {"status": "scheduled", "send_at_local": send_at.astimezone(TZ).isoformat()}

@app.post("/webhook/twilio-inbound")
async def twilio_inbound(request: Request, background: BackgroundTasks):
    form = dict(await request.form())
    if TWILIO_VALIDATE:
        sig = request.headers.get("X-Twilio-Signature", "")
        url = PUBLIC_URL + "/webhook/twilio-inbound"
        if not RequestValidator(env("TWILIO_AUTH_TOKEN")).validate(url, form, sig):
            raise HTTPException(403, "bad signature")
    background.add_task(handle_inbound, norm_phone(form.get("From", "")), form.get("Body", "").strip())
    return Response("<Response/>", media_type="application/xml")

@app.post("/webhook/twilio-status")
async def twilio_status(request: Request):
    form = dict(await request.form())
    if TWILIO_VALIDATE:
        sig = request.headers.get("X-Twilio-Signature", "")
        url = PUBLIC_URL + "/webhook/twilio-status"
        if not RequestValidator(env("TWILIO_AUTH_TOKEN")).validate(url, form, sig):
            raise HTTPException(403, "bad signature")
    sid, status = form.get("MessageSid"), form.get("MessageStatus")
    if sid and status:
        # never downgrade 'read' back to 'delivered'
        db("""UPDATE visits SET delivery_status=? WHERE first_sid=?
              AND COALESCE(delivery_status,'')!='read'""", (status, sid))
    return Response(status_code=204)

@app.get("/health")
def health():
    return {"ok": True, "time": now_utc().astimezone(TZ).isoformat()}
