#!/usr/bin/env python3
import os
import sys
import time
import json
import random
import logging
import traceback
import threading
from logging.handlers import RotatingFileHandler
from datetime import datetime, UTC, timedelta

import pymysql

# Telegram bot (python-telegram-bot v20+)
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ------------------------
# LOGGING
# ------------------------
LOG_FILE = os.getenv("LOG_FILE", "etarch.log")
CKPT_FILE = os.getenv("CKPT_FILE", "etarch.ckpt.json")

logger = logging.getLogger("etarch")
logger.setLevel(logging.INFO)
fh = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=7, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh); logger.addHandler(ch)

# ------------------------
# ENV / CONFIG
# ------------------------
DB = dict(
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME"),
    charset="utf8mb4",
    autocommit=False,
    connect_timeout=int(os.getenv("CONNECT_TIMEOUT", "10")),
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20000"))
MIN_BATCH_SIZE = int(os.getenv("MIN_BATCH_SIZE", "2000"))
BATCH_SHRINK_FACTOR = float(os.getenv("BATCH_SHRINK_FACTOR", "0.5"))

BATCH_SLEEP_MS = int(os.getenv("BATCH_SLEEP_MS", "150"))
BATCH_SLEEP_JITTER_MS = int(os.getenv("BATCH_SLEEP_JITTER_MS", "100"))
SLOWDOWN_AFTER_ROWS = int(os.getenv("SLOWDOWN_AFTER_ROWS", "15000"))
SLOWDOWN_EXTRA_MS = int(os.getenv("SLOWDOWN_EXTRA_MS", "500"))

BACKOFF_BASE_MS = int(os.getenv("BACKOFF_BASE_MS", "250"))
BACKOFF_MAX_MS  = int(os.getenv("BACKOFF_MAX_MS",  "5000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))

# Scheduler settings
CRON_TIME = os.getenv("CRON_TIME", "10 2 * * *")  # default 02:10 every day (min hour dom mon dow)
# Example formats accepted: "10 2 * * *" or "0 3 * * 1-5"
TZ = os.getenv("TZ", "UTC")

# Telegram
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
TG_ALLOWED_USER_IDS = {uid.strip() for uid in os.getenv("TG_ALLOWED_USER_IDS", "").split(",") if uid.strip()}

_run_lock = threading.Lock()
_last_run_info = {
    "started_at": None,
    "finished_at": None,
    "running": False,
    "total_updated": 0,
    "last_id": 0,
    "error": None,
    "batch_size": BATCH_SIZE,
}

# ------------------------
# Telegram Notifier
# ------------------------
class Notifier:
    def __init__(self, app: Application | None):
        self.app = app

    async def send(self, text: str, chat_id: str | None = None):
        if not self.app or not TG_BOT_TOKEN:
            return
        target = chat_id or TG_CHAT_ID
        if not target:
            return
        try:
            await self.app.bot.send_message(chat_id=target, text=text[:4096])
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")

notifier: Notifier | None = None

# ------------------------
# Helpers
# ------------------------
def load_ckpt():
    if not os.path.exists(CKPT_FILE):
        return {"last_id": 0, "last_run": None}
    try:
        with open(CKPT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"last_id": 0, "last_run": None}

def save_ckpt(last_id):
    data = {"last_id": last_id, "last_run": datetime.now(UTC).isoformat().replace("+00:00", "Z")}
    with open(CKPT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def sleep_between_batches(updated_rows: int):
    base = BATCH_SLEEP_MS / 1000.0
    jitter = random.uniform(0, max(0, BATCH_SLEEP_JITTER_MS)) / 1000.0
    extra = (SLOWDOWN_EXTRA_MS / 1000.0) if updated_rows > SLOWDOWN_AFTER_ROWS else 0.0
    total = base + jitter + extra
    if total > 0:
        logger.info(f"⏸️ Пауза между батчами: {total:.3f} сек")
        time.sleep(total)

def is_transient(e: Exception) -> bool:
    from pymysql.err import OperationalError, InternalError, InterfaceError
    transient_codes = {1205, 1213, 2006, 2013}  # lock wait, deadlock, server gone, lost connection
    if isinstance(e, (OperationalError, InternalError, InterfaceError)):
        try:
            code = int(getattr(e, "args", [None])[0] or 0)
        except Exception:
            code = 0
        msg = str(e).lower()
        return (code in transient_codes) or any(s in msg for s in [
            "lock wait timeout", "deadlock", "server has gone away", "lost connection", "timed out"
        ])
    return False

class DBSession:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        if self.conn and self.conn.open:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = pymysql.connect(**self.cfg)
        self.cursor = self.conn.cursor()
        logger.info("✅ Подключение к БД установлено")

    def ping(self):
        try:
            self.conn.ping(reconnect=True)
        except Exception as e:
            logger.warning(f"🔌 ping: {e}; пересоздаю соединение")
            self.connect()

    def execute(self, sql: str, params=None, commit=False):
        attempt = 0
        backoff_ms = BACKOFF_BASE_MS
        while True:
            try:
                self.ping()
                self.cursor.execute(sql, params or ())
                if commit:
                    self.conn.commit()
                return self.cursor
            except Exception as e:
                if not is_transient(e) or attempt >= MAX_RETRIES:
                    raise
                attempt += 1
                sleep_s = min(backoff_ms, BACKOFF_MAX_MS) / 1000.0
                logger.warning(f"🔁 Транзиентная ошибка: {e}. Повтор {attempt}/{MAX_RETRIES} через {sleep_s:.3f} сек")
                time.sleep(sleep_s)
                backoff_ms *= 2
                try:
                    self.connect()
                except Exception as e2:
                    logger.error(f"♻️ Ошибка пересоздания коннекта: {e2}")

    def safe_commit(self):
        try:
            if self.conn.open:
                self.conn.commit()
        except Exception as e:
            logger.warning(f"⚠️ commit не удался: {e}; пересоздаю соединение")
            self.connect()

    def safe_rollback(self):
        try:
            if self.conn.open:
                self.conn.rollback()
        except Exception as e:
            logger.warning(f"⚠️ rollback не удался: {e}")

    def close(self):
        try:
            if self.conn and self.conn.open:
                self.conn.close()
        finally:
            logger.info("🔌 Соединение закрыто")

def select_batch_ids(db: DBSession, cutoff, last_id, limit):
    sql = """
        SELECT id
        FROM eaisto_to
        WHERE validity < %s
          AND status <> 'ARCHIVAL'
          AND id > %s
        ORDER BY id
        LIMIT %s
    """
    cur = db.execute(sql, (cutoff, last_id, limit))
    return [row[0] for row in cur.fetchall()]

def update_by_ids(db: DBSession, id_list):
    sql = "UPDATE eaisto_to SET status = 'ARCHIVAL' WHERE id IN ({})".format(
        ",".join(["%s"] * len(id_list))
    )
    cur = db.execute(sql, id_list)
    return cur.rowcount

# ------------------------
# Core job
# ------------------------
def run_archival_job():
    global _last_run_info
    started = datetime.now(UTC)
    with _run_lock:
        if _last_run_info.get("running"):
            logger.info("⏭️ Запуск пропущен: задача уже выполняется")
            return "already_running"
        _last_run_info.update({
            "started_at": started.isoformat().replace("+00:00", "Z"),
            "finished_at": None,
            "running": True,
            "total_updated": 0,
            "error": None,
        })

    try:
        ckpt = load_ckpt()
        last_id = ckpt.get("last_id", 0)
        cutoff = datetime.now(UTC) - timedelta(days=1)
        logger.info(f"🚀 Старт архивации. Порог validity < {cutoff.isoformat().replace('+00:00','Z')} (UTC)")

        db = DBSession(DB)
        total = 0
        start_ts = time.time()
        batch_size = BATCH_SIZE

        while True:
            try:
                ids = select_batch_ids(db, cutoff, last_id, batch_size)
            except Exception as e:
                if is_transient(e) and batch_size > MIN_BATCH_SIZE:
                    old = batch_size
                    batch_size = max(MIN_BATCH_SIZE, int(batch_size * BATCH_SHRINK_FACTOR))
                    logger.warning(f"📉 Таймаут на SELECT. Уменьшаю батч: {old} → {batch_size}")
                    time.sleep(0.5)
                    continue
                raise

            if not ids:
                break

            try:
                updated = update_by_ids(db, ids)
                db.safe_commit()
            except Exception as e:
                if is_transient(e) and batch_size > MIN_BATCH_SIZE:
                    db.safe_rollback()
                    old = batch_size
                    batch_size = max(MIN_BATCH_SIZE, int(batch_size * BATCH_SHRINK_FACTOR))
                    logger.warning(f"📉 Таймаут/обрыв на UPDATE. Уменьшаю батч: {old} → {batch_size}")
                    time.sleep(0.5)
                    continue
                raise

            last_id = ids[-1]
            total += updated
            save_ckpt(last_id)

            rate = int(total / max(1, time.time() - start_ts))
            logger.info(f"📦 Батч: {len(ids)} ID, обновлено: {updated}, всего: {total}, "
                        f"последний id: {last_id}, скорость: ~{rate} строк/с, текущий батч={batch_size}")
            sleep_between_batches(updated)

        elapsed = round(time.time() - start_ts, 2)
        logger.info(f"✅ Готово. Всего обновлено: {total}. Время: {elapsed} сек.")
        _last_run_info.update({
            "finished_at": datetime.now(UTC).isoformat().replace("+00:00","Z"),
            "total_updated": total,
            "last_id": last_id,
            "batch_size": batch_size,
        })
        return "ok"
    except Exception as e:
        err_text = f"Ошибка архивации: {e}\n{traceback.format_exc()[:3500]}"
        logger.exception(err_text)
        _last_run_info["error"] = str(e)
        try:
            import asyncio
            if notifier and notifier.app:
                asyncio.run(notifier.send(f"💥 {err_text}"))
        except Exception:
            pass
        return "error"
    finally:
        _last_run_info["running"] = False

# ------------------------
# Telegram Commands
# ------------------------
def _is_allowed(user_id: int) -> bool:
    if not TG_ALLOWED_USER_IDS:
        return True  # allow all if not configured
    return str(user_id) in TG_ALLOWED_USER_IDS

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return
    await update.message.reply_text("Привет! Команды: /status, /run_now, /restart_container")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return
    info = _last_run_info.copy()
    text = json.dumps(info, ensure_ascii=False, indent=2)
    await update.message.reply_text(f"Статус:\n<pre>{text}</pre>", parse_mode="HTML")

async def cmd_run_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return
    await update.message.reply_text("Запускаю задачу архивации...")
    # Run in background thread to avoid blocking bot
    def _run():
        result = run_archival_job()
        msg = "✅ Готово" if result == "ok" else ("⏭️ Уже выполняется" if result == "already_running" else "💥 Ошибка")
        # Send result
        import asyncio
        asyncio.run(notifier.send(f"{msg}. Итог: обновлено {_last_run_info.get('total_updated')} строк."))
    threading.Thread(target=_run, daemon=True).start()

async def cmd_restart_container(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update.effective_user.id):
        return
    await update.message.reply_text("Перезапускаю контейнер (Docker restart policy должен быть 'always').")
    # Exit with non-zero; Docker will restart the container
    threading.Thread(target=lambda: (time.sleep(1), os._exit(1)), daemon=True).start()

# ------------------------
# Scheduler bootstrap
# ------------------------
def start_scheduler(app: Application):
    global notifier
    notifier = Notifier(app)

    # Parse CRON_TIME "m h dom mon dow"
    parts = CRON_TIME.split()
    if len(parts) != 5:
        logger.warning(f"CRON_TIME '{CRON_TIME}' некорректен. Использую '10 2 * * *'")
        minute, hour, dom, mon, dow = "10", "2", "*", "*", "*"
    else:
        minute, hour, dom, mon, dow = parts

    scheduler = BackgroundScheduler(timezone=TZ)
    scheduler.add_job(
        func=lambda: threading.Thread(target=run_archival_job, daemon=True).start(),
        trigger=CronTrigger(minute=minute, hour=hour, day=dom, month=mon, day_of_week=dow, timezone=TZ),
        id="daily_archival",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"🗓️ Планировщик запущен: CRON={CRON_TIME}, TZ={TZ}")

# ------------------------
# Main
# ------------------------
def main():
    # Telegram bot is optional; if token absent, we just run scheduler service
    if TG_BOT_TOKEN:
        app = Application.builder().token(TG_BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("run_now", cmd_run_now))
        app.add_handler(CommandHandler("restart_container", cmd_restart_container))
        start_scheduler(app)
        logger.info("🤖 Telegram bot запущен (long polling).")
        app.run_polling(close_loop=False)  # keeps process alive
    else:
        logger.info("⚠️ TG_BOT_TOKEN не задан. Запущу только планировщик без Telegram.")
        class DummyApp: pass
        start_scheduler(DummyApp())
        while True:
            time.sleep(3600)

if __name__ == "__main__":
    main()
