import pymysql
import time
import os
import sys
import json
import random
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, UTC
import dotenv

load_dotenv()
LOG_FILE = "etarch.log"
CKPT_FILE = "etarch.ckpt.json"

logger = logging.getLogger("etarch")
logger.setLevel(logging.INFO)
fh = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=7, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh); logger.addHandler(ch)

DB = dict(
    user=os.getenv(USER),
    password=os.getenv(PASS),
    host=os.getenv(HOST),
    port=os.getenv(PORT),
    database=os.getenv(DB),
    charset="utf8mb4",
    autocommit=False,
)

# -------------------------
# Настройки батчей/задержек
# -------------------------
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MIN_BATCH_SIZE = int(os.getenv("MIN_BATCH_SIZE", "25"))
BATCH_SHRINK_FACTOR = float(os.getenv("BATCH_SHRINK_FACTOR", "0.5"))

BATCH_SLEEP_MS = int(os.getenv("BATCH_SLEEP_MS", "500"))
BATCH_SLEEP_JITTER_MS = int(os.getenv("BATCH_SLEEP_JITTER_MS", "100"))
SLOWDOWN_AFTER_ROWS = int(os.getenv("SLOWDOWN_AFTER_ROWS", "15000"))
SLOWDOWN_EXTRA_MS = int(os.getenv("SLOWDOWN_EXTRA_MS", "500"))

BACKOFF_BASE_MS = int(os.getenv("BACKOFF_BASE_MS", "250"))
BACKOFF_MAX_MS  = int(os.getenv("BACKOFF_MAX_MS",  "5000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))

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
    transient_codes = {1205, 1213, 2006, 2013}
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
        """Выполнить SQL с повторами и авто‑reconnect."""
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
                # пересоздаём соединение/курсор
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

def load_ckpt():
    if not os.path.exists(CKPT_FILE):
        return {"last_id": 0, "last_run": None}
    try:
        with open(CKPT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"last_id": 0, "last_run": None}

def save_ckpt(last_id):
    data = {
        "last_id": last_id,
        "last_run": datetime.now(UTC).isoformat().replace("+00:00", "Z")
    }
    with open(CKPT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

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

def process_updates():
    logger.info("🚀 Старт архивации")
    ckpt = load_ckpt()
    last_id = ckpt.get("last_id", 0)

    cutoff = datetime.now() - timedelta(days=1)
    logger.info(f"⏱️ Порог архивации: validity < {cutoff.isoformat(sep=' ', timespec='seconds')}")

    db = DBSession(DB)

    total = 0
    start_ts = time.time()
    batch_size = BATCH_SIZE

    try:
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
    except Exception as e:
        logger.exception(f"💥 Критическая ошибка: {e}")
        db.safe_rollback()
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":
    process_updates()
