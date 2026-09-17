#!/usr/bin/env python3
"""
Telegram File Receiver Bot — Production Final (24x7 Stable)
- Robust polling with auto-reconnect (fresh Application on restart)
- Owner Broadcast (Receiver / Uploader)
- Daily Upload Reminders every 3 hours (YES / NO flow)
- YouTube upload follow-up (2 min, then every 10 min if NO)
- Health server on 0.0.0.0:$PORT
"""

import os
import sys
import sqlite3
import uuid
import asyncio
import logging
import gc
from datetime import datetime, timedelta
from contextlib import contextmanager
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import (
    TelegramError,
    Forbidden,
    BadRequest,
    TimedOut,
    NetworkError,
    Conflict,
)

# ---------------------------------------------------------------------------
# ENV
# ---------------------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DB_PATH = os.getenv("DB_PATH", "bot.db")
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN or OWNER_ID == 0:
    print("ERROR: BOT_TOKEN and OWNER_ID required")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("gvm-bot")

CONTACT_HANDLE = "@GVM_TRUST"
COOLDOWN_HOURS = 3
STALE_RESERVATION_MINUTES = 30

REMINDER_INTERVAL_SECONDS = 3 * 3600
YOUTUBE_INITIAL_DELAY = 2 * 60
YOUTUBE_REPEAT_DELAY = 10 * 60

# Global references for the reminder loop (survives app restarts)
CURRENT_BOT = None
PENDING_YT_REMINDERS = {}


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def split_message(text, max_length=4000):
    if len(text) <= max_length:
        return [text]
    chunks = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > max_length:
            if current:
                chunks.append(current)
            current = line
        else:
            current += ("\n" if current else "") + line
    if current:
        chunks.append(current)
    return chunks


def unauth_msg(user_id):
    return (
        "❌ You don't have access.\n\n"
        f"🆔 Your Telegram ID: {user_id}\n\n"
        f"📩 Access लेने के लिए {CONTACT_HANDLE} से contact करें."
    )


def main_menu_kb():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📥 Receive", callback_data="menu_receive"),
                InlineKeyboardButton("📤 Upload", callback_data="menu_upload"),
            ]
        ]
    )


def receive_type_kb():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🖼 Thumbnail", callback_data="recv:thumbnail"),
                InlineKeyboardButton("🎬 Video", callback_data="recv:video"),
            ]
        ]
    )


def upload_type_kb():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🖼 Thumbnail", callback_data="upl:thumbnail"),
                InlineKeyboardButton("🎬 Video", callback_data="upl:video"),
            ]
        ]
    )


def format_remaining(seconds):
    if seconds <= 0:
        return "0m"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if not parts:
        parts.append(f"{seconds % 60}s")
    return " ".join(parts)


YOUTUBE_INSTRUCTION_HTML = (
    "🎬 <b>Video Received!</b>\n\n"
    "Ab ye follow karo:\n\n"
    "1️⃣ YouTube par video upload karo\n"
    "2️⃣ Thumbnail le lena\n"
    "3️⃣ Acha title + description likho\n"
    "4️⃣ Tag section me tags daalo taki video viral ho\n"
    "5️⃣ YouTube ke comment section me ye paste karo (tap to copy):\n\n"
    "<code>𝘿𝙊𝙒𝙉𝙇𝙊𝘼𝘿 𝙇𝙄𝙉𝙆 - https://t.me/+3ng9H1jBE9JhMzI1\n"
    "𝙊𝙒𝙉𝙀𝙍 - https://t.me/GVM_TRUST</code>\n\n"
    "⏳ 2 minute baad bot aapse puchhega ki YouTube par upload hua ya nahi."
)


# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------
class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_db()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except sqlite3.OperationalError as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error(f"DB Lock: {e}")
            raise
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def init_db(self):
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir:
            try:
                os.makedirs(db_dir, exist_ok=True)
            except Exception as e:
                logger.warning(f"Could not create DB directory {db_dir}: {e}")

        with self.get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    authorized INTEGER DEFAULT 0,
                    created_at TEXT,
                    updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS uploads (
                    upload_id TEXT PRIMARY KEY,
                    uploader_id INTEGER,
                    uploader_username TEXT,
                    uploader_name TEXT,
                    upload_type TEXT,
                    telegram_file_id TEXT,
                    telegram_file_unique_id TEXT,
                    filename TEXT,
                    mime_type TEXT,
                    created_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deliveries (
                    upload_id TEXT,
                    receiver_id INTEGER,
                    status TEXT DEFAULT 'reserved',
                    reserved_at TEXT,
                    delivered_at TEXT,
                    PRIMARY KEY (upload_id, receiver_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cooldowns (
                    user_id INTEGER,
                    upload_type TEXT,
                    last_received_at TEXT,
                    PRIMARY KEY (user_id, upload_type)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_uploads_type ON uploads(upload_type)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_uploads_uploader ON uploads(uploader_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_deliveries_recv ON deliveries(receiver_id, status)"
            )
            logger.info("Database initialized")

        try:
            self.release_stale_reservations()
        except Exception as e:
            logger.warning(f"startup release_stale_reservations failed: {e}")

    # -------- users --------
    def get_user(self, user_id):
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM users WHERE user_id = ?", (user_id,)
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_user error: {e}")
            return None

    def upsert_user(self, user_id, username, full_name):
        try:
            now = datetime.utcnow().isoformat()
            with self.get_connection() as conn:
                row = conn.execute(
                    "SELECT user_id FROM users WHERE user_id = ?", (user_id,)
                ).fetchone()
                if row:
                    conn.execute(
                        "UPDATE users SET username = ?, full_name = ?, updated_at = ? WHERE user_id = ?",
                        (username or "", full_name or "", now, user_id),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO users (user_id, username, full_name, authorized, created_at, updated_at)
                        VALUES (?, ?, ?, 0, ?, ?)
                        """,
                        (user_id, username or "", full_name or "", now, now),
                    )
        except Exception as e:
            logger.error(f"upsert_user error: {e}")

    def authorize_user(self, user_id):
        try:
            now = datetime.utcnow().isoformat()
            with self.get_connection() as conn:
                row = conn.execute(
                    "SELECT user_id FROM users WHERE user_id = ?", (user_id,)
                ).fetchone()
                if row:
                    conn.execute(
                        "UPDATE users SET authorized = 1, updated_at = ? WHERE user_id = ?",
                        (now, user_id),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO users (user_id, username, full_name, authorized, created_at, updated_at)
                        VALUES (?, '', '', 1, ?, ?)
                        """,
                        (user_id, now, now),
                    )
        except Exception as e:
            logger.error(f"authorize_user error: {e}")

    def remove_user(self, user_id):
        try:
            now = datetime.utcnow().isoformat()
            with self.get_connection() as conn:
                conn.execute(
                    "UPDATE users SET authorized = 0, updated_at = ? WHERE user_id = ?",
                    (now, user_id),
                )
        except Exception as e:
            logger.error(f"remove_user error: {e}")

    def is_authorized(self, user_id):
        if user_id == OWNER_ID:
            return True
        try:
            user = self.get_user(user_id)
            return bool(user and user["authorized"])
        except Exception as e:
            logger.error(f"is_authorized error: {e}")
            return False

    def get_all_authorized_receivers(self):
        try:
            with self.get_connection() as conn:
                rows = conn.execute(
                    "SELECT user_id FROM users WHERE authorized = 1"
                ).fetchall()
                ids = [r["user_id"] for r in rows]
                if OWNER_ID not in ids:
                    ids.append(OWNER_ID)
                return ids
        except Exception as e:
            logger.error(f"get_all_authorized_receivers error: {e}")
            return [OWNER_ID]

    def get_all_uploaders(self):
        try:
            with self.get_connection() as conn:
                rows = conn.execute(
                    "SELECT DISTINCT uploader_id FROM uploads"
                ).fetchall()
                return [r["uploader_id"] for r in rows if r["uploader_id"]]
        except Exception as e:
            logger.error(f"get_all_uploaders error: {e}")
            return []

    # -------- uploads --------
    def store_upload(
        self,
        uploader_id,
        uploader_username,
        uploader_name,
        upload_type,
        telegram_file_id,
        telegram_file_unique_id,
        filename,
        mime_type,
    ):
        try:
            upload_id = str(uuid.uuid4())
            now = datetime.utcnow().isoformat()
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO uploads
                    (upload_id, uploader_id, uploader_username, uploader_name, upload_type,
                     telegram_file_id, telegram_file_unique_id, filename, mime_type, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        upload_id,
                        uploader_id,
                        uploader_username,
                        uploader_name,
                        upload_type,
                        telegram_file_id,
                        telegram_file_unique_id,
                        filename,
                        mime_type,
                        now,
                    ),
                )
            return upload_id
        except Exception as e:
            logger.error(f"store_upload error: {e}")
            return None

    def get_all_uploads(self):
        try:
            with self.get_connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM uploads ORDER BY created_at DESC"
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"get_all_uploads error: {e}")
            return []

    def get_uploads_by_uploader(self, uploader_id):
        try:
            with self.get_connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM uploads WHERE uploader_id = ? ORDER BY created_at DESC",
                    (uploader_id,),
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"get_uploads_by_uploader error: {e}")
            return []

    def get_upload_by_id(self, upload_id):
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM uploads WHERE upload_id = ?", (upload_id,)
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_upload_by_id error: {e}")
            return None

    def get_upload_receive_count(self, upload_id):
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM deliveries WHERE upload_id = ? AND status = 'sent'",
                    (upload_id,),
                ).fetchone()
                return int(row["c"]) if row else 0
        except Exception as e:
            logger.error(f"get_upload_receive_count error: {e}")
            return 0

    def delete_upload(self, upload_id):
        try:
            with self.get_connection() as conn:
                conn.execute(
                    "DELETE FROM deliveries WHERE upload_id = ?", (upload_id,)
                )
                conn.execute(
                    "DELETE FROM uploads WHERE upload_id = ?", (upload_id,)
                )
            return True
        except Exception as e:
            logger.error(f"delete_upload error: {e}")
            return False

    def has_uploaded_today(self, user_id):
        try:
            today = datetime.utcnow().date().isoformat()
            with self.get_connection() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM uploads WHERE uploader_id = ? AND created_at LIKE ?",
                    (user_id, f"{today}%"),
                ).fetchone()
                return int(row["c"]) > 0 if row else False
        except Exception as e:
            logger.error(f"has_uploaded_today error: {e}")
            return False

    # -------- deliveries --------
    def reserve_upload(self, upload_id, receiver_id):
        try:
            with self.get_connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT status FROM deliveries WHERE upload_id = ? AND receiver_id = ?",
                    (upload_id, receiver_id),
                ).fetchone()
                now = datetime.utcnow().isoformat()
                if row:
                    if row["status"] == "sent":
                        return (False, "already")
                    conn.execute(
                        "UPDATE deliveries SET reserved_at = ? WHERE upload_id = ? AND receiver_id = ?",
                        (now, upload_id, receiver_id),
                    )
                    return (True, "ok")
                conn.execute(
                    """
                    INSERT INTO deliveries (upload_id, receiver_id, status, reserved_at)
                    VALUES (?, ?, 'reserved', ?)
                    """,
                    (upload_id, receiver_id, now),
                )
                return (True, "ok")
        except Exception as e:
            logger.error(f"reserve_upload error: {e}")
            return (False, "error")

    def mark_sent(self, upload_id, receiver_id):
        try:
            with self.get_connection() as conn:
                now = datetime.utcnow().isoformat()
                conn.execute(
                    """
                    UPDATE deliveries SET status = 'sent', delivered_at = ?
                    WHERE upload_id = ? AND receiver_id = ?
                    """,
                    (now, upload_id, receiver_id),
                )
        except Exception as e:
            logger.error(f"mark_sent error: {e}")

    def release_reservation(self, upload_id, receiver_id):
        try:
            with self.get_connection() as conn:
                conn.execute(
                    "DELETE FROM deliveries WHERE upload_id = ? AND receiver_id = ? AND status = 'reserved'",
                    (upload_id, receiver_id),
                )
        except Exception as e:
            logger.error(f"release_reservation error: {e}")

    def release_stale_reservations(self):
        try:
            cutoff = (
                datetime.utcnow() - timedelta(minutes=STALE_RESERVATION_MINUTES)
            ).isoformat()
            with self.get_connection() as conn:
                cur = conn.execute(
                    "DELETE FROM deliveries WHERE status = 'reserved' AND reserved_at < ?",
                    (cutoff,),
                )
                if cur.rowcount:
                    logger.info(f"Released {cur.rowcount} stale reservation(s)")
        except Exception as e:
            logger.error(f"release_stale_reservations error: {e}")

    def get_unreceived_upload(self, receiver_id, upload_type):
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    """
                    SELECT u.* FROM uploads u
                    WHERE u.upload_type = ?
                    AND NOT EXISTS (
                        SELECT 1 FROM deliveries d
                        WHERE d.upload_id = u.upload_id
                        AND d.receiver_id = ?
                        AND d.status = 'sent'
                    )
                    ORDER BY u.created_at ASC
                    LIMIT 1
                    """,
                    (upload_type, receiver_id),
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_unreceived_upload error: {e}")
            return None

    def get_last_received_upload(self, receiver_id, upload_type):
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    """
                    SELECT u.* FROM uploads u
                    JOIN deliveries d ON d.upload_id = u.upload_id
                    WHERE d.receiver_id = ? AND d.status = 'sent' AND u.upload_type = ?
                    ORDER BY u.created_at DESC
                    LIMIT 1
                    """,
                    (receiver_id, upload_type),
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_last_received_upload error: {e}")
            return None

    # -------- cooldowns --------
    def get_last_received_at(self, user_id, upload_type):
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    "SELECT last_received_at FROM cooldowns WHERE user_id = ? AND upload_type = ?",
                    (user_id, upload_type),
                ).fetchone()
                if row and row["last_received_at"]:
                    try:
                        return datetime.fromisoformat(row["last_received_at"])
                    except Exception:
                        return None
                return None
        except Exception as e:
            logger.error(f"get_last_received_at error: {e}")
            return None

    def set_last_received_at(self, user_id, upload_type):
        try:
            now = datetime.utcnow()
            with self.get_connection() as conn:
                row = conn.execute(
                    "SELECT 1 FROM cooldowns WHERE user_id = ? AND upload_type = ?",
                    (user_id, upload_type),
                ).fetchone()
                if row:
                    conn.execute(
                        "UPDATE cooldowns SET last_received_at = ? WHERE user_id = ? AND upload_type = ?",
                        (now.isoformat(), user_id, upload_type),
                    )
                else:
                    conn.execute(
                        "INSERT INTO cooldowns (user_id, upload_type, last_received_at) VALUES (?, ?, ?)",
                        (user_id, upload_type, now.isoformat()),
                    )
        except Exception as e:
            logger.error(f"set_last_received_at error: {e}")

    def get_cooldown_remaining(self, user_id, upload_type):
        if user_id == OWNER_ID:
            return 0
        last = self.get_last_received_at(user_id, upload_type)
        if not last:
            return 0
        elapsed = (datetime.utcnow() - last).total_seconds()
        remaining = COOLDOWN_HOURS * 3600 - elapsed
        return int(remaining) if remaining > 0 else 0


db = Database(DB_PATH)


# ---------------------------------------------------------------------------
# COMMAND HANDLERS
# ---------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if user is None or update.message is None:
            return
        db.upsert_user(user.id, user.username or "", user.full_name or "")
        await update.message.reply_text(
            "👋 Welcome!\nUse /help to check all command and help.",
            reply_markup=main_menu_kb(),
        )
    except Exception as e:
        logger.error(f"/start error: {e}", exc_info=e)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.message is None:
            return
        user = update.effective_user
        is_owner = user is not None and user.id == OWNER_ID
        lines = [
            "📖 Help & Commands",
            "",
            "/start - Welcome menu",
            "/help - Show this help",
            "/upload - Upload Thumbnail / Video (open to all)",
            "/receive - Receive Thumbnail / Video (authorized only)",
            "/checkyourupload - Your uploaded files",
            "/checkall - All uploads with receive count",
        ]
        if is_owner:
            lines += [
                "",
                "👑 Owner commands:",
                "/add USERID - Authorize a user",
                "/remove USERID - Remove a user",
                "/filter - Send all uploaded documents to owner",
                "/delete_post UPLOAD_ID - Delete an upload",
                "/ownerbroadcast - Broadcast a message (receiver / uploader)",
            ]
        lines += [
            "",
            "ℹ️ Only Telegram DOCUMENT/FILE uploads are accepted.",
            f"⏳ Receive cooldown: {COOLDOWN_HOURS} hours (Owner: unlimited).",
        ]
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.error(f"/help error: {e}", exc_info=e)


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if update.message is None or user is None:
            return
        if user.id != OWNER_ID:
            await update.message.reply_text("❌ Owner only command.")
            return
        if not context.args:
            await update.message.reply_text("Usage: /add USERID")
            return
        raw = context.args[0].strip()
        if not raw.isdigit():
            await update.message.reply_text(
                "❌ Invalid Telegram ID. It must be numeric."
            )
            return
        target = int(raw)
        if target <= 0:
            await update.message.reply_text("❌ Invalid Telegram ID.")
            return
        if target == OWNER_ID or target == user.id:
            await update.message.reply_text(
                "❌ You cannot authorize yourself. Owner is always authorized."
            )
            return
        db.authorize_user(target)
        await update.message.reply_text(
            f"✅ Authorized successfully.\n🆔 User ID: {target}"
        )
    except Exception as e:
        logger.error(f"/add error: {e}", exc_info=e)


async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if update.message is None or user is None:
            return
        if user.id != OWNER_ID:
            await update.message.reply_text("❌ Owner only command.")
            return
        if not context.args:
            await update.message.reply_text("Usage: /remove USERID")
            return
        raw = context.args[0].strip()
        if not raw.isdigit():
            await update.message.reply_text(
                "❌ Invalid Telegram ID. It must be numeric."
            )
            return
        target = int(raw)
        if target == OWNER_ID:
            await update.message.reply_text("❌ Owner cannot be removed.")
            return
        db.remove_user(target)
        await update.message.reply_text(
            f"✅ Removed authorization.\n🆔 User ID: {target}"
        )
    except Exception as e:
        logger.error(f"/remove error: {e}", exc_info=e)


async def cmd_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if update.message is None or user is None:
            return
        db.upsert_user(user.id, user.username or "", user.full_name or "")
        context.user_data.pop("pending_upload_type", None)
        await update.message.reply_text(
            "📤 Upload\n\nChoose the upload type:",
            reply_markup=upload_type_kb(),
        )
    except Exception as e:
        logger.error(f"/upload error: {e}", exc_info=e)


async def cmd_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if update.message is None or user is None:
            return
        if not db.is_authorized(user.id):
            await update.message.reply_text(unauth_msg(user.id))
            return
        await update.message.reply_text(
            "📥 Receive\n\nChoose the file type:",
            reply_markup=receive_type_kb(),
        )
    except Exception as e:
        logger.error(f"/receive error: {e}", exc_info=e)


async def cmd_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if update.message is None or user is None:
            return
        if user.id != OWNER_ID:
            await update.message.reply_text("❌ Owner only command.")
            return

        uploads = db.get_all_uploads()
        if not uploads:
            await update.message.reply_text("📭 No uploads found.")
            return

        await update.message.reply_text(
            f"📤 Sending {len(uploads)} upload(s) to you..."
        )

        for u in uploads:
            caption = (
                f"Upload ID: {u['upload_id']}\n"
                f"Type: {u['upload_type']}\n"
                f"Uploader Name: {u['uploader_name'] or 'Unknown'}\n"
                f"Uploader ID: {u['uploader_id']}"
            )
            try:
                await context.bot.send_document(
                    chat_id=OWNER_ID,
                    document=u["telegram_file_id"],
                    caption=caption,
                )
            except Forbidden:
                logger.warning("Forbidden sending filter file to owner")
                break
            except BadRequest as e:
                logger.warning(
                    f"BadRequest sending filter file {u['upload_id']}: {e}"
                )
            except (TimedOut, NetworkError) as e:
                logger.warning(f"Network issue sending filter file: {e}")
            except TelegramError as e:
                logger.warning(f"Telegram error sending filter file: {e}")
            except Exception as e:
                logger.error(f"Unexpected error sending filter file: {e}")
            await asyncio.sleep(0.1)
    except Exception as e:
        logger.error(f"/filter error: {e}", exc_info=e)
        try:
            await update.message.reply_text("❌ Error sending filter report.")
        except Exception:
            pass


async def cmd_delete_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if update.message is None or user is None:
            return
        if user.id != OWNER_ID:
            await update.message.reply_text("❌ Owner only command.")
            return
        if not context.args:
            await update.message.reply_text("Usage: /delete_post UPLOAD_ID")
            return
        upload_id = context.args[0].strip()
        upload = db.get_upload_by_id(upload_id)
        if not upload:
            await update.message.reply_text("❌ Upload not found.")
            return
        ok = db.delete_upload(upload_id)
        if ok:
            await update.message.reply_text(
                f"🗑️ Deleted upload.\n🆔 ID: {upload_id}"
            )
        else:
            await update.message.reply_text("❌ Delete failed.")
    except Exception as e:
        logger.error(f"/delete_post error: {e}", exc_info=e)


async def cmd_checkyourupload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if update.message is None or user is None:
            return
        if not db.is_authorized(user.id):
            await update.message.reply_text(unauth_msg(user.id))
            return
        uploads = db.get_uploads_by_uploader(user.id)
        if not uploads:
            await update.message.reply_text("📭 You have no uploads.")
            return
        lines = ["📁 Your Uploads:", ""]
        for u in uploads:
            count = db.get_upload_receive_count(u["upload_id"])
            lines.append(
                f"• {u['filename']}\n"
                f"  Type: {u['upload_type']} | Received: {count}\n"
                f"  ID: {u['upload_id']}"
            )
        for chunk in split_message("\n".join(lines)):
            await update.message.reply_text(chunk)
    except Exception as e:
        logger.error(f"/checkyourupload error: {e}", exc_info=e)


async def cmd_checkall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if update.message is None or user is None:
            return
        if not db.is_authorized(user.id):
            await update.message.reply_text(unauth_msg(user.id))
            return
        uploads = db.get_all_uploads()
        if not uploads:
            await update.message.reply_text("📭 No uploads.")
            return

        lines = ["📊 All Uploads:", ""]
        for u in uploads:
            count = db.get_upload_receive_count(u["upload_id"])
            lines.append(
                f"• {u['filename']}\n"
                f"  Type: {u['upload_type']} | Received: {count}\n"
                f"  ID: {u['upload_id']}"
            )
        for chunk in split_message("\n".join(lines)):
            await update.message.reply_text(chunk)
    except Exception as e:
        logger.error(f"/checkall error: {e}", exc_info=e)


async def cmd_ownerbroadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        if update.message is None or user is None:
            return
        if user.id != OWNER_ID:
            await update.message.reply_text("❌ Owner only command.")
            return
        context.user_data["awaiting_broadcast"] = True
        context.user_data.pop("broadcast_text", None)
        await update.message.reply_text(
            "📢 Send the broadcast message now.\n"
            "I will then show you two buttons:\n"
            "• 📥 Send to Receivers\n"
            "• 📤 Send to Uploaders"
        )
    except Exception as e:
        logger.error(f"/ownerbroadcast error: {e}", exc_info=e)


async def _broadcast_to(chat_ids, text, context):
    sent = 0
    for uid in chat_ids:
        try:
            await context.bot.send_message(uid, text)
            sent += 1
        except Forbidden:
            logger.warning(f"Broadcast forbidden for {uid}")
        except (BadRequest, TimedOut, NetworkError) as e:
            logger.warning(f"Broadcast error for {uid}: {e}")
        except TelegramError as e:
            logger.warning(f"Broadcast telegram error for {uid}: {e}")
        except Exception as e:
            logger.warning(f"Broadcast unexpected error for {uid}: {e}")
        await asyncio.sleep(0.05)
    return sent


# ---------------------------------------------------------------------------
# RECEIVE FLOW
# ---------------------------------------------------------------------------
def _type_to_db(t: str) -> str:
    return "Thumbnail" if t == "thumbnail" else "Video"


async def _schedule_yt_followup(bot, user_id, delay):
    try:
        await asyncio.sleep(delay)
        if not PENDING_YT_REMINDERS.get(user_id):
            return
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ YES", callback_data="yt:yes"),
                    InlineKeyboardButton("❌ NO", callback_data="yt:no"),
                ]
            ]
        )
        await bot.send_message(
            user_id,
            "🎬 YouTube me video upload ki kya?\n\nYES / NO dabao.",
            reply_markup=kb,
        )
    except Exception as e:
        logger.warning(f"yt follow-up send failed for {user_id}: {e}")


async def _do_receive(chat_id, user, upload_type_key, context):
    upload_type = _type_to_db(upload_type_key)

    if not db.is_authorized(user.id):
        try:
            await context.bot.send_message(chat_id, unauth_msg(user.id))
        except Exception:
            pass
        return

    if user.id != OWNER_ID:
        remaining = db.get_cooldown_remaining(user.id, upload_type)
        if remaining > 0:
            try:
                await context.bot.send_message(
                    chat_id,
                    f"⏳ Cooldown active.\nPlease try again in {format_remaining(remaining)}.",
                )
            except Exception:
                pass
            return

    upload = db.get_unreceived_upload(user.id, upload_type)
    if not upload:
        last = db.get_last_received_upload(user.id, upload_type)
        if last:
            count = db.get_upload_receive_count(last["upload_id"])
            try:
                await context.bot.send_message(
                    chat_id,
                    f"Already received.\n\n📊 Receive count: {count}",
                )
            except Exception:
                pass
        else:
            try:
                await context.bot.send_message(
                    chat_id,
                    f"❌ No new {upload_type.lower()} files available.",
                )
            except Exception:
                pass
        return

    ok, reason = db.reserve_upload(upload["upload_id"], user.id)
    if not ok:
        if reason == "already":
            count = db.get_upload_receive_count(upload["upload_id"])
            try:
                await context.bot.send_message(
                    chat_id,
                    f"Already received.\n\n📊 Receive count: {count}",
                )
            except Exception:
                pass
        elif reason == "error":
            try:
                await context.bot.send_message(
                    chat_id, "⚠️ Please try again in a moment."
                )
            except Exception:
                pass
        return

    try:
        await context.bot.send_document(
            chat_id=user.id,
            document=upload["telegram_file_id"],
            filename=upload.get("filename") or None,
        )
        db.mark_sent(upload["upload_id"], user.id)
        if user.id != OWNER_ID:
            db.set_last_received_at(user.id, upload_type)
        db.release_stale_reservations()

        if upload_type == "Video":
            try:
                await context.bot.send_message(
                    chat_id,
                    YOUTUBE_INSTRUCTION_HTML,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.warning(f"YT instruction send failed: {e}")

            PENDING_YT_REMINDERS[user.id] = True
            asyncio.create_task(
                _schedule_yt_followup(context.bot, user.id, YOUTUBE_INITIAL_DELAY)
            )
    except Forbidden:
        db.release_reservation(upload["upload_id"], user.id)
        logger.warning(f"Forbidden sending to user {user.id}")
    except BadRequest as e:
        db.release_reservation(upload["upload_id"], user.id)
        logger.error(f"BadRequest sending file: {e}")
        try:
            await context.bot.send_message(chat_id, "❌ Send failed. Try again.")
        except Exception:
            pass
    except (TimedOut, NetworkError) as e:
        db.release_reservation(upload["upload_id"], user.id)
        logger.error(f"Network error sending file: {e}")
        try:
            await context.bot.send_message(chat_id, "⚠️ Network issue. Try again.")
        except Exception:
            pass
    except TelegramError as e:
        db.release_reservation(upload["upload_id"], user.id)
        logger.error(f"Telegram error sending file: {e}")
    except Exception as e:
        db.release_reservation(upload["upload_id"], user.id)
        logger.error(f"Unexpected error sending file: {e}", exc_info=e)


# ---------------------------------------------------------------------------
# CALLBACK HANDLER
# ---------------------------------------------------------------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None:
        return
    try:
        await query.answer()
    except Exception:
        pass

    user = query.from_user
    data = query.data or ""

    try:
        if data == "menu_receive":
            if not db.is_authorized(user.id):
                try:
                    await query.edit_message_text(unauth_msg(user.id))
                except Exception:
                    try:
                        await query.message.reply_text(unauth_msg(user.id))
                    except Exception:
                        pass
                return
            try:
                await query.edit_message_text(
                    "📥 Receive\n\nChoose the file type:",
                    reply_markup=receive_type_kb(),
                )
            except Exception:
                try:
                    await query.message.reply_text(
                        "📥 Receive\n\nChoose the file type:",
                        reply_markup=receive_type_kb(),
                    )
                except Exception:
                    pass
            return

        if data == "menu_upload":
            context.user_data.pop("pending_upload_type", None)
            try:
                await query.edit_message_text(
                    "📤 Upload\n\nChoose the upload type:",
                    reply_markup=upload_type_kb(),
                )
            except Exception:
                try:
                    await query.message.reply_text(
                        "📤 Upload\n\nChoose the upload type:",
                        reply_markup=upload_type_kb(),
                    )
                except Exception:
                    pass
            return

        if data.startswith("recv:"):
            key = data.split(":", 1)[1]
            if key not in ("thumbnail", "video"):
                return
            await _do_receive(query.message.chat_id, user, key, context)
            return

        if data.startswith("upl:"):
            key = data.split(":", 1)[1]
            if key not in ("thumbnail", "video"):
                return
            context.user_data["pending_upload_type"] = key
            label = "Thumbnail" if key == "thumbnail" else "Video"
            try:
                await query.edit_message_text(
                    f"📤 Selected: {label}\n\nNow send the file as a Telegram DOCUMENT."
                )
            except Exception:
                try:
                    await query.message.reply_text(
                        f"📤 Selected: {label}\n\nNow send the file as a Telegram DOCUMENT."
                    )
                except Exception:
                    pass
            return

        if data == "bc:receiver":
            if user.id != OWNER_ID:
                return
            text = context.user_data.get("broadcast_text", "")
            ids = db.get_all_authorized_receivers()
            try:
                await query.edit_message_text("📤 Sending to receivers...")
            except Exception:
                pass
            sent = await _broadcast_to(ids, text, context)
            try:
                await query.edit_message_text(
                    f"✅ Broadcast sent to {sent} receiver(s)."
                )
            except Exception:
                pass
            return

        if data == "bc:uploader":
            if user.id != OWNER_ID:
                return
            text = context.user_data.get("broadcast_text", "")
            ids = db.get_all_uploaders()
            try:
                await query.edit_message_text("📤 Sending to uploaders...")
            except Exception:
                pass
            sent = await _broadcast_to(ids, text, context)
            try:
                await query.edit_message_text(
                    f"✅ Broadcast sent to {sent} uploader(s)."
                )
            except Exception:
                pass
            return

        if data == "rem:yes":
            if db.has_uploaded_today(user.id):
                try:
                    await query.edit_message_text(
                        "✅ Theek hai, ho gaya!\n\n"
                        "Aaj ka upload complete. Aage koi reminder nahi aayega aaj."
                    )
                except Exception:
                    pass
            else:
                try:
                    await query.edit_message_text(
                        "❌ Nahi, aapne aaj upload nahi kiya.\n\n"
                        "Jaldi /upload se apna Thumbnail ya Video upload karo.\n"
                        "Tab tak reminder aate rahenge."
                    )
                except Exception:
                    pass
            return

        if data == "rem:no":
            try:
                await query.edit_message_text(
                    "📤 Jao /upload karo bot me aur aaj ka Thumbnail ya Video upload karo.\n\n"
                    "Jaldi karo, reminder aate rahenge jab tak upload nahi hota."
                )
            except Exception:
                pass
            return

        if data == "yt:yes":
            PENDING_YT_REMINDERS[user.id] = False
            try:
                await query.edit_message_text(
                    "✅ Shabash! YouTube upload complete.\n\n"
                    "Aage bhi isi tarah karte raho 💪"
                )
            except Exception:
                pass
            return

        if data == "yt:no":
            if not PENDING_YT_REMINDERS.get(user.id):
                PENDING_YT_REMINDERS[user.id] = True
            try:
                await query.edit_message_text(
                    "⚠️ Jaldi YouTube par video upload karo!\n\n"
                    "Har 10 minute me bot aapko yaad dilata rahega."
                )
            except Exception:
                pass
            asyncio.create_task(
                _schedule_yt_followup(context.bot, user.id, YOUTUBE_REPEAT_DELAY)
            )
            return

    except Exception as e:
        logger.error(f"on_callback error: {e}", exc_info=e)


# ---------------------------------------------------------------------------
# MESSAGE HANDLERS
# ---------------------------------------------------------------------------
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.message is None or update.effective_user is None:
            return
        user = update.effective_user
        db.upsert_user(user.id, user.username or "", user.full_name or "")

        doc = update.message.document
        if doc is None:
            return

        filename = doc.file_name or "file"
        mime_type = doc.mime_type or "application/octet-stream"

        pending = context.user_data.get("pending_upload_type")
        if pending == "thumbnail":
            upload_type = "Thumbnail"
        elif pending == "video":
            upload_type = "Video"
        elif mime_type.startswith("image/"):
            upload_type = "Thumbnail"
        elif mime_type.startswith("video/"):
            upload_type = "Video"
        else:
            await update.message.reply_text(
                "❌ Unknown file type.\n"
                "Please use /upload and select Thumbnail or Video first, "
                "or send an image/video document."
            )
            return

        upload_id = db.store_upload(
            user.id,
            user.username or "",
            user.full_name or "",
            upload_type,
            doc.file_id,
            doc.file_unique_id,
            filename,
            mime_type,
        )
        context.user_data.pop("pending_upload_type", None)

        if upload_id:
            await update.message.reply_text(
                f"✅ Uploaded as {upload_type}.\n\n"
                f"🆔 Upload ID: {upload_id}\n"
                f"📎 File: {filename}"
            )
        else:
            await update.message.reply_text("❌ Upload failed. Please try again.")
    except Forbidden:
        logger.warning("Forbidden in handle_document")
    except BadRequest as e:
        logger.error(f"BadRequest in handle_document: {e}")
    except (TimedOut, NetworkError) as e:
        logger.error(f"Network issue in handle_document: {e}")
    except TelegramError as e:
        logger.error(f"Telegram error in handle_document: {e}")
    except Exception as e:
        logger.error(f"handle_document error: {e}", exc_info=e)
        try:
            await update.message.reply_text("❌ Error uploading file.")
        except Exception:
            pass


async def handle_photo_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.message is None:
            return
        await update.message.reply_text(
            "❌ Only Telegram DOCUMENT/FILE uploads are accepted.\n"
            "Please send the file as a document (attach as file), not as photo/video."
        )
    except Exception as e:
        logger.error(f"handle_photo_video error: {e}", exc_info=e)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.message is None or update.effective_user is None:
            return
        user = update.effective_user

        if user.id == OWNER_ID and context.user_data.get("awaiting_broadcast"):
            context.user_data["awaiting_broadcast"] = False
            text = update.message.text or ""
            context.user_data["broadcast_text"] = text
            kb = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "📥 Send to Receivers", callback_data="bc:receiver"
                        ),
                        InlineKeyboardButton(
                            "📤 Send to Uploaders", callback_data="bc:uploader"
                        ),
                    ]
                ]
            )
            await update.message.reply_text(
                f"📢 Broadcast preview:\n\n{text}\n\nChoose audience:",
                reply_markup=kb,
            )
            return
    except Exception as e:
        logger.error(f"handle_text error: {e}", exc_info=e)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.error("Exception while handling an update:", exc_info=context.error)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# REMINDER LOOP
# ---------------------------------------------------------------------------
async def reminder_loop():
    global CURRENT_BOT
    await asyncio.sleep(90)  # wait for startup
    while True:
        try:
            bot = CURRENT_BOT
            if bot is not None:
                uploaders = db.get_all_uploaders()
                for uid in uploaders:
                    if uid == OWNER_ID:
                        continue
                    if db.has_uploaded_today(uid):
                        continue
                    try:
                        kb = InlineKeyboardMarkup(
                            [
                                [
                                    InlineKeyboardButton(
                                        "✅ YES", callback_data="rem:yes"
                                    ),
                                    InlineKeyboardButton(
                                        "❌ NO", callback_data="rem:no"
                                    ),
                                ]
                            ]
                        )
                        await bot.send_message(
                            uid,
                            "🔔 OWNER KI DEAL KE ANUSAR\n\n"
                            "Aaj ka apne Thumbnail ya Video jo aapko bola tha,\n"
                            "kya upload kar diya?\n\n"
                            "YES / NO dabao.",
                            reply_markup=kb,
                        )
                    except Forbidden:
                        logger.warning(f"Reminder forbidden for {uid}")
                    except (BadRequest, TimedOut, NetworkError) as e:
                        logger.warning(f"Reminder error for {uid}: {e}")
                    except TelegramError as e:
                        logger.warning(f"Reminder telegram error for {uid}: {e}")
                    except Exception as e:
                        logger.warning(f"Reminder unexpected error for {uid}: {e}")
                    await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"reminder_loop error: {e}", exc_info=e)

        await asyncio.sleep(REMINDER_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# HEALTH SERVER
# ---------------------------------------------------------------------------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            pass

    def do_HEAD(self):
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
        except Exception:
            pass

    def log_message(self, format, *args):
        return


def run_health_server():
    while True:
        try:
            server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
            logger.info(f"Health server listening on 0.0.0.0:{PORT}")
            server.serve_forever()
        except Exception as e:
            logger.error(f"Health server error: {e}", exc_info=e)
            import time as _t
            _t.sleep(5)


# ---------------------------------------------------------------------------
# APP BUILDER
# ---------------------------------------------------------------------------
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("remove", cmd_remove))
    app.add_handler(CommandHandler("upload", cmd_upload))
    app.add_handler(CommandHandler("receive", cmd_receive))
    app.add_handler(CommandHandler("filter", cmd_filter))
    app.add_handler(CommandHandler("delete_post", cmd_delete_post))
    app.add_handler(CommandHandler("checkyourupload", cmd_checkyourupload))
    app.add_handler(CommandHandler("checkall", cmd_checkall))
    app.add_handler(CommandHandler("ownerbroadcast", cmd_ownerbroadcast))

    app.add_handler(CallbackQueryHandler(on_callback))

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(
        MessageHandler(filters.PHOTO | filters.VIDEO, handle_photo_video)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
    )

    app.add_error_handler(on_error)
    return app


# ---------------------------------------------------------------------------
# MAIN — 24x7 robust polling loop
# ---------------------------------------------------------------------------
async def run_bot_forever():
    """
    Rebuilds the Application on fatal errors so the bot never stays dead.
    Health server runs in background. Reminder loop runs once, referencing
    CURRENT_BOT which is updated on each restart.
    """
    global CURRENT_BOT

    # Start reminder loop once
    asyncio.create_task(reminder_loop())

    backoff = 3
    while True:
        app = None
        try:
            logger.info("Building Telegram application...")
            app = build_app()

            await app.initialize()
            await app.start()

            CURRENT_BOT = app.bot

            logger.info("Starting polling...")
            await app.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=False,
                poll_interval=1.0,
                timeout=30,
            )

            # If start_polling returns, polling has stopped. Wait for it or exit.
            # Keep this coroutine alive by waiting on updater.running
            while app.updater.running:
                await asyncio.sleep(5)

            logger.warning("Polling stopped gracefully.")
        except Conflict as e:
            logger.error(
                f"Conflict: {e}. Another instance might be running. Waiting..."
            )
        except TelegramError as e:
            logger.error(f"Telegram error in main loop: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
        finally:
            CURRENT_BOT = None
            if app is not None:
                try:
                    if app.updater and app.updater.running:
                        await app.updater.stop()
                except Exception as e:
                    logger.warning(f"updater.stop() error: {e}")
                try:
                    await app.stop()
                except Exception as e:
                    logger.warning(f"app.stop() error: {e}")
                try:
                    await app.shutdown()
                except Exception as e:
                    logger.warning(f"app.shutdown() error: {e}")

            try:
                gc.collect()
            except Exception:
                pass

        logger.info(f"Restarting in {backoff}s...")
        await asyncio.sleep(backoff)
        # small bounded backoff to avoid tight restart loops
        backoff = min(backoff + 3, 30)


def main():
    # Health server thread
    health_thread = Thread(target=run_health_server, daemon=True)
    health_thread.start()
    logger.info(f"Health server thread started (PID: {os.getpid()})")

    while True:
        try:
            asyncio.run(run_bot_forever())
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Fatal error, restarting event loop: {e}", exc_info=True)
            import time as _t
            _t.sleep(5)


if __name__ == "__main__":
    main()
