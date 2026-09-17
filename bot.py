#!/usr/bin/env python3
"""
Telegram File Receiver Bot — Production Patched
Buttons, Authorization, Cooldown, Duplicate Protection, Reservation Recovery
SQLite persistence, HTTP health server + polling in one process
"""

import os
import sys
import sqlite3
import uuid
import asyncio
import logging
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
            conn.rollback()
            logger.error(f"DB Lock: {e}")
            raise
        except Exception:
            conn.rollback()
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

        # Release stale reservations at startup (do not delete DB)
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

    # -------- deliveries --------
    def reserve_upload(self, upload_id, receiver_id):
        """
        Returns (ok: bool, reason: str)
        reason: 'ok' | 'already' | 'error'
        """
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
                    # already reserved - refresh timestamp, treat as ok
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
        # Never reset authorization — just record/update profile
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
            "/upload - Upload a Thumbnail / Video document",
            "/receive - Receive a Thumbnail / Video document",
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
        if not raw.lstrip("-").isdigit() or not raw.isdigit():
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
        if not db.is_authorized(user.id):
            await update.message.reply_text(unauth_msg(user.id))
            return
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


# ---------------------------------------------------------------------------
# RECEIVE FLOW
# ---------------------------------------------------------------------------
def _type_to_db(t: str) -> str:
    return "Thumbnail" if t == "thumbnail" else "Video"


async def _do_receive(chat_id, user, upload_type_key, context):
    """Handle actual file delivery to user for a given type."""
    upload_type = _type_to_db(upload_type_key)

    if not db.is_authorized(user.id):
        try:
            await context.bot.send_message(chat_id, unauth_msg(user.id))
        except Exception:
            pass
        return

    # Cooldown check
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
# CALLBACK QUERY HANDLER
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
            if not db.is_authorized(user.id):
                try:
                    await query.edit_message_text(unauth_msg(user.id))
                except Exception:
                    try:
                        await query.message.reply_text(unauth_msg(user.id))
                    except Exception:
                        pass
                return
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
            if not db.is_authorized(user.id):
                try:
                    await query.edit_message_text(unauth_msg(user.id))
                except Exception:
                    pass
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

    except Exception as e:
        logger.error(f"on_callback error: {e}", exc_info=e)


# ---------------------------------------------------------------------------
# DOCUMENT / MEDIA HANDLERS
# ---------------------------------------------------------------------------
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Accept ONLY Telegram DOCUMENT uploads."""
    try:
        if update.message is None or update.effective_user is None:
            return
        user = update.effective_user

        if not db.is_authorized(user.id):
            await update.message.reply_text(unauth_msg(user.id))
            return

        doc = update.message.document
        if doc is None:
            return

        filename = doc.file_name or "file"
        mime_type = doc.mime_type or "application/octet-stream"

        # Determine upload type: pending selection first, else derive from mime
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
    """Reject normal photo/video messages."""
    try:
        if update.message is None:
            return
        user = update.effective_user
        if user is None:
            return
        if not db.is_authorized(user.id):
            await update.message.reply_text(unauth_msg(user.id))
            return
        await update.message.reply_text(
            "❌ Only Telegram DOCUMENT/FILE uploads are accepted.\n"
            "Please send the file as a document (attach as file), not as photo/video."
        )
    except Exception as e:
        logger.error(f"handle_photo_video error: {e}", exc_info=e)


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
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        logger.info(f"Health server listening on 0.0.0.0:{PORT}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}", exc_info=e)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
async def main():
    # Health server in background thread
    health_thread = Thread(target=run_health_server, daemon=True)
    health_thread.start()
    logger.info(f"Health server thread started (PID: {os.getpid()})")

    app = Application.builder().token(BOT_TOKEN).build()

    # Command handlers
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

    # Callback handler
    app.add_handler(CallbackQueryHandler(on_callback))

    # Document handler
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Reject photo/video
    app.add_handler(
        MessageHandler(filters.PHOTO | filters.VIDEO, handle_photo_video)
    )

    logger.info("Bot polling starting...")
    await app.initialize()
    await app.start()

    backoff = 1
    while True:
        try:
            logger.info(f"Starting polling (backoff={backoff}s)")
            await app.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=False,
            )
            backoff = 1
            logger.warning("Polling stopped, restarting...")
            await asyncio.sleep(2)
        except TelegramError as e:
            logger.error(f"Telegram error in polling: {e}")
            await asyncio.sleep(min(backoff, 60))
            backoff = min(backoff * 2, 60)
        except Exception as e:
            logger.error(f"Unexpected error in polling: {e}", exc_info=e)
            await asyncio.sleep(min(backoff, 60))
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=e)
        sys.exit(1)
