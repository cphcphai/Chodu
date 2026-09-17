#!/usr/bin/env python3
"""
Telegram File Receiver Bot — Production Fixed
Authorization, Cooldown, Duplicate Protection, Reservation Recovery
SQLite persistence, HTTP health server + polling in one process
With /checkall, /checkyourupload, message splitting, polling recovery, error handling
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

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError

# Environment
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DB_PATH = os.getenv("DB_PATH", "bot.db")
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN or OWNER_ID == 0:
    print("ERROR: BOT_TOKEN and OWNER_ID required")
    sys.exit(1)

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# UTILITY
# ============================================================================


def split_message(text, max_length=4096):
    """Split long message into chunks for Telegram."""
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


# ============================================================================
# DATABASE
# ============================================================================


class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_db()

    @contextmanager
    def get_connection(self):
        """Thread-safe connection with timeout."""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except sqlite3.OperationalError as e:
            conn.rollback()
            logger.error(f"DB Lock: {e}")
            raise
        finally:
            conn.close()

    def init_db(self):
        """Initialize schema if not exists."""
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
                    telegram_file_id TEXT UNIQUE,
                    telegram_file_unique_id TEXT UNIQUE,
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
                    PRIMARY KEY (upload_id, receiver_id),
                    FOREIGN KEY (upload_id) REFERENCES uploads(upload_id)
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
            logger.info("Database initialized")

    # User authorization
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

    def authorize_user(self, user_id, username, full_name):
        try:
            with self.get_connection() as conn:
                user = self.get_user(user_id)
                now = datetime.utcnow().isoformat()
                if user:
                    conn.execute(
                        "UPDATE users SET authorized = 1, updated_at = ? WHERE user_id = ?",
                        (now, user_id),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO users (user_id, username, full_name, authorized, created_at, updated_at)
                        VALUES (?, ?, ?, 1, ?, ?)
                    """,
                        (user_id, username, full_name, now, now),
                    )
        except Exception as e:
            logger.error(f"authorize_user error: {e}")

    def remove_user(self, user_id):
        try:
            with self.get_connection() as conn:
                conn.execute(
                    "UPDATE users SET authorized = 0 WHERE user_id = ?", (user_id,)
                )
        except Exception as e:
            logger.error(f"remove_user error: {e}")

    def is_authorized(self, user_id):
        try:
            user = self.get_user(user_id)
            return user and user["authorized"]
        except Exception as e:
            logger.error(f"is_authorized error: {e}")
            return False

    # Uploads
    def store_upload(self, uploader_id, uploader_username, uploader_name, upload_type, 
                     telegram_file_id, telegram_file_unique_id, filename, mime_type):
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
                    "SELECT upload_id, filename, upload_type, created_at, uploader_id, uploader_username, uploader_name FROM uploads ORDER BY created_at DESC"
                ).fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"get_all_uploads error: {e}")
            return []

    def get_uploads_by_uploader(self, uploader_id):
        try:
            with self.get_connection() as conn:
                rows = conn.execute(
                    "SELECT upload_id, filename, upload_type, created_at, uploader_username, uploader_name FROM uploads WHERE uploader_id = ? ORDER BY created_at DESC",
                    (uploader_id,)
                ).fetchall()
                return [dict(row) for row in rows]
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
                    "SELECT COUNT(*) as count FROM deliveries WHERE upload_id = ? AND status = 'sent'",
                    (upload_id,),
                ).fetchone()
                return row["count"] if row else 0
        except Exception as e:
            logger.error(f"get_upload_receive_count error: {e}")
            return 0

    def delete_upload(self, upload_id):
        try:
            with self.get_connection() as conn:
                conn.execute("DELETE FROM deliveries WHERE upload_id = ?", (upload_id,))
                conn.execute("DELETE FROM uploads WHERE upload_id = ?", (upload_id,))
        except Exception as e:
            logger.error(f"delete_upload error: {e}")

    # Reservations & Deliveries
    def reserve_upload(self, upload_id, receiver_id):
        """BEGIN IMMEDIATE, reserve upload."""
        try:
            with self.get_connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    existing = conn.execute(
                        "SELECT status FROM deliveries WHERE upload_id = ? AND receiver_id = ?",
                        (upload_id, receiver_id),
                    ).fetchone()
                    if existing:
                        if existing["status"] == "sent":
                            return False, "Already received"
                        # Already reserved, replace timestamp
                        now = datetime.utcnow().isoformat()
                        conn.execute(
                            "UPDATE deliveries SET reserved_at = ? WHERE upload_id = ? AND receiver_id = ?",
                            (now, upload_id, receiver_id),
                        )
                        return True, "Reserved"
                    else:
                        now = datetime.utcnow().isoformat()
                        conn.execute(
                            """
                            INSERT INTO deliveries (upload_id, receiver_id, status, reserved_at)
                            VALUES (?, ?, 'reserved', ?)
                        """,
                            (upload_id, receiver_id, now),
                        )
                        return True, "Reserved"
                except Exception as e:
                    conn.rollback()
                    logger.error(f"reserve_upload error: {e}")
                    raise
        except Exception as e:
            logger.error(f"reserve_upload transaction error: {e}")
            return False, "Error"

    def mark_sent(self, upload_id, receiver_id):
        """Mark as sent. Assume already reserved."""
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
        """Delete if still reserved."""
        try:
            with self.get_connection() as conn:
                conn.execute(
                    "DELETE FROM deliveries WHERE upload_id = ? AND receiver_id = ? AND status = 'reserved'",
                    (upload_id, receiver_id),
                )
        except Exception as e:
            logger.error(f"release_reservation error: {e}")

    def release_stale_reservations(self):
        """Remove reservations older than 30 min."""
        try:
            with self.get_connection() as conn:
                cutoff = (datetime.utcnow() - timedelta(minutes=30)).isoformat()
                conn.execute(
                    "DELETE FROM deliveries WHERE status = 'reserved' AND reserved_at < ?",
                    (cutoff,),
                )
        except Exception as e:
            logger.error(f"release_stale_reservations error: {e}")

    def get_unreceived_upload(self, receiver_id, upload_type):
        """Get next unreceived upload of type."""
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    """
                    SELECT u.upload_id, u.telegram_file_id, u.filename, u.mime_type
                    FROM uploads u
                    WHERE u.upload_type = ?
                    AND NOT EXISTS (
                        SELECT 1 FROM deliveries d
                        WHERE d.upload_id = u.upload_id
                        AND d.receiver_id = ?
                        AND d.status = 'sent'
                    )
                    ORDER BY u.created_at DESC
                    LIMIT 1
                """,
                    (upload_type, receiver_id),
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_unreceived_upload error: {e}")
            return None

    # Cooldowns
    def get_last_received_at(self, user_id, upload_type):
        try:
            with self.get_connection() as conn:
                row = conn.execute(
                    "SELECT last_received_at FROM cooldowns WHERE user_id = ? AND upload_type = ?",
                    (user_id, upload_type),
                ).fetchone()
                if row and row["last_received_at"]:
                    return datetime.fromisoformat(row["last_received_at"])
                return None
        except Exception as e:
            logger.error(f"get_last_received_at error: {e}")
            return None

    def set_last_received_at(self, user_id, upload_type):
        try:
            now = datetime.utcnow()
            with self.get_connection() as conn:
                existing = conn.execute(
                    "SELECT 1 FROM cooldowns WHERE user_id = ? AND upload_type = ?",
                    (user_id, upload_type),
                ).fetchone()
                if existing:
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

    def is_on_cooldown(self, user_id, upload_type):
        try:
            last = self.get_last_received_at(user_id, upload_type)
            if not last:
                return False
            return datetime.utcnow() - last < timedelta(hours=3)
        except Exception as e:
            logger.error(f"is_on_cooldown error: {e}")
            return False


db = Database(DB_PATH)


# ============================================================================
# HANDLERS
# ============================================================================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User /start."""
    try:
        user_id = update.effective_user.id
        if db.is_authorized(user_id):
            await update.message.reply_text(
                "Welcome back! Choose:\n\n"
                "/thumbnail - Get thumbnail\n"
                "/video - Get video"
            )
        else:
            await update.message.reply_text(
                f"Access denied.\n\n"
                f"Your Telegram ID: `{user_id}`\n\n"
                f"Contact @GVM_TRUST for access.",
                parse_mode="Markdown",
            )
    except Exception as e:
        logger.error(f"start handler error: {e}", exc_info=e)


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner /add USERID"""
    try:
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("Owner only.")
            return

        if not context.args or len(context.args) < 1:
            await update.message.reply_text("Usage: /add USERID")
            return

        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Invalid user ID.")
            return

        db.authorize_user(target_id, "", "")
        await update.message.reply_text(f"✅ User {target_id} authorized.")
    except Exception as e:
        logger.error(f"cmd_add error: {e}", exc_info=e)


async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner /remove USERID"""
    try:
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("Owner only.")
            return

        if not context.args or len(context.args) < 1:
            await update.message.reply_text("Usage: /remove USERID")
            return

        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Invalid user ID.")
            return

        db.remove_user(target_id)
        await update.message.reply_text(f"✅ User {target_id} removed.")
    except Exception as e:
        logger.error(f"cmd_remove error: {e}", exc_info=e)


async def cmd_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner /filter - Generate and send a document file with all uploads."""
    try:
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("Owner only.")
            return

        uploads = db.get_all_uploads()
        if not uploads:
            await update.message.reply_text("No uploads.")
            return

        # Create text content for file
        content = "File Name | Uploader | Total Received\n"
        content += "=" * 60 + "\n"
        
        for u in uploads:
            count = db.get_upload_receive_count(u["upload_id"])
            uploader = u['uploader_username'] or u['uploader_name']
            content += f"{u['filename']} | {uploader} | {count}\n"

        # Write to temporary file
        temp_file = f"/tmp/uploads_report_{OWNER_ID}.txt"
        with open(temp_file, 'w') as f:
            f.write(content)

        # Send file to owner
        with open(temp_file, 'rb') as f:
            await context.bot.send_document(
                chat_id=OWNER_ID,
                document=f,
                filename=f"uploads_report.txt"
            )
        
        # Clean up
        os.remove(temp_file)
        
    except Exception as e:
        logger.error(f"cmd_filter error: {e}", exc_info=e)
        try:
            await update.message.reply_text("❌ Error generating report.")
        except:
            pass


async def cmd_checkall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all uploads with receive count. Owner sees usernames, others don't."""
    try:
        user_id = update.effective_user.id
        
        if not db.is_authorized(user_id):
            await update.message.reply_text(f"Access denied. Your ID: `{user_id}`", parse_mode="Markdown")
            return
        
        uploads = db.get_all_uploads()
        if not uploads:
            await update.message.reply_text("No uploads.")
            return

        # Owner sees usernames, others don't
        if user_id == OWNER_ID:
            msg = "📊 **All Uploads (with uploader):**\n\n"
            for u in uploads:
                count = db.get_upload_receive_count(u["upload_id"])
                uploader = u['uploader_username'] or u['uploader_name']
                msg += f"{u['filename']} ({count}) - {uploader}\n"
        else:
            msg = "📊 **All Uploads:**\n\n"
            for u in uploads:
                count = db.get_upload_receive_count(u["upload_id"])
                msg += f"{u['filename']} ({count})\n"

        chunks = split_message(msg, max_length=4096)
        for chunk in chunks:
            await update.message.reply_text(chunk, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"cmd_checkall error: {e}", exc_info=e)


async def cmd_checkyourupload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Any authorized user - List own uploads with receive count."""
    try:
        user_id = update.effective_user.id
        
        if not db.is_authorized(user_id):
            await update.message.reply_text(f"Access denied. Your ID: `{user_id}`", parse_mode="Markdown")
            return

        uploads = db.get_uploads_by_uploader(user_id)
        if not uploads:
            await update.message.reply_text("No uploads from you.")
            return

        msg = "📁 **Your Uploads:**\n\n"
        for u in uploads:
            count = db.get_upload_receive_count(u["upload_id"])
            msg += f"{u['filename']} ({count})\n"

        chunks = split_message(msg, max_length=4096)
        for chunk in chunks:
            await update.message.reply_text(chunk, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"cmd_checkyourupload error: {e}", exc_info=e)


async def cmd_delete_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner /delete_post UPLOAD_ID"""
    try:
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("Owner only.")
            return

        if not context.args or len(context.args) < 1:
            await update.message.reply_text("Usage: /delete_post UPLOAD_ID")
            return

        upload_id = context.args[0]
        upload = db.get_upload_by_id(upload_id)
        if not upload:
            await update.message.reply_text("Upload not found.")
            return

        db.delete_upload(upload_id)
        await update.message.reply_text(f"🗑️ Deleted {upload['filename']}")
    except Exception as e:
        logger.error(f"cmd_delete_post error: {e}", exc_info=e)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Accept ONLY Telegram DOCUMENT uploads. Reject photo/video."""
    try:
        user_id = update.effective_user.id
        username = update.effective_user.username or "unknown"
        full_name = update.effective_user.full_name or "User"

        # Only owner can upload
        if user_id != OWNER_ID:
            await update.message.reply_text("Upload not allowed.")
            return

        # Reject photo/video
        if update.message.photo or update.message.video:
            await update.message.reply_text("Documents only. No photos or videos.")
            return

        # Accept document
        if not update.message.document:
            return

        doc = update.message.document
        file_id = doc.file_id
        file_unique_id = doc.file_unique_id
        filename = doc.file_name or "file"
        mime_type = doc.mime_type or "application/octet-stream"

        # Determine type from MIME
        if mime_type.startswith("image/"):
            upload_type = "Thumbnail"
        elif mime_type.startswith("video/"):
            upload_type = "Video"
        else:
            upload_type = "Document"

        upload_id = db.store_upload(
            user_id, username, full_name, upload_type, file_id, file_unique_id, filename, mime_type
        )

        if upload_id:
            await update.message.reply_text(
                f"✅ Uploaded: {filename}\n\n" f"ID: `{upload_id}`", parse_mode="Markdown"
            )
        else:
            await update.message.reply_text("❌ Upload failed.")
    except Exception as e:
        logger.error(f"handle_document error: {e}", exc_info=e)
        try:
            await update.message.reply_text("❌ Error uploading file.")
        except:
            pass


async def cmd_thumbnail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request thumbnail."""
    try:
        user_id = update.effective_user.id

        if not db.is_authorized(user_id):
            if update.message:
                await update.message.reply_text(
                    f"Access denied.\n\nYour ID: `{user_id}`\n\nContact @GVM_TRUST",
                    parse_mode="Markdown",
                )
            return

        # Owner bypass cooldown
        if user_id != OWNER_ID and db.is_on_cooldown(user_id, "Thumbnail"):
            last = db.get_last_received_at(user_id, "Thumbnail")
            wait_until = last + timedelta(hours=3)
            await update.message.reply_text(
                f"⏱️ Cooldown. Try after: {wait_until.strftime('%H:%M:%S UTC')}"
            )
            return

        upload = db.get_unreceived_upload(user_id, "Thumbnail")
        if not upload:
            await update.message.reply_text("❌ No new thumbnails.")
            return

        reserved, msg = db.reserve_upload(upload["upload_id"], user_id)
        if not reserved:
            await update.message.reply_text("Already received this one. Try /thumbnail again.")
            return

        try:
            await context.bot.send_document(
                chat_id=user_id,
                document=upload["telegram_file_id"],
                filename=upload["filename"],
                parse_mode=None,
            )
            db.mark_sent(upload["upload_id"], user_id)
            db.set_last_received_at(user_id, "Thumbnail")
            db.release_stale_reservations()
        except TelegramError as e:
            db.release_reservation(upload["upload_id"], user_id)
            logger.error(f"Send failed: {e}")
            await update.message.reply_text("❌ Send failed. Try again.")
    except Exception as e:
        logger.error(f"cmd_thumbnail error: {e}", exc_info=e)
        try:
            if update.message:
                await update.message.reply_text("❌ Error processing request.")
        except:
            pass


async def cmd_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request video."""
    try:
        user_id = update.effective_user.id

        if not db.is_authorized(user_id):
            if update.message:
                await update.message.reply_text(
                    f"Access denied.\n\nYour ID: `{user_id}`\n\nContact @GVM_TRUST",
                    parse_mode="Markdown",
                )
            return

        # Owner bypass cooldown
        if user_id != OWNER_ID and db.is_on_cooldown(user_id, "Video"):
            last = db.get_last_received_at(user_id, "Video")
            wait_until = last + timedelta(hours=3)
            await update.message.reply_text(
                f"⏱️ Cooldown. Try after: {wait_until.strftime('%H:%M:%S UTC')}"
            )
            return

        upload = db.get_unreceived_upload(user_id, "Video")
        if not upload:
            await update.message.reply_text("❌ No new videos.")
            return

        reserved, msg = db.reserve_upload(upload["upload_id"], user_id)
        if not reserved:
            await update.message.reply_text("Already received this one. Try /video again.")
            return

        try:
            await context.bot.send_document(
                chat_id=user_id,
                document=upload["telegram_file_id"],
                filename=upload["filename"],
                parse_mode=None,
            )
            db.mark_sent(upload["upload_id"], user_id)
            db.set_last_received_at(user_id, "Video")
            db.release_stale_reservations()
        except TelegramError as e:
            db.release_reservation(upload["upload_id"], user_id)
            logger.error(f"Send failed: {e}")
            await update.message.reply_text("❌ Send failed. Try again.")
    except Exception as e:
        logger.error(f"cmd_video error: {e}", exc_info=e)
        try:
            if update.message:
                await update.message.reply_text("❌ Error processing request.")
        except:
            pass


# ============================================================================
# HEALTH SERVER (HTTP)
# ============================================================================


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress logs


def run_health_server():
    """Run HTTP health server on 0.0.0.0:PORT."""
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        logger.info(f"Health server listening on 0.0.0.0:{PORT}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Health server error: {e}", exc_info=e)


# ============================================================================
# MAIN
# ============================================================================


async def main():
    """Run polling + health server with auto-reconnect."""
    # Start health server in background thread
    health_thread = Thread(target=run_health_server, daemon=True)
    health_thread.start()
    logger.info(f"Health server started (PID: {os.getpid()})")

    # Build application
    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("remove", cmd_remove))
    app.add_handler(CommandHandler("filter", cmd_filter))
    app.add_handler(CommandHandler("checkall", cmd_checkall))
    app.add_handler(CommandHandler("checkyourupload", cmd_checkyourupload))
    app.add_handler(CommandHandler("delete_post", cmd_delete_post))
    app.add_handler(CommandHandler("thumbnail", cmd_thumbnail))
    app.add_handler(CommandHandler("video", cmd_video))

    # Document handler
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Start polling with reconnect logic
    logger.info("Bot polling started")
    await app.initialize()
    await app.start()
    
    backoff = 1
    while True:
        try:
            logger.info(f"Starting polling (backoff: {backoff}s)")
            await app.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                error_callback=lambda u, e: logger.error(f"Polling error: {e}", exc_info=e)
            )
            # If polling stops normally, reset backoff
            backoff = 1
            logger.warning("Polling stopped, restarting...")
            await asyncio.sleep(2)
        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
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
