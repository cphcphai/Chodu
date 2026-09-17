# Telegram File Receiver Bot — Production

A production-grade bot for managing file uploads and controlled distribution to authorized users with cooldowns, duplicate protection, and persistent state across restarts.

## Features

- **Authorization System**: Owner controls who gets access
- **Cooldown Tracking**: Users get 3-hour cooldowns between thumbnail/video requests (owner exempt)
- **Duplicate Protection**: Same upload never delivered twice to same user; reservation-based with recovery
- **Persistent State**: SQLite database survives restarts
- **Stale Reservation Recovery**: Cleans up reservations older than 30 minutes
- **No File Conversion**: Stores only Telegram file IDs; no download/encode/compress
- **Health Server**: HTTP endpoint at `0.0.0.0:$PORT` returns `OK` for load balancers
- **Single Process**: Polling + health server run together

## Environment Variables

```bash
BOT_TOKEN          # Your Telegram bot token (required)
OWNER_ID           # Your Telegram user ID (required)
DB_PATH            # Path to SQLite database (default: bot.db)
PORT               # HTTP health server port (default: 8080, Render sets automatically)
```

## Installation

### Local Development

```bash
git clone <repo>
cd telegram-file-receiver-bot

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp .env.example .env
# Edit .env with your BOT_TOKEN and OWNER_ID

# Run bot
python bot.py
```

### Render Deployment

1. **Push code to GitHub** (with render.yaml in root)

2. **Create new Web Service on Render**:
   - Connect GitHub repo
   - Runtime: Python 3.11
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python bot.py`
   - Add environment variables:
     - `BOT_TOKEN`: Your Telegram bot token
     - `OWNER_ID`: Your Telegram user ID
     - `DB_PATH`: `/var/data/bot.db` (persistent)
     - `PORT`: (Render sets automatically)

3. **Database Persistence**:
   - Render mounts `/var/data/` as persistent storage
   - Database survives service restarts
   - Path must be `/var/data/bot.db` in render.yaml

4. **Health Check**:
   - Render pings `GET /` every 30 seconds
   - Bot responds with HTTP 200 OK
   - Service stays alive as long as health check passes

## Bot Commands

### User Commands

```
/start              Show welcome message or access denied
/thumbnail          Receive next unreceived thumbnail (3h cooldown)
/video              Receive next unreceived video (3h cooldown)
```

### Owner Commands

```
/add USERID         Authorize a user
/remove USERID      Revoke user access
/filter             List all uploads with receive count (no receiver list)
/delete_post ID     Delete upload and its delivery records
```

## Upload Flow

1. **Owner uploads document** (MIME type determines category)
   - `image/*` → Thumbnail
   - `video/*` → Video
   - Other → Document (rarely used)

2. **Bot generates UUID** (`upload_id`)

3. **Metadata stored in SQLite**:
   - Telegram file_id (never expires within message lifetime)
   - Telegram file_unique_id (for duplicate detection)
   - Filename, MIME type, uploader info
   - Upload timestamp

4. **No file downloaded or converted**

## Receive Flow

1. **User requests** `/thumbnail` or `/video`

2. **Bot checks authorization**:
   - If not authorized: Show Telegram ID and "@GVM_TRUST" contact
   - If authorized: Continue

3. **Bot checks cooldown** (3 hours, separate per type, owner exempt)

4. **Bot finds next unreceived upload** of requested type (ordered by recency)

5. **Reservation phase** (`BEGIN IMMEDIATE` transaction):
   - Check if already received
   - If yes: "Already received, try another"
   - If no: Create delivery record with status=`reserved`
   - Timestamp saved for stale cleanup

6. **Send via Telegram API**:
   - If successful: Mark delivery status=`sent`, update cooldown
   - If failed (network, bot blocked, etc): Release reservation

7. **Stale cleanup**: Remove reservations older than 30 minutes

## Cooldown System

- **Duration**: 3 hours per upload type (separate timers)
  - Requesting thumbnail doesn't block video request
  - Requesting video doesn't block thumbnail request

- **Owner bypass**: OWNER_ID ignores cooldowns

- **Persistence**: Stored in SQLite `cooldowns` table
  - Survives bot restart
  - Last received timestamp updated on every successful delivery

## Duplicate Protection

### Reservation Pattern

```
User clicks /thumbnail
  ↓
BEGIN IMMEDIATE transaction
  ↓
Check if (upload_id, user_id) exists in deliveries
  ├─ If status='sent': Reject (already received)
  ├─ If status='reserved': Replace timestamp (re-request safety)
  └─ If not exists: INSERT with status='reserved'
  ↓
COMMIT
  ↓
Send file
  ├─ Success: UPDATE status='sent', UPDATE cooldown
  └─ Failure: DELETE reservation
  ↓
Stale cleanup (every send): DELETE reserved older than 30min
```

### Edge Cases Handled

| Scenario | Result |
|----------|--------|
| Rapid clicks same upload | First reserves, second gets existing record |
| Send fails mid-delivery | Reservation deleted; next request tries new upload |
| Bot crashes mid-send | Reservation remains; stale cleanup removes it after 30min |
| User requests while reserved | Re-request safe (timestamp updated) |
| Same upload to 100 different users | 100 separate delivery records, all succeed |

## Database Schema

### users
```sql
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    authorized INTEGER DEFAULT 0,
    created_at TEXT,
    updated_at TEXT
);
```

### uploads
```sql
CREATE TABLE uploads (
    upload_id TEXT PRIMARY KEY,
    uploader_id INTEGER,
    uploader_username TEXT,
    uploader_name TEXT,
    upload_type TEXT,              -- Thumbnail, Video, Document
    telegram_file_id TEXT UNIQUE,
    telegram_file_unique_id TEXT UNIQUE,
    filename TEXT,
    mime_type TEXT,
    created_at TEXT
);
```

### deliveries
```sql
CREATE TABLE deliveries (
    upload_id TEXT,
    receiver_id INTEGER,
    status TEXT DEFAULT 'reserved', -- reserved, sent
    reserved_at TEXT,
    delivered_at TEXT,
    PRIMARY KEY (upload_id, receiver_id),
    FOREIGN KEY (upload_id) REFERENCES uploads(upload_id)
);
```

### cooldowns
```sql
CREATE TABLE cooldowns (
    user_id INTEGER,
    upload_type TEXT,               -- Thumbnail, Video
    last_received_at TEXT,
    PRIMARY KEY (user_id, upload_type)
);
```

## Health Endpoint

```bash
GET /
Response: 200 OK
Body: OK
```

Used by Render to keep service alive. Responds even if Telegram API is down.

## Error Handling

| Error | Bot Behavior |
|-------|-------------|
| Telegram API timeout | Log error, release reservation, notify user |
| SQLite lock (concurrent access) | Retry with 10s timeout, fail gracefully |
| Invalid callback (old button click) | Ignore silently |
| Network partition | Reservation released, next request works |
| Bot token invalid | Fails on startup, logs error |
| Missing OWNER_ID | Fails on startup, logs error |

## Testing Checklist

- [ ] **Auth**: Unauthorized `/start` shows ID + contact message
- [ ] **Auth**: Authorized user sees command menu
- [ ] **Auth**: `/add USERID` authorizes
- [ ] **Auth**: Authorization persists after restart
- [ ] **Upload**: Owner uploads thumbnail (image file)
- [ ] **Upload**: Owner uploads video (video file)
- [ ] **Upload**: Non-owner upload rejected
- [ ] **Receive**: Authorized user `/thumbnail` gets file
- [ ] **Receive**: Authorized user `/video` gets file
- [ ] **Receive**: Cooldown blocks 2nd request within 3 hours
- [ ] **Receive**: Owner bypasses cooldown
- [ ] **Receive**: Same upload reaches multiple users
- [ ] **Receive**: Same user never gets same upload twice
- [ ] **Receive**: Rapid clicks (duplicate protection) handled
- [ ] **Receive**: Send failure releases reservation
- [ ] **Owner**: `/filter` shows uploads + receive count (no receivers)
- [ ] **Owner**: `/delete_post` removes upload + deliveries
- [ ] **Health**: `GET /` returns 200 OK
- [ ] **Persistence**: Kill bot, restart, cooldowns/auth/uploads still there
- [ ] **Stale Cleanup**: 30+ minute old reservation removed

## Troubleshooting

### Bot not responding
- Check `BOT_TOKEN` is valid
- Check bot is not blocked by Telegram
- Check logs for exceptions

### Database locked errors
- Normal during high concurrency; bot retries automatically
- Reduce if using multiple bot instances (not recommended)

### Files not sending
- Check file isn't too large (Telegram has limits per type)
- Check file MIME type is correct
- Check Telegram API isn't rate-limited

### Health check failing
- Bot crashed: Check logs
- Port not binding: Check PORT env var
- Firewall blocking: Render default firewall allows health checks

### Cooldown not working
- Check `DB_PATH` is persistent (`/var/data/bot.db` on Render)
- Check last_received_at timestamp in cooldowns table

## Deployment Checklist

- [ ] `render.yaml` in repo root
- [ ] `BOT_TOKEN` added as Render env var
- [ ] `OWNER_ID` added as Render env var
- [ ] `DB_PATH` set to `/var/data/bot.db`
- [ ] requirements.txt includes `python-telegram-bot==22.0`
- [ ] Health check returns 200 OK
- [ ] Bot responds to `/start` in Telegram
- [ ] Owner can upload files
- [ ] Owner can authorize users
- [ ] User can receive files with cooldown

## License

MIT
