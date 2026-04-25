import discord
from discord.ext import commands, tasks
import datetime
import re
import asyncio
import os
import random
import gspread
import json
from google.oauth2.service_account import Credentials
import csv
import io
import sqlite3
import threading
import functools
import time
import concurrent.futures
from dotenv import load_dotenv

load_dotenv()



# --- DATABASE SETUP ---
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database.db')
DB_LOCK = threading.RLock()
DB_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=200)

def db_retry(max_retries=5, backoff=0.1):
    """Decorator to retry database operations on OperationalError (locked)."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_err = None
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower():
                        last_err = e
                        print(f"⚠️ DB Locked, retrying ({i+1}/{max_retries})...")
                        time.sleep(backoff * (2 ** i))
                        continue
                    raise e
            print(f"❌ DB retry limit reached: {last_err}")
            raise last_err
        return wrapper
    return decorator

# Thread-local connection pool for performance
_local = threading.local()

def _get_db_conn():
    """Returns a thread-local cached sqlite3 connection with row factory. Auto-heals broken connections."""
    conn = getattr(_local, 'conn', None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")  # Health check
        except Exception:
            try: conn.close()
            except Exception: pass
            conn = None
            _local.conn = None
    if conn is None:
        conn = sqlite3.connect(DB_PATH, timeout=120)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        _local.conn = conn
    return conn

@db_retry()
def init_wal_mode():
    """Consolidated: WAL mode and performance PRAGMAs are now primarily handled in the connection factory."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                pass # The factory already ensures WAL mode
        print("✅ SQLite Performance Engine (WAL) Active.")
    except Exception as e:
        print(f"⚠️ Performance Optimization Note: {e}")

@db_retry()
def _ensure_log_schema():
    """Automated migration to ensure schema is up-to-date."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                # Check for lobby_protection column
                cursor = conn.execute("PRAGMA table_info(registration_log)")
                columns = [col[1] for col in cursor.fetchall()]
                if 'lobby_protection' not in columns:
                    print("⚠️ Migrating: Adding 'lobby_protection' column to registration_log...")
                    conn.execute("ALTER TABLE registration_log ADD COLUMN lobby_protection TEXT DEFAULT ''")
                    conn.commit()
                    print("✅ Migration Complete.")

                # Create deleted_teams archive table if it doesn't exist
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS deleted_teams (
                        discord_id TEXT,
                        team_name TEXT,
                        owner_name TEXT,
                        owner_email TEXT,
                        contact TEXT,
                        player1 TEXT,
                        player2 TEXT,
                        player3 TEXT,
                        player4 TEXT,
                        sub1 TEXT,
                        sub2 TEXT,
                        ig_handles TEXT,
                        lobby TEXT,
                        tagged_ids TEXT,
                        p1_tag TEXT,
                        p2_tag TEXT,
                        p3_tag TEXT,
                        p4_tag TEXT,
                        deleted_at TEXT
                    )
                ''')
                conn.commit()

                # Add created_at column to teams table if missing
                cursor2 = conn.execute("PRAGMA table_info(teams)")
                team_columns = [col[1] for col in cursor2.fetchall()]
                if 'created_at' not in team_columns:
                    print("⚠️ Migrating: Adding 'created_at' column to teams...")
                    conn.execute("ALTER TABLE teams ADD COLUMN created_at TEXT DEFAULT ''")
                    conn.commit()
                    print("✅ 'created_at' column added to teams.")
                if 'tagged_names' not in team_columns:
                    print("⚠️ Migrating: Adding 'tagged_names' column to teams...")
                    conn.execute("ALTER TABLE teams ADD COLUMN tagged_names TEXT DEFAULT ''")
                    conn.commit()

                # --- PERFORMANCE INDICES ---
                conn.execute("CREATE INDEX IF NOT EXISTS idx_teams_discord_id ON teams (discord_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_teams_lobby ON teams (lobby)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_log_discord_id ON registration_log (discord_id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_log_p1_tag ON registration_log (p1_tag)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_log_p2_tag ON registration_log (p2_tag)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_log_p3_tag ON registration_log (p3_tag)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_log_p4_tag ON registration_log (p4_tag)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_discord_id ON pending_teams (discord_id)")
                conn.commit()

                # --- BAN TABLE MIGRATION ---
                ban_cursor = conn.execute("PRAGMA table_info(bans)")
                ban_columns = [col[1] for col in ban_cursor.fetchall()]
                for col_name in ['reason', 'banned_by', 'team_name']:
                    if col_name not in ban_columns:
                        conn.execute(f"ALTER TABLE bans ADD COLUMN {col_name} TEXT DEFAULT ''")
                        conn.commit()
                print("✅ Database schema & indices ready.")
    except Exception as e:
        print(f"Error in _ensure_log_schema: {e}")

async def _get_tagger_role(guild: discord.Guild):
    """Gets or creates the Tagger role for the confirm channel."""
    role = discord.utils.get(guild.roles, name=TAGGER_ROLE_NAME)
    if not role:
        try:
            role = await guild.create_role(
                name=TAGGER_ROLE_NAME,
                reason='Auto-created for tag-confirm channel access'
            )
            # Set permissions on the confirm channel for this role
            confirm_ch = guild.get_channel(CONFIRM_CHANNEL_ID)
            if confirm_ch:
                await confirm_ch.set_permissions(role, send_messages=True, create_public_threads=False, reason='Tagger role access')
            print(f"✅ Created '{TAGGER_ROLE_NAME}' role and set channel permissions.")
        except Exception as e:
            print(f"❌ Error creating Tagger role: {e}")
            return None
    return role

TAGGER_EXPIRY_SECONDS = 120  # 2 minutes

async def _schedule_tagger_expiry(member: discord.Member, user_id: str, session_id: str):
    """Removes the Tagger role after 2 minutes if user hasn't tagged yet.
    
    Smart Logic:
    - Checks session_id to prevent "Ghost Deletions" from overlapping sessions.
    - NEW team (no lobby, first-time) → DELETE registration, user must re-fill everything
    - OLD team (has lobby or established profile) → Only clear session, KEEP team data
    """
    await asyncio.sleep(TAGGER_EXPIRY_SECONDS)
    # 🛑 GHOST DELETION PROTECTION: Only proceed if this is the ACTIVE session
    session_data = awaiting_tag.get(user_id)
    if session_data and session_data.get('session_id') == session_id:
        team_name = session_data.get('team_name', 'Unknown')
        try:
            tagger_role = discord.utils.get(member.guild.roles, name=TAGGER_ROLE_NAME)
            if tagger_role and tagger_role in member.roles:
                await member.remove_roles(tagger_role, reason="Tagger expired (2 min timeout)")
            
            # 🔍 SMART CHECK: Is this an established team or a brand new one?
            loop = asyncio.get_event_loop()
            team_data = await loop.run_in_executor(None, _get_team, user_id)
            
            if team_data:
                has_lobby = bool(team_data.get('lobby'))
                has_tags = bool(team_data.get('tagged_ids'))
                
                if has_lobby or has_tags:
                    # 🛡️ OLD/ESTABLISHED team — only clear session, KEEP profile
                    pass
                else:
                    # 🗑️ NEW team — never tagged, no lobby → DELETE so user must re-register
                    await loop.run_in_executor(None, _soft_delete_team, user_id)
            
            # Always clear runtime state
            awaiting_tag.pop(user_id, None)
            
            # Notify in confirm channel
            confirm_ch = member.guild.get_channel(CONFIRM_CHANNEL_ID)
            if confirm_ch:
                await confirm_ch.send(
                    f"⏳ {member.mention}, your tagging session for **{team_name}** expired. "
                    f"{'Please register again.' if not (team_data and (bool(team_data.get('lobby')) or bool(team_data.get('tagged_ids')))) else 'Your team profile is saved. Click Register again to re-tag.'}",
                    delete_after=20
                )
        except Exception:
            pass

# ─── UTILS ───────────────────────────────────────────────────────────
async def schedule_delete(interaction: discord.Interaction, delay: int = 15):
    """Schedules the deletion of an interaction response after a delay."""
    await asyncio.sleep(delay)
    try:
        await interaction.delete_original_response()
    except Exception:
        pass

async def schedule_delete_fup(fup: discord.WebhookMessage, delay: int = 15):
    """Schedules the deletion of a followup message after a delay."""
    await asyncio.sleep(delay)
    try:
        await fup.delete()
    except Exception:
        pass


async def _log_critical_error(guild: discord.Guild, context: str, error: Exception):
    """Centralized error logger for critical backend failures."""
    print(f"CRITICAL ERROR [{context}]: {error}")
    try:
        # Move import here to avoid circular dependencies if any, though traceback is standard
        import traceback
        err_str = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        err_str = err_str[-1500:] # Keep last 1500 chars to fit in embed
        
        log_channel = guild.get_channel(1493660854640709643) # SERVER_LOG_CHANNEL_ID
        if log_channel:
            embed = discord.Embed(
                title="🚨 CRITICAL SYSTEM ERROR",
                description=f"**Context:** `{context}`\n**Error:** `{type(error).__name__}`\n```python\n{err_str}\n```",
                color=discord.Color.red(),
                timestamp=discord.utils.utcnow()
            )
            embed.set_footer(text="Auto-Generated System Alert")
            await log_channel.send(embed=embed)
    except Exception as e:
        print(f"Failed to log error to Discord: {e}")

async def _safe_gspread_call(guild: discord.Guild, context: str, func, *args, **kwargs):
    """Executes a gspread call in the thread pool with exponential backoff on API rate limits."""
    loop = asyncio.get_event_loop()
    max_retries = 5
    for attempt in range(max_retries):
        try:
            return await loop.run_in_executor(DB_POOL, functools.partial(func, *args, **kwargs))
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429: # Too Many Requests
                wait_time = 2 ** attempt
                print(f"⚠️ Google Sheets API Rate Limit (429). Retrying in {wait_time}s... (Attempt {attempt+1}/{max_retries})")
                await asyncio.sleep(wait_time)
                continue
            else:
                # Other API errors
                await _log_critical_error(guild, f"{context} - GSpread API Error", e)
                return False
        except Exception as e:
            await _log_critical_error(guild, f"{context} - Unknown GSpread Error", e)
            return False
    
    # If we exhausted retries
    err_msg = Exception("Exhausted all retries for Google Sheets API due to Rate Limiting (429)")
    await _log_critical_error(guild, f"{context} - Exhausted Retries", err_msg)
    return False

# ─── CONFIGURATION (All IDs in One Place) ────────────────────────────────────

# ROLES
SUPER_ADMIN_IDS         = [0]                   # To Reset Bot Discord user id (Dangenerous to use)
BOT_ADMIN_ROLE_ID       = 1480606697205530835   # Full admin access
MOD_ROLE_ID             = 1487798552863309894   # Moderator — clear, ban, slotlist, id_panel, delete_lobby
LOBBY_VIEWER_ROLE_ID    = 1487798460202876938   # Can view all bot-created lobby channels
BAN_ROLE_ID             = 1482377564982083654   # Role given to banned players
TOLI_ROLE_ID            = 1482803978239611022   # Only this role can register teams
TAGGER_ROLE_NAME        = 'Tagger'              # Temporary role for tagging in confirm channel

# CHANNELS
REGISTRATION_CHANNEL_ID = 1480546201328943114   # Channel where registration panel is posted
CONFIRM_CHANNEL_ID      = 1480599940425187519   # Channel where players tag their teammates
ASSIGNMENT_CHANNEL_ID   = 1480792846318698537   # Channel where lobby assignment notifications go
ID_PANEL_CHANNEL_ID     = 1484234510408220815   # Channel where ID/Password panel is posted
AUDIT_LOG_CHANNEL_ID    = 1493521467613380698   # Channel ID for audit logs
SERVER_LOG_CHANNEL_ID   = 1493660854640709643   # Channel ID for channel/role logs
ADMIN_DASHBOARD_CHANNEL_ID = 1495385242863931452 # Dedicated channel for Admin Control Center
BAN_LOG_CHANNEL_ID      = 1496879892271202404     # Banned team notifications

# ANTI-SPAM / RATE LIMIT PREVENTION
MASS_ACTION_IN_PROGRESS = False

# CATEGORY & LOBBIES
LOBBY_CATEGORY_NAME     = 'SCRIMS QUALIFIER IDP' # Category name for lobby channels
LOBBY_SIZE              = 20                     # Maximum teams allowed per lobby

# GOOGLE SHEETS
PRIMARY_SPREADSHEET_ID   = os.getenv('PRIMARY_SPREADSHEET_ID')
SECONDARY_SPREADSHEET_ID = os.getenv('SECONDARY_SPREADSHEET_ID')

# ─── RUNTIME STATE ───────────────────────────────────────────────────────────
# Tracks teams waiting for player tag confirmation: {owner_user_id: {"team_name": str, "session_id": str}}
awaiting_tag = {}
# Tracks registration start times for 7-minute deadline: {user_id: start_timestamp}
active_registrations = {}
# Concurrency locks for per-lobby slotlist updates
SLOTLIST_LOCKS = {}
SLOTLIST_LOCKS_LOCK = asyncio.Lock()
# Global lock for waitlist filling to prevent "Double Snatching"
WAITLIST_LOCK = asyncio.Lock()
is_mirroring = False # Lock for sheet sync

# --- SMART SYNC: DIRTY FLAGS ---
# Per-table flags. The 15s drainer pushes only flagged tables.
# Start clean — Sheets are only touched when an actual DB write occurs.
SYNC_DIRTY = {'teams': False, 'logs': False, 'bans': False, 'deleted': False, 'config': False}
SYNC_DIRTY_LOCK = threading.Lock()
_drain_running = False  # Re-entrancy guard for the drainer

def _mark_dirty(*tables):
    """Flags one or more tables for the next sheet sync drain."""
    with SYNC_DIRTY_LOCK:
        for t in tables:
            if t in SYNC_DIRTY:
                SYNC_DIRTY[t] = True

def _consume_dirty():
    """Atomically reads and clears all dirty flags. Returns the snapshot."""
    with SYNC_DIRTY_LOCK:
        snap = {k: v for k, v in SYNC_DIRTY.items()}
        for k in SYNC_DIRTY:
            SYNC_DIRTY[k] = False
        return snap


# Tracks user interaction cooldowns (Rate limiting)
button_cooldowns = {}

def check_cooldown(user_id: int, cooldown_seconds: int = 4) -> bool:
    """Returns True if action is allowed, False if on cooldown."""
    now = time.time()
    uid = str(user_id)
    if uid in button_cooldowns:
        if now - button_cooldowns[uid] < cooldown_seconds:
            return False
    button_cooldowns[uid] = now
    
    # Cleanup to prevent memory leak
    if len(button_cooldowns) > 500:
        expired = [k for k, v in button_cooldowns.items() if now - v > 10]
        for k in expired:
            del button_cooldowns[k]
            
    return True

# ─── GOOGLE SHEETS SETUP ─────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

creds_path = os.path.join(BASE_DIR, os.getenv('CREDENTIALS_PATH', 'credentials.json'))
creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
gc = gspread.authorize(creds)

TEAMS_HEADERS = [
    'Timestamp', 'Discord ID', 'Team Name', 'Owner Name', 'Owner Email', 'Contact',
    'Player 1', 'Player 2', 'Player 3', 'Player 4', 'Sub 1', 'Sub 2', 'IG Handles', 'Lobby', 
    'Tagged Names', 'Tagged IDs', 'P1 Tag', 'P2 Tag', 'P3 Tag', 'P4 Tag'
]
PENDING_HEADERS = ['Discord ID', 'Team Name', 'Owner Name', 'Owner Email', 'Contact', 'P1', 'P2', 'P3', 'P4']
CONFIG_HEADERS = ['Key', 'Value']
LOG_HEADERS = ['Discord ID', 'Team Name', 'P1 IGN', 'P2 IGN', 'P3 IGN', 'P4 IGN', 'P1 Tag', 'P2 Tag', 'P3 Tag', 'P4 Tag', 'Lobby (Assigned)', 'Lobby (Protection)']
SCHEDULE_HEADERS = ['Lobby ID', 'M1 IDP', 'M1 Start', 'M2 IDP', 'M2 Start']
LOBBY_SCHEDULES_HEADERS = ['Lobby Number', 'Match Date', 'M1 IDP', 'M1 Start', 'M2 IDP', 'M2 Start', 'Control Panel Msg ID']
BANS_HEADERS = ['User ID', 'Unban Timestamp']
DELETED_TEAMS_HEADERS = [
    'Discord ID', 'Team Name', 'Owner Name', 'Owner Email', 'Contact',
    'Player 1', 'Player 2', 'Player 3', 'Player 4', 'Sub 1', 'Sub 2', 'IG Handles', 'Lobby',
    'Tagged IDs', 'P1 Tag', 'P2 Tag', 'P3 Tag', 'P4 Tag', 'Deleted At'
]

def _get_col_letter(col_num):
    """Converts a 1-based column number to an Excel-style column letter (e.g., 1 -> A, 27 -> AA)."""
    string = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        string = chr(65 + remainder) + string
    return string

def _get_lobby_num(name: str):
    """Extracts the integer lobby number from role/channel names like 'Lobby-12'."""
    try:
        if '-' in name:
            return int(name.split('-')[-1])
        return 999
    except Exception:
        return 999  # Fallback to high number if partition fails

def _clean_for_sheets(text):
    """Prevents spreadsheet injection by prepending an apostrophe to formula triggers."""
    if not text: return ""
    text = str(text).strip()
    if text.startswith(('=', '+', '-', '@')):
        return f"'{text}"
    return text

def _get_or_create_worksheet(ss_id, name, headers):
    try:
        ss = gc.open_by_key(ss_id)
        try:
            ws = ss.worksheet(name)
            # If headers changed or columns added, sync them
            if ws.col_count < len(headers):
                ws.add_cols(len(headers) - ws.col_count)
            
            # Ensure headers are updated in the sheet
            first_row = ws.row_values(1)
            if first_row != headers:
                # Force rename/sync row 1 to ensure headers match the code exactly
                last_col = _get_col_letter(len(headers))
                ws.update(range_name=f'A1:{last_col}1', values=[headers])
                print(f"✅ Sync'd all headers for {name}")
                
        except gspread.exceptions.WorksheetNotFound:
            ws = ss.add_worksheet(title=name, rows=1000, cols=max(len(headers), 20))
            ws.append_row(headers)
            print(f"Created new worksheet: {name}")
        return ws
    except Exception as e:
        print(f"Error in _get_or_create_worksheet for {name}: {e}")
        return None

# These will be initialized in MyBot.setup_hook
teams_ws = None
pending_ws = None
log_ws = None
config_ws = None
schedule_ws = None
lobby_scheds_ws = None
bans_ws = None
deleted_teams_ws = None

def initialize_sheets():
    """Initializes all worksheets in one go."""
    global teams_ws, pending_ws, log_ws, config_ws, schedule_ws, lobby_scheds_ws, bans_ws, deleted_teams_ws
    print("🔄 Initializing Google Worksheets...")
    teams_ws = _get_or_create_worksheet(PRIMARY_SPREADSHEET_ID, 'Teams', TEAMS_HEADERS)
    pending_ws = _get_or_create_worksheet(PRIMARY_SPREADSHEET_ID, 'Pending', PENDING_HEADERS)
    log_ws = _get_or_create_worksheet(PRIMARY_SPREADSHEET_ID, 'Registration Log', LOG_HEADERS)
    deleted_teams_ws = _get_or_create_worksheet(PRIMARY_SPREADSHEET_ID, 'Deleted Teams', DELETED_TEAMS_HEADERS)
    config_ws = _get_or_create_worksheet(SECONDARY_SPREADSHEET_ID, 'Config', CONFIG_HEADERS)
    schedule_ws = _get_or_create_worksheet(SECONDARY_SPREADSHEET_ID, 'Schedule', SCHEDULE_HEADERS)
    lobby_scheds_ws = _get_or_create_worksheet(SECONDARY_SPREADSHEET_ID, 'LobbySchedules', LOBBY_SCHEDULES_HEADERS)
    bans_ws = _get_or_create_worksheet(SECONDARY_SPREADSHEET_ID, 'Bans', BANS_HEADERS)
    print("✅ All Worksheets ready.")

# --- CACHE SETUP ---
import time
CACHE_TTL = 30
TEAM_CACHE = {'last_fetched': 0, 'data': []}
LOG_CACHE = {'last_fetched': 0, 'data': []}
CONFIG_CACHE = {'last_fetched': 0, 'data': []}
SCHEDULE_CACHE = {'last_fetched': 0, 'data': []}
BANS_CACHE = {'last_fetched': 0, 'data': []}
LOBBY_SCHED_CACHE = {'last_fetched': 0, 'data': []}

# --- CONFIG MEMORY CACHE ---
CONFIG_MEM_CACHE = {} 
CONFIG_CACHE_TTL = 30 # 30 seconds

# ─── CONFIG HELPER (SQLITE) ──────────────────────────────────────────────────
@db_retry()
def _get_config(key, default=None):
    try:
        # Check cache first
        now = time.time()
        if key in CONFIG_MEM_CACHE:
            val, expires = CONFIG_MEM_CACHE[key]
            if now < expires:
                return val

        with DB_LOCK:
            with _get_db_conn() as conn:
                row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
                if row:
                    val = row['value']
                    CONFIG_MEM_CACHE[key] = (val, now + CONFIG_CACHE_TTL)
                    return val
            return default
    except Exception as e:
        print(f"Error in _get_config ({key}): {e}")
        return default

@db_retry()
def _set_config(key, value):
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))
                conn.commit()
                # Update cache
                CONFIG_MEM_CACHE[key] = (str(value), time.time() + CONFIG_CACHE_TTL)
        _mark_dirty('config')
        return True
    except Exception as e:
        print(f"Error in _set_config ({key}): {e}")
        return False

# ─── SCHEDULE HELPERS (SQLITE) ────────────────────────────────────────────────
@db_retry()
def _get_all_schedules():
    """Fetches all schedules from the template table."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                rows = conn.execute("SELECT * FROM schedule_templates").fetchall()
                return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error in _get_all_schedules: {e}")
        return []

@db_retry()
def _set_schedule(lobby_index, m1_idp, m1_start, m2_idp, m2_start):
    """Updates a specific schedule template."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO schedule_templates 
                    (lobby_index, m1_idp, m1_start, m2_idp, m2_start)
                    VALUES (?, ?, ?, ?, ?)
                """, (str(lobby_index), m1_idp, m1_start, m2_idp, m2_start))
                conn.commit()
        return True
    except Exception as e:
        print(f"Error in _set_schedule: {e}")
        return False

# Re-defining based on the actual SQL tables created:
@db_retry()
def _get_all_lobby_schedules():
    """Fetches all specific lobby schedules."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                rows = conn.execute("SELECT * FROM lobby_schedules").fetchall()
                return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error in _get_all_lobby_schedules: {e}")
        return []

@db_retry()
def _set_lobby_schedule(lobby_num, match_date, m1_idp, m1_start, m2_idp, m2_start, msg_id=""):
    """Updates or creates a specific lobby schedule."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                # Check if exists to preserve msg_id if new msg_id is empty
                if not msg_id:
                    old = conn.execute("SELECT control_panel_msg_id FROM lobby_schedules WHERE lobby_number = ?", (lobby_num,)).fetchone()
                    if old:
                        msg_id = old['control_panel_msg_id']
                
                conn.execute("""
                    INSERT OR REPLACE INTO lobby_schedules 
                    (lobby_number, match_date, m1_idp, m1_start, m2_idp, m2_start, control_panel_msg_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (lobby_num, match_date, m1_idp, m1_start, m2_idp, m2_start, str(msg_id)))
                conn.commit()
        return True
    except Exception as e:
        print(f"Error in _set_lobby_schedule: {e}")
        return False

@db_retry()
def _get_lobby_schedule(lobby_num):
    """Fetches a specific lobby schedule."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                row = conn.execute("SELECT * FROM lobby_schedules WHERE lobby_number = ?", (lobby_num,)).fetchone()
                if row:
                    return dict(row)
    except Exception as e:
        print(f"Error in _get_lobby_schedule: {e}")
    return None



@db_retry()
def _seed_schedules():
    """Seeds the SQLite schedule template table if it's empty."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                # Check if template table is empty
                count = conn.execute("SELECT COUNT(*) FROM schedule_templates").fetchone()[0]
                if count == 0:
                    defaults = [
                        (str(i), '12:50 PM', '1:00 PM', '1:30 PM', '1:38 PM')
                        for i in range(1, 11)
                    ]
                    conn.executemany("""
                        INSERT INTO schedule_templates (lobby_index, m1_idp, m1_start, m2_idp, m2_start)
                        VALUES (?, ?, ?, ?, ?)
                    """, defaults)
                    conn.commit()
                    print("✅ Seeded SQLite Schedule Template.")
    except Exception as e:
        print(f"Error seeding schedules in SQLite: {e}")

# ─── SHEET HELPERS ────────────────────────────────────────────────────────────
def _get_slotlist_embed(lobby_name, lobby_teams):
    """Generates a premium, sleek slotlist Embed."""
    embed = discord.Embed(
        title=f"  OFFICIAL ROSTER: {lobby_name.upper()}  ",
        description="**Welcome to the SQ**\nBelow are the confirmed teams locking their slots for this clash.\n",
        color=0xFFD700  # Premium Gold Color
    )
    
    slots_text = "🔒 **Slot-01** ▬▬ `[ Reserved ]`\n🔒 **Slot-02** ▬▬ `[ Reserved ]`\n"
    
    for i, t in enumerate(lobby_teams, 3):
        t_name = t.get('team_name') or t.get('Team Name', 'Unknown')
        c_id = t.get('creator_id') or t.get('Discord ID', 'Unknown')
        slot_num = f"{i:02d}"
        slots_text += f"🎮 **Slot-{slot_num}** ▬▬ **{t_name}** (<@{c_id}>)\n"
        
    embed.add_field(name="📋 Team Assignments", value=slots_text, inline=False)
    embed.set_footer(text="Automated Tournament System • Live Synchronized Slots")
    embed.timestamp = discord.utils.utcnow()
    
    return embed

async def _get_lobby_lock(lobby_name):
    async with SLOTLIST_LOCKS_LOCK:
        if lobby_name not in SLOTLIST_LOCKS:
            SLOTLIST_LOCKS[lobby_name] = asyncio.Lock()
        return SLOTLIST_LOCKS[lobby_name]

async def _update_lobby_slotlist(guild, lobby_name):
    """Refreshes the slotlist message in the corresponding lobby channel."""
    # 🔒 CONCURRENCY PROTECTION: Prevent multiple tasks from editing the same slotlist at once
    lobby_lock = await _get_lobby_lock(lobby_name)
    async with lobby_lock:
        # Find channel (Lobby-1 -> lobby-1)
        ch_name = lobby_name.lower().replace('lobby-', 'lobby-')
        channel = discord.utils.get(guild.text_channels, name=ch_name)
        if not channel:
            return
            
        loop = asyncio.get_event_loop()
        # 🟢 OPTIMIZED: Targeted query instead of fetching all teams
        lobby_teams = await loop.run_in_executor(None, _get_teams_in_lobby, lobby_name)
    
    # Check if the lobby is already locked (previously published)
    locked_str = str(await loop.run_in_executor(None, _get_config, 'locked_lobbies', ''))
    locked_list = [l.strip() for l in locked_str.split(',') if l.strip()]
    if lobby_name not in locked_list:
        return # Not published yet, don't auto-post
        
    embed = _get_slotlist_embed(lobby_name, lobby_teams)
    
    # Try to find the last Slotlist message and edit it, or send a fresh one
    async for message in channel.history(limit=200):
        found = False
        if message.author.bot:
            if message.embeds and message.embeds[0].title and "OFFICIAL ROSTER" in message.embeds[0].title:
                found = True
            elif "🔥 **Slotlist" in message.content:
                found = True
                
        if found:
            try:
                # Clear content and set embed
                await message.edit(content=None, embed=embed)
            except Exception:
                # If we cannot edit (e.g. sent by old webhook), delete and resend
                try:
                    await message.delete()
                except Exception:
                    pass
                await channel.send(embed=embed)
            return
            
    # Fallback: send as new message
    await channel.send(embed=embed)

async def _fill_lobby_from_waitlist(guild: discord.Guild, lobby_name: str):
    """
    Checks for vacancies in a specific lobby and pulls the next team(s) from the waitlist.
    """
    # 🔒 GLOBAL WAITLIST LOCK: Prevent two lobbies from pulling the same team at the same time
    async with WAITLIST_LOCK:
        # 1. Verification: Don't fill published (locked) lobbies automatically
        loop = asyncio.get_event_loop()
        locked_str = str(await loop.run_in_executor(None, _get_config, 'locked_lobbies', ''))
        locked_list = [l.strip() for l in locked_str.split(',') if l.strip()]
        if lobby_name in locked_list:
            return

    # 2. Check Vacancies in the database (Source of Truth)
    lobby_teams = await loop.run_in_executor(None, _get_teams_in_lobby, lobby_name)
    current_occupancy = len(lobby_teams)
    vacancies = LOBBY_SIZE - current_occupancy
    
    lobby_role = discord.utils.get(guild.roles, name=lobby_name)
    if not lobby_role:
        return

    
    if vacancies <= 0:
        return

    # 3. Fetch Waitlisted Teams (Ordered by FCFS)
    # 🟢 OPTIMIZED: Targeted query for waitlist
    waiting_teams = await loop.run_in_executor(None, _get_teams_in_lobby, 'WAITING')
    
    if not waiting_teams:
        return

    assigned_count = 0
    for team in waiting_teams[:vacancies]:
        creator_id = str(team.get('creator_id', ''))
        t_name = team.get('team_name', 'Unknown')
        
        if not creator_id or not creator_id.isdigit():
            continue
            
        try:
            member = await guild.fetch_member(int(creator_id))
            if member:
                # A. Apply Role
                try:
                    await member.add_roles(lobby_role, reason=f'Waitlist Auto-Assign (Slot opened in {lobby_name})')
                except discord.Forbidden:
                    print(f"❌ Role Hierarchy Error: Bot cannot assign {lobby_role.name}. Move Bot Role ABOVE it.")
                    continue
                except Exception as e:
                    print(f"❌ Failed to assign role to {member.display_name}: {e}")
                    continue
                
                # B. Update Database
                await loop.run_in_executor(None, _update_team_lobby, creator_id, lobby_name)
                # C. Sync Registration Log (Security Lock)
                await loop.run_in_executor(None, _update_log_lobby, creator_id, lobby_name)
                
                assigned_count += 1
                
                # D. Notify Assignment Channel
                assign_ch = guild.get_channel(ASSIGNMENT_CHANNEL_ID)
                if assign_ch:
                    current_count = await loop.run_in_executor(None, _get_config, 'total_assignments', '0')
                    new_count = int(current_count or 0) + 1
                    await loop.run_in_executor(None, _set_config, 'total_assignments', new_count)
                    
                    raw_tags = str(team.get('tagged_ids', '')).split(',')
                    player_mentions = [f'<@{tid.strip()}>' for tid in raw_tags if tid.strip()]
                    
                    embed = discord.Embed(
                        title=f"{new_count}. Team {t_name}",
                        description=f"✅ **Waitlist Auto-Assign** ➡️ {lobby_name}",
                        color=discord.Color.green()
                    )
                    embed.add_field(name="Owner", value=member.mention, inline=False)
                    embed.add_field(name="Tagged Players", value=' '.join(player_mentions), inline=False)
                    embed.set_footer(text="A slot opened up and the next team was automatically moved in.")
                    
                    try: await assign_ch.send(embed=embed)
                    except: pass

                # E. Notify Confirm Channel
                confirm_ch = guild.get_channel(CONFIRM_CHANNEL_ID)
                if confirm_ch:
                    try: await confirm_ch.send(f"🎰 **Waitlist Update**: Team **{t_name}** (<@{creator_id}>) has been assigned to **{lobby_name}**!", delete_after=30)
                    except: pass
                    
                # F. Notify in the Lobby itself
                lobby_ch_name = lobby_name.lower()
                lobby_ch = discord.utils.get(guild.text_channels, name=lobby_ch_name)
                if lobby_ch:
                    try: await lobby_ch.send(f"👋 **Welcome {member.mention}!** Your team **{t_name}** has been moved from the waitlist to fill a vacancy.")
                    except: pass

                # 🟢 RATE LIMIT PROTECTION: Sleep to prevent API freezes when filling 20 teams at once
                await asyncio.sleep(1.5)

        except discord.NotFound:
            # 🛑 DEAD TEAM: Owner left server. Purge from DB to unblock queue.
            print(f"🗑️ Purging dead waitlist team: {t_name} (Owner {creator_id} left server)")
            await loop.run_in_executor(None, _soft_delete_team, creator_id)
            continue
        except Exception as e:
            print(f"Error auto-filling from waitlist for {creator_id}: {e}")
            continue

    if assigned_count > 0:
        # Final Step: Sync the slotlist
        await _update_lobby_slotlist(guild, lobby_name)

def _get_col_letter(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

# ─── TEAM & REGISTRATION LOG HELPERS (SQLITE) ─────────────────────────────────
@db_retry()
def _get_team(user_id: str):
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                row = conn.execute("SELECT * FROM teams WHERE discord_id = ?", (str(user_id),)).fetchone()
                if not row: return None
                data = dict(row)
                return {
                    'created_at': data.get('created_at', ''),
                    'creator_id': data['discord_id'],
                    'team_name': data['team_name'],
                    'owner_name': data['owner_name'],
                    'owner_email': data['owner_email'],
                    'contact_number': data['contact'],
                    'player1': data['player1'],
                    'player2': data['player2'],
                    'player3': data['player3'],
                    'player4': data['player4'],
                    'sub1': data['sub1'],
                    'sub2': data['sub2'],
                    'insta_handles': data['ig_handles'],
                    'lobby': data['lobby'],
                    'tagged_names': data.get('tagged_names', ''),
                    'tagged_ids': data['tagged_ids'],
                    'p1_tag': data['p1_tag'],
                    'p2_tag': data['p2_tag'],
                    'p3_tag': data['p3_tag'],
                    'p4_tag': data['p4_tag'],
                }
    except Exception as e:
        print(f"Error in _get_team: {e}")
        return None

@db_retry()
def _get_teams_in_lobby(lobby_name: str):
    """Targeted query to fetch teams for a specific lobby. Indexed lookup."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                rows = conn.execute(
                    "SELECT * FROM teams WHERE lobby = ? ORDER BY created_at ASC", 
                    (lobby_name,)
                ).fetchall()
                standardized = []
                for row in rows:
                    data = dict(row)
                    standardized.append({
                        'created_at': data.get('created_at', ''),
                        'creator_id': data['discord_id'],
                        'team_name': data['team_name'],
                        'owner_name': data['owner_name'],
                        'owner_email': data['owner_email'],
                        'contact_number': data['contact'],
                        'player1': data['player1'],
                        'player2': data['player2'],
                        'player3': data['player3'],
                        'player4': data['player4'],
                        'sub1': data.get('sub1', ''),
                        'sub2': data.get('sub2', ''),
                        'insta_handles': data.get('ig_handles', ''),
                        'lobby': data['lobby'],
                        'tagged_names': data.get('tagged_names', ''),
                        'tagged_ids': data['tagged_ids'],
                        'p1_tag': data['p1_tag'],
                        'p2_tag': data['p2_tag'],
                        'p3_tag': data['p3_tag'],
                        'p4_tag': data['p4_tag'],
                    })
                return standardized
    except Exception as e:
        print(f"Error in _get_teams_in_lobby: {e}")
        return []

@db_retry()
def _all_teams_sqlite():
    """Helper for mirroring and internal use. Enforces FCFS order."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                rows = conn.execute("SELECT * FROM teams ORDER BY created_at ASC").fetchall()
                return [dict(r) for r in rows]
    except: return []

@db_retry()
def _save_team(data: dict):
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                # Check if team already exists (to preserve original timestamp)
                existing = conn.execute("SELECT created_at FROM teams WHERE discord_id = ?", (str(data['creator_id']),)).fetchone()
                if existing and existing.get('created_at') and str(existing['created_at']).replace('.','').isdigit():
                    created_at = existing['created_at']
                else:
                    # 🟢 Unix Timestamp (float as string) for high-precision math and FCFS waitlist
                    created_at = str(time.time())


                
                conn.execute("""
                    INSERT OR REPLACE INTO teams (
                        discord_id, team_name, owner_name, owner_email, contact, 
                        player1, player2, player3, player4, sub1, sub2, 
                        ig_handles, lobby, tagged_names, tagged_ids, p1_tag, p2_tag, p3_tag, p4_tag, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(data['creator_id']), data.get('team_name', ''), data.get('owner_name', ''),
                    data.get('owner_email', ''), data.get('contact_number', ''),
                    data.get('player1', ''), data.get('player2', ''), data.get('player3', ''), data.get('player4', ''),
                    data.get('sub1', ''), data.get('sub2', ''), data.get('insta_handles', ''),
                    data.get('lobby', ''), data.get('tagged_names', ''), data.get('tagged_ids', ''), 
                    data.get('p1_tag', ''), data.get('p2_tag', ''), data.get('p3_tag', ''), data.get('p4_tag', ''),
                    created_at
                ))
                conn.commit()
        _mark_dirty('teams')
        print(f"✅ Saved team: {data['team_name']} to SQLITE")
    except Exception as e:
        print(f"❌ Error in _save_team: {e}")

@db_retry()
def _log_registration(team_data: dict, tagged_ids: list, player_tags: list, lobby_name: str):
    """
    Logs a new registration.
    Stores raw IDs in p*_tag columns for better searchability.
    """
    try:
        def extract_ign(p_str):
            if not p_str or ',' not in p_str: return p_str or 'Unknown'
            return p_str.split(',')[-1].strip()

        # 🛡️ SECURITY: Force standard raw ID format (strip <@! > characters)
        import re
        clean_tags = []
        for tid in tagged_ids:
            clean_id = re.sub(r"\D", "", str(tid))
            if clean_id: clean_tags.append(clean_id)
        
        tags = clean_tags + [""] * (4 - len(clean_tags))

        
        with DB_LOCK:
            with _get_db_conn() as conn:
                conn.execute("""
                    INSERT INTO registration_log (
                        discord_id, team_name, p1_ign, p2_ign, p3_ign, p4_ign, 
                        p1_tag, p2_tag, p3_tag, p4_tag, lobby_assigned, lobby_protection
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(team_data.get('creator_id')), team_data.get('team_name'),
                    extract_ign(team_data.get('player1')), extract_ign(team_data.get('player2')), 
                    extract_ign(team_data.get('player3')), extract_ign(team_data.get('player4')),
                    tags[0], tags[1], tags[2], tags[3],
                    lobby_name, lobby_name
                ))
                conn.commit()
        _mark_dirty('logs')
        print(f"✅ Logged registration for {team_data.get('team_name')} to SQLite (ID-Locked)")
    except Exception as e:
        print(f"Error in _log_registration: {e}")

@db_retry()
def _update_log_lobby(user_id: str, lobby_name: str):
    """
    Updates the active lock for a registration.
    User said: 'par lobby assignement ko mat haath lagna'.
    So we only update lobby_protection.
    """
    try:
        with DB_LOCK:
            # Update the LATEST log entry for this user
            with _get_db_conn() as conn:
                row = conn.execute("SELECT rowid FROM registration_log WHERE discord_id = ? ORDER BY rowid DESC LIMIT 1", (str(user_id),)).fetchone()
                if row:
                    conn.execute("UPDATE registration_log SET lobby_protection = ? WHERE rowid = ?", (lobby_name, row['rowid']))
                    conn.commit()
                    _mark_dirty('logs')
                    return True
    except Exception as e:
        print(f"Error in _update_log_lobby: {e}")
    return False

@db_retry()
def _clear_log_protection_by_player(player_id: str):
    """Deletes registration_log entries involving this player to release tag protection."""
    try:
        raw_id = str(player_id)
        mention_1 = f"<@{player_id}>"
        mention_2 = f"<@!{player_id}>"
        with DB_LOCK:
            with _get_db_conn() as conn:
                sql = """
                    SELECT rowid, discord_id, team_name, lobby_protection
                    FROM registration_log
                    WHERE (
                        discord_id = ? OR
                        p1_tag IN (?, ?, ?) OR 
                        p2_tag IN (?, ?, ?) OR 
                        p3_tag IN (?, ?, ?) OR 
                        p4_tag IN (?, ?, ?)
                    )
                    AND (lobby_protection IS NOT NULL AND lobby_protection != '')
                """
                p = [raw_id] + [raw_id, mention_1, mention_2] * 4
                rows = conn.execute(sql, p).fetchall()
                results = []
                for row in rows:
                    results.append({
                        'owner_id': row['discord_id'],
                        'team_name': row['team_name'],
                        'lobby': row['lobby_protection']
                    })
                    conn.execute("DELETE FROM registration_log WHERE rowid = ?", (row['rowid'],))
                if results:
                    conn.commit()
                    _mark_dirty('logs')
                    print(f"🗑️ Deleted {len(results)} log entry(s) for player {player_id}")
                return results
    except Exception as e:
        print(f"Error in _clear_log_protection_by_player: {e}")
    return []

@db_retry()
def _is_player_locked(player_id: str):
    """Read-only check: Returns (team_name, lobby) if player has active protection, else None."""
    try:
        import re
        raw_id = re.sub(r"\D", "", str(player_id))
        if not raw_id: return None
        
        with DB_LOCK:
            with _get_db_conn() as conn:
                sql = """
                    SELECT team_name, lobby_protection
                    FROM registration_log
                    WHERE (
                        discord_id = ? OR
                        p1_tag = ? OR 
                        p2_tag = ? OR 
                        p3_tag = ? OR 
                        p4_tag = ?
                    )
                    AND (lobby_protection IS NOT NULL AND lobby_protection != '')
                    ORDER BY rowid DESC LIMIT 1
                """
                row = conn.execute(sql, [raw_id] * 5).fetchone()
                if row:
                    return row['team_name'], row['lobby_protection']
    except Exception as e:
        print(f"Error in _is_player_locked: {e}")
    return None

@db_retry()
def _clear_lobby_assignments(lobby_name: str):
    """Clears all team assignments for a specific lobby (used for manual cleanup)."""
    with DB_LOCK:
        with _get_db_conn() as conn:
            conn.execute("UPDATE teams SET lobby = '' WHERE lobby = ?", (lobby_name,))
            conn.commit()
            _mark_dirty('teams')


@db_retry()
def _delete_log_by_lobby(lobby_name: str):
    """Deletes all registration_log entries for a specific lobby."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                result = conn.execute("DELETE FROM registration_log WHERE lobby_protection = ?", (lobby_name,))
                conn.commit()
                _mark_dirty('logs')
                print(f"🗑️ Deleted {result.rowcount} log entries for {lobby_name}")
    except Exception as e:
        print(f"Error in _delete_log_by_lobby: {e}")

@db_retry()
def _delete_all_logs():
    """Deletes ALL registration_log entries (full reset)."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                conn.execute("DELETE FROM registration_log")
                conn.commit()
                _mark_dirty('logs')
                print("🗑️ All registration_log entries deleted.")
    except Exception as e:
        print(f"Error in _delete_all_logs: {e}")

@db_retry()
def _reset_all_locks_internal():
    """
    CLEAN SLATE: Clears all active lobby assignments and security locks globally.
    Used for ending a tournament phase or refreshing the board.
    """
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                # 1. Unassign all teams from their current lobbies
                conn.execute("UPDATE teams SET lobby = ''")
                # 2. Delete all entries from registration_log (temporary protection only)
                conn.execute("DELETE FROM registration_log")
                conn.commit()
                _mark_dirty('teams', 'logs')
                print("💣 DATABASE RESET: All lobby filters and protection locks CLEARED.")
                return True
    except Exception as e:
        print(f"Error in _reset_all_locks_internal: {e}")
    return False

@db_retry()
def _check_tag_conflict(player_ids: list):
    """
    Checks if any player ID in the list is already locked in another registration.
    Targeted strictly at raw IDs for maximum security and speed.
    """
    try:
        import re
        with DB_LOCK:
            with _get_db_conn() as conn:
                for pid in player_ids:
                    if not pid or str(pid) == '0': continue
                    
                    # 🛡️ CLEAN: Get raw ID (strip mentions)
                    raw_id = re.sub(r"\D", "", str(pid))
                    if not raw_id: continue
                    
                    # Search all 4 tag columns AND the discord_id column
                    sql = """
                        SELECT team_name, lobby_protection 
                        FROM registration_log 
                        WHERE (
                            p1_tag = ? OR 
                            p2_tag = ? OR 
                            p3_tag = ? OR 
                            p4_tag = ? OR
                            discord_id = ?
                        )
                        AND (lobby_protection IS NOT NULL AND lobby_protection != '')
                        ORDER BY rowid DESC LIMIT 1
                    """
                    res = conn.execute(sql, [raw_id] * 5).fetchone()
                    if res:
                        return raw_id, res['team_name'], res['lobby_protection']
    except Exception as e:
        print(f"Error in _check_tag_conflict: {e}")
    return None


@db_retry()
def _update_team_lobby(user_id: str, lobby_name: str, tagged_ids: str = None, tags: list = None, tagged_names: str = None):
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                params = [lobby_name]
                sql = "UPDATE teams SET lobby = ?"
                if tagged_names is not None:
                    sql += ", tagged_names = ?"
                    params.append(tagged_names)
                if tagged_ids is not None:
                    sql += ", tagged_ids = ?"
                    params.append(tagged_ids)
                if tags and len(tags) >= 4:
                    sql += ", p1_tag = ?, p2_tag = ?, p3_tag = ?, p4_tag = ?"
                    params.extend([str(t) for t in tags[:4]])
                sql += " WHERE discord_id = ?"
                params.append(str(user_id))
                conn.execute(sql, params)
                conn.commit()
        _mark_dirty('teams')
        return True
    except Exception as e:
        print(f"Error in _update_team_lobby: {e}")
        return False

@db_retry()
def _get_all_logs():
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                rows = conn.execute("SELECT * FROM registration_log").fetchall()
                return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error in _get_all_logs: {e}")
        return []

@db_retry()
def _soft_delete_team(user_id: str):
    """Moves the team to deleted_teams archive and hard-deletes from teams."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                # 1. Fetch the team data
                row = conn.execute("SELECT * FROM teams WHERE discord_id = ?", (str(user_id),)).fetchone()
                if not row:
                    return False
                d = dict(row)
                # 2. Insert into deleted_teams archive with timestamp
                now_str = time.strftime('%Y-%m-%d %H:%M:%S')
                conn.execute("""
                    INSERT INTO deleted_teams (
                        discord_id, team_name, owner_name, owner_email, contact,
                        player1, player2, player3, player4, sub1, sub2,
                        ig_handles, lobby, tagged_ids, p1_tag, p2_tag, p3_tag, p4_tag, deleted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    d['discord_id'], d['team_name'], d['owner_name'], d['owner_email'], d['contact'],
                    d['player1'], d['player2'], d['player3'], d['player4'],
                    d['sub1'], d['sub2'], d['ig_handles'], d['lobby'],
                    d['tagged_ids'], d['p1_tag'], d['p2_tag'], d['p3_tag'], d['p4_tag'], now_str
                ))
                # 3. Hard delete from teams
                conn.execute("DELETE FROM teams WHERE discord_id = ?", (str(user_id),))
                conn.commit()
                _mark_dirty('teams', 'deleted')
                print(f"🗑️ Team '{d['team_name']}' archived to deleted_teams and removed from teams.")
            return True
    except Exception as e:
        print(f"Error in _soft_delete_team: {e}")
    return False

@db_retry()
def _get_all_deleted_teams():
    """Fetches all archived deleted teams (for mirroring only)."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                rows = conn.execute("SELECT * FROM deleted_teams").fetchall()
                return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error in _get_all_deleted_teams: {e}")
        return []

@db_retry()
def _get_all_teams():
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                # FCFS: Sort by creation time to ensure waitlist is fair
                rows = conn.execute("SELECT * FROM teams ORDER BY created_at ASC").fetchall()
                standardized = []
                for row in rows:
                    data = dict(row)
                    standardized.append({
                        'created_at': data.get('created_at', ''),
                        'creator_id': data['discord_id'],
                        'team_name': data['team_name'],
                        'owner_name': data['owner_name'],
                        'owner_email': data['owner_email'],
                        'contact_number': data['contact'],
                        'player1': data['player1'],
                        'player2': data['player2'],
                        'player3': data['player3'],
                        'player4': data['player4'],
                        'sub1': data['sub1'],
                        'sub2': data['sub2'],
                        'insta_handles': data['ig_handles'],
                        'lobby': data['lobby'],
                        'tagged_names': data.get('tagged_names', ''),
                        'tagged_ids': data['tagged_ids'],
                        'p1_tag': data['p1_tag'],
                        'p2_tag': data['p2_tag'],
                        'p3_tag': data['p3_tag'],
                        'p4_tag': data['p4_tag'],
                    })
                return standardized
    except Exception as e:
        print(f"Error in _get_all_teams: {e}")
        return []

        
# ─── BAN HELPERS (SQLITE) ─────────────────────────────────────────────────────
@db_retry()
def _get_all_bans():
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                rows = conn.execute("SELECT * FROM bans").fetchall()
                return [dict(r) for r in rows]
    except Exception as e:
        print(f"Error in _get_all_bans: {e}")
        return []

@db_retry()
def _is_banned(user_id: str):
    """Checks the database to see if a user has an active ban, preventing evasion."""
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                row = conn.execute("SELECT unban_timestamp FROM bans WHERE user_id = ?", (str(user_id),)).fetchone()
                if row:
                    if time.time() > row['unban_timestamp']:
                        return False # Ban expired
                    return True # Still banned
    except Exception as e:
        print(f"Error in _is_banned: {e}")
    return False


@db_retry()
def _add_ban(user_id, unban_timestamp, reason='', banned_by='', team_name=''):
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO bans (user_id, unban_timestamp, reason, banned_by, team_name) VALUES (?, ?, ?, ?, ?)",
                    (str(user_id), float(unban_timestamp), str(reason), str(banned_by), str(team_name))
                )
                conn.commit()
        _mark_dirty('bans')
    except Exception as e:
        pass

@db_retry()
def _remove_ban(user_id):
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                conn.execute("DELETE FROM bans WHERE user_id = ?", (str(user_id),))
                conn.commit()
                _mark_dirty('bans')
                return True
    except Exception as e:
        pass
    return False

def _find_team_for_user(user_id: str):
    """Searches teams and deleted_teams for a user (as owner or tagged player). Returns team_name or 'Unregistered'."""
    try:
        uid = str(user_id)
        mention_1 = f"<@{uid}>"
        mention_2 = f"<@!{uid}>"
        with DB_LOCK:
            with _get_db_conn() as conn:
                # 1. Check as owner in active teams
                row = conn.execute("SELECT team_name FROM teams WHERE discord_id = ?", (uid,)).fetchone()
                if row: return row['team_name']
                
                # 2. Check as tagged player in active teams
                row = conn.execute(
                    "SELECT team_name FROM teams WHERE tagged_ids LIKE ? OR tagged_ids LIKE ? OR tagged_ids LIKE ?",
                    (f"%{uid}%", f"%{mention_1}%", f"%{mention_2}%")
                ).fetchone()
                if row: return row['team_name']
                
                # 3. Check in deleted teams
                row = conn.execute("SELECT team_name FROM deleted_teams WHERE discord_id = ?", (uid,)).fetchone()
                if row: return row['team_name']
                
                # 4. Check as tagged player in deleted teams
                row = conn.execute(
                    "SELECT team_name FROM deleted_teams WHERE tagged_ids LIKE ? OR tagged_ids LIKE ? OR tagged_ids LIKE ?",
                    (f"%{uid}%", f"%{mention_1}%", f"%{mention_2}%")
                ).fetchone()
                if row: return row['team_name']
    except Exception:
        pass
    return 'Unregistered Player'

def parse_duration(duration_str):
    regex = r"(\d+)([smhd])"
    matches = re.findall(regex, duration_str.lower())
    if not matches:
        return None
    total_seconds = 0
    for val, unit in matches:
        val = int(val)
        if unit == 's': total_seconds += val
        elif unit == 'm': total_seconds += val * 60
        elif unit == 'h': total_seconds += val * 3600
        elif unit == 'd': total_seconds += val * 86400
    return total_seconds

@db_retry()
def _get_pending(user_id: str):
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                row = conn.execute("SELECT * FROM pending_teams WHERE discord_id = ?", (str(user_id),)).fetchone()
                if not row: return None
                data = dict(row)
                return {
                    'creator_id': data['discord_id'],
                    'team_name': data['team_name'],
                    'owner_name': data['owner_name'],
                    'owner_email': data['owner_email'],
                    'contact_number': data['contact'],
                    'player1': data['p1'],
                    'player2': data['p2'],
                    'player3': data['p3'],
                    'player4': data['p4'],
                }
    except Exception as e:
        print(f"Error in _get_pending: {e}")
        return None

@db_retry()
def _save_pending(data: dict):
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO pending_teams 
                    (discord_id, team_name, owner_name, owner_email, contact, p1, p2, p3, p4)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(data['creator_id']), data.get('team_name', ''), data.get('owner_name', ''), 
                    data.get('owner_email', ''), data.get('contact_number', ''), 
                    data.get('player1', ''), data.get('player2', ''), data.get('player3', ''), data.get('player4', '')
                ))
                conn.commit()
    except Exception as e:
        print(f"Error in _save_pending: {e}")

@db_retry()
def _delete_pending(user_id: str):
    try:
        with DB_LOCK:
            with _get_db_conn() as conn:
                conn.execute("DELETE FROM pending_teams WHERE discord_id = ?", (str(user_id),))
                conn.commit()
    except Exception as e:
        print(f"Error in _delete_pending: {e}")



# ─── ADMIN CHECK ──────────────────────────────────────────────────────────────
def is_bot_admin():
    """Access for both Admin and Moderator roles."""
    async def predicate(ctx):
        if discord.utils.get(ctx.author.roles, id=BOT_ADMIN_ROLE_ID):
            return True
        if discord.utils.get(ctx.author.roles, id=MOD_ROLE_ID):
            return True
        raise commands.MissingPermissions(['Tournament Staff Access Only'])
    return commands.check(predicate)

def is_admin_only():
    """Strictly restricted to the Admin role ONLY."""
    async def predicate(ctx):
        if discord.utils.get(ctx.author.roles, id=BOT_ADMIN_ROLE_ID):
            return True
        raise commands.MissingPermissions(['Admin Access Only'])
    return commands.check(predicate)

def is_super_admin():
    """Nuclear power: Only the specific User IDs in SUPER_ADMIN_IDS list."""
    async def predicate(ctx):
        if ctx.author.id in SUPER_ADMIN_IDS:
            return True
        raise commands.MissingPermissions(['Super Admin (Owner) Access Only'])
    return commands.check(predicate)

def _has_staff_role(member):
    """Helper: Returns True if the member has Admin or Mod role."""
    return (
        discord.utils.get(member.roles, id=BOT_ADMIN_ROLE_ID) or
        discord.utils.get(member.roles, id=MOD_ROLE_ID)
    )

# ─── STEP 3: SUBSTITUTES & SOCIALS ───────────────────────────────────────────
class Step3View(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Proceed to Step 3', style=discord.ButtonStyle.success, custom_id='step3_btn')
    async def step3_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(Step3Modal())

class Step3Modal(discord.ui.Modal, title='Step 3/3: Substitutes & Socials'):
    sub1 = discord.ui.TextInput(label='Substitute 1 (UID, IGN) [Optional]', placeholder="UID, IGN or 'no'", required=False, max_length=100)
    sub2 = discord.ui.TextInput(label='Substitute 2 (UID, IGN) [Optional]', placeholder="UID, IGN or 'no'", required=False, max_length=100)
    insta_follow = discord.ui.TextInput(label='Followed @virelApp on Instagram?', placeholder='Yes / No', required=True, max_length=10)
    insta_handles = discord.ui.TextInput(label='Players Instagram Handles', style=discord.TextStyle.paragraph, placeholder='e.g., @player1_ig, @player2_ig', required=True, max_length=300)

    async def on_submit(self, interaction: discord.Interaction):
        # 🟢 Refactored for 15s Auto-Delete
        await interaction.response.defer(ephemeral=True)
        user_id = str(interaction.user.id)
        loop = asyncio.get_event_loop()

        pending = await loop.run_in_executor(None, _get_pending, user_id)
        
        # 🕒 CHECK REGISTRATION DEADLINE (7-min limit)
        start_time = active_registrations.get(user_id)
        if start_time and (time.time() - start_time > 420): # 7 minutes
            active_registrations.pop(user_id, None)
            await loop.run_in_executor(None, _delete_pending, user_id)
            fup = await interaction.followup.send("❌ **Session Expired!** You must complete all registration steps within 7 minutes. Please start again.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        if not pending:
            fup = await interaction.followup.send('Session expired. Start over.', ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        import re
        clean_regex = r"^[a-zA-Z0-9\s.,_\-@#/]+$"
        
        # VALIDATE ON RAW INPUT FIRST
        for i, s_val in enumerate([self.sub1.value, self.sub2.value], 1):
            s_clean = s_val.strip()
            if s_clean.lower() != 'no' and s_clean != "" and not re.match(clean_regex, s_clean):
                fup = await interaction.followup.send(f"❌ **Invalid Input in Substitute {i}!** Special symbols like {s_val} are restricted. (Allowed: _ - . @ # /)", ephemeral=True)
                asyncio.create_task(schedule_delete_fup(fup))
                return
        
        if not re.match(clean_regex, self.insta_handles.value):
            fup = await interaction.followup.send(f"❌ **Invalid Input in Socials!** Use only standard characters. (Allowed: @ _ - .)", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        sub1_raw = _clean_for_sheets(self.sub1.value)
        sub2_raw = _clean_for_sheets(self.sub2.value)
        ig_raw = _clean_for_sheets(self.insta_handles.value)

        def parse_sub(val):
            v = val.strip().lower()
            return '' if v == 'no' or not v else val.strip()

        team_name = pending.get('team_name', 'Unknown')
        data = {
            'creator_id': user_id,
            'team_name': team_name,
            'owner_name': pending.get('owner_name', ''),
            'owner_email': pending.get('owner_email', ''),
            'contact_number': pending.get('contact_number', ''),
            'player1': pending.get('player1', ''),
            'player2': pending.get('player2', ''),
            'player3': pending.get('player3', ''),
            'player4': pending.get('player4', ''),
            'sub1': parse_sub(sub1_raw),
            'sub2': parse_sub(sub2_raw),
            'insta_handles': ig_raw,
            'lobby': ''
        }

        await loop.run_in_executor(None, _save_team, data)
        await loop.run_in_executor(None, _delete_pending, user_id)
        active_registrations.pop(user_id, None) # Clear registration timer

        # 🟢 GENERATE SESSION TOKEN FOR GHOST DELETION PROTECTION
        session_id = str(time.time())
        awaiting_tag[user_id] = {"team_name": team_name, "session_id": session_id}

        # Send confirmation prompt
        guild = interaction.guild
        confirm_ch = guild.get_channel(CONFIRM_CHANNEL_ID) if guild else None
        if confirm_ch:
            try:
                tagger_role = await _get_tagger_role(guild)
                if tagger_role:
                    await interaction.user.add_roles(tagger_role, reason="Registration Complete - Awaiting Tags")
                    # 🟢 PASS SESSION_ID
                    asyncio.create_task(_schedule_tagger_expiry(interaction.user, user_id, session_id))
                
                await confirm_ch.send(
                    f'**{team_name}** registration complete.\n\n'
                    f'{interaction.user.mention}, tag your **4 players** here to confirm.\n'
                    f'All tagged members must be in this server.',
                    delete_after=30
                )
            except Exception as e:
                print(f"Error sending confirm message: {e}")

        fup = await interaction.followup.send(
            f'✅ Team **{team_name}** registered.\n\n'
            f'Navigate to <#{CONFIRM_CHANNEL_ID}> and mention your roster to finalize.',
            ephemeral=True
        )
        asyncio.create_task(schedule_delete_fup(fup))

# ─── STEP 2: MAIN ROSTER ───────────────────────────────────────────────────
class Step2View(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Proceed to Step 2', style=discord.ButtonStyle.success, custom_id='step2_btn')
    async def step2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(Step2Modal())

class Step2Modal(discord.ui.Modal, title='Step 2/3: Main Roster'):
    player1 = discord.ui.TextInput(label='Player 1 (UID, IGN) [Captain]', placeholder='e.g., 12345678, VAxSATTU', required=True, max_length=100)
    player2 = discord.ui.TextInput(label='Player 2 (UID, IGN)', placeholder='e.g., 87654321, VAxFLICKS', required=True, max_length=100)
    player3 = discord.ui.TextInput(label='Player 3 (UID, IGN)', placeholder='e.g., 11223344, VAxADARSH', required=True, max_length=100)
    player4 = discord.ui.TextInput(label='Player 4 (UID, IGN)', placeholder='e.g., 99887766, VAxDARE', required=True, max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
        # 🟢 Refactored for 15s Auto-Delete
        await interaction.response.defer(ephemeral=True)
        user_id = str(interaction.user.id)
        loop = asyncio.get_event_loop()
        
        pending = await loop.run_in_executor(None, _get_pending, user_id)
        if not pending:
            fup = await interaction.followup.send('Session expired. Start over.', ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        import re
        clean_regex = r"^[a-zA-Z0-9\s.,_\-@#/]+$"

        # VALIDATE ON RAW INPUT FIRST
        for i, p_val in enumerate([self.player1.value, self.player2.value, self.player3.value, self.player4.value], 1):
            if not re.match(clean_regex, p_val):
                fup = await interaction.followup.send(f"❌ **Invalid Input in Player {i}!** Special characters are restricted. (Allowed: _ - . @ # /)", ephemeral=True)
                asyncio.create_task(schedule_delete_fup(fup))
                return
        
        # NOW CLEAN FOR SHEETS
        p1 = _clean_for_sheets(self.player1.value)
        p2 = _clean_for_sheets(self.player2.value)
        p3 = _clean_for_sheets(self.player3.value)
        p4 = _clean_for_sheets(self.player4.value)

        pending['player1'] = p1
        pending['player2'] = p2
        pending['player3'] = p3
        pending['player4'] = p4
        pending['creator_id'] = user_id

        await loop.run_in_executor(None, _save_pending, pending)
        fup = await interaction.followup.send('Step 2 complete! Click below to enter Substitute and Instagram details (Step 3/3).', view=Step3View(), ephemeral=True)
        asyncio.create_task(schedule_delete_fup(fup))

# ─── STEP 1: BASIC DETAILS ────────────────────────────────────────────────────
class Step1Modal(discord.ui.Modal, title='Step 1/3: Basic Team Details'):
    def __init__(self, delete_old_id=None):
        super().__init__()
        self.delete_old_id = delete_old_id

    team_name = discord.ui.TextInput(label='Team Name', placeholder='Enter your team name', required=True, max_length=100)
    owner_name = discord.ui.TextInput(label='Owner Name', placeholder='Enter owner name', required=True, max_length=100)
    owner_email = discord.ui.TextInput(label='Owner Email', placeholder='Enter owner email', required=True, max_length=100)
    contact_number = discord.ui.TextInput(label='Contact Number', placeholder='Enter contact number', required=True, max_length=20)

    async def on_submit(self, interaction: discord.Interaction):
        # 🟢 Refactored for 15s Auto-Delete
        await interaction.response.defer(ephemeral=True)
        user_id = str(interaction.user.id)
        loop = asyncio.get_event_loop()
        
        # 🕒 TRACK REGISTRATION START TIME (7-min limit)
        if user_id not in active_registrations:
            active_registrations[user_id] = time.time()

        # 🛑 SECURITY: If this is a 'Create New' over an old profile, delete old now.
        if self.delete_old_id:
            await loop.run_in_executor(None, _soft_delete_team, self.delete_old_id)
            print(f"♻️ Overwriting old profile for {self.delete_old_id} during Step 1.")


        email = _clean_for_sheets(self.owner_email.value)
        contact = _clean_for_sheets(self.contact_number.value)

        # Email Validation
        import re
        raw_email = self.owner_email.value
        if not re.match(r"[^@]+@[^@]+\.[^@]+", raw_email):
            fup = await interaction.followup.send("❌ Invalid Format: Please provide a standard email address.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        # Check for other fields
        t_name_raw = self.team_name.value
        o_name_raw = self.owner_name.value

        # Team Name Validation
        if not re.match(r"^[a-zA-Z0-9\s]+$", t_name_raw):
            fup = await interaction.followup.send("❌ **Invalid Team Name!** Emojis and special characters are not allowed. (Only letters, numbers, and spaces).", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        # Owner Name Validation
        if not re.match(r"^[a-zA-Z\s.]+$", o_name_raw):
            fup = await interaction.followup.send("❌ **Invalid Owner Name!** Use only letters and dots.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        # NOW CLEAN FOR SHEETS
        email = _clean_for_sheets(raw_email)
        contact = _clean_for_sheets(self.contact_number.value)
        t_name = _clean_for_sheets(t_name_raw)
        o_name = _clean_for_sheets(o_name_raw)

        # Contact Number Validation
        raw_contact = self.contact_number.value.strip()
        if not (raw_contact.isdigit() and 10 <= len(raw_contact) <= 15):
            fup = await interaction.followup.send("❌ Invalid Format: Contact number must contain 10-15 numerical digits.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        data = {
            'creator_id': user_id,
            'team_name': t_name,
            'owner_name': o_name,
            'owner_email': email,
            'contact_number': contact,
            'player1': '', 'player2': '', 'player3': '', 'player4': '',
        }
        await loop.run_in_executor(None, _save_pending, data)
        fup = await interaction.followup.send('✅ Step 1 Verified: Proceed to Phase 2 (Main Roster).', view=Step2View(), ephemeral=True)
        asyncio.create_task(schedule_delete_fup(fup))

# ─── MAIN REGISTRATION PANEL ──────────────────────────────────────────────────

class OldOrNewTeamView(discord.ui.View):
    def __init__(self, team_data: dict):
        super().__init__(timeout=None)
        self.team_data = team_data

    @discord.ui.button(label='Use Old Team', style=discord.ButtonStyle.blurple, custom_id='use_old_team_btn')
    async def use_old_team(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 🟢 Hardened Deferral
        try:
            await interaction.response.defer(ephemeral=True)
        except:
            pass
        user_id = str(interaction.user.id)
        active_registrations.pop(user_id, None) # Clear registration timer if any
        team_name = self.team_data.get('team_name', 'Unknown')
        
        # 🟢 GENERATE SESSION TOKEN FOR GHOST DELETION PROTECTION
        session_id = str(time.time())
        awaiting_tag[user_id] = {"team_name": team_name, "session_id": session_id}

        guild = interaction.guild
        confirm_ch = guild.get_channel(CONFIRM_CHANNEL_ID) if guild else None
        if confirm_ch:
            try:
                # DYNAMIC UNLOCK: Give the user the Tagger role
                tagger_role = await _get_tagger_role(guild)
                if tagger_role:
                    await interaction.user.add_roles(tagger_role, reason="Existing team selected - Awaiting Tags")
                    # 🟢 PASS SESSION_ID
                    asyncio.create_task(_schedule_tagger_expiry(interaction.user, user_id, session_id))
                
                await confirm_ch.send(
                    f'**{team_name}** is ready to confirm.\n\n'
                    f'{interaction.user.mention}, tag your **4 players** here.\n'
                    f'All tagged members must be in this server.',
                    delete_after=30
                )
            except Exception as e:
                print(f"Error sending confirm message: {e}")

        fup = await interaction.followup.send(
            f'✅ Profile **{team_name}** selected.\n\n'
            f'Navigate to <#{CONFIRM_CHANNEL_ID}> and mention your roster to finalize.',
            ephemeral=True
        )
        asyncio.create_task(schedule_delete_fup(fup))

    @discord.ui.button(label='Create New Team', style=discord.ButtonStyle.success, custom_id='create_new_team_btn')
    async def create_new_team(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 🟢 Not deferred because it opens a Modal
        if not check_cooldown(interaction.user.id):
            await interaction.response.send_message("⏳ Action on cooldown. Please try again shortly.", ephemeral=True)
            return

        # Optimization: Send modal immediately. 
        # Deletion happens in Step1Modal.on_submit to avoid 404 Interaction timeouts.
        user_id = str(interaction.user.id)
        await interaction.response.send_modal(Step1Modal(delete_old_id=user_id))

class MainTeamView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Register / Create Team', style=discord.ButtonStyle.success, custom_id='register_team_btn')
    async def register_team(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 🟢 Hardened Deferral (Prevents 404 Unknown Interaction)
        try:
            await interaction.response.defer(ephemeral=True)
        except:
            pass

        if not check_cooldown(interaction.user.id):
            fup = await interaction.followup.send("⏳ Action on cooldown. Please try again shortly.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        # Restriction: Only allow specific role
        if not discord.utils.get(interaction.user.roles, id=TOLI_ROLE_ID):
            fup = await interaction.followup.send(f"🚫 Only members with the <@&{TOLI_ROLE_ID}> role can register.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        # Restriction: Ban Check
        user_id = str(interaction.user.id)
        loop = asyncio.get_event_loop()
        is_banned_db = await loop.run_in_executor(None, _is_banned, user_id)
        
        if discord.utils.get(interaction.user.roles, id=BAN_ROLE_ID) or is_banned_db:
            fup = await interaction.followup.send("❌ Access Denied: You are banned from the Scrims", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        # Restriction: Active lobby protection check
        lock_info = await loop.run_in_executor(None, _is_player_locked, user_id)
        if lock_info:
            team_name, lobby = lock_info
            embed = discord.Embed(
                title="⚠️ REGISTRATION BLOCKED",
                description="You are already locked in a team registration as a player.",
                color=discord.Color.red()
            )
            embed.add_field(name="Currently Playing In", value=f"**{team_name}**", inline=False)
            embed.add_field(name="Assigned Lobby", value=f"**{lobby or 'Waitlist'}**", inline=False)
            embed.set_footer(text="Please clear your current registration or leave that team to register again.")
            
            fup = await interaction.followup.send(embed=embed, ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return

        existing = await loop.run_in_executor(None, _get_team, user_id)
        if existing:
            # Block registration if already assigned to a lobby
            assigned_lobby = existing.get('lobby')
            if assigned_lobby:
                guild = interaction.guild
                member = interaction.user
                if guild and not isinstance(member, discord.Member):
                    member = guild.get_member(member.id)
                
                has_role = False
                if isinstance(member, discord.Member):
                    has_role = any(r.name == assigned_lobby for r in member.roles)
                
                if has_role:
                    embed = discord.Embed(
                        title="⚠️ REGISTRATION BLOCKED",
                        description="You are already associated with an active registration.",
                        color=discord.Color.red()
                    )
                    embed.add_field(name="Associated Team", value=f"**{existing.get('team_name', 'Unknown')}**", inline=False)
                    embed.add_field(name="Assigned Lobby", value=f"**{assigned_lobby}**", inline=False)
                    embed.set_footer(text="Please request an Admin to clear your registration if you wish to re-register.")
                    
                    fup = await interaction.followup.send(embed=embed, ephemeral=True)
                    asyncio.create_task(schedule_delete_fup(fup))
                    return
                else:
                    await loop.run_in_executor(None, _update_team_lobby, user_id, "")
                    existing['lobby'] = ""

            embed = discord.Embed(
                title="Existing Team Found",
                description=f"You are already the owner of team: **{existing.get('team_name', 'Unknown')}**",
                color=discord.Color.blue()
            )
            embed.add_field(name="Owner", value=existing.get('owner_name', 'N/A'), inline=True)
            embed.add_field(name="Email", value=existing.get('owner_email', 'N/A'), inline=True)
            embed.add_field(name="Contact", value=existing.get('contact_number', 'N/A'), inline=True)
            
            roster = (
                f"1: {existing.get('player1', 'N/A')}\n"
                f"2: {existing.get('player2', 'N/A')}\n"
                f"3: {existing.get('player3', 'N/A')}\n"
                f"4: {existing.get('player4', 'N/A')}"
             )
            embed.add_field(name="Main Roster", value=roster, inline=False)
            
            fup = await interaction.followup.send(
                "An active profile exists. Please select your next action:",
                embed=embed,
                view=OldOrNewTeamView(existing),
                ephemeral=True
            )
            asyncio.create_task(schedule_delete_fup(fup))
        else:
            fup = await interaction.followup.send(
                'Select below to initiate the registration sequence.',
                view=StartRegistrationView(),
                ephemeral=True
            )
            asyncio.create_task(schedule_delete_fup(fup))

    @discord.ui.button(label='Delete My Team', style=discord.ButtonStyle.danger, custom_id='delete_team_btn')
    async def delete_team(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 🟢 Hardened Deferral
        try:
            await interaction.response.defer(ephemeral=True)
        except:
            pass

        if not check_cooldown(interaction.user.id):
            fup = await interaction.followup.send("⏳ Action on cooldown. Please try again shortly.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return
            
        user_id = str(interaction.user.id)
        loop = asyncio.get_event_loop()

        # Block deletion if player is locked
        lock_info = await loop.run_in_executor(None, _is_player_locked, user_id)
        if lock_info:
            team_name, lobby = lock_info
            fup = await interaction.followup.send(
                f"🔒 Registration Locked: Actively assigned to **{team_name}**. Please request clearance first.",
                ephemeral=True
            )
            asyncio.create_task(schedule_delete_fup(fup))
            return

        existing = await loop.run_in_executor(None, _get_team, user_id)
        if existing:
            await loop.run_in_executor(None, _soft_delete_team, user_id)
            fup = await interaction.followup.send(
                f"✅ Registration for **{existing['team_name']}** successfully purged.", ephemeral=True
            )
            asyncio.create_task(schedule_delete_fup(fup))
        else:
            fup = await interaction.followup.send("❌ No active registration profile found.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))

class StartRegistrationView(discord.ui.View):
    """Bridge view to open Step1Modal after deferring MainTeamView."""
    def __init__(self):
        super().__init__(timeout=60)

    @discord.ui.button(label='Start Registration', style=discord.ButtonStyle.success, custom_id='start_reg_btn')
    async def start_registration(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.response.is_done():
            return
        await interaction.response.send_modal(Step1Modal())

# ─── SETUP WIZARD ─────────────────────────────────────────────────────────────
class SetupLobbyModal(discord.ui.Modal, title='Create Scrims Lobbies'):
    lobby_count = discord.ui.TextInput(
        label='Number of Lobbies to Create',
        placeholder='e.g., 5',
        required=True,
        max_length=3
    )

    async def on_submit(self, interaction: discord.Interaction):
        # 🟢 DEFER IMMEDIATELY
        await interaction.response.defer(ephemeral=True)
        
        global MASS_ACTION_IN_PROGRESS
        MASS_ACTION_IN_PROGRESS = True
        
        try:
            try:
                count = int(self.lobby_count.value)
                if count <= 0 or count > 50:
                    raise ValueError()
            except ValueError:
                await interaction.followup.send("❌ Error: Please enter a number between 1 and 50.", ephemeral=True)
                return
                
            guild = interaction.guild
            
            # 🟢 ROLE LIMIT PROTECTION
            if len(guild.roles) + count > 240:
                await interaction.followup.send(f"❌ **Role Limit Approaching!** Discord only allows 250 roles. You currently have {len(guild.roles)} roles. Please use `!delete_all_lobbies` to clear old lobbies before creating new ones.", ephemeral=True)
                return

            category_name = LOBBY_CATEGORY_NAME
            admin_role = guild.get_role(BOT_ADMIN_ROLE_ID)

            # Freshly fetch category to avoid "Category does not exist"
            category = discord.utils.get(guild.categories, name=category_name)
            if not category:
                overwrites = {guild.default_role: discord.PermissionOverwrite(read_messages=False)}
                if admin_role:
                    overwrites[admin_role] = discord.PermissionOverwrite(read_messages=True, manage_channels=True, manage_messages=True)
                category = await guild.create_category(category_name, overwrites=overwrites)

            # Fetch setup data
            loop = asyncio.get_event_loop()
            last_lobby = await loop.run_in_executor(None, _get_config, 'last_lobby_number', '0')
            tourney_date = await loop.run_in_executor(None, _get_config, 'tournament_date', 'To be announced')
            schedule_templates = await loop.run_in_executor(None, _get_all_schedules)
            
            try:
                highest = int(last_lobby) if last_lobby is not None else 0
            except ValueError:
                highest = 0

            created_roles = []
            created = 0
            start = highest + 1
            for i in range(start, start + count):
                ch_name = f'lobby-{i}'
                role_name = f'Lobby-{i}'

                lobby_role = discord.utils.get(guild.roles, name=role_name)
                if not lobby_role:
                    try:
                        lobby_role = await guild.create_role(name=role_name, reason='Tournament Setup')
                    except Exception:
                        pass

                overwrites = {guild.default_role: discord.PermissionOverwrite(read_messages=False)}
                if lobby_role:
                    created_roles.append(lobby_role)
                    overwrites[lobby_role] = discord.PermissionOverwrite(
                        read_messages=True, 
                        send_messages=False, 
                        attach_files=False, 
                        add_reactions=False,
                        create_public_threads=False,
                        create_private_threads=False
                    )

                # Viewer role: read-only access to all lobbies
                viewer_role = discord.utils.get(guild.roles, id=LOBBY_VIEWER_ROLE_ID)
                if viewer_role:
                    overwrites[viewer_role] = discord.PermissionOverwrite(read_messages=True, send_messages=False)
                # Mod role: view + send messages in lobbies
                mod_role = discord.utils.get(guild.roles, id=MOD_ROLE_ID)
                if mod_role:
                    overwrites[mod_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
                if admin_role:
                    overwrites[admin_role] = discord.PermissionOverwrite(read_messages=True, manage_channels=True, manage_messages=True)

                    if not discord.utils.get(category.channels, name=ch_name):
                        new_channel = await guild.create_text_channel(ch_name, category=category, overwrites=overwrites)
                        created += 1
                        
                        # SAFETY: Small break to prevent Discord API Rate Limiting (429)
                        await asyncio.sleep(0.8)
                        
                        # Use divmod to avoid '%' character in traceback (prevents TypeError in shard logging)
                        _, template_idx = divmod(i - 1, 10)
                        sched = schedule_templates[template_idx] if len(schedule_templates) > template_idx else {}
                        
                        # Use LOWERCASE SQLITE KEYS (Fix TBA)
                        m1_idp = sched.get('m1_idp', 'TBD')
                        m1_start = sched.get('m1_start', 'TBD')
                        m2_idp = sched.get('m2_idp', 'TBD')
                        m2_start = sched.get('m2_start', 'TBD')

                        # Create attractive Control Panel embed
                        lobby_title = role_name.upper()
                        desc = (
                            f"# 📅 Schedule for {lobby_title}\n\n"
                            f"**🗓Match Date**\n"
                            f"{tourney_date}\n\n"
                            f"**Match 1**\n"
                            f"🕛IDP/PW: {m1_idp} | Start: {m1_start}\n"
                            f"🗺Map: Erangel\n\n"
                            f"**Match 2**\n"
                            f"🕛IDP/PW: {m2_idp} | Start: {m2_start}\n"
                            f"🗺Map: Miramar\n\n"
                            f"*To change your schedule, simply click the **🎮 Manage Matches** button.*"
                        )
                        embed = discord.Embed(
                            title=f"LOBBY SCHEDULE",
                            description=desc,
                            color=discord.Color.gold()
                        )
                        embed.set_footer(text="Scrims Management System • VirelApp")

                        cp_msg = await new_channel.send(embed=embed, view=LobbyControlView())
                        
                        # Store SPECIFIC lobby timing in LobbySchedules sheet
                        await loop.run_in_executor(None, _set_lobby_schedule, 
                            i, tourney_date, m1_idp, m1_start, m2_idp, m2_start, cp_msg.id
                        )


            # Update persistent config
            await loop.run_in_executor(None, _set_config, 'last_lobby_number', start + created - 1)

            # Distribute waitlisted teams across all available vacancies
            all_lobby_roles = [r for r in guild.roles if r.name.startswith('Lobby-')]
            # Sort to fill earlier lobbies first
            all_lobby_roles.sort(key=lambda r: _get_lobby_num(r.name))
            
            assigned_msg = []
            for role in all_lobby_roles:
                # Check for existing logic or vacancies
                before_count = len(role.members)
                if before_count < LOBBY_SIZE:
                    # Use our new helper to fill this specific lobby
                    # Note: Helper also calls _update_lobby_slotlist
                    await _fill_lobby_from_waitlist(guild, role.name)
                    
                    # Check how many were added (roughly, for the summary message)
                    after_count = len(role.members)
                    if after_count > before_count:
                        assigned_msg.append(f"**Lobbies refilled from waitlist.**")

            msg = f'✅ Created {created} lobbies'
            if created > 0:
                msg += f' (Lobby-{start} to Lobby-{start + created - 1}).'
                # 🟢 UPDATE TRACKING: Save the new highest lobby number
                await loop.run_in_executor(None, _set_config, 'last_lobby_number', start + created - 1)
                print(f"📊 Tracking updated: last_lobby_number is now {start + created - 1}")
            else:
                msg += '.'
            
            fup = await interaction.followup.send(msg, ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            
            # Audit Log: Setup Lobbies
            if AUDIT_LOG_CHANNEL_ID != 0 and created > 0:
                audit_ch = guild.get_channel(AUDIT_LOG_CHANNEL_ID)
                if audit_ch:
                    embed = discord.Embed(
                        title="📝 Action Audit Log",
                        color=discord.Color.light_grey(),
                        timestamp=discord.utils.utcnow()
                    )
                    embed.add_field(name="User", value=f"{interaction.user.mention} (`{interaction.user.id}`)", inline=False)
                    embed.add_field(name="Action", value=f"Created {created} lobbies (Lobby-{start} to Lobby-{start + created - 1})", inline=False)
                    try:
                        await audit_ch.send(embed=embed)
                    except Exception:
                        pass

            # Announce waitlist if applicable
            if assigned_msg:
                confirm_ch = guild.get_channel(CONFIRM_CHANNEL_ID)
                if confirm_ch:
                    announcement = "Waitlist teams assigned:\n\n"
                    announcement += "\n".join(assigned_msg)
                    try:
                        await confirm_ch.send(announcement, delete_after=30)
                    except Exception:
                        pass
        finally:
            MASS_ACTION_IN_PROGRESS = False

class SetupRoleView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

    @discord.ui.button(label='⚙️ Create Lobbies', style=discord.ButtonStyle.blurple, custom_id='open_lobby_modal_btn')
    async def open_lobby_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.message.edit(content='Opening...', view=None)
        await interaction.response.send_modal(SetupLobbyModal())

    @discord.ui.button(label='📅 Set Date', style=discord.ButtonStyle.secondary, custom_id='set_tournament_date_btn')
    async def set_date(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SetDateModal())

    @discord.ui.button(label='🕒 Edit Schedule', style=discord.ButtonStyle.secondary, custom_id='edit_schedule_btn')
    async def edit_schedule_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        loop = asyncio.get_event_loop()
        schedules = await loop.run_in_executor(None, _get_all_schedules)
        count = len(schedules) if schedules else 10
        fup = await interaction.followup.send(f"Select a schedule template (1-{count}) to edit, or add a new one:", view=ScheduleIndexSelectView(count), ephemeral=True)
        asyncio.create_task(schedule_delete_fup(fup, delay=60))

    @discord.ui.button(label='🎯 Edit Specific Lobby', style=discord.ButtonStyle.secondary, custom_id='edit_specific_lobby_btn')
    async def edit_specific_lobby(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(EditSpecificLobbyModal())

class EditSpecificLobbyModal(discord.ui.Modal, title='Edit Specific Lobby Schedule'):
    lobby_num = discord.ui.TextInput(label='Lobby Number', placeholder='e.g., 12', required=True)
    m1_idp = discord.ui.TextInput(label='Match 1 IDP Time', placeholder='e.g., 12:50 PM', required=True)
    m1_start = discord.ui.TextInput(label='Match 1 Start Time', placeholder='e.g., 1:00 PM', required=True)
    m2_idp = discord.ui.TextInput(label='Match 2 IDP Time', placeholder='e.g., 1:30 PM', required=True)
    m2_start = discord.ui.TextInput(label='Match 2 Start Time', placeholder='e.g., 1:38 PM', required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        num = self.lobby_num.value.strip()
        loop = asyncio.get_event_loop()
        
        # 1. Update Sheet
        existing = await loop.run_in_executor(None, _get_lobby_schedule, num)
        match_date = existing.get('Match Date', 'TBD') if existing else 'TBD'
        msg_id = existing.get('Control Panel Msg ID', '') if existing else ''
        
        success = await loop.run_in_executor(None, _set_lobby_schedule, 
            num, match_date, self.m1_idp.value.strip(), self.m1_start.value.strip(), 
            self.m2_idp.value.strip(), self.m2_start.value.strip(), msg_id
        )
        
        if success:
            fup = await interaction.followup.send(f"✅ Schedule for **Lobby {num}** synchronized successfully.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            
            # 2. Try to update the embed in the channel
            guild = interaction.guild
            ch_name = f'lobby-{num}'
            channel = discord.utils.get(guild.text_channels, name=ch_name)
            if channel and msg_id:
                try:
                    msg = await channel.fetch_message(int(msg_id))
                    if msg:
                        # Rebuild refined embed
                        lobby_title = f"LOBBY-{num}"
                        desc = (
                            f"# 📅 Schedule for {lobby_title}\n\n"
                            
                            f"**🗓Match Date**\n"
                            f"{match_date}\n\n"
                            f"**Match 1**\n"
                            f"🕛IDP/PW: {self.m1_idp.value} | Start: {self.m1_start.value}\n"
                            f"🗺Map: Erangel\n\n"
                            f"**Match 2**\n"
                            f"🕛IDP/PW: {self.m2_idp.value} | Start: {self.m2_start.value}\n"
                            f"🗺Map: Miramar\n\n"
                            f"*To change your schedule, simply click the **🎮 Manage Matches** button.*"
                        )
                        embed = discord.Embed(
                            title=f"Lobby CONTROL PANEL",
                            description=desc,
                            color=discord.Color.gold()
                        )
                        embed.set_footer(text="Scrims Management System • VirelApp ")
                        
                        await msg.edit(embed=embed)
                        fup = await interaction.followup.send(f"Control Panel in {channel.mention} updated!", ephemeral=True)
                        asyncio.create_task(schedule_delete_fup(fup, delay=15))
                except Exception as e:
                    print(f"Error updating CP msg: {e}")
                    fup = await interaction.followup.send(f"⚠️ Record synchronized. (Local channel display update failed in {channel.mention}).", ephemeral=True)
                    asyncio.create_task(schedule_delete_fup(fup, delay=15))
        else:
            fup = await interaction.followup.send("❌ Synchronization Error: Transaction failed.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))

class SetDateModal(discord.ui.Modal, title='Set Tournament Date'):
    match_date = discord.ui.TextInput(
        label='Match Date',
        placeholder='e.g., March 20th, 2026',
        required=True,
        max_length=50
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _set_config, 'tournament_date', self.match_date.value.strip())
        fup = await interaction.followup.send(f"✅ Tournament date set to: **{self.match_date.value}**", ephemeral=True)
        asyncio.create_task(schedule_delete_fup(fup, delay=15))

class ScheduleIndexSelectView(discord.ui.View):
    def __init__(self, count):
        super().__init__(timeout=60)
        self.count = count
        
        # Max Discord select options is 25. Cap it just in case.
        max_opt = min(count, 25)
        options = [discord.SelectOption(label=f"Schedule {i}", value=str(i)) for i in range(1, max_opt + 1)]
        
        self.select = discord.ui.Select(placeholder="Choose a schedule template to edit...", options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)
        
        # Add button to create a new template
        add_btn = discord.ui.Button(label='➕ Add New Template', style=discord.ButtonStyle.success)
        add_btn.callback = self.add_new_callback
        self.add_item(add_btn)

    async def select_callback(self, interaction: discord.Interaction):
        index = int(self.select.values[0])
        await interaction.response.send_modal(EditScheduleModal(index))

    async def add_new_callback(self, interaction: discord.Interaction):
        next_index = self.count + 1
        await interaction.response.send_modal(EditScheduleModal(next_index))

class EditScheduleModal(discord.ui.Modal):
    def __init__(self, index):
        super().__init__(title=f'Edit Schedule {index}')
        self.index = index
        self.m1_idp = discord.ui.TextInput(label='Match 1 IDP Time', placeholder='e.g., 12:50 PM', required=True)
        self.m1_start = discord.ui.TextInput(label='Match 1 Start Time', placeholder='e.g., 1:00 PM', required=True)
        self.m2_idp = discord.ui.TextInput(label='Match 2 IDP Time', placeholder='e.g., 1:30 PM', required=True)
        self.m2_start = discord.ui.TextInput(label='Match 2 Start Time', placeholder='e.g., 1:38 PM', required=True)
        
        # Pre-fill
        self.add_item(self.m1_idp)
        self.add_item(self.m1_start)
        self.add_item(self.m2_idp)
        self.add_item(self.m2_start)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(None, _set_schedule, 
            self.index, 
            self.m1_idp.value.strip(), 
            self.m1_start.value.strip(), 
            self.m2_idp.value.strip(), 
            self.m2_start.value.strip()
        )
        if success:
            fup = await interaction.followup.send(f"✅ Schedule {self.index} synchronized successfully.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
        else:
            fup = await interaction.followup.send("❌ Synchronization Error: Failed to process schedule.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))


# ─── LOBBY CONTROL PANEL ──────────────────────────────────────────────────────
class QualifiedTeamsModal(discord.ui.Modal, title='🏆 Set Qualified Teams'):
    slots = discord.ui.TextInput(
        label='Enter Qualified Slot Numbers',
        placeholder='e.g., s4, s21, s12',
        style=discord.TextStyle.paragraph,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        # Staff check
        if not _has_staff_role(interaction.user):
            fup = await interaction.followup.send("❌ Access denied.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return
        
        # Parse inputs: s4, s21 -> [3, 20]
        try:
            raw_input = self.slots.value.lower().replace('s', '')
            # Split by comma or space
            parts = [p.strip() for p in raw_input.replace(',', ' ').split() if p.strip()]
            # Slot numbers start at 3 now. index = slot_num - 3 (e.g. s3 -> index 0)
            qualified_indices = [int(p) - 3 for p in parts if p.isdigit()]
            
            # Identify if any selected slots are 1 or 2 (Locked)
            locked_selected = [p for p in parts if p.isdigit() and int(p) < 3]
            if locked_selected:
                fup = await interaction.followup.send(f"❌ **Error**: Slot(s) **{', '.join(f's{s}' for s in locked_selected)}** are **Locked** and cannot be used for qualification.", ephemeral=True)
                asyncio.create_task(schedule_delete_fup(fup, delay=15))
                return
        except Exception:
            fup = await interaction.followup.send("❌ Invalid format. Use: s3, s4, s5 or 3, 4, 5", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return

        if not qualified_indices:
            fup = await interaction.followup.send("❌ No valid slot numbers provided (Minimum Slot is s3).", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return

        lobby_name = interaction.channel.name.replace('lobby-', 'Lobby-')
        guild = interaction.guild
        lobby_role = discord.utils.get(guild.roles, name=lobby_name)
        
        loop = asyncio.get_event_loop()
        all_teams = await loop.run_in_executor(None, _get_all_teams)
        
        lobby_teams = [
            t for t in all_teams 
            if str(t.get('lobby', '')).strip().lower() == lobby_name.strip().lower()
        ]

        if not lobby_teams:
            fup = await interaction.followup.send(f"❌ No teams are currently assigned to **{lobby_name}**.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return

        # Validate indices
        invalid_slots = [idx + 1 for idx in qualified_indices if idx < 0 or idx >= len(lobby_teams)]
        if invalid_slots:
            slot_list = ", ".join(f"s{s}" for s in invalid_slots)
            fup = await interaction.followup.send(
                f"❌ **Error**: Slot(s) **{slot_list}** are empty. No team is assigned to these positions in **{lobby_name}**.",
                ephemeral=True,
                wait=True
            )
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return

        qualified_teams = []
        disqualified_teams = []
        
        for idx, team in enumerate(lobby_teams):
            if idx in qualified_indices:
                qualified_teams.append(team)
            else:
                disqualified_teams.append(team)

        # 1. Prepare Announcement
        lines = []
        for q_team in qualified_teams:
            mention = f"<@{q_team.get('creator_id')}>"
            lines.append(f"🎉 Congratulations {mention}, your team has qualified for Tier 3.")
        
        # 2. Results Footer (Always posted together in one single message)
        if disqualified_teams or not qualified_teams:
            footer = "\n🔔 **Results are out! All disqualified teams can now register again.**"
            lines.append(footer)
        
        if lines:
            # JOIN and SEND as ONE single message
            await interaction.channel.send("\n".join(lines))
            fup = await interaction.followup.send("✅ Qualification announcement posted!", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))

class LobbyConfirmationView(discord.ui.View):
    """Generic Confirm/Cancel UI for destructive actions."""
    def __init__(self, target_owner: discord.Member, callback, timeout=30):
        super().__init__(timeout=timeout)
        self.target_owner = target_owner
        self.callback = callback
        self.confirmed = False

    @discord.ui.button(label="✅ Confirm Deletion", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Restriction: Only the person who triggered the command can confirm
        if interaction.user.id != self.target_owner.id:
            await interaction.response.send_message("❌ This confirmation is not for you.", ephemeral=True)
            return
            
        self.confirmed = True
        try:
            # 🟢 Use ephemeral defer to ensure we stay within the 3s window safely
            await interaction.response.defer(ephemeral=True)
        except discord.errors.NotFound:
            print("⚠️ Interaction expired before defer (Bot might be under heavy load).")
            return
        except Exception as e:
            print(f"⚠️ Defer Error: {e}")
            
        await self.callback(interaction)
        
        # 🟢 CLEANUP: Delete the "Are you sure?" prompt after success
        try:
            await interaction.delete_original_response()
        except: pass
        self.stop()

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.target_owner.id:
            await interaction.response.send_message("❌ This confirmation is not for you.", ephemeral=True)
            return
            
        # 🟢 CLEANUP: Delete the prompt immediately on cancel
        try:
            await interaction.response.defer()
            await interaction.delete_original_response()
        except: pass
        self.stop()

class RescheduleSelectView(discord.ui.View):
    def __init__(self, current_lobby_name: str, target_owner_id: str, available_roles: list, lobby_schedules: list):
        super().__init__(timeout=None)
        self.current_lobby_name = current_lobby_name
        self.target_owner_id = target_owner_id
        
        options = []
        for role in available_roles:
            try:
                r_num = role.name.split('-')[-1]
                sched = next((s for s in lobby_schedules if str(s.get('lobby_number', '')) == str(r_num)), None)
                if sched:
                    date = str(sched.get('match_date', 'TBA')).strip()
                    m1_time = str(sched.get('m1_start', 'TBA')).strip()
                    m2_time = str(sched.get('m2_start', 'TBA')).strip()
                    label = f"L-{r_num} | {date} | ({m1_time}) - {m2_time}"
                else:
                    label = f"L-{r_num} | Schedule Pending"
                    
                if len(label) > 100:
                    label = label[:97] + "..."
                    
                options.append(discord.SelectOption(label=label, value=role.name))
            except Exception:
                options.append(discord.SelectOption(label=role.name, value=role.name))
            
        self.select = discord.ui.Select(placeholder="Select a new Lobby to join...", min_values=1, max_values=1, options=options)
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        selected_lobby_name = self.select.values[0]
        guild = interaction.guild
        
        # Identify the target owner (team owner)
        try:
            target_member = await guild.fetch_member(int(self.target_owner_id))
        except Exception:
            fup = await interaction.followup.send("❌ Could not find the team owner in this server.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return
        
        # Verify the new role still exists and has space
        new_role = discord.utils.get(guild.roles, name=selected_lobby_name)
        if not new_role or len(new_role.members) >= LOBBY_SIZE:
            fup = await interaction.followup.send(f"❌ **{selected_lobby_name}** is now full or unavailable. Please try again.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return
            
        current_role = discord.utils.get(guild.roles, name=self.current_lobby_name)
        
        # 1. Update Sheet & Log using target_owner_id
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(None, _update_team_lobby, self.target_owner_id, selected_lobby_name)
        # Shift the lock in the Registration Log
        await loop.run_in_executor(None, _update_log_lobby, self.target_owner_id, selected_lobby_name)
        
        if success:
            # 2. Swap Roles — ADD NEW FIRST, then remove old!
            # CRITICAL: If we remove first, on_member_update sees no Lobby- role
            # and wipes all DB data (race condition). Adding first prevents this.
            try:
                await target_member.add_roles(new_role, reason="Rescheduled Match")
                if current_role and current_role in target_member.roles:
                    await target_member.remove_roles(current_role, reason="Rescheduled Match")
                
                # 3. Refresh slotlists for BOTH old and new lobby
                await _update_lobby_slotlist(guild, self.current_lobby_name)
                await _update_lobby_slotlist(guild, selected_lobby_name)
                
                fup = await interaction.followup.send(f"✅ Successfully rescheduled to **{selected_lobby_name}**!", ephemeral=True)
                asyncio.create_task(schedule_delete_fup(fup, delay=15))
            except Exception as e:
                print(f"Role swap error: {e}")
                fup = await interaction.followup.send("⚠️ Record synchronized, but Discord access roles failed to update.", ephemeral=True)
                asyncio.create_task(schedule_delete_fup(fup, delay=15))
        else:
            fup = await interaction.followup.send("❌ System update failed. Access roles remain unchanged.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))


class ManageMatchesView(discord.ui.View):
    def __init__(self, lobby_name: str):
        super().__init__(timeout=None)
        self.lobby_name = lobby_name

    @discord.ui.button(label='📅 Reschedule Match', style=discord.ButtonStyle.success, custom_id='reschedule_match_btn')
    async def reschedule_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        
        # Check if lobby is locked
        loop = asyncio.get_event_loop()
        locked_str = str(await loop.run_in_executor(None, _get_config, 'locked_lobbies', ''))
        locked_list = [l.strip() for l in locked_str.split(',') if l.strip()]
        if self.lobby_name in locked_list:
            fup = await interaction.followup.send(f"❌ The slotlist for **{self.lobby_name}** has already been published. You can no longer reschedule.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return
            
        # Identify the specific team owned by the clicker in THIS lobby
        user_id_str = str(interaction.user.id)
        all_teams = await loop.run_in_executor(None, _get_all_teams)
        target_team = next((t for t in all_teams if str(t.get('creator_id') or t.get('Discord ID')) == user_id_str and str(t.get('lobby') or t.get('Lobby', '')).strip().lower() == self.lobby_name.strip().lower()), None)
        
        if not target_team:
            fup = await interaction.followup.send("❌ No team is currently assigned to this lobby in the database.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return

        target_owner_id = str(target_team.get('creator_id') or target_team.get('Discord ID'))
        
        # STRICT PERMISSION: Only the team owner can reschedule via this menu
        if str(interaction.user.id) != target_owner_id:
            fup = await interaction.followup.send("❌ Only the team owner can reschedule this match.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return
            
        # Parse current lobby number to find strictly higher ones
        try:
            current_num = int(self.lobby_name.split('-')[-1])
        except ValueError:
            fup = await interaction.followup.send("❌ Error parsing your current lobby number.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return

        lobby_roles = [r for r in guild.roles if r.name.startswith('Lobby-')]
        available_higher_roles = []
        
        for r in lobby_roles:
            try:
                r_num = int(r.name.split('-')[-1])
                # Owners can only move to HIGHER numbered lobbies
                if r_num > current_num and len(r.members) < LOBBY_SIZE and r.name not in locked_list:
                    available_higher_roles.append(r)
            except ValueError:
                continue
                
        if not available_higher_roles:
            fup = await interaction.followup.send("❌ No lobbies available for rescheduling.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return
            
        # Sort ascending (Lobby-3, Lobby-4, ...) and limit to 25 for select menus
        available_higher_roles.sort(key=lambda r: _get_lobby_num(r.name))
        available_higher_roles = available_higher_roles[:25]
        
        lobby_schedules = await loop.run_in_executor(None, _get_all_lobby_schedules)
            
        fup = await interaction.followup.send(
            "Select a lobby to reschedule to:", 
            view=RescheduleSelectView(self.lobby_name, target_owner_id, available_higher_roles, lobby_schedules),
            ephemeral=True
        )
        asyncio.create_task(schedule_delete_fup(fup, delay=15))

    @discord.ui.button(label='❌ Remove Team', style=discord.ButtonStyle.danger, custom_id='user_remove_team_btn')
    async def remove_team(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        user = interaction.user
        
        # Check if lobby is locked
        loop = asyncio.get_event_loop()
        locked_str = str(await loop.run_in_executor(None, _get_config, 'locked_lobbies', ''))
        locked_list = [l.strip() for l in locked_str.split(',') if l.strip()]
        if self.lobby_name in locked_list:
            fup = await interaction.followup.send(f"❌ Slotlist for **{self.lobby_name}** is published. Removal is locked.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return
            
        # Identify the specific team owned by the clicker in THIS lobby
        user_id_str = str(interaction.user.id)
        all_teams = await loop.run_in_executor(None, _get_all_teams)
        target_team = next((t for t in all_teams if str(t.get('creator_id') or t.get('Discord ID')) == user_id_str and str(t.get('lobby') or t.get('Lobby', '')).strip().lower() == self.lobby_name.strip().lower()), None)
        
        if not target_team:
            fup = await interaction.followup.send("❌ No team is currently assigned to this lobby in the database.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return

        target_owner_id = str(target_team.get('creator_id') or target_team.get('Discord ID'))
        
        # STRICT PERMISSION: Only the team owner can remove via this menu
        if str(user.id) != target_owner_id:
            fup = await interaction.followup.send("❌ Only the team owner can remove their team from this lobby.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return

        # Perform removal on target_owner_id
        success = await loop.run_in_executor(None, _update_team_lobby, target_owner_id, "")
        await loop.run_in_executor(None, _update_log_lobby, target_owner_id, "")
        
        if success:
            try:
                target_member = await guild.fetch_member(int(target_owner_id))
                current_role = discord.utils.get(guild.roles, name=self.lobby_name)
                if current_role and target_member and current_role in target_member.roles:
                    await target_member.remove_roles(current_role, reason="Requested team removal from lobby")
                
                # 1. Automatically fill the vacancy from waitlist
                await _fill_lobby_from_waitlist(guild, self.lobby_name)
                
                # 2. Refresh the slotlist (Helper above already does it, but double sync won't hurt)
                await _update_lobby_slotlist(guild, self.lobby_name)
                
                fup = await interaction.followup.send(f"✅ Team **{target_team.get('team_name')}** removed from **{self.lobby_name}**.", ephemeral=True)
                asyncio.create_task(schedule_delete_fup(fup, delay=15))
                
                # Announce to the lobby channel
                await interaction.channel.send(f"⚠️ **{target_team.get('team_name')}** (<@{target_owner_id}>) was removed from **{self.lobby_name}**.")
            except Exception as e:
                print(f"Role removal error: {e}")
                fup = await interaction.followup.send("⚠️ Team unassigned in DB but role removal failed.", ephemeral=True)
                asyncio.create_task(schedule_delete_fup(fup, delay=15))
        else:
            fup = await interaction.followup.send("❌ Failed to remove team from database.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))


class LobbyControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='📋 Publish Slotlist', style=discord.ButtonStyle.secondary, custom_id='lobby_publish_slots')
    async def publish_slotlist(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 🟢 OPTIMIZED: Direct Response
        
        # Admin check
        if not _has_staff_role(interaction.user):
            await interaction.response.send_message("❌ Access denied.", ephemeral=True)
            return
        
        # Get teams for this lobby
        lobby_name = interaction.channel.name.replace('lobby-', 'Lobby-')
        loop = asyncio.get_event_loop()
        all_teams = await loop.run_in_executor(None, _get_all_teams)
        
        lobby_teams = [
            t for t in all_teams 
            if str(t.get('lobby') or t.get('Lobby', '')).strip().lower() == lobby_name.strip().lower()
        ]
        
        if not lobby_teams:
            await interaction.response.send_message(f"❌ No teams in **{lobby_name}**.", ephemeral=True)
            return

        # Lock the lobby against future manage matches activity
        locked_str = str(await loop.run_in_executor(None, _get_config, 'locked_lobbies', ''))
        locked_list = [l.strip() for l in locked_str.split(',') if l.strip()]
        if lobby_name not in locked_list:
            locked_list.append(lobby_name)
            new_locked = ",".join(locked_list)
            await loop.run_in_executor(None, _set_config, 'locked_lobbies', new_locked)

        await interaction.response.defer(ephemeral=True)
        await _update_lobby_slotlist(interaction.guild, lobby_name)
        await interaction.followup.send("✅ Slotlist published.", ephemeral=True)

    @discord.ui.button(label='🔔 Match Reminder', style=discord.ButtonStyle.success, custom_id='lobby_match_reminder')
    async def match_reminder(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Staff-only: Pings the lobby role with match rules and reminder."""
        if not _has_staff_role(interaction.user):
            await interaction.response.send_message("❌ Access denied. Staff/Mod only.", ephemeral=True)
            return
            
        await interaction.response.defer(ephemeral=True)
        
        lobby_name = interaction.channel.name.replace('lobby-', 'Lobby-')
        role = discord.utils.get(interaction.guild.roles, name=lobby_name)
        
        if not role:
            await interaction.followup.send(f"❌ Error: Role **{lobby_name}** not found.", ephemeral=True)
            return
            
        reminder_msg = (
            "Hey Teams,\n\n"
            "• **Today** is your match, Please check the IDP timings provided above.\n"
            "• Slot cancellation is only allowed **10 minutes** before the IDP timings.\n"
            "• Wasting a slot will result in a **Scrims ban**.\n"
            "• Screenshots submission is compulsory within 30 minutes after completion of both matches or you will get NO points.\n\n"
            "> **--- NO SS = NO POINTS ---**\n\n"
            "Thanks & Regards,\n"
            "**VirelApp**\n"
            f"{role.mention}"
        )
        
        await interaction.channel.send(reminder_msg)
        await interaction.followup.send(f"✅ Match reminder sent for **{lobby_name}**.", ephemeral=True)
        await _log_staff_action(interaction, f"Sent Match Reminder in {interaction.channel.mention}")


    @discord.ui.button(label='✅ Qualified Teams', style=discord.ButtonStyle.success, custom_id='lobby_qualified')
    async def qualified_teams(self, interaction: discord.Interaction, button: discord.ui.Button):
        # We cannot defer before sending a modal! Modals must be the interaction response.
        # But we can simplify the check and respond immediately.
        if not _has_staff_role(interaction.user):
            await interaction.response.send_message("❌ Access denied.", ephemeral=True)
            return
        await interaction.response.send_modal(QualifiedTeamsModal())

    @discord.ui.button(label='🎮 Manage Matches', style=discord.ButtonStyle.primary, custom_id='lobby_manage_matches')
    async def manage_matches(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 🟢 OPTIMIZED: Direct Response
        # Admin OR specific Lobby member
        is_admin = _has_staff_role(interaction.user)
        
        lobby_name = interaction.channel.name.replace('lobby-', 'Lobby-')
        has_lobby_role = discord.utils.get(interaction.user.roles, name=lobby_name)
        
        if not is_admin and not has_lobby_role:
            await interaction.response.send_message("❌ Access denied.", ephemeral=True)
            return
            
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send(
            f"Select an action for **{lobby_name}**:", 
            view=ManageMatchesView(lobby_name), 
            ephemeral=True
        )

    @discord.ui.button(label='❌ Remove Team', style=discord.ButtonStyle.danger, custom_id='lobby_remove_team')
    async def remove_team_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not _has_staff_role(interaction.user):
            await interaction.response.send_message("❌ Access denied.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        
        lobby_name = interaction.channel.name.replace('lobby-', 'Lobby-')
        loop = asyncio.get_event_loop()
        # 🟢 OPTIMIZED: Targeted lobby query
        lobby_teams = await loop.run_in_executor(None, _get_teams_in_lobby, lobby_name)
        
        if not lobby_teams:
            await interaction.followup.send("❌ No teams in this lobby.", ephemeral=True)
            return

        # Show select menu to choose team
        view = discord.ui.View()
        select = discord.ui.Select(placeholder="Select a team to remove from this lobby...")
        
        for t in lobby_teams[:25]: # Max 25 in select
            t_name = t.get('team_name') or t.get('Team Name', 'Unknown')
            c_id = t.get('creator_id') or t.get('Discord ID', '')
            select.add_option(label=t_name, value=c_id, description=f"ID: {c_id}")

        async def select_callback(inter: discord.Interaction):
            if not _has_staff_role(inter.user):
                await inter.response.send_message("❌ Access denied. Only staff can perform this action.", ephemeral=True)
                return
                
            removed_id = select.values[0]
            # Clear lobby in DB
            await loop.run_in_executor(None, _update_team_lobby, removed_id, '')
            # Clear registration log entry so !clear doesn't break
            await loop.run_in_executor(None, _clear_log_protection_by_player, removed_id)
            
            # Remove role
            guild = inter.guild
            try:
                member = await guild.fetch_member(int(removed_id))
                role = discord.utils.get(guild.roles, name=lobby_name)
                if member and role:
                    await member.remove_roles(role)
            except Exception:
                pass
            
            # 1. Automatically fill the vacancy from waitlist
            await _fill_lobby_from_waitlist(guild, lobby_name)
            
            # 2. Auto-update the slotlist after admin removes a team (Helper above already does it)
            await _update_lobby_slotlist(guild, lobby_name)
            
            await inter.response.send_message(f"✅ Team removed from **{lobby_name}**.", ephemeral=True)
            asyncio.create_task(schedule_delete(inter))

        select.callback = select_callback
        view.add_item(select)
        fup = await interaction.followup.send("Select a team to remove:", view=view, ephemeral=True)
        asyncio.create_task(schedule_delete_fup(fup))

# ─── BOT ──────────────────────────────────────────────────────────────────────
class MyBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix='!', intents=intents, help_command=None)

    async def background_sheet_init(self):
        """Asynchronously initializes Google Sheets in the background."""
        try:
            # authorize in executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, initialize_sheets)
            print("✨ Google Sheets Mirroring Background Sync Active.")
        except Exception as e:
            print(f"⚠️ Background Sheet Sync Failed: {e}")

    async def setup_hook(self):
        # 0. Initialize DB WAL mode & Seed schedules (Offloaded to executor to avoid blocking)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, init_wal_mode)
        await loop.run_in_executor(None, _seed_schedules)
        # Automated Schema Migration
        await loop.run_in_executor(None, _ensure_log_schema)
        
        # 1. Start Google Sheets initialization in the background (Non-blocking!)
        asyncio.create_task(self.background_sheet_init())
        
        # 3. Add views
        self.add_view(MainTeamView())
        self.add_view(OldOrNewTeamView(''))
        self.add_view(Step2View())
        self.add_view(Step3View())
        self.add_view(LobbyControlView())
        self.add_view(AdminControlCenterView())

    async def on_ready(self):
        print(f'Logged in as {self.user}!')
        print('Bot is ready.')
        
        # 1. Start Background Tasks (Unified)
        if not check_expiring_bans.is_running():
            check_expiring_bans.start()
        if not drain_dirty_sync.is_running():
            drain_dirty_sync.start()
        if not registration_timeout_task.is_running():
            registration_timeout_task.start()

        
        # 2. Register Persistent Views
        self.add_view(IdPassLauncherView())
        
        # 3. Load Teams Awaiting Tag Confirmation
        loop = asyncio.get_event_loop()
        
        # 🟢 OPTIMIZED: Targeted SQL query to only fetch teams where lobby is empty
        def _get_awaiting_tag_teams():
            with _get_db_conn() as conn:
                return conn.execute("SELECT discord_id, team_name FROM teams WHERE (lobby IS NULL OR lobby = '')").fetchall()
        
        recovered_rows = await loop.run_in_executor(None, _get_awaiting_tag_teams)
        
        if recovered_rows:
            # 🟢 OPTIMIZED: Cache the role and confirm channel to prevent redundant lookups
            for guild in self.guilds:
                tagger_role = await _get_tagger_role(guild)
                confirm_ch = guild.get_channel(CONFIRM_CHANNEL_ID)
                
                for row in recovered_rows:
                    did, tname = str(row['discord_id']), row['team_name']
                    awaiting_tag[did] = {"team_name": tname, "session_id": "RECOVERY"}
                    
                    if tagger_role and confirm_ch:
                        member = guild.get_member(int(did))
                        if member:
                            async def _recover_member(m, d, r):
                                try:
                                    await m.add_roles(r, reason="Startup Recovery - Awaiting Tags")
                                    # 🟢 FIX: Start the expiry timer for recovered sessions too!
                                    asyncio.create_task(_schedule_tagger_expiry(m, d, "RECOVERY"))
                                except Exception as e:
                                    print(f"Failed to recover role for {m.display_name}: {e}")
                            
                            # 🟢 NON-BLOCKING: Fire and forget recovery tasks
                            asyncio.create_task(_recover_member(member, did, tagger_role))
                            
        print(f'✅ Startup Sync: Recovered {len(awaiting_tag)} active sessions.')

    # DELETED: Older on_ready content below was merged here.



        

            





bot = MyBot()

# ─── COMMANDS ──────────────────────────────────────────────────────────────────
# Admin commands setup, panel, setdate deleted - Use the Admin Dashboard (!ac) instead.

@bot.command(name='teams')
@is_bot_admin()
async def list_teams(ctx):
    """Generates and sends CSV files for both Active and Deleted teams."""
    loop = asyncio.get_event_loop()
    teams = await loop.run_in_executor(None, _get_all_teams)
    del_teams = await loop.run_in_executor(None, _get_all_deleted_teams)

    if not teams and not del_teams:
        await ctx.send('No registration data available (Active or Deleted).', delete_after=30)
        return

    files_to_send = []

    # 🟢 1. Create Active Teams CSV
    if teams:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(TEAMS_HEADERS)
        for team in teams:
            # OPTIMIZED: Use a refined row builder to ensure alignment with TEAMS_HEADERS
            def _get(key): return team.get(key) or ''
            writer.writerow([
                _get('created_at'), _get('creator_id'), _get('team_name'),
                _get('owner_name'), _get('owner_email'), _get('contact_number'),
                _get('player1'), _get('player2'), _get('player3'), _get('player4'),
                _get('sub1'), _get('sub2'), _get('insta_handles'), _get('lobby'),
                _get('tagged_names'), _get('tagged_ids'), _get('p1_tag'),
                _get('p2_tag'), _get('p3_tag'), _get('p4_tag')
            ])
        output.seek(0)
        files_to_send.append(discord.File(fp=io.BytesIO(output.getvalue().encode('utf-8')), filename='Active_Teams.csv'))

    # 🔴 2. Create Deleted Teams CSV
    if del_teams:
        output_del = io.StringIO()
        writer_del = csv.writer(output_del)
        writer_del.writerow(DELETED_TEAMS_HEADERS)
        for dt in del_teams:
            # OPTIMIZED: Use DELETED_TEAMS_HEADERS order
            writer_del.writerow([
                dt.get('discord_id', ''), dt.get('team_name', ''), dt.get('owner_name', ''),
                dt.get('owner_email', ''), dt.get('contact', ''), dt.get('player1', ''),
                dt.get('player2', ''), dt.get('player3', ''), dt.get('player4', ''),
                dt.get('sub1', ''), dt.get('sub2', ''), dt.get('ig_handles', ''),
                dt.get('lobby', ''), dt.get('tagged_ids', ''), dt.get('p1_tag', ''),
                dt.get('p2_tag', ''), dt.get('p3_tag', ''), dt.get('p4_tag', ''),
                dt.get('deleted_at', '')
            ])
        output_del.seek(0)
        files_to_send.append(discord.File(fp=io.BytesIO(output_del.getvalue().encode('utf-8')), filename='Archived_Teams_History.csv'))

    if files_to_send:
        await ctx.send('📊 **Registration Export**: Found active and/or archived team data.', files=files_to_send)
    
    try: await ctx.message.delete()
    except: pass

async def list_teams_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send('❌ Access denied.', delete_after=15)

@bot.command(name='delete_lobby')
@is_bot_admin()
async def delete_lobby(ctx):
    """Deletes the current lobby channel and its associated role (With Confirmation)."""
    channel = ctx.channel
    category = channel.category
    if not category or category.name.upper() != LOBBY_CATEGORY_NAME.upper() or not channel.name.startswith('lobby-'):
        await ctx.send('❌ Use this inside a lobby channel.', delete_after=15)
        return

    async def _confirm_delete_lobby(interaction):
        parts = channel.name.split('-')
        if len(parts) == 2:
            role_name = f'{parts[0].capitalize()}-{parts[1]}'
            role = discord.utils.get(ctx.guild.roles, name=role_name)
            if role:
                # 1. Update Registration Log
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _delete_log_by_lobby, role_name)
                
                # 2. Update Locked Lobbies Config (Cleanup)
                locked_str = str(await loop.run_in_executor(None, _get_config, 'locked_lobbies', ''))
                locked_list = [l.strip() for l in locked_str.split(',') if l.strip()]
                if role_name in locked_list:
                    locked_list.remove(role_name)
                    new_locked = ','.join(locked_list)
                    await loop.run_in_executor(None, _set_config, 'locked_lobbies', new_locked)
                    print(f"🔓 Removed {role_name} from Locked Lobbies list.")

                # 3. Delete Role
                try: await role.delete(reason='Lobby deleted by admin')
                except: pass
        try:
            await channel.delete(reason='Lobby deleted by admin')
        except Exception as e:
            await interaction.followup.send(f'❌ Failed to delete channel: {e}', ephemeral=True)

    await ctx.send(
        f"⚠️ **Confirm Deletion**: Are you sure you want to delete this lobby and its role? (<@{ctx.author.id}>)",
        view=LobbyConfirmationView(ctx.author, _confirm_delete_lobby)
    )


# ─── ADMIN COMMANDS (Staff Only) ──────────────────────────────────────────────

@bot.command(name='purge_waitlist')
@is_admin_only()
async def purge_waitlist(ctx):
    """Purges all teams currently in the WAITING list from the database."""
    loop = asyncio.get_event_loop()
    def _purge_db():
        with _get_db_conn() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM teams WHERE lobby = 'WAITING'")
            deleted = c.rowcount
            conn.commit()
            return deleted
            
    deleted_count = await loop.run_in_executor(None, _purge_db)
    await ctx.send(f"✅ Successfully purged **{deleted_count}** waitlisted teams from the database.", delete_after=30)
    try: await ctx.message.delete()
    except: pass

@bot.command(name='reset_all_locks')
@is_admin_only()
async def reset_all_locks(ctx):
    """Clears all DB Assignments and Player Locks. Admin only."""
    await ctx.send("Resetting all locks...", delete_after=15)
    loop = asyncio.get_event_loop()
    db_reset = await loop.run_in_executor(None, _reset_all_locks_internal)
    if db_reset:
        await ctx.send("✅ All locks cleared.", delete_after=15)
    else:
        await ctx.send("❌ Reset failed.", delete_after=15)
    try: await ctx.message.delete()
    except: pass

@bot.command(name='clear')
@is_bot_admin()
async def clear_user_registration(ctx, member: discord.Member):
    """Clears registration and lobby protection for a player."""
    target_id = str(member.id)
    loop = asyncio.get_event_loop()

    cleared_entries = await loop.run_in_executor(None, _clear_log_protection_by_player, target_id)
    active_registrations.pop(target_id, None) # Clear registration timer
    awaiting_tag.pop(target_id, None) # Clear tagging session

    guild = ctx.guild

    if cleared_entries:
        # Found in registration_log — clear teams table + roles + slotlist
        for entry in cleared_entries:
            owner_id = entry['owner_id']
            lobby_name = entry['lobby']
            await loop.run_in_executor(None, _update_team_lobby, owner_id, '', '', [], '')
            if lobby_name and lobby_name.upper() != 'WAITING':
                try:
                    owner_member = guild.get_member(int(owner_id))
                    lobby_role = discord.utils.get(guild.roles, name=lobby_name)
                    if lobby_role and owner_member and lobby_role in owner_member.roles:
                        await owner_member.remove_roles(lobby_role, reason=f"!clear by {ctx.author}")
                except Exception:
                    pass
                # Auto-fill vacancy
                await _fill_lobby_from_waitlist(guild, lobby_name)
                await _update_lobby_slotlist(guild, lobby_name)
    else:
        # FALLBACK: Check teams table directly (handles reschedule desync)
        team = await loop.run_in_executor(None, _get_team, target_id)
        if not team:
            await ctx.send(f"{member.mention} registration not found.", delete_after=15)
            try: await ctx.message.delete()
            except: pass
            return
        
        lobby_name = team.get('lobby', '')
        # Clear the teams table lobby
        await loop.run_in_executor(None, _update_team_lobby, target_id, '', '', [], '')
        
        if lobby_name and lobby_name.upper() != 'WAITING':
            try:
                lobby_role = discord.utils.get(guild.roles, name=lobby_name)
                if lobby_role and lobby_role in member.roles:
                    await member.remove_roles(lobby_role, reason=f"!clear by {ctx.author}")
            except Exception:
                pass
            # Auto-fill vacancy
            await _fill_lobby_from_waitlist(guild, lobby_name)
            await _update_lobby_slotlist(guild, lobby_name)
        
        # Treat it as a single cleared entry for the success message
        cleared_entries = [{'owner_id': target_id, 'lobby': lobby_name}]

    # Prepare the success message
    affected_owners = list(set([str(e['owner_id']) for e in cleared_entries]))
    owner_mentions = " ".join([f"<@{uid}>" for uid in affected_owners])
    
    await ctx.send(f"✅ **Registration Cleared** for {member.mention} {owner_mentions}", delete_after=20)
    try: await ctx.message.delete()
    except: pass

@clear_user_registration.error
async def clear_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send('❌ Access denied.', delete_after=15)
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send('Usage: `!clear @user`', delete_after=15)
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send('❌ Member not found.', delete_after=15)
    else:
        await ctx.send(f'❌ Error: `{error}`', delete_after=15)
        print(f"DEBUG: !clear error: {error}")

@bot.command(name='team')
@is_bot_admin()
async def team_info(ctx, member: discord.Member = None):
    """Shows team details for a user. Restricted to Staff (Admins/Mods)."""
    target = member or ctx.author
    user_id = str(target.id)
    loop = asyncio.get_event_loop()
    team = await loop.run_in_executor(None, _get_team, user_id)

    if not team:
        await ctx.send(f"❌ {target.mention} has no registered team.", delete_after=15)
        try: await ctx.message.delete()
        except: pass
        return

    lobby = team.get('lobby') or 'Not Assigned'
    tagged = team.get('tagged_ids', '')
    tagged_mentions = ' '.join(f'<@{tid.strip()}>' for tid in tagged.split(',') if tid.strip()) if tagged else 'Not tagged yet'

    embed = discord.Embed(
        title=f"Team: {team.get('team_name', 'Unknown')}",
        color=discord.Color.blue()
    )
    embed.add_field(name="Owner", value=f"<@{user_id}>", inline=True)
    embed.add_field(name="Contact", value=team.get('contact_number', 'N/A'), inline=True)

    roster = (
        f"1. {team.get('player1', 'N/A')}\n"
        f"2. {team.get('player2', 'N/A')}\n"
        f"3. {team.get('player3', 'N/A')}\n"
        f"4. {team.get('player4', 'N/A')}"
    )
    embed.add_field(name="Main Roster", value=roster, inline=False)

    subs = []
    if team.get('sub1'): subs.append(f"Sub 1: {team['sub1']}")
    if team.get('sub2'): subs.append(f"Sub 2: {team['sub2']}")
    if subs:
        embed.add_field(name="Substitutes", value="\n".join(subs), inline=False)

    embed.add_field(name="Tagged Players", value=tagged_mentions, inline=False)
    embed.set_footer(text=f"Requested by {ctx.author.display_name}")

    await ctx.send(embed=embed, delete_after=60)
    try: await ctx.message.delete()
    except: pass

@team_info.error
async def team_info_error(ctx, error):
    if isinstance(error, commands.MemberNotFound):
        await ctx.send('❌ Member not found. Tag them correctly.', delete_after=15)
    else:
        await ctx.send(f'Usage: `!team` or `!team @user`', delete_after=15)

@bot.command(name='lobby_status')
@is_bot_admin()
async def lobby_status(ctx):
    """Shows all lobbies with filled/empty slot counts. Restricted to Staff."""
        
    guild = ctx.guild
    lobby_roles = [r for r in guild.roles if r.name.startswith('Lobby-')]

    if not lobby_roles:
        await ctx.send("❌ No lobbies found.", delete_after=15)
        try: await ctx.message.delete()
        except: pass
        return

    lobby_roles.sort(key=lambda r: _get_lobby_num(r.name))

    # Get locked lobbies
    loop = asyncio.get_event_loop()
    locked_str = str(await loop.run_in_executor(None, _get_config, 'locked_lobbies', ''))
    locked_list = [l.strip() for l in locked_str.split(',') if l.strip()]

    total_filled = 0
    total_capacity = 0
    lines = []

    for role in lobby_roles:
        # 🟢 HONEST AUDIT: Check both DB and Role counts
        db_teams = await loop.run_in_executor(None, _get_teams_in_lobby, role.name)
        db_count = len(db_teams)
        role_count = len(role.members)
        
        count_display = f"`{db_count}/{LOBBY_SIZE}`"
        if db_count != role_count:
            count_display = f"`DB:{db_count} | R:{role_count}`"

        capacity = LOBBY_SIZE
        total_filled += db_count
        total_capacity += capacity
        is_locked = "🔒" if role.name in locked_list else "🔓"

        # Progress bar based on DB count
        bar_len = 10
        fill_count = round((db_count / capacity) * bar_len)
        bar = "█" * fill_count + "░" * (bar_len - fill_count)

        lines.append(f"{is_locked} **{role.name}** — {count_display} {bar}")


    # Split into chunks of 15 for embed field limits
    embed = discord.Embed(
        title="📊 Lobby Status",
        description=f"**Total:** `{total_filled}/{total_capacity}` slots filled across `{len(lobby_roles)}` lobbies\n",
        color=discord.Color.gold()
    )

    chunk_size = 15
    for i in range(0, len(lines), chunk_size):
        chunk = lines[i:i + chunk_size]
        name = "Lobbies" if i == 0 else f"Lobbies (cont.)"
        embed.add_field(name=name, value="\n".join(chunk), inline=False)

    embed.set_footer(text="🔒 = Published  |  🔓 = Open")

    await ctx.send(embed=embed, delete_after=120)
    try: await ctx.message.delete()
    except: pass

@bot.command(name='help')
async def help_cmd(ctx):
    """Shows all available bot commands."""
    is_admin = discord.utils.get(ctx.author.roles, id=BOT_ADMIN_ROLE_ID) is not None
    
    embed = discord.Embed(
        title='Commands',
        color=discord.Color.gold()
    )
    
    if is_admin:
        admin_cmds = (
            "`!ac` — Admin Dashboard (Setup/All Del/Reset)\n"
            "`!id_panel` — Post ID/Password Control\n"
            "`!teams` — Export teams to CSV\n"
            "`!clear @user` — Clear a registration\n"
            "`!ban_team @user <time>` — Ban a user\n"
            "`!unban_team @user` — Unban a user\n"
            "`!delete_lobby` — Delete current lobby (Confirms)\n"
            "`!lobby_status` — Occupancy overview\n"
            "`!reset_all_locks` — Global Reset (Hard Wipe)\n"
        )
        embed.add_field(name='Admin', value=admin_cmds, inline=False)
    
    lobby_cmds = (
        "`!schedule` — View match timings\n"
        "`!team @user` — View team details\n"
        "`!help` — This menu"
    )
    embed.add_field(name='General', value=lobby_cmds, inline=False)
    
    await ctx.send(embed=embed)
    try:
        await ctx.message.delete()
    except Exception:
        pass


# --- GLOBAL HELPERS ---
async def _log_staff_action(interaction: discord.Interaction, action_name: str):
    """Global helper to log button clicks to the audit channel."""
    if AUDIT_LOG_CHANNEL_ID == 0: return
    audit_ch = interaction.guild.get_channel(AUDIT_LOG_CHANNEL_ID)
    if not audit_ch: return
    
    embed = discord.Embed(
        title="🛠️ Staff Action Log",
        description=f"**Action**: `{action_name}`",
        color=discord.Color.blue(),
        timestamp=discord.utils.utcnow()
    )
    embed.add_field(name="Staff Member", value=f"{interaction.user.mention} (`{interaction.user.id}`)", inline=False)
    embed.set_footer(text="Bot Infrastructure Audit")
    try: await audit_ch.send(embed=embed)
    except: pass

# ─── ADMIN CONTROL CENTER (DASHBOARD) ─────────────────────────────────────────
class AdminControlCenterView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)


    async def _get_stats_embed(self, guild):
        loop = asyncio.get_event_loop()
        all_teams = await loop.run_in_executor(None, _get_all_teams)
        bans = await loop.run_in_executor(None, _get_all_bans)
        
        existing_lobby_roles = {r.name for r in guild.roles if r.name.startswith('Lobby-')}
        total_registered = len(all_teams)
        active_lobby_teams = [t for t in all_teams if t.get('lobby') and t.get('lobby') in existing_lobby_roles]
        waiting_teams = [t for t in all_teams if t.get('lobby') == 'WAITING']
        
        total_lobbies = len(existing_lobby_roles)
        total_slots = total_lobbies * 20
        filled_slots = len(active_lobby_teams)
        occupancy_pct = (filled_slots / total_slots * 100) if total_slots > 0 else 0

        bar_length = 20
        filled_blocks = int(occupancy_pct / (100 / bar_length)) if total_slots > 0 else 0
        bar = "█" * filled_blocks + "░" * (bar_length - filled_blocks)

        embed = discord.Embed(
            title="",
            description=(
                f"```ansi\n"
                f"\u001b[1;33m╔{'═' * 38}╗\n"
                f"\u001b[1;33m║  ⚡ ADMIN CONTROL CENTER             ║\n"
                f"\u001b[1;33m╚{'═' * 38}╝\n```\n"
                f"🟢 **System Status:** `ONLINE` │ **Sync:** {discord.utils.format_dt(discord.utils.utcnow(), 'R')}\n"
                f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"
            ),
            color=0x2b2d31
        )
        
        embed.add_field(
            name="📋 Registration Overview",
            value=(
                f"```\n"
                f"├─ Registered  : {total_registered}\n"
                f"├─ In Lobbies  : {filled_slots}\n"
                f"└─ Waitlisted  : {len(waiting_teams)}\n```"
            ),
            inline=True
        )
        embed.add_field(
            name="🛡️ Security & Infra",
            value=(
                f"```\n"
                f"├─ Active Bans : {len(bans)}\n"
                f"├─ Lobbies     : {total_lobbies}\n"
                f"└─ Capacity    : {total_slots}\n```"
            ),
            inline=True
        )
        embed.add_field(
            name=f"📊 Occupancy — {occupancy_pct:.1f}%",
            value=f"`{bar}` **{filled_slots}/{total_slots}**",
            inline=False
        )
        
        embed.set_footer(text=f"🎮 {guild.name} • VirelApp Tournament Engine")
        embed.timestamp = discord.utils.utcnow()
        return embed


    @discord.ui.button(label='🛠️ Setup Lobbies', style=discord.ButtonStyle.secondary, custom_id='admin_setup_lobbies')
    async def setup_lobbies(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not discord.utils.get(interaction.user.roles, id=BOT_ADMIN_ROLE_ID):
            await interaction.response.send_message("❌ Admin only.", ephemeral=True)
            return
        if MASS_ACTION_IN_PROGRESS:
            await interaction.response.send_message("⏳ System Busy: Another mass update is in progress. Please wait.", ephemeral=True)
            return
        if not check_cooldown(interaction.user.id):
            await interaction.response.send_message("⏳ Dash on cooldown.", ephemeral=True)
            return

        await interaction.response.send_modal(SetupLobbyModal())
        await _log_staff_action(interaction, "Triggered Setup Lobbies Modal")

    @discord.ui.button(label='🗓️ Set Date', style=discord.ButtonStyle.secondary, custom_id='admin_set_date')
    async def set_date(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not discord.utils.get(interaction.user.roles, id=BOT_ADMIN_ROLE_ID):
            await interaction.response.send_message("❌ Admin only.", ephemeral=True)
            return
        await interaction.response.send_modal(SetDateModal())
        await _log_staff_action(interaction, "Opened Set Date Modal")

    @discord.ui.button(label='📝 Reg Panel', style=discord.ButtonStyle.secondary, custom_id='admin_post_reg')
    async def post_reg(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not discord.utils.get(interaction.user.roles, id=BOT_ADMIN_ROLE_ID):
            await interaction.response.send_message("❌ Admin only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        
        channel = interaction.guild.get_channel(REGISTRATION_CHANNEL_ID)
        if not channel:
            fup = await interaction.followup.send("❌ Error: Registration channel not found.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup, delay=15))
            return

        embed = discord.Embed(
            title='Scrims Team Registration',
            description='Click the buttons below to create or delete your team registration.',
            color=discord.Color.green()
        )
        await channel.send(embed=embed, view=MainTeamView())
        fup = await interaction.followup.send(f"✅ Registration panel posted in {channel.mention}!", ephemeral=True)
        asyncio.create_task(schedule_delete_fup(fup, delay=15))
        await self._log_staff_action(interaction, f"Posted Reg Panel in {channel.mention}")

    @discord.ui.button(label='🕛 ID/PW Control', style=discord.ButtonStyle.primary, custom_id='admin_id_pw_control')
    async def id_pw_control(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_mod = discord.utils.get(interaction.user.roles, id=MOD_ROLE_ID)
        is_admin = _has_staff_role(interaction.user)
        
        if not is_mod and not is_admin:
            await interaction.response.send_message("❌ Access denied. Moderator or Admin role required.", ephemeral=True)
            return
            
        # 🟢 HARDENED: Defer before heavy panel generation
        await interaction.response.defer(ephemeral=True)
        await _send_id_panel(interaction)
        await interaction.followup.send("✅ ID/Password control panel sent.", ephemeral=True)
        await _log_staff_action(interaction, "Sent ID/PW Control Panel")

    @discord.ui.button(label='🕒 Edit Schedule', style=discord.ButtonStyle.secondary, custom_id='admin_edit_schedule')
    async def edit_schedule_dash(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not discord.utils.get(interaction.user.roles, id=BOT_ADMIN_ROLE_ID):
            await interaction.response.send_message("❌ Admin only.", ephemeral=True)
            return
        # 🟢 OPTIMIZED: Direct Response
        loop = asyncio.get_event_loop()
        schedules = await loop.run_in_executor(None, _get_all_schedules)
        count = len(schedules) if schedules else 10
        fup = await interaction.followup.send(f"Select a schedule template (1-{count}) to edit, or add a new one:", view=ScheduleIndexSelectView(count), ephemeral=True)
        asyncio.create_task(schedule_delete_fup(fup, delay=60))
        await _log_staff_action(interaction, "Opened Edit Schedule Selection")

    @discord.ui.button(label='🎯 Edit Specific Lobby', style=discord.ButtonStyle.secondary, custom_id='admin_edit_specific')
    async def edit_specific_dash(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not discord.utils.get(interaction.user.roles, id=BOT_ADMIN_ROLE_ID):
            await interaction.response.send_message("❌ Admin only.", ephemeral=True)
            return
        await interaction.response.send_modal(EditSpecificLobbyModal())
        await _log_staff_action(interaction, "Opened Edit Specific Lobby Modal")

    @discord.ui.button(label='🗑️ Delete All Lobbies', style=discord.ButtonStyle.danger, custom_id='admin_delete_all_lobbies')
    async def delete_all_lobbies_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not discord.utils.get(interaction.user.roles, id=BOT_ADMIN_ROLE_ID):
            await interaction.response.send_message("❌ Admin only.", ephemeral=True)
            return
        if MASS_ACTION_IN_PROGRESS:
            await interaction.response.send_message("⏳ System Busy: Wipe in progress.", ephemeral=True)
            return

        async def _confirm_clear(confirm_interaction):
            global MASS_ACTION_IN_PROGRESS
            MASS_ACTION_IN_PROGRESS = True
            
            try:
                guild = confirm_interaction.guild
                category = discord.utils.get(guild.categories, name=LOBBY_CATEGORY_NAME)
                channels_deleted = 0
                roles_deleted = 0

                if category:
                    for ch in list(category.channels):
                        try:
                            await ch.delete(reason='Mass lobby deletion via Dashboard')
                            channels_deleted += 1
                            await asyncio.sleep(0.5) # Increased delay for rate limit safety
                        except: pass
                    try: 
                        await category.delete(reason='Mass lobby deletion via Dashboard')
                    except: pass

                for role in guild.roles:
                    if role.name.startswith('Lobby-'):
                        try:
                            await role.delete(reason='Mass lobby deletion via Dashboard')
                            roles_deleted += 1
                            await asyncio.sleep(0.5) # Increased delay for rate limit safety
                        except: pass

                # Clean all database assignments and registration locks
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _reset_all_locks_internal)
                
                # 🧼 CLEANUP: Clear locked_lobbies list globally during mass wipe
                await loop.run_in_executor(None, _set_config, 'locked_lobbies', '')
                print("🧼 Locked Lobbies list cleared.")

                res_msg = f"✅ Success: Deleted {channels_deleted} channels and {roles_deleted} roles."
                await confirm_interaction.followup.send(res_msg, ephemeral=True)
                await _log_staff_action(confirm_interaction, "EXECUTED: Delete All Lobbies (Mass Wipe)")
            finally:
                MASS_ACTION_IN_PROGRESS = False

        # 🟢 HARDENED: Confirmation is fast but defer to be safe
        await interaction.response.send_message(
            "⚠️ **DANGER ZONE**\nAre you sure you want to delete **ALL** lobbies, roles, and categories? This cannot be undone.",
            view=LobbyConfirmationView(interaction.user, _confirm_clear),
            ephemeral=True
        )


    @discord.ui.button(label='🔄 Refresh', style=discord.ButtonStyle.success, custom_id='admin_refresh')
    async def refresh_dash(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not check_cooldown(interaction.user.id, 3):
            await interaction.response.send_message("⏳ Refresh on cooldown.", ephemeral=True)
            return
            
        await interaction.response.defer()
        embed = await self._get_stats_embed(interaction.guild)
        await interaction.edit_original_response(embed=embed, view=self)



#  BLOCK 4: DISCORD EVENTS & SERVER LOGS (on_message, audit) 

@bot.event
async def on_command_completion(ctx):
    """Audit Logging: Logs successfully executed text commands to the audit channel."""
    if AUDIT_LOG_CHANNEL_ID == 0:
        return
        
    audit_ch = bot.get_channel(AUDIT_LOG_CHANNEL_ID)
    if not audit_ch:
        return

    command_name = ctx.command.name

    embed = discord.Embed(
        title="📝 Command Audit Log",
        color=discord.Color.light_grey(),
        timestamp=discord.utils.utcnow()
    )
    embed.add_field(name="User", value=f"{ctx.author.mention} (`{ctx.author.id}`)", inline=False)
    embed.add_field(name="Command", value=f"`{ctx.message.content}`", inline=False)
    embed.add_field(name="Channel", value=f"{ctx.channel.mention}", inline=False)
    
    if hasattr(ctx, 'audit_info'):
        embed.add_field(name="Result / Info", value=ctx.audit_info, inline=False)
    
    # 🟢 HARDENED: Fire-and-forget to avoid stalling the loop for simple logs
    asyncio.create_task(audit_ch.send(embed=embed))

# --- SERVER INFRASTRUCTURE LOGGING ---
@bot.event
async def on_guild_channel_create(channel):
    # Restore individual logs even during mass setup, but use background task for speed
    if SERVER_LOG_CHANNEL_ID == 0: return
    log_ch = channel.guild.get_channel(SERVER_LOG_CHANNEL_ID)
    if not log_ch: return
    
    embed = discord.Embed(title="📁 Channel Created", description=f"**{channel.name}** was created.\nType: `{channel.type}`", color=discord.Color.green())
    # Fire and forget: won't slow down the loop
    asyncio.create_task(log_ch.send(embed=embed))

@bot.event
async def on_guild_channel_delete(channel):
    if SERVER_LOG_CHANNEL_ID == 0: return
    log_ch = channel.guild.get_channel(SERVER_LOG_CHANNEL_ID)
    if not log_ch: return
    
    embed = discord.Embed(title="🗑️ Channel Deleted", description=f"**{channel.name}** was deleted.\nType: `{channel.type}`", color=discord.Color.red())
    asyncio.create_task(log_ch.send(embed=embed))

@bot.event
async def on_guild_role_create(role):
    if SERVER_LOG_CHANNEL_ID == 0: return
    log_ch = role.guild.get_channel(SERVER_LOG_CHANNEL_ID)
    if not log_ch: return
    
    embed = discord.Embed(title="🛡️ Role Created", description=f"**{role.name}** was created.\nID: `{role.id}`", color=discord.Color.green())
    asyncio.create_task(log_ch.send(embed=embed))

@bot.event
async def on_guild_role_delete(role):
    if SERVER_LOG_CHANNEL_ID == 0: return
    log_ch = role.guild.get_channel(SERVER_LOG_CHANNEL_ID)
    if not log_ch: return
    
    embed = discord.Embed(title="🔥 Role Deleted", description=f"**{role.name}** was deleted.\nID: `{role.id}`", color=discord.Color.red())
    asyncio.create_task(log_ch.send(embed=embed))
    
    # 🛑 GHOST LOCK PROTECTION: If a Lobby role is manually deleted, clear DB assignments
    if role.name.startswith('Lobby-'):
        lobby_name = role.name
        loop = asyncio.get_event_loop()
        # 1. Clear teams table
        asyncio.create_task(loop.run_in_executor(None, _clear_lobby_assignments, lobby_name))
        # 2. Clear registration_log (unlock tags)
        asyncio.create_task(loop.run_in_executor(None, _delete_log_by_lobby, lobby_name))
        print(f"🧼 Ghost Lock Cleared: Role {lobby_name} was deleted, database assignments purged.")

@bot.event
async def on_member_join(member: discord.Member):
    # Re-apply ban role if they try to evade it by leaving and rejoining
    loop = asyncio.get_event_loop()
    is_banned_db = await loop.run_in_executor(None, _is_banned, str(member.id))
    if is_banned_db:
        ban_role = discord.utils.get(member.guild.roles, id=BAN_ROLE_ID)
        if ban_role:
            try:
                await member.add_roles(ban_role, reason="Ban Evasion Prevention: Re-applied active ban")
            except Exception:
                pass

@bot.event
async def on_member_remove(member: discord.Member):
    # Prevent Ghost Teams: Automatically clear registration and release tags if the owner leaves
    user_id = str(member.id)
    loop = asyncio.get_event_loop()
    team_data = await loop.run_in_executor(None, _get_team, user_id)
    
    if team_data:
        # 🔒 HARDENED: Verify this user is the ACTUAL OWNER (creator_id)
        owner_id = str(team_data.get('creator_id', ''))
        if user_id != owner_id:
            # This user is not the owner (maybe a player or legacy data)
            return
            
        lobby_name = str(team_data.get('lobby', ''))
        
        # 1. Soft delete and release tags silently
        await loop.run_in_executor(None, _soft_delete_team, user_id)
        await loop.run_in_executor(None, _clear_log_protection_by_player, user_id)
        # 🟢 OPTIMIZED: Redundancy removed. Cleanup is now silent and efficient.


        # 4. Auto-fill the vacancy and update slotlist
        if lobby_name:
            await _fill_lobby_from_waitlist(member.guild, lobby_name)
            await _update_lobby_slotlist(member.guild, lobby_name)

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    # If a member LOSES a lobby role, we should clear their team from the sheet
    # to maintain sync and prevent tag lockouts.
    removed_roles = [r for r in before.roles if r not in after.roles and r.name.startswith('Lobby-')]
    still_has_lobby = any(r.name.startswith('Lobby-') for r in after.roles)
    
    if removed_roles and not still_has_lobby:
        # Check if they were an owner
        user_id = str(after.id)
        loop = asyncio.get_event_loop()
        team_data = await loop.run_in_executor(None, _get_team, user_id)
        
        if team_data:
            old_lobby = team_data.get('lobby', '')
            # 1. Clear the lobby assignment in teams table
            await loop.run_in_executor(None, _update_team_lobby, user_id, '', '', [], '')
            # 2. Delete registration_log entries (release tag protection)
            await loop.run_in_executor(None, _clear_log_protection_by_player, user_id)
            print(f"✔️ Role removed from {after.display_name}. Cleared lobby + log. Team data PRESERVED.")
            
            # 3. Auto-fill the vacancy and update slotlist
            if old_lobby and old_lobby != 'WAITING':
                await _fill_lobby_from_waitlist(after.guild, old_lobby)
                await _update_lobby_slotlist(after.guild, old_lobby)

# 🟢 SUPER-SCALABILITY: Per-user locks instead of global lock
# This allows 100+ teams to tag simultaneously without blocking each other
_user_tag_locks = {}
_user_locks_lock = asyncio.Lock()  # Protects the dict itself

async def _get_user_lock(user_id: str) -> asyncio.Lock:
    """Gets or creates a per-user lock for concurrent tagging."""
    async with _user_locks_lock:
        if user_id not in _user_tag_locks:
            _user_tag_locks[user_id] = asyncio.Lock()
        return _user_tag_locks[user_id]

async def _cleanup_user_lock(user_id: str):
    """Removes lock after use to prevent memory leak."""
    async with _user_locks_lock:
        _user_tag_locks.pop(user_id, None)

@bot.event
async def on_message(message: discord.Message):
    # Always process commands first
    await bot.process_commands(message)

    # Only handle messages in the confirmation channel from non-bots
    if message.author.bot:
        return
    if message.channel.id != CONFIRM_CHANNEL_ID:
        return
    
    user_id = str(message.author.id)
    user_lock = await _get_user_lock(user_id)
    async with user_lock:
        await _process_confirm_channel(message)
    await _cleanup_user_lock(user_id)

async def _process_confirm_channel(message: discord.Message):
    loop = asyncio.get_event_loop()
    user_id = str(message.author.id)
    if user_id not in awaiting_tag:
        # Check if the user is an admin bypassing this rule
        if not discord.utils.get(message.author.roles, id=BOT_ADMIN_ROLE_ID):
            try:
                await message.delete()
            except Exception:
                pass
            await message.channel.send(
                f"❌ {message.author.mention}, register your team first before tagging players here.", 
                delete_after=15
            )
        return  # This person isn't waiting to confirm a team

    team_name = awaiting_tag[user_id]["team_name"]
    mentions = list(set(message.mentions))  # Get unique mentions

    # Must tag exactly 4 unique players
    if len(mentions) != 4:
        current_tags = len(mentions)
        try:
            await message.add_reaction('❌')
        except Exception:
            pass
        await message.channel.send(
            f'⚠️ Tag exactly **4 unique players**. You tagged {current_tags}.',
            delete_after=30
        )
        return

    # All 4 must be members of the server (not bots)
    guild = message.guild
    invalid = [m for m in mentions if m.bot or guild.get_member(m.id) is None]
    if invalid:
        invalid_names = ', '.join(m.mention for m in invalid)
        try:
            await message.add_reaction('❌')
        except Exception:
            pass
        await message.channel.send(
            f'⚠️ Invalid members: {invalid_names}. Tag only active server members.',
            delete_after=30
        )
        return

    # All valid — react with tick
    try:
        await message.add_reaction('✅')
    except Exception:
        pass

    # 🟢 OPTIMIZED: Instead of fetching ALL logs, we use the indexed conflict check
    # Check for each tagged player
    for mention in mentions:
        conflict = await loop.run_in_executor(None, _check_tag_conflict, [mention.id])
        if conflict:
            offender_id, conflict_team, protected_lobby = conflict
            
            # Allow if it's the SAME team re-confirming (optional, but keep original strictness if needed)
            if conflict_team != team_name:
                try:
                    await message.add_reaction('❌')
                except Exception: pass
                await message.channel.send(
                    f'🚨 **Conflict Detected!**\n'
                    f'{mention.mention} is already assigned to **{conflict_team}** in **{protected_lobby}**.\n'
                    f'They cannot be in two teams at once.',
                    delete_after=30
                )
                return
    # Also fetch all_teams for retrieving IGNs and team data
    all_teams = await loop.run_in_executor(None, _get_all_teams)

    # Fetch locked lobbies to prevent joining published lists
    locked_str = str(await loop.run_in_executor(None, _get_config, 'locked_lobbies', ''))
    locked_list = [l.strip() for l in locked_str.split(',') if l.strip()]

    # Find all available Lobby roles with capacity < LOBBY_SIZE
    lobby_roles = [r for r in guild.roles if r.name.startswith('Lobby-')]
    available_roles = [r for r in lobby_roles if len(r.members) < LOBBY_SIZE and r.name not in locked_list]

    tagged_ids = [str(m.id) for m in mentions]

    # TAG CONFLICT CHECK: Ensure no player is already locked in another registration
    loop = asyncio.get_event_loop()
    tagged_ids_str = ",".join(tagged_ids)
    tagged_names_str = ",".join(m.display_name for m in mentions)
    player_tags = [m.mention for m in mentions]

    # Identifiers for logging
    team_data = next((t for t in all_teams if str(t.get('creator_id') or t.get('Discord ID')) == user_id), None)

    if not lobby_roles or not available_roles:
        # Add to waitlist instead of rejecting
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _update_team_lobby, user_id, "WAITING", tagged_ids_str, player_tags, tagged_names_str)
        
        if team_data:
            await loop.run_in_executor(None, _log_registration, team_data, tagged_ids, player_tags, "WAITING")
            
            # DYNAMIC RELOCK: Remove Tagger role after waitlisting
            try:
                tagger_role = discord.utils.get(message.guild.roles, name=TAGGER_ROLE_NAME)
                if tagger_role:
                    await message.author.remove_roles(tagger_role, reason="Tags processed (Waitlist)")
            except Exception: pass

        embed = discord.Embed(
            title="⏳ Waitlisted",
            description=(
                f"Team **{team_name}** — players confirmed.\n\n"
                f"All lobbies are currently full.\n"
                f"You'll be assigned automatically when a slot opens."
            ),
            color=discord.Color.orange()
        )
        embed.set_footer(text="Protection active — your spot is secured.")
        await message.channel.send(embed=embed, delete_after=60)
        
        awaiting_tag.pop(user_id, None)
        return

    # Assign a priority AVAILABLE lobby role (sequential filling)
    available_sorted = sorted(available_roles, key=lambda r: _get_lobby_num(r.name))
    assigned_role = available_sorted[0]

    # Increment and save sequential assignment ID
    loop = asyncio.get_event_loop()
    current_count = await loop.run_in_executor(None, _get_config, 'total_assignments', '0')
    
    try:
        new_count = int(current_count) + 1
    except (ValueError, TypeError):
        new_count = 1
    await loop.run_in_executor(None, _set_config, 'total_assignments', new_count)
    team_id = str(new_count)

    # Update Google Sheet with the assigned lobby, tagged IDs, and player tags
    await loop.run_in_executor(None, _update_team_lobby, user_id, assigned_role.name, tagged_ids_str, player_tags, tagged_names_str)

    # 3. Log the successful assignment
    if team_data:
        await loop.run_in_executor(None, _log_registration, team_data, tagged_ids, player_tags, assigned_role.name)

    # NEW: Send Team Assignment Notification Embed to ASSIGNMENT_CHANNEL_ID
    assign_ch = guild.get_channel(ASSIGNMENT_CHANNEL_ID)
    if assign_ch:
        embed = discord.Embed(
            title=f"{team_id}. Team {team_name}",
            color=discord.Color.green()
        )
        embed.add_field(name="Owner", value=message.author.mention, inline=False)
        embed.add_field(name="Tagged Players", value=' '.join(m.mention for m in mentions), inline=False)
        
        embed.set_footer(text="Once your group is created, you'll have the option to reschedule from there.")
        try:
            await assign_ch.send(embed=embed)
        except Exception as e:
            pass

    # Give the role ONLY to the owner who tagged the players
    try:
        await message.author.add_roles(assigned_role, reason=f'Tournament lobby assignment for team {team_name}')
    except discord.Forbidden:
        error_msg = f"❌ Permission Error: Please elevate bot role above target lobby roles."
        await message.channel.send(error_msg, delete_after=60)
    except Exception as e:
        error_msg = f"❌ **Error adding role**: {e}"
        await message.channel.send(error_msg, delete_after=30)

    player_mentions = ' '.join(m.mention for m in mentions)
    
    # DYNAMIC RELOCK: Remove Tagger role after assignment
    try:
        tagger_role = discord.utils.get(message.guild.roles, name=TAGGER_ROLE_NAME)
        if tagger_role:
            await message.author.remove_roles(tagger_role, reason="Tags processed (Lobby Assigned)")
    except Exception: pass

    # Lobby Assignment Embed
    embed = discord.Embed(
        title="✅ Lobby Assigned",
        description=(
            f"**{team_name}** — {message.author.mention}\n"
            f"Players: {player_mentions}\n\n"
            f"Lobby: **{assigned_role.name}**\n"
            f"You now have access to your lobby channel."
        ),
        color=discord.Color.green()
    )
    await message.channel.send(embed=embed, delete_after=30)

    # Remove from awaiting list
    awaiting_tag.pop(user_id, None)


@bot.command(name='ban_team')
@is_bot_admin()
async def ban_team(ctx, *args):
    """Direct Ban Command: !ban_team @user1 @user2 2h Reason text goes here"""
    try: await ctx.message.delete()
    except: pass

    if not args:
        await ctx.send("Usage: `!ban_team @owner @player (optional) <duration> <reason...>`\nExample: `!ban_team @Rahul 2h Slot Waste`", delete_after=15)
        return

    all_to_ban = []
    duration_str = None
    reason_words = []
    
    converter = commands.MemberConverter()
    
    idx = 0
    while idx < len(args):
        arg = args[idx]
        
        # Try to parse as Member
        try:
            member = await converter.convert(ctx, arg)
            all_to_ban.append(member)
            idx += 1
            continue
        except Exception:
            pass
            
        # Try raw ID parsing if converter fails
        raw_id = re.sub(r'[<@!>]', '', arg)
        if raw_id.isdigit():
            member = ctx.guild.get_member(int(raw_id))
            if member:
                all_to_ban.append(member)
                idx += 1
                continue
                
        # If not a member, check if it's the duration
        if parse_duration(arg) is not None:
            duration_str = arg
            reason_words = args[idx+1:]
            break
            
        # If we reach here, it's neither a valid member nor a valid duration.
        break

    if not all_to_ban:
        await ctx.send("❌ Could not find any valid members to ban. Make sure to @mention them or use their ID.", delete_after=15)
        return
        
    if not duration_str:
        await ctx.send("❌ You must provide a valid duration (e.g., `1h`, `2d`, `30m`). Make sure it comes after the tags.", delete_after=15)
        return
        
    reason = ' '.join(reason_words).strip()
    if not reason:
        await ctx.send("❌ You must provide a reason for the ban. Example: `!ban_team @user 1h abusive`", delete_after=15)
        return

    seconds = parse_duration(duration_str)
    if seconds > 432000:
        await ctx.send("❌ Max duration is 5 days.", delete_after=15)
        return

    unban_timestamp = time.time() + seconds
    ban_role = discord.utils.get(ctx.guild.roles, id=BAN_ROLE_ID)
    if not ban_role:
        await ctx.send("❌ Ban role not found.", delete_after=15)
        return

    # Owner is assumed to be the first person tagged
    owner = all_to_ban[0]
    loop = asyncio.get_event_loop()
    owner_team_name = await loop.run_in_executor(None, _find_team_for_user, str(owner.id))

    banned_count = 0
    for member in all_to_ban:
        try:
            await member.add_roles(ban_role, reason=f"Banned by {ctx.author} | {reason}")
            await loop.run_in_executor(
                None, _add_ban, str(member.id), unban_timestamp,
                reason, str(ctx.author.id), owner_team_name
            )
            banned_count += 1
        except Exception:
            pass

    # Build premium ban notification embed
    expires_ts = int(unban_timestamp)
    other_players = all_to_ban[1:]
    
    embed = discord.Embed(
        title="",
        description=(
            f"```ansi\n"
            f"\u001b[1;31m╔{'═' * 38}╗\n"
            f"\u001b[1;31m║  🚫 SCRIMS BAN ISSUED                ║\n"
            f"\u001b[1;31m╚{'═' * 38}╝\n```\n"
            f"🏆 **Team:** `{owner_team_name}`\n"
            f"👑 **Owner:** {owner.mention}\n"
            f"👥 **Players:** {', '.join(m.mention for m in other_players) if other_players else 'None'}\n\n"
            f"📝 **Reason:** {reason}\n"
            f"⏳ **Duration:** `{duration_str}`\n"
            f"📅 **Expires:** <t:{expires_ts}:F> (<t:{expires_ts}:R>)\n"
            f"👮 **Banned By:** {ctx.author.mention}\n\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"
        ),
        color=0xE74C3C
    )
    embed.set_footer(text="VirelApp • Moderation System")
    embed.timestamp = discord.utils.utcnow()

    # Send to ban log channel
    log_ch_id = BAN_LOG_CHANNEL_ID if BAN_LOG_CHANNEL_ID != 0 else AUDIT_LOG_CHANNEL_ID
    log_ch = ctx.guild.get_channel(log_ch_id)
    if log_ch:
        try:
            await log_ch.send(embed=embed)
        except Exception:
            pass

    # Confirm to admin
    confirm_embed = discord.Embed(
        title="🚫 Ban Executed",
        description=(
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"👤 **Banned:** `{banned_count}` users\n"
            f"⏳ **Duration:** `{duration_str}`\n"
            f"📝 **Reason:** {reason}\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"
        ),
        color=0xE74C3C
    )

@bot.command(name='unban_team')
@is_bot_admin()
async def unban_team(ctx, *members: discord.Member):
    """Manually unbans multiple users and removes the ban role."""
    if not members:
        await ctx.send('Usage: `!unban_team @user1 @user2`', delete_after=15)
        return

    role = discord.utils.get(ctx.guild.roles, id=BAN_ROLE_ID)
    unbanned_count = 0
    loop = asyncio.get_event_loop()

    for member in members:
        if role and role in member.roles:
            try:
                await member.remove_roles(role, reason=f"Unbanned by {ctx.author}")
            except Exception:
                pass
                
        success = await loop.run_in_executor(None, _remove_ban, str(member.id))
        if success:
            unbanned_count += 1
    
    embed = discord.Embed(
        title="✅ Users Unbanned",
        description=(
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
            f"🔓 **Unbanned:** `{unbanned_count}` users\n"
            f"▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"
        ),
        color=0x2ECC71
    )
    embed.set_footer(text="VirelApp • Moderation System")
    embed.timestamp = discord.utils.utcnow()
    await ctx.send(embed=embed, delete_after=15)

    try: await ctx.message.delete()
    except: pass

# ─── idp panel ──────────────────────────────────────────────────────────────────────
class IdPassModal(discord.ui.Modal, title='Distribute Match Details'):
    match_num = discord.ui.TextInput(label='Match Number / Title', placeholder='e.g., Match 1', required=True, max_length=50)
    map_name = discord.ui.TextInput(label='Map', placeholder='e.g., Erangel or Miramar', required=True, max_length=50)
    room_id = discord.ui.TextInput(label='Room ID', placeholder='e.g., 523412', required=True, max_length=50)
    room_pass = discord.ui.TextInput(label='Password', placeholder='e.g., Sq123', required=True, max_length=50)

    def __init__(self, target_lobby_name: str):
        super().__init__()
        self.target_lobby_name = target_lobby_name

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        
        channel_name = self.target_lobby_name.lower()
        target_channel = discord.utils.get(guild.channels, name=channel_name)
        lobby_role = discord.utils.get(guild.roles, name=self.target_lobby_name)
        
        if not target_channel or not lobby_role:
            fup = await interaction.followup.send(f"❌ Channel or role not found for {self.target_lobby_name}.", ephemeral=True)
            asyncio.create_task(schedule_delete_fup(fup))
            return
            
        lobby_name_upper = self.target_lobby_name.upper()
        match_title = self.match_num.value.upper()
        map_title = self.map_name.value.upper()
        
        start_time = discord.utils.utcnow() + datetime.timedelta(minutes=8)
        start_timestamp = int(start_time.timestamp())
        
        msg_content = (
            f"**{lobby_name_upper}**\n"
            f"**MATCH - {match_title}**\n"
            f"**MAP- {map_title}**\n\n"
            f"**ID - `{self.room_id.value}`**\n"
            f"**PASS- `{self.room_pass.value}`**\n"
            f"**START AT - <t:{start_timestamp}:t>**\n\n"
            f">> **NO DELAY AVAILABLE!!**\n\n"
            f">> **SLOT WASTE AND USE OF ABUSIVE LANGUAGE IN ROOMCHAT/ANYWHERE = SCRIMS BAN !!**\n\n"
            f">> **JOIN ROOM MAKER VC FOR A KICK REQUEST!!**\n\n"
            f">> **AFTER START TIME MANAGEMENT WILL NOT BE RESPONSIBLE FOR ANY DISPUTES!!**\n"
            f"{lobby_role.mention}"
        )
        
        try:
            await target_channel.send(msg_content)
            fup = await interaction.followup.send(f"✅ Posted to #{channel_name}.", ephemeral=True)
        except Exception as e:
            fup = await interaction.followup.send(f"❌ Failed to send: {e}", ephemeral=True)
            
        asyncio.create_task(schedule_delete_fup(fup))

class IdPassSelect(discord.ui.Select):
    def __init__(self, lobby_names):
        options = [discord.SelectOption(label=name, value=name, description=f'Distribute IP/Pass to {name}') for name in lobby_names]
        super().__init__(placeholder='Select the target lobby...', min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        selected_lobby = self.values[0]
        await interaction.response.send_modal(IdPassModal(selected_lobby))

class IdPassSelectView(discord.ui.View):
    def __init__(self, lobby_names):
        super().__init__(timeout=180) # Timeout after 3 mins
        self.add_item(IdPassSelect(lobby_names))

class IdPassLauncherView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None) # Persistent
        
    @discord.ui.button(label='🚀 Send ID/Password', style=discord.ButtonStyle.primary, custom_id='idpass_launcher_btn')
    async def launch_idpass(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not _has_staff_role(interaction.user):
            await interaction.response.send_message("❌ Admin or Mod access required.", ephemeral=True)
            return
            
        lobby_roles = [r for r in interaction.guild.roles if r.name.startswith('Lobby-')]
        
        def extract_num(role_name):
            try: return int(role_name.split('-')[-1])
            except: return 999
            
        lobby_roles.sort(key=lambda r: extract_num(r.name))
        
        if not lobby_roles:
            await interaction.response.send_message("❌ No lobbies found in the server.", ephemeral=True)
            return
            
        lobby_names = [r.name for r in lobby_roles[:25]]
        
        await interaction.response.send_message(
            "**Select which lobby you want to distribute the Room details to:**",
            view=IdPassSelectView(lobby_names),
            ephemeral=True
        )

async def _send_id_panel(ctx_or_interaction):
    """Internal helper to send the ID/Password control panel."""
    embed = discord.Embed(
        title="🛠️ MATCH ID & PASSWORD CONTROL PANEL",
        description=(
            "Select the lobby and fill in Room ID and Password.\n"
            "Click the button below to get started."
        ),
        color=0x2b2d31
    )
    if isinstance(ctx_or_interaction, commands.Context):
        await ctx_or_interaction.send(embed=embed, view=IdPassLauncherView())
    else:
        # 🟢 OPTIMIZED: Smart response
        if not ctx_or_interaction.response.is_done():
            await ctx_or_interaction.response.send_message(embed=embed, view=IdPassLauncherView(), ephemeral=True)
        else:
            await ctx_or_interaction.followup.send(embed=embed, view=IdPassLauncherView(), ephemeral=True)

@bot.command(name='id_panel')
@is_bot_admin()
async def id_panel_cmd(ctx):
    """Admin/Mod command to post the ID/Password control panel."""
    await _send_id_panel(ctx)
    try: await ctx.message.delete()
    except: pass

@tasks.loop(seconds=60)
async def check_expiring_bans():
    """Optimized Unban Loop: Checks DB once per cycle."""
    try:
        loop = asyncio.get_event_loop()
        bans_data = await loop.run_in_executor(None, _get_all_bans)
        now = time.time()
        
        # 💡 Filter expiring bans first to save API calls
        expiring = [row for row in bans_data if float(row.get('unban_timestamp', 0)) <= now]
        if not expiring:
            return

        for row in expiring:
            user_id = str(row.get('user_id'))
            
            # Remove from all guilds
            for guild in bot.guilds:
                role = discord.utils.get(guild.roles, id=BAN_ROLE_ID)
                if not role: continue
                member = guild.get_member(int(user_id))
                if member and role in member.roles:
                    try:
                        await member.remove_roles(role, reason="Auto-unban (Duration Expired)")
                        print(f"♻️ Unbanned {user_id} in {guild.name}")
                    except Exception: pass
            
            # Clean DB
            await loop.run_in_executor(None, _remove_ban, user_id)
    except Exception as e:
        print(f"Error in check_expiring_bans: {e}")


@tasks.loop(minutes=5.0)
async def registration_timeout_task():
    """Wipes STALE registrations that didn't confirm (tag roster) within 5 minutes."""
    try:
        now = time.time()
        loop = asyncio.get_event_loop()
        all_teams = await loop.run_in_executor(None, _get_all_teams)
        
        for team in all_teams:
            # 💡 TARGET: Only teams NOT in a lobby
            if not team.get('lobby') or team.get('lobby') == 'WAITING':
                
                c_at = team.get('created_at', '0')
                try:
                    # Handle legacy string dates vs new floats
                    if '-' in str(c_at):
                        import datetime
                        dt = datetime.datetime.strptime(str(c_at), '%Y-%m-%d %H:%M:%S')
                        created_time = dt.timestamp()
                    else:
                        created_time = float(c_at)
                except:
                    created_time = 0
                
                # Check 5-minute threshold (300s)
                if created_time > 0 and (now - created_time) > 300:
                    team_name = team.get('team_name', 'Unknown')
                    user_id = team.get('creator_id')
                    
                    # 🔍 SMART CHECK: Is this a 'Complete' profile or an 'Incomplete' one?
                    is_complete = bool(team.get('player4'))
                    
                    if not is_complete:
                        # 🗑️ Scenario A: Brand new registration that never finished Step 3.
                        # We PURGE entirely to keep the DB fresh.
                        await loop.run_in_executor(None, _soft_delete_team, user_id)
                    else:
                        # 🛡️ Scenario B: Established team using 'Old Profile' but failed to tag.
                        # We only clear the SESSION locks, but KEEP the profile data.
                        await loop.run_in_executor(None, _update_team_lobby, user_id, "")
                    
                    # 📝 ALWAYS clear Tag protection locks and runtime state
                    await loop.run_in_executor(None, _clear_log_protection_by_player, user_id)
                    awaiting_tag.pop(str(user_id), None)
    except Exception as e:
        pass # Background loop errors silenced to prevent loop noise

@registration_timeout_task.before_loop
async def before_registration_timeout_task():
    await bot.wait_until_ready()

@check_expiring_bans.before_loop
async def before_check_expiring_bans():
    await bot.wait_until_ready()




# ─── MIRRORING ENGINE ────────────────────────────────────────────────────────

# 🟢 BLOCK 5: MIRRORING ENGINE (Smart-Sync Google Sheets) 🟢
#
# Architecture:
#   - DB writers flip per-table flags via _mark_dirty(...).
#   - drain_dirty_sync (15s) pushes only flagged tables to Sheets — fast feedback.
#   - mirror_to_sheets (30m) is the safety reconciler — pushes everything in case
#     a flag was missed (e.g. a writer added later that forgets to flag).
# Both use _sheet_retry to absorb 429 rate limits without losing data.

async def _sheet_retry(func, *args, max_retries=4, **kwargs):
    """Runs a blocking gspread call in a thread; retries on 429 with exponential backoff.
    Returns the result on success, or re-raises after max_retries."""
    loop = asyncio.get_event_loop()
    last_err = None
    for attempt in range(max_retries):
        try:
            return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))
        except gspread.exceptions.APIError as e:
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            if status == 429:
                wait = 2 ** attempt
                print(f"⚠️ Sheets 429: retrying in {wait}s ({attempt+1}/{max_retries})")
                await asyncio.sleep(wait)
                last_err = e
                continue
            raise
    if last_err:
        raise last_err

async def _replace_sheet(ws, headers, rows, chunk_size=300):
    """Atomically clears a worksheet and rewrites headers + rows.
    Always writes the header row, even when rows is empty — this fixes the
    'stale rows when table is empty' bug in the previous mirror loop."""
    if ws is None:
        return
    await _sheet_retry(ws.clear)
    await _sheet_retry(ws.update, range_name='A1', values=[headers])
    if not rows:
        return
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        await _sheet_retry(ws.update, range_name=f'A{i+2}', values=chunk)
        await asyncio.sleep(1)  # Breathe for API quota & heartbeat

# ── PER-TABLE PREP + PUSH HELPERS ────────────────────────────────────────────
def _prep_teams_rows(teams):
    rows = []
    for t in teams:
        def _s(v): return f"'{v}" if str(v).startswith('=') else str(v)
        c_at = str(t.get('created_at', ''))
        if c_at.replace('.', '').isdigit() and '.' in c_at:
            try:
                c_at = datetime.datetime.fromtimestamp(float(c_at)).strftime('%Y-%m-%d %H:%M:%S')
            except Exception: pass
        rows.append([
            _s(c_at), _s(t['creator_id']), _s(t['team_name']),
            _s(t['owner_name']), _s(t['owner_email']), _s(t['contact_number']),
            _s(t['player1']), _s(t['player2']), _s(t['player3']), _s(t['player4']),
            _s(t['sub1']), _s(t['sub2']), _s(t['insta_handles']), _s(t['lobby']),
            _s(t.get('tagged_names', '')), _s(t['tagged_ids']), _s(t['p1_tag']),
            _s(t['p2_tag']), _s(t['p3_tag']), _s(t['p4_tag'])
        ])
    return rows

def _prep_logs_rows(logs):
    return [[f"'{l['discord_id']}", l['team_name'], l['p1_ign'], l['p2_ign'], l['p3_ign'], l['p4_ign'],
             l['p1_tag'], l['p2_tag'], l['p3_tag'], l['p4_tag'], l['lobby_assigned'], l['lobby_protection']] for l in logs]

def _prep_bans_rows(bans):
    rows = []
    for b in bans:
        unban_ts = b['unban_timestamp']
        try:
            unban_str = datetime.datetime.fromtimestamp(float(unban_ts)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            unban_str = str(unban_ts)
        rows.append([b['user_id'], unban_str])
    return rows

def _prep_deleted_rows(del_teams):
    return [[dt['discord_id'], dt['team_name'], dt['owner_name'], dt['owner_email'], dt['contact'],
             dt['player1'], dt['player2'], dt['player3'], dt['player4'], dt['sub1'], dt['sub2'],
             dt['ig_handles'], dt['lobby'], dt['tagged_ids'], dt['p1_tag'], dt['p2_tag'],
             dt['p3_tag'], dt['p4_tag'], dt.get('deleted_at', '')] for dt in del_teams]

def _read_config_rows():
    with _get_db_conn() as conn:
        return [[r['key'], r['value']] for r in conn.execute("SELECT key, value FROM config").fetchall()]

async def _push_table(name):
    """Pushes a single table to its Google Sheet. Catches & logs errors."""
    loop = asyncio.get_event_loop()
    try:
        if name == 'teams' and teams_ws is not None:
            data = await loop.run_in_executor(None, _get_all_teams)
            await _replace_sheet(teams_ws, TEAMS_HEADERS, await loop.run_in_executor(None, _prep_teams_rows, data))
        elif name == 'logs' and log_ws is not None:
            data = await loop.run_in_executor(None, _get_all_logs)
            await _replace_sheet(log_ws, LOG_HEADERS, await loop.run_in_executor(None, _prep_logs_rows, data))
        elif name == 'bans' and bans_ws is not None:
            data = await loop.run_in_executor(None, _get_all_bans)
            await _replace_sheet(bans_ws, BANS_HEADERS, await loop.run_in_executor(None, _prep_bans_rows, data))
        elif name == 'deleted' and deleted_teams_ws is not None:
            data = await loop.run_in_executor(None, _get_all_deleted_teams)
            await _replace_sheet(deleted_teams_ws, DELETED_TEAMS_HEADERS, await loop.run_in_executor(None, _prep_deleted_rows, data))
        elif name == 'config' and config_ws is not None:
            await _replace_sheet(config_ws, CONFIG_HEADERS, await loop.run_in_executor(None, _read_config_rows))
        else:
            return False
        return True
    except Exception as e:
        # Re-flag so the next cycle retries the failed table
        _mark_dirty(name)
        print(f"❌ Push '{name}' failed: {e}")
        return False

# ── 15-SECOND DRAINER (event-driven sync) ────────────────────────────────────
@tasks.loop(seconds=15.0)
async def drain_dirty_sync():
    """Pushes only the tables flagged dirty since the last drain. Idempotent."""
    global _drain_running
    if _drain_running:
        return
    if not all([teams_ws, log_ws, bans_ws, config_ws]):
        return  # Sheets not ready yet
    snap = _consume_dirty()
    pending = [t for t, dirty in snap.items() if dirty]
    if not pending:
        return
    _drain_running = True
    try:
        for t in pending:
            await _push_table(t)
            await asyncio.sleep(0.5)  # Inter-table breather
    finally:
        _drain_running = False

@drain_dirty_sync.before_loop
async def before_drain_dirty_sync():
    await bot.wait_until_ready()

# ── MANUAL FULL PUSH (used only by !sync_now) ────────────────────────────────
# No periodic schedule — Sheets are touched only when DB actually changes
# (via drain_dirty_sync) or when an admin explicitly forces a full push.
async def mirror_to_sheets():
    """Pushes every table to Sheets right now. Invoked manually by !sync_now."""
    global is_mirroring
    if is_mirroring:
        return
    is_mirroring = True
    try:
        if not all([teams_ws, log_ws, bans_ws, config_ws]):
            print("⚠️ Manual push skipped: Sheets not ready.")
            return
        for t in ('teams', 'logs', 'bans', 'deleted', 'config'):
            await _push_table(t)
            await asyncio.sleep(1)
        _consume_dirty()
    except Exception as e:
        print(f"❌ Manual push error: {e}")
    finally:
        is_mirroring = False

# ─── ADMIN COMMANDS ──────────────────────────────────────────────────────────
@bot.command(name='ac')
@is_admin_only()
async def post_admin_center(ctx):
    """Posts the Admin Control Center Dashboard."""
    if ctx.channel.id != ADMIN_DASHBOARD_CHANNEL_ID:
        await ctx.send(f"❌ This command can only be used in <#{ADMIN_DASHBOARD_CHANNEL_ID}>.", delete_after=10)
        try: await ctx.message.delete()
        except: pass
        return

    view = AdminControlCenterView()
    embed = await view._get_stats_embed(ctx.guild)
    await ctx.send(embed=embed, view=view)
    try: await ctx.message.delete()
    except: pass

@bot.command()
@is_admin_only()
async def sync(ctx):
    """Manually triggers the mirroring process."""
    await ctx.send("Triggering Manual Synchronization...")
    await mirror_to_sheets()
    await ctx.send("✅ Manual Synchronization Completed.")

class WipeDataConfirmationView(discord.ui.View):
    def __init__(self, owner):
        super().__init__(timeout=30)
        self.owner = owner

    @discord.ui.button(label="☣️ CONFIRM WIPE", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user != self.owner:
            await interaction.response.send_message("❌ Not your session.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        try:
            loop = asyncio.get_event_loop()
            def _wipe_all():
                with _get_db_conn() as conn:
                    conn.execute("DELETE FROM teams")
                    conn.execute("DELETE FROM deleted_teams")
                    conn.execute("DELETE FROM registration_log")
                    conn.commit()
            
            await loop.run_in_executor(None, _wipe_all)
            
            # Reset all runtime states
            global awaiting_tag
            awaiting_tag = {}
            
            # Immediately mirror to sheets to clear them too
            await mirror_to_sheets()
            
            await interaction.followup.send("💥 **NUCLEAR WIPE COMPLETE**: All Teams, Archives, and Logs have been erased.", ephemeral=True)
            
            # Log this major event
            audit_ch = interaction.guild.get_channel(AUDIT_LOG_CHANNEL_ID)
            if audit_ch:
                embed = discord.Embed(
                    title="🚀 SYSTEM HARD RESET", 
                    description=f"All data tables (Teams, Deleted Teams, Logs) have been wiped manually by Super Admin {interaction.user.mention}.", 
                    color=discord.Color.red(),
                    timestamp=discord.utils.utcnow()
                )
                await audit_ch.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"❌ Wipe failed: {e}", ephemeral=True)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Wipe cancelled.", ephemeral=True)
        self.stop()

@bot.command(name='wipe_data')
@is_super_admin()
async def wipe_data_cmd(ctx):
    """[SUPER ADMIN ONLY] Erases all registration data from DB and Sheets."""
    view = WipeDataConfirmationView(ctx.author)
    await ctx.send("🚨 **WARNING**: You are about to erase **ALL** registration data, archives, and logs permanently. This cannot be undone.\nConfirm below:", view=view)

bot.run(os.getenv('BOT_TOKEN'))
