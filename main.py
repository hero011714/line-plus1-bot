import os
import re
import psycopg2
import asyncio
import time
import psutil
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, BackgroundTasks
from linebot import LineBotApi, WebhookHandler
from linebot.models import MessageEvent, TextMessage, TextSendMessage, JoinEvent
from linebot.exceptions import InvalidSignatureError

CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
ADMIN_ID = os.getenv("ADMIN_ID", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)
_bot_user_id = None

def get_bot_user_id():
    global _bot_user_id
    if _bot_user_id:
        return _bot_user_id
    try:
        import requests as _requests
        resp = _requests.get(
            "https://api.line.me/v2/profile",
            headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"},
            timeout=5
        )
        if resp.status_code == 200:
            _bot_user_id = resp.json().get("userId")
            print(f"[BOT_ID] Cached: {_bot_user_id}")
        else:
            print(f"[BOT_ID] Failed: {resp.status_code}")
    except Exception as e:
        print(f"[BOT_ID] Error: {e}")
    return _bot_user_id

app = FastAPI()

conn = None

def get_db():
    global conn
    if not conn or conn.closed:
        try:
            conn = psycopg2.connect(DATABASE_URL, sslmode='require')
            conn.autocommit = True
            print("Database connected successfully")
        except Exception as e:
            print(f"Database connection failed: {e}")
            return None
    else:
        try:
            conn.reset()
        except:
            pass
    return conn

def get_cursor():
    c = get_db()
    if c:
        return c.cursor()
    return None

def init_tables():
    cur = get_cursor()
    if not cur:
        return False
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                name TEXT DEFAULT '',
                count INTEGER DEFAULT 0,
                last_fetch INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, group_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS config (
                group_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                PRIMARY KEY (group_id, key)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signups (
                user_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                name TEXT DEFAULT '',
                count INTEGER DEFAULT 1,
                signup_time INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, group_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                group_id TEXT PRIMARY KEY,
                started_at INTEGER DEFAULT 0,
                expires_at INTEGER DEFAULT 0,
                total_count INTEGER DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS yearly_members (
                user_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                name TEXT DEFAULT '',
                PRIMARY KEY (user_id, group_id)
            )
        """)
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='events' AND column_name='total_count'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE events ADD COLUMN total_count INTEGER DEFAULT 0")
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='signups' AND column_name='expires_at'
        """)
        if cur.fetchone():
            cur.execute("ALTER TABLE signups DROP COLUMN expires_at")
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='signups' AND column_name='count'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE signups ADD COLUMN count INTEGER DEFAULT 1")
        cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'price', '50') ON CONFLICT DO NOTHING")
        cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'max_per_action', '10') ON CONFLICT DO NOTHING")
        cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'fetch_interval', '86400') ON CONFLICT DO NOTHING")
        cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'signup_limit', '12') ON CONFLICT DO NOTHING")
        print("Tables initialized successfully")
        return True
    except Exception as e:
        print(f"Init tables error: {e}")
        return False

init_tables()
get_bot_user_id()  # Cache bot user_id at startup

def get_group_id(event):
    source_type = event.source.type
    if source_type == 'group':
        return event.source.group_id
    elif source_type == 'room':
        return event.source.room_id
    else:
        return 'private_' + event.source.user_id

def clear_group_data(group_id):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("DELETE FROM users WHERE group_id=%s", (group_id,))
        cur.execute("DELETE FROM signups WHERE group_id=%s", (group_id,))
        cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))
        cur.execute("DELETE FROM config WHERE group_id=%s", (group_id,))
    except:
        pass

def get_price(group_id):
    cur = get_cursor()
    if not cur:
        return 50
    try:
        cur.execute("SELECT value FROM config WHERE group_id=%s AND key='price'", (group_id,))
        result = cur.fetchone()
        if result:
            return int(result[0])
        cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, 'price', '50')", (group_id,))
        return 50
    except:
        return 50

def get_max_per_action():
    cur = get_cursor()
    if not cur:
        return 10
    try:
        cur.execute("SELECT value FROM config WHERE group_id='default' AND key='max_per_action'")
        result = cur.fetchone()
        return int(result[0]) if result else 10
    except:
        return 10

def get_event_duration():
    cur = get_cursor()
    if not cur:
        return 30
    try:
        cur.execute("SELECT value FROM config WHERE group_id='default' AND key='event_duration'")
        result = cur.fetchone()
        if result:
            return int(result[0])
        cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'event_duration', '30')")
        return 30
    except:
        return 30

def set_price(group_id, price):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, 'price', %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, str(price), str(price)))
    except:
        pass

def add_user(user_id, group_id, name=""):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("""
            INSERT INTO users (user_id, group_id, name, count, last_fetch)
            VALUES (%s, %s, %s, 0, 0)
            ON CONFLICT (user_id, group_id) 
            DO UPDATE SET name = EXCLUDED.name
        """, (user_id, group_id, name))
    except:
        pass

def update_user_name(user_id, group_id, name):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("UPDATE users SET name=%s, last_fetch=%s WHERE user_id=%s AND group_id=%s", (name, int(time.time()), user_id, group_id))
    except:
        pass

def should_fetch_profile(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return True
    try:
        cur.execute("SELECT last_fetch FROM users WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        result = cur.fetchone()
        if not result or result[0] == 0:
            return True
        cur.execute("SELECT value FROM config WHERE group_id='default' AND key='fetch_interval'")
        result2 = cur.fetchone()
        interval = int(result2[0]) if result2 else 86400
        return (int(time.time()) - result[0]) > interval
    except:
        return True

def get_user_name(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return user_id[-4:]
    try:
        cur.execute("SELECT name FROM users WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        result = cur.fetchone()
        if result and result[0]:
            return result[0]
        return user_id[-4:]
    except:
        return user_id[-4:]

def fetch_group_member_name(user_id, group_id):
    """Try to fetch user's display name from group member API (works without being friends)."""
    if not group_id or group_id.startswith('private_'):
        print(f"[NAME] Skip: private chat or no group_id (uid={user_id[-4:]})")
        return None
    try:
        profile = line_bot_api.get_group_member_profile(group_id, user_id)
        if profile and profile.display_name:
            print(f"[NAME] OK: {user_id[-4:]} -> {profile.display_name}")
            return profile.display_name
        else:
            print(f"[NAME] Empty profile for {user_id[-4:]}")
    except Exception as e:
        print(f"[NAME] Error: {user_id[-4:]} -> {e}")
    return None

def add_count(user_id, group_id, n=1, name=""):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("""
            INSERT INTO users (user_id, group_id, name, count, last_fetch)
            VALUES (%s, %s, %s, %s, 0)
            ON CONFLICT (user_id, group_id) 
            DO UPDATE SET count = users.count + EXCLUDED.count
        """, (user_id, group_id, name, n))
        cur.execute("""
            UPDATE signups SET count = count + %s WHERE user_id = %s AND group_id = %s
        """, (n, user_id, group_id))
    except:
        pass

def get_count(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return 0
    try:
        cur.execute("SELECT count FROM users WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        result = cur.fetchone()
        return result[0] if result else 0
    except:
        return 0

def get_signup_count_for_user(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return 0
    try:
        cur.execute("SELECT count FROM signups WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        result = cur.fetchone()
        return result[0] if result else 0
    except:
        return 0

def clear_user(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("UPDATE users SET count=0 WHERE user_id=%s AND group_id=%s", (user_id, group_id))
    except:
        pass

def clear_all_users(group_id):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("UPDATE users SET count=0 WHERE group_id=%s", (group_id,))
        cur.execute("UPDATE events SET total_count=0 WHERE group_id=%s", (group_id,))
    except:
        pass

def get_all_users(group_id):
    cur = get_cursor()
    if not cur:
        return []
    try:
        cur.execute("SELECT user_id, name, count FROM users WHERE group_id=%s", (group_id,))
        return cur.fetchall()
    except:
        return []

def get_user_by_name(name, group_id):
    cur = get_cursor()
    if not cur:
        return None
    try:
        cur.execute("SELECT user_id FROM users WHERE name=%s AND group_id=%s", (name, group_id))
        result = cur.fetchone()
        if result:
            return result[0]
        return None
    except:
        return None

def get_group_stats(group_id):
    cur = get_cursor()
    if not cur:
        return 0
    try:
        cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s", (group_id,))
        user_count = cur.fetchone()[0]
        return user_count
    except:
        return 0

def get_signup_limit(group_id):
    cur = get_cursor()
    if not cur:
        return 12
    try:
        cur.execute("SELECT value FROM config WHERE group_id='default' AND key='signup_limit'")
        result = cur.fetchone()
        return int(result[0]) if result else 12
    except:
        return 12

def is_event_active(group_id):
    cur = get_cursor()
    if not cur:
        return False
    try:
        now = int(time.time())
        cur.execute("SELECT expires_at FROM events WHERE group_id=%s", (group_id,))
        result = cur.fetchone()
        return result is not None and result[0] > now
    except:
        return False

def get_event_remaining_hours(group_id):
    cur = get_cursor()
    if not cur:
        return 0
    try:
        now = int(time.time())
        cur.execute("SELECT expires_at FROM events WHERE group_id=%s", (group_id,))
        result = cur.fetchone()
        if result and result[0] > now:
            return (result[0] - now) // 3600
        return 0
    except:
        return 0

def coach_open_event(user_id, group_id, user_name, auto_opened=False):
    conn_local = get_db()
    if not conn_local:
        return 0
    cur = conn_local.cursor()
    try:
        now = int(time.time())
        expires = now + get_event_duration() * 3600
        cur.execute("DELETE FROM signups WHERE group_id=%s", (group_id,))
        cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))
        cur.execute("""
            INSERT INTO events (group_id, started_at, expires_at, total_count)
            VALUES (%s, %s, %s, 1)
        """, (group_id, now, expires))
        cur.execute("""
            INSERT INTO signups (user_id, group_id, name, count, signup_time)
            VALUES (%s, %s, %s, 1, %s)
        """, (user_id, group_id, user_name, now))
        cur.execute("""
            INSERT INTO users (user_id, group_id, name, count, last_fetch)
            VALUES (%s, %s, %s, 1, 0)
            ON CONFLICT (user_id, group_id)
            DO UPDATE SET count = users.count + 1, name = EXCLUDED.name
        """, (user_id, group_id, user_name))
        if auto_opened:
            cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, 'auto_opened', '1') ON CONFLICT (group_id, key) DO UPDATE SET value = '1'", (group_id,))
        else:
            cur.execute("DELETE FROM config WHERE group_id=%s AND key='auto_opened'", (group_id,))
        cur.execute("SELECT total_count FROM events WHERE group_id=%s", (group_id,))
        result = cur.fetchone()
        return result[0] if result else 1
    except Exception as e:
        print(f"coach_open_event error: {e}")
        return 1

def is_event_auto_opened(group_id):
    cur = get_cursor()
    if not cur:
        return False
    try:
        cur.execute("SELECT value FROM config WHERE group_id=%s AND key='auto_opened'", (group_id,))
        result = cur.fetchone()
        return result and result[0] == '1'
    except:
        return False

def should_allow_signup(user_id, group_id):
    if not is_event_auto_opened(group_id):
        return True
    if is_yearly_member(user_id, group_id):
        return True
    if get_zero_play_open_triggered(group_id):
        return True
    
    days_str = get_zero_play_open_config(group_id, 'zero_play_open_days')
    time_str = get_zero_play_open_config(group_id, 'zero_play_open_time')
    
    if days_str and time_str:
        taiwan_tz = timezone(timedelta(hours=8))
        now = datetime.now(taiwan_tz)
        current_weekday = now.weekday() + 1
        current_hour = now.hour
        current_minute = now.minute
        
        scheduled_days = [int(d.strip()) for d in days_str.split(',') if d.strip().isdigit()]
        if current_weekday in scheduled_days:
            time_parts = time_str.split(':')
            target_hour = int(time_parts[0])
            target_minute = int(time_parts[1])
            
            if current_hour > target_hour or (current_hour == target_hour and current_minute >= target_minute):
                return True
            else:
                remaining_mins = target_minute - current_minute
                if current_hour < target_hour:
                    remaining_mins = (target_hour - current_hour - 1) * 60 + (60 - current_minute) + target_minute
                return False, remaining_mins
    
    return True

def add_total_count(group_id, n):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("""
            UPDATE events SET total_count = total_count + %s WHERE group_id = %s
        """, (n, group_id))
    except:
        pass

def get_total_count(group_id):
    cur = get_cursor()
    if not cur:
        return 0
    try:
        cur.execute("SELECT COALESCE(SUM(count), 0) FROM signups WHERE group_id=%s", (group_id,))
        result = cur.fetchone()
        return result[0] if result else 0
    except:
        return 0

def clear_signups(group_id):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("DELETE FROM signups WHERE group_id=%s", (group_id,))
    except:
        pass

def get_signup_count(group_id):
    cur = get_cursor()
    if not cur:
        return 0
    try:
        cur.execute("SELECT COUNT(*) FROM signups WHERE group_id=%s", (group_id,))
        return cur.fetchone()[0]
    except:
        return 0

def is_signed_up(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return False
    try:
        cur.execute("SELECT 1 FROM signups WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        return cur.fetchone() is not None
    except:
        return False

def is_yearly_member(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return False
    try:
        cur.execute("SELECT 1 FROM yearly_members WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        return cur.fetchone() is not None
    except:
        return False

def add_yearly_member(user_id, group_id, name=""):
    cur = get_cursor()
    if not cur:
        return False
    try:
        cur.execute("INSERT INTO yearly_members (user_id, group_id, name) VALUES (%s, %s, %s) ON CONFLICT (user_id, group_id) DO UPDATE SET name = %s", (user_id, group_id, name, name))
        return True
    except:
        return False

def remove_yearly_member(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return False
    try:
        cur.execute("DELETE FROM yearly_members WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        return True
    except:
        return False

def get_yearly_members(group_id):
    cur = get_cursor()
    if not cur:
        return []
    try:
        cur.execute("SELECT user_id, name FROM yearly_members WHERE group_id=%s", (group_id,))
        return cur.fetchall()
    except:
        return []

def get_auto_open_config(group_id, key):
    cur = get_cursor()
    if not cur:
        return None
    try:
        cur.execute("SELECT value FROM config WHERE group_id=%s AND key=%s", (group_id, key))
        result = cur.fetchone()
        return result[0] if result else None
    except:
        return None

def set_auto_open_config(group_id, key, value):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, %s, %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, key, value, value))
    except:
        pass

def get_groups_with_auto_open():
    cur = get_cursor()
    if not cur:
        return []
    try:
        cur.execute("SELECT DISTINCT group_id FROM config WHERE key = 'auto_open_days'")
        return [row[0] for row in cur.fetchall()]
    except:
        return []

def should_auto_open(group_id):
    taiwan_tz = timezone(timedelta(hours=8))
    now = datetime.now(taiwan_tz)
    current_weekday = now.weekday() + 1
    
    days_str = get_auto_open_config(group_id, 'auto_open_days')
    print(f"[AUTO_OPEN] should_auto_open - group: {group_id[:8] if group_id else 'none'}..., days: {days_str}")
    if not days_str:
        return False
    
    scheduled_days = [int(d.strip()) for d in days_str.split(',') if d.strip().isdigit()]
    print(f"[AUTO_OPEN] current weekday: {current_weekday}, scheduled: {scheduled_days}")
    if current_weekday not in scheduled_days:
        return False
    
    time_str = get_auto_open_config(group_id, 'auto_open_time')
    print(f"[AUTO_OPEN] time: {time_str}")
    if not time_str:
        return False
    
    try:
        time_parts = time_str.split(':')
        target_hour = int(time_parts[0])
        target_minute = int(time_parts[1])
        current_hour = now.hour
        current_minute = now.minute
        
        window_end = target_minute + 30
        print(f"[AUTO_OPEN] target: {target_hour}:{target_minute:02d}, current: {current_hour}:{current_minute:02d}, window_end: {window_end}")
        in_window = False
        if window_end >= 60:
            next_hour = (target_hour + 1) % 24
            if (current_hour == target_hour and current_minute >= target_minute) or \
               (current_hour == next_hour and current_minute <= window_end - 60):
                in_window = True
        else:
            if current_hour == target_hour and target_minute <= current_minute <= window_end:
                in_window = True
        
        print(f"[AUTO_OPEN] in_window: {in_window}")
        return in_window
    except:
        return False

def get_zero_play_open_config(group_id, key):
    cur = get_cursor()
    if not cur:
        return None
    try:
        cur.execute("SELECT value FROM config WHERE group_id=%s AND key=%s", (group_id, key))
        result = cur.fetchone()
        return result[0] if result else None
    except:
        return None

def set_zero_play_open_config(group_id, key, value):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, %s, %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, key, value, value))
    except:
        pass

def get_zero_play_open_triggered(group_id):
    cur = get_cursor()
    if not cur:
        return False
    try:
        cur.execute("SELECT value FROM config WHERE group_id=%s AND key='zero_play_open_triggered'", (group_id,))
        result = cur.fetchone()
        return result and result[0] == '1'
    except:
        return False

def set_zero_play_open_triggered(group_id, value):
    cur = get_cursor()
    if not cur:
        return
    try:
        if value:
            cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, 'zero_play_open_triggered', '1') ON CONFLICT (group_id, key) DO UPDATE SET value = '1'", (group_id,))
        else:
            cur.execute("DELETE FROM config WHERE group_id=%s AND key='zero_play_open_triggered'", (group_id,))
    except:
        pass

def get_schedule_config(group_id, key):
    cur = get_cursor()
    if not cur:
        return None
    try:
        cur.execute("SELECT value FROM config WHERE group_id=%s AND key=%s", (group_id, key))
        result = cur.fetchone()
        return result[0] if result else None
    except:
        return None

def set_schedule_config(group_id, key, value):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, %s, %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, key, value, value))
    except:
        pass

def get_groups_with_schedule():
    cur = get_cursor()
    if not cur:
        return []
    try:
        cur.execute("SELECT DISTINCT group_id FROM config WHERE key = 'schedule_days'")
        return [row[0] for row in cur.fetchall()]
    except:
        return []

def should_auto_schedule(group_id):
    taiwan_tz = timezone(timedelta(hours=8))
    now = datetime.now(taiwan_tz)
    current_weekday = now.weekday() + 1
    
    days_str = get_schedule_config(group_id, 'schedule_days')
    if not days_str:
        return False
    
    scheduled_days = [int(d.strip()) for d in days_str.split(',') if d.strip().isdigit()]
    if current_weekday not in scheduled_days:
        return False
    
    time_str = get_schedule_config(group_id, 'schedule_time')
    if not time_str:
        return False
    
    try:
        time_parts = time_str.split(':')
        target_hour = int(time_parts[0])
        target_minute = int(time_parts[1])
        current_hour = now.hour
        current_minute = now.minute
        
        window_end = target_minute + 30
        in_window = False
        if window_end >= 60:
            next_hour = (target_hour + 1) % 24
            if (current_hour == target_hour and current_minute >= target_minute) or \
               (current_hour == next_hour and current_minute <= window_end - 60):
                in_window = True
        else:
            if current_hour == target_hour and target_minute <= current_minute <= window_end:
                in_window = True
        
        return in_window
    except:
        return False

def get_active_groups():
    cur = get_cursor()
    if not cur:
        return []
    try:
        now = int(time.time())
        cur.execute("SELECT group_id FROM events WHERE expires_at > %s", (now,))
        all_groups = [row[0] for row in cur.fetchall()]
        # Filter out private chats (they start with 'private_')
        return [g for g in all_groups if not g.startswith('private_')]
    except:
        return []

def build_list_message(group_id):
    cur = get_cursor()
    if not cur:
        return None
    if not is_event_active(group_id):
        return None
    cur.execute("""
        SELECT s.user_id, COALESCE(u.name, s.name), COALESCE(s.count, 0)
        FROM signups s
        LEFT JOIN users u ON s.user_id = u.user_id AND s.group_id = u.group_id
        WHERE s.group_id = %s
        ORDER BY COALESCE(s.count, 0) DESC
    """, (group_id,))
    rows = cur.fetchall()
    
    if not rows:
        return "📋 目前无人报名"
    
    total_count = sum(r[2] for r in rows)
    limit = get_signup_limit(group_id)
    msg = "🏀 打球名单：\n\n"
    for idx, (uid, name, count) in enumerate(rows, 1):
        if not name or len(name) <= 4:
            cur.execute("SELECT name FROM signups WHERE user_id=%s AND group_id=%s", (uid, group_id))
            signup_row = cur.fetchone()
            if signup_row and signup_row[0] and len(signup_row[0]) > 4:
                name = signup_row[0]
            else:
                api_name = fetch_group_member_name(uid, group_id)
                if api_name:
                    name = api_name
        if not name:
            name = uid[-4:]
        display_name = name
        name_prefix = "" if len(name) <= 4 else "@"
        member_tag = " [年缴]" if is_yearly_member(uid, group_id) else ""
        msg += f"{idx}. {name_prefix}{display_name} ({count}人){member_tag}\n"
    msg += "----------------------\n"
    msg += f"👥 报名：{total_count} 人 / 上限：{limit} 人"
    return msg

def auto_end_event(group_id):
    cur = get_cursor()
    if not cur:
        return False
    try:
        cur.execute("DELETE FROM signups WHERE group_id=%s", (group_id,))
        cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))
        return True
    except:
        return False

def run_auto_schedule():
    groups = get_groups_with_schedule()
    print(f"[SCHEDULE] run_auto_schedule called, groups: {len(groups)}")
    for group_id in groups:
        try:
            if not is_event_active(group_id):
                print(f"[SCHEDULE] {group_id[:8]}... no active event")
                continue
            if not should_auto_schedule(group_id):
                print(f"[SCHEDULE] {group_id[:8]}... should_auto_schedule = False")
                continue
            
            print(f"[SCHEDULE] Triggering for {group_id[:8]}...")
            list_msg = build_list_message(group_id)
            if list_msg:
                print(f"[SCHEDULE] Sending list message to {group_id[:8]}...")
                line_bot_api.push_message(group_id, TextSendMessage(text=list_msg))
            time.sleep(0.5)
            auto_end_event(group_id)
            line_bot_api.push_message(group_id, TextSendMessage(text="✅ 活动已结束（自动排程）"))
            print(f"[SCHEDULE] Event ended for {group_id[:8]}...")
            time.sleep(0.5)
        except Exception as e:
            print(f"[SCHEDULE] Error for {group_id[:8]}...: {e}")

def atomic_signup(user_id, group_id, name=""):
    cur = get_cursor()
    if not cur:
        return False
    try:
        now = int(time.time())
        cur.execute("""
            WITH event_check AS (
                SELECT expires_at FROM events WHERE group_id=%s
            ),
            limit_check AS (
                SELECT COALESCE((
                    SELECT value::int FROM config
                    WHERE group_id='default' AND key='signup_limit'
                ), 12) AS lim
            ),
            count_check AS (
                SELECT COUNT(*)::int AS cnt FROM signups WHERE group_id=%s
            ),
            decision AS (
                SELECT
                    (SELECT expires_at FROM event_check) AS expires_at,
                    (SELECT lim FROM limit_check) AS lim,
                    (SELECT cnt FROM count_check) AS cnt
            )
            SELECT
                CASE
                    WHEN (SELECT expires_at FROM decision) IS NULL THEN 'no_event'
                    WHEN (SELECT expires_at FROM decision) <= %s THEN 'expired'
                    WHEN (SELECT cnt FROM decision) >= (SELECT lim FROM decision) THEN 'full'
                    ELSE 'ok'
                END AS result
        """, (group_id, group_id, now))
        result = cur.fetchone()
        if not result or result[0] != 'ok':
            return False
        cur.execute("""
            INSERT INTO signups (user_id, group_id, name, count, signup_time)
            VALUES (%s, %s, %s, 1, %s)
            ON CONFLICT (user_id, group_id)
            DO UPDATE SET name = EXCLUDED.name, count = 1
        """, (user_id, group_id, name, now))
        return True
    except Exception as e:
        print(f"atomic_signup error: {e}")
        return False

def get_mentioned_users(event, exclude_id=None):
    mentioned = []
    group_id = get_group_id(event)
    mention = getattr(event.message, 'mention', None)
    if mention and hasattr(mention, 'mentionees'):
        for m in mention.mentionees:
            if m.user_id and m.user_id != exclude_id:
                name = get_user_name(m.user_id, group_id)
                try:
                    if should_fetch_profile(m.user_id, group_id):
                        profile = line_bot_api.get_profile(m.user_id)
                        name = profile.display_name
                        add_user(m.user_id, group_id, name)
                        update_user_name(m.user_id, group_id, name)
                    else:
                        add_user(m.user_id, group_id, name)
                except:
                    add_user(m.user_id, group_id, name)
                mentioned.append((m.user_id, name))
    return mentioned, group_id

def run_auto_open():
    groups = get_groups_with_auto_open()
    print(f"[AUTO_OPEN] run_auto_open called, groups: {len(groups)}")
    for group_id in groups:
        should_trigger = should_auto_open(group_id)
        print(f"[AUTO_OPEN] should_auto_open({group_id[:8]}...) = {should_trigger}")
        if not should_trigger:
            continue
        if is_event_active(group_id):
            print(f"[AUTO_OPEN] Event already active for {group_id[:8]}...")
            continue
        try:
            count = coach_open_event(ADMIN_ID, group_id, "系統", auto_opened=True)
            print(f"[AUTO_OPEN] Event opened, count: {count}")
            line_bot_api.push_message(group_id, TextSendMessage(text=f"🏀 活動已自動開啟（系統），報名成功，累計人數 {count} 人"))
        except Exception as e:
            print(f"[AUTO_OPEN] Error: {e}")

def check_and_trigger_zero_play():
    taiwan_tz = timezone(timedelta(hours=8))
    now = datetime.now(taiwan_tz)
    current_weekday = now.weekday() + 1
    current_hour = now.hour
    current_minute = now.minute
    
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("SELECT DISTINCT group_id FROM config WHERE key = 'zero_play_open_days'")
        groups = [row[0] for row in cur.fetchall()]
    except:
        groups = []
    
    print(f"[ZERO_PLAY] check_and_trigger_zero_play called, groups: {len(groups)}")
    
    for group_id in groups:
        try:
            if not is_event_active(group_id):
                print(f"[ZERO_PLAY] {group_id[:8]}... no active event")
                continue
            if get_zero_play_open_triggered(group_id):
                print(f"[ZERO_PLAY] {group_id[:8]}... already triggered")
                continue
            
            days_str = get_zero_play_open_config(group_id, 'zero_play_open_days')
            time_str = get_zero_play_open_config(group_id, 'zero_play_open_time')
            print(f"[ZERO_PLAY] {group_id[:8]}... days: {days_str}, time: {time_str}")
            
            if not days_str or not time_str:
                continue
            
            scheduled_days = [int(d.strip()) for d in days_str.split(',') if d.strip().isdigit()]
            if current_weekday not in scheduled_days:
                print(f"[ZERO_PLAY] {group_id[:8]}... weekday {current_weekday} not in {scheduled_days}")
                continue
            
            time_parts = time_str.split(':')
            target_hour = int(time_parts[0])
            target_minute = int(time_parts[1])
            
            window_end = target_minute + 30
            print(f"[ZERO_PLAY] {group_id[:8]}... target: {target_hour}:{target_minute:02d}, current: {current_hour}:{current_minute:02d}, window_end: {window_end}")
            in_window = False
            if window_end >= 60:
                next_hour = (target_hour + 1) % 24
                if (current_hour == target_hour and current_minute >= target_minute) or \
                   (current_hour == next_hour and current_minute <= window_end - 60):
                    in_window = True
            else:
                if current_hour == target_hour and target_minute <= current_minute <= window_end:
                    in_window = True
            
            print(f"[ZERO_PLAY] {group_id[:8]}... in_window: {in_window}")
            if in_window:
                set_zero_play_open_triggered(group_id, True)
                print(f"[ZERO_PLAY] Triggering zero play open for {group_id[:8]}...")
                try:
                    line_bot_api.push_message(group_id, TextSendMessage(text="🎉 零打報名+1！現在所有人都可以報名了！"))
                except Exception as e:
                    print(f"[ZERO_PLAY] Push message error: {e}")
        except Exception as e:
            print(f"[ZERO_PLAY] Error for {group_id[:8]}...: {e}")

@app.head("/")
async def health_check_head():
    return "OK"

@app.get("/")
async def health_check(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_auto_schedule)
    background_tasks.add_task(run_auto_open)
    background_tasks.add_task(check_and_trigger_zero_play)
    return "OK"

@app.get("/me")
async def bot_me():
    uid = get_bot_user_id()
    return {"bot_user_id": uid}

@handler.add(JoinEvent)
def handle_join(event):
    reply_token = event.reply_token
    msg = """大家好！我是記帳機器人，專門用來管理球場活動報名和記帳。

有任何問題請輸入「幫助」查看完整指令列表！"""
    line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))

@app.head("/callback")
async def callback_head():
    return "OK"

@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")

    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, handler.handle, body.decode(), signature)
    except InvalidSignatureError:
        return "Invalid signature"
    except Exception as e:
        print(f"Error: {e}")
        return "Error"

    return "OK"

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    original_text = event.message.text.strip()
    text = original_text
    group_id = get_group_id(event)
    price = get_price(group_id)
    user_name = get_user_name(user_id, group_id)
    reply_token = event.reply_token
    source_type = event.source.type

    # @人 +N / @人 -N: admin shortcut, check BEFORE mention stripping
    if user_id == ADMIN_ID and source_type == 'group':
        mention = getattr(event.message, 'mention', None)
        if mention and hasattr(mention, 'mentionees'):
            non_bot_mentionees = [m for m in mention.mentionees
                                  if getattr(m, 'user_id', None) and m.user_id != get_bot_user_id()]
            if non_bot_mentionees:
                # Check if +/-N follows the mention
                after_mention = original_text
                for m in reversed(non_bot_mentionees):
                    idx = getattr(m, 'index', None)
                    length = getattr(m, 'length', None)
                    if idx is not None and length is not None:
                        after_mention = after_mention[:idx] + after_mention[idx + length:]
                after_mention = after_mention.strip()
                
                # Extract target info
                target_user_id = non_bot_mentionees[0].user_id
                m_idx = getattr(non_bot_mentionees[0], 'index', None)
                m_len = getattr(non_bot_mentionees[0], 'length', None)
                if m_idx is not None and m_len is not None:
                    mention_text = original_text[m_idx:m_idx + m_len].strip()
                    mention_name = mention_text.lstrip('@').strip()
                else:
                    mention_name = None
                target_name = get_user_name(target_user_id, group_id)
                # Fetch target profile if needed (always group context here)
                if should_fetch_profile(target_user_id, group_id):
                    try:
                        profile = line_bot_api.get_group_member_profile(group_id, target_user_id)
                        target_name = profile.display_name
                        add_user(target_user_id, group_id, target_name)
                        update_user_name(target_user_id, group_id, target_name)
                    except:
                        if mention_name:
                            target_name = mention_name
                        add_user(target_user_id, group_id, target_name)
                else:
                    add_user(target_user_id, group_id, target_name)
                
                # @人 +N
                if after_mention.startswith('+'):
                    if after_mention == '+':
                        n = 1
                    else:
                        try:
                            n = int(after_mention.lstrip('+'))
                        except:
                            n = 0
                    if n > 0:
                        if not is_event_active(group_id):
                            line_bot_api.reply_message(reply_token, TextSendMessage(text="活動尚未開始"))
                            return
                        limit = get_signup_limit(group_id)
                        current_total = get_total_count(group_id)
                        is_yearly = is_yearly_member(target_user_id, group_id)
                        if not is_yearly and current_total + n > limit:
                            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 人數已滿（{current_total}/{limit}），無法報名"))
                            return
                        first_signup = not is_signed_up(target_user_id, group_id)
                        if first_signup:
                            signed_up = atomic_signup(target_user_id, group_id, target_name)
                            if not signed_up:
                                current = get_signup_count(group_id)
                                line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 報名人數已滿（{current}/{limit}）"))
                                return
                        add_count(target_user_id, group_id, n, target_name)
                        add_total_count(group_id, n)
                        new_count = get_total_count(group_id)
                        if first_signup:
                            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"報名成功（@{target_name}），累計人數 {new_count} 人"))
                        else:
                            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 累計人數 {new_count} 人"))
                        return
                
                # @人 -N
                if after_mention.startswith('-'):
                    if after_mention == '-':
                        n = 1
                    else:
                        try:
                            n = int(after_mention.lstrip('-'))
                        except:
                            n = 0
                    if n > 0:
                        if not is_event_active(group_id):
                            line_bot_api.reply_message(reply_token, TextSendMessage(text="⚠️ 活動尚未開始或已結束"))
                            return
                        if not is_signed_up(target_user_id, group_id):
                            name_display = f"@{target_name}" if target_name else target_user_id[-4:]
                            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"{name_display} 尚未報到"))
                            return
                        current_count = get_signup_count_for_user(target_user_id, group_id)
                        if n > current_count:
                            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 次數不足，@{target_name} 目前最多可扣 {current_count} 次"))
                            return
                        cur = get_cursor()
                        if cur:
                            cur.execute("UPDATE users SET count = GREATEST(count - %s, 0) WHERE user_id=%s AND group_id=%s", (n, target_user_id, group_id))
                            cur.execute("UPDATE signups SET count = GREATEST(count - %s, 0) WHERE user_id=%s AND group_id=%s", (n, target_user_id, group_id))
                            cur.execute("UPDATE events SET total_count = GREATEST(total_count - %s, 0) WHERE group_id=%s", (n, group_id))
                        new_count = get_total_count(group_id)
                        if new_count < 0:
                            new_count = 0
                        if current_count - n == 0:
                            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"本次不報名（@{target_name}），累計人數 {new_count} 人"))
                        else:
                            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 累計人數 {new_count} 人"))
                        return

    # Normal mention stripping for other cases
    mention = getattr(event.message, 'mention', None)
    if mention and hasattr(mention, 'mentionees'):
        mentionees = list(mention.mentionees)
        mentionees.sort(key=lambda m: getattr(m, 'index', 0) if getattr(m, 'index', 0) is not None else 0, reverse=True)
        for m in mentionees:
            uid = getattr(m, 'user_id', None)
            if uid:
                text = re.sub(r'@' + re.escape(uid), '', text)
            m_len = getattr(m, 'length', None)
            m_idx = getattr(m, 'index', None)
            if m_idx is not None and m_len is not None:
                text = text[:m_idx] + text[m_idx + m_len:]
    text = re.sub(r'@[^\s@]+', '', text).strip()
    while '  ' in text:
        text = text.replace('  ', ' ')

    if should_fetch_profile(user_id, group_id):
        try:
            # Use group member API for group chats (works even if user is not bot's friend)
            if source_type == 'group':
                profile = line_bot_api.get_group_member_profile(group_id, user_id)
            else:
                profile = line_bot_api.get_profile(user_id)
            user_name = profile.display_name
            add_user(user_id, group_id, user_name)
            update_user_name(user_id, group_id, user_name)
        except:
            add_user(user_id, group_id, user_name)
    else:
        add_user(user_id, group_id, user_name)

    if text in ["幫助", "help"]:
        is_admin = (user_id == ADMIN_ID)
        msg = "📋 記帳機器人指令：\n\n"
        msg += "【指令】\n"
        msg += "今天打球+1 / 明天打球+1：開團\n"
        msg += "+ / +N：報名（最多 10 次）\n"
        msg += "- / -N：扣次數\n"
        msg += "查帳：查看自己的帳目\n"
        msg += "名單：查看本次報名名單\n"
        if is_admin:
            msg += "\n【管理員指令】\n"
            msg += "設定單價 [數字]\n"
            msg += "設定報名人數上限 [數字]\n"
            msg += "設定活動時間 [小時]\n"
            msg += "@人 +N / -N：替他人記錄（教練）\n"
            msg += "已繳 @人：清除用戶帳目（繳費確認）\n"
            msg += "年繳加入 / 年繳移除 @人：管理年繳會員\n"
            msg += "年繳名單：查看年繳會員\n"
            msg += "年繳全部移除：移除所有年繳會員\n"
            msg += "重置全部：清除所有紀錄資料\n"
            msg += "活動結束：提早結束活動\n"
            msg += "全部帳單：查看所有群組的欠款\n"
            msg += "退出群組：清除資料並退出\n"
            msg += "狀態：查看系統狀態\n"
            msg += "\n【開團設定】\n"
            msg += "開團設定查看：查看目前設定\n"
            msg += "開團設定 [開團日] [開團時間] [零打日] [零打時間] [排程日] [排程時間]\n"
            msg += "  例：開團設定 3 20:00 4 11:30 4 23:00\n"
            msg += "  開團日=3(週三) 開團時間=20:00\n"
            msg += "  零打日=4(週四) 零打時間=11:30\n"
            msg += "  排程日=4(週四) 排程時間=23:00\n"
            msg += "開團設定關閉：關閉所有設定"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    if text == "狀態" and user_id == ADMIN_ID:
        try:
            cur = get_cursor()
            user_count = get_group_stats(group_id)
            
            msg = "📊 系統狀態：\n\n"
            db_size_mb = 0
            
            if cur:
                try:
                    cur.execute("SELECT pg_database_size(current_database())")
                    db_size_bytes = cur.fetchone()[0]
                    db_size_mb = db_size_bytes / (1024 * 1024)
                    msg += f"💾 資料庫使用：{db_size_mb:.1f} MB\n\n"
                except:
                    msg += f"💾 資料庫使用：查詢失敗\n\n"
            else:
                msg += f"💾 資料庫使用：無法連線\n\n"
            
            event_active = is_event_active(group_id)
            remaining = get_event_remaining_hours(group_id)
            
            msg += f"👥 目前群組：\n"
            msg += f"   活動：{'已開啟（剩餘 ' + str(remaining) + ' 小時）' if event_active else '未開啟'}\n"
            msg += f"   報名人數：{get_signup_count(group_id)} / {get_signup_limit(group_id)} 人\n"
            msg += f"   登記用戶：{user_count} 人\n"
            msg += f"   目前單價：{price} 元"
            
            if db_size_mb > 900:
                msg += "\n\n⚠️ 警告：資料庫使用量接近上限！"
        except Exception as e:
            msg = f"❌ 無法取得狀態：{e}"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    if text == "退出群組" and user_id == ADMIN_ID and source_type in ['group', 'room']:
        try:
            clear_group_data(group_id)
            line_bot_api.reply_message(reply_token, TextSendMessage(text="👋 已清除所有資料，即將退出群組..."))
            time.sleep(1)
            if source_type == 'group':
                line_bot_api.leave_group(group_id)
            else:
                line_bot_api.leave_room(group_id)
        except Exception as e:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"❌ 退出失敗：{e}"))
        return

    if text.startswith("設定單價") and user_id == ADMIN_ID:
        try:
            new_price = int(text.split()[-1])
            set_price(group_id, new_price)
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 單價已設定為 {new_price} 元"))
        except:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 格式錯誤，請輸入：設定單價 數字"))
        return

    if text.startswith("設定報名人數上限") and user_id == ADMIN_ID:
        try:
            new_limit = int(text.split()[-1])
            cur = get_cursor()
            if cur:
                cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'signup_limit', %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (str(new_limit), str(new_limit)))
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 報名人數上限已設定為 {new_limit} 人"))
        except:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 格式錯誤，請輸入：設定報名人數上限 數字"))
        return

    if text.startswith("設定活動時間") and user_id == ADMIN_ID:
        try:
            new_hours = int(text.split()[-1])
            cur = get_cursor()
            if cur:
                cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'event_duration', %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (str(new_hours), str(new_hours)))
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 活動時間已設定為 {new_hours} 小時"))
        except:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 格式錯誤，請輸入：設定活動時間 小時"))
        return

    if text == "重置全部" and user_id == ADMIN_ID:
        clear_all_users(group_id)
        clear_signups(group_id)
        cur = get_cursor()
        if cur:
            try:
                cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))
                cur.execute("DELETE FROM config WHERE group_id=%s", (group_id,))
            except:
                pass
        line_bot_api.reply_message(reply_token, TextSendMessage(text="✅ 已清除所有紀錄資料（含開團設定）"))
        return

    if text == "活動結束" and user_id == ADMIN_ID:
        if not is_event_active(group_id):
            line_bot_api.reply_message(reply_token, TextSendMessage(text="目前沒有進行中的活動"))
            return
        cur = get_cursor()
        if cur:
            try:
                cur.execute("DELETE FROM signups WHERE group_id=%s", (group_id,))
                cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))
            except:
                pass
        set_zero_play_open_triggered(group_id, False)
        line_bot_api.reply_message(reply_token, TextSendMessage(text="✅ 活動已結束"))
        return

    if text.startswith("已繳") and user_id == ADMIN_ID:
        target_user_id = None
        target_name = None
        
        # Try to get target from mention
        mentioned, _ = get_mentioned_users(event, ADMIN_ID)
        for m_user_id, m_name in mentioned:
            target_user_id = m_user_id
            target_name = m_name
            break
        
        # If no mention, try to find by name
        if not target_user_id:
            parts = text.replace("已繳", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts, group_id)
                target_name = parts
        
        if target_user_id:
            clear_user(target_user_id, group_id)
            if not target_name:
                target_name = target_user_id[-4:]
            # For ID fragments (4 chars or less), don't add @ prefix
            name_display = target_name if len(target_name) <= 4 else f"@{target_name}"
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已清除 {name_display} 的帳目（目前 0 次）"))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「已繳 @人」格式"))
        return

    if text == "全部帳單" and user_id == ADMIN_ID:
        cur = get_cursor()
        if not cur:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 資料庫連線失敗"))
            return
        cur.execute("""
            SELECT u.user_id, u.name, u.count, u.group_id
            FROM users u
            WHERE u.count > 0
            ORDER BY u.group_id, u.count DESC
        """)
        rows = cur.fetchall()
        
        msg = "📋 全部帳單（跨所有群組）：\n\n"
        has_debt = False
        current_gid = None
        for uid, name, count, gid in rows:
            # Skip yearly members (they don't need to pay)
            if is_yearly_member(uid, gid):
                continue
            if gid != current_gid:
                current_gid = gid
                if gid.startswith('private_'):
                    group_label = '私聊'
                else:
                    group_label = gid
                    try:
                        summary = line_bot_api.get_group_summary(gid)
                        group_label = summary.group_name
                    except:
                        try:
                            summary = line_bot_api.get_room(room_id=gid)
                            group_label = summary.room_name
                        except:
                            pass
                msg += f"【{group_label}】\n"
            display_name = name
            if not display_name:
                cur.execute("SELECT name FROM signups WHERE user_id=%s", (uid,))
                row = cur.fetchone()
                if row and row[0]:
                    display_name = row[0]
            if not display_name or len(display_name) <= 4:
                # Try group member API first (works without being friends)
                api_name = fetch_group_member_name(uid, gid)
                if api_name:
                    display_name = api_name
                    cur.execute("""
                        INSERT INTO users (user_id, group_id, name, count, last_fetch)
                        VALUES (%s, %s, %s, 0, 0)
                        ON CONFLICT (user_id, group_id)
                        DO UPDATE SET name = EXCLUDED.name
                    """, (uid, gid, display_name))
                else:
                    try:
                        profile = line_bot_api.get_profile(uid)
                        display_name = profile.display_name
                        cur.execute("""
                            INSERT INTO users (user_id, group_id, name, count, last_fetch)
                            VALUES (%s, %s, %s, 0, 0)
                            ON CONFLICT (user_id, group_id)
                            DO UPDATE SET name = EXCLUDED.name
                        """, (uid, gid, display_name))
                    except:
                        pass
            if not display_name:
                display_name = uid[-4:]
            cur.execute("SELECT value FROM config WHERE group_id='default' AND key='price'")
            price_row = cur.fetchone()
            group_price = int(price_row[0]) if price_row else 50
            # For ID fragments (4 chars or less), don't add @ prefix
            if len(display_name) <= 4:
                msg += f"  {display_name}: {count}次 / {count*group_price}元\n"
            else:
                msg += f"  @{display_name}: {count}次 / {count*group_price}元\n"
            has_debt = True
        if not has_debt:
            msg = "✅ 目前無欠款"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    # Yearly member commands (admin only)
    if text.startswith("年繳加入") and user_id == ADMIN_ID:
        target_user_id = None
        target_name = None
        mentioned, _ = get_mentioned_users(event, ADMIN_ID)
        for m_user_id, m_name in mentioned:
            target_user_id = m_user_id
            target_name = m_name
            break
        if not target_user_id:
            parts = text.replace("年繳加入", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts, group_id)
                target_name = parts
        if target_user_id:
            add_yearly_member(target_user_id, group_id, target_name or "")
            name_display = f"@{target_name}" if target_name else target_user_id[-4:]
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已將 {name_display} 設為年繳會員"))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「年繳加入 @人」格式"))
        return

    if text.startswith("年繳移除") and user_id == ADMIN_ID:
        target_user_id = None
        target_name = None
        mentioned, _ = get_mentioned_users(event, ADMIN_ID)
        for m_user_id, m_name in mentioned:
            target_user_id = m_user_id
            target_name = m_name
            break
        if not target_user_id:
            parts = text.replace("年繳移除", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts, group_id)
                target_name = parts
        if target_user_id:
            remove_yearly_member(target_user_id, group_id)
            name_display = f"@{target_name}" if target_name else target_user_id[-4:]
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已將 {name_display} 移出年繳會員"))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「年繳移除 @人」格式"))
        return

    if text == "年繳名單" and user_id == ADMIN_ID:
        rows = get_yearly_members(group_id)
        if rows:
            msg = "📋 年繳會員名單：\n"
            for uid, name in rows:
                display_name = name if name else uid[-4:]
                if len(display_name) <= 4:
                    msg += f"  {display_name}\n"
                else:
                    msg += f"  @{display_name}\n"
        else:
            msg = "📋 目前沒有年繳會員"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    if text == "年繳全部移除" and user_id == ADMIN_ID:
        cur = get_cursor()
        if not cur:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 資料庫連線失敗"))
            return
        cur.execute("SELECT COUNT(*) FROM yearly_members WHERE group_id=%s", (group_id,))
        count = cur.fetchone()[0]
        if count == 0:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="📋 目前沒有年繳會員"))
            return
        cur.execute("DELETE FROM yearly_members WHERE group_id=%s", (group_id,))
        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已移除全部 {count} 位年繳會員"))
        return

    if text == "開團設定查看" and user_id == ADMIN_ID:
        auto_days = get_auto_open_config(group_id, 'auto_open_days')
        auto_time = get_auto_open_config(group_id, 'auto_open_time')
        zero_days = get_zero_play_open_config(group_id, 'zero_play_open_days')
        zero_time = get_zero_play_open_config(group_id, 'zero_play_open_time')
        schedule_days = get_schedule_config(group_id, 'schedule_days')
        schedule_time = get_schedule_config(group_id, 'schedule_time')
        
        day_names = {1: "週一", 2: "週二", 3: "週三", 4: "週四", 5: "週五", 6: "週六", 7: "週日"}
        
        msg = "🏀 開團設定：\n"
        
        if auto_days and auto_time:
            try:
                days_list = [int(d.strip()) for d in auto_days.split(',') if d.strip().isdigit()]
                auto_display = ", ".join([day_names.get(d, str(d)) for d in days_list])
            except:
                auto_display = auto_days
            msg += f"  自動開團：{auto_display} {auto_time}\n"
        else:
            msg += "  自動開團：未設定\n"
        
        if zero_days and zero_time:
            try:
                days_list = [int(d.strip()) for d in zero_days.split(',') if d.strip().isdigit()]
                zero_display = ", ".join([day_names.get(d, str(d)) for d in days_list])
            except:
                zero_display = zero_days
            msg += f"  零打開放：{zero_display} {zero_time}\n"
        else:
            msg += "  零打開放：未設定\n"
        
        if schedule_days and schedule_time:
            try:
                days_list = [int(d.strip()) for d in schedule_days.split(',') if d.strip().isdigit()]
                schedule_display = ", ".join([day_names.get(d, str(d)) for d in days_list])
            except:
                schedule_display = schedule_days
            msg += f"  自動排程：{schedule_display} {schedule_time}\n"
        else:
            msg += "  自動排程：未設定\n"
        
        msg += "\n※ 自動開團後僅年繳會員可報名\n"
        msg += "※ 零打開放後所有人都可報名\n"
        msg += "※ 自動排程結束活動並發送名單"
        
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    if text.startswith("開團設定") and user_id == ADMIN_ID:
        parts = text.replace("開團設定", "").strip()
        if not parts:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 格式錯誤，請輸入：開團設定 3 20:00 4 11:30 4 23:00\n（開團日 開團時間 零打日 零打時間 排程日 排程時間）"))
            return
        
        parts_list = parts.split()
        if len(parts_list) < 6:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 格式錯誤，請輸入：開團設定 3 20:00 4 11:30 4 23:00\n（開團日 開團時間 零打日 零打時間 排程日 排程時間）"))
            return
        
        try:
            open_days = parts_list[0]
            open_time = parts_list[1]
            zero_days = parts_list[2]
            zero_time = parts_list[3]
            schedule_days = parts_list[4]
            schedule_time = parts_list[5]
            
            open_days_list = [int(d.strip()) for d in open_days.split(',') if d.strip().isdigit()]
            if not open_days_list or any(d < 1 or d > 7 for d in open_days_list):
                raise ValueError
            
            zero_days_list = [int(d.strip()) for d in zero_days.split(',') if d.strip().isdigit()]
            if not zero_days_list or any(d < 1 or d > 7 for d in zero_days_list):
                raise ValueError
            
            schedule_days_list = [int(d.strip()) for d in schedule_days.split(',') if d.strip().isdigit()]
            if not schedule_days_list or any(d < 1 or d > 7 for d in schedule_days_list):
                raise ValueError
            
            open_time_parts = open_time.split(':')
            if len(open_time_parts) != 2:
                raise ValueError
            open_hour = int(open_time_parts[0])
            open_minute = int(open_time_parts[1])
            if open_hour < 0 or open_hour > 23 or open_minute < 0 or open_minute > 59:
                raise ValueError
            
            zero_time_parts = zero_time.split(':')
            if len(zero_time_parts) != 2:
                raise ValueError
            zero_hour = int(zero_time_parts[0])
            zero_minute = int(zero_time_parts[1])
            if zero_hour < 0 or zero_hour > 23 or zero_minute < 0 or zero_minute > 59:
                raise ValueError
            
            schedule_time_parts = schedule_time.split(':')
            if len(schedule_time_parts) != 2:
                raise ValueError
            schedule_hour = int(schedule_time_parts[0])
            schedule_minute = int(schedule_time_parts[1])
            if schedule_hour < 0 or schedule_hour > 23 or schedule_minute < 0 or schedule_minute > 59:
                raise ValueError
            
            set_auto_open_config(group_id, 'auto_open_days', open_days)
            set_auto_open_config(group_id, 'auto_open_time', open_time)
            set_zero_play_open_config(group_id, 'zero_play_open_days', zero_days)
            set_zero_play_open_config(group_id, 'zero_play_open_time', zero_time)
            set_zero_play_open_triggered(group_id, False)
            set_schedule_config(group_id, 'schedule_days', schedule_days)
            set_schedule_config(group_id, 'schedule_time', schedule_time)
            
            day_names = {1: "週一", 2: "週二", 3: "週三", 4: "週四", 5: "週五", 6: "週六", 7: "週日"}
            open_display = ", ".join([day_names.get(d, str(d)) for d in open_days_list])
            zero_display = ", ".join([day_names.get(d, str(d)) for d in zero_days_list])
            schedule_display = ", ".join([day_names.get(d, str(d)) for d in schedule_days_list])
            
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 開團設定已設定：\n  自動開團：{open_display} {open_time}\n  零打開放：{zero_display} {zero_time}\n  自動排程：{schedule_display} {schedule_time}\n\n※ 自動開團後僅年繳會員可報名\n※ 零打開放後所有人都可報名\n※ 自動排程結束活動並發送名單"))
        except:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 格式錯誤，請輸入：開團設定 3 20:00 4 11:30 4 23:00"))
        return

    if text == "開團設定關閉" and user_id == ADMIN_ID:
        set_auto_open_config(group_id, 'auto_open_days', '')
        set_auto_open_config(group_id, 'auto_open_time', '')
        set_zero_play_open_config(group_id, 'zero_play_open_days', '')
        set_zero_play_open_config(group_id, 'zero_play_open_time', '')
        set_zero_play_open_triggered(group_id, False)
        set_schedule_config(group_id, 'schedule_days', '')
        set_schedule_config(group_id, 'schedule_time', '')
        line_bot_api.reply_message(reply_token, TextSendMessage(text="✅ 開團設定已關閉"))
        return

    if text == "查帳":
        count = get_count(user_id, group_id)
        if is_yearly_member(user_id, group_id):
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"@{user_name} 目前 {count} 次，應繳 0 元（年繳會員）"))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"@{user_name} 目前 {count} 次，應繳 {count*price} 元"))
        return

    if text == "名單":
        if not is_event_active(group_id):
            line_bot_api.reply_message(reply_token, TextSendMessage(text="目前沒有進行中的活動"))
            return
        cur = get_cursor()
        if not cur:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 資料庫連線失敗"))
            return
        cur.execute("""
            SELECT s.user_id, COALESCE(u.name, s.name), COALESCE(s.count, 0)
            FROM signups s
            LEFT JOIN users u ON s.user_id = u.user_id AND s.group_id = u.group_id
            WHERE s.group_id = %s
            ORDER BY COALESCE(s.count, 0) DESC
        """, (group_id,))
        rows = cur.fetchall()
        
        if not rows:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="📋 目前無人報名"))
            return
        
        total_count = sum(r[2] for r in rows)
        limit = get_signup_limit(group_id)
        msg = "🏀 打球名單：\n\n"
        for idx, (uid, name, count) in enumerate(rows, 1):
            # Get display name - try multiple sources
            if not name or len(name) <= 4:
                # Step 1: Try signups table
                cur.execute("SELECT name FROM signups WHERE user_id=%s AND group_id=%s", (uid, group_id))
                signup_row = cur.fetchone()
                if signup_row and signup_row[0] and len(signup_row[0]) > 4:
                    name = signup_row[0]
                    update_user_name(uid, group_id, name)
                else:
                    # Step 2: Try group member API
                    api_name = fetch_group_member_name(uid, group_id)
                    if api_name:
                        name = api_name
                        update_user_name(uid, group_id, api_name)
            if not name:
                name = uid[-4:]
            display_name = name
            name_prefix = "" if len(name) <= 4 else "@"
            member_tag = " [年繳]" if is_yearly_member(uid, group_id) else ""
            msg += f"{idx}. {name_prefix}{display_name} ({count}人){member_tag}\n"
        msg += "----------------------\n"
        msg += f"👥 報名：{total_count} 人 / 上限：{limit} 人"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    signup_prefixes = ["今天打球", "明天打球"]
    for prefix in signup_prefixes:
        if text == f"{prefix}+1" or text == f"{prefix}+":
            if is_event_active(group_id):
                remaining = get_event_remaining_hours(group_id)
                line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 活動進行中，請等待結束後再開團（剩餘 {remaining} 小時）"))
                return
            count = coach_open_event(user_id, group_id, user_name)
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"報名成功，累計人數 {count} 人"))
            return

    if text.startswith("+"):
        if not is_event_active(group_id):
            line_bot_api.reply_message(reply_token, TextSendMessage(text="活動尚未開始"))
            return
        
        allow_result = should_allow_signup(user_id, group_id)
        if isinstance(allow_result, tuple):
            allowed, remaining = allow_result
            if not allowed:
                line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 目前僅開放年繳會員報名，{remaining} 分鐘後開放所有人"))
                return
        elif not allow_result:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="⚠️ 目前僅開放年繳會員報名"))
            return
        
        if text == "+":
            n = 1
        else:
            try:
                n = int(text.lstrip("+"))
            except:
                n = 0
        if n <= 0:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請輸入有效整數（如 +、+5）"))
            return
        max_n = get_max_per_action()
        if n > max_n:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 單次最多 +{max_n} 次"))
            return
        limit = get_signup_limit(group_id)
        current_total = get_total_count(group_id)
        is_yearly = is_yearly_member(user_id, group_id)
        if not is_yearly and current_total + n > limit:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 人數已滿（{current_total}/{limit}），無法報名"))
            return
        
        first_signup = not is_signed_up(user_id, group_id)
        if first_signup:
            signed_up = atomic_signup(user_id, group_id, user_name)
            if not signed_up:
                current = get_signup_count(group_id)
                line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 報名人數已滿（{current}/{limit}）"))
                return
        
        add_count(user_id, group_id, n, user_name)
        add_total_count(group_id, n)
        count = get_total_count(group_id)
        if first_signup:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"報名成功，累計人數 {count} 人"))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 累計人數 {count} 人"))
        return

    if text.startswith("-"):
        if not is_event_active(group_id):
            line_bot_api.reply_message(reply_token, TextSendMessage(text="⚠️ 活動尚未開始或已結束"))
            return
        if not is_signed_up(user_id, group_id):
            line_bot_api.reply_message(reply_token, TextSendMessage(text="請先「+1」報到"))
            return
        if text == "-":
            n = 1
        else:
            try:
                n = int(text.lstrip("-"))
            except:
                n = 0
        if n <= 0:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請輸入有效整數（如 -、-5）"))
            return
        current_count = get_signup_count_for_user(user_id, group_id)
        if n > current_count:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 次數不足，目前最多可扣 {current_count} 次"))
            return
        cur = get_cursor()
        if cur:
            cur.execute("UPDATE users SET count = GREATEST(count - %s, 0) WHERE user_id=%s AND group_id=%s", (n, user_id, group_id))
            cur.execute("UPDATE signups SET count = GREATEST(count - %s, 0) WHERE user_id=%s AND group_id=%s", (n, user_id, group_id))
            cur.execute("UPDATE events SET total_count = GREATEST(total_count - %s, 0) WHERE group_id=%s", (n, group_id))
        count = get_total_count(group_id)
        if count < 0:
            count = 0
        if current_count - n == 0:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"本次不報名，累計人數 {count} 人"))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 累計人數 {count} 人"))
        return
