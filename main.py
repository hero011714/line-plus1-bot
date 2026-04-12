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
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres.rtsxmbvjbfbtltcfotnt:t%3EvTPtZt9%2FQX@aws-0-us-west-2.pooler.supabase.com:5432/postgres")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)
_bot_user_id = None

def get_bot_user_id():
    global _bot_user_id
    if _bot_user_id:
        return _bot_user_id
    try:
        bot_info = line_bot_api.get_bot_info()
        _bot_user_id = bot_info.user_id
        print(f"[BOT_ID] Cached: {_bot_user_id}")
    except Exception as e:
        print(f"[BOT_ID] Error: {e}")
    return _bot_user_id

app = FastAPI()

conn = None

def get_db():
    global conn
    if not conn or conn.closed:
        try:
            import urllib.parse
            parsed = urllib.parse.urlparse(DATABASE_URL)
            conn = psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port or 5432,
                database=parsed.path.lstrip('/'),
                user=parsed.username,
                password=urllib.parse.unquote(parsed.password) if parsed.password else '',
                sslmode='require'
            )
            conn.autocommit = True
            print("Database connected successfully")
        except Exception as e:
            print(f"Database connection failed: {e}")
            return None
    else:
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
        except Exception:
            try:
                conn.close()
            except:
                pass
            conn = None
            return get_db()
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
        cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'signup_limit', '10') ON CONFLICT DO NOTHING")
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
        cur.execute("SELECT value FROM config WHERE group_id=%s AND key='signup_limit'", (group_id,))
        result = cur.fetchone()
        if result:
            return int(result[0])
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
            VALUES (%s, %s, %s, 0, %s)
        """, (user_id, group_id, user_name, now))
        cur.execute("""
            UPDATE signups SET count = count + 1 WHERE user_id = %s AND group_id = %s
        """, (user_id, group_id))
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
    
    return False

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
        set_zero_play_open_triggered(group_id, False)
        return True
    except:
        return False

def run_auto_schedule():
    taiwan_tz = timezone(timedelta(hours=8))
    today_str = datetime.now(taiwan_tz).strftime("%Y-%m-%d")
    groups = get_groups_with_schedule()
    print(f"[SCHEDULE] run_auto_schedule called, groups: {len(groups)}")
    for group_id in groups:
        try:
            last_trigger = get_auto_trigger_date(group_id, 'auto_schedule_triggered_date')
            if last_trigger == today_str:
                print(f"[SCHEDULE] {group_id[:8]}... already triggered today")
                continue
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
                print(f"[SCHEDULE] List message sent OK to {group_id[:8]}...")
            time.sleep(0.5)
            auto_end_event(group_id)
            set_auto_trigger_date(group_id, 'auto_schedule_triggered_date')
            line_bot_api.push_message(group_id, TextSendMessage(text="✅ 活动已结束（自动排程）"))
            print(f"[SCHEDULE] End message sent OK to {group_id[:8]}...")
            time.sleep(0.5)
        except Exception as e:
            print(f"[SCHEDULE] Error for {group_id[:8]}...: {type(e).__name__}: {e}")

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
                    WHERE group_id=%s AND key='signup_limit'
                ), COALESCE((
                    SELECT value::int FROM config
                    WHERE group_id='default' AND key='signup_limit'
                ), 10)) AS lim
            ),
            count_check AS (
                SELECT COALESCE(SUM(count), 0)::int AS cnt FROM signups WHERE group_id=%s
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
        """, (group_id, group_id, group_id, now))
        result = cur.fetchone()
        if not result or result[0] != 'ok':
            return False
        cur.execute("""
            INSERT INTO signups (user_id, group_id, name, count, signup_time)
            VALUES (%s, %s, %s, 0, %s)
            ON CONFLICT (user_id, group_id)
            DO UPDATE SET name = EXCLUDED.name, count = 0
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

def get_auto_trigger_date(group_id, key):
    cur = get_cursor()
    if not cur:
        return None
    try:
        cur.execute("SELECT value FROM config WHERE group_id=%s AND key=%s", (group_id, key))
        result = cur.fetchone()
        return result[0] if result else None
    except:
        return None

def set_auto_trigger_date(group_id, key):
    taiwan_tz = timezone(timedelta(hours=8))
    today_str = datetime.now(taiwan_tz).strftime("%Y-%m-%d")
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, %s, %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, key, today_str, today_str))
    except:
        pass

def run_auto_open():
    taiwan_tz = timezone(timedelta(hours=8))
    today_str = datetime.now(taiwan_tz).strftime("%Y-%m-%d")
    groups = get_groups_with_auto_open()
    print(f"[AUTO_OPEN] run_auto_open called, groups: {len(groups)}")
    for group_id in groups:
        last_trigger = get_auto_trigger_date(group_id, 'auto_open_triggered_date')
        if last_trigger == today_str:
            print(f"[AUTO_OPEN] {group_id[:8]}... already triggered today")
            continue
        should_trigger = should_auto_open(group_id)
        print(f"[AUTO_OPEN] should_auto_open({group_id[:8]}...) = {should_trigger}")
        if not should_trigger:
            continue
        if is_event_active(group_id):
            print(f"[AUTO_OPEN] Event already active for {group_id[:8]}...")
            continue
        try:
            count = coach_open_event(ADMIN_ID, group_id, "系統", auto_opened=True)
            set_auto_trigger_date(group_id, 'auto_open_triggered_date')
            print(f"[AUTO_OPEN] Event opened, count: {count}")
            line_bot_api.push_message(group_id, TextSendMessage(text=f"🏀 活動已自動開啟（系統），報名成功，累計人數 {count} 人"))
            print(f"[AUTO_OPEN] Push message sent OK to {group_id[:8]}...")
        except Exception as e:
            print(f"[AUTO_OPEN] Push failed for {group_id[:8]}...: {type(e).__name__}: {e}")

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
                    print(f"[ZERO_PLAY] Push message sent OK to {group_id[:8]}...")
                except Exception as e:
                    print(f"[ZERO_PLAY] Push failed for {group_id[:8]}...: {type(e).__name__}: {e}")
        except Exception as e:
            print(f"[ZERO_PLAY] Error for {group_id[:8]}...: {e}")

@app.head("/")
async def health_check_head(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_all_auto_tasks)
    return "OK"

def run_all_auto_tasks():
    try:
        run_auto_schedule()
    except Exception as e:
        print(f"[AUTO] Schedule error: {e}")
    try:
        run_auto_open()
    except Exception as e:
        print(f"[AUTO] Open error: {e}")
    try:
        check_and_trigger_zero_play()
    except Exception as e:
        print(f"[AUTO] Zero play error: {e}")

@app.get("/")
async def health_check(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_all_auto_tasks)
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
        handler.handle(body.decode(), signature)
    except InvalidSignatureError:
        return "Invalid signature"
    except Exception as e:
        print(f"Error: {e}")
        return "Error"

    return "OK"

def run_bot_test(group_id, reply_token, price):
    TEST_A = "TEST_USER_A"
    TEST_B = "TEST_USER_B"
    NAME_A = "測試A"
    NAME_B = "測試B"

    results = []
    passed = 0
    failed = 0

    def check(name, condition, detail=""):
        nonlocal passed, failed
        if condition:
            results.append(f"✅ {name}" + (f"：{detail}" if detail else ""))
            passed += 1
        else:
            results.append(f"❌ {name}" + (f"：{detail}" if detail else ""))
            failed += 1

    def end_event_test():
        cur = get_cursor()
        if cur:
            cur.execute("DELETE FROM signups WHERE group_id=%s", (group_id,))
            cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))

    def do_minus(uid, gid, n):
        """扣除 n 次，若扣到 0 則從 signups 刪除"""
        cur = get_cursor()
        if not cur:
            return
        current = get_signup_count_for_user(uid, gid)
        cur.execute("UPDATE users SET count = GREATEST(count - %s, 0) WHERE user_id=%s AND group_id=%s", (n, uid, gid))
        cur.execute("UPDATE events SET total_count = GREATEST(total_count - %s, 0) WHERE group_id=%s", (n, gid))
        if current - n <= 0:
            cur.execute("DELETE FROM signups WHERE user_id=%s AND group_id=%s", (uid, gid))
        else:
            cur.execute("UPDATE signups SET count = GREATEST(count - %s, 0) WHERE user_id=%s AND group_id=%s", (n, uid, gid))

    try:
        # ── 清除 ──────────────────────────────────────────
        clear_group_data(group_id)

        # ═══════════════════════════════════════════════════
        # Phase 0：活動開始前
        # ═══════════════════════════════════════════════════
        results.append("\n【Phase 0 - 活動開始前】")

        # 1. +1 被拒
        check("+1 拒絕", not is_event_active(group_id), "活動尚未開始")
        # 2. -1 被拒
        check("-1 拒絕", not is_event_active(group_id), "活動尚未開始")
        # 3. 查帳 A
        count_a = get_count(TEST_A, group_id)
        check("查帳 A", count_a == 0, f"{count_a}次 {count_a*price}元 正確")
        # 4. 查帳 B
        count_b = get_count(TEST_B, group_id)
        check("查帳 B", count_b == 0, f"{count_b}次 {count_b*price}元 正確")
        # 5. 名單
        check("名單", not is_event_active(group_id), "無活動 正確")
        # 6. 全部帳單
        check("全部帳單", count_a == 0 and count_b == 0, "無欠款 正確")

        # ═══════════════════════════════════════════════════
        # Phase 1：活動進行中
        # ═══════════════════════════════════════════════════
        results.append("\n【Phase 1 - 活動進行中】")

        # 7. 開團 A
        coach_open_event(TEST_A, group_id, NAME_A)
        total = get_total_count(group_id)
        a_signups = get_signup_count_for_user(TEST_A, group_id)
        a_count = get_count(TEST_A, group_id)
        check("開團 A", total == 1 and a_signups == 1 and a_count == 1,
              f"total={total}, A.signups={a_signups}, A.count={a_count}")

        # 8. 查帳 A
        a_count = get_count(TEST_A, group_id)
        check("查帳 A", a_count == 1, f"{a_count}次 {a_count*price}元 正確")
        # 9. 查帳 B
        b_count = get_count(TEST_B, group_id)
        check("查帳 B", b_count == 0, f"{b_count}次 {b_count*price}元 正確")
        # 10. 名單
        total = get_total_count(group_id)
        b_signed = is_signed_up(TEST_B, group_id)
        check("名單", total == 1 and not b_signed, f"1人(A) total={total} 正確")
        # 11. 全部帳單
        check("全部帳單", get_count(TEST_A, group_id) == 1, "A欠1次 正確")

        # 12. B +1
        atomic_signup(TEST_B, group_id, NAME_B)
        add_count(TEST_B, group_id, 1, NAME_B)
        add_total_count(group_id, 1)
        total = get_total_count(group_id)
        b_signups = get_signup_count_for_user(TEST_B, group_id)
        check("B +1", total == 2 and b_signups == 1,
              f"total={total}, B.signups={b_signups}")

        # 13. 查帳 A
        a_count = get_count(TEST_A, group_id)
        check("查帳 A", a_count == 1, f"{a_count}次 正確")
        # 14. 查帳 B
        b_count = get_count(TEST_B, group_id)
        check("查帳 B", b_count == 1, f"{b_count}次 {b_count*price}元 正確")
        # 15. 名單
        total = get_total_count(group_id)
        check("名單", total == 2 and is_signed_up(TEST_B, group_id), f"2人(A,B) total={total} 正確")
        # 16. 全部帳單
        check("全部帳單", get_count(TEST_A, group_id) == 1 and get_count(TEST_B, group_id) == 1,
              "A欠1次 B欠1次 正確")

        # 17. B +2
        add_count(TEST_B, group_id, 2, NAME_B)
        add_total_count(group_id, 2)
        total = get_total_count(group_id)
        b_signups = get_signup_count_for_user(TEST_B, group_id)
        check("B +2", total == 4 and b_signups == 3,
              f"total={total}, B.signups={b_signups}")

        # 18. 查帳 B
        b_count = get_count(TEST_B, group_id)
        check("查帳 B", b_count == 3, f"{b_count}次 {b_count*price}元 正確")
        # 19. 名單
        total = get_total_count(group_id)
        check("名單", total == 4, f"total={total}人 正確")
        # 20. 全部帳單
        check("全部帳單", get_count(TEST_A, group_id) == 1 and get_count(TEST_B, group_id) == 3,
              "A欠1次 B欠3次 正確")

        # 21. B -1
        do_minus(TEST_B, group_id, 1)
        total = get_total_count(group_id)
        b_signups = get_signup_count_for_user(TEST_B, group_id)
        check("B -1", total == 3 and b_signups == 2,
              f"total={total}, B.signups={b_signups}")

        # 22. 查帳 B
        b_count = get_count(TEST_B, group_id)
        check("查帳 B", b_count == 2, f"{b_count}次 {b_count*price}元 正確")
        # 23. 名單
        total = get_total_count(group_id)
        check("名單", total == 3, f"total={total}人 正確")
        # 24. 全部帳單
        check("全部帳單", get_count(TEST_A, group_id) == 1 and get_count(TEST_B, group_id) == 2,
              "A欠1次 B欠2次 正確")

        # 25. B -2（到0）
        do_minus(TEST_B, group_id, 2)
        total = get_total_count(group_id)
        b_removed = not is_signed_up(TEST_B, group_id)
        check("B -2(到0)", total == 1 and b_removed,
              f"total={total}, B從名單移除={b_removed}")

        # 26. 查帳 B
        b_count = get_count(TEST_B, group_id)
        check("查帳 B", b_count == 0, f"{b_count}次 {b_count*price}元 正確")
        # 27. 名單
        total = get_total_count(group_id)
        check("名單", total == 1 and not is_signed_up(TEST_B, group_id),
              f"1人(A) total={total} 正確")
        # 28. 全部帳單
        check("全部帳單", get_count(TEST_A, group_id) == 1 and get_count(TEST_B, group_id) == 0,
              "A欠1次 B無欠款 正確")

        # ═══════════════════════════════════════════════════
        # Phase 2：結束後重開第二輪
        # ═══════════════════════════════════════════════════
        results.append("\n【Phase 2 - 結束後重開第二輪】")

        # 29. 結束活動
        end_event_test()
        check("結束活動", not is_event_active(group_id), "is_active=False 正確")
        # 30. +1 被拒
        check("+1 拒絕", not is_event_active(group_id), "活動尚未開始")
        # 31. -1 被拒
        check("-1 拒絕", not is_event_active(group_id), "活動尚未開始")
        # 32. 查帳 A
        a_count = get_count(TEST_A, group_id)
        check("查帳 A", a_count == 1, f"保留{a_count}次 正確")
        # 33. 查帳 B
        b_count = get_count(TEST_B, group_id)
        check("查帳 B", b_count == 0, f"保留{b_count}次 正確")
        # 34. 名單
        check("名單", not is_event_active(group_id), "無活動 正確")
        # 35. 全部帳單
        check("全部帳單", get_count(TEST_A, group_id) == 1 and get_count(TEST_B, group_id) == 0,
              "A仍欠1次 B無欠款 正確")

        # 36. 重新開團 A
        coach_open_event(TEST_A, group_id, NAME_A)
        total = get_total_count(group_id)
        a_signups = get_signup_count_for_user(TEST_A, group_id)
        a_count = get_count(TEST_A, group_id)
        check("重新開團 A", total == 1 and a_signups == 1 and a_count == 2,
              f"total={total}, A.signups={a_signups}, A.count={a_count}(累積)")

        # 37. 查帳 A
        a_count = get_count(TEST_A, group_id)
        check("查帳 A", a_count == 2, f"{a_count}次(累積) {a_count*price}元 正確")
        # 38. 查帳 B
        b_count = get_count(TEST_B, group_id)
        check("查帳 B", b_count == 0, f"{b_count}次 正確")
        # 39. 名單
        total = get_total_count(group_id)
        check("名單", total == 1 and not is_signed_up(TEST_B, group_id), f"1人(A) 正確")
        # 40. 全部帳單
        check("全部帳單", get_count(TEST_A, group_id) == 2 and get_count(TEST_B, group_id) == 0,
              "A欠2次 B無欠款 正確")

        # 41. B 重新報名 +1
        atomic_signup(TEST_B, group_id, NAME_B)
        add_count(TEST_B, group_id, 1, NAME_B)
        add_total_count(group_id, 1)
        total = get_total_count(group_id)
        b_signups = get_signup_count_for_user(TEST_B, group_id)
        check("B重新報名 +1", total == 2 and b_signups == 1,
              f"total={total}, B.signups={b_signups}")

        # 42. 查帳 A
        a_count = get_count(TEST_A, group_id)
        check("查帳 A", a_count == 2, f"{a_count}次 正確")
        # 43. 查帳 B
        b_count = get_count(TEST_B, group_id)
        check("查帳 B", b_count == 1, f"{b_count}次(新增) {b_count*price}元 正確")
        # 44. 名單
        total = get_total_count(group_id)
        check("名單", total == 2 and is_signed_up(TEST_B, group_id),
              f"2人(A,B) total={total} 正確")
        # 45. 全部帳單
        check("全部帳單", get_count(TEST_A, group_id) == 2 and get_count(TEST_B, group_id) == 1,
              "A欠2次 B欠1次 正確")

    finally:
        # ── 清除測試資料 ──────────────────────────────────
        clear_group_data(group_id)

    # ── 組合報告 ──────────────────────────────────────────
    total_tests = passed + failed
    summary = f"✅ 全部通過 {passed}/{total_tests}" if failed == 0 else f"⚠️ 通過 {passed}/{total_tests}，失敗 {failed} 項"

    msg = "🧪 測試報告\n"
    msg += "\n".join(results)
    msg += f"\n\n{summary}"
    msg += "\n🧹 測試資料已清除"

    line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))



def run_open_group_test(group_id, reply_token):
    TEST_A = "TEST_USER_A"
    TEST_B = "TEST_USER_B"
    TEST_C = "TEST_USER_C"
    NAME_A = "測試A"
    NAME_B = "測試B"
    NAME_C = "測試C"

    results = []
    passed = 0
    failed = 0

    def check(name, condition, detail=""):
        nonlocal passed, failed
        if condition:
            results.append(f"✅ {name}" + (f"：{detail}" if detail else ""))
            passed += 1
        else:
            results.append(f"❌ {name}" + (f"：{detail}" if detail else ""))
            failed += 1

    def clear_all():
        cur = get_cursor()
        if cur:
            cur.execute("DELETE FROM signups WHERE group_id=%s", (group_id,))
            cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))
            cur.execute("DELETE FROM yearly_members WHERE group_id=%s", (group_id,))
            cur.execute("DELETE FROM users WHERE group_id=%s", (group_id,))
            cur.execute("DELETE FROM config WHERE group_id=%s AND key='zero_play_open_triggered'", (group_id,))

    def set_auto_open_config_test():
        set_auto_open_config(group_id, 'auto_open_days', '3')
        set_auto_open_config(group_id, 'auto_open_time', '20:00')
        set_auto_open_config(group_id, 'zero_play_open_days', '4')
        set_auto_open_config(group_id, 'zero_play_open_time', '11:30')
        set_auto_open_config(group_id, 'auto_schedule_days', '4')
        set_auto_open_config(group_id, 'auto_schedule_time', '23:00')

    def clear_auto_open_config():
        set_auto_open_config(group_id, 'auto_open_days', '')
        set_auto_open_config(group_id, 'auto_open_time', '')
        set_auto_open_config(group_id, 'zero_play_open_days', '')
        set_auto_open_config(group_id, 'zero_play_open_time', '')
        set_auto_open_config(group_id, 'auto_schedule_days', '')
        set_auto_open_config(group_id, 'auto_schedule_time', '')

    def reset_config():
        cur = get_cursor()
        if cur:
            cur.execute("DELETE FROM config WHERE group_id='default' AND key='signup_limit'")
            cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'signup_limit', '10') ON CONFLICT DO NOTHING")
            cur.execute("DELETE FROM config WHERE group_id='default' AND key='event_duration'")
            cur.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'event_duration', '30') ON CONFLICT DO NOTHING")

    try:
        clear_all()
        reset_config()

        results.append("\n【Phase A - 自動開團設定】")
        set_auto_open_config_test()
        auto_days = get_auto_open_config(group_id, 'auto_open_days')
        auto_time = get_auto_open_config(group_id, 'auto_open_time')
        zero_days = get_zero_play_open_config(group_id, 'zero_play_open_days')
        zero_time = get_zero_play_open_config(group_id, 'zero_play_open_time')
        check("設定自動開團日", auto_days == '3', f"{auto_days}=3 正確")
        check("設定自動開團時間", auto_time == '20:00', f"{auto_time}=20:00 正確")
        check("設定零打日", zero_days == '4', f"{zero_days}=4 正確")
        check("設定零打時間", zero_time == '11:30', f"{zero_time}=11:30 正確")

        clear_auto_open_config()
        reset_config()

        results.append("\n【Phase B - 自動開團流程】")
        clear_all()
        reset_config()
        set_auto_open_config_test()
        set_auto_trigger_date(group_id, 'auto_open_triggered_date')
        coach_open_event(ADMIN_ID, group_id, "系統", auto_opened=True)
        is_auto = is_event_auto_opened(group_id)
        check("自動開團", is_auto, f"auto_opened={is_auto} 正確")

        add_yearly_member(TEST_A, group_id, NAME_A)
        is_yearly_a = is_yearly_member(TEST_A, group_id)
        check("年繳會員設定", is_yearly_a, f"is_yearly={is_yearly_a} 正確")

        atomic_signup(TEST_A, group_id, NAME_A)
        add_count(TEST_A, group_id, 1, NAME_A)
        should_allow_yearly = should_allow_signup(TEST_A, group_id)
        check("年繳會員應可報名", should_allow_yearly == True, f"{should_allow_yearly} 正確")

        count_a = get_count(TEST_A, group_id)
        total = get_total_count(group_id)
        check("年繳會員+1", count_a == 1 and total == 2, f"count_a={count_a}, total={total} 正確")

        should_allow_non_yearly = should_allow_signup(TEST_B, group_id)
        check("非年繳會員應被拒", should_allow_non_yearly == False, f"{should_allow_non_yearly} 正確")

        b_before = None
        cur = get_cursor()
        if cur:
            cur.execute("SELECT user_id FROM signups WHERE user_id=%s AND group_id=%s", (TEST_B, group_id))
            b_before = cur.fetchone()
        check("非年繳+1被拒", b_before is None, f"B未報名={b_before is None} 正確")

        set_zero_play_open_triggered(group_id, True)
        zero_triggered = get_zero_play_open_triggered(group_id)
        check("零打觸發", zero_triggered, f"zero_triggered={zero_triggered} 正確")

        should_allow_after_zero = should_allow_signup(TEST_B, group_id)
        check("零打後應開放", should_allow_after_zero == True, f"{should_allow_after_zero} 正確")

        atomic_signup(TEST_B, group_id, NAME_B)
        add_count(TEST_B, group_id, 1, NAME_B)
        count_b = get_count(TEST_B, group_id)
        total = get_total_count(group_id)
        check("非年繳零打後+1", count_b == 1 and total == 3, f"count_b={count_b}, total={total} 正確")

        should_allow_yearly_after_zero = should_allow_signup(TEST_A, group_id)
        check("年繳零打後仍可報名", should_allow_yearly_after_zero == True, f"{should_allow_yearly_after_zero} 正確")

        results.append("\n【Phase C - 報名人數上限】")
        clear_all()
        reset_config()
        new_limit = 3
        cur = get_cursor()
        if cur:
            cur.execute("DELETE FROM config WHERE group_id=%s AND key='signup_limit'", (group_id,))
            cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, 'signup_limit', %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, str(new_limit), str(new_limit)))
        limit = get_signup_limit(group_id)
        check("設定報名上限", limit == new_limit, f"limit={limit} 正確")

        coach_open_event(TEST_A, group_id, NAME_A)
        add_yearly_member(TEST_B, group_id, NAME_B)
        atomic_signup(TEST_B, group_id, NAME_B)
        add_count(TEST_B, group_id, 1, NAME_B)
        total = get_total_count(group_id)
        check("2人報名", total == 3, f"total={total} 正確")

        atomic_signup(TEST_C, group_id, NAME_C)
        add_count(TEST_C, group_id, 1, NAME_C)
        total = get_total_count(group_id)
        check("3人達上限", total == 4, f"total={total} 正確")

        add_yearly_member(TEST_C, group_id, NAME_C)
        signed_up_when_full = atomic_signup(TEST_C, group_id, NAME_C)
        check("年繳會員達上限後報名", signed_up_when_full == False, f"被拒絕={not signed_up_when_full} 正確")

        c_in_signups = None
        cur = get_cursor()
        if cur:
            cur.execute("SELECT user_id FROM signups WHERE user_id=%s AND group_id=%s", (TEST_C, group_id))
            c_in_signups = cur.fetchone()
        check("年繳會員未加入名單", c_in_signups is None, f"C不在名單={c_in_signups is None} 正確")

        results.append("\n【Phase D - 活動過期】")
        clear_all()
        reset_config()
        coach_open_event(TEST_A, group_id, NAME_A)
        cur = get_cursor()
        if cur:
            cur.execute("UPDATE events SET expires_at = 0 WHERE group_id=%s", (group_id,))
        is_active = is_event_active(group_id)
        check("活動過期", not is_active, f"is_active={is_active} 正確")

        b_can_signup = False
        cur = get_cursor()
        if cur:
            cur.execute("SELECT user_id FROM signups WHERE user_id=%s AND group_id=%s", (TEST_B, group_id))
            b_can_signup = cur.fetchone() is not None
        check("過期後+1失敗", not b_can_signup, f"B無法報名={not b_can_signup} 正確")

        results.append("\n【Phase E - 多人報名】")
        clear_all()
        reset_config()
        coach_open_event(TEST_A, group_id, NAME_A)
        atomic_signup(TEST_B, group_id, NAME_B)
        atomic_signup(TEST_C, group_id, NAME_C)
        add_count(TEST_A, group_id, 2, NAME_A)
        add_count(TEST_B, group_id, 3, NAME_B)
        add_count(TEST_C, group_id, 1, NAME_C)
        count_a = get_count(TEST_A, group_id)
        count_b = get_count(TEST_B, group_id)
        count_c = get_count(TEST_C, group_id)
        total = get_total_count(group_id)
        check("多人報名統計", count_a == 3 and count_b == 3 and count_c == 1 and total == 7,
              f"A={count_a}, B={count_b}, C={count_c}, total={total} 正確")

        results.append("\n【Phase F - 活動結束】")
        clear_all()
        reset_config()
        coach_open_event(TEST_A, group_id, NAME_A)
        add_count(TEST_A, group_id, 1, NAME_A)
        count_before = get_count(TEST_A, group_id)
        is_active_before = is_event_active(group_id)
        check("結束前有活動", is_active_before and count_before == 2, f"active={is_active_before}, count={count_before}")

        cur = get_cursor()
        if cur:
            cur.execute("DELETE FROM signups WHERE group_id=%s", (group_id,))
            cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))
        is_active_after = is_event_active(group_id)
        count_after = get_count(TEST_A, group_id)
        check("活動結束", not is_active_after, f"active={is_active_after} 正確")
        check("帳單保留", count_after == 2, f"count={count_after} 正確")

        coach_open_event(TEST_A, group_id, NAME_A)
        count_accumulated = get_count(TEST_A, group_id)
        check("重新開團累加", count_accumulated == 3, f"count={count_accumulated} 正確")

        results.append("\n【Phase G - 每週循環持續性】")
        clear_all()
        reset_config()

        # G1: 空設定不觸發
        set_auto_open_config(group_id, 'auto_open_days', '')
        set_auto_open_config(group_id, 'auto_open_time', '')
        should_trigger_empty = should_auto_open(group_id)
        check("空設定不觸發", should_trigger_empty == False, f"{should_trigger_empty} 正確")

        # G2: 跨週觸發 date 重置
        clear_all()
        reset_config()
        set_auto_open_config_test()
        
        # 模擬昨天已觸發
        cur = get_cursor()
        if cur:
            taiwan_tz = timezone(timedelta(hours=8))
            yesterday = (datetime.now(taiwan_tz) - timedelta(days=1)).strftime("%Y-%m-%d")
            cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, %s, %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, 'auto_open_triggered_date', yesterday, yesterday))
            
        last_trigger = get_auto_trigger_date(group_id, 'auto_open_triggered_date')
        today_str = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        
        # 只要 last_trigger 不是今天，程式就不會擋下觸發
        check("舊日期不會被阻擋", last_trigger != today_str, f"last={last_trigger}, today={today_str}")

    finally:
        clear_all()
        clear_auto_open_config()
        reset_config()

    total_tests = passed + failed
    summary = f"✅ 全部通過 {passed}/{total_tests}" if failed == 0 else f"⚠️ 通過 {passed}/{total_tests}，失敗 {failed} 項"

    msg = "🧪 開團功能測試報告\n"
    msg += "\n".join(results)
    msg += f"\n\n{summary}"
    msg += "\n🧹 測試資料已清除"

    line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))




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
                        if current_total + n > limit:
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
                            cur.execute("UPDATE events SET total_count = GREATEST(total_count - %s, 0) WHERE group_id=%s", (n, group_id))
                            if current_count - n == 0:
                                cur.execute("DELETE FROM signups WHERE user_id=%s AND group_id=%s", (target_user_id, group_id))
                            else:
                                cur.execute("UPDATE signups SET count = GREATEST(count - %s, 0) WHERE user_id=%s AND group_id=%s", (n, target_user_id, group_id))
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
            msg += "已繳 @人：清除用戶帳目（繳費確認，可一次 @多人）\n"
            msg += "年繳加入 / 年繳移除 @人：管理年繳會員（可一次 @多人）\n"
            msg += "年繳名單：查看年繳會員\n"
            msg += "年繳全部移除：移除所有年繳會員\n"
            msg += "重置全部：清除所有紀錄資料\n"
            msg += "活動結束：提早結束活動\n"
            msg += "全部帳單：查看所有群組的欠款\n"
            msg += "退出群組：清除資料並退出\n"
            msg += "狀態：查看系統狀態\n"
            msg += "測試：執行自動化測試\n"
            msg += "開團功能測試：執行開團功能測試\n"
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
                cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, 'signup_limit', %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, str(new_limit), str(new_limit)))
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
        mentioned, _ = get_mentioned_users(event, ADMIN_ID)
        if mentioned:
            results = []
            for m_user_id, m_name in mentioned:
                clear_user(m_user_id, group_id)
                name_display = f"@{m_name}" if m_name else m_user_id[-4:]
                results.append(name_display)
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已清除 {', '.join(results)} 的帳目（目前 0 次）"))
        else:
            # Try single name
            parts = text.replace("已繳", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts, group_id)
                if target_user_id:
                    clear_user(target_user_id, group_id)
                    name_display = f"@{parts}" if len(parts) > 4 else parts
                    line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已清除 {name_display} 的帳目（目前 0 次）"))
                else:
                    line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 找不到指定用戶"))
            else:
                line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「已繳 @人」格式（可一次 @多人）"))
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
        mentioned, _ = get_mentioned_users(event, ADMIN_ID)
        if mentioned:
            results = []
            for m_user_id, m_name in mentioned:
                add_yearly_member(m_user_id, group_id, m_name or "")
                name_display = f"@{m_name}" if m_name else m_user_id[-4:]
                results.append(name_display)
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已將 {', '.join(results)} 設為年繳會員"))
        else:
            parts = text.replace("年繳加入", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts, group_id)
                if target_user_id:
                    add_yearly_member(target_user_id, group_id, parts)
                    name_display = f"@{parts}" if len(parts) > 4 else parts
                    line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已將 {name_display} 設為年繳會員"))
                else:
                    line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 找不到指定用戶"))
            else:
                line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「年繳加入 @人」格式（可一次 @多人）"))
        return

    if text.startswith("年繳移除") and user_id == ADMIN_ID:
        mentioned, _ = get_mentioned_users(event, ADMIN_ID)
        if mentioned:
            results = []
            for m_user_id, m_name in mentioned:
                remove_yearly_member(m_user_id, group_id)
                name_display = f"@{m_name}" if m_name else m_user_id[-4:]
                results.append(name_display)
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已將 {', '.join(results)} 移出年繳會員"))
        else:
            parts = text.replace("年繳移除", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts, group_id)
                if target_user_id:
                    remove_yearly_member(target_user_id, group_id)
                    name_display = f"@{parts}" if len(parts) > 4 else parts
                    line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已將 {name_display} 移出年繳會員"))
                else:
                    line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 找不到指定用戶"))
            else:
                line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「年繳移除 @人」格式（可一次 @多人）"))
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
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 格式錯誤，請輸入：開團設定 3 20:00 4 11:30 4 23:00 [12]\n（開團日 開團時間 零打日 零打時間 排程日 排程時間 可選：報名人數上限）"))
            return
        
        try:
            open_days = parts_list[0]
            open_time = parts_list[1]
            zero_days = parts_list[2]
            zero_time = parts_list[3]
            schedule_days = parts_list[4]
            schedule_time = parts_list[5]
            
            signup_limit = None
            if len(parts_list) >= 7:
                signup_limit = int(parts_list[6])
                if signup_limit < 1 or signup_limit > 100:
                    raise ValueError("signup_limit out of range")
            
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
            
            if signup_limit is not None:
                cur = get_cursor()
                if cur:
                    cur.execute("INSERT INTO config (group_id, key, value) VALUES (%s, 'signup_limit', %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, str(signup_limit), str(signup_limit)))
            
            day_names = {1: "週一", 2: "週二", 3: "週三", 4: "週四", 5: "週五", 6: "週六", 7: "週日"}
            open_display = ", ".join([day_names.get(d, str(d)) for d in open_days_list])
            zero_display = ", ".join([day_names.get(d, str(d)) for d in zero_days_list])
            schedule_display = ", ".join([day_names.get(d, str(d)) for d in schedule_days_list])
            
            limit_msg = f"\n  報名人數上限：{signup_limit} 人" if signup_limit else ""
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 開團設定已設定：\n  自動開團：{open_display} {open_time}\n  零打開放：{zero_display} {zero_time}\n  自動排程：{schedule_display} {schedule_time}{limit_msg}\n\n※ 自動開團後僅年繳會員可報名\n※ 零打開放後所有人都可報名\n※ 自動排程結束活動並發送名單"))
        except:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 格式錯誤，請輸入：開團設定 3 20:00 4 11:30 4 23:00 [12]\n（開團日 開團時間 零打日 零打時間 排程日 排程時間 可選：報名人數上限）"))
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

    if text == "測試" and user_id == ADMIN_ID:
        run_bot_test(group_id, reply_token, price)
        return

    if text == "開團功能測試" and user_id == ADMIN_ID:
        run_open_group_test(group_id, reply_token)
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
        if current_total + n > limit:
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
            cur.execute("UPDATE events SET total_count = GREATEST(total_count - %s, 0) WHERE group_id=%s", (n, group_id))
            if current_count - n == 0:
                cur.execute("DELETE FROM signups WHERE user_id=%s AND group_id=%s", (user_id, group_id))
            else:
                cur.execute("UPDATE signups SET count = GREATEST(count - %s, 0) WHERE user_id=%s AND group_id=%s", (n, user_id, group_id))
        count = get_total_count(group_id)
        if count < 0:
            count = 0
        if current_count - n == 0:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"本次不報名，累計人數 {count} 人"))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 累計人數 {count} 人"))
        return
