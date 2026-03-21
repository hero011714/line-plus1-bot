import os
import psycopg2
import asyncio
import time
import psutil
from fastapi import FastAPI, Request
from linebot import LineBotApi, WebhookHandler
from linebot.models import MessageEvent, TextMessage, TextSendMessage
from linebot.exceptions import InvalidSignatureError

CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
ADMIN_ID = os.getenv("ADMIN_ID", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

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
            CREATE TABLE IF NOT EXISTS whitelist (
                user_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                name TEXT DEFAULT '',
                PRIMARY KEY (user_id, group_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signups (
                user_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                name TEXT DEFAULT '',
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
        cur.execute("DELETE FROM whitelist WHERE group_id=%s", (group_id,))
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
        cur.execute("SELECT name FROM whitelist WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        result2 = cur.fetchone()
        if result2 and result2[0]:
            return result2[0]
        return user_id[-4:]
    except:
        return user_id[-4:]

def is_whitelist(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return False
    try:
        cur.execute("SELECT 1 FROM whitelist WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        return cur.fetchone() is not None
    except:
        return False

def add_to_whitelist(user_id, group_id, name=""):
    old_count = get_count(user_id, group_id)
    cur = get_cursor()
    if not cur:
        return old_count
    try:
        cur.execute("INSERT INTO whitelist (user_id, group_id, name) VALUES (%s, %s, %s) ON CONFLICT (user_id, group_id) DO UPDATE SET name = %s", (user_id, group_id, name, name))
        cur.execute("UPDATE users SET count=0 WHERE user_id=%s AND group_id=%s", (user_id, group_id))
    except:
        pass
    return old_count

def remove_from_whitelist(user_id, group_id):
    cur = get_cursor()
    if not cur:
        return
    try:
        cur.execute("DELETE FROM whitelist WHERE user_id=%s AND group_id=%s", (user_id, group_id))
    except:
        pass

def get_whitelist(group_id):
    cur = get_cursor()
    if not cur:
        return []
    try:
        cur.execute("SELECT user_id, name FROM whitelist WHERE group_id=%s", (group_id,))
        return cur.fetchall()
    except:
        return []

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
        cur.execute("SELECT user_id FROM whitelist WHERE name=%s AND group_id=%s", (name, group_id))
        result2 = cur.fetchone()
        return result2[0] if result2 else None
    except:
        return None

def get_group_stats(group_id):
    cur = get_cursor()
    if not cur:
        return 0, 0
    try:
        cur.execute("SELECT COUNT(*) FROM users WHERE group_id=%s", (group_id,))
        user_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM whitelist WHERE group_id=%s", (group_id,))
        whitelist_count = cur.fetchone()[0]
        return user_count, whitelist_count
    except:
        return 0, 0

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

def coach_open_event(user_id, group_id, user_name):
    conn_local = get_db()
    if not conn_local:
        return 0
    cur = conn_local.cursor()
    try:
        now = int(time.time())
        expires = now + 48 * 3600
        cur.execute("DELETE FROM signups WHERE group_id=%s", (group_id,))
        cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))
        cur.execute("""
            INSERT INTO events (group_id, started_at, expires_at, total_count)
            VALUES (%s, %s, %s, 1)
        """, (group_id, now, expires))
        cur.execute("""
            INSERT INTO signups (user_id, group_id, name, signup_time)
            VALUES (%s, %s, %s, %s)
        """, (user_id, group_id, user_name, now))
        cur.execute("""
            INSERT INTO users (user_id, group_id, name, count, last_fetch)
            VALUES (%s, %s, %s, 1, 0)
            ON CONFLICT (user_id, group_id)
            DO UPDATE SET count = users.count + 1, name = EXCLUDED.name
        """, (user_id, group_id, user_name))
        cur.execute("SELECT total_count FROM events WHERE group_id=%s", (group_id,))
        result = cur.fetchone()
        return result[0] if result else 1
    except Exception as e:
        print(f"coach_open_event error: {e}")
        return 1

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
        cur.execute("SELECT total_count FROM events WHERE group_id=%s", (group_id,))
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
            INSERT INTO signups (user_id, group_id, name, signup_time)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, group_id)
            DO UPDATE SET name = EXCLUDED.name
        """, (user_id, group_id, name, now))
        return True
    except Exception as e:
        print(f"atomic_signup error: {e}")
        return False

def get_mentioned_users(event, exclude_id=None):
    mentioned = []
    group_id = get_group_id(event)
    mention = getattr(event.message, 'mention', None)
    print(f"[DEBUG get_mentioned_users] mention={mention}, exclude_id={exclude_id}")
    if mention and hasattr(mention, 'mentionees'):
        for m in mention.mentionees:
            print(f"[DEBUG get_mentioned_users] mentionee user_id={m.user_id}, is_middle={getattr(m, 'is_middle', None)}")
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

@app.get("/")
async def health_check():
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
    raw_text = event.message.text
    text = raw_text.strip()
    print(f"[DEBUG] raw_text={repr(raw_text)}, stripped={repr(text)}, starts_plus={text.startswith('+')}, starts_at={text.startswith('@')}, equals_plus={text=='+'}")
    group_id = get_group_id(event)
    price = get_price(group_id)
    user_name = get_user_name(user_id, group_id)
    reply_token = event.reply_token
    source_type = event.source.type
    
    if should_fetch_profile(user_id, group_id):
        try:
            profile = line_bot_api.get_profile(user_id)
            user_name = profile.display_name
            add_user(user_id, group_id, user_name)
            update_user_name(user_id, group_id, user_name)
        except:
            add_user(user_id, group_id, user_name)
    else:
        add_user(user_id, group_id, user_name)
    
    try:
        mentioned, _ = get_mentioned_users(event, user_id)
        for m_user_id, m_name in mentioned:
            pass
    except:
        pass

    if is_whitelist(user_id, group_id):
        return

    if text in ["幫助", "help"]:
        is_admin = (user_id == ADMIN_ID)
        msg = "📋 記帳機器人指令：\n\n"
        msg += "【指令】\n"
        msg += "今天打球+1 / 明天打球+1：開團\n"
        msg += "+：+1 次（需先開團）\n"
        msg += "+N：+N 次（上限 10）\n"
        msg += "-：-1 次\n"
        msg += "-N：-N 次\n"
        msg += "查帳：查看自己的帳目\n"
        if is_admin:
            msg += "\n【管理員指令】\n"
            msg += "設定單價 [數字]\n"
            msg += "設定報名人數上限 [數字]\n"
            msg += "白名單加入 @人\n"
            msg += "白名單移除 @人\n"
            msg += "白名單：查看白名單\n"
            msg += "已繳 @人：清除帳目\n"
            msg += "@人 +N：替他人記錄（教練）\n"
            msg += "重置全部：清除所有紀錄資料\n"
            msg += "全部帳單：查看所有群組的欠款\n"
            msg += "退出群組：清除資料並退出\n"
            msg += "狀態：查看系統狀態"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    if text == "狀態" and user_id == ADMIN_ID:
        try:
            cur = get_cursor()
            user_count, whitelist_count = get_group_stats(group_id)
            
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
            msg += f"   白名單：{whitelist_count} 人\n"
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

    if text == "重置全部" and user_id == ADMIN_ID:
        clear_all_users(group_id)
        clear_signups(group_id)
        cur = get_cursor()
        if cur:
            try:
                cur.execute("DELETE FROM events WHERE group_id=%s", (group_id,))
            except:
                pass
        line_bot_api.reply_message(reply_token, TextSendMessage(text="✅ 已清除所有紀錄資料"))
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
            if gid != current_gid:
                current_gid = gid
                group_label = gid if not gid.startswith('private_') else '私聊'
                msg += f"【{group_label}】\n"
            cur.execute("SELECT 1 FROM whitelist WHERE user_id=%s AND group_id=%s", (uid, gid))
            if cur.fetchone():
                continue
            display_name = name if name else uid[-4:]
            cur.execute("SELECT value FROM config WHERE group_id='default' AND key='price'")
            price_row = cur.fetchone()
            group_price = int(price_row[0]) if price_row else 50
            msg += f"  @{display_name}: {count}次 / {count*group_price}元\n"
            has_debt = True
        if not has_debt:
            msg = "✅ 目前無欠款"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    if text == "白名單" and user_id == ADMIN_ID:
        rows = get_whitelist(group_id)
        if rows:
            msg = "📋 白名單：\n"
            for uid, name in rows:
                msg += f"✅ @{name}\n"
        else:
            msg = "📋 白名單是空的"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    if text == "查帳":
        if not is_signed_up(user_id, group_id):
            line_bot_api.reply_message(reply_token, TextSendMessage(text="尚無資料"))
            return
        count = get_count(user_id, group_id)
        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"@{user_name} 目前 {count} 次，應繳 {count*price} 元"))
        return

    if text.startswith("白名單加入") and user_id == ADMIN_ID:
        target_user_id = None
        target_name = None
        
        mentioned, _ = get_mentioned_users(event, ADMIN_ID)
        for m_user_id, m_name in mentioned:
            target_user_id = m_user_id
            target_name = m_name
            break
        
        if not target_user_id:
            parts = text.replace("白名單加入", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts, group_id)
                target_name = parts
        
        if target_user_id:
            old_count = add_to_whitelist(target_user_id, group_id, target_name or "")
            name_display = f"@{target_name}" if target_name else target_user_id[-4:]
            msg = f"✅ 已將 {name_display} 加入白名單"
            if old_count > 0:
                msg += f"\n（已清除 {old_count} 次帳目）"
            line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「白名單加入 @人」格式"))
        return

    if text.startswith("白名單移除") and user_id == ADMIN_ID:
        mentioned, _ = get_mentioned_users(event, ADMIN_ID)
        target_user_id = None
        target_name = None
        for m_user_id, m_name in mentioned:
            target_user_id = m_user_id
            target_name = m_name
            break
        
        if not target_user_id:
            parts = text.replace("白名單移除", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts, group_id)
                target_name = parts
        
        if target_user_id:
            remove_from_whitelist(target_user_id, group_id)
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已將 @{target_name} 移出白名單"))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「白名單移除 @人」格式"))
        return

    if text.startswith("已繳") and user_id == ADMIN_ID:
        target_user_id = None
        target_name = None
        
        mentioned, _ = get_mentioned_users(event, ADMIN_ID)
        for m_user_id, m_name in mentioned:
            target_user_id = m_user_id
            target_name = m_name
            break
        
        if not target_user_id:
            parts = text.replace("已繳", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts, group_id)
                target_name = parts
        
        if target_user_id:
            clear_user(target_user_id, group_id)
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 累計人數 0 人"))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「已繳 @人」格式"))
        return

    if text.startswith("@") and user_id == ADMIN_ID:
        print(f"[DEBUG] ENTERED @ BLOCK: text={repr(text)}, parts={text.split()}")
        parts = text.split()
        if len(parts) >= 2:
            target_name = parts[0].replace("@", "")
            
            mentioned, _ = get_mentioned_users(event, ADMIN_ID)
            target_user_id = None
            for m_user_id, m_name in mentioned:
                target_user_id = m_user_id
                target_name = m_name
                break
            
            if not target_user_id:
                target_user_id = get_user_by_name(target_name, group_id)
            
            if not target_user_id:
                line_bot_api.reply_message(reply_token, TextSendMessage(text=f"❌ 找不到 @{target_name}，請先傳訊息讓機器人學習"))
                return
            
            if target_user_id and len(parts) >= 2:
                count_text = parts[1]
                if count_text in ["+", "++"]:
                    n = 1
                elif count_text.startswith("+"):
                    try:
                        n = int(count_text.lstrip("+"))
                    except:
                        n = 0
                else:
                    n = 0
                
                if n > 0:
                    if is_whitelist(target_user_id, group_id):
                        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"❌ @{target_name} 在白名單中，無法記錄"))
                        return
                    if not is_signed_up(target_user_id, group_id):
                        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"❌ @{target_name} 尚未報到"))
                        return
                    limit = get_signup_limit(group_id)
                    current_total = get_total_count(group_id)
                    if current_total + n > limit:
                        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 人數已滿（{current_total}/{limit}），無法報名"))
                        return
                    add_count(target_user_id, group_id, n)
                    add_total_count(group_id, n)
                    new_count = get_total_count(group_id)
                    line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 累計人數 {new_count} 人"))
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
        print(f"[DEBUG] ENTERED + BLOCK: text={repr(text)}, raw={repr(raw_text)}")
        if not is_event_active(group_id):
            line_bot_api.reply_message(reply_token, TextSendMessage(text="活動尚未開始"))
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
        cur = get_cursor()
        if cur:
            cur.execute("""
                INSERT INTO users (user_id, group_id, count, last_fetch)
                VALUES (%s, %s, 0, 0)
                ON CONFLICT (user_id, group_id) 
                DO UPDATE SET count = GREATEST(users.count - %s, 0)
            """, (user_id, group_id, n))
            cur.execute("""
                UPDATE events SET total_count = GREATEST(total_count - %s, 0) WHERE group_id = %s
            """, (n, group_id))
        count = get_total_count(group_id)
        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 累計人數 {count} 人"))
        return

    line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 指令錯誤，請輸入「幫助」查看指令列表"))
