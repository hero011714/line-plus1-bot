import os
import psycopg2
from psycopg2 import pool
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

db_pool = None
conn = None
cursor = None

def init_db():
    global db_pool, conn, cursor
    if not DATABASE_URL:
        print("WARNING: DATABASE_URL not set!")
        return False
    try:
        db_pool = pool.ThreadedConnectionPool(1, 10, DATABASE_URL)
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                name TEXT DEFAULT '',
                count INTEGER DEFAULT 0,
                last_fetch INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, group_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS config (
                group_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                PRIMARY KEY (group_id, key)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS whitelist (
                user_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                name TEXT DEFAULT '',
                PRIMARY KEY (user_id, group_id)
            )
        """)
        cursor.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'price', '50') ON CONFLICT DO NOTHING")
        cursor.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'max_per_action', '10') ON CONFLICT DO NOTHING")
        cursor.execute("INSERT INTO config (group_id, key, value) VALUES ('default', 'fetch_interval', '86400') ON CONFLICT DO NOTHING")
        conn.commit()
        print("Database initialized successfully")
        return True
    except Exception as e:
        print(f"Database initialization failed: {e}")
        return False

init_db()

_pending_updates = []

def commit_all():
    global conn, cursor, _pending_updates
    if not conn or not cursor:
        _pending_updates = []
        return
    try:
        for sql, params in _pending_updates:
            cursor.execute(sql, params)
        conn.commit()
    except Exception as e:
        print(f"Commit error: {e}")
        conn.rollback()
    _pending_updates = []

def get_group_id(event):
    source_type = event.source.type
    if source_type == 'group':
        return event.source.group_id
    elif source_type == 'room':
        return event.source.room_id
    else:
        return 'private_' + event.source.user_id

def clear_group_data(group_id):
    global cursor, conn
    if not cursor:
        return
    try:
        cursor.execute("DELETE FROM whitelist WHERE group_id=%s", (group_id,))
        cursor.execute("DELETE FROM users WHERE group_id=%s", (group_id,))
        cursor.execute("DELETE FROM config WHERE group_id=%s", (group_id,))
        conn.commit()
    except:
        pass

def get_price(group_id):
    global cursor
    if not cursor:
        return 50
    try:
        cursor.execute("SELECT value FROM config WHERE group_id=%s AND key='price'", (group_id,))
        result = cursor.fetchone()
        if result:
            return int(result[0])
        cursor.execute("INSERT INTO config (group_id, key, value) VALUES (%s, 'price', '50') ON CONFLICT DO NOTHING", (group_id,))
        conn.commit()
        return 50
    except:
        return 50

def get_max_per_action():
    global cursor
    if not cursor:
        return 10
    try:
        cursor.execute("SELECT value FROM config WHERE group_id='default' AND key='max_per_action'")
        result = cursor.fetchone()
        return int(result[0]) if result else 10
    except:
        return 10

def set_price(group_id, price):
    global cursor, conn
    if not cursor:
        return
    try:
        cursor.execute("INSERT INTO config (group_id, key, value) VALUES (%s, 'price', %s) ON CONFLICT (group_id, key) DO UPDATE SET value = %s", (group_id, str(price), str(price)))
        conn.commit()
    except:
        pass

def add_user(user_id, group_id, name=""):
    global cursor, conn
    if not cursor:
        return
    try:
        cursor.execute("""
            INSERT INTO users (user_id, group_id, name, count, last_fetch)
            VALUES (%s, %s, %s, 0, 0)
            ON CONFLICT (user_id, group_id) 
            DO UPDATE SET name = EXCLUDED.name
        """, (user_id, group_id, name))
        conn.commit()
    except Exception as e:
        print(f"add_user error: {e}")
        pass

def update_user_name(user_id, group_id, name):
    global cursor, conn
    if not cursor:
        return
    try:
        cursor.execute("UPDATE users SET name=%s, last_fetch=%s WHERE user_id=%s AND group_id=%s", (name, int(time.time()), user_id, group_id))
    except:
        pass

def should_fetch_profile(user_id, group_id):
    global cursor
    if not cursor:
        return True
    try:
        cursor.execute("SELECT last_fetch FROM users WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        result = cursor.fetchone()
        if not result or result[0] == 0:
            return True
        cursor.execute("SELECT value FROM config WHERE group_id='default' AND key='fetch_interval'")
        result2 = cursor.fetchone()
        interval = int(result2[0]) if result2 else 86400
        return (int(time.time()) - result[0]) > interval
    except:
        return True

def get_user_name(user_id, group_id):
    global cursor
    if not cursor:
        return user_id[-4:]
    try:
        cursor.execute("SELECT name FROM users WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        cursor.execute("SELECT name FROM whitelist WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        result2 = cursor.fetchone()
        if result2 and result2[0]:
            return result2[0]
        return user_id[-4:]
    except:
        return user_id[-4:]

def is_whitelist(user_id, group_id):
    global cursor
    if not cursor:
        return False
    try:
        cursor.execute("SELECT 1 FROM whitelist WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        return cursor.fetchone() is not None
    except:
        return False

def add_to_whitelist(user_id, group_id, name=""):
    global cursor, conn
    old_count = get_count(user_id, group_id)
    if not cursor:
        return old_count
    try:
        cursor.execute("INSERT INTO whitelist (user_id, group_id, name) VALUES (%s, %s, %s) ON CONFLICT (user_id, group_id) DO UPDATE SET name = %s", (user_id, group_id, name, name))
        cursor.execute("UPDATE users SET count=0 WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        conn.commit()
    except:
        pass
    return old_count

def remove_from_whitelist(user_id, group_id):
    global cursor, conn
    if not cursor:
        return
    try:
        cursor.execute("DELETE FROM whitelist WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        conn.commit()
    except:
        pass

def get_whitelist(group_id):
    global cursor
    if not cursor:
        return []
    try:
        cursor.execute("SELECT user_id, name FROM whitelist WHERE group_id=%s", (group_id,))
        return cursor.fetchall()
    except:
        return []

def add_count(user_id, group_id, n=1, name=""):
    global cursor, conn
    if not cursor:
        return
    try:
        cursor.execute("""
            INSERT INTO users (user_id, group_id, name, count, last_fetch)
            VALUES (%s, %s, %s, %s, 0)
            ON CONFLICT (user_id, group_id) 
            DO UPDATE SET count = users.count + EXCLUDED.count
        """, (user_id, group_id, name, n))
        conn.commit()
    except Exception as e:
        print(f"add_count error: {e}")
        pass

def get_count(user_id, group_id):
    global cursor
    if not cursor:
        return 0
    try:
        cursor.execute("SELECT count FROM users WHERE user_id=%s AND group_id=%s", (user_id, group_id))
        result = cursor.fetchone()
        return result[0] if result else 0
    except:
        return 0

def clear_user(user_id, group_id):
    global cursor
    if not cursor:
        return
    try:
        cursor.execute("UPDATE users SET count=0 WHERE user_id=%s AND group_id=%s", (user_id, group_id))
    except:
        pass

def clear_all_users(group_id):
    global cursor
    if not cursor:
        return
    try:
        cursor.execute("UPDATE users SET count=0 WHERE group_id=%s", (group_id,))
    except:
        pass

def get_all_users(group_id):
    global cursor
    if not cursor:
        return []
    try:
        cursor.execute("SELECT user_id, name, count FROM users WHERE group_id=%s", (group_id,))
        return cursor.fetchall()
    except:
        return []

def get_user_by_name(name, group_id):
    global cursor
    if not cursor:
        return None
    try:
        cursor.execute("SELECT user_id FROM users WHERE name=%s AND group_id=%s", (name, group_id))
        result = cursor.fetchone()
        return result[0] if result else None
    except:
        return None

def get_group_stats(group_id):
    global cursor
    if not cursor:
        return 0, 0
    try:
        cursor.execute("SELECT COUNT(*) FROM users WHERE group_id=%s", (group_id,))
        user_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM whitelist WHERE group_id=%s", (group_id,))
        whitelist_count = cursor.fetchone()[0]
        return user_count, whitelist_count
    except:
        return 0, 0

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
    finally:
        commit_all()

    return "OK"

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()
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
        except Exception as e:
            print(f"Profile fetch error: {e}")
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
        msg = "📋 記帳機器人指令：\n\n"
        msg += "【一般指令】\n"
        msg += "+ 或 ++：+1 次\n"
        msg += "+N：+N 次（上限 10）\n"
        msg += "- 或 --：-1 次\n"
        msg += "-N：-N 次\n"
        msg += "查帳：查看自己的帳目\n\n"
        msg += "【管理員指令】\n"
        msg += "設定單價 [數字]\n"
        msg += "白名單加入 @人\n"
        msg += "白名單移除 @人\n"
        msg += "白名單：查看白名單\n"
        msg += "已繳 @人：清除帳目\n"
        msg += "@人 +N：替他人記錄（管理員）\n"
        msg += "重置全部：清除所有人帳目\n"
        msg += "全部帳單：查看所有人的欠款\n"
        msg += "退出群組：清除資料並退出\n"
        msg += "狀態：查看系統狀態\n\n"
        msg += "【特殊指令】\n"
        msg += "群組ID：查看目前群組ID"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    if text == "群組ID":
        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"📌 目前群組ID：\n{group_id}"))
        return

    if text == "狀態" and user_id == ADMIN_ID:
        try:
            disk = psutil.disk_usage('/')
            disk_used = disk.used / (1024 * 1024 * 1024)
            disk_total = disk.total / (1024 * 1024 * 1024)
            disk_percent = disk.percent
            
            user_count, whitelist_count = get_group_stats(group_id)
            
            msg = "📊 系統狀態：\n\n"
            msg += f"💾 硬碟使用：\n"
            msg += f"   已使用：{disk_used:.2f} GB\n"
            msg += f"   總計：{disk_total:.2f} GB\n"
            msg += f"   使用率：{disk_percent:.1f}%\n\n"
            msg += f"👥 目前群組：\n"
            msg += f"   登記用戶：{user_count} 人\n"
            msg += f"   白名單：{whitelist_count} 人\n"
            msg += f"   目前單價：{price} 元"
            
            if disk_percent > 80:
                msg += "\n\n⚠️ 警告：硬碟使用率過高！"
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

    if text == "重置全部" and user_id == ADMIN_ID:
        clear_all_users(group_id)
        if conn:
            conn.commit()
        line_bot_api.reply_message(reply_token, TextSendMessage(text="✅ 已清除此群組所有人的帳目"))
        return

    if text == "全部帳單" and user_id == ADMIN_ID:
        rows = get_all_users(group_id)
        whitelist_data = get_whitelist(group_id)
        whitelist_ids = [u[0] for u in whitelist_data]
        msg = "📋 全部帳單：\n"
        has_debt = False
        for uid, name, count in rows:
            if uid in whitelist_ids:
                continue
            if count > 0:
                display_name = f"@{name}" if name else uid[-4:]
                msg += f"{display_name}: {count}次 / {count*price}元\n"
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
        parts = text.replace("白名單移除", "").strip()
        if parts.startswith("@"):
            parts = parts[1:]
        
        target_user_id = None
        if parts:
            target_user_id = get_user_by_name(parts, group_id)
        
        if target_user_id:
            remove_from_whitelist(target_user_id, group_id)
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已將 @{parts} 移出白名單"))
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
            old_count = get_count(target_user_id, group_id)
            clear_user(target_user_id, group_id)
            if conn:
                conn.commit()
            name_display = f"@{target_name}" if target_name else target_user_id[-4:]
            msg = f"✅ 已清除 {name_display} 的帳目"
            if old_count > 0:
                msg += f"\n（已清除 {old_count} 次，金額 {old_count*price} 元）"
            line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「已繳 @人」格式"))
        return

    if text.startswith("@") and user_id == ADMIN_ID:
        parts = text.split()
        if len(parts) >= 2:
            target_name = parts[0].replace("@", "")
            target_user_id = get_user_by_name(target_name, group_id)
            
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
                    
                    add_count(target_user_id, group_id, n)
                    if conn:
                        conn.commit()
                    new_count = get_count(target_user_id, group_id)
                    line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已替 @{target_name} 記錄 +{n} 次（共 {new_count} 次，應繳 {new_count*price} 元）"))
                    return

    if text.startswith("+"):
        if text == "+" or text == "++":
            add_count(user_id, group_id, 1)
        else:
            try:
                n = int(text.lstrip("+"))
                max_n = get_max_per_action()
                if n > max_n:
                    line_bot_api.reply_message(reply_token, TextSendMessage(text=f"⚠️ 單次最多 +{max_n} 次"))
                    return
                add_count(user_id, group_id, n)
            except:
                pass
        return

    if text.startswith("-"):
        try:
            if text == "-" or text == "--":
                n = 1
            else:
                n = int(text.lstrip("-"))
            count = get_count(user_id, group_id)
            new_count = max(0, count - n)
            if cursor:
                cursor.execute("UPDATE users SET count=%s WHERE user_id=%s AND group_id=%s", (new_count, user_id, group_id))
                conn.commit()
        except:
            pass
