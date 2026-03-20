import os
import sqlite3
import asyncio
import time
from fastapi import FastAPI, Request
from linebot import LineBotApi, WebhookHandler
from linebot.models import MessageEvent, TextMessage, TextSendMessage
from linebot.exceptions import InvalidSignatureError

CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
ADMIN_ID = os.getenv("ADMIN_ID", "")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

app = FastAPI()

@app.get("/")
def root():
    commit_all()
    return {"status": "ok", "message": "LINE Bot is running"}

conn = sqlite3.connect("database.db", check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")
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

cursor.execute("INSERT OR IGNORE INTO config (group_id, key, value) VALUES ('default', 'price', '50')")
cursor.execute("INSERT OR IGNORE INTO config (group_id, key, value) VALUES ('default', 'max_per_action', '10')")
cursor.execute("INSERT OR IGNORE INTO config (group_id, key, value) VALUES ('default', 'fetch_interval', '86400')")
conn.commit()

_pending_updates = []

def queue_update(sql, params=()):
    _pending_updates.append((sql, params))

def commit_all():
    global _pending_updates
    if _pending_updates:
        for sql, params in _pending_updates:
            cursor.execute(sql, params)
        conn.commit()
        _pending_updates = []

def get_group_id(event):
    source_type = event.source.type
    if source_type == 'group':
        return event.source.group_id
    elif source_type == 'room':
        return event.source.room_id
    else:
        return 'private_' + event.source.user_id

def get_price(group_id):
    cursor.execute("SELECT value FROM config WHERE group_id=? AND key='price'", (group_id,))
    result = cursor.fetchone()
    if result:
        return int(result[0])
    cursor.execute("INSERT OR IGNORE INTO config (group_id, key, value) VALUES (?, 'price', '50')", (group_id,))
    conn.commit()
    return 50

def get_max_per_action():
    cursor.execute("SELECT value FROM config WHERE group_id='default' AND key='max_per_action'")
    result = cursor.fetchone()
    return int(result[0]) if result else 10

def set_price(group_id, price):
    cursor.execute("INSERT OR REPLACE INTO config (group_id, key, value) VALUES (?, 'price', ?)", (group_id, str(price)))
    conn.commit()

def add_user(user_id, group_id, name=""):
    cursor.execute("INSERT OR IGNORE INTO users (user_id, group_id, name, count, last_fetch) VALUES (?, ?, ?, 0, 0)", (user_id, group_id, name))
    if name:
        cursor.execute("UPDATE users SET name=? WHERE user_id=? AND group_id=?", (name, user_id, group_id))

def update_user_name(user_id, group_id, name):
    cursor.execute("UPDATE users SET name=?, last_fetch=? WHERE user_id=? AND group_id=?", (name, int(time.time()), user_id, group_id))

def should_fetch_profile(user_id, group_id):
    cursor.execute("SELECT last_fetch FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    result = cursor.fetchone()
    if not result or result[0] == 0:
        return True
    cursor.execute("SELECT value FROM config WHERE group_id='default' AND key='fetch_interval'")
    result2 = cursor.fetchone()
    interval = int(result2[0]) if result2 else 86400
    return (int(time.time()) - result[0]) > interval

def get_user_name(user_id, group_id):
    cursor.execute("SELECT name FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    result = cursor.fetchone()
    if result and result[0]:
        return result[0]
    cursor.execute("SELECT name FROM whitelist WHERE user_id=? AND group_id=?", (user_id, group_id))
    result2 = cursor.fetchone()
    if result2 and result2[0]:
        return result2[0]
    return user_id[-4:]

def is_whitelist(user_id, group_id):
    cursor.execute("SELECT 1 FROM whitelist WHERE user_id=? AND group_id=?", (user_id, group_id))
    return cursor.fetchone() is not None

def add_to_whitelist(user_id, group_id, name=""):
    old_count = get_count(user_id, group_id)
    cursor.execute("INSERT OR REPLACE INTO whitelist (user_id, group_id, name) VALUES (?, ?, ?)", (user_id, group_id, name))
    cursor.execute("UPDATE users SET count=0 WHERE user_id=? AND group_id=?", (user_id, group_id))
    conn.commit()
    return old_count

def remove_from_whitelist(user_id, group_id):
    cursor.execute("DELETE FROM whitelist WHERE user_id=? AND group_id=?", (user_id, group_id))
    conn.commit()

def get_whitelist(group_id):
    cursor.execute("SELECT user_id, name FROM whitelist WHERE group_id=?", (group_id,))
    return cursor.fetchall()

def add_count(user_id, group_id, n=1):
    cursor.execute("UPDATE users SET count = count + ? WHERE user_id=? AND group_id=?", (n, user_id, group_id))

def get_count(user_id, group_id):
    cursor.execute("SELECT count FROM users WHERE user_id=? AND group_id=?", (user_id, group_id))
    result = cursor.fetchone()
    return result[0] if result else 0

def clear_user(user_id, group_id):
    cursor.execute("UPDATE users SET count=0 WHERE user_id=? AND group_id=?", (user_id, group_id))

def clear_all_users(group_id):
    cursor.execute("UPDATE users SET count=0 WHERE group_id=?", (group_id,))

def get_all_users(group_id):
    cursor.execute("SELECT user_id, name, count FROM users WHERE group_id=?", (group_id,))
    return cursor.fetchall()

def get_user_by_name(name, group_id):
    cursor.execute("SELECT user_id FROM users WHERE name=? AND group_id=?", (name, group_id))
    result = cursor.fetchone()
    return result[0] if result else None

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
    
    # 只在需要時取得用戶名稱（快取 24 小時）
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
    
    # 學習被提及者的名字
    try:
        mentioned, _ = get_mentioned_users(event, user_id)
        for m_user_id, m_name in mentioned:
            pass
    except:
        pass

    # 白名單用戶不打 +1
    if is_whitelist(user_id, group_id):
        return

    # 幫助指令
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
        msg += "全部帳單：查看所有人的欠款\n\n"
        msg += "【特殊指令】\n"
        msg += "群組ID：查看目前群組ID"
        line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        return

    # 群組ID查詢
    if text == "群組ID":
        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"📌 目前群組ID：\n{group_id}"))
        return

    # 設定單價（管理員）
    if text.startswith("設定單價") and user_id == ADMIN_ID:
        try:
            new_price = int(text.split()[-1])
            set_price(group_id, new_price)
            line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 單價已設定為 {new_price} 元"))
        except:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 格式錯誤，請輸入：設定單價 數字"))
        return

    # 重置全部（管理員）
    if text == "重置全部" and user_id == ADMIN_ID:
        clear_all_users(group_id)
        conn.commit()
        line_bot_api.reply_message(reply_token, TextSendMessage(text="✅ 已清除此群組所有人的帳目"))
        return

    # 全部帳單（管理員）
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

    # 查看白名單（管理員）
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

    # 查帳
    if text == "查帳":
        count = get_count(user_id, group_id)
        line_bot_api.reply_message(reply_token, TextSendMessage(text=f"@{user_name} 目前 {count} 次，應繳 {count*price} 元"))
        return

    # 加入白名單（管理員）
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

    # 移除白名單（管理員）
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

    # 已繳（管理員）
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
            conn.commit()
            name_display = f"@{target_name}" if target_name else target_user_id[-4:]
            msg = f"✅ 已清除 {name_display} 的帳目"
            if old_count > 0:
                msg += f"\n（已清除 {old_count} 次，金額 {old_count*price} 元）"
            line_bot_api.reply_message(reply_token, TextSendMessage(text=msg))
        else:
            line_bot_api.reply_message(reply_token, TextSendMessage(text="❌ 請使用「已繳 @人」格式"))
        return

    # @人 +N（管理員）
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
                    conn.commit()
                    new_count = get_count(target_user_id, group_id)
                    line_bot_api.reply_message(reply_token, TextSendMessage(text=f"✅ 已替 @{target_name} 記錄 +{n} 次（共 {new_count} 次，應繳 {new_count*price} 元）"))
                    return

    # + 加次數
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

    # - 扣次數
    if text.startswith("-"):
        try:
            if text == "-" or text == "--":
                n = 1
            else:
                n = int(text.lstrip("-"))
            count = get_count(user_id, group_id)
            new_count = max(0, count - n)
            cursor.execute("UPDATE users SET count=? WHERE user_id=? AND group_id=?", (new_count, user_id, group_id))
        except:
            pass
