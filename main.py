import os
import sqlite3
import asyncio
from fastapi import FastAPI, Request
from linebot import LineBotApi, WebhookHandler
from linebot.models import MessageEvent, TextMessage, TextSendMessage, Mention, Mentionee
from linebot.exceptions import InvalidSignatureError

CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
ADMIN_ID = os.getenv("ADMIN_ID", "")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok", "message": "LINE Bot is running"}

conn = sqlite3.connect("database.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    name TEXT DEFAULT '',
    count INTEGER DEFAULT 0
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")
cursor.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('price', '50')")
conn.commit()

def get_price():
    cursor.execute("SELECT value FROM config WHERE key='price'")
    return int(cursor.fetchone()[0])

def set_price(price):
    cursor.execute("UPDATE config SET value=? WHERE key='price'", (str(price),))
    conn.commit()

def add_user(user_id, name=""):
    cursor.execute("INSERT OR IGNORE INTO users (user_id, name) VALUES (?, ?)", (user_id, name))
    conn.commit()
    if name:
        cursor.execute("UPDATE users SET name=? WHERE user_id=?", (name, user_id))
        conn.commit()

def update_user_name(user_id, name):
    cursor.execute("UPDATE users SET name=? WHERE user_id=?", (name, user_id))
    conn.commit()

def get_user_name(user_id):
    cursor.execute("SELECT name FROM users WHERE user_id=?", (user_id,))
    result = cursor.fetchone()
    return result[0] if result and result[0] else user_id[-4:]

def add_count(user_id):
    cursor.execute("UPDATE users SET count = count + 1 WHERE user_id=?", (user_id,))
    conn.commit()

def get_count(user_id):
    cursor.execute("SELECT count FROM users WHERE user_id=?", (user_id,))
    result = cursor.fetchone()
    return result[0] if result else 0

def clear_user(user_id):
    cursor.execute("UPDATE users SET count=0 WHERE user_id=?", (user_id,))
    conn.commit()

def get_all_users():
    cursor.execute("SELECT user_id, name, count FROM users")
    return cursor.fetchall()

def get_user_by_name(name):
    cursor.execute("SELECT user_id FROM users WHERE name=?", (name,))
    result = cursor.fetchone()
    return result[0] if result else None

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
    text = event.message.text.strip()
    price = get_price()
    
    # 取得用戶名稱並儲存
    try:
        profile = line_bot_api.get_profile(user_id)
        update_user_name(user_id, profile.display_name)
    except:
        pass
    
    add_user(user_id)

    # +1 記錄（不回覆）
    if text == "+1":
        add_count(user_id)
        return

    # -1 扣減（不小於0）
    if text == "-1":
        count = get_count(user_id)
        if count > 0:
            cursor.execute("UPDATE users SET count = count - 1 WHERE user_id=?", (user_id,))
            conn.commit()

    # 查帳
    elif text == "查帳":
        count = get_count(user_id)
        name = get_user_name(user_id)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"@{name} 目前 {count} 次，應繳 {count*price} 元")
        )

    # 設定單價（管理員）
    elif text.startswith("設定單價") and user_id == ADMIN_ID:
        try:
            new_price = int(text.split()[-1])
            set_price(new_price)
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"單價已設定為 {new_price} 元")
            )
        except:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="格式錯誤，請輸入：設定單價 數字")
            )

    # 已繳 - 支援 @人 格式
    elif text.startswith("已繳") and user_id == ADMIN_ID:
        # 檢查是否有 mention
        mention = getattr(event.message, 'mention', None)
        target_name = None
        target_user_id = None
        
        if mention and hasattr(mention, 'mentionees'):
            for m in mention.mentionees:
                if m.user_id != ADMIN_ID:
                    target_user_id = m.user_id
                    try:
                        profile = line_bot_api.get_profile(target_user_id)
                        target_name = profile.display_name
                    except:
                        target_name = get_user_name(target_user_id)
                    break
        
        # 如果沒有 mention，嘗試用名稱查詢
        if not target_user_id:
            parts = text.replace("已繳", "").strip()
            if parts.startswith("@"):
                parts = parts[1:]
            if parts:
                target_user_id = get_user_by_name(parts)
                target_name = parts
        
        if target_user_id:
            clear_user(target_user_id)
            name_display = f"@{target_name}" if target_name else target_user_id[-4:]
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"已清除 {name_display} 的帳目")
            )
        else:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="請使用「已繳 @人」格式（需要 mention）")
            )

    # 全部帳單
    elif text == "全部帳單":
        rows = get_all_users()
        msg = "📋 全部帳單：\n"
        has_debt = False
        for uid, name, count in rows:
            if count > 0:
                display_name = f"@{name}" if name else uid[-4:]
                msg += f"{display_name}: {count}次 / {count*price}元\n"
                has_debt = True
        if not has_debt:
            msg = "目前無欠款 ✅"
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=msg)
        )
