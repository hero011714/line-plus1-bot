import os
import sqlite3
import asyncio
from fastapi import FastAPI, Request
from linebot import LineBotApi, WebhookHandler
from linebot.models import MessageEvent, TextMessage, TextSendMessage
from linebot.exceptions import InvalidSignatureError

# ===== 環境變數 =====
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET")
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

app = FastAPI()

# ===== 初始化資料庫 =====
conn = sqlite3.connect("database.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
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

def add_user(user_id):
    cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    conn.commit()

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

    add_user(user_id)
    price = get_price()

    if text == "+1":
        add_count(user_id)
        count = get_count(user_id)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"已記錄，目前 {count} 次，應繳 {count*price} 元")
        )

    elif text == "查帳":
        count = get_count(user_id)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"你目前 {count} 次，應繳 {count*price} 元")
        )

    elif text.startswith("設定單價") and user_id == ADMIN_ID:
        new_price = int(text.split()[-1])
        set_price(new_price)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"單價已設定為 {new_price} 元")
        )

    elif text.startswith("已繳 ") and user_id == ADMIN_ID:
        parts = text.split()
        if len(parts) >= 2:
            target_user_id = parts[1]
            clear_user(target_user_id)
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="已清除該用戶帳目")
            )
        else:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="請輸入：已繳 [user_id]")
            )

    elif text == "全部帳單":
        cursor.execute("SELECT user_id, count FROM users")
        rows = cursor.fetchall()
        msg = ""
        for uid, count in rows:
            if count > 0:
                msg += f"{uid[-4:]}: {count*price} 元\n"
        if msg == "":
            msg = "目前無欠款"
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=msg)
        )