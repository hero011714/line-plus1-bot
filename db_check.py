import os
import psycopg2
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://admin:ZgW3S5h1X2P0D9v@dpg-cuid6vdsvqrc73bbnnf0-a.oregon-postgres.render.com/line_bot_db_658x")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()
cur.execute("SELECT group_id, expires_at FROM events")
print("Events:", cur.fetchall())
cur.execute("SELECT group_id, count FROM signups")
print("Signups:", cur.fetchall())
