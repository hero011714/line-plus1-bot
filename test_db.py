import psycopg2, os
url = os.environ.get("DATABASE_URL", "postgresql://admin:ZgW3S5h1X2P0D9v@dpg-cuid6vdsvqrc73bbnnf0-a.oregon-postgres.render.com/line_bot_db_658x")
conn = psycopg2.connect(url)
cur = conn.cursor()
cur.execute("SELECT * FROM signups")
print("Signups:", cur.fetchall())
