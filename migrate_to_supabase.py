"""
Migration script: Copy data from Render PostgreSQL to Supabase
Run this once to migrate existing data.
"""
import pg8000

OLD_DB = {
    "host": "dpg-cuid6vdsvqrc73bbnnf0.oregon-postgres.render.com",
    "database": "line_bot_db_658x",
    "user": "admin",
    "password": "ZgW3S5h1X2P0D9v",
    "port": 5432,
    "ssl_context": True
}

NEW_DB = {
    "host": "db.rtsxmbvjbfbtltcfotnt.supabase.co",
    "database": "postgres",
    "user": "postgres",
    "password": "t>vTPtZt9/QX",
    "port": 5432,
    "ssl_context": True
}

TABLES = ["users", "signups", "events", "config"]

def migrate():
    print("Connecting to old database...")
    old_conn = pg8000.connect(**OLD_DB)
    old_cur = old_conn.cursor()

    print("Connecting to Supabase...")
    new_conn = pg8000.connect(**NEW_DB)
    new_cur = new_conn.cursor()

    for table in TABLES:
        print(f"\nMigrating {table}...")
        try:
            old_cur.execute(f"SELECT * FROM {table}")
            rows = old_cur.fetchall()
            print(f"  Found {len(rows)} rows in old DB")

            if rows:
                # Get column names
                old_cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' ORDER BY ordinal_position")
                columns = [col[0] for col in old_cur.fetchall()]
                print(f"  Columns: {columns}")

                # Delete existing data in new DB
                new_cur.execute(f"DELETE FROM {table}")

                # Insert rows
                placeholders = ', '.join(['%s'] * len(columns))
                col_names = ', '.join(columns)
                insert_sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

                for row in rows:
                    new_cur.execute(insert_sql, row)

                new_conn.commit()
                print(f"  Migrated {len(rows)} rows to Supabase")
            else:
                print(f"  No data to migrate")
        except Exception as e:
            print(f"  Error: {e}")

    old_conn.close()
    new_conn.close()
    print("\nMigration complete!")

if __name__ == "__main__":
    migrate()
