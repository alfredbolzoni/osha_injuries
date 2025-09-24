import psycopg2

conn_str = (
    "host=db.qlgqlgrupwspehmjwgvm.supabase.co "
    "port=5432 "
    "dbname=postgres "
    "user=postgres "
    "password=Freddomala1993$ "
    "sslmode=require"
)

try:
    conn = psycopg2.connect(conn_str)
    print("✅ Connection successful!")
    cur = conn.cursor()
    cur.execute("SELECT version();")
    print("Database version:", cur.fetchone())
    cur.close()
    conn.close()
except Exception as e:
    print("❌ Connection failed:")
    print(e)