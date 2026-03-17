import psycopg2

conn = psycopg2.connect(
    dbname="tpch",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)
cur = conn.cursor()
cur.execute("SELECT count(*) FROM lineitem")
print(cur.fetchone())
conn.close()

#python test_db.py

#(6001215,)

