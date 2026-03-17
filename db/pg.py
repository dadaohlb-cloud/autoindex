import psycopg2

DB_CONFIG = {
    "dbname": "tpch",
    "user": "postgres",
    "password": "123456",
    "host": "127.0.0.1",
    "port": "5432",
}

def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout TO 30000;")  # 30秒
    conn.commit()

    return conn