from db.pg import get_conn
from model.dataset_builder import timed_query

sql = "SELECT * FROM lineitem WHERE l_orderkey = 10"

conn = get_conn()
try:
    t = timed_query(conn, sql, repeat=1)
    conn.commit()
    print("baseline time:", t)
finally:
    conn.close()