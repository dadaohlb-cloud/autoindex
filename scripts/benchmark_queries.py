import os
import re
import time
import pandas as pd

from db.pg import get_conn
from parser.load_workload import load_workload


def explain_analyze_time(conn, sql: str, repeat: int = 3):
    explain_sql = f"EXPLAIN ANALYZE {sql}"
    times = []

    with conn.cursor() as cur:
        for _ in range(repeat):
            cur.execute(explain_sql)
            rows = cur.fetchall()

            exec_time_ms = None
            for row in rows:
                line = row[0]
                m = re.search(r"Execution Time: ([0-9.]+) ms", line)
                if m:
                    exec_time_ms = float(m.group(1))
                    break

            if exec_time_ms is None:
                raise ValueError(f"无法解析执行时间: {sql}")

            times.append(exec_time_ms / 1000.0)

    conn.commit()
    return sum(times) / len(times)


def list_user_indexes(conn):
    sql = """
    SELECT indexname
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname LIKE 'idx_%'
    ORDER BY indexname
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    conn.commit()
    return [r[0] for r in rows]


def drop_user_indexes(conn):
    indexes = list_user_indexes(conn)
    with conn.cursor() as cur:
        for idx in indexes:
            cur.execute(f"DROP INDEX IF EXISTS {idx};")
    conn.commit()
    return indexes


def apply_sql_file(conn, sql_file: str):
    if not os.path.exists(sql_file):
        raise FileNotFoundError(sql_file)

    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()

    statements = [s.strip() for s in content.split(";") if s.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            if stmt.startswith("--"):
                continue
            cur.execute(stmt + ";")
    conn.commit()


def benchmark_workload(
    workload_path: str,
    output_csv: str,
    repeat: int = 3
):
    queries = load_workload(workload_path)
    conn = get_conn()

    try:
        rows = []
        total_time = 0.0

        for qid, sql in enumerate(queries, start=1):
            t = explain_analyze_time(conn, sql, repeat=repeat)
            rows.append({
                "sql_id": qid,
                "sql_text": sql,
                "exec_time": t,
            })
            total_time += t

        df = pd.DataFrame(rows)
        df.loc[len(df)] = {
            "sql_id": "TOTAL",
            "sql_text": "",
            "exec_time": total_time,
        }

        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        df.to_csv(output_csv, index=False, encoding="utf-8")

        print(f"[INFO] Benchmark saved to: {output_csv}")
        print(df.tail())

        return df

    finally:
        conn.close()


def run_benchmark_suite(
    workload_path: str = "workload/benchmark_workload.sql",
    recommended_sql: str = "output/recommended_indexes.sql",
    out_dir: str = "output/benchmarks",
    repeat: int = 3
):
    os.makedirs(out_dir, exist_ok=True)

    # 1) No Index
    conn = get_conn()
    try:
        dropped = drop_user_indexes(conn)
        print(f"[INFO] Dropped indexes for No Index baseline: {dropped}")
    finally:
        conn.close()

    benchmark_workload(
        workload_path=workload_path,
        output_csv=os.path.join(out_dir, "benchmark_no_index.csv"),
        repeat=repeat
    )
   # 2) BTree-only Recommended
    conn = get_conn()
    try:
        drop_user_indexes(conn)
        apply_sql_file(conn, "output/recommended_indexes_btree_only.sql")
        print("[INFO] Applied BTree-only SQL: output/recommended_indexes_btree_only.sql")
    finally:
        conn.close()

    benchmark_workload(
        workload_path=workload_path,
        output_csv=os.path.join(out_dir, "benchmark_btree_only.csv"),
        repeat=repeat
    )

    # 3) Hybrid Recommended
    conn = get_conn()
    try:
        drop_user_indexes(conn)
        apply_sql_file(conn, recommended_sql)
        print(f"[INFO] Applied recommended SQL: {recommended_sql}")
    finally:
        conn.close()

    benchmark_workload(
        workload_path=workload_path,
        output_csv=os.path.join(out_dir, "benchmark_hybrid.csv"),
        repeat=repeat
    )

    print("[INFO] Benchmark suite finished.")


if __name__ == "__main__":
    run_benchmark_suite()