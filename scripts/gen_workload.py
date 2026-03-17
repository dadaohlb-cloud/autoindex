import os
import random
from pathlib import Path


OUT_DIR = Path("workload/generated")
OUT_DIR.mkdir(parents=True, exist_ok=True)

random.seed(42)


def gen_lineitem_point_queries(n=30):
    queries = []
    values = [10, 100, 1000, 5000, 10000, 20000, 50000]
    for _ in range(n):
        v = random.choice(values)
        queries.append(f"SELECT * FROM lineitem WHERE l_orderkey = {v};")
    return queries


def gen_orders_point_order_queries(n=30):
    queries = []
    values = [100, 500, 1000, 2000, 5000, 10000, 20000]
    for _ in range(n):
        v = random.choice(values)
        queries.append(
            f"SELECT * FROM orders WHERE o_custkey = {v} ORDER BY o_orderdate;"
        )
    return queries


def gen_lineitem_range_group_queries(n=30):
    queries = []
    values = [5, 10, 15, 20, 25, 30, 35]
    for _ in range(n):
        v = random.choice(values)
        queries.append(
            "SELECT l_partkey, COUNT(*) "
            f"FROM lineitem WHERE l_quantity > {v} "
            "GROUP BY l_partkey ORDER BY l_partkey;"
        )
    return queries


def gen_join_queries(n=20):
    queries = []
    values = [1, 3, 5, 10, 15, 20]
    for _ in range(n):
        v = random.choice(values)
        queries.append(
            "SELECT * "
            "FROM customer c "
            "JOIN orders o ON c.c_custkey = o.o_custkey "
            f"WHERE c.c_nationkey = {v};"
        )
    return queries


def gen_train_workload():
    queries = []
    queries += gen_lineitem_point_queries(40)
    queries += gen_orders_point_order_queries(30)
    queries += gen_lineitem_range_group_queries(30)
    queries += gen_join_queries(20)
    return queries


def gen_test_workload():
    queries = []
    queries += gen_lineitem_point_queries(10)
    queries += gen_orders_point_order_queries(8)
    queries += gen_lineitem_range_group_queries(8)
    queries += gen_join_queries(4)
    return queries


def gen_benchmark_workload():
    # benchmark 集尽量固定，便于论文表格复现
    queries = [
        "SELECT * FROM lineitem WHERE l_orderkey = 10;",
        "SELECT * FROM lineitem WHERE l_orderkey = 1000;",
        "SELECT * FROM lineitem WHERE l_orderkey = 10000;",
        "SELECT * FROM orders WHERE o_custkey = 100 ORDER BY o_orderdate;",
        "SELECT * FROM orders WHERE o_custkey = 1000 ORDER BY o_orderdate;",
        "SELECT * FROM orders WHERE o_custkey = 10000 ORDER BY o_orderdate;",
        "SELECT l_partkey, COUNT(*) FROM lineitem WHERE l_quantity > 10 GROUP BY l_partkey ORDER BY l_partkey;",
        "SELECT l_partkey, COUNT(*) FROM lineitem WHERE l_quantity > 20 GROUP BY l_partkey ORDER BY l_partkey;",
        "SELECT l_partkey, COUNT(*) FROM lineitem WHERE l_quantity > 30 GROUP BY l_partkey ORDER BY l_partkey;",
        "SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey WHERE c.c_nationkey = 3;",
        "SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey WHERE c.c_nationkey = 10;",
        "SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey WHERE c.c_nationkey = 20;",
    ]
    return queries


def write_queries(path: Path, queries):
    with open(path, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(q.strip() + "\n")


def main():
    train_queries = gen_train_workload()
    test_queries = gen_test_workload()
    benchmark_queries = gen_benchmark_workload()

    write_queries(Path("workload/train_workload.sql"), train_queries)
    write_queries(Path("workload/test_workload.sql"), test_queries)
    write_queries(Path("workload/benchmark_workload.sql"), benchmark_queries)

    print(f"[INFO] train queries: {len(train_queries)}")
    print(f"[INFO] test queries: {len(test_queries)}")
    print(f"[INFO] benchmark queries: {len(benchmark_queries)}")
    print("[INFO] workload files generated:")
    print(" - workload/train_workload.sql")
    print(" - workload/test_workload.sql")
    print(" - workload/benchmark_workload.sql")


if __name__ == "__main__":
    main()