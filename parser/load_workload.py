def load_workload(path: str):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    queries = [q.strip() for q in content.split(";") if q.strip()]
    return queries


if __name__ == "__main__":
    qs = load_workload("workload/tpch.sql")
    for i, q in enumerate(qs, 1):
        print(f"{i}: {q}")