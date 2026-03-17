from itertools import combinations
from collections import Counter


def collect_indexable_columns(parsed_query: dict):
    cols = []
    cols.extend(parsed_query.get("where_cols", []))
    cols.extend(parsed_query.get("join_cols", []))
    cols.extend(parsed_query.get("order_cols", []))
    return list(dict.fromkeys(cols))


def count_column_frequency(parsed_queries: list):
    counter = Counter()

    for pq in parsed_queries:
        cols = set(collect_indexable_columns(pq))
        for c in cols:
            counter[c] += 1

    return counter


def get_high_frequency_columns(parsed_queries: list, freq_threshold: float = 0.1):
    counter = count_column_frequency(parsed_queries)
    total_queries = max(len(parsed_queries), 1)

    high_freq_cols = []
    for col, cnt in counter.items():
        freq = cnt / total_queries
        if freq >= freq_threshold:
            high_freq_cols.append(col)

    return sorted(high_freq_cols), counter


def generate_single_column_candidates(high_freq_cols: list,parsed_queries: list,enable_btree: bool = True,enable_fiting: bool = True):

    candidates = []
    seen = set()

    for pq in parsed_queries:
        column_to_tables = pq.get("column_to_tables", {})
        for col in high_freq_cols:
            tables = column_to_tables.get(col, [])
            if len(tables) != 1:
                continue

            table_name = tables[0]
            if enable_btree:
                key1 = ("btree", table_name, col)
                if key1 not in seen:
                    seen.add(key1)
                    candidates.append({
                        "index_type": "btree",
                        "table_name": table_name,
                        "columns": (col,),
                        "width": 1
                    })
            if enable_fiting:
                key2 = ("fiting", table_name, col)
                if key2 not in seen:
                    seen.add(key2)
                    candidates.append({
                        "index_type": "fiting",
                        "table_name": table_name,
                        "columns": (col,),
                        "width": 1
                    })

    return candidates

def generate_composite_candidates(parsed_queries: list, high_freq_cols: list, max_width: int = 3,enable_btree: bool = True):
    if not enable_btree:
        return []
    candidates = []
    seen = set()
    high_freq_set = set(high_freq_cols)

    for pq in parsed_queries:
        table_to_cols = {}

        ordered_cols = []
        for col in pq.get("where_cols", []):
            if col in high_freq_set and col not in ordered_cols:
                ordered_cols.append(col)
        for col in pq.get("join_cols", []):
            if col in high_freq_set and col not in ordered_cols:
                ordered_cols.append(col)
        for col in pq.get("order_cols", []):
            if col in high_freq_set and col not in ordered_cols:
                ordered_cols.append(col)

        column_to_tables = pq.get("column_to_tables", {})

        # 把列分到所属表里
        for col in ordered_cols:
            tables = column_to_tables.get(col, [])
            if len(tables) == 1:
                table = tables[0]
                table_to_cols.setdefault(table, [])
                if col not in table_to_cols[table]:
                    table_to_cols[table].append(col)

        # 每张表内部单独组合
        for table_name, cols in table_to_cols.items():
            if len(cols) < 2:
                continue

            upper = min(max_width, len(cols))
            for k in range(2, upper + 1):
                for combo in combinations(cols, k):
                    key = ("btree", table_name, combo)
                    if key not in seen:
                        seen.add(key)
                        candidates.append({
                            "index_type": "btree",
                            "table_name": table_name,
                            "columns": combo,
                            "width": len(combo)
                        })

    return candidates


def generate_all_candidates(parsed_queries: list, freq_threshold: float = 0.1, max_width: int = 3,enable_btree: bool = True,
enable_fiting: bool = True):
    high_freq_cols, counter = get_high_frequency_columns(
        parsed_queries,
        freq_threshold=freq_threshold
    )
    single_candidates = generate_single_column_candidates(
        high_freq_cols,
        parsed_queries,
        enable_btree=enable_btree,
        enable_fiting=enable_fiting
    )
    # single_candidates = generate_single_column_candidates(high_freq_cols)
    composite_candidates = generate_composite_candidates(
        parsed_queries=parsed_queries,
        high_freq_cols=high_freq_cols,
        max_width=max_width,
        enable_btree=enable_btree
    )

    all_candidates = single_candidates + composite_candidates

    return {
        "high_freq_cols": high_freq_cols,
        "column_counter": counter,
        "single_candidates": single_candidates,
        "composite_candidates": composite_candidates,
        "all_candidates": all_candidates
    }
    


if __name__ == "__main__":
    sample_parsed_queries = [
        {
            "where_cols": ["l_orderkey"],
            "join_cols": [],
            "order_cols": [],
            "group_cols": [],
            "select_cols": ["l_orderkey"],
            "predicate_type": {"eq": True, "range": False, "join": False},
        },
        {
            "where_cols": ["o_custkey"],
            "join_cols": [],
            "order_cols": ["o_orderdate"],
            "group_cols": [],
            "select_cols": ["o_custkey", "o_orderdate"],
            "predicate_type": {"eq": True, "range": False, "join": False},
        },
        {
            "where_cols": ["c_nationkey"],
            "join_cols": ["c_custkey", "o_custkey"],
            "order_cols": [],
            "group_cols": [],
            "select_cols": ["c_custkey", "c_nationkey", "o_custkey"],
            "predicate_type": {"eq": True, "range": False, "join": True},
        },
        {
            "where_cols": ["l_quantity"],
            "join_cols": [],
            "order_cols": ["l_partkey"],
            "group_cols": ["l_partkey"],
            "select_cols": ["l_partkey", "l_quantity"],
            "predicate_type": {"eq": False, "range": True, "join": False},
        }
    ]

    result = generate_all_candidates(
        parsed_queries=sample_parsed_queries,
        freq_threshold=0.1,
        max_width=3
    )

    print("=== 高频列 ===")
    print(result["high_freq_cols"])

    print("\n=== 单列候选索引 ===")
    for item in result["single_candidates"]:
        print(item)

    print("\n=== 复合候选索引 ===")
    for item in result["composite_candidates"]:
        print(item)

    print("\n=== 全部候选索引数量 ===")
    print(len(result["all_candidates"]))