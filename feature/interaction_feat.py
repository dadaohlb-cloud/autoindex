def prefix_match_depth(query_cols, index_cols):
    depth = 0
    for qc, ic in zip(query_cols, index_cols):
        if qc == ic:
            depth += 1
        else:
            break
    return depth


def build_interaction_feature(parsed_query: dict, candidate: dict):
    index_cols = list(candidate["columns"])

    predicate_cols = []
    predicate_cols.extend(parsed_query.get("where_cols", []))
    predicate_cols.extend(parsed_query.get("join_cols", []))
    predicate_cols = list(dict.fromkeys(predicate_cols))

    select_cols = list(dict.fromkeys(parsed_query.get("select_cols", [])))
    select_star = bool(parsed_query.get("select_star", False))

    hit_predicate = int(any(col in index_cols for col in predicate_cols))
    hit_order = int(any(col in index_cols for col in parsed_query.get("order_cols", [])))
    hit_group = int(any(col in index_cols for col in parsed_query.get("group_cols", [])))

    prefix_depth = prefix_match_depth(predicate_cols, index_cols)
    prefix_ratio = prefix_depth / len(index_cols) if len(index_cols) > 0 else 0.0
    contiguous_prefix_hit = int(prefix_depth > 0)

    if select_star or "*" in select_cols:
        covering = 0
        cover_ratio = 0.0
    else:
        select_set = set(select_cols)
        index_set = set(index_cols)
        covering = int(select_set.issubset(index_set)) if select_set else 0
        cover_ratio = len(select_set & index_set) / len(select_set) if select_set else 0.0

    return {
        "x_hit_predicate": hit_predicate,
        "x_hit_order": hit_order,
        "x_hit_group": hit_group,
        "x_prefix_depth": prefix_depth,
        "x_prefix_ratio": prefix_ratio,
        "x_contiguous_prefix_hit": contiguous_prefix_hit,
        "x_covering": covering,
        "x_cover_ratio": cover_ratio,
        "x_predicate_col_cnt": len(predicate_cols),
        "x_index_col_cnt": len(index_cols),
    }


if __name__ == "__main__":
    sample_query = {
        "where_cols": ["l_quantity"],
        "join_cols": [],
        "order_cols": ["l_partkey"],
        "group_cols": ["l_partkey"],
        "select_cols": ["l_partkey"],
        "predicate_type": {"eq": False, "range": True, "join": False},
    }

    sample_index = {
        "index_type": "btree",
        "columns": ("l_quantity", "l_partkey"),
        "width": 2
    }

    feat = build_interaction_feature(sample_query, sample_index)
    print(feat)