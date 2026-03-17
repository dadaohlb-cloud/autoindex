def build_query_feature(parsed_query: dict, frequency: float = 1.0, selectivity: float = 1.0):
    return {
        "q_eq": int(parsed_query["predicate_type"]["eq"]),
        "q_range": int(parsed_query["predicate_type"]["range"]),
        "q_join": int(parsed_query["predicate_type"]["join"]),
        "q_filter_cnt": len(parsed_query.get("where_cols", [])),
        "q_join_cnt": len(parsed_query.get("join_cols", [])),
        "q_order_cnt": len(parsed_query.get("order_cols", [])),
        "q_group_cnt": len(parsed_query.get("group_cols", [])),
        "q_select_cnt": len(parsed_query.get("select_cols", [])),
        "q_frequency": float(frequency),
        "q_selectivity": float(selectivity),
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

    feat = build_query_feature(sample_query, frequency=3, selectivity=0.25)
    print(feat)
