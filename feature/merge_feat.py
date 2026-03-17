def merge_features(query_feat: dict, index_feat: dict, interaction_feat: dict):
    merged = {}
    merged.update(query_feat)
    merged.update(index_feat)
    merged.update(interaction_feat)
    return merged


if __name__ == "__main__":
    qf = {"q_eq": 1, "q_range": 0}
    inf = {"i_type_btree": 1, "i_col_count": 2}
    xf = {"x_hit_predicate": 1, "x_prefix_depth": 1}

    print(merge_features(qf, inf, xf))