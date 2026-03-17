def estimate_index_storage(candidate: dict, fiting_model_size=None):
    if candidate["index_type"] == "btree":
        base = 8.0
        per_col = 4.0
        return base + per_col * len(candidate["columns"])

    elif candidate["index_type"] == "fiting":
        if fiting_model_size is not None:
            return float(fiting_model_size)
        return 4.0

    return 10.0


def build_index_feature(candidate: dict, fiting_model_size=None):
    storage_est = estimate_index_storage(
        candidate,
        fiting_model_size=fiting_model_size
    )

    return {
        "i_type_btree": int(candidate["index_type"] == "btree"),
        "i_type_fiting": int(candidate["index_type"] == "fiting"),
        "i_col_count": len(candidate["columns"]),
        "i_is_single": int(len(candidate["columns"]) == 1),
        "i_is_multi": int(len(candidate["columns"]) > 1),
        "i_storage_est": float(storage_est),
    }

if __name__ == "__main__":
    sample_index = {
        "index_type": "btree",
        "columns": ("l_quantity", "l_partkey"),
        "width": 2
    }

    feat = build_index_feature(sample_index)
    print(feat)