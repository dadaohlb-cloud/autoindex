import os
import json
import pandas as pd


def export_fiting_manifest(
    selected_csv: str = "output/selected_indexes.csv",
    output_json: str = "output/fiting_manifest.json"
):
    os.makedirs("output", exist_ok=True)

    df = pd.read_csv(selected_csv)
    fiting_df = df[df["index_type"] == "fiting"].copy()

    items = []
    for rank, (_, row) in enumerate(fiting_df.iterrows(), start=1):
        items.append({
            "deployment_rank": rank,
            "table_name": row["table_name"],
            "index_type": row["index_type"],
            "columns": row["index_cols"].split("|"),
            "storage_est": float(row["storage_est"]),
            "total_benefit": float(row["total_benefit"]),
            "avg_benefit": float(row["avg_benefit"]),
            "builder": "SimpleFitingTree",
            "error_threshold": 32,
            "build_steps": [
                "extract_and_sort_keys",
                "segment_linear_fit",
                "store_segment_directory",
                "predict_position_and_local_search"
            ]
        })

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    print(f"[INFO] FITING manifest 已保存到: {output_json}")
    print(json.dumps(items, ensure_ascii=False, indent=2))

    return items


if __name__ == "__main__":
    export_fiting_manifest()