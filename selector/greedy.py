import os
import pandas as pd


def parse_storage_from_prediction_row(row):
    # 这里沿用你当前特征里的 i_storage_est
    return float(row["i_storage_est"])


def aggregate_predictions(
    prediction_csv: str = "output/predictions.csv"
):
    df = pd.read_csv(prediction_csv)

    grouped = (
        df.groupby(["index_type", "table_name", "index_cols"], as_index=False)
        .agg(
            total_benefit=("pred_benefit", "sum"),
            avg_benefit=("pred_benefit", "mean"),
            storage_est=("i_storage_est", "mean"),
        )
        .sort_values("total_benefit", ascending=False)
        .reset_index(drop=True)
    )

    return grouped


def greedy_select(
    prediction_csv: str = "output/predictions.csv",
    output_csv: str = "output/selected_indexes.csv",
    storage_budget: float = 40.0,
    max_indexes: int = 5
):
    os.makedirs("output", exist_ok=True)

    grouped = aggregate_predictions(prediction_csv)

    selected = []
    used_storage = 0.0

    for _, row in grouped.iterrows():
        if len(selected) >= max_indexes:
            break

        storage = float(row["storage_est"])

        if used_storage + storage <= storage_budget:
            selected.append({
                "index_type": row["index_type"],
                "table_name": row["table_name"],
                "index_cols": row["index_cols"],
                "total_benefit": float(row["total_benefit"]),
                "avg_benefit": float(row["avg_benefit"]),
                "storage_est": storage,
            })
            used_storage += storage

    selected_df = pd.DataFrame(selected)
    selected_df.to_csv(output_csv, index=False, encoding="utf-8")

    print(f"[INFO] 已选择索引保存到: {output_csv}")
    print(f"[INFO] 已使用存储预算: {used_storage:.2f} / {storage_budget:.2f}")
    print(selected_df)

    return selected_df


if __name__ == "__main__":
    greedy_select()