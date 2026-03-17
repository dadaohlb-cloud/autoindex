import pandas as pd


def aggregate_benefit(
    prediction_csv: str = "output/predictions.csv",
    output_csv: str = "output/aggregated_predictions.csv"
):
    df = pd.read_csv(prediction_csv)

    grouped = (
        df.groupby(["index_type", "index_cols"], as_index=False)["pred_benefit"]
        .sum()
        .rename(columns={"pred_benefit": "total_benefit"})
        .sort_values("total_benefit", ascending=False)
    )

    grouped.to_csv(output_csv, index=False, encoding="utf-8")

    print(f"[INFO] 汇总结果已保存到: {output_csv}")
    print(grouped.head(20))

    return grouped


if __name__ == "__main__":
    aggregate_benefit()