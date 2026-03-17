import os
import pandas as pd


def load_total(csv_path: str):
    df = pd.read_csv(csv_path)
    total_row = df[df["sql_id"].astype(str) == "TOTAL"]
    if total_row.empty:
        raise ValueError(f"TOTAL row not found in {csv_path}")
    return float(total_row.iloc[0]["exec_time"])


def export_benchmark_summary(
    no_index_csv: str = "output/benchmarks/benchmark_no_index.csv",
    btree_only_csv: str = "output/benchmarks/benchmark_btree_only.csv",
    hybrid_csv: str = "output/benchmarks/benchmark_hybrid.csv",
    output_csv: str = "output/benchmarks/benchmark_summary.csv"
):
    no_index_total = load_total(no_index_csv)
    btree_only_total = load_total(btree_only_csv)
    hybrid_total = load_total(hybrid_csv)

    df = pd.DataFrame([
        {
            "method": "No Index",
            "total_exec_time": no_index_total,
            "speedup_vs_no_index": 1.0,
            "reduction_ratio": 0.0,
        },
        {
            "method": "BTree Only",
            "total_exec_time": btree_only_total,
            "speedup_vs_no_index": no_index_total / btree_only_total if btree_only_total > 0 else 0.0,
            "reduction_ratio": (no_index_total - btree_only_total) / no_index_total if no_index_total > 0 else 0.0,
        },
        {
            "method": "Hybrid",
            "total_exec_time": hybrid_total,
            "speedup_vs_no_index": no_index_total / hybrid_total if hybrid_total > 0 else 0.0,
            "reduction_ratio": (no_index_total - hybrid_total) / no_index_total if no_index_total > 0 else 0.0,
        }
    ])

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8")

    print(f"[INFO] Summary saved to: {output_csv}")
    print(df)

    return df


if __name__ == "__main__":
    export_benchmark_summary()