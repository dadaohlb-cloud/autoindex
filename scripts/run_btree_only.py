from model.infer import predict_benefit
from selector.greedy import greedy_select
from deploy.create_index import export_create_index_sql


def main():
    print("=== BTree-only Predict Benefit ===")
    predict_benefit(
        workload_path="workload/test_workload.sql",
        model_path="output/benefit_mlp.pt",
        scaler_path="output/scaler.pkl",
        output_csv="output/predictions_btree_only.csv",
        enable_btree=True,
        enable_fiting=False
    )

    print("\n=== BTree-only Select Indexes ===")
    greedy_select(
        prediction_csv="output/predictions_btree_only.csv",
        output_csv="output/selected_indexes_btree_only.csv",
        storage_budget=40.0,
        max_indexes=5
    )

    print("\n=== BTree-only Export SQL ===")
    export_create_index_sql(
        selected_csv="output/selected_indexes_btree_only.csv",
        output_sql="output/recommended_indexes_btree_only.sql"
    )

    print("\n=== BTree-only Finished ===")


if __name__ == "__main__":
    main()