from model.dataset_builder import build_dataset
from model.train import train_model
from model.infer import predict_benefit
from selector.greedy import greedy_select
from deploy.create_index import export_create_index_sql
from deploy.export_fiting_manifest import export_fiting_manifest


def main():
    train_workload = "workload/train_workload.sql"
    test_workload = "workload/test_workload.sql"

    print("=== Step 1: Build Dataset ===")
    build_dataset(
        workload_path=train_workload,
        output_csv="output/train.csv",
        freq_threshold=0.05,
        max_width=3,
        max_candidates_per_query=5,
        repeat=2
    )

    print("\n=== Step 2: Train Model ===")
    train_model(
        csv_path="output/train.csv",
        model_path="output/benefit_mlp.pt",
        scaler_path="output/scaler.pkl",
        epochs=200,
        lr=1e-3,
        test_size=0.2,
        random_state=42,
    )

    print("\n=== Step 3: Predict Benefit ===")
    predict_benefit(
        workload_path=test_workload,
        model_path="output/benefit_mlp.pt",
        scaler_path="output/scaler.pkl",
        output_csv="output/predictions.csv"
    )

    print("\n=== Step 4: Select Indexes ===")
    greedy_select(
        prediction_csv="output/predictions.csv",
        output_csv="output/selected_indexes.csv",
        storage_budget=40.0,
        max_indexes=5
    )

    print("\n=== Step 5: Export SQL ===")
    export_create_index_sql(
        selected_csv="output/selected_indexes.csv",
        output_sql="output/recommended_indexes.sql"
    )

    print("\n=== Step 6: Export FITING Manifest ===")
    export_fiting_manifest(
        selected_csv="output/selected_indexes.csv",
        output_json="output/fiting_manifest.json"
    )

    print("\n=== Pipeline Finished ===")


if __name__ == "__main__":
    main()