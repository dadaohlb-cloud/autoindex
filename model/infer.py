

import os
import joblib
import pandas as pd
import torch
from db.pg import get_conn
from learned_index.evaluator import build_fiting_on_values

from model.dataset_builder import filter_relevant_candidates
from parser.load_workload import load_workload
from parser.predicate_parser import extract_single_column_predicate
from parser.sql_parser import parse_sql
from candidate.generator import generate_all_candidates
from feature.query_feat import build_query_feature
from feature.index_feat import build_index_feature
from feature.interaction_feat import build_interaction_feature
from feature.merge_feat import merge_features
from model.mlp import BenefitMLP


DROP_META_COLS = [
    "sql_id",
    "table_name",
    "sql_text",
    "index_type",
    "index_cols",
]


def load_model_and_scaler(
    model_path: str = "output/benefit_mlp.pt",
    scaler_path: str = "output/scaler.pkl"
):
    checkpoint = torch.load(model_path, map_location="cpu")
    scaler = joblib.load(scaler_path)

    input_dim = checkpoint["input_dim"]
    feature_cols = checkpoint["feature_cols"]

    model = BenefitMLP(input_dim=input_dim)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    return model, scaler, feature_cols

def fetch_column_values_for_infer(conn, table_name: str, column_name: str, limit: int = 10000):
    sql = f"""
    SELECT {column_name}
    FROM {table_name}
    WHERE {column_name} IS NOT NULL
    ORDER BY {column_name}
    LIMIT {limit}
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    values = []
    for r in rows:
        v = r[0]
        if isinstance(v, (int, float)):
            values.append(v)
        else:
            try:
                values.append(float(v))
            except Exception:
                pass

    return values

def fiting_candidate_supported_for_query(candidate: dict, sql: str):
    if candidate["index_type"] != "fiting":
        return True

    if len(candidate["columns"]) != 1:
        return False

    predicate = extract_single_column_predicate(sql)
    if predicate is None:
        return False

    col = candidate["columns"][0]
    return predicate["column"] == col

def estimate_fiting_features_for_candidate(conn, candidate: dict, sample_limit: int = 10000):
    """
    在推理阶段为 FITING 候选真实估计：
    - fiting_model_size
    - fiting_segment_count

    对于非 FITING 或不支持的候选，返回 (0.0, 0.0)
    """
    if candidate["index_type"] != "fiting":
        return 0.0, 0.0

    if len(candidate["columns"]) != 1:
        return 0.0, 0.0

    table_name = candidate.get("table_name")
    if not table_name:
        return 0.0, 0.0

    col = candidate["columns"][0]

    try:
        values = fetch_column_values_for_infer(
            conn,
            table_name=table_name,
            column_name=col,
            limit=sample_limit
        )

        if not values:
            return 0.0, 0.0

        _, fiting = build_fiting_on_values(values, error_threshold=32)
        model_size = float(fiting.model_size_estimate())
        segment_count = float(len(fiting.segments))
        return model_size, segment_count

    except Exception:
        return 0.0, 0.0
    
def build_inference_rows(workload_path: str, freq_threshold: float = 0.1, max_width: int = 3,enable_btree: bool = True,
    enable_fiting: bool = True):
    queries = load_workload(workload_path)
    parsed_queries = [parse_sql(q) for q in queries]

    candidate_result = generate_all_candidates(
        parsed_queries=parsed_queries,
        freq_threshold=freq_threshold,
        max_width=max_width,
        enable_btree=enable_btree,
        enable_fiting=enable_fiting
    )
    all_candidates = candidate_result["all_candidates"]

    rows = []
    conn = get_conn()

    try:
        for qid, (sql, parsed_query) in enumerate(zip(queries, parsed_queries), start=1):
            query_cols = set(parsed_query.get("where_cols", [])) \
                | set(parsed_query.get("join_cols", [])) \
                | set(parsed_query.get("order_cols", [])) \
                | set(parsed_query.get("group_cols", []))

            relevant_candidates = []
            for cand in all_candidates:
                if any(col in query_cols for col in cand["columns"]):
                    relevant_candidates.append(cand)

            for candidate in relevant_candidates:
                if not fiting_candidate_supported_for_query(candidate, sql):
                    continue
                qf = build_query_feature(
                    parsed_query=parsed_query,
                    frequency=1.0,
                    selectivity=1.0
                )

                # 推理阶段真实估计 FITING 特征
                fiting_model_size, fiting_segment_count = estimate_fiting_features_for_candidate(
                    conn,
                    candidate,
                    sample_limit=10000
                )

                inf = build_index_feature(
                    candidate,
                    fiting_model_size=(fiting_model_size if candidate["index_type"] == "fiting" else None)
                )
                xf = build_interaction_feature(parsed_query, candidate)
                merged = merge_features(qf, inf, xf)

                merged["fiting_model_size"] = float(fiting_model_size)
                merged["fiting_segment_count"] = float(fiting_segment_count)

                row = {
                    "sql_id": qid,
                    "table_name": candidate.get("table_name", "unknown"),
                    "sql_text": sql,
                    "index_type": candidate["index_type"],
                    "index_cols": "|".join(candidate["columns"]),
                }
                row.update(merged)
                rows.append(row)
    finally:
        conn.close()

    df = pd.DataFrame(rows)
    return df


def predict_benefit(
    workload_path: str = "workload/test_workload.sql",
    model_path: str = "output/benefit_mlp.pt",
    scaler_path: str = "output/scaler.pkl",
    output_csv: str = "output/predictions.csv",
    enable_btree: bool = True,
    enable_fiting: bool = True
):
    os.makedirs("output", exist_ok=True)

    model, scaler, feature_cols = load_model_and_scaler(
        model_path=model_path,
        scaler_path=scaler_path
    )

    df = build_inference_rows(workload_path,enable_btree=enable_btree,enable_fiting=enable_fiting)
    if df.empty:
        raise ValueError("没有生成可预测的数据行。")

    for col in feature_cols:
        if col not in df.columns:
            df[col] = 0.0

    X = df[feature_cols].copy()

    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce")

    X = X.fillna(0.0)

    X_scaled = scaler.transform(X)
    X_tensor = torch.tensor(X_scaled, dtype=torch.float32)

    with torch.no_grad():
        pred = model(X_tensor).cpu().numpy().reshape(-1)

    df["pred_benefit"] = pred
    df.to_csv(output_csv, index=False, encoding="utf-8")

    print(f"[INFO] 预测结果已保存到: {output_csv}")
    print(df[[
        "sql_id", "index_type", "index_cols", "pred_benefit"
    ]].head(20))

    return df


if __name__ == "__main__":
    predict_benefit()