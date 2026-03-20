from __future__ import annotations
from typing import Dict, Iterable, Optional
from feature.stats import summarize_index_columns

def estimate_key_position_mean(columns: Iterable[str]) -> float:
    """
    键列顺序的平均位置编码。
    """
    cols = list(columns)
    if not cols:
        return 0.0
    positions = list(range(1, len(cols) + 1))
    return float(sum(positions) / len(positions))

def estimate_covering_feature(parsed_query: Optional[dict], candidate: dict) -> float:
    """
    是否覆盖查询所需列。
    """
    if not parsed_query:
        return 0.0
    required_cols = set(parsed_query.get("select_cols", []))
    required_cols |= set(parsed_query.get("where_cols", []))
    required_cols |= set(parsed_query.get("join_cols", []))
    required_cols |= set(parsed_query.get("order_cols", []))
    required_cols |= set(parsed_query.get("group_cols", []))
    required_cols.discard("*")

    if not required_cols:
        return 0.0

    index_cols = set(candidate.get("columns", []))
    return 1.0 if required_cols.issubset(index_cols) else 0.0

def estimate_btree_storage(row_count: float, col_count: int) -> float:
    """
    B+Tree 空间估计。随列数增加而线性增大
    """
    if row_count <= 0 or col_count <= 0:
        return 0.0
    base_overhead_bytes = 8.0
    per_entry_per_col_bytes = 4.0
    storage_bytes = base_overhead_bytes + row_count * col_count * per_entry_per_col_bytes
    storage_mb = storage_bytes / (1024 * 1024)
    return float(storage_mb)

def estimate_fiting_storage(
    row_count: float,
    col_count: int,
    fiting_model_size: Optional[float] = None,
    numeric_col_ratio: float = 1.0,
) -> float:
    """
    FITing-Tree 空间估计
    - FITing 目前在你系统中本质上还是单列 learned index
    - 分段数、参数数
    """
    if fiting_model_size is not None:
        return float(fiting_model_size/ (1024 * 1024))
    if row_count <= 0 or col_count <= 0:
        return 0.0
    import math
    base_overhead_bytes = 128.0
    segment_factor = max(1.0, math.log2(max(row_count, 2)))
    numeric_bonus = 1.0 if numeric_col_ratio > 0 else 1.5
    storage_bytes = base_overhead_bytes + segment_factor * 16.0 * numeric_bonus
    storage_mb = storage_bytes / (1024 * 1024)
    return float(storage_mb)

def estimate_index_storage(
    candidate: dict,
    row_count: float = 0.0,
    numeric_col_ratio: float = 1.0,
    fiting_model_size: Optional[float] = None,
) -> Dict[str, float]:
    """
    i_storage_est 根据当前 index_type 选择对应值。
    """
    col_count = len(candidate.get("columns", []))
    btree_storage = estimate_btree_storage(
        row_count=row_count,
        col_count=col_count,
    )
    fiting_storage = estimate_fiting_storage(
        row_count=row_count,
        col_count=col_count,
        fiting_model_size=fiting_model_size,
        numeric_col_ratio=numeric_col_ratio,
    )
    index_type = candidate.get("index_type", "")
    if index_type == "btree":
        storage_est = btree_storage
    elif index_type == "fiting":
        storage_est = fiting_storage
    else:
        storage_est = max(btree_storage, fiting_storage)

    return {
        "i_storage_est_btree": float(btree_storage),
        "i_storage_est_fiting": float(fiting_storage),
        "i_storage_est": float(storage_est),
    }

def build_index_feature(
    candidate: dict,
    fiting_model_size: Optional[float] = None,
    conn=None,
    parsed_query: Optional[dict] = None,
) -> Dict[str, float]:
    """
    构建索引特征。基础结构特征 多列聚合统计 存储开销特征
    """
    columns = tuple(candidate.get("columns", ()))
    table_name = candidate.get("table_name", "")
    col_count = len(columns)

    feature = {
        "i_type_btree": float(candidate.get("index_type") == "btree"),
        "i_type_fiting": float(candidate.get("index_type") == "fiting"),
        "i_col_count": float(col_count),
        "i_is_single": float(col_count == 1),
        "i_is_multi": float(col_count > 1),
        "i_key_pos_mean": estimate_key_position_mean(columns),
        "i_is_covering": estimate_covering_feature(parsed_query, candidate),
    }
    if conn is None or not table_name or not columns:
        storage = estimate_index_storage(
            candidate=candidate,
            row_count=0.0,
            numeric_col_ratio=1.0,
            fiting_model_size=fiting_model_size,
        )
        feature.update({
            "i_row_count": 0.0,
            "i_ndv_mean": 0.0,
            "i_ndv_ratio_mean": 0.0,
            "i_null_ratio_mean": 0.0,
            "i_range_span_mean": 0.0,
            "i_iqr_span_mean": 0.0,
            "i_numeric_col_ratio": 0.0,
        })
        feature.update(storage)
        return feature

    stats = summarize_index_columns(
        conn=conn,
        table_name=table_name,
        columns=columns,
        use_cache=True,
    )

    feature.update({
        "i_row_count": float(stats["row_count"]),
        "i_ndv_mean": float(stats["ndv_mean"]),
        "i_ndv_ratio_mean": float(stats["ndv_ratio_mean"]),
        "i_null_ratio_mean": float(stats["null_ratio_mean"]),
        "i_range_span_mean": float(stats["range_span_mean"]),
        "i_iqr_span_mean": float(stats["iqr_span_mean"]),
        "i_numeric_col_ratio": float(stats["numeric_col_ratio"]),
    })

    storage = estimate_index_storage(
        candidate=candidate,
        row_count=float(stats["row_count"]),
        numeric_col_ratio=float(stats["numeric_col_ratio"]),
        fiting_model_size=fiting_model_size,
    )
    feature.update(storage)

    return feature


if __name__ == "__main__":
    sample_index = {
        "index_type": "btree",
        "table_name": "lineitem",
        "columns": ("l_quantity", "l_partkey"),
        "width": 2,
    }

    # 无 conn 时，自动退化为轻量版本
    feat = build_index_feature(sample_index, fiting_model_size=None, conn=None, parsed_query=None)
    print(feat)