from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple
from psycopg2 import sql


# -----------------------------
# 简单缓存，避免重复扫表
# key 形式：
# ("row_count", table_name)
# ("col_stats", table_name, col_name)
# -----------------------------
_STATS_CACHE: Dict[Tuple, dict] = {}


# PostgreSQL 中常见的数值类型
_NUMERIC_TYPES = {
    "smallint",
    "integer",
    "bigint",
    "decimal",
    "numeric",
    "real",
    "double precision",
    "smallserial",
    "serial",
    "bigserial",
    "money",
}


def _split_table_name(table_name: str) -> Tuple[str, str]:
    if "." in table_name:
        schema_name, pure_table_name = table_name.split(".", 1)
        return schema_name, pure_table_name
    return "public", table_name


def _safe_table_identifier(table_name: str) -> sql.Composed:
    """
    生成安全的表标识符，防止表名拼接出错。
    """
    schema_name, pure_table_name = _split_table_name(table_name)
    return sql.Identifier(schema_name, pure_table_name)


def _safe_column_identifier(col_name: str) -> sql.Identifier:
    """
    生成安全的列标识符。
    """
    return sql.Identifier(col_name)


def clear_stats_cache() -> None:
    _STATS_CACHE.clear()


def get_column_type(conn, table_name: str, col_name: str) -> Optional[str]:
    """
    获取列的数据类型（来自 information_schema.columns）。
    返回 PostgreSQL 的 data_type 字符串，如:
      - integer
      - numeric
      - character varying
      - date
    """
    schema_name, pure_table_name = _split_table_name(table_name)

    query = """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, pure_table_name, col_name))
        row = cur.fetchone()

    if not row:
        return None
    return row[0]


def is_numeric_column(conn, table_name: str, col_name: str) -> bool:
    """
    判断列是否为数值类型。
    """
    col_type = get_column_type(conn, table_name, col_name)
    return col_type in _NUMERIC_TYPES


def get_table_row_count(conn, table_name: str, use_cache: bool = True) -> int:
    """
    获取表总行数。
    """
    cache_key = ("row_count", table_name)
    if use_cache and cache_key in _STATS_CACHE:
        return int(_STATS_CACHE[cache_key]["value"])

    query = sql.SQL("SELECT COUNT(*) FROM {};").format(
        _safe_table_identifier(table_name)
    )

    with conn.cursor() as cur:
        cur.execute(query)
        row_count = cur.fetchone()[0]

    row_count = int(row_count or 0)

    if use_cache:
        _STATS_CACHE[cache_key] = {"value": row_count}

    return row_count


def get_column_ndv(conn, table_name: str, col_name: str, use_cache: bool = True) -> int:
    """
    获取列的不同值数量 NDV（COUNT DISTINCT）。
    """
    stats = get_column_basic_stats(conn, table_name, col_name, use_cache=use_cache)
    return int(stats["ndv"])


def get_column_null_ratio(
    conn, table_name: str, col_name: str, use_cache: bool = True
) -> float:
    """
    获取列的空值比例:
      null_count / row_count
    """
    stats = get_column_basic_stats(conn, table_name, col_name, use_cache=use_cache)
    return float(stats["null_ratio"])


def get_column_basic_stats(
    conn, table_name: str, col_name: str, use_cache: bool = True
) -> Dict[str, float]:
    """
    获取列的基础统计:
      - row_count
      - ndv
      - ndv_ratio
      - null_count
      - null_ratio

    这是 index_feat.py 最常用的一组统计。
    """
    cache_key = ("col_basic_stats", table_name, col_name)
    if use_cache and cache_key in _STATS_CACHE:
        return _STATS_CACHE[cache_key].copy()

    row_count = get_table_row_count(conn, table_name, use_cache=use_cache)

    query = sql.SQL(
        """
        SELECT
            COUNT(DISTINCT {col}) AS ndv,
            SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count
        FROM {table};
        """
    ).format(
        col=_safe_column_identifier(col_name),
        table=_safe_table_identifier(table_name),
    )

    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()

    ndv = int((row[0] or 0))
    null_count = int((row[1] or 0))
    ndv_ratio = float(ndv / row_count) if row_count > 0 else 0.0
    null_ratio = float(null_count / row_count) if row_count > 0 else 0.0

    result = {
        "row_count": float(row_count),
        "ndv": float(ndv),
        "ndv_ratio": ndv_ratio,
        "null_count": float(null_count),
        "null_ratio": null_ratio,
    }

    if use_cache:
        _STATS_CACHE[cache_key] = result.copy()

    return result


def get_numeric_column_distribution(
    conn, table_name: str, col_name: str, use_cache: bool = True
) -> Dict[str, float]:
    """
    获取数值列的分布统计:
      - min_value
      - max_value
      - q1
      - q3
      - range_span = max - min
      - iqr_span = q3 - q1

    若不是数值列，统一返回 0。
    若列为空，也返回 0。
    """
    cache_key = ("col_numeric_dist", table_name, col_name)
    if use_cache and cache_key in _STATS_CACHE:
        return _STATS_CACHE[cache_key].copy()

    if not is_numeric_column(conn, table_name, col_name):
        result = {
            "min_value": 0.0,
            "max_value": 0.0,
            "q1": 0.0,
            "q3": 0.0,
            "range_span": 0.0,
            "iqr_span": 0.0,
        }
        if use_cache:
            _STATS_CACHE[cache_key] = result.copy()
        return result

    query = sql.SQL(
        """
        SELECT
            MIN({col})::double precision AS min_value,
            MAX({col})::double precision AS max_value,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY {col})::double precision AS q1,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY {col})::double precision AS q3
        FROM {table}
        WHERE {col} IS NOT NULL;
        """
    ).format(
        col=_safe_column_identifier(col_name),
        table=_safe_table_identifier(table_name),
    )

    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()

    min_value = float(row[0]) if row and row[0] is not None else 0.0
    max_value = float(row[1]) if row and row[1] is not None else 0.0
    q1 = float(row[2]) if row and row[2] is not None else 0.0
    q3 = float(row[3]) if row and row[3] is not None else 0.0

    result = {
        "min_value": min_value,
        "max_value": max_value,
        "q1": q1,
        "q3": q3,
        "range_span": float(max_value - min_value),
        "iqr_span": float(q3 - q1),
    }

    if use_cache:
        _STATS_CACHE[cache_key] = result.copy()

    return result


def get_column_stats(
    conn, table_name: str, col_name: str, use_cache: bool = True
) -> Dict[str, float]:
    """
    获取单列完整统计。
    合并:
      - 基础统计
      - 数值分布统计
      - 列类型
      - 是否数值列
    """
    basic = get_column_basic_stats(conn, table_name, col_name, use_cache=use_cache)
    dist = get_numeric_column_distribution(conn, table_name, col_name, use_cache=use_cache)
    col_type = get_column_type(conn, table_name, col_name)

    result = {
        "column_type": col_type or "",
        "is_numeric": 1.0 if (col_type in _NUMERIC_TYPES) else 0.0,
        **basic,
        **dist,
    }
    return result


def summarize_index_columns(
    conn, table_name: str, columns: Iterable[str], use_cache: bool = True
) -> Dict[str, float]:
    """
    对一个候选索引的多列统计做“按列聚合”。
    用于 index_feat.py 中快速构造复合索引统计特征。

    输出：
      - row_count
      - ndv_mean
      - ndv_ratio_mean
      - null_ratio_mean
      - range_span_mean
      - iqr_span_mean
      - numeric_col_ratio
    """
    columns = list(columns)
    if not columns:
        return {
            "row_count": 0.0,
            "ndv_mean": 0.0,
            "ndv_ratio_mean": 0.0,
            "null_ratio_mean": 0.0,
            "range_span_mean": 0.0,
            "iqr_span_mean": 0.0,
            "numeric_col_ratio": 0.0,
        }

    row_count = float(get_table_row_count(conn, table_name, use_cache=use_cache))

    ndvs: List[float] = []
    ndv_ratios: List[float] = []
    null_ratios: List[float] = []
    range_spans: List[float] = []
    iqr_spans: List[float] = []
    numeric_flags: List[float] = []

    for col in columns:
        stats = get_column_stats(conn, table_name, col, use_cache=use_cache)
        ndvs.append(float(stats["ndv"]))
        ndv_ratios.append(float(stats["ndv_ratio"]))
        null_ratios.append(float(stats["null_ratio"]))
        range_spans.append(float(stats["range_span"]))
        iqr_spans.append(float(stats["iqr_span"]))
        numeric_flags.append(float(stats["is_numeric"]))

    n = float(len(columns))

    return {
        "row_count": row_count,
        "ndv_mean": sum(ndvs) / n,
        "ndv_ratio_mean": sum(ndv_ratios) / n,
        "null_ratio_mean": sum(null_ratios) / n,
        "range_span_mean": sum(range_spans) / n,
        "iqr_span_mean": sum(iqr_spans) / n,
        "numeric_col_ratio": sum(numeric_flags) / n,
    }