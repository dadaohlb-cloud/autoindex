import os
import time
import hashlib
import pandas as pd
import sqlglot
from sqlglot import exp

from db.pg import get_conn
from parser.load_workload import load_workload
from parser.sql_parser import parse_sql
from candidate.generator import generate_all_candidates
from feature.query_feat import build_query_feature
from feature.index_feat import build_index_feature
from feature.interaction_feat import build_interaction_feature
from feature.merge_feat import merge_features

from parser.predicate_parser import extract_single_column_predicate
from learned_index.evaluator import (
    build_fiting_on_values,
    timed_btree_point,
    timed_fiting_point,
    timed_btree_range,
    timed_fiting_range,
)

def fetch_column_values(conn, table_name: str, column_name: str, limit: int = 10000):
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

    values = [r[0] for r in rows]

    # 只保留数值型 / 可转数值的值
    cleaned = []
    for v in values:
        if isinstance(v, (int, float)):
            cleaned.append(v)
        else:
            # 日期等后面再扩展；当前阶段只保留数值
            try:
                cleaned.append(float(v))
            except Exception:
                pass

    return cleaned

# t_btree：模拟 B+Tree 访问阶段时间
# t_fiting：模拟 FITing-Tree 访问阶段时间
# label = (t_btree - t_fiting) / t_btree
# 如果 FITing 更快，label 就大；如果不占优，label 就接近 0。
# def compute_fiting_label(conn, table_name: str, candidate: dict, sql: str):
#     if candidate["index_type"] != "fiting":
#         return None, None

#     if len(candidate["columns"]) != 1:
#         return None, None

#     predicate = extract_single_column_predicate(sql)
#     if predicate is None:
#         return None, None

#     col = candidate["columns"][0]
#     if predicate["column"] != col:
#         return None, None

#     print(f"[DEBUG] FITING start: table={table_name}, col={col}")

#     values = fetch_column_values(conn, table_name, col, limit=10000)
#     print(f"[DEBUG] fetched values: {len(values)}")
#     conn.commit()

#     if len(values) == 0:
#         return None, None

#     sorted_keys, fiting = build_fiting_on_values(values, error_threshold=32)
#     print(f"[DEBUG] built fiting segments: {len(fiting.segments)}")

#     if predicate["type"] == "eq":
#         key = predicate["value"]
#         t_btree = timed_btree_point(sorted_keys, key)
#         t_fiting = timed_fiting_point(fiting, key)

#     elif predicate["type"] in ("gt", "ge"):
#         key = predicate["value"]
#         t_btree = timed_btree_range(sorted_keys, key, sorted_keys[-1])
#         t_fiting = timed_fiting_range(fiting, key, sorted_keys[-1])

#     elif predicate["type"] in ("lt", "le"):
#         key = predicate["value"]
#         t_btree = timed_btree_range(sorted_keys, sorted_keys[0], key)
#         t_fiting = timed_fiting_range(fiting, sorted_keys[0], key)

#     elif predicate["type"] == "between":
#         left_key = predicate["left"]
#         right_key = predicate["right"]
#         t_btree = timed_btree_range(sorted_keys, left_key, right_key)
#         t_fiting = timed_fiting_range(fiting, left_key, right_key)

#     else:
#         return None, None

#     label = compute_benefit_label(t_btree, t_fiting)
#     print(f"[DEBUG] fiting done: t_btree={t_btree:.8f}, label={label:.6f}")

#     return t_btree, label
import math
def compute_fiting_label(conn, table_name: str, candidate: dict, sql: str):
    if candidate["index_type"] != "fiting":
        return None

    if len(candidate["columns"]) != 1:
        return None

    predicate = extract_single_column_predicate(sql)
    if predicate is None:
        return None

    col = candidate["columns"][0]
    if predicate["column"] != col:
        return None

    print(f"[DEBUG] FITING start: table={table_name}, col={col}")

    values = fetch_column_values(conn, table_name, col, limit=10000)
    print(f"[DEBUG] fetched values: {len(values)}")

    if len(values) == 0:
        return None

    sorted_keys, fiting = build_fiting_on_values(values, error_threshold=32)
    print(f"[DEBUG] built fiting segments: {len(fiting.segments)}")

    n = len(sorted_keys)
    btree_base_cost = math.log2(max(n, 2))

    if predicate["type"] == "eq":
        key = predicate["value"]
        seg, _ = fiting.predict_position(key)
        if seg is None:
            return None

        error_window = math.ceil(seg.max_error) * 2 + 1

        btree_cost = btree_base_cost
        fiting_cost = 1.0 + math.log2(max(error_window, 2))

    elif predicate["type"] in ("gt", "ge"):
        key = predicate["value"]
        result_count = sum(1 for v in sorted_keys if v >= key)

        seg, _ = fiting.predict_position(key)
        if seg is None:
            return None

        error_window = math.ceil(seg.max_error) * 2 + 1

        btree_cost = btree_base_cost + result_count
        fiting_cost = 1.0 + error_window + result_count

    elif predicate["type"] in ("lt", "le"):
        key = predicate["value"]
        result_count = sum(1 for v in sorted_keys if v <= key)

        seg, _ = fiting.predict_position(key)
        if seg is None:
            return None

        error_window = math.ceil(seg.max_error) * 2 + 1

        btree_cost = btree_base_cost + result_count
        fiting_cost = 1.0 + error_window + result_count

    elif predicate["type"] == "between":
        left_key = predicate["left"]
        right_key = predicate["right"]
        result_count = sum(1 for v in sorted_keys if left_key <= v <= right_key)

        seg_l, _ = fiting.predict_position(left_key)
        seg_r, _ = fiting.predict_position(right_key)
        if seg_l is None or seg_r is None:
            return None

        error_window = (math.ceil(seg_l.max_error) * 2 + 1) + (math.ceil(seg_r.max_error) * 2 + 1)

        btree_cost = btree_base_cost + result_count
        fiting_cost = 2.0 + error_window + result_count

    else:
        return None

    label = compute_benefit_label(btree_cost, fiting_cost)

    print(
        f"[DEBUG] fiting done: "
        f"btree_cost={btree_cost:.4f}, fiting_cost={fiting_cost:.4f}, label={label:.6f}"
    )

    return {
        "btree_cost": btree_cost,
        "label": label,
        "fiting_model_size": fiting.model_size_estimate(),
        "fiting_segment_count": len(fiting.segments),
    }


def safe_index_name(table_name: str, columns: tuple):
    raw = f"{table_name}_{'_'.join(columns)}"
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()[:8]
    name = f"idx_{table_name}_{digest}"
    return name[:60]


def extract_tables(sql: str):
    try:
        tree = sqlglot.parse_one(sql)
        tables = []
        for table in tree.find_all(exp.Table):
            name = table.name
            if name and name not in tables:
                tables.append(name)
        return tables
    except Exception:
        return []


def get_table_columns(conn, table_name: str):
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table_name,))
        rows = cur.fetchall()
    return {r[0] for r in rows}


def candidate_matches_table(conn, table_name: str, candidate: dict):
    table_cols = get_table_columns(conn, table_name)
    return all(col in table_cols for col in candidate["columns"])


def filter_relevant_candidates(parsed_query: dict, all_candidates: list):
    query_cols = set(parsed_query.get("where_cols", [])) \
        | set(parsed_query.get("join_cols", [])) \
        | set(parsed_query.get("order_cols", [])) \
        | set(parsed_query.get("group_cols", []))

    result = []
    for cand in all_candidates:
        if any(col in query_cols for col in cand["columns"]):
            result.append(cand)
    return result


def create_index(conn, table_name: str, candidate: dict):
    columns = candidate["columns"]
    idx_name = safe_index_name(table_name, columns)
    cols_sql = ", ".join(columns)

    sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({cols_sql});"

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

    return idx_name


def drop_index(conn, idx_name: str):
    sql = f"DROP INDEX IF EXISTS {idx_name};"
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


import re


def timed_query(conn, sql: str, repeat: int = 1):
    """
    使用 EXPLAIN ANALYZE 获取 PostgreSQL 实际执行时间（毫秒），再转成秒。
    """
    explain_sql = f"EXPLAIN ANALYZE {sql}"

    times = []
    with conn.cursor() as cur:
        for _ in range(repeat):
            cur.execute(explain_sql)
            rows = cur.fetchall()

            exec_time_ms = None
            for row in rows:
                line = row[0]
                m = re.search(r"Execution Time: ([0-9.]+) ms", line)
                if m:
                    exec_time_ms = float(m.group(1))
                    break

            if exec_time_ms is None:
                raise ValueError("未能从 EXPLAIN ANALYZE 输出中解析 Execution Time")

            times.append(exec_time_ms / 1000.0)

    return sum(times) / len(times)


def compute_benefit_label(t_no_index: float, t_with_index: float):
    if t_no_index <= 0:
        return 0.0

    ratio = (t_no_index - t_with_index) / t_no_index
    ratio = max(0.0, min(1.0, ratio))
    return ratio


def build_dataset(
    workload_path: str,
    output_csv: str = "output/train.csv",
    freq_threshold: float = 0.1,
    max_width: int = 3,
    max_candidates_per_query: int = 2,
    repeat: int = 1
):
    os.makedirs("output", exist_ok=True)

    queries = load_workload(workload_path)
    parsed_queries = [parse_sql(q) for q in queries]

    candidate_result = generate_all_candidates(
        parsed_queries=parsed_queries,
        freq_threshold=freq_threshold,
        max_width=max_width
    )
    all_candidates = candidate_result["all_candidates"]

    rows = []

    for qid, (sql, parsed_query) in enumerate(zip(queries, parsed_queries), start=1):
        conn = get_conn()

        try:
            print(f"\n[INFO] Processing Query {qid}")
            print(sql)

            tables = extract_tables(sql)
            if not tables:
                print(f"[WARN] Query {qid} 无法识别表，跳过")
                conn.close()
                continue

            try:
                print(f"[DEBUG] baseline start for Query {qid}")
                t_no_index = timed_query(conn, sql, repeat=repeat)
                conn.commit()
                print(f"[DEBUG] baseline end for Query {qid}")
            except Exception as e:
                conn.rollback()
                print(f"[WARN] Query {qid} 无索引执行失败，跳过: {e}")
                conn.close()
                continue

            print(f"[INFO] Baseline time: {t_no_index:.6f}s")

            relevant_candidates = filter_relevant_candidates(parsed_query, all_candidates)

            usable = []
            for cand in relevant_candidates:
                matched_table = None
                for table_name in tables:
                    try:
                        if candidate_matches_table(conn, table_name, cand):
                            matched_table = table_name
                            break
                    except Exception:
                        conn.rollback()

                if matched_table is not None:
                    usable.append((matched_table, cand))

            usable = sorted(
                usable,
                key=lambda x: candidate_priority(parsed_query, x[1]),
                reverse=True
            )
            used_candidates = usable[:max_candidates_per_query]

            if not used_candidates:
                print(f"[WARN] Query {qid} 没有可用候选索引")
                conn.close()
                continue

            for cid, (table_name, candidate) in enumerate(used_candidates, start=1):
                idx_name = None

                try:
                    if candidate["index_type"] == "btree":
                        idx_name = create_index(conn, table_name, candidate)
                        t_with_index = timed_query(conn, sql, repeat=repeat)
                        conn.commit()

                        label = compute_benefit_label(t_no_index, t_with_index)
                        indexed_time_value = t_with_index
                        fiting_model_size = 0.0
                        fiting_segment_count = 0

                    elif candidate["index_type"] == "fiting":
                        fiting_info = compute_fiting_label(conn, table_name, candidate, sql)

                        if fiting_info is None:
                            print(f"[WARN] Candidate {cid} fiting 暂不支持该查询，跳过: table={table_name}, candidate={candidate}")
                            continue

                        label = fiting_info["label"]
                        indexed_time_value = None
                        fiting_model_size = fiting_info["fiting_model_size"]
                        fiting_segment_count = fiting_info["fiting_segment_count"]

                    else:
                        continue

                    qf = build_query_feature(
                        parsed_query=parsed_query,
                        frequency=1.0,
                        selectivity=1.0
                    )
                    # inf = build_index_feature(candidate, fiting_model_size=fiting_model_size)
                    inf = build_index_feature(
                        candidate=candidate,
                        fiting_model_size=fiting_model_size,
                        conn=conn,
                        parsed_query=parsed_query,
                    )
                    print(f"[DEBUG] index features for candidate {cid}: {inf}")
                    xf = build_interaction_feature(parsed_query, candidate)
                    merged = merge_features(qf, inf, xf)

                    row = {
                        "sql_id": qid,
                        "candidate_id": cid,
                        "table_name": table_name,
                        "sql_text": sql,
                        "index_type": candidate["index_type"],
                        "index_cols": "|".join(candidate["columns"]),
                        "baseline_time": t_no_index,
                        "indexed_time": indexed_time_value,
                        "label": label,
                        "fiting_model_size": fiting_model_size,
                        "fiting_segment_count": fiting_segment_count,
                    }
                    row.update(merged)
                    rows.append(row)

                    if candidate["index_type"] == "btree":
                        print(
                            f"[INFO] Candidate {cid}: table={table_name}, type=btree, cols={candidate['columns']} | "
                            f"t_idx={indexed_time_value:.6f}s | label={label:.6f}"
                        )
                    else:
                        print(
                            f"[INFO] Candidate {cid}: table={table_name}, type=fiting, cols={candidate['columns']} | "
                            f"label={label:.6f}"
                        )

                except Exception as e:
                    conn.rollback()
                    print(f"[WARN] Candidate {cid} 失败: table={table_name}, candidate={candidate} | {e}")

                finally:
                    if idx_name is not None:
                        try:
                            drop_index(conn, idx_name)
                        except Exception as e:
                            conn.rollback()
                            print(f"[WARN] 删除索引失败 {idx_name}: {e}")

        finally:
            try:
                conn.close()
            except Exception:
                pass

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"\n[INFO] 数据集已保存到: {output_csv}")
    print(f"[INFO] 样本数: {len(df)}")
    return df

def candidate_priority(parsed_query: dict, candidate: dict):
    predicate_cols = set(parsed_query.get("where_cols", [])) | set(parsed_query.get("join_cols", []))
    order_cols = set(parsed_query.get("order_cols", []))
    group_cols = set(parsed_query.get("group_cols", []))
    cand_cols = set(candidate["columns"])

    score = 0

    # 命中谓词列优先级最高
    if cand_cols & predicate_cols:
        score += 100

    # learned index 对单列谓词列优先
    if candidate["index_type"] == "fiting" and len(candidate["columns"]) == 1:
        if list(candidate["columns"])[0] in predicate_cols:
            score += 80

    # 命中排序/分组列其次
    if cand_cols & order_cols:
        score += 20
    if cand_cols & group_cols:
        score += 10

    # 列数少的优先一点
    score -= len(candidate["columns"])

    return score

if __name__ == "__main__":
    df = build_dataset(
        workload_path="workload/tpch.sql",
        output_csv="output/train.csv",
        freq_threshold=0.1,
        max_width=3,
        max_candidates_per_query=2,
        repeat=2
    )
    print(df.head())