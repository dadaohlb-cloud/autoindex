import os
import pandas as pd


def build_index_name(table_name: str, index_cols: list):
    return f"idx_{table_name}_{'_'.join(index_cols)}"


def guess_table_name(index_cols: list):
    # 当前系统还没有完整“列->表”字典，这里先做一个简单映射
    # 只适配你现在的 tpch.sql 示例
    first_col = index_cols[0]

    if first_col.startswith("l_"):
        return "lineitem"
    elif first_col.startswith("o_"):
        return "orders"
    elif first_col.startswith("c_"):
        return "customer"
    else:
        return "unknown_table"


def export_create_index_sql(
    selected_csv: str = "output/selected_indexes.csv",
    output_sql: str = "output/recommended_indexes.sql"
):
    os.makedirs("output", exist_ok=True)

    df = pd.read_csv(selected_csv)

    sql_lines = []

    for _, row in df.iterrows():
        index_type = row["index_type"]
        index_cols = row["index_cols"].split("|")
        table_name = row["table_name"]

        if index_type == "btree":
            idx_name = build_index_name(table_name, index_cols)
            cols_sql = ", ".join(index_cols)
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({cols_sql});"
            sql_lines.append(sql)

        elif index_type == "fiting":
            # 当前先导出说明信息，后面再做真实部署
            sql_lines.append(
                f"-- FITING index candidate on {table_name} ({', '.join(index_cols)}) needs external build module"
            )

    with open(output_sql, "w", encoding="utf-8") as f:
        for line in sql_lines:
            f.write(line + "\n")

    print(f"[INFO] 推荐索引 SQL 已保存到: {output_sql}")
    for line in sql_lines:
        print(line)

    return sql_lines


if __name__ == "__main__":
    export_create_index_sql()