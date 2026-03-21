import sqlglot
from sqlglot import exp


def normalize_column_name(col_name: str):
    if "." in col_name:
        return col_name.split(".")[-1]
    return col_name


def parse_sql(sql: str):
    tree = sqlglot.parse_one(sql)

    where_cols = []
    join_cols = []
    order_cols = []
    group_cols = []
    select_cols = []

    has_select_star = False
    alias_to_table = {}
    column_to_tables = {}
    all_tables = []

    for table in tree.find_all(exp.Table):
        table_name = table.name
        alias = table.alias_or_name
        alias_to_table[alias] = table_name
        if table_name not in all_tables:
            all_tables.append(table_name)

    def record_column(col):
        raw = col.sql()
        col_name = normalize_column_name(raw)

        if "." in raw:
            alias = raw.split(".")[0]
            real_table = alias_to_table.get(alias, alias)
            column_to_tables.setdefault(col_name, set()).add(real_table)
        else:
            if len(all_tables) == 1:
                column_to_tables.setdefault(col_name, set()).add(all_tables[0])

        return col_name

    # 修复：显式识别 SELECT * / t.*
    for node in tree.find_all(exp.Select):
        for expr in node.expressions or []:
            if isinstance(expr, exp.Star):
                has_select_star = True
                select_cols.append("*")
                continue

            has_star_inside = False
            for sub in expr.walk():
                if isinstance(sub, exp.Star):
                    has_select_star = True
                    has_star_inside = True
                    select_cols.append("*")
                    break
            if has_star_inside:
                continue

            for col in expr.find_all(exp.Column):
                select_cols.append(record_column(col))

    for node in tree.find_all(exp.Where):
        for col in node.find_all(exp.Column):
            where_cols.append(record_column(col))

    for node in tree.find_all(exp.Join):
        on_expr = node.args.get("on")
        if on_expr:
            for col in on_expr.find_all(exp.Column):
                join_cols.append(record_column(col))

    for node in tree.find_all(exp.Order):
        for col in node.find_all(exp.Column):
            order_cols.append(record_column(col))

    for node in tree.find_all(exp.Group):
        for col in node.find_all(exp.Column):
            group_cols.append(record_column(col))

    sql_lower = " ".join(sql.lower().split())
    predicate_type = {
        "eq": "=" in sql and all(op not in sql for op in [">=", "<=", "<>", "!=", ">", "<"]),
        "range": any(op in sql_lower for op in [">", "<", ">=", "<=", " between "]),
        "join": " join " in sql_lower,
    }

    return {
        "where_cols": sorted(set(where_cols)),
        "join_cols": sorted(set(join_cols)),
        "order_cols": sorted(set(order_cols)),
        "group_cols": sorted(set(group_cols)),
        "select_cols": sorted(set(select_cols)),
        "select_star": has_select_star,
        "predicate_type": predicate_type,
        "alias_to_table": alias_to_table,
        "column_to_tables": {k: sorted(v) for k, v in column_to_tables.items()},
        "all_tables": all_tables,
    }

if __name__ == "__main__":
    test_sqls = [
        "SELECT * FROM lineitem WHERE l_orderkey = 10",
        "SELECT * FROM orders WHERE o_custkey = 100 ORDER BY o_orderdate",
        "SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey WHERE c.c_nationkey = 3",
        "SELECT l_partkey, COUNT(*) FROM lineitem WHERE l_quantity > 10 GROUP BY l_partkey ORDER BY l_partkey",
    ]

    for i, sql in enumerate(test_sqls, 1):
        print(f"\n--- SQL {i} ---")
        print(sql)
        print(parse_sql(sql))