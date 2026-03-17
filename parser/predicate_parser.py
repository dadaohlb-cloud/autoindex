import re


def extract_single_column_predicate(sql: str):
    sql_norm = " ".join(sql.strip().split())

    # col = 10
    m_eq = re.search(
        r"where\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([0-9]+)",
        sql_norm,
        re.I
    )
    if m_eq:
        return {
            "type": "eq",
            "column": m_eq.group(1),
            "value": int(m_eq.group(2))
        }

    # col > 10
    m_gt = re.search(
        r"where\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*>\s*([0-9]+)",
        sql_norm,
        re.I
    )
    if m_gt:
        return {
            "type": "gt",
            "column": m_gt.group(1),
            "value": int(m_gt.group(2))
        }

    # col >= 10
    m_ge = re.search(
        r"where\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*>=\s*([0-9]+)",
        sql_norm,
        re.I
    )
    if m_ge:
        return {
            "type": "ge",
            "column": m_ge.group(1),
            "value": int(m_ge.group(2))
        }

    # col < 10
    m_lt = re.search(
        r"where\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*<\s*([0-9]+)",
        sql_norm,
        re.I
    )
    if m_lt:
        return {
            "type": "lt",
            "column": m_lt.group(1),
            "value": int(m_lt.group(2))
        }

    # col <= 10
    m_le = re.search(
        r"where\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*<=\s*([0-9]+)",
        sql_norm,
        re.I
    )
    if m_le:
        return {
            "type": "le",
            "column": m_le.group(1),
            "value": int(m_le.group(2))
        }

    # col BETWEEN 5 AND 20
    m_between = re.search(
        r"where\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+between\s+([0-9]+)\s+and\s+([0-9]+)",
        sql_norm,
        re.I
    )
    if m_between:
        return {
            "type": "between",
            "column": m_between.group(1),
            "left": int(m_between.group(2)),
            "right": int(m_between.group(3))
        }

    return None


if __name__ == "__main__":
    tests = [
        "SELECT * FROM lineitem WHERE l_orderkey = 10",
        "SELECT * FROM lineitem WHERE l_quantity > 10",
        "SELECT * FROM lineitem WHERE l_quantity BETWEEN 5 AND 20",
    ]

    for sql in tests:
        print(sql)
        print(extract_single_column_predicate(sql))
        print()