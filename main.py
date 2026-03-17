#V1
# from parser.load_workload import load_workload
# from parser.sql_parser import parse_sql
# from candidate.generator import generate_all_candidates


# def main():
#     queries = load_workload("workload/tpch.sql")
#     parsed_queries = [parse_sql(q) for q in queries]

#     result = generate_all_candidates(
#         parsed_queries=parsed_queries,
#         freq_threshold=0.1,
#         max_width=3
#     )

#     print("=== 高频列 ===")
#     print(result["high_freq_cols"])

#     print("\n=== 全部候选索引 ===")
#     for idx in result["all_candidates"]:
#         print(idx)


# if __name__ == "__main__":
#     main()

#V2
from parser.load_workload import load_workload
from parser.sql_parser import parse_sql
from candidate.generator import generate_all_candidates
from feature.query_feat import build_query_feature
from feature.index_feat import build_index_feature
from feature.interaction_feat import build_interaction_feature
from feature.merge_feat import merge_features


def main():
    queries = load_workload("workload/tpch.sql")
    parsed_queries = [parse_sql(q) for q in queries]

    result = generate_all_candidates(
        parsed_queries=parsed_queries,
        freq_threshold=0.1,
        max_width=3
    )

    all_candidates = result["all_candidates"]

    print("=== 示例特征构造 ===")
    for i, pq in enumerate(parsed_queries):
        print(f"\n--- Query {i+1} ---")
        print(pq)

        for cand in all_candidates[:3]:
            qf = build_query_feature(pq, frequency=1.0, selectivity=1.0)
            inf = build_index_feature(cand)
            xf = build_interaction_feature(pq, cand)
            merged = merge_features(qf, inf, xf)

            print("\nCandidate:", cand)
            print("Merged Feature:", merged)


if __name__ == "__main__":
    main()