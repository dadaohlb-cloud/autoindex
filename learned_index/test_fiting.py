from learned_index.fiting_tree import SimpleFitingTree


def main():
    keys = list(range(1, 1001))

    tree = SimpleFitingTree(error_threshold=8)
    tree.fit(keys)

    print("段数量:", len(tree.segments))
    print("模型大小估计:", tree.model_size_estimate())

    test_keys = [1, 10, 100, 500, 999, 1001]
    for k in test_keys:
        pos = tree.point_lookup(k)
        print(f"key={k}, pos={pos}")

    result = tree.range_lookup(100, 120)
    print("range [100,120] 命中数量:", len(result))
    print("前5个位置:", result[:5])


if __name__ == "__main__":
    main()