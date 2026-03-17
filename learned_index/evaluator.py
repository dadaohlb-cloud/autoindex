import time
import bisect

from learned_index.fiting_tree import SimpleFitingTree


def build_sorted_keys(values):
    return sorted(values)


def btree_point_lookup(sorted_keys, key):
    idx = bisect.bisect_left(sorted_keys, key)
    if idx < len(sorted_keys) and sorted_keys[idx] == key:
        return idx
    return -1


def btree_range_lookup(sorted_keys, left_key, right_key):
    l = bisect.bisect_left(sorted_keys, left_key)
    r = bisect.bisect_right(sorted_keys, right_key)
    return list(range(l, r))


def timed_btree_point(sorted_keys, key, repeat=20):
    ts = []
    for _ in range(repeat):
        st = time.perf_counter()
        btree_point_lookup(sorted_keys, key)
        ts.append(time.perf_counter() - st)
    return sum(ts) / len(ts)


def timed_fiting_point(fiting, key, repeat=20):
    ts = []
    for _ in range(repeat):
        st = time.perf_counter()
        fiting.point_lookup(key)
        ts.append(time.perf_counter() - st)
    return sum(ts) / len(ts)


def timed_btree_range(sorted_keys, left_key, right_key, repeat=20):
    ts = []
    for _ in range(repeat):
        st = time.perf_counter()
        btree_range_lookup(sorted_keys, left_key, right_key)
        ts.append(time.perf_counter() - st)
    return sum(ts) / len(ts)


def timed_fiting_range(fiting, left_key, right_key, repeat=20):
    ts = []
    for _ in range(repeat):
        st = time.perf_counter()
        fiting.range_lookup(left_key, right_key)
        ts.append(time.perf_counter() - st)
    return sum(ts) / len(ts)


def build_fiting_on_values(values, error_threshold=32):
    sorted_keys = build_sorted_keys(values)
    fiting = SimpleFitingTree(error_threshold=error_threshold)
    fiting.fit(sorted_keys)
    return sorted_keys, fiting