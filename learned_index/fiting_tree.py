import math
import bisect
import numpy as np


class FitingSegment:
    def __init__(self, start_key, end_key, a, b, max_error, start_pos, end_pos):
        self.start_key = start_key
        self.end_key = end_key
        self.a = a
        self.b = b
        self.max_error = max_error
        self.start_pos = start_pos
        self.end_pos = end_pos

    def __repr__(self):
        return (
            f"FitingSegment(start_key={self.start_key}, end_key={self.end_key}, "
            f"a={self.a:.6f}, b={self.b:.6f}, max_error={self.max_error:.2f}, "
            f"start_pos={self.start_pos}, end_pos={self.end_pos})"
        )


class SimpleFitingTree:
    def __init__(self, error_threshold=32):
        self.error_threshold = error_threshold
        self.keys = []
        self.positions = []
        self.segments = []

    def fit(self, sorted_keys):
        if not sorted_keys:
            self.keys = []
            self.positions = []
            self.segments = []
            return

        self.keys = list(sorted_keys)
        self.positions = list(range(len(sorted_keys)))
        self.segments = self._build_segments(self.keys)

    def _linear_fit(self, xs, ys):
        if len(xs) == 1:
            return 0.0, float(ys[0])

        x = np.array(xs, dtype=float)
        y = np.array(ys, dtype=float)

        x_mean = x.mean()
        y_mean = y.mean()

        denom = ((x - x_mean) ** 2).sum()
        if denom == 0:
            a = 0.0
        else:
            a = ((x - x_mean) * (y - y_mean)).sum() / denom

        b = y_mean - a * x_mean
        return float(a), float(b)

    def _max_error(self, xs, ys, a, b):
        preds = [a * x + b for x in xs]
        errs = [abs(p - y) for p, y in zip(preds, ys)]
        return max(errs) if errs else 0.0

    def _build_segments(self, keys):
        segments = []
        n = len(keys)

        left = 0
        while left < n:
            right = left + 1
            best_right = left
            best_seg = None

            while right <= n:
                xs = keys[left:right]
                ys = list(range(left, right))

                a, b = self._linear_fit(xs, ys)
                max_err = self._max_error(xs, ys, a, b)

                if max_err <= self.error_threshold:
                    best_right = right
                    best_seg = FitingSegment(
                        start_key=keys[left],
                        end_key=keys[right - 1],
                        a=a,
                        b=b,
                        max_error=max_err,
                        start_pos=left,
                        end_pos=right - 1,
                    )
                    right += 1
                else:
                    break

            if best_seg is None:
                a, b = self._linear_fit([keys[left]], [left])
                best_seg = FitingSegment(
                    start_key=keys[left],
                    end_key=keys[left],
                    a=a,
                    b=b,
                    max_error=0.0,
                    start_pos=left,
                    end_pos=left,
                )
                best_right = left + 1

            segments.append(best_seg)
            left = best_right

        return segments

    def _find_segment(self, key):
        if not self.segments:
            return None

        for seg in self.segments:
            if seg.start_key <= key <= seg.end_key:
                return seg

        if key < self.segments[0].start_key:
            return self.segments[0]

        return self.segments[-1]

    def predict_position(self, key):
        seg = self._find_segment(key)
        if seg is None:
            return None, -1

        pos = int(seg.a * key + seg.b)
        return seg, pos

    def point_lookup(self, key):
        seg, pred = self.predict_position(key)
        if seg is None:
            return -1

        lo = max(seg.start_pos, pred - math.ceil(seg.max_error))
        hi = min(seg.end_pos, pred + math.ceil(seg.max_error))

        window = self.keys[lo:hi + 1]
        idx = bisect.bisect_left(window, key)
        real_pos = lo + idx

        if real_pos <= hi and real_pos < len(self.keys) and self.keys[real_pos] == key:
            return real_pos
        return -1

    def range_lookup(self, left_key, right_key):
        if not self.keys:
            return []

        seg_l, pred_l = self.predict_position(left_key)
        seg_r, pred_r = self.predict_position(right_key)

        if seg_l is None or seg_r is None:
            return []

        left_pos = max(seg_l.start_pos, pred_l - math.ceil(seg_l.max_error))
        right_pos = min(seg_r.end_pos, pred_r + math.ceil(seg_r.max_error))

        left_pos = max(0, left_pos)
        right_pos = min(len(self.keys) - 1, right_pos)

        result = []
        for i in range(left_pos, right_pos + 1):
            if left_key <= self.keys[i] <= right_key:
                result.append(i)

        return result

    def model_size_estimate(self):
        # 粗略估计：每段约 40 字节
        return len(self.segments) * 40.0
    

if __name__ == "__main__":
    keys = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    tree = SimpleFitingTree(error_threshold=2)
    tree.fit(keys)

    print("=== Segments ===")
    for seg in tree.segments:
        print(seg)

    print("\n=== Point Lookup ===")
    for key in [1, 5, 21, 100]:
        pos = tree.point_lookup(key)
        print(f"key={key}, pos={pos}")

    print("\n=== Range Lookup ===")
    result = tree.range_lookup(5, 34)
    print(result)

    print("\n=== Model Size Estimate ===")
    print(tree.model_size_estimate())