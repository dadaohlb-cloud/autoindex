系统当前状态可以概括为：
已有完整主链路：dataset_builder -> train -> infer -> greedy -> deploy
已支持两类索引：btree 和 fiting

fiting 目前是：
单列
单谓词
外部构建 + 成本模型评估

# 先扩 workload，再搭实验系统

## 拆 workload
把你现在的 workload/tpch.sql 拆成3套：
train_workload.sql：给 dataset_builder 和 train
test_workload.sql：给 infer / greedy
benchmark_workload.sql：给最终性能测试

## 写 workload 生成器
自动生成比现在更多的 SQL，而不是手工写。

## 改 pipeline
让它支持“训练 workload”和“测试 workload”分开。
现在的 workload/tpch.sql 只有 12 条左右，而且模板太少：
l_orderkey = ?
o_custkey = ? ORDER BY o_orderdate
l_quantity > ? GROUP BY/ORDER BY
一个 join

这对“跑通系统”够了，但对论文实验不够。
因为现在模型学到的是“这几种固定模板”的规律，不是真正的 workload 分布。

## workload 规模
训练 workload
目标：120 条 SQL
4 类：
点查：40 条
范围查：30 条
排序/聚合查：30 条
join 查：20 条

测试 workload
目标：30 条 SQL
和训练模板相似，但常量不同。

benchmark workload
目标：20 条 SQL
专门用来做最终实验表格和响应时间对比。

# 扩 workload

mkdir -p scripts workload/generated output/benchmarks output/figures
touch scripts/gen_workload.py
* scripts/gen_workload.py

python scripts/gen_workload.py

[INFO] train queries: 120
[INFO] test queries: 30
[INFO] benchmark queries: 12
[INFO] workload files generated:
 - workload/train_workload.sql
 - workload/test_workload.sql
 - workload/benchmark_workload.sql


# 改 run_pipeline.py

当前 run_pipeline.py 的问题是：
训练和推理都用 workload/tpch.sql
它适合 demo，不适合论文实验
先改成“训练 workload”和“测试 workload”分开。
改run_pipeline.py


train_workload.sql 用来构建训练样本
test_workload.sql 用来模拟未见 workload 的推荐
benchmark_workload.sql 留给后面做真实性能测试


按顺序执行：

python scripts/gen_workload.py
python run_pipeline.py

然后把下面这几项贴给我：

wc -l workload/train_workload.sql
wc -l workload/test_workload.sql
wc -l workload/benchmark_workload.sql
cat output/selected_indexes.csv
cat output/fiting_manifest.json

