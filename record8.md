# 搭 benchmark 性能测试系统

这一阶段要解决的问题是：
推荐出来的索引，到底能不能让查询真的更快？
也就是做论文里最核心的对比实验：
No Index
BTree Only
Hybrid Method

## touch scripts/benchmark_queries.py
两组实验：
1）No Index
删除所有 idx_% 索引
测 benchmark_workload.sql
输出：
output/benchmarks/benchmark_no_index.csv
2）Hybrid Recommended
删除旧索引
执行 output/recommended_indexes.sql
再测同一批 benchmark 查询
输出：
output/benchmarks/benchmark_hybrid.csv


执行：
python -m scripts.benchmark_queries
[INFO] Dropped indexes for No Index baseline: []
[INFO] Benchmark saved to: output/benchmarks/benchmark_no_index.csv
   sql_id                                           sql_text  exec_time
8       9  SELECT l_partkey, COUNT(*) FROM lineitem WHERE...   0.813051
9      10  SELECT * FROM customer c JOIN orders o ON c.c_...   0.109088
10     11  SELECT * FROM customer c JOIN orders o ON c.c_...   0.107851
11     12  SELECT * FROM customer c JOIN orders o ON c.c_...   0.104851
12  TOTAL                                                      3.993889
[INFO] Applied recommended SQL: output/recommended_indexes.sql
[INFO] Benchmark saved to: output/benchmarks/benchmark_hybrid.csv
   sql_id                                           sql_text  exec_time
8       9  SELECT l_partkey, COUNT(*) FROM lineitem WHERE...   0.831575
9      10  SELECT * FROM customer c JOIN orders o ON c.c_...   0.109707
10     11  SELECT * FROM customer c JOIN orders o ON c.c_...   0.111969
11     12  SELECT * FROM customer c JOIN orders o ON c.c_...   0.109617
12  TOTAL                                                      3.330814
[INFO] Benchmark suite finished.
跑完后看：
cat output/benchmarks/benchmark_no_index.csv
cat output/benchmarks/benchmark_hybrid.csv

## 做一个自动汇总脚本
新建：
touch scripts/export_results.py

python -m scripts.export_results
cat output/benchmarks/benchmark_summary.csv
最核心的一张表了

[INFO] Summary saved to: output/benchmarks/benchmark_summary.csv
     method  total_exec_time  speedup_vs_no_index  reduction_ratio
0  No Index         3.993889             1.000000         0.000000
1    Hybrid         3.330814             1.199073         0.166022

## 加入 BTree-only 对照组

学习型索引到底有没有额外价值，还是仅靠传统 B+Tree 就够了？

一、为什么现在 Hybrid 提升不算特别大
1）benchmark workload 里 join 查询占比不低
而你当前系统对 join 查询主要还是靠 btree，fiting 基本不参与。
2）当前推荐结果里 fiting 还是“外部清单”，没有真正进 PostgreSQL 执行路径
所以 benchmark 时实际生效的只有 recommended_indexes.sql 里的 B+Tree 部分。
3）你的 benchmark workload 中，l_quantity > ? GROUP BY/ORDER BY 这类查询很重
这类查询主要依赖复合 B+Tree，而不是 fiting。
4）当前 benchmark 只测了 12 条查询
样本还不够大，波动会比较明显。
所以现在的 16.6% 并不说明系统弱，而是说明：
B+Tree 部署链路已经能带来稳定收益
Hybrid 理念已经在推荐层成立
但要证明 fiting 的增益，还需要一个更合适的对照设计


在完全相同 workload、完全相同预算下比较：
BTree-only selector
Hybrid selector


## 在 candidate/generator.py 里加一个开关

改 candidate/generator.py
改 generate_single_column_candidates
改 generate_composite_candidates
改 generate_all_candidates

## 让 infer.py 支持 BTree-only / Hybrid 两种模式
## scripts/run_btree_only.py

touch scripts/run_btree_only.py

## 改 benchmark，让它支持多方案

把 scripts/benchmark_queries.py 里 run_benchmark_suite() 扩展成支持：
No Index
BTree-only
Hybrid

## 改 scripts/export_results.py
把它扩成三组汇总。


执行
python -m scripts.run_btree_only
python -m scripts.benchmark_queries
python -m scripts.export_results
cat output/benchmarks/benchmark_summary.csv
九、你下一步会得到什么

如果一切正常，你的论文核心结果表会变成这样：

method	total_exec_time	speedup_vs_no_index	reduction_ratio
No Index	3.99	1.00	0.00
BTree Only	x.xx	x.xx	x.xx
Hybrid	3.33	1.20	0.166

然后你就可以真正回答论文的核心问题：

和无索引比，自动推荐方法能不能提速？

和只选 B+Tree 比，混合索引值不值得？

这一步做完，你的实验体系就基本成型了。

把 benchmark_summary.csv 的新内容贴给我，我下一条继续带你做最后一步：自动生成论文图表。