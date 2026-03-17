# 比较的是 Python 内部 bisect 的微秒级时间,btree_point 和 fiting_point 都太快了,时间差已经小到被计时噪声淹没了,所以 label 会塌成 0。把 fiting 标签从“单次微秒计时”改成“可学习标签”
(venv) ddhlb@ddhlb-Lenovo-Legion-Y7000-2019-1050:~/auto_index$ python -m model.dataset_builder

[INFO] Processing Query 1
SELECT * FROM lineitem WHERE l_orderkey = 10
[DEBUG] baseline start for Query 1
[DEBUG] baseline end for Query 1
[INFO] Baseline time: 0.167581s
[INFO] Candidate 1: table=lineitem, type=btree, cols=('l_orderkey',) | t_idx=0.000077s | label=0.999544
[DEBUG] FITING start: table=lineitem, col=l_orderkey
[DEBUG] fetched values: 10000
[DEBUG] built fiting segments: 9
[DEBUG] fiting done: t_btree=0.00000057, label=0.000000
[INFO] Candidate 2: table=lineitem, type=fiting, cols=('l_orderkey',) | label=0.000000

[INFO] Processing Query 2
SELECT * FROM orders WHERE o_custkey = 100 ORDER BY o_orderdate
[DEBUG] baseline start for Query 2
[DEBUG] baseline end for Query 2
[INFO] Baseline time: 0.053443s
[INFO] Candidate 1: table=orders, type=btree, cols=('o_custkey',) | t_idx=0.000159s | label=0.997034
[DEBUG] FITING start: table=orders, col=o_custkey
[DEBUG] fetched values: 10000
[DEBUG] built fiting segments: 8
[DEBUG] fiting done: t_btree=0.00000045, label=0.000000
[INFO] Candidate 2: table=orders, type=fiting, cols=('o_custkey',) | label=0.000000

[INFO] Processing Query 3
SELECT * 
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
WHERE c.c_nationkey = 3
[DEBUG] baseline start for Query 3
[DEBUG] baseline end for Query 3
[INFO] Baseline time: 0.126496s
[INFO] Candidate 1: table=customer, type=btree, cols=('c_custkey',) | t_idx=0.111474s | label=0.118751
[WARN] Candidate 2 fiting 暂不支持该查询，跳过: table=customer, candidate={'index_type': 'fiting', 'columns': ('c_custkey',), 'width': 1}

[INFO] Processing Query 4
SELECT l_partkey, COUNT(*) 
FROM lineitem
WHERE l_quantity > 10
GROUP BY l_partkey
ORDER BY l_partkey
[DEBUG] baseline start for Query 4
[DEBUG] baseline end for Query 4
[INFO] Baseline time: 1.221699s
[INFO] Candidate 1: table=lineitem, type=btree, cols=('l_partkey',) | t_idx=1.149413s | label=0.059168
[WARN] Candidate 2 fiting 暂不支持该查询，跳过: table=lineitem, candidate={'index_type': 'fiting', 'columns': ('l_partkey',), 'width': 1}

[INFO] 数据集已保存到: output/train.csv
[INFO] 样本数: 6
   sql_id  candidate_id table_name  ... x_covering x_predicate_col_cnt x_index_col_cnt
0       1             1   lineitem  ...          1                   1               1
1       1             2   lineitem  ...          1                   1               1
2       2             1     orders  ...          0                   1               1
3       2             2     orders  ...          0                   1               1
4       3             1   customer  ...          0                   3               1

[5 rows x 32 columns]
(venv) ddhlb@ddhlb-Lenovo-Legion-Y7000-2019-1050:~/auto_index$ ^C
(venv) ddhlb@ddhlb-Lenovo-Legion-Y7000-2019-1050:~/auto_index$ python -c "import pandas as pd; df=pd.read_csv('output/train.csv'); print(df[['index_type','index_cols','label']]); print(df['index_type'].value_counts())"
  index_type  index_cols     label
0      btree  l_orderkey  0.999544
1     fiting  l_orderkey  0.000000
2      btree   o_custkey  0.997034
3     fiting   o_custkey  0.000000
4      btree   c_custkey  0.118751
5      btree   l_partkey  0.059168
index_type
btree     4
fiting    2
Name: count, dtype: int64
(venv) ddhlb@ddhlb-Lenovo-Legion-Y7000-2019-1050:~/auto_index$ 

# 统一评估索引效益”，不一定非要是 CPU 墙钟时间。专利里强调的是效益评估与预测，而不是必须完全依赖数据库真实执行。
对于单列点查：
btree_cost = log2(N)
fiting_cost = 1 + log2(error_window + 1)
对于范围查：
btree_cost = log2(N) + result_count
fiting_cost = 1 + error_window + result_count
然后：
label = (btree_cost - fiting_cost) / btree_cost
这样：
learned index 如果窗口小，就会有明显正收益
信号稳定，不会塌成 0
非常适合训练


# 后面要再把 fiting.model_size_estimate() 真接进来。

第一步
重新生成训练集：
rm -f output/train.csv
python -m model.dataset_builder
然后看 fiting 的 label 是否不再全 0。
执行：
python -c "import pandas as pd; df=pd.read_csv('output/train.csv'); print(df[['index_type','index_cols','label']]); print(df['index_type'].value_counts())"
第二步
如果 fiting 标签正常了，就重新训练：
python -m model.train
第三步
再推理：
python -m model.infer
python selector/greedy.py
python deploy/create_index.py
然后看看：
predictions.csv
selected_indexes.csv
里有没有 fiting


# 候选索引生成器必须“按表生成”

也就是：

customer 表的列，只能和 customer 表的列组合

orders 表的列，只能和 orders 表的列组合

不能把 c_custkey 和 o_custkey 放进同一个 B+Tree 复合索引

# 部署模块不能再靠列名前缀瞎猜表名

现在 deploy/create_index.py 的 guess_table_name() 对单表单列还凑合，对多表复合就一定会错。


第一步：让 SQL 解析器返回“列 -> 表”映射
你现在的 sql_parser.py 已经能抽列，但没有保留表别名和真实表名关系。现在补上。parser/sql_parser.py 

第二步：让候选生成器“按表内组合” candidate/generator.py。

核心思想：
单列候选照旧
复合候选不再直接把 where/join/order 全部混在一起
而是先按 column_to_tables 分组到各表，再在每个表内部组合
同时把单列候选也带上 table_name：

第三步：让部署模块直接使用候选里的 table_name
现在不该再猜表名了。

第四步：让 selector/greedy.py 保留 table_name
现在 aggregate_predictions() 只按：index_type index_cols 聚合
这不够，因为同名列可能出现在不同表里。
同时 selected.append() 里也保留 table_name

第五步：让 infer.py 也输出 table_name
因为现在候选里已经有 table_name 了，infer.py 里不要再写：
"table_name": "unknown",
改成：
"table_name": candidate.get("table_name", "unknown"),

非法跨表复合索引会消失
selected_indexes.csv 里的每条索引都带真实表名
recommended_indexes.sql 会变成可执行 SQL
fiting 候选也会被正确保留下来