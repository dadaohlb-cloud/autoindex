# 修改 dataset_builder.py，让 fiting 真正参与训练样本生成
之前dataset_builder.py 对 btree 的逻辑是：对 fiting 不能直接用
建索引
跑 SQL
计时
算 label
改
对 fiting：
走外部 learned index 评估：
取出目标列数据
排序
构建 SimpleFitingTree
从 SQL 里抽出单列点查或范围查条件
用 fiting 做查询时间估计
用同样数据上的 bisect 模拟 btree 查询时间
得到 learned index 的相对收益 label

新增一个谓词解析器touch parser/predicate_parser.py
新增一个learned evaluator touch learned_index/evaluator.py
改 dataset_builder.py
新增 import
新增取列数据的函数fetch_column_values
新增learned label 计算函数 compute_fiting_label

修改打印日志
（learned index目前只支持：单列 数值列 单谓词，所以 fiting 很多候选会被后面 compute_fiting_label() 自然跳过，这没关系。）
feature/index_feat.py 里估计存储开销时，fiting （还没真正体现模型体积）


第一步：先删旧训练集
rm -f output/train.csv
第二步：重新生成训练集
python -m model.dataset_builder
你应该开始看到两类样本：

btree 样本

类似现在这样：

[INFO] Candidate 1: table=lineitem, type=btree, cols=('l_orderkey',) | t_idx=... | label=...
fiting 样本

比如 Query 1：

[INFO] Candidate X: table=lineitem, type=fiting, cols=('l_orderkey',) | label=...

特别是这两条查询最容易成功：

SELECT * FROM lineitem WHERE l_orderkey = 10

SELECT * FROM lineitem WHERE l_quantity > 10

因为它们正好都是：

单列

数值列

单谓词

成功后的检查方法

生成完后执行：

python -c "import pandas as pd; df=pd.read_csv('output/train.csv'); print(df[['index_type','index_cols','label']].head(20)); print(df['index_type'].value_counts())"

你希望看到类似：

btree     8
fiting    3

只要 train.csv 里真的出现了 fiting，这一步就成功了。

这一步的意义

这一步完成后，你的训练数据就不再只有“传统索引”，而是真正变成：

btree

fiting

混合训练样本。

这时你的 MLP 才有可能学会：

对点查/范围查，什么时候 learned index 更优

对排序/覆盖/多列组合，什么时候 btree 更优

这正是你论文真正的核心。



tpch=# SELECT pid, state, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE datname = 'tpch';

SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY tablename, indexname;

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'tpch'
  AND pid <> pg_backend_pid();

DROP INDEX IF EXISTS idx_lineitem_6ab4b83a;

SELECT 'DROP INDEX IF EXISTS ' || indexname || ';'
FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname LIKE 'idx_%';