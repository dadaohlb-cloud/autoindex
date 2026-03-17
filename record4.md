# 贪心选择 输出 CREATE INDEX
一、先建文件
mkdir -p selector deploy
touch selector/greedy.py
touch deploy/create_index.py
二、selector/greedy.py
这个模块：
汇总同一个候选索引在 workload 上的总收益
在存储预算下做贪心选择
三、运行贪心选择
执行：
python selector/greedy.py

[INFO] 已选择索引保存到: output/selected_indexes.csv
[INFO] 已使用存储预算: 40.00 / 40.00
  index_type             index_cols  total_benefit  avg_benefit  storage_est
0      btree  o_custkey|o_orderdate       1.421662     0.355415         16.0
1      btree             l_orderkey       1.327407     0.331852         12.0
2      btree              o_custkey       1.323597     0.330899         12.0
...
四、deploy/create_index.py
把选中的索引输出成 SQL 文件。

五、运行 SQL 导出
python deploy/create_index.py
然后看输出文件：
cat output/recommended_indexes.sql

CREATE INDEX IF NOT EXISTS idx_orders_o_custkey_o_orderdate ON orders (o_custkey, o_orderdate);
CREATE INDEX IF NOT EXISTS idx_lineitem_l_orderkey ON lineitem (l_orderkey);
CREATE INDEX IF NOT EXISTS idx_orders_o_custkey ON orders (o_custkey);
六、直接部署到数据库手动执行：

psql -h 127.0.0.1 -U postgres -d tpch -f output/recommended_indexes.sql
输入密码后，推荐索引建进去。

然后你可以再测试 workload 查询时间。


完整闭环：
读取 workload
SQL 解析
候选索引生成
特征构造
构造训练集
训练 MLP
预测索引收益
汇总收益
贪心选择
输出建索引 SQL
这已经是一个可运行的自动索引推荐原型，和你专利的主链路是一致的：S1–S6 分别覆盖查询特征提取、候选索引生成、查询-索引组合特征构造、索引效益预测、约束下索引选择以及最终索引部署。
