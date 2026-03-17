# 训练集构造器 
model/dataset_builder.py
读取 workload
解析 SQL
生成候选索引
构造查询-索引特征
真正建索引、执行查询、计算收益标签，输出 CSV

（最小可运行版，只支持：PostgreSQL B+Tree 少量SQL 少量候选索引 真实执行时间标签）

一、先建文件
执行：
mkdir -p model output db
touch db/pg.py
touch model/dataset_builder.py
二、先写数据库连接模块 db/pg.py
三、直接写 model/dataset_builder.py
python -m model.dataset_builder

# 训练收益预测模型
目标：
读取 output/train.csv
训练一个 MLP 回归模型
保存模型和标准化器
输出训练误差
一、执行：
touch model/mlp.py
touch model/train.py
二、model/mlp.py
三、model/train.py
四、直接训练
python -m model.train

Epoch 001 | train_loss=0.183803 | val_loss=0.238303
Epoch 020 | train_loss=0.114426 | val_loss=0.156923
Epoch 040 | train_loss=0.043829 | val_loss=0.096888
Epoch 060 | train_loss=0.008291 | val_loss=0.054217
Epoch 080 | train_loss=0.002031 | val_loss=0.052743
Epoch 100 | train_loss=0.000795 | val_loss=0.063065
Epoch 120 | train_loss=0.000515 | val_loss=0.067890
Epoch 140 | train_loss=0.000400 | val_loss=0.071691
Epoch 160 | train_loss=0.000338 | val_loss=0.074359
Epoch 180 | train_loss=0.000299 | val_loss=0.076751
Epoch 200 | train_loss=0.000272 | val_loss=0.078771

=== 训练完成 ===
训练集 MAE: 0.011718
验证集 MAE: 0.179231
训练集 MSE: 0.000271
验证集 MSE: 0.078771
模型已保存到: output/benefit_mlp.pt
Scaler 已保存到: output/scaler.pkl
（真正要提升模型效果，主要靠两件事：
增加 workload SQL 数量
增加每条查询可评估的候选索引数量）

# 模型推理 对应专利里 S4-3、S4-4 的“索引效益预测 + 汇总”前半部分。
这个模块要做的事是：
读取训练好的模型
读取 scaler
读取新 workload
生成候选索引
构造特征
预测每个候选索引的收益
输出候选索引预测结果表

一、先建文件
touch model/infer.py
二、model/infer.py
三、运行推理
python -m model.infer

(venv) ddhlb@ddhlb-Lenovo-Legion-Y7000-2019-1050:~/auto_index$ python -m model.infer
[INFO] 预测结果已保存到: output/predictions.csv
    sql_id index_type                       index_cols  pred_benefit
0        1      btree                        c_custkey      0.166335
1        1      btree                      c_nationkey      0.166335
2        1      btree                       l_orderkey      0.972786
3        1      btree                        l_partkey      0.166335
4        1      btree                       l_quantity      0.166335
5        1      btree                        o_custkey      0.166335
6        1      btree                      o_orderdate      0.166335
7        1      btree            o_custkey|o_orderdate      0.279246
8        1      btree            c_nationkey|c_custkey      0.279246
9        1      btree            c_nationkey|o_custkey      0.279246
10       1      btree              c_custkey|o_custkey      0.279246
11       1      btree  c_nationkey|c_custkey|o_custkey      0.158245
12       1      btree             l_quantity|l_partkey      0.279246
13       2      btree                        c_custkey      0.227195
14       2      btree                      c_nationkey      0.227195
15       2      btree                       l_orderkey      0.227195
16       2      btree                        l_partkey      0.227195
17       2      btree                       l_quantity      0.227195
18       2      btree                        o_custkey      0.971423
19       2      btree                      o_orderdate      0.124693

# 做收益汇总 专利里 S4-4 的“频率加权汇总综合效益”

现在 predictions.csv 里是一条条 “查询-索引对” 的预测结果。
但真正推荐索引时，应该把 同一个候选索引在整个 workload 上的收益汇总。
也就是：同一个 index_cols在所有 sql_id 上的 pred_benefit 做累加 / 频率加权

新建：
touch selector/aggregate.py
然后运行：

mkdir -p selector
python selector/aggregate.py