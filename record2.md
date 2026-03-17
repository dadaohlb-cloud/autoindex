# sql解析
第一步：安装 SQL 解析库
在虚拟环境里执行：
pip install sqlglot
检查：
python -c "import sqlglot; print(sqlglot.__version__)"
第二步：创建目录和文件
在项目目录下执行：
mkdir -p parser workload
touch parser/sql_parser.py
touch workload/tpch.sql
parser/sql_parser.py parser/load_workload.py
(其中 select_cols 现在是“语句中出现过的列”，不完全等于真正投影列，但当前阶段完全够用。不要继续抠解析细节，直接进入 候选索引生成模块。)

# 索引候选集合
目标：从 workload 的解析结果里统计高频列，然后生成：
单列候选索引
复合候选索引
最大宽度先设为 3
（先只做 B+Tree 候选，不要碰 FITing-Tree）
第一步：建文件
执行：
mkdir -p candidate
touch candidate/generator.py
第二步：直接写 candidate/generator.py

# 特征模块
先建目录和文件
mkdir -p feature
touch feature/query_feat.py
touch feature/index_feat.py
touch feature/interaction_feat.py

先做查询特征 feature/query_feat.py
这个模块负责把一条查询转成数值特征。

再做索引特征 feature/index_feat.py
(当前阶段先不查 PostgreSQL 统计信息，先做一个最小版)
(这里保留了 i_type_fiting，虽然现在还没做 FITing-Tree，但以后直接扩展方便。)

交互特征 feature/interaction_feat.py
这个最重要，因为它描述“这个索引对这条查询有没有帮助”。

再做一个统一拼接函数
新建：touch feature/merge_feat.py


