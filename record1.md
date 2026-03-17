# 环境安装

sudo apt install -y python3 python3-pip python3-venv
进入项目目录：
mkdir auto_index
cd auto_index
创建虚拟环境：
python3 -m venv venv
激活：
source venv/bin/activate
看到：
(venv)
安装 Python 依赖
pip install psycopg2-binary pandas numpy sqlglot scikit-learn torch tqdm xgboost
验证：
python
import torch
import psycopg2
import sqlglot

切换 postgres 用户：
sudo -u postgres psql
创建数据库：
CREATE DATABASE tpch;
创建用户（可选）：
CREATE USER dbuser WITH PASSWORD '123456';
ALTER ROLE dbuser SUPERUSER;
退出：
\q

安装 TPC-H 数据集
下载：
cd ~
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
编译：
make
生成 1GB 数据：
./dbgen -s 1
会生成：
customer.tbl
lineitem.tbl
orders.tbl
part.tbl
partsupp.tbl
supplier.tbl
nation.tbl
region.tbl

创建数据库表

进入数据库：

sudo -u postgres psql tpch

创建表：
CREATE TABLE region (
    r_regionkey int,
    r_name char(25),
    r_comment varchar(152)
);
CREATE TABLE nation (
    n_nationkey int,
    n_name char(25),
    n_regionkey int,
    n_comment varchar(152)
);
CREATE TABLE supplier (
    s_suppkey int,
    s_name char(25),
    s_address varchar(40),
    s_nationkey int,
    s_phone char(15),
    s_acctbal numeric,
    s_comment varchar(101)
);
CREATE TABLE customer (
    c_custkey int,
    c_name varchar(25),
    c_address varchar(40),
    c_nationkey int,
    c_phone char(15),
    c_acctbal numeric,
    c_mktsegment char(10),
    c_comment varchar(117)
);
CREATE TABLE orders (
    o_orderkey int,
    o_custkey int,
    o_orderstatus char(1),
    o_totalprice numeric,
    o_orderdate date,
    o_orderpriority char(15),
    o_clerk char(15),
    o_shippriority int,
    o_comment varchar(79)
);
CREATE TABLE lineitem (
    l_orderkey int,
    l_partkey int,
    l_suppkey int,
    l_linenumber int,
    l_quantity numeric,
    l_extendedprice numeric,
    l_discount numeric,
    l_tax numeric,
    l_returnflag char(1),
    l_linestatus char(1),
    l_shipdate date,
    l_commitdate date,
    l_receiptdate date,
    l_shipinstruct char(25),
    l_shipmode char(10),
    l_comment varchar(44)
);
CREATE TABLE part (
    p_partkey int,
    p_name varchar(55),
    p_mfgr char(25),
    p_brand char(10),
    p_type varchar(25),
    p_size int,
    p_container char(10),
    p_retailprice numeric,
    p_comment varchar(23)
);
CREATE TABLE partsupp (
    ps_partkey int,
    ps_suppkey int,
    ps_availqty int,
    ps_supplycost numeric,
    ps_comment varchar(199)
);
\q 

先把每个 .tbl 文件 行尾最后一个 | 去掉，再导入。
在 ~/tpch-dbgen 下执行：
sed 's/|$//' region.tbl > region.csv
sed 's/|$//' nation.tbl > nation.csv
sed 's/|$//' supplier.tbl > supplier.csv
sed 's/|$//' customer.tbl > customer.csv
sed 's/|$//' orders.tbl > orders.csv
sed 's/|$//' lineitem.tbl > lineitem.csv
sed 's/|$//' part.tbl > part.csv
sed 's/|$//' partsupp.tbl > partsupp.csv

再导入处理后的文件：
sudo -u postgres psql tpch -c "\copy region from 'region.csv' delimiter '|' csv"
sudo -u postgres psql tpch -c "\copy nation from 'nation.csv' delimiter '|' csv"
sudo -u postgres psql tpch -c "\copy supplier from 'supplier.csv' delimiter '|' csv"
sudo -u postgres psql tpch -c "\copy customer from 'customer.csv' delimiter '|' csv"
sudo -u postgres psql tpch -c "\copy orders from 'orders.csv' delimiter '|' csv"
sudo -u postgres psql tpch -c "\copy lineitem from 'lineitem.csv' delimiter '|' csv"
sudo -u postgres psql tpch -c "\copy part from 'part.csv' delimiter '|' csv"
sudo -u postgres psql tpch -c "\copy partsupp from 'partsupp.csv' delimiter '|' csv"
导入后检查
sudo -u postgres psql tpch
SELECT count(*) FROM region;
SELECT count(*) FROM nation;
SELECT count(*) FROM supplier;
SELECT count(*) FROM customer;
SELECT count(*) FROM orders;
SELECT count(*) FROM lineitem;
TPC-H 1GB 常见行数大致是：
region      5
nation      25
supplier    10000
customer    150000
orders      1500000
lineitem    6001215
part        200000
partsupp    800000

创建系统工程
回到项目目录：
cd ~/auto_index
创建结构：
mkdir parser candidate feature model selector deploy db workload output
touch main.py config.py