SELECT * FROM lineitem WHERE l_orderkey = 20000;
SELECT * FROM lineitem WHERE l_partkey = 1000;
SELECT * FROM orders WHERE o_custkey = 500;
SELECT * FROM customer WHERE c_custkey = 100;

SELECT * FROM lineitem WHERE l_quantity > 20;
SELECT * FROM orders WHERE o_totalprice > 100000;
SELECT * FROM orders WHERE o_orderdate >= DATE '1995-01-01';

SELECT * FROM orders
WHERE o_custkey = 500 AND o_orderdate >= DATE '1995-01-01';

SELECT * FROM lineitem
WHERE l_partkey = 1000 AND l_quantity > 20;

SELECT *
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
WHERE c.c_custkey = 100;

SELECT *
FROM orders
WHERE o_custkey = 500
ORDER BY o_orderdate;

SELECT l_partkey, COUNT(*)
FROM lineitem
WHERE l_quantity > 20
GROUP BY l_partkey;