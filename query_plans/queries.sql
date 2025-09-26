-- =================================================================
-- Query ID: U1
-- Description: A UNION ALL query combining top parts by available quantity
-- with top suppliers by available quantity.
-- =================================================================
(SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100
Order By p_partkey Limit 5)
UNION ALL (SELECT s_suppkey, s_name FROM supplier, partsupp where s_suppkey = ps_suppkey and
ps_availqty > 200 Order By s_suppkey Limit 7);