-- =================================================================
-- Query ID: Q1
-- Description: Standard query Q1 (pricing summary).
-- =================================================================
SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate < '1998-11-29' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

-- =================================================================
-- Query ID: Q2
-- Description: Standard query Q2 (minimum cost supplier-like).
-- =================================================================
WITH min_supplycost AS ( SELECT ps_partkey, MIN(ps_supplycost) AS min_cost FROM partsupp JOIN supplier ON s_suppkey = ps_suppkey JOIN nation ON s_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey WHERE r_name = 'EUROPE' GROUP BY ps_partkey ) SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part JOIN partsupp ON p_partkey = ps_partkey JOIN supplier ON s_suppkey = ps_suppkey JOIN nation ON s_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey JOIN min_supplycost ON part.p_partkey = min_supplycost.ps_partkey AND partsupp.ps_supplycost = min_supplycost.min_cost WHERE p_size = 15 AND p_type LIKE '%BRASS' AND r_name = 'EUROPE' ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;

-- =================================================================
-- Query ID: Q3
-- Description: Standard query Q3 (shipping priority revenue).
-- =================================================================
with customer_orders_cte as ( select o_orderkey, o_orderdate, o_shippriority from customer, orders where c_mktsegment = 'FURNITURE' and c_custkey = o_custkey and o_orderdate < date '1995-01-01' ), lineitem_revenue_cte as ( select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue from lineitem where l_shipdate > date '1995-01-01' group by l_orderkey ) select co.o_orderkey, lr.revenue, co.o_orderdate, co.o_shippriority from customer_orders_cte co join lineitem_revenue_cte lr on co.o_orderkey = lr.l_orderkey order by lr.revenue desc, co.o_orderdate;

-- =================================================================
-- Query ID: Q4
-- Description: Standard query Q4 (order priority counting).
-- =================================================================
select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '3' month and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority ;

-- =================================================================
-- Query ID: Q5
-- Description: Standard query Q5 (local supplier volume-like).
-- =================================================================
with filtered_orders as ( select * from orders where o_orderkey in ( select l_orderkey from lineitem ) ) select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, filtered_orders as orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date '1995-01-01' and o_orderdate < date '1995-01-01' + interval '1' year group by n_name order by revenue desc;

-- =================================================================
-- Query ID: Q6
-- Description: Standard query Q6 (discounted revenue variant).
-- =================================================================
select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date '1993-01-01' and l_shipdate < date '1994-03-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 10 ;

-- =================================================================
-- Query ID: Q7
-- Description: Standard query Q7 (trade volume between two nations).
-- =================================================================
select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') or (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') ) and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year ;

-- =================================================================
-- Query ID: Q8
-- Description: Standard query Q8 (market share by year).
-- =================================================================
select o_year, sum(case when nation = 'INDIA' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'ASIA' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year ;

-- =================================================================
-- Query ID: Q9
-- Description: Standard query Q9 (profit by nation and year).
-- =================================================================
SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit FROM part JOIN partsupp ON p_partkey = ps_partkey JOIN lineitem ON l_partkey = p_partkey AND l_suppkey = ps_suppkey JOIN supplier ON s_suppkey = l_suppkey JOIN orders ON o_orderkey = l_orderkey JOIN nation ON s_nationkey = n_nationkey WHERE p_name LIKE 'co%' GROUP BY n_name, o_year ORDER BY n_name, o_year DESC;

-- =================================================================
-- Query ID: Q10
-- Description: Standard query Q10 (returned item reporting).
-- =================================================================
SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, c.c_acctbal, n.n_name, c.c_address, c.c_phone, c.c_comment FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON l.l_orderkey = o.o_orderkey JOIN nation n ON c.c_nationkey = n.n_nationkey WHERE o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-03-31' AND l.l_returnflag = 'R' GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment ORDER BY revenue DESC;

-- =================================================================
-- Query ID: Q11
-- Description: Standard query Q11 (important stock identification).
-- =================================================================
SELECT ps_partkey, n_name, SUM(ps_supplycost * ps_availqty) AS total_value FROM partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'INDIA' GROUP BY ps_partkey, n_name HAVING SUM(ps_supplycost * ps_availqty) > ( SELECT SUM(ps_supplycost * ps_availqty) * 0.00001 FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'INDIA' ) ORDER BY total_value DESC ;

-- =================================================================
-- Query ID: Q12
-- Description: Standard query Q12 (shipping modes and order priority analysis).
-- =================================================================
SELECT l_shipmode, SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count FROM orders JOIN lineitem ON o_orderkey = l_orderkey WHERE l_shipmode = 'SHIP' AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= DATE '1995-01-01' AND l_receiptdate < DATE '1996-01-01' GROUP BY l_shipmode ORDER BY l_shipmode;

-- =================================================================
-- Query ID: Q13
-- Description: Standard query Q13 (customer distribution by order count).
-- =================================================================
select c_count, c_orderdate, count(*) as custdist from ( select c_custkey, o_orderdate, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey, o_orderdate ) as c_orders (c_custkey, c_count, c_orderdate) group by c_count, c_orderdate order by custdist desc, c_count desc ;

-- =================================================================
-- Query ID: Q14
-- Description: Standard query Q14 (promotion effect calculation).
-- =================================================================
select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= date '1995-01-01' and l_shipdate < date '1995-01-01' + interval '1' month ;

-- =================================================================
-- Query ID: Q15
-- Description: Standard query Q15 (top supplier revenue using CTE).
-- =================================================================
SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, r.total_revenue FROM ( SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue, MAX(SUM(l_extendedprice * (1 - l_discount))) OVER () AS max_revenue FROM lineitem WHERE l_shipdate >= DATE '1995-01-01' AND l_shipdate < DATE '1995-01-01' + INTERVAL '3' MONTH GROUP BY l_suppkey ) r JOIN supplier s ON s.s_suppkey = r.supplier_no WHERE r.total_revenue = r.max_revenue ORDER BY s.s_suppkey;

-- =================================================================
-- Query ID: Q16
-- Description: Standard query Q16 (parts/supplier relationship stats).
-- =================================================================
SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp JOIN part ON p_partkey = ps_partkey LEFT JOIN supplier ON ps_suppkey = s_suppkey AND s_comment LIKE '%Customer%Complaints%' WHERE p_brand <> 'Brand#23' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (1, 4, 7) AND s_suppkey IS NULL GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;

-- =================================================================
-- Query ID: Q17
-- Description: Standard query Q17 (average yearly revenue for small-quantity orders).
-- =================================================================
select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#53' and p_container = 'MED BAG' and l_quantity < ( select 0.7 * avg(l_quantity) from lineitem where l_partkey = p_partkey ) ;

-- =================================================================
-- Query ID: Q18
-- Description: Standard query Q18 (customers with large order quantities).
-- =================================================================
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate ;

-- =================================================================
-- Query ID: Q19
-- Description: Standard query Q19 (discounted revenue across brands and containers).
-- =================================================================
select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) ;

-- =================================================================
-- Query ID: Q20
-- Description: Standard query Q20 (suppliers for ivory parts in France with availability filter).
-- =================================================================
with filtered_supplier as ( select * from supplier where s_nationkey in ( select n_nationkey from nation where n_name = 'FRANCE' ) ) select s_name, s_address from filtered_supplier as supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like '%ivory%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1995-01-01' and l_shipdate < date '1995-01-01' + interval '1' year ) ) and s_nationkey = n_nationkey and n_name = 'FRANCE' order by s_name; 

-- =================================================================
-- Query ID: Q21
-- Description: Standard query Q21 (suppliers who kept orders waiting in Argentina).
-- =================================================================
select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'ARGENTINA' group by s_name order by numwait desc, s_name ;

-- =================================================================
-- Query ID: Q22
-- Description: Standard query Q22 (country code customer/account balance aggregation).
-- =================================================================
with filtered_customer as ( select * from customer where substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17') ) select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from filtered_customer where c_acctbal > ( select avg(c_acctbal) from filtered_customer where c_acctbal > 0.00 ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode;
