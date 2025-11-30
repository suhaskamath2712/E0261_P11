-- =================================================================
-- Query ID: U1 (Mutated)
-- Semantics: Swap thresholds and reverse entity focus
-- =================================================================
(SELECT s_suppkey, s_name FROM supplier, partsupp WHERE s_suppkey = ps_suppkey AND ps_availqty > 250 ORDER BY s_suppkey LIMIT 4)
UNION ALL
(SELECT p_partkey, p_name FROM part, partsupp WHERE p_partkey = ps_partkey AND ps_availqty <= 100 ORDER BY p_partkey LIMIT 8);

-- =================================================================
-- Query ID: U2 (Mutated)
-- Semantics: Different nation and order priority
-- =================================================================
(SELECT s_suppkey, s_name FROM supplier, nation WHERE s_nationkey = n_nationkey AND n_name = 'FRANCE' ORDER BY s_suppkey DESC, s_name LIMIT 12)
UNION ALL
(SELECT c_custkey, c_name FROM customer, orders WHERE c_custkey = o_custkey AND o_orderpriority = '3-MEDIUM' ORDER BY c_custkey, c_name DESC LIMIT 10);

-- =================================================================
-- Query ID: U3 (Mutated)
-- Semantics: Change country and quantity threshold
-- =================================================================
(SELECT c_custkey AS key, c_name AS name FROM customer, nation WHERE c_nationkey = n_nationkey AND n_name = 'CANADA' ORDER BY key LIMIT 12)
UNION ALL
(SELECT p_partkey AS key, p_name AS name FROM part, lineitem WHERE p_partkey = l_partkey AND l_quantity <= 10 ORDER BY key LIMIT 15)
UNION ALL
(SELECT n_nationkey AS key, r_name AS name FROM nation, region WHERE n_name LIKE 'A%' ORDER BY key LIMIT 6);

-- =================================================================
-- Query ID: U4 (Mutated)
-- Semantics: Alter quantities and regions
-- =================================================================
(SELECT c_custkey, c_name FROM customer, nation WHERE c_nationkey = n_nationkey AND n_name = 'GERMANY' ORDER BY c_custkey ASC LIMIT 7)
UNION ALL
(SELECT s_suppkey, s_name FROM supplier, nation WHERE s_nationkey = n_nationkey AND n_name = 'UNITED STATES' ORDER BY s_suppkey DESC LIMIT 4)
UNION ALL
(SELECT p_partkey, p_name FROM part, lineitem WHERE p_partkey = l_partkey AND l_quantity BETWEEN 5 AND 15 ORDER BY p_partkey LIMIT 9)
UNION ALL
(SELECT ps_partkey, p_name FROM part, partsupp WHERE p_partkey = ps_partkey AND ps_supplycost < 500 ORDER BY ps_partkey LIMIT 10);

-- =================================================================
-- Query ID: U5 (Mutated)
-- Semantics: Different customer pattern and date filters
-- =================================================================
(SELECT o_orderkey, o_orderdate, n_name FROM orders, customer, nation WHERE o_custkey = c_custkey AND c_nationkey = n_nationkey AND c_name LIKE '%0001300%' AND o_orderdate >= '1996-01-01' ORDER BY o_orderkey LIMIT 12)
UNION ALL
(SELECT l_orderkey, l_shipdate, o_orderstatus FROM lineitem, orders WHERE l_orderkey = o_orderkey AND o_orderdate >= '1996-01-01' AND l_quantity <= 10 AND l_extendedprice < 500 ORDER BY l_orderkey LIMIT 8);

-- =================================================================
-- Query ID: U6 (Mutated)
-- Semantics: Different date cutoff and nation name pattern
-- =================================================================
(SELECT o_clerk AS name, SUM(l_extendedprice) AS total_price FROM orders, lineitem WHERE o_orderkey = l_orderkey AND o_orderdate > '1996-01-01' GROUP BY o_clerk ORDER BY total_price ASC LIMIT 10)
UNION ALL
(SELECT n_name AS name, SUM(s_acctbal) AS total_price FROM nation, supplier WHERE n_nationkey = s_nationkey AND n_name NOT LIKE '%UNITED%' GROUP BY n_name ORDER BY n_name ASC LIMIT 10);

-- =================================================================
-- Query ID: U7 (Mutated)
-- Semantics: Swap ranges and price/availability conditions
-- =================================================================
(SELECT l_orderkey AS key, l_extendedprice AS price, l_partkey AS s_key FROM lineitem WHERE l_shipdate >= DATE '1995-01-01' AND l_shipdate < DATE '1996-01-01' AND l_quantity < 20 ORDER BY key DESC LIMIT 15)
UNION ALL
(SELECT ps_partkey AS key, p_retailprice AS price, ps_suppkey AS s_key FROM partsupp, supplier, part WHERE ps_suppkey = s_suppkey AND ps_partkey = p_partkey AND ps_supplycost >= 200 ORDER BY price DESC LIMIT 15);

-- =================================================================
-- Query ID: U8 (Mutated)
-- Semantics: Invert date windows and aggregate direction
-- =================================================================
(SELECT c_custkey AS order_id, COUNT(*) AS total FROM customer, orders WHERE c_custkey = o_custkey AND o_orderdate < '1994-01-01' GROUP BY c_custkey ORDER BY total DESC LIMIT 8)
UNION ALL
(SELECT l_orderkey AS order_id, AVG(l_quantity) AS total FROM orders, lineitem WHERE l_orderkey = o_orderkey AND o_orderdate >= DATE '1997-01-01' GROUP BY l_orderkey ORDER BY total ASC LIMIT 8);

-- =================================================================
-- Query ID: U9 (Mutated)
-- Semantics: Different segment and balance thresholds
-- =================================================================
(SELECT c_name, n_name FROM customer, nation WHERE c_mktsegment = 'HOUSEHOLD' AND c_acctbal < 0 AND c_nationkey = n_nationkey)
UNION ALL
(SELECT s_name, n_name FROM supplier, nation WHERE s_acctbal BETWEEN 1000 AND 3000 AND s_nationkey = n_nationkey);

-- =================================================================
-- Query ID: O1 (Mutated)
-- Semantics: LEFT OUTER JOIN instead of RIGHT and different balance filter
-- =================================================================
SELECT c_name, n_name, COUNT(*) AS total
FROM customer LEFT OUTER JOIN nation ON c_nationkey = n_nationkey AND c_acctbal >= 2000
GROUP BY c_name, n_name
ORDER BY n_name, c_name DESC
LIMIT 5;

-- =================================================================
-- Query ID: O2 (Mutated)
-- Semantics: Different linenumber and quantity threshold
-- =================================================================
SELECT l_shipmode, o_shippriority, COUNT(*) AS high_line_count
FROM lineitem LEFT OUTER JOIN orders ON (l_orderkey = o_orderkey AND o_totalprice < 10000)
WHERE l_linenumber = 2 AND l_quantity >= 40
GROUP BY l_shipmode, o_shippriority
ORDER BY o_shippriority DESC
LIMIT 7;

-- =================================================================
-- Query ID: O3 (Mutated)
-- Semantics: Use INNER JOIN and include only non-finished orders
-- =================================================================
SELECT o_custkey AS customer_key, SUM(c_acctbal) AS balance_sum, o_clerk, c_name
FROM orders INNER JOIN customer ON c_custkey = o_custkey
WHERE o_orderstatus <> 'F'
GROUP BY o_custkey, o_clerk, c_name
ORDER BY balance_sum DESC
LIMIT 20;

-- =================================================================
-- Query ID: O4 (Mutated)
-- Semantics: Alter join directions and constraints
-- =================================================================
SELECT P.p_size, S.s_phone, PS.ps_supplycost, N.n_name
FROM partsupp AS PS
LEFT JOIN part AS P ON P.p_partkey = PS.ps_partkey AND P.p_size <= 5
RIGHT JOIN supplier AS S ON PS.ps_suppkey = S.s_suppkey AND S.s_acctbal >= 5000
LEFT JOIN nation AS N ON S.s_nationkey = N.n_nationkey AND N.n_regionkey <= 2
ORDER BY P.p_size DESC
LIMIT 30;

-- =================================================================
-- Query ID: O5 (Mutated)
-- Semantics: Filter on part attributes instead of availability
-- =================================================================
SELECT ps.ps_suppkey, p.p_name, p.p_type
FROM partsupp AS ps INNER JOIN part AS p ON p.p_partkey = ps.ps_partkey
WHERE p.p_size <= 3 OR p.p_type LIKE 'SMALL%'
ORDER BY p.p_name DESC
LIMIT 60;

-- =================================================================
-- Query ID: O6 (Mutated)
-- Semantics: Different ordering and constraints
-- =================================================================
SELECT pa.p_name, su.s_phone, ps.ps_supplycost, na.n_name
FROM partsupp ps
LEFT OUTER JOIN part pa ON pa.p_partkey = ps.ps_partkey AND pa.p_size < 5
RIGHT OUTER JOIN supplier su ON ps.ps_suppkey = su.s_suppkey AND su.s_acctbal >= 3000
LEFT OUTER JOIN nation na ON su.s_nationkey = na.n_nationkey AND na.n_regionkey <= 1
ORDER BY na.n_name, ps.ps_supplycost ASC, su.s_phone
LIMIT 25;

-- =================================================================
-- Query ID: A1 (Mutated)
-- Semantics: Different date range and price constraints
-- =================================================================
SELECT l_shipmode, COUNT(*) AS cnt
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
  AND l_commitdate >= l_receiptdate
  AND l_shipdate >= l_commitdate
  AND l_receiptdate BETWEEN '1996-01-01' AND '1996-12-31'
  AND l_extendedprice > o_totalprice
  AND o_totalprice <= 20000
GROUP BY l_shipmode
ORDER BY cnt DESC;

-- =================================================================
-- Query ID: A2 (Mutated)
-- Semantics: Different window and commit/receipt relation
-- =================================================================
SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders, lineitem
WHERE l_orderkey = o_orderkey
  AND o_orderdate >= '1994-01-01'
  AND o_orderdate < '1994-04-01'
  AND l_commitdate > l_receiptdate
GROUP BY o_orderpriority
ORDER BY order_count DESC;

-- =================================================================
-- Query ID: A3 (Mutated)
-- Semantics: Loosen availability conditions and change date bounds
-- =================================================================
SELECT l_orderkey, l_linenumber
FROM orders, lineitem, partsupp
WHERE o_orderkey = l_orderkey
  AND ps_partkey = l_partkey
  AND ps_suppkey = l_suppkey
  AND ps_availqty <> l_linenumber
  AND l_shipdate < o_orderdate
  AND o_orderdate < '1990-01-01'
  AND l_commitdate >= l_receiptdate
  AND l_shipdate >= l_commitdate
  AND l_receiptdate <= '1993-12-31'
ORDER BY l_linenumber DESC
LIMIT 9;

-- =================================================================
-- Query ID: A4 (Mutated)
-- Semantics: Include non-finished orders and inverse waiting condition
-- =================================================================
SELECT s_name, COUNT(*) AS numwait
FROM supplier, lineitem, orders, nation
WHERE s_suppkey = l_suppkey
  AND o_orderkey = l_orderkey
  AND o_orderstatus <> 'F'
  AND l_receiptdate < l_commitdate
  AND s_nationkey = n_nationkey
GROUP BY s_name
ORDER BY s_name ASC
LIMIT 50;

-- =================================================================
-- Query ID: N1 (Mutated)
-- Semantics: Different brand exclusion and size constraint
-- =================================================================
SELECT p_brand, p_type, p_size, COUNT(*) AS supplier_cnt
FROM part, partsupp
WHERE p_partkey = ps_partkey
  AND p_size < 4
  AND p_type LIKE 'SMALL PLATED%'
  AND p_brand = 'Brand#45'
GROUP BY p_brand, p_size, p_type
ORDER BY p_brand DESC, p_type DESC, p_size DESC;

-- =================================================================
-- Query ID: F1 (Mutated)
-- Semantics: Reverse series ranges and container type
-- =================================================================
(SELECT l_orderkey, l_extendedprice AS price, p_partkey FROM lineitem, part WHERE l_partkey = p_partkey AND p_container NOT LIKE 'JUMBO%' AND p_partkey BETWEEN 100 AND 500 ORDER BY price ASC LIMIT 50)
UNION ALL
(SELECT o_orderkey, c_acctbal AS price, c_custkey FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey WHERE c_custkey BETWEEN 2000 AND 2010 ORDER BY price ASC, o_orderkey, c_custkey LIMIT 50);

-- =================================================================
-- Query ID: MQ1 (Mutated)
-- Semantics: Different shipment cutoff and remove tax in charge
-- =================================================================
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_charge,
       AVG(l_quantity) AS avg_qty,
       AVG(l_extendedprice) AS avg_price,
       AVG(l_discount) AS avg_disc,
       COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate > DATE '1998-12-01' - INTERVAL '40' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag DESC, l_linestatus DESC;

-- =================================================================
-- Query ID: MQ2 (Mutated)
-- Semantics: Different region and part filter
-- =================================================================
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type LIKE '%STEEL%'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
ORDER BY s_acctbal ASC, n_name, s_name, p_partkey
LIMIT 80;

-- =================================================================
-- Query ID: MQ3 (Mutated)
-- Semantics: Different segment and date directions
-- =================================================================
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'AUTOMOBILE'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= DATE '1996-01-01'
  AND l_shipdate <= DATE '1996-12-31'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY o_shippriority ASC, o_orderdate ASC
LIMIT 12;

-- =================================================================
-- Query ID: MQ4 (Mutated)
-- Semantics: Different quarter
-- =================================================================
SELECT o_orderdate, o_orderpriority, COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= DATE '1994-10-01'
  AND o_orderdate < DATE '1995-01-01'
GROUP BY o_orderdate, o_orderpriority
ORDER BY o_orderpriority DESC
LIMIT 8;

-- =================================================================
-- Query ID: MQ5 (Mutated)
-- Semantics: Different region and time window
-- =================================================================
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND o_orderdate BETWEEN DATE '1996-01-01' AND DATE '1996-12-31'
GROUP BY n_name
ORDER BY n_name ASC
LIMIT 60;

-- =================================================================
-- Query ID: MQ6 (Mutated)
-- Semantics: Different year and add discount lower bound
-- =================================================================
SELECT l_shipmode, SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1993-01-01'
  AND l_shipdate < DATE '1994-01-01'
  AND l_quantity >= 24
  AND l_discount > 0.02
GROUP BY l_shipmode
LIMIT 50;

-- =================================================================
-- Query ID: MQ10 (Mutated)
-- Semantics: Different date window and exclude certain comments
-- =================================================================
SELECT c_name,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate BETWEEN DATE '1995-06-01' AND DATE '1995-09-01'
  AND l_returnflag <> 'R'
  AND c_nationkey = n_nationkey
  AND c_comment NOT LIKE '%pending%'
GROUP BY c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY n_name ASC, revenue ASC
LIMIT 15;

-- =================================================================
-- Query ID: MQ11 (Mutated)
-- Semantics: Different nation and use MAX instead of SUM
-- =================================================================
SELECT ps_comment, MAX(ps_supplycost * ps_availqty) AS peak_value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'BRAZIL'
GROUP BY ps_comment
ORDER BY peak_value ASC
LIMIT 50;

-- =================================================================
-- Query ID: MQ17 (Mutated)
-- Semantics: Different brand/container
-- =================================================================
SELECT AVG(l_extendedprice) AS avg_total
FROM lineitem, part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#11'
  AND p_container = 'SM CASE';

-- =================================================================
-- Query ID: MQ18 (Mutated)
-- Semantics: Different phone prefix and price ordering
-- =================================================================
SELECT c_name, o_orderdate, o_totalprice, SUM(l_quantity)
FROM customer, orders, lineitem
WHERE c_phone LIKE '13-%'
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, o_orderdate, o_totalprice
ORDER BY o_totalprice ASC, o_orderdate DESC
LIMIT 80;

-- =================================================================
-- Query ID: MQ21 (Mutated)
-- Semantics: Different nation and status filter
-- =================================================================
SELECT s_name, COUNT(*) AS numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'P'
  AND s_nationkey = n_nationkey
  AND n_name = 'INDIA'
GROUP BY s_name
ORDER BY numwait ASC, s_name DESC
LIMIT 40;

-- =================================================================
-- Query ID: Alaap (Mutated)
-- Semantics: Change revenue formula and date
-- =================================================================
SELECT c_mktsegment,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= DATE '1996-01-01'
  AND l_extendedprice NOT BETWEEN 212 AND 3000
GROUP BY o_orderdate, o_shippriority, c_mktsegment
ORDER BY o_shippriority DESC, o_orderdate DESC;

-- =================================================================
-- Query ID: TPCH_Q9 (Mutated)
-- Semantics: Different part name prefix and reverse order
-- =================================================================
SELECT nation, o_year, SUM(amount) AS sum_profit
FROM (
  SELECT n_name AS nation,
         EXTRACT(YEAR FROM o_orderdate) AS o_year,
         (l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS amount
  FROM part, supplier, lineitem, partsupp, orders, nation
  WHERE s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE 'a%'
) AS profit
GROUP BY nation, o_year
ORDER BY o_year ASC, nation ASC;

-- =================================================================
-- Query ID: TPCH_Q13 (Mutated)
-- Semantics: Include orders containing a specific phrase and different grouping
-- =================================================================
SELECT c_count, COUNT(*) AS custdist
FROM (
  SELECT c_custkey, COUNT(o_orderkey) AS c_count
  FROM customer LEFT OUTER JOIN orders
    ON c_custkey = o_custkey AND o_comment LIKE '%special%requests%'
  GROUP BY c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY c_count ASC;

-- =================================================================
-- Query ID: ETPCH_Q15 (Mutated)
-- Semantics: Different month and min revenue instead of max
-- =================================================================
WITH revenue (supplier_no, total_revenue) AS (
  SELECT l_suppkey, SUM(l_extendedprice * (1 - l_discount))
  FROM lineitem
  WHERE l_shipdate >= DATE '1995-02-01'
    AND l_shipdate < DATE '1995-02-01' + INTERVAL '1' MONTH
  GROUP BY l_suppkey
)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM supplier, revenue
WHERE s_suppkey = supplier_no
  AND total_revenue = (SELECT MIN(total_revenue) FROM revenue)
ORDER BY s_name;

-- =================================================================
-- Query ID: Nested_Test (Mutated)
-- Semantics: Different country and part name filter
-- =================================================================
SELECT s_name, s_address
FROM supplier, nation, partsupp, part
WHERE s_suppkey = ps_suppkey
  AND ps_partkey = p_partkey
  AND p_name LIKE '%steel%'
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
  AND ps_availqty < (SELECT MAX(c_acctbal) FROM customer)
ORDER BY s_address DESC;

-- =================================================================
-- Query ID: paper_sample (Mutated)
-- Semantics: Swap UNION order and invert price constraints
-- =================================================================
(SELECT s_name AS entity_name, n_name AS country, AVG(l_extendedprice * (1 - l_discount)) AS price
 FROM lineitem, nation, orders, region, supplier
 WHERE l_suppkey = s_suppkey
   AND n_nationkey = s_nationkey
   AND l_orderkey = o_orderkey
   AND n_regionkey = r_regionkey
   AND s_acctbal > o_totalprice
 GROUP BY n_name, s_name
 ORDER BY price ASC, country ASC, entity_name ASC)
UNION ALL
(SELECT c_name AS entity_name, n_name AS country, o_totalprice AS price
 FROM orders JOIN customer ON c_custkey = o_custkey
 JOIN nation ON c_nationkey = n_nationkey
 WHERE o_totalprice >= c_acctbal AND c_acctbal < 9000
 GROUP BY c_name, n_name, o_totalprice
 ORDER BY price DESC, country DESC, entity_name DESC);

-- =================================================================
-- Query ID: ETPCH_Q1 (Mutated)
-- Semantics: Different cutoff and swap averages
-- =================================================================
SELECT l_returnflag AS returnflag, l_linestatus AS linestatus,
       SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       AVG(l_extendedprice) AS avg_qty,
       AVG(l_quantity) AS avg_price,
       AVG(l_discount) AS avg_disc,
       COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-11-15' - INTERVAL '5' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY linestatus, returnflag;

-- =================================================================
-- Query ID: ETPCH_Q3 (Mutated)
-- Semantics: Different segment and date ranges
-- =================================================================
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= DATE '1995-01-01'
  AND l_shipdate <= DATE '1995-01-01'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY o_shippriority DESC
LIMIT 8;

-- =================================================================
-- Query ID: ETPCH_Q4 (Mutated)
-- Semantics: Different order window and distinct removal
-- =================================================================
SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
  AND l_commitdate >= l_receiptdate
  AND o_orderdate BETWEEN '1995-04-01' AND '1995-06-30'
GROUP BY o_orderpriority
ORDER BY o_orderpriority DESC;

-- =================================================================
-- Query ID: ETPCH_Q5 (Mutated)
-- Semantics: Different region
-- =================================================================
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, nation, orders, region, supplier, lineitem
WHERE customer.c_custkey = orders.o_custkey
  AND customer.c_nationkey = nation.n_nationkey
  AND nation.n_nationkey = supplier.s_nationkey
  AND orders.o_orderkey = lineitem.l_orderkey
  AND nation.n_regionkey = region.r_regionkey
  AND supplier.s_suppkey = lineitem.l_suppkey
  AND region.r_name = 'EUROPE'
  AND orders.o_orderdate BETWEEN '1996-01-01' AND '1996-12-31'
GROUP BY n_name
ORDER BY n_name;

-- =================================================================
-- Query ID: ETPCH_Q6 (Mutated)
-- Semantics: Narrow date and quantity
-- =================================================================
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate BETWEEN DATE '1994-01-01' AND DATE '1994-06-30'
  AND l_discount BETWEEN 0.01 AND 0.03
  AND l_quantity >= 24;

-- =================================================================
-- Query ID: ETPCH_Q6_1 (Mutated)
-- Semantics: Reverse date order
-- =================================================================
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate < DATE '1993-01-01'
  AND l_shipdate >= DATE '1992-01-01'
  AND l_quantity >= 24;

-- =================================================================
-- Query ID: ETPCH_Q6_2 (Mutated)
-- Semantics: Remove discount constraint
-- =================================================================
SELECT SUM(l_extendedprice) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1995-01-01'
  AND l_quantity >= 50;

-- =================================================================
-- Query ID: ETPCH_Q7 (Mutated)
-- Semantics: Swap nations in predicate
-- =================================================================
SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM l_shipdate) AS l_year, SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, nation n1, nation n2, orders, supplier, lineitem
WHERE orders.o_orderkey = lineitem.l_orderkey
  AND supplier.s_suppkey = lineitem.l_suppkey
  AND customer.c_custkey = orders.o_custkey
  AND customer.c_nationkey = n1.n_nationkey
  AND n2.n_nationkey = supplier.s_nationkey
  AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'ARGENTINA') OR (n2.n_name = 'FRANCE' AND n1.n_name = 'ARGENTINA'))
  AND lineitem.l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY n1.n_name, n2.n_name, EXTRACT(YEAR FROM l_shipdate)
ORDER BY l_year DESC;

-- =================================================================
-- Query ID: ETPCH_Q9 (Mutated)
-- Semantics: Different part name filter and group ordering
-- =================================================================
SELECT nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, SUM(profit) AS sum_profit
FROM (
  SELECT n_name AS nation, o_orderdate, (l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS profit
  FROM nation, orders, part, partsupp, supplier, lineitem
  WHERE orders.o_orderkey = lineitem.l_orderkey
    AND part.p_partkey = partsupp.ps_partkey
    AND partsupp.ps_partkey = lineitem.l_partkey
    AND partsupp.ps_suppkey = supplier.s_suppkey
    AND supplier.s_suppkey = lineitem.l_suppkey
    AND nation.n_nationkey = supplier.s_nationkey
    AND part.p_name LIKE '%steel%'
) AS combined
GROUP BY nation, o_year
ORDER BY o_year ASC, nation DESC;

-- =================================================================
-- Query ID: ETPCH_Q10 (Mutated)
-- Semantics: Different date window and include non-returned items
-- =================================================================
SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, nation, orders, lineitem
WHERE orders.o_orderkey = lineitem.l_orderkey
  AND customer.c_nationkey = nation.n_nationkey
  AND customer.c_custkey = orders.o_custkey
  AND lineitem.l_returnflag <> 'R'
  AND orders.o_orderdate BETWEEN '1995-04-01' AND '1995-06-30'
GROUP BY c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment
ORDER BY revenue ASC
LIMIT 10;

-- =================================================================
-- Query ID: ETPCH_Q12 (Mutated)
-- Semantics: Different shipmodes and reversed date condition order
-- =================================================================
SELECT l_shipmode,
       SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count,
       SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
FROM orders, lineitem
WHERE orders.o_orderkey = lineitem.l_orderkey
  AND lineitem.l_shipmode IN ('AIR', 'MAIL')
  AND lineitem.l_commitdate > lineitem.l_receiptdate
  AND lineitem.l_shipdate > lineitem.l_commitdate
  AND lineitem.l_receiptdate BETWEEN '1996-01-01' AND '1996-12-31'
GROUP BY l_shipmode
ORDER BY l_shipmode DESC;

-- =================================================================
-- Query ID: ETPCH_Q14 (Mutated)
-- Semantics: Different month and type prefix
-- =================================================================
SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'ECONOMY%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue_percentage
FROM part, lineitem
WHERE part.p_partkey = lineitem.l_partkey
  AND lineitem.l_shipdate BETWEEN '1995-02-01' AND '1995-02-28';

-- =================================================================
-- Query ID: ETPCH_Q21 (Mutated)
-- Semantics: Different nation and commit/receipt inversion
-- =================================================================
SELECT s_name, COUNT(*) AS numwait
FROM supplier, nation, orders, lineitem l1
WHERE s_suppkey = l1.l_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
  AND l1.l_orderkey = o_orderkey
  AND o_orderstatus <> 'F'
  AND l1.l_commitdate > l1.l_receiptdate
GROUP BY s_name
ORDER BY s_name;

-- =================================================================
-- Query ID: ETPCH_Q23 (Mutated)
-- Semantics: Different substring width and year filter
-- =================================================================
SELECT substring(c_address from char_length(c_address)-2) AS city, p_brand AS part_brand
FROM customer, orders, lineitem, part
WHERE c_custkey = o_custkey
  AND o_orderkey = l_orderkey
  AND l_partkey = p_partkey
  AND l_returnflag = 'R'
  AND o_orderdate BETWEEN DATE '1994-01-01' AND DATE '1994-12-31'
GROUP BY substring(c_address from char_length(c_address)-2), p_brand
ORDER BY city DESC, part_brand ASC;

-- =================================================================
-- Query ID: ETPCH_Q24 (Mutated)
-- Semantics: Distinct city with different year
-- =================================================================
SELECT DISTINCT c_address AS city
FROM customer JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_returnflag = 'R'
  AND o_orderdate BETWEEN '1996-01-01' AND '1996-12-31';

-- =================================================================
-- Query ID: LITHE_1 (Mutated)
-- Semantics: Change cutoff and ordering
-- =================================================================
SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-10-01' GROUP BY l_returnflag, l_linestatus ORDER BY l_linestatus, l_returnflag;

-- =================================================================
-- Query ID: LITHE_2 (Mutated)
-- Semantics: Different size and region
-- =================================================================
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 10 AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' ORDER BY s_acctbal ASC, n_name DESC, s_name DESC, p_partkey DESC LIMIT 80;

-- =================================================================
-- Query ID: LITHE_3 (Mutated)
-- Semantics: Different segment/date
-- =================================================================
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'FURNITURE' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= DATE '1995-01-01' AND l_shipdate <= DATE '1995-01-01' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY o_shippriority DESC;

-- =================================================================
-- Query ID: LITHE_4 (Mutated)
-- Semantics: Different quarter
-- =================================================================
SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= DATE '1994-04-01' AND o_orderdate < DATE '1994-07-01' AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate > l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority DESC;

-- =================================================================
-- Query ID: LITHE_5 (Mutated)
-- Semantics: Region change and distinct
-- =================================================================
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND o_orderdate BETWEEN DATE '1996-01-01' AND DATE '1996-12-31' GROUP BY n_name ORDER BY n_name ASC;

-- =================================================================
-- Query ID: LITHE_6 (Mutated)
-- Semantics: Narrow date and stricter quantity
-- =================================================================
SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1994-03-01' AND l_discount BETWEEN 0.02 AND 0.04 AND l_quantity < 5;

-- =================================================================
-- Query ID: LITHE_7 (Mutated)
-- Semantics: Replace nation pair
-- =================================================================
SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM (
  SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume
  FROM supplier, lineitem, orders, customer, nation n1, nation n2
  WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'ARGENTINA' AND n2.n_name = 'BRAZIL') OR (n1.n_name = 'BRAZIL' AND n2.n_name = 'ARGENTINA'))
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY l_year DESC;

-- =================================================================
-- Query ID: LITHE_8 (Mutated)
-- Semantics: Different nation focus and type
-- =================================================================
SELECT o_year, SUM(CASE WHEN nation = 'GERMANY' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share FROM (
  SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation
  FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
  WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
    AND r_name = 'EUROPE' AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1996-01-01' AND DATE '1996-12-31'
    AND p_type = 'PROMO POLISHED COPPER'
) AS all_nations
GROUP BY o_year
ORDER BY o_year DESC;

-- =================================================================
-- Query ID: LITHE_9 (Mutated)
-- Semantics: Different part name filter
-- =================================================================
SELECT nation, o_year, SUM(amount) AS sum_profit FROM (
  SELECT n_name AS nation, p_name, EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
  FROM part, supplier, lineitem, partsupp, orders, nation
  WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE 'st%'
) AS profit
GROUP BY nation, o_year
ORDER BY o_year ASC;

-- =================================================================
-- Query ID: LITHE_10 (Mutated)
-- Semantics: Different month and include non-returned items
-- =================================================================
SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate BETWEEN DATE '1995-04-01' AND DATE '1995-06-30' AND l_returnflag <> 'R' AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue ASC;

-- =================================================================
-- Query ID: LITHE_11 (Mutated)
-- Semantics: Different nation and threshold
-- =================================================================
SELECT ps_partkey, n_name, SUM(ps_supplycost * ps_availqty) AS total_value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'ARGENTINA'
GROUP BY ps_partkey, n_name
HAVING SUM(ps_supplycost * ps_availqty) < (
  SELECT SUM(ps_supplycost * ps_availqty) * 0.00002 FROM partsupp, supplier, nation
  WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'ARGENTINA'
)
ORDER BY total_value ASC;

-- =================================================================
-- Query ID: LITHE_12 (Mutated)
-- Semantics: Different shipmode and reversed conditions
-- =================================================================
SELECT l_shipmode, SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey AND l_shipmode = 'TRUCK' AND l_commitdate > l_receiptdate AND l_shipdate > l_commitdate AND l_receiptdate BETWEEN DATE '1996-01-01' AND DATE '1996-12-31'
GROUP BY l_shipmode
ORDER BY l_shipmode DESC;

-- =================================================================
-- Query ID: LITHE_13 (Mutated)
-- Semantics: Different phrase and order
-- =================================================================
SELECT c_count, c_orderdate, COUNT(*) AS custdist
FROM (
  SELECT c_custkey, o_orderdate, COUNT(o_orderkey)
  FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment LIKE '%special%requests%'
  GROUP BY c_custkey, o_orderdate
) AS c_orders (c_custkey, c_count, c_orderdate)
GROUP BY c_count, c_orderdate
ORDER BY c_count ASC, custdist ASC;

-- =================================================================
-- Query ID: LITHE_14 (Mutated)
-- Semantics: Different promo type and month
-- =================================================================
SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'MEDIUM%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey AND l_shipdate BETWEEN DATE '1995-02-01' AND DATE '1995-02-28';

-- =================================================================
-- Query ID: LITHE_15 (Mutated)
-- Semantics: Different month and pick second-highest revenue
-- =================================================================
WITH revenue (supplier_no, total_revenue) AS (
  SELECT l_suppkey, SUM(l_extendedprice * (1 - l_discount))
  FROM lineitem
  WHERE l_shipdate >= DATE '1995-02-01' AND l_shipdate < DATE '1995-02-01' + INTERVAL '3' MONTH
  GROUP BY l_suppkey
), ranked AS (
  SELECT supplier_no, total_revenue, DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS rk FROM revenue
)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM supplier JOIN ranked ON s_suppkey = supplier_no
WHERE rk = 2
ORDER BY s_suppkey DESC;

-- =================================================================
-- Query ID: LITHE_16 (Mutated)
-- Semantics: Include suppliers flagged in comments
-- =================================================================
SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND p_brand = 'Brand#23'
  AND p_type LIKE 'MEDIUM POLISHED%'
  AND p_size NOT IN (1, 4, 7)
  AND ps_suppkey IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%')
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt ASC;

-- =================================================================
-- Query ID: LITHE_17 (Mutated)
-- Semantics: Different brand/container thresholds
-- =================================================================
SELECT SUM(l_extendedprice) / 5.0 AS avg_yearly
FROM lineitem, part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#51'
  AND p_container = 'SM BOX'
  AND l_quantity >= (
    SELECT 1.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey
  );

-- =================================================================
-- Query ID: LITHE_18 (Mutated)
-- Semantics: Lower quantity threshold
-- =================================================================
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (
  SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 100
)
AND c_custkey = o_custkey
AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_orderdate DESC;

-- =================================================================
-- Query ID: LITHE_19 (Mutated)
-- Semantics: Different brand sets and shipinstruct
-- =================================================================
SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem, part
WHERE (
  p_partkey = l_partkey AND p_brand = 'Brand#11' AND p_container IN ('SM CASE', 'SM BOX') AND l_quantity BETWEEN 2 AND 8 AND p_size BETWEEN 2 AND 6 AND l_shipmode IN ('TRUCK') AND l_shipinstruct = 'COLLECT AT STORE'
) OR (
  p_partkey = l_partkey AND p_brand = 'Brand#22' AND p_container IN ('MED BAG', 'MED BOX') AND l_quantity BETWEEN 8 AND 16 AND p_size BETWEEN 3 AND 9 AND l_shipmode IN ('MAIL') AND l_shipinstruct = 'COLLECT AT STORE'
) OR (
  p_partkey = l_partkey AND p_brand = 'Brand#33' AND p_container IN ('LG CASE', 'LG BOX') AND l_quantity BETWEEN 15 AND 25 AND p_size BETWEEN 4 AND 12 AND l_shipmode IN ('MAIL') AND l_shipinstruct = 'COLLECT AT STORE'
);

-- =================================================================
-- Query ID: LITHE_20 (Mutated)
-- Semantics: Different country and availability threshold
-- =================================================================
SELECT s_name, s_address
FROM supplier, nation
WHERE s_suppkey IN (
  SELECT ps_suppkey FROM partsupp
  WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE '%ivory%')
    AND ps_availqty < (
      SELECT 0.3 * SUM(l_quantity)
      FROM lineitem
      WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1995-12-31'
    )
)
AND s_nationkey = n_nationkey
AND n_name = 'GERMANY'
ORDER BY s_address DESC;

-- =================================================================
-- Query ID: LITHE_21 (Mutated)
-- Semantics: Different nation and status
-- =================================================================
SELECT s_name, COUNT(*) AS numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'P'
  AND l1.l_receiptdate < l1.l_commitdate
  AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey = l1.l_suppkey)
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY s_name
ORDER BY s_name ASC;

-- =================================================================
-- Query ID: LITHE_22 (Mutated)
-- Semantics: Different country code set and invert acctbal comparison
-- =================================================================
SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
FROM (
  SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
  FROM customer
  WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('11', '12', '14')
    AND c_acctbal < (
      SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('11', '12', '14')
    )
    AND EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)
) AS custsale
GROUP BY cntrycode
ORDER BY totacctbal DESC;
