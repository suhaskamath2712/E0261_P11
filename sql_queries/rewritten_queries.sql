-- =================================================================
-- Query ID: U1 (Rewritten)
-- Description: A UNION ALL query combining top parts by available quantity
-- with top suppliers by available quantity.
-- =================================================================
(SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100 Order By p_partkey Limit 5) UNION ALL (SELECT s_suppkey, s_name FROM supplier, partsupp where s_suppkey = ps_suppkey and ps_availqty > 200 Order By s_suppkey Limit 7);

-- =================================================================
-- Query ID: U2 (Rewritten)
-- Description: Combines top suppliers from Germany with top urgent priority customers.
-- =================================================================
(SELECT s_suppkey, s_name FROM supplier, nation where s_nationkey = n_nationkey and  n_name = 'GERMANY' order by s_suppkey desc, s_name limit 12) UNION ALL (SELECT c_custkey, c_name FROM customer,  orders where c_custkey = o_custkey and o_orderpriority = '1-URGENT' order by c_custkey, c_name desc limit 10);

-- =================================================================
-- Query ID: U3 (Rewritten)
-- Description: A three-part UNION ALL query fetching customers from the USA, parts with high quantity,
-- and nations starting with 'B'.
-- =================================================================
(SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and  n_name = 'UNITED STATES' Order by key Limit 10) UNION ALL (SELECT p_partkey as key, p_name as name FROM part , lineitem where p_partkey = l_partkey and l_quantity > 35 Order By key Limit 10) UNION ALL (select n_nationkey as key, r_name as name from nation, region where n_name LIKE 'B%' Order By key Limit 5);

-- =================================================================
-- Query ID: U4 (Rewritten)
-- Description: A four-part UNION ALL query combining customers from the USA, suppliers from Canada,
-- parts with high quantity, and parts with high supply cost.
-- =================================================================
(SELECT c_custkey, c_name FROM customer,  nation where c_nationkey = n_nationkey and n_name = 'UNITED STATES' Order By c_custkey desc Limit 5) UNION ALL (SELECT s_suppkey, s_name FROM supplier ,  nation where s_nationkey = n_nationkey and n_name = 'CANADA' Order By s_suppkey Limit 6) UNION ALL (SELECT p_partkey, p_name FROM part ,  lineitem where p_partkey = l_partkey and l_quantity > 20 Order By p_partkey desc Limit 7) UNION ALL (SELECT ps_partkey, p_name FROM part ,  partsupp where p_partkey = ps_partkey and ps_supplycost >= 1000 Order By ps_partkey Limit 8);

-- =================================================================
-- Query ID: U5 (Rewritten)
-- Description: Combines recent orders for a specific customer with older line items that have
-- high quantity and price.
-- =================================================================
(SELECT o_orderkey, o_orderdate, n_name FROM orders, customer, nation where o_custkey = c_custkey and c_nationkey = n_nationkey and c_name like '%0001248%'  AND o_orderdate >= '1997-01-01' order by o_orderkey Limit 20) UNION ALL (SELECT l_orderkey, l_shipdate, o_orderstatus FROM lineitem, orders where l_orderkey = o_orderkey and o_orderdate < '1994-01-01'   AND l_quantity > 20   AND l_extendedprice > 1000 order by l_orderkey Limit 5);

-- =================================================================
-- Query ID: U6 (Rewritten)
-- Description: Combines top clerks by total price on older orders with total account balance for
-- nations with 'UNITED' in their name.
-- =================================================================
(SELECT o_clerk as name, SUM(l_extendedprice) AS total_price FROM orders, lineitem where o_orderkey = l_orderkey and o_orderdate <= '1995-01-01' GROUP BY o_clerk ORDER BY total_price DESC LIMIT 10) UNION ALL (SELECT n_name as name, SUM(s_acctbal) AS total_price FROM nation ,supplier where n_nationkey = s_nationkey and n_name like '%UNITED%' GROUP BY n_name ORDER BY n_name DESC Limit 10);

-- =================================================================
-- Query ID: U7 (Rewritten)
-- Description: Combines line items from a specific date range and quantity with parts having a low supply cost.
-- =================================================================
(SELECT l_orderkey as key, l_extendedprice as price, l_partkey as s_key FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_quantity > 30  Order By key Limit 20) UNION ALL (SELECT ps_partkey as key, p_retailprice as price, ps_suppkey as s_key FROM partsupp,supplier,part where ps_suppkey = s_suppkey and ps_partkey = p_partkey AND ps_supplycost < 100 Order By price Limit 20);

-- =================================================================
-- Query ID: U8 (Rewritten)
-- Description: Combines customers with the fewest recent orders and line items with the highest average quantity.
-- =================================================================
(SELECT c_custkey as order_id, COUNT(*) AS total FROM customer, orders WHERE c_custkey = o_custkey AND o_orderdate >= '1995-01-01' GROUP BY c_custkey ORDER BY total ASC LIMIT 10) UNION ALL (SELECT l_orderkey as order_id, AVG(l_quantity) AS total FROM orders, lineitem WHERE l_orderkey = o_orderkey AND o_orderdate < DATE '1996-07-01' GROUP BY l_orderkey ORDER BY total DESC LIMIT 10);

-- =================================================================
-- Query ID: U9 (Rewritten)
-- Description: Combines customers in the 'BUILDING' segment with high account balance and suppliers
-- with a very high account balance.
-- =================================================================
(select c_name, n_name from customer, nation where c_mktsegment='BUILDING' and c_acctbal > 100 and c_nationkey = n_nationkey) UNION ALL (select s_name, n_name from supplier, nation where s_acctbal > 4000 and s_nationkey = n_nationkey);

-- =================================================================
-- Query ID: O1 (Rewritten)
-- Description: Uses a RIGHT OUTER JOIN to list customers and their nations, counting totals.
-- =================================================================
select c_name, n_name, count(*) as total from nation RIGHT OUTER JOIN customer ON c_nationkey = n_nationkey and c_acctbal < 1000 GROUP BY c_name, n_name Order by c_name, n_name desc Limit 10;

-- =================================================================
-- Query ID: O2 (Rewritten)
-- Description: Uses a LEFT OUTER JOIN to count low-quantity line items based on ship mode and priority.
-- =================================================================
SELECT l_shipmode, o_shippriority ,count(*) as low_line_count FROM lineitem LEFT OUTER JOIN orders ON ( l_orderkey = o_orderkey AND o_totalprice > 50000 ) WHERE l_linenumber = 4 AND l_quantity < 30 GROUP BY l_shipmode, o_shippriority Order By l_shipmode Limit 5;

-- =================================================================
-- Query ID: O3 (Rewritten)
-- Description: Uses a FULL OUTER JOIN to sum customer account balances for finished orders.
-- =================================================================
SELECT o_custkey as key, sum(c_acctbal), o_clerk, c_name from orders FULL OUTER JOIN customer on c_custkey = o_custkey and o_orderstatus = 'F' group by o_custkey, o_clerk, c_name order by key limit 35;

-- =================================================================
-- Query ID: O4 (Rewritten)
-- Description: A complex query with multiple OUTER JOINs to retrieve part, supplier, and nation data.
-- =================================================================
SELECT p_size, s_phone, ps_supplycost, n_name FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 FULL OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey > 3 Order by ps_supplycost asc Limit 50;

-- =================================================================
-- Query ID: O5 (Rewritten)
-- Description: Uses a RIGHT OUTER JOIN to get parts with specific size and availability.
-- =================================================================
Select ps_suppkey, p_name, p_type from part RIGHT outer join partsupp on p_partkey=ps_partkey and p_size > 4 and ps_availqty > 3350 Order By ps_suppkey Limit 100;

-- =================================================================
-- Query ID: O6 (Rewritten)
-- Description: Similar to O4, this query uses multiple OUTER JOINs with different ordering.
-- =================================================================
SELECT p_name, s_phone, ps_supplycost, n_name FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 FULL OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey > 3 Order By p_name, s_phone, ps_supplycost, n_name desc Limit 20;

-- =================================================================
-- Query ID: A1 (Rewritten)
-- Description: An aggregate query counting line items by ship mode based on date and price conditions.
-- =================================================================
SELECT l_shipmode, COUNT(*) AS cnt
FROM lineitem JOIN orders
ON o_orderkey = l_orderkey
WHERE l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= '1994-01-01' 
    AND l_receiptdate < '1995-01-01' 
    -- CAUGHT: If o_totalprice is NULL, LEAST returns NULL, so the condition fails
    AND l_extendedprice <= LEAST(o_totalprice, 70000)
    AND o_totalprice > 60000 
GROUP BY l_shipmode
ORDER BY l_shipmode;

-- =================================================================
-- Query ID: A2 (Rewritten)
-- Description: An aggregate query counting orders by priority within a specific date range.
-- =================================================================
Select o_orderpriority, count(*) as order_count From orders, lineitem Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and l_commitdate <= l_receiptdate Group By o_orderpriority Order By o_orderpriority;

-- =================================================================
-- Query ID: A3 (Rewritten)
-- Description: Selects line items based on a complex set of date and availability conditions.
-- =================================================================
Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where o_orderkey = l_orderkey and ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber and l_shipdate >= o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate > '1994-01-01' Order By l_orderkey Limit 7;

-- =================================================================
-- Query ID: A4 (Rewritten)
-- Description: Counts the number of waiting line items for each supplier for finished orders.
-- =================================================================
Select s_name, count(*) as numwait From supplier, lineitem, orders, nation Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' and l_receiptdate >= l_commitdate and s_nationkey = n_nationkey Group By s_name Order By numwait desc Limit 100;

-- =================================================================
-- Query ID: N1 (Rewritten)
-- Description: Counts suppliers for parts based on brand, type, and size.
-- =================================================================
Select p_brand, p_type, p_size, Count(*) as supplier_cnt From part, partsupp Where p_partkey = ps_partkey and p_size >= 4 and p_type NOT LIKE 'SMALL PLATED%' and p_brand <> 'Brand#45' Group By p_brand, p_size, p_type Order By supplier_cnt desc, p_brand asc, p_type asc, p_size asc;

-- =================================================================
-- Query ID: F1 (Rewritten)
-- Description: Combines line items for JUMBO container parts with customers having specific keys.
-- =================================================================
(SELECT
    o.o_orderkey,
    c.c_acctbal AS price,
    c.c_custkey
FROM
    customer AS c
LEFT JOIN
    orders AS o ON c.c_custkey = o.o_custkey
WHERE
    c.c_custkey BETWEEN 1001 AND 1009
ORDER BY
    price DESC,
    o.o_orderkey,
    c.c_custkey
LIMIT 100)
UNION ALL
(SELECT
    l.l_orderkey,
    l.l_extendedprice AS price,
    p.p_partkey
FROM
    lineitem AS l,
    part AS p
WHERE
    l.l_partkey = p.p_partkey
    AND p.p_container LIKE 'JUMBO%'
    AND p.p_partkey BETWEEN 3001 AND 3009
ORDER BY
    l.l_orderkey,
    price DESC
LIMIT 100);

-- =================================================================
-- Query ID: MQ1 (Rewritten)
-- Description: Pricing Summary Report Query. Provides a summary of line item pricing.
-- =================================================================
SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '71' DAY GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

-- =================================================================
-- Query ID: MQ2 (Rewritten)
-- Description: Minimum Cost Supplier Query. Finds suppliers in a region who can supply a specific part at minimum cost.
-- =================================================================
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 38 AND p_type LIKE '%TIN' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'MIDDLE EAST' ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;

-- =================================================================
-- Query ID: MQ3 (Rewritten)
-- Description: Shipping Priority Query. Lists unshipped orders with the highest revenue.
-- =================================================================
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;

-- =================================================================
-- Query ID: MQ4 (Rewritten)
-- Description: Order Priority Checking Query. Counts orders by priority for a given quarter.
-- =================================================================
SELECT o_orderdate, o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= DATE '1997-07-01' AND o_orderdate < DATE '1997-07-01' + INTERVAL '3' MONTH GROUP BY o_orderdate, o_orderpriority ORDER BY o_orderpriority LIMIT 10;

-- =================================================================
-- Query ID: MQ5 (Rewritten)
-- Description: Local Supplier Volume Query. Lists revenue volume for suppliers in a specific region.
-- =================================================================
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'MIDDLE EAST' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR GROUP BY n_name ORDER BY revenue DESC LIMIT 100;

-- =================================================================
-- Query ID: MQ6 (Rewritten)
-- Description: Forecasting Revenue Change Query. Quantifies revenue increase if discounts were eliminated.
-- =================================================================
SELECT l_shipmode, SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR AND l_quantity < 24 GROUP BY l_shipmode LIMIT 100;

-- =================================================================
-- Query ID: MQ10 (Rewritten)
-- Description: Returned Item Reporting Query. Identifies customers who have returned items.
-- =================================================================
SELECT c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1994-01-01' + INTERVAL '3' MONTH AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20;

-- =================================================================
-- Query ID: MQ11 (Rewritten)
-- Description: Important Stock Identification Query. Finds the most important stock in a given nation.
-- =================================================================
SELECT ps_comment, SUM(ps_supplycost * ps_availqty) AS total_value FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'ARGENTINA' GROUP BY ps_comment ORDER BY total_value DESC LIMIT 100;

-- =================================================================
-- Query ID: MQ17 (Rewritten)
-- Description: Small-Quantity-Order Revenue Query. Determines average extended price for small-quantity orders.
-- =================================================================
SELECT AVG(l_extendedprice) AS avg_total FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = 'Brand#52' AND p_container = 'LG CAN';

-- =================================================================
-- Query ID: MQ18 (Rewritten)
-- Description: Large Volume Customer Query. Finds a large volume of orders for customers with a specific phone prefix.
-- =================================================================
SELECT c_name, o_orderdate, o_totalprice, SUM(l_quantity) FROM customer, orders, lineitem WHERE c_phone LIKE '27-%' AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, o_orderdate, o_totalprice ORDER BY o_orderdate, o_totalprice DESC LIMIT 100;

-- =================================================================
-- Query ID: MQ21 (Rewritten)
-- Description: Suppliers Who Kept Orders Waiting Query. Finds suppliers who had line items not shipped on time.
-- =================================================================
SELECT s_name, COUNT(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;

-- =================================================================
-- Query ID: Alaap (Rewritten)
-- Description: A custom query calculating revenue by market segment and shipping priority.
-- =================================================================
select c_mktsegment, sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate <= date '1995-10-13' and l_extendedprice between 212 and 3000 and l_quantity <= 123 group by o_orderdate, o_shippriority, c_mktsegment order by revenue desc, o_orderdate asc, o_shippriority asc;

-- =================================================================
-- Query ID: TPCH_Q9 (Rewritten)
-- Description: Product Type Profit Measure Query. Computes profit for all parts with a certain name substring.
-- =================================================================
select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like 'co%' ) as profit group by nation, o_year order by nation, o_year desc;

-- =================================================================
-- Query ID: TPCH_Q13 (Rewritten)
-- Description: Customer Distribution Query. Counts customers by the number of orders they have placed.
-- =================================================================
select c_count, c_orderdate, count(*) as custdist from ( select c_custkey, o_orderdate, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%among%regular%' group by c_custkey, o_orderdate ) as c_orders (c_custkey, c_count, c_orderdate) group by c_count, c_orderdate order by custdist desc, c_count desc;

-- =================================================================
-- Query ID: ETPCH_Q15 (Rewritten)
-- Description: Top Supplier Query. Finds the supplier with the maximum total revenue for a given period.
-- =================================================================
WITH revenue (supplier_no, total_revenue) AS ( SELECT l_suppkey, SUM(l_extendedprice * (1 - l_discount)) FROM lineitem WHERE l_shipdate >= DATE '1995-01-01' AND l_shipdate < DATE '1995-01-01' + INTERVAL '1' MONTH GROUP BY l_suppkey ) SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, revenue WHERE s_suppkey = supplier_no AND total_revenue = ( SELECT MAX(total_revenue) FROM revenue ) ORDER BY s_suppkey;

-- =================================================================
-- Query ID: Nested_Test (Rewritten)
-- Description: A sample nested query to find suppliers from France for 'ivory' parts.
-- =================================================================
select s_name, s_address from supplier, nation, partsupp, part where s_suppkey = ps_suppkey and ps_partkey = p_partkey and p_name like '%ivory%' and s_nationkey = n_nationkey and n_name = 'FRANCE' and ps_availqty > (select min(c_acctbal) from customer) order by s_name;

-- =================================================================
-- Query ID: paper_sample (Rewritten & Corrected)
-- Description: A custom UNION ALL query combining high-value customers with suppliers based on average price.
-- =================================================================
(SELECT c_name AS entity_name, n_name AS country, o_totalprice AS price FROM orders JOIN customer ON c_custkey = o_custkey JOIN nation ON c_nationkey = n_nationkey WHERE o_totalprice <= 15000 AND o_totalprice <= c_acctbal AND c_acctbal >= 9000 AND c_mktsegment IN ('HOUSEHOLD', 'MACHINERY') GROUP BY c_name, n_name, o_totalprice ORDER BY price ASC, country ASC, entity_name) UNION ALL (SELECT s_name AS entity_name, n_name AS country, AVG(l_extendedprice * (1 - l_discount)) AS price FROM lineitem JOIN supplier ON l_suppkey = s_suppkey JOIN nation ON n_nationkey = s_nationkey JOIN orders ON l_orderkey = o_orderkey JOIN region ON n_regionkey = r_regionkey WHERE s_acctbal <= o_totalprice AND o_totalprice <= 15000 GROUP BY n_name, s_name ORDER BY price DESC, country DESC, entity_name);

-- =================================================================
-- Query ID: ETPCH_Q1 (Rewritten)
-- Description: Simplified version of ETPCH Q1, using the standard lineitem table.
-- =================================================================
SELECT l_returnflag as returnflag, l_linestatus as linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

-- =================================================================
-- Query ID: ETPCH_Q3 (Rewritten)
-- Description: Simplified version of ETPCH Q3, using the standard lineitem table for shipping priority analysis.
-- =================================================================
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'FURNITURE' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-01-01' AND l_shipdate > DATE '1995-01-01' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC LIMIT 10;

-- =================================================================
-- Query ID: ETPCH_Q4 (Rewritten)
-- Description: Simplified version of ETPCH Q4, using the standard lineitem table for order priority checking.
-- =================================================================
SELECT o_orderpriority, COUNT(DISTINCT o_orderkey) AS order_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_commitdate < l_receiptdate AND o_orderdate BETWEEN '1995-01-01' AND '1995-03-31' GROUP BY o_orderpriority ORDER BY o_orderpriority ASC;

-- =================================================================
-- Query ID: ETPCH_Q5 (Rewritten)
-- Description: Simplified version of ETPCH Q5, using standard tables for local supplier volume.
-- =================================================================
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, nation, orders, region, supplier, lineitem WHERE customer.c_custkey = orders.o_custkey AND customer.c_nationkey = nation.n_nationkey AND nation.n_nationkey = supplier.s_nationkey AND orders.o_orderkey = lineitem.l_orderkey AND nation.n_regionkey = region.r_regionkey AND supplier.s_suppkey = lineitem.l_suppkey AND region.r_name = 'ASIA' AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31' GROUP BY n_name ORDER BY revenue DESC, n_name ASC;

-- =================================================================
-- Query ID: ETPCH_Q6 (Rewritten)
-- Description: Simplified version of ETPCH Q6, using the standard lineitem table for forecasting revenue change.
-- =================================================================
SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1993-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;

-- =================================================================
-- Query ID: ETPCH_Q6_1 (Rewritten)
-- Description: A variation of ETPCH_Q6 focusing only on the standard lineitem table.
-- =================================================================
SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1993-01-01' AND l_shipdate < DATE '1995-01-01' AND l_quantity < 24;

-- =================================================================
-- Query ID: ETPCH_Q6_2 (Rewritten)
-- Description: Another variation of ETPCH_Q6, summing revenue from the standard lineitem table.
-- =================================================================
SELECT sum(l_extendedprice ) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1993-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;

-- =================================================================
-- Query ID: ETPCH_Q7 (Rewritten)
-- Description: Simplified version of ETPCH Q7, analyzing trade volume between two nations using standard tables.
-- =================================================================
SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM l_shipdate) AS l_year, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, nation n1, nation n2, orders, supplier, lineitem WHERE orders.o_orderkey = lineitem.l_orderkey AND supplier.s_suppkey = lineitem.l_suppkey AND customer.c_custkey = orders.o_custkey AND customer.c_nationkey = n2.n_nationkey AND n1.n_nationkey = supplier.s_nationkey AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n2.n_name = 'FRANCE' AND n1.n_name = 'GERMANY')) AND lineitem.l_shipdate BETWEEN '1995-01-01' AND '1996-12-31' GROUP BY n1.n_name, n2.n_name, EXTRACT(YEAR FROM l_shipdate) ORDER BY n1.n_name, n2.n_name, EXTRACT(YEAR FROM l_shipdate);

-- =================================================================
-- Query ID: ETPCH_Q9 (Rewritten)
-- Description: Simplified version of ETPCH Q9, calculating profit by nation and year using standard tables.
-- =================================================================
SELECT nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, SUM(profit) AS sum_profit FROM ( SELECT n_name AS nation, o_orderdate, (l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS profit FROM nation, orders, part, partsupp, supplier, lineitem WHERE orders.o_orderkey = lineitem.l_orderkey AND part.p_partkey = partsupp.ps_partkey AND partsupp.ps_partkey = lineitem.l_partkey AND partsupp.ps_suppkey = supplier.s_suppkey AND supplier.s_suppkey = lineitem.l_suppkey AND nation.n_nationkey = supplier.s_nationkey AND part.p_name LIKE '%co%' ) AS combined GROUP BY nation, EXTRACT(YEAR FROM o_orderdate) ORDER BY nation ASC, o_year DESC;

-- =================================================================
-- Query ID: ETPCH_Q10 (Rewritten)
-- Description: Simplified version of ETPCH Q10, reporting on returned items using the standard lineitem table.
-- =================================================================
SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, nation, orders, lineitem WHERE orders.o_orderkey = lineitem.l_orderkey AND customer.c_nationkey = nation.n_nationkey AND customer.c_custkey = orders.o_custkey AND lineitem.l_returnflag = 'R' AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31' GROUP BY c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment ORDER BY revenue DESC, c_custkey ASC, c_name ASC, c_acctbal ASC, c_phone ASC, n_name ASC, c_address ASC, c_comment ASC LIMIT 20;

-- =================================================================
-- Query ID: ETPCH_Q12 (Rewritten)
-- Description: Simplified ETPCH Q12, analyzing shipping modes and order priority using the standard lineitem table.
-- =================================================================
SELECT l_shipmode as shipmode, SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE orders.o_orderkey = lineitem.l_orderkey AND lineitem.l_shipdate < lineitem.l_commitdate AND lineitem.l_commitdate < lineitem.l_receiptdate AND lineitem.l_shipmode IN ('SHIP', 'TRUCK') AND lineitem.l_receiptdate BETWEEN '1995-01-01' AND '1995-12-31' GROUP BY l_shipmode;

-- =================================================================
-- Query ID: ETPCH_Q14 (Rewritten)
-- Description: Simplified ETPCH Q14, calculating promotion effect using the standard lineitem table.
-- =================================================================
SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue_percentage FROM part, lineitem WHERE part.p_partkey = lineitem.l_partkey AND lineitem.l_shipdate BETWEEN '1995-01-01' AND '1995-01-31';

-- =================================================================
-- Query ID: ETPCH_Q21 (Rewritten)
-- Description: Simplified ETPCH Q21, finding suppliers in Argentina who kept orders waiting, using standard tables.
-- =================================================================
SELECT s_name, COUNT(*) AS numwait FROM supplier, nation, orders, lineitem l1 WHERE s_suppkey = l1.l_suppkey AND s_nationkey = n_nationkey AND n_name = 'ARGENTINA' AND l1.l_orderkey = o_orderkey AND o_orderstatus = 'F' AND l1.l_commitdate < l1.l_receiptdate AND EXISTS ( SELECT 1 FROM lineitem l2 WHERE l1.l_orderkey = l2.l_orderkey AND l1.l_suppkey <> l2.l_suppkey ) AND NOT EXISTS ( SELECT 1 FROM lineitem l3 WHERE l1.l_orderkey = l3.l_orderkey AND l1.l_suppkey <> l3.l_suppkey AND l3.l_commitdate < l3.l_receiptdate ) GROUP BY s_name ORDER BY numwait DESC, s_name;

-- =================================================================
-- Query ID: ETPCH_Q23 (Rewritten)
-- Description: A simplified version of a complex query analyzing customer returns using standard tables.
-- =================================================================
SELECT substring(c_address from char_length(c_address)-4) AS city, p_brand AS part_brand FROM customer, orders, lineitem, part WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey AND l_partkey = p_partkey AND l_returnflag = 'R' AND o_orderdate BETWEEN date '1995-01-01' AND date '1995-12-31' GROUP BY substring(c_address from char_length(c_address)-4), p_brand ORDER BY city, part_brand;

-- =================================================================
-- Query ID: ETPCH_Q24 (Rewritten)
-- Description: A simplified version of a complex query analyzing returns and supplier availability.
-- =================================================================
select c_address as city from customer, orders, lineitem, part, partsupp where c_custkey = o_custkey and o_orderkey = l_orderkey and l_partkey = p_partkey and l_suppkey = ps_suppkey and l_returnflag = 'R' and o_orderdate between date '1995-01-01' and date '1995-12-31' group by c_address;

-- =================================================================
-- Query ID: LITHE_1
-- Description: Standard query Q1 (pricing summary).
-- =================================================================
SELECT l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= '1998-11-28'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- =================================================================
-- Query ID: LITHE_2
-- Description: Standard query Q2 (minimum cost supplier-like).
-- =================================================================
WITH min_supplycost AS ( SELECT ps_partkey, MIN(ps_supplycost) AS min_cost FROM partsupp JOIN supplier ON s_suppkey = ps_suppkey JOIN nation ON s_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey WHERE r_name = 'EUROPE' GROUP BY ps_partkey ) SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part JOIN partsupp ON p_partkey = ps_partkey JOIN supplier ON s_suppkey = ps_suppkey JOIN nation ON s_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey JOIN min_supplycost ON part.p_partkey = min_supplycost.ps_partkey AND partsupp.ps_supplycost = min_supplycost.min_cost WHERE p_size = 15 AND p_type LIKE '%BRASS' AND r_name = 'EUROPE' ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;

-- =================================================================
-- Query ID: LITHE_3
-- Description: Standard query Q3 (shipping priority revenue).
-- =================================================================
with customer_orders_cte as 
    (select o_orderkey,
        o_orderdate,
        o_shippriority
    from customer, orders
    where c_mktsegment = 'FURNITURE'
        and c_custkey = o_custkey
        and o_orderdate < date '1995-01-01'
    ),
    lineitem_revenue_cte as
    (select l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue
        from lineitem
        where l_shipdate > date '1995-01-01'
        group by l_orderkey
    )
select co.o_orderkey, lr.revenue, co.o_orderdate, co.o_shippriority
from customer_orders_cte co join lineitem_revenue_cte lr
on co.o_orderkey = lr.l_orderkey
order by lr.revenue desc, co.o_orderdate;

-- =================================================================
-- Query ID: LITHE_4
-- Description: Standard query Q4 (order priority counting).
-- =================================================================
select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + INTERVAL '3' MONTH and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority ;

-- =================================================================
-- Query ID: LITHE_5
-- Description: Standard query Q5 (local supplier volume-like).
-- =================================================================
with filtered_orders as
    (select * from orders where o_orderkey in
        (select l_orderkey from lineitem))
select n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from customer, filtered_orders as orders, lineitem, supplier, nation, region
where c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey 
    and c_nationkey = s_nationkey 
    and s_nationkey = n_nationkey 
    and n_regionkey = r_regionkey 
    and r_name = 'ASIA' 
    and o_orderdate >= date '1995-01-01' 
    and o_orderdate < date '1995-01-01' + INTERVAL '1' YEAR 
group by n_name
order by revenue desc;

-- =================================================================
-- Query ID: LITHE_6
-- Description: Standard query Q6 (discounted revenue variant).
-- =================================================================
select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date '1993-01-01' and l_shipdate < date '1994-03-01' + INTERVAL '1' YEAR and l_discount between 0.05 and 0.07 and l_quantity < 10 ;

-- =================================================================
-- Query ID: LITHE_7
-- Description: Standard query Q7 (trade volume between two nations).
-- =================================================================
select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') or (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') ) and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year ;

-- =================================================================
-- Query ID: LITHE_8
-- Description: Standard query Q8 (market share by year).
-- =================================================================
select o_year, sum(case when nation = 'INDIA' then volume else 0 end) / sum(volume) as mkt_share from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'ASIA' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year ;

-- =================================================================
-- Query ID: LITHE_9
-- Description: Standard query Q9 (profit by nation and year).
-- =================================================================
SELECT n_name AS nation,
    EXTRACT(YEAR FROM o_orderdate) AS o_year,
    SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit
    FROM part JOIN partsupp ON p_partkey = ps_partkey
    JOIN lineitem ON l_partkey = p_partkey AND l_suppkey = ps_suppkey
    JOIN supplier ON s_suppkey = l_suppkey
    JOIN orders ON o_orderkey = l_orderkey
    JOIN nation ON s_nationkey = n_nationkey
    WHERE p_name LIKE 'co%'
GROUP BY n_name, o_year
ORDER BY n_name, o_year DESC;

-- =================================================================
-- Query ID: LITHE_10
-- Description: Standard query Q10 (returned item reporting).
-- =================================================================
SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, c.c_acctbal, n.n_name, c.c_address, c.c_phone, c.c_comment FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON l.l_orderkey = o.o_orderkey JOIN nation n ON c.c_nationkey = n.n_nationkey WHERE o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-03-31' AND l.l_returnflag = 'R' GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment ORDER BY revenue DESC;

-- =================================================================
-- Query ID: LITHE_11
-- Description: Standard query Q11 (important stock identification).
-- =================================================================
SELECT ps_partkey, n_name, SUM(ps_supplycost * ps_availqty) AS total_value FROM partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'INDIA' GROUP BY ps_partkey, n_name HAVING SUM(ps_supplycost * ps_availqty) > ( SELECT SUM(ps_supplycost * ps_availqty) * 0.00001 FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'INDIA' ) ORDER BY total_value DESC ;

-- =================================================================
-- Query ID: LITHE_12
-- Description: Standard query Q12 (shipping modes and order priority analysis).
-- =================================================================
SELECT l_shipmode, SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count FROM orders JOIN lineitem ON o_orderkey = l_orderkey WHERE l_shipmode = 'SHIP' AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= DATE '1995-01-01' AND l_receiptdate < DATE '1996-01-01' GROUP BY l_shipmode ORDER BY l_shipmode;

-- =================================================================
-- Query ID: LITHE_13
-- Description: Standard query Q13 (customer distribution by order count).
-- =================================================================
select c_count, c_orderdate, count(*) as custdist from ( select c_custkey, o_orderdate, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey, o_orderdate ) as c_orders (c_custkey, c_count, c_orderdate) group by c_count, c_orderdate order by custdist desc, c_count desc ;

-- =================================================================
-- Query ID: LITHE_14
-- Description: Standard query Q14 (promotion effect calculation).
-- =================================================================
select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= date '1995-01-01' and l_shipdate < date '1995-01-01' + INTERVAL '1' MONTH ;

-- =================================================================
-- Query ID: LITHE_15
-- Description: Standard query Q15 (top supplier revenue using CTE).
-- =================================================================
SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, r.total_revenue
FROM (SELECT l_suppkey AS supplier_no,
            SUM(l_extendedprice * (1 - l_discount)) AS total_revenue,
            MAX(SUM(l_extendedprice * (1 - l_discount))) OVER () AS max_revenue
        FROM lineitem
        WHERE l_shipdate >= DATE '1995-01-01' 
            AND l_shipdate < DATE '1995-01-01' + INTERVAL '3' MONTH 
        GROUP BY l_suppkey ) r
JOIN supplier s ON s.s_suppkey = r.supplier_no
WHERE r.total_revenue = r.max_revenue
ORDER BY s.s_suppkey;

-- =================================================================
-- Query ID: LITHE_16
-- Description: Standard query Q16 (parts/supplier relationship stats).
-- =================================================================
SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp JOIN part ON p_partkey = ps_partkey
LEFT JOIN supplier ON ps_suppkey = s_suppkey
    AND s_comment LIKE '%Customer%Complaints%'
WHERE p_brand <> 'Brand#23'
    AND p_type NOT LIKE 'MEDIUM POLISHED%' 
    AND p_size IN (1, 4, 7) 
    AND s_suppkey IS NULL 
GROUP BY p_brand, p_type, p_size 
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;

-- =================================================================
-- Query ID: LITHE_17
-- Description: Standard query Q17 (average yearly revenue for small-quantity orders).
-- =================================================================
select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#53' and p_container = 'MED BAG' and l_quantity < ( select 0.7 * avg(l_quantity) from lineitem where l_partkey = p_partkey ) ;

-- =================================================================
-- Query ID: LITHE_18
-- Description: Standard query Q18 (customers with large order quantities).
-- =================================================================
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate ;

-- =================================================================
-- Query ID: LITHE_19
-- Description: Standard query Q19 (discounted revenue across brands and containers).
-- =================================================================
select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) ;

-- =================================================================
-- Query ID: LITHE_20
-- Description: Standard query Q20 (suppliers for ivory parts in France with availability filter).
-- =================================================================
with filtered_supplier as (select * from supplier where s_nationkey in 
    (select n_nationkey from nation where n_name = 'FRANCE'))
select s_name, s_address 
from filtered_supplier as supplier, nation 
where s_suppkey in 
    (select ps_suppkey from partsupp where ps_partkey in 
        (select p_partkey from part where p_name like '%ivory%') 
        and ps_availqty >  
            (select 0.5 * sum(l_quantity) from lineitem
            where l_partkey = ps_partkey 
                and l_suppkey = ps_suppkey 
                and l_shipdate >= date '1995-01-01' 
                and l_shipdate < date '1995-01-01' + INTERVAL '1' YEAR)) 
    and s_nationkey = n_nationkey 
    and n_name = 'FRANCE' 
order by s_name; 

-- =================================================================
-- Query ID: LITHE_21
-- Description: Standard query Q21 (suppliers who kept orders waiting in Argentina).
-- =================================================================
select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'ARGENTINA' group by s_name order by numwait desc, s_name ;

-- =================================================================
-- Query ID: LITHE_22
-- Description: Standard query Q22 (country code customer/account balance aggregation).
-- =================================================================
with filtered_customer as ( select * from customer where substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17') ) select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from filtered_customer where c_acctbal > ( select avg(c_acctbal) from filtered_customer where c_acctbal > 0.00 ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode;

