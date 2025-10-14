-- =================================================================
-- Query ID: U1 (Mutated)
-- Description: A UNION ALL query combining top parts by available quantity
-- with top suppliers by available quantity.
-- Change: Flipped availqty logic from > to < and changed values.
-- =================================================================
(SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty < 50
Order By p_partkey Limit 5)
UNION ALL (SELECT s_suppkey, s_name FROM supplier, partsupp where s_suppkey = ps_suppkey and
ps_availqty < 100 Order By s_suppkey Limit 7);

-- =================================================================
-- Query ID: U2 (Mutated)
-- Description: Combines top suppliers from Germany with top urgent priority customers.
-- Change: Changed nation to 'FRANCE' and priority to '2-HIGH'.
-- =================================================================
(SELECT s_suppkey, s_name FROM supplier, nation where s_nationkey = n_nationkey and  n_name = 'FRANCE' order by s_suppkey desc, s_name limit 12)UNION ALL (SELECT c_custkey, c_name FROM customer,  orders where c_custkey = o_custkey and o_orderpriority = '2-HIGH' order by c_custkey, c_name desc limit 10);

-- =================================================================
-- Query ID: U3 (Mutated)
-- Description: A three-part UNION ALL query fetching customers from the USA, parts with high quantity,
-- and nations starting with 'B'.
-- Change: Changed quantity from > 35 to < 10.
-- =================================================================
(SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and  n_name = 'UNITED STATES' Order by key Limit 10)
 UNION ALL (SELECT p_partkey as key, p_name as name FROM part , lineitem where p_partkey = l_partkey and l_quantity < 10 Order By key Limit 10)
 UNION ALL (select n_nationkey as key, r_name as name from nation, region where n_name LIKE 'B%' Order By key Limit 5);

-- =================================================================
-- Query ID: U4 (Mutated)
-- Description: A four-part UNION ALL query combining customers from the USA, suppliers from Canada,
-- parts with high quantity, and parts with high supply cost.
-- Change: Changed supply cost from >= 1000 to < 100.
-- =================================================================
(SELECT c_custkey, c_name FROM customer,  nation where c_nationkey = n_nationkey and n_name = 'UNITED STATES' Order By c_custkey desc Limit 5)
 UNION ALL (SELECT s_suppkey, s_name FROM supplier ,  nation where s_nationkey = n_nationkey and n_name = 'CANADA' Order By s_suppkey Limit 6)
 UNION ALL (SELECT p_partkey, p_name FROM part ,  lineitem where p_partkey = l_partkey and l_quantity > 20 Order By p_partkey desc Limit 7)
 UNION ALL (SELECT ps_partkey, p_name FROM part ,  partsupp where p_partkey = ps_partkey and ps_supplycost < 100 Order By ps_partkey Limit 8);

-- =================================================================
-- Query ID: U5 (Mutated)
-- Description: Combines recent orders for a specific customer with older line items that have
-- high quantity and price.
-- Change: Flipped date condition from >= to <.
-- =================================================================
(SELECT o_orderkey, o_orderdate, n_name FROM orders, customer, nation where o_custkey = c_custkey and c_nationkey = n_nationkey and c_name like '%0001248%'  AND o_orderdate < '1997-01-01' order by o_orderkey Limit 20)
 UNION ALL (SELECT l_orderkey, l_shipdate, o_orderstatus FROM lineitem, orders where l_orderkey = o_orderkey and o_orderdate < '1994-01-01'   AND l_quantity > 20   AND l_extendedprice > 1000 order by l_orderkey Limit 5);

-- =================================================================
-- Query ID: U6 (Mutated)
-- Description: Combines top clerks by total price on older orders with total account balance for
-- nations with 'UNITED' in their name.
-- Change: Changed SUM to AVG for a different aggregate result.
-- =================================================================
(SELECT o_clerk as name, AVG(l_extendedprice) AS total_price FROM orders, lineitem where o_orderkey = l_orderkey and o_orderdate <= '1995-01-01' GROUP BY o_clerk ORDER BY total_price DESC LIMIT 10)
 UNION ALL (SELECT n_name as name, AVG(s_acctbal) AS total_price FROM nation ,supplier where n_nationkey = s_nationkey and n_name like '%UNITED%' GROUP BY n_name ORDER BY n_name DESC Limit 10);

-- =================================================================
-- Query ID: U7 (Mutated)
-- Description: Combines line items from a specific date range and quantity with parts having a low supply cost.
-- Change: Changed supply cost from < 100 to > 900.
-- =================================================================
(SELECT     l_orderkey as key,     l_extendedprice as price,     l_partkey as s_key FROM     lineitem WHERE     l_shipdate >= DATE '1994-01-01'     AND l_shipdate < DATE '1995-01-01'     AND l_quantity > 30  Order By key Limit 20)
 UNION ALL  (SELECT     ps_partkey as key,     p_retailprice as price,     ps_suppkey as s_key FROM     partsupp,supplier,part where ps_suppkey = s_suppkey and ps_partkey = p_partkey     AND ps_supplycost > 900 Order By price Limit 20);

-- =================================================================
-- Query ID: U8 (Mutated)
-- Description: Combines customers with the fewest recent orders and line items with the highest average quantity.
-- Change: Changed AVG to SUM.
-- =================================================================
(SELECT     c_custkey as order_id,     COUNT(*) AS total FROM
customer, orders where c_custkey = o_custkey and     o_orderdate >= '1995-01-01'
GROUP BY     c_custkey ORDER BY     total ASC LIMIT 10) UNION ALL
(SELECT     l_orderkey as order_id,     SUM(l_quantity) AS total FROM     orders, lineitem where
l_orderkey = o_orderkey     AND o_orderdate < DATE '1996-07-01' GROUP BY     l_orderkey ORDER BY
total DESC LIMIT 10);

-- =================================================================
-- Query ID: U9 (Mutated)
-- Description: Combines customers in the 'BUILDING' segment with high account balance and suppliers
-- with a very high account balance.
-- Change: Changed mktsegment to 'AUTOMOBILE'.
-- =================================================================
(select c_name, n_name from customer, nation where
c_mktsegment='AUTOMOBILE' and c_acctbal > 100 and c_nationkey
= n_nationkey) UNION ALL (select s_name, n_name from supplier,
nation where s_acctbal > 4000 and s_nationkey = n_nationkey);

-- =================================================================
-- Query ID: O1 (Mutated)
-- Description: Uses a RIGHT OUTER JOIN to list customers and their nations, counting totals.
-- Change: Changed join type to LEFT JOIN.
-- =================================================================
select c_name, n_name, count(*) as total from nation LEFT OUTER
JOIN customer ON c_nationkey = n_nationkey and c_acctbal < 1000
        GROUP BY c_name,
n_name Order by c_name, n_name desc Limit 10;

-- =================================================================
-- Query ID: O2 (Mutated)
-- Description: Uses a LEFT OUTER JOIN to count low-quantity line items based on ship mode and priority.
-- Change: Changed quantity from < 30 to > 45.
-- =================================================================
SELECT l_shipmode, o_shippriority ,count(*) as low_line_count FROM
lineitem LEFT OUTER JOIN orders ON ( l_orderkey = o_orderkey AND
o_totalprice > 50000 ) WHERE l_linenumber = 4 AND l_quantity > 45
GROUP BY l_shipmode, o_shippriority Order By l_shipmode Limit 5;

-- =================================================================
-- Query ID: O3 (Mutated)
-- Description: Uses a FULL OUTER JOIN to sum customer account balances for finished orders.
-- Change: Changed order status from 'F' to 'O'.
-- =================================================================
SELECT o_custkey as key, sum(c_acctbal), o_clerk, c_name from orders FULL OUTER JOIN customer on c_custkey = o_custkey and
o_orderstatus = 'O' group by o_custkey, o_clerk, c_name order by key
limit 35;

-- =================================================================
-- Query ID: O4 (Mutated)
-- Description: A complex query with multiple OUTER JOINs to retrieve part, supplier, and nation data.
-- Change: Changed part size from > 7 to < 3.
-- =================================================================
SELECT p_size, s_phone, ps_supplycost, n_name FROM part RIGHT
OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size <
3 LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND
s_acctbal < 2000 FULL OUTER JOIN nation on s_nationkey =
n_nationkey and n_regionkey > 3 Order by ps_supplycost asc Limit 50;

-- =================================================================
-- Query ID: O5 (Mutated)
-- Description: Uses a RIGHT OUTER JOIN to get parts with specific size and availability.
-- Change: Changed availqty from > 3350 to < 100.
-- =================================================================
Select ps_suppkey, p_name, p_type from part RIGHT outer join partsupp on p_partkey=ps_partkey and p_size > 4 and ps_availqty < 100 Order By ps_suppkey Limit 100;

-- =================================================================
-- Query ID: O6 (Mutated)
-- Description: Similar to O4, this query uses multiple OUTER JOINs with different ordering.
-- Change: Changed region key from > 3 to = 1.
-- =================================================================
SELECT p_name, s_phone, ps_supplycost, n_name FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7
	LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 FULL OUTER JOIN nation on s_nationkey = n_nationkey
	and n_regionkey = 1 Order By p_name, s_phone, ps_supplycost, n_name desc Limit 20;

-- =================================================================
-- Query ID: A1 (Mutated)
-- Description: An aggregate query counting line items by ship mode based on date and price conditions.
-- Change: Changed date range to 1996.
-- =================================================================
Select l_shipmode, count(*) as cnt From orders, lineitem Where
o_orderkey = l_orderkey and l_commitdate < l_receiptdate and
l_shipdate < l_commitdate and l_receiptdate >= '1996-01-01' and
l_receiptdate < '1997-01-01' and l_extendedprice <= o_totalprice
and l_extendedprice <= 70000 and o_totalprice > 60000 Group By
l_shipmode Order By l_shipmode;

-- =================================================================
-- Query ID: A2 (Mutated)
-- Description: An aggregate query counting orders by priority within a specific date range.
-- Change: Changed operator from <= to >= for commit/receipt date.
-- =================================================================
Select o_orderpriority, count(*) as order_count From orders, lineitem
Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and
o_orderdate < '1993-10-01' and l_commitdate >= l_receiptdate Group
By o_orderpriority Order By o_orderpriority;

-- =================================================================
-- Query ID: A3 (Mutated)
-- Description: Selects line items based on a complex set of date and availability conditions.
-- Change: Changed order date from >= '1990-01-01' to < '1990-01-01'.
-- =================================================================
Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where
o_orderkey = l_orderkey and ps_partkey = l_partkey and ps_suppkey
= l_suppkey and ps_availqty = l_linenumber and l_shipdate >=
o_orderdate and o_orderdate < '1990-01-01' and l_commitdate <=
l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate > '1994-01-01' Order By l_orderkey Limit 7;

-- =================================================================
-- Query ID: A4 (Mutated)
-- Description: Counts the number of waiting line items for each supplier for finished orders.
-- Change: Changed order status from 'F' to 'P'.
-- =================================================================
Select s_name, count(*) as numwait From supplier, lineitem, orders,
nation Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and
o_orderstatus = 'P' and l_receiptdate >= l_commitdate and s_nationkey
= n_nationkey Group By s_name Order By numwait desc Limit 100;

-- =================================================================
-- Query ID: A5 (Mutated)
-- Description: A summary query that calculates various aggregates (sum, avg, count) on line items.
-- Change: Flipped date logic from <= to >=.
-- =================================================================
Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price, sum(l_extendedprice
* (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 -
l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*)
as count_order From lineitem Where l_shipdate >= l_receiptdate and
l_receiptdate >= l_commitdate Group By l_returnflag, l_linestatus
Order by l_returnflag, l_linestatus;

-- =================================================================
-- Query ID: N1 (Mutated)
-- Description: Counts suppliers for parts based on brand, type, and size.
-- Change: Changed part size from >= 4 to = 1.
-- =================================================================
Select p_brand, p_type, p_size, Count(*) as supplier_cnt
 From part, partsupp
 Where p_partkey = ps_partkey
 and p_size = 1 and p_type NOT LIKE 'SMALL PLATED%'  and p_brand <> 'Brand#45'
 Group By p_brand, p_size, p_type
 Order By supplier_cnt desc, p_brand asc, p_type asc, p_size asc;

-- =================================================================
-- Query ID: F1 (Mutated)
-- Description: A UNION ALL query combining high-balance customers from India with suppliers from Argentina.
-- Change: Changed nation from 'INDIA' to 'CHINA'.
-- =================================================================
(SELECT c_name as name, c_acctbal as account_balance FROM orders,
customer, nation WHERE c_custkey = o_custkey and c_nationkey
= n_nationkey and c_mktsegment = 'FURNITURE' and n_name =
'CHINA' and o_orderdate between '1998-01-01' and '1998-12-05' and
o_totalprice <= c_acctbal) UNION ALL (SELECT s_name as name,
s_acctbal as account_balance FROM supplier, lineitem, orders, nation
WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey
and s_nationkey = n_nationkey and n_name = 'ARGENTINA' and
o_orderdate between '1998-01-01' and '1998-01-05' and o_totalprice >
s_acctbal and o_totalprice >= 30000 and 50000 >= s_acctbal Order by account_balance desc limit 20);

-- =================================================================
-- Query ID: F2 (Mutated)
-- Description: Combines order details with supplier details based on various part and date conditions.
-- Change: Changed container type from 'LG CAN' to 'SM JAR'.
-- =================================================================
(Select p_brand, o_clerk, l_shipmode From orders, lineitem, part Where
l_partkey = p_partkey and o_orderkey = l_orderkey and l_shipdate >=
o_orderdate and o_orderdate > '1994-01-01' and l_shipdate > '1995-01-01' and p_retailprice >= l_extendedprice and p_partkey < 10000 and
l_suppkey < 10000 and p_container = 'SM JAR' Order By o_clerk LIMIT
5) UNION ALL (Select p_brand, s_name, l_shipmode From lineitem,
part, supplier Where l_partkey = p_partkey and s_suppkey = l_suppkey
and l_shipdate > '1995-01-01' and s_acctbal >= l_extendedprice and
p_partkey < 15000 and l_suppkey < 14000 and p_container = 'SM JAR'
Order By s_name LIMIT 10);

-- =================================================================
-- Query ID: F3 (Mutated)
-- Description: Combines line items for JUMBO container parts with customers having specific keys.
-- Change: Removed the LEFT JOIN, making it an INNER JOIN, which will change results.
-- =================================================================
(
	select l_orderkey, l_extendedprice as price, p_partkey from lineitem, part
	where l_partkey = p_partkey  and p_container LIKE 'JUMBO%' and p_partkey > 3000 and l_partkey < 3010
	Order by l_orderkey, price desc Limit 100
) union all (select o_orderkey, c_acctbal as price, c_custkey
from customer, orders where c_custkey = o_custkey
 and c_custkey > 1000 and c_custkey < 1010 Order By price desc, o_orderkey, c_custkey Limit 100);

-- =================================================================
-- Query ID: F4 (Mutated)
-- Description: Selects nation names and customer account balances using a LEFT OUTER JOIN with conditions.
-- Change: Changed nationkey range from < 20 to > 20.
-- =================================================================
select n_name, c_acctbal from nation LEFT OUTER JOIN customer
                     ON n_nationkey = c_nationkey and c_nationkey > 3 and n_nationkey > 20 and
                     c_nationkey != 10 LIMIT 200;

-- =================================================================
-- Query ID: MQ1 (Mutated)
-- Description: Pricing Summary Report Query. Provides a summary of line item pricing.
-- Change: Changed interval from 71 to 171 days.
-- =================================================================
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       AVG(l_quantity) AS avg_qty,
       AVG(l_extendedprice) AS avg_price,
       AVG(l_discount) AS avg_disc,
       COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '171 days'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- =================================================================
-- Query ID: MQ2 (Mutated)
-- Description: Minimum Cost Supplier Query. Finds suppliers in a region who can supply a specific part at minimum cost.
-- Change: Changed region from 'MIDDLE EAST' to 'AFRICA'.
-- =================================================================
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 38
  AND p_type LIKE '%TIN'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'AFRICA'
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100;

-- =================================================================
-- Query ID: MQ3 (Mutated)
-- Description: Shipping Priority Query. Lists unshipped orders with the highest revenue.
-- Change: Changed market segment from 'BUILDING' to 'HOUSEHOLD'.
-- =================================================================
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'HOUSEHOLD'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;

-- =================================================================
-- Query ID: MQ4 (Mutated)
-- Description: Order Priority Checking Query. Counts orders by priority for a given quarter.
-- Change: Added 1 year to the date range.
-- =================================================================
SELECT o_orderdate, o_orderpriority, COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= DATE '1998-07-01'
  AND o_orderdate < DATE '1998-07-01' + INTERVAL '3 months'
GROUP BY o_orderdate, o_orderpriority
ORDER BY o_orderpriority
LIMIT 10;

-- =================================================================
-- Query ID: MQ5 (Mutated)
-- Description: Local Supplier Volume Query. Lists revenue volume for suppliers in a specific region.
-- Change: Changed region from 'MIDDLE EAST' to 'EUROPE'.
-- =================================================================
SELECT n_name,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1 year'
GROUP BY n_name
ORDER BY revenue DESC
LIMIT 100;

-- =================================================================
-- Query ID: MQ6 (Mutated)
-- Description: Forecasting Revenue Change Query. Quantifies revenue increase if discounts were eliminated.
-- Change: Changed quantity from < 24 to > 48.
-- =================================================================
SELECT l_shipmode,
       SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 year'
  AND l_quantity > 48
GROUP BY l_shipmode
LIMIT 100;

-- =================================================================
-- Query ID: MQ10 (Mutated)
-- Description: Returned Item Reporting Query. Identifies customers who have returned items.
-- Change: Changed return flag from 'R' to 'A'.
-- =================================================================
SELECT c_name,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '3 months'
  AND l_returnflag = 'A'
  AND c_nationkey = n_nationkey
GROUP BY c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20;

-- =================================================================
-- Query ID: MQ11 (Mutated)
-- Description: Important Stock Identification Query. Finds the most important stock in a given nation.
-- Change: Changed nation from 'ARGENTINA' to 'BRAZIL'.
-- =================================================================
SELECT ps_comment,
       SUM(ps_supplycost * ps_availqty) AS value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'BRAZIL'
GROUP BY ps_comment
ORDER BY value DESC
LIMIT 100;

-- =================================================================
-- Query ID: MQ17 (Mutated)
-- Description: Small-Quantity-Order Revenue Query. Determines average extended price for small-quantity orders.
-- Change: Changed brand from 'Brand#52' to 'Brand#11'.
-- =================================================================
SELECT AVG(l_extendedprice) AS avg_total
FROM lineitem, part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#11'
  AND p_container = 'LG CAN';

-- =================================================================
-- Query ID: MQ18 (Mutated)
-- Description: Large Volume Customer Query. Finds a large volume of orders for customers with a specific phone prefix.
-- Change: Changed phone prefix from '27-%' to '31-%'.
-- =================================================================
SELECT c_name, o_orderdate, o_totalprice, SUM(l_quantity)
FROM customer, orders, lineitem
WHERE c_phone LIKE '31-%'
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, o_orderdate, o_totalprice
ORDER BY o_orderdate, o_totalprice DESC
LIMIT 100;

-- =================================================================
-- Query ID: MQ21 (Mutated)
-- Description: Suppliers Who Kept Orders Waiting Query. Finds suppliers who had line items not shipped on time.
-- Change: Changed nation from 'GERMANY' to 'EGYPT'.
-- =================================================================
SELECT s_name, COUNT(*) AS numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'F'
  AND s_nationkey = n_nationkey
  AND n_name = 'EGYPT'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100;

-- =================================================================
-- Query ID: Alaap (Mutated)
-- Description: A custom query calculating revenue by market segment and shipping priority.
-- Change: Changed extendedprice range.
-- =================================================================
select c_mktsegment,
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority
                         from customer, orders, lineitem
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and
                         o_orderdate <= date '1995-10-13' and l_extendedprice between 5000 and 10000
                        and l_quantity <= 123  group by o_orderdate, o_shippriority, c_mktsegment
                         order by revenue desc, o_orderdate asc, o_shippriority asc;

-- =================================================================
-- Query ID: TPCH_Q9 (Mutated)
-- Description: Product Type Profit Measure Query. Computes profit for all parts with a certain name substring.
-- Change: Changed LIKE pattern from 'co%' to 'gr%'.
-- =================================================================
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like 'gr%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;

-- =================================================================
-- Query ID: TPCH_Q11 (Mutated)
-- Description: Important Stock Identification Query. Finds parts from a nation that represent a significant
-- fraction of the total stock value.
-- Change: Changed multiplier from 0.00001 to 0.0001.
-- =================================================================
SELECT
    ps_partkey, n_name,
    SUM(ps_supplycost * ps_availqty) AS total_value
FROM
    partsupp, supplier, nation
where
    ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'INDIA'
GROUP BY
    ps_partkey, n_name
HAVING
    SUM(ps_supplycost * ps_availqty) > (
        SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
        FROM partsupp, supplier, nation WHERE
        ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'INDIA'
    )
ORDER BY
    total_value DESC;

-- =================================================================
-- Query ID: TPCH_Q12 (Mutated)
-- Description: Shipping Modes and Order Priority Query. Determines if late shipping is related to order priority.
-- Change: Changed shipmode from 'SHIP' to 'MAIL'.
-- =================================================================
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode = 'MAIL'
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1995-01-01'
	and l_receiptdate < date '1995-01-01' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode;

-- =================================================================
-- Query ID: TPCH_Q13 (Mutated)
-- Description: Customer Distribution Query. Counts customers by the number of orders they have placed.
-- Change: Changed comment pattern from '%among%regular%' to '%special%requests%'.
-- =================================================================
select
	c_count, c_orderdate,
	count(*) as custdist
from
	(
		select
			c_custkey, o_orderdate,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%special%requests%'
		group by
			c_custkey, o_orderdate
	) as c_orders (c_custkey, c_count, c_orderdate)
group by
	c_count, c_orderdate
order by
	custdist desc,
	c_count desc;

-- =================================================================
-- Query ID: TPCH_Q14 (Mutated)
-- Description: Promotion Effect Query. Monitors market share of promotional parts.
-- Change: Changed date interval from 1 month to 6 months.
-- =================================================================
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1995-01-01'
	and l_shipdate < date '1995-01-01' + interval '6' month;

-- =================================================================
-- Query ID: ETPCH_Q15 (Mutated)
-- Description: Top Supplier Query. Finds the supplier with the maximum total revenue for a given period.
-- Change: Changed MAX to MIN to find the supplier with the least revenue.
-- =================================================================
with revenue as (
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
        lineitem
    where
        l_shipdate >= date '1995-01-01'
        and l_shipdate < date '1995-01-01' + interval '1' month
    group by
        l_suppkey
)
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier s,
    revenue r
where
    s_suppkey = r.supplier_no
and total_revenue = (
    select
        min(total_revenue)
    from
        revenue
)
order by
    s_suppkey;

-- =================================================================
-- Query ID: TPCH_Q16 (Mutated)
-- Description: Parts/Supplier Relationship Query. Counts suppliers for parts matching certain criteria,
-- excluding suppliers with complaints.
-- Change: Changed brand from '<> Brand#23' to '= Brand#23'.
-- =================================================================
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand = 'Brand#23'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
	and p_size IN (1, 4, 7)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;

-- =================================================================
-- Query ID: TPCH_Q17 (Mutated)
-- Description: Small-Quantity-Order Revenue Query. Calculates average yearly revenue for parts with low quantity.
-- Change: Changed container from 'MED BAG' to 'LG BOX'.
-- =================================================================
SELECT SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem l
JOIN part p ON p.p_partkey = l.l_partkey
JOIN (
    SELECT l_partkey, 0.7 * AVG(l_quantity) AS threshold_quantity
    FROM lineitem
    GROUP BY l_partkey
) AS avg_lineitem ON avg_lineitem.l_partkey = l.l_partkey
WHERE p.p_brand = 'Brand#53'
  AND p.p_container = 'LG BOX'
  AND l.l_quantity < avg_lineitem.threshold_quantity;

-- =================================================================
-- Query ID: ETPCH_Q18 (Mutated)
-- Description: Large Volume Customer Query. Finds top customers who have placed a large volume of orders.
-- Change: Changed quantity from > 300 to < 10.
-- =================================================================
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem l
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) < 10
    )
    and c_custkey = o_custkey
    and o_orderkey = l.l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate;

-- =================================================================
-- Query ID: Nested_Test (Mutated)
-- Description: A sample nested query to find suppliers from France for 'ivory' parts.
-- Change: Changed subquery from MIN to MAX.
-- =================================================================
select
        s_name,
        s_address
from
        supplier,
        nation,
		partsupp,
		part
where
        s_suppkey = ps_suppkey
        and ps_partkey = p_partkey
        and p_name like '%ivory%'
		and s_nationkey = n_nationkey
        and n_name = 'FRANCE'
        and ps_availqty > (select max(c_acctbal) from customer)
order by
        s_name;

-- =================================================================
-- Query ID: paper_sample (Mutated)
-- Description: A custom UNION ALL query combining high-value customers with suppliers based on average price.
-- Change: Changed mktsegment to 'FURNITURE'.
-- =================================================================
(Select c_name as entity_name, n_name as country, o_totalprice as price
From orders JOIN customer ON c_custkey = o_custkey
and o_totalprice <= c_acctbal and c_acctbal >= 9000
JOIN nation ON c_nationkey = n_nationkey Where o_totalprice <= 15000
and c_mktsegment IN ('FURNITURE')
Group By c_name, n_name, o_totalprice
Order By price asc, country asc, entity_name)
UNION ALL
(Select s_name as entity_name, n_name as country,
Avg(l_extendedprice*(1 - l_discount)) as price
From lineitem, nation, orders, region, supplier
Where l_suppkey = s_suppkey and n_nationkey = s_nationkey and
l_orderkey = o_orderkey and n_regionkey = r_regionkey and s_acctbal
<= o_totalprice and o_totalprice <= 15000
Group By n_name, s_name
Order By price desc, country desc, entity_name);

-- =================================================================
-- Query ID: ETPCH_Q1 (Mutated)
-- Description: Simplified version of ETPCH Q1, using the standard lineitem table.
-- Change: Changed ship date to be after the specified date instead of before.
-- =================================================================
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS total_qty,
    AVG(l_extendedprice) AS avg_price,
    COUNT(*) AS num_orders
FROM
    lineitem
WHERE
    l_shipdate > date '1998-12-01' - interval '3 day'
GROUP BY
    l_returnflag, l_linestatus
ORDER BY
    l_returnflag, l_linestatus;

-- =================================================================
-- Query ID: ETPCH_Q3 (Mutated)
-- Description: Simplified version of ETPCH Q3, using the standard lineitem table for shipping priority analysis.
-- Change: Changed market segment to 'AUTOMOBILE'.
-- =================================================================
SELECT
    L.l_orderkey,
    SUM(L.l_extendedprice * (1 - L.l_discount)) AS revenue,
    O.o_orderdate,
    O.o_shippriority
FROM
    customer C, orders O, lineitem L
WHERE
    C.c_mktsegment = 'AUTOMOBILE'
    AND C.c_custkey = O.o_custkey
    AND L.l_orderkey = O.o_orderkey
    AND O.o_orderdate < '1995-01-01'
    AND L.l_shipdate > '1995-01-01'
GROUP BY
    L.l_orderkey, O.o_orderdate, O.o_shippriority
ORDER BY
    revenue DESC
LIMIT 10;

-- =================================================================
-- Query ID: ETPCH_Q4 (Mutated)
-- Description: Simplified version of ETPCH Q4, using the standard lineitem table for order priority checking.
-- Change: Flipped date logic from < to >.
-- =================================================================
SELECT
    o.o_orderpriority,
    COUNT(DISTINCT o.o_orderkey) AS order_count
FROM
    orders o, lineitem l
WHERE
    o.o_orderkey = l.l_orderkey
    AND l.l_commitdate > l.l_receiptdate
    AND o.o_orderdate >= '1995-01-01' AND o.o_orderdate <= '1995-03-31'
GROUP BY
    o.o_orderpriority
ORDER BY
    o.o_orderpriority ASC;

-- =================================================================
-- Query ID: ETPCH_Q5 (Mutated)
-- Description: Simplified version of ETPCH Q5, using standard tables for local supplier volume.
-- Change: Changed region from 'ASIA' to 'AMERICA'.
-- =================================================================
SELECT
    n.n_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    customer c
JOIN
    orders o ON c.c_custkey = o.o_custkey
JOIN
    lineitem l ON o.o_orderkey = l.l_orderkey
JOIN
    supplier s ON l.l_suppkey = s.s_suppkey AND c.c_nationkey = s.s_nationkey
JOIN
    nation n ON s.s_nationkey = n.n_nationkey
JOIN
    region r ON n.n_regionkey = r.r_regionkey
WHERE
    r.r_name = 'AMERICA'
    AND o.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31'
GROUP BY
    n.n_name
ORDER BY
    revenue DESC, n.n_name ASC;

-- =================================================================
-- Query ID: ETPCH_Q6 (Mutated)
-- Description: Simplified version of ETPCH Q6, using the standard lineitem table for forecasting revenue change.
-- Change: Flipped discount range.
-- =================================================================
SELECT
    SUM(l_extendedprice * l_discount)
FROM
    lineitem
WHERE
    l_shipdate >= '1993-01-01'
    AND l_shipdate < '1995-01-01'
    AND l_discount NOT BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;

-- =================================================================
-- Query ID: ETPCH_Q6_1 (Mutated)
-- Description: A variation of ETPCH_Q6 focusing only on the standard lineitem table.
-- Change: Flipped quantity from < 24 to > 40.
-- =================================================================
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate < '1995-01-01'
    AND l_shipdate >= '1993-01-01'
    AND l_quantity > 40;

-- =================================================================
-- Query ID: ETPCH_Q6_2 (Mutated)
-- Description: Another variation of ETPCH_Q6, summing revenue from the standard lineitem table.
-- Change: Changed aggregate from SUM to AVG.
-- =================================================================
SELECT
    AVG(l_extendedprice) AS revenue
FROM
    lineitem
WHERE
    l_quantity < 24
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_shipdate >= '1993-01-01'
    AND l_shipdate < '1995-01-01';

-- =================================================================
-- Query ID: ETPCH_Q7 (Mutated)
-- Description: Simplified version of ETPCH Q7, analyzing trade volume between two nations using standard tables.
-- Change: Changed nations from FRANCE/GERMANY to CANADA/BRAZIL.
-- =================================================================
SELECT
    supp_nation.n_name AS supp_nation,
    cust_nation.n_name AS cust_nation,
    EXTRACT(YEAR FROM l.l_shipdate) AS l_year,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    lineitem l
JOIN
    supplier s ON l.l_suppkey = s.s_suppkey
JOIN
    nation supp_nation ON s.s_nationkey = supp_nation.n_nationkey
JOIN
    orders o ON l.l_orderkey = o.o_orderkey
JOIN
    customer c ON o.o_custkey = c.c_custkey
JOIN
    nation cust_nation ON c.c_nationkey = cust_nation.n_nationkey
WHERE
    l.l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
    AND ((supp_nation.n_name = 'CANADA' AND cust_nation.n_name = 'BRAZIL')
         OR (supp_nation.n_name = 'BRAZIL' AND cust_nation.n_name = 'CANADA'))
GROUP BY
    supp_nation.n_name, cust_nation.n_name, l_year
ORDER BY
    supp_nation.n_name, cust_nation.n_name, l_year;

-- =================================================================
-- Query ID: ETPCH_Q9 (Mutated)
-- Description: Simplified version of ETPCH Q9, calculating profit by nation and year using standard tables.
-- Change: Changed part name pattern from '%co%' to '%tin%'.
-- =================================================================
SELECT
    n.n_name AS nation,
    EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
    SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit
FROM
    part p,
    supplier s,
    lineitem l,
    partsupp ps,
    orders o,
    nation n
WHERE
    s.s_suppkey = l.l_suppkey
    AND ps.ps_suppkey = l.l_suppkey
    AND ps.ps_partkey = l.l_partkey
    AND p.p_partkey = l.l_partkey
    AND o.o_orderkey = l.l_orderkey
    AND s.s_nationkey = n.n_nationkey
    AND p.p_name LIKE '%tin%'
GROUP BY
    n.n_name, o_year
ORDER BY
    nation ASC, o_year DESC;

-- =================================================================
-- Query ID: ETPCH_Q10 (Mutated)
-- Description: Simplified version of ETPCH Q10, reporting on returned items using the standard lineitem table.
-- Change: Changed return flag from 'R' to 'N'.
-- =================================================================
SELECT
    c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    c.c_acctbal, n.n_name, c.c_address, c.c_phone, c.c_comment
FROM
    customer c, orders o, lineitem l, nation n
WHERE
    c.c_custkey = o.o_custkey
    AND o.o_orderkey = l.l_orderkey
    AND c.c_nationkey = n.n_nationkey
    AND l.l_returnflag = 'N'
    AND o.o_orderdate >= '1995-01-01' AND o.o_orderdate < '1995-04-01'
GROUP BY
    c.c_custkey, c.c_name, c.c_acctbal, n.n_name, c.c_address, c.c_phone, c.c_comment
ORDER BY
    revenue DESC
LIMIT 20;

-- =================================================================
-- Query ID: ETPCH_Q12 (Mutated)
-- Description: Simplified ETPCH Q12, analyzing shipping modes and order priority using the standard lineitem table.
-- Change: Changed shipmode list to ('FOB', 'REG AIR').
-- =================================================================
SELECT
    l.l_shipmode,
    COUNT(CASE WHEN o.o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 END) AS high_line_count,
    COUNT(CASE WHEN o.o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 END) AS low_line_count
FROM
    orders o, lineitem l
WHERE
    o.o_orderkey = l.l_orderkey
    AND l.l_shipmode IN ('FOB', 'REG AIR')
    AND l.l_commitdate < l.l_receiptdate
    AND l.l_shipdate < l.l_commitdate
    AND l.l_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
GROUP BY
    l.l_shipmode
ORDER BY
    l.l_shipmode;

-- =================================================================
-- Query ID: ETPCH_Q14 (Mutated)
-- Description: Simplified ETPCH Q14, calculating promotion effect using the standard lineitem table.
-- Change: Changed promo pattern from 'PROMO%' to '%STANDARD%'.
-- =================================================================
WITH PromoData AS (
    SELECT
        p.p_type,
        l.l_extendedprice,
        l.l_discount
    FROM
        lineitem l, part p
    WHERE
        l.l_partkey = p.p_partkey
        AND l.l_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
)
SELECT
    100.00 * SUM(CASE WHEN p_type LIKE '%STANDARD%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) /
    SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM PromoData;

-- =================================================================
-- Query ID: ETPCH_Q13 (Mutated)
-- Description: Extended TPC-H Q13, analyzing customer distribution by order count.
-- Change: Changed comment logic from NOT LIKE to LIKE.
-- =================================================================
SELECT
    c_orders.c_count,
    c_orders.o_orderdate,
    count(*) AS custdist
FROM (
    SELECT
        c_custkey,
        o_orderdate,
        count(o_orderkey) AS c_count
    FROM
        customer
    LEFT OUTER JOIN
        orders ON c_custkey = o_custkey AND o_comment LIKE '%special%requests%'
    GROUP BY
        c_custkey, o_orderdate
) AS c_orders
GROUP BY
    c_orders.c_count, c_orders.o_orderdate
ORDER BY
    custdist DESC, c_count DESC;

-- =================================================================
-- Query ID: ETPCH_Q2 (Mutated)
-- Description: Extended TPC-H Q2, finding the minimum cost supplier for a part in Europe.
-- Change: Changed part size from 15 to 45.
-- =================================================================
SELECT
    s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment
FROM
    part p, supplier s, partsupp ps, nation n, region r
WHERE
    p.p_partkey = ps.ps_partkey
    AND s.s_suppkey = ps.ps_suppkey
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND p.p_size = 45
    AND p.p_type LIKE '%BRASS'
    AND r.r_name = 'EUROPE'
    AND ps.ps_supplycost = (
        SELECT MIN(ps_min.ps_supplycost)
        FROM partsupp ps_min
        JOIN supplier s_min ON s_min.s_suppkey = ps_min.ps_suppkey
        JOIN nation n_min ON s_min.s_nationkey = n_min.n_nationkey
        JOIN region r_min ON n_min.n_regionkey = r_min.r_regionkey
        WHERE r_min.r_name = 'EUROPE' AND ps_min.ps_partkey = p.p_partkey
    )
ORDER BY
    s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey
LIMIT 100;

-- =================================================================
-- Query ID: ETPCH_Q22 (Mutated)
-- Description: Simplified ETPCH Q22, Global Sales Opportunity Query, using the standard customer table.
-- Change: Changed the list of country codes.
-- =================================================================
WITH AvgBalance AS (
    SELECT AVG(c_acctbal) as avg_bal
    FROM customer
    WHERE c_acctbal > 0.00
      AND SUBSTRING(c_phone, 1, 2) IN ('10', '20', '30')
),
FilteredCustomers AS (
    SELECT
        SUBSTRING(c.c_phone, 1, 2) AS cntrycode,
        c.c_acctbal
    FROM
        customer c, AvgBalance ab
    WHERE
        SUBSTRING(c.c_phone, 1, 2) IN ('10', '20', '30')
        AND c.c_acctbal > ab.avg_bal
        AND NOT EXISTS (
            SELECT 1
            FROM orders o
            WHERE o.o_custkey = c.c_custkey
        )
)
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM FilteredCustomers
GROUP BY cntrycode
ORDER BY cntrycode;

-- =================================================================
-- Query ID: ETPCH_Q8 (Mutated)
-- Description: Simplified ETPCH Q8, calculating market share using standard tables.
-- Change: Changed part type from 'ECONOMY ANODIZED STEEL' to 'LARGE PLATED TIN'.
-- =================================================================
WITH MarketData AS (
    SELECT
        EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
        l.l_extendedprice * (1 - l.l_discount) AS volume,
        n2.n_name AS nation
    FROM
        part p, supplier s, lineitem l, orders o, customer c, nation n1, nation n2, region r
    WHERE
        p.p_partkey = l.l_partkey
        AND s.s_suppkey = l.l_suppkey
        AND l.l_orderkey = o.o_orderkey
        AND o.o_custkey = c.c_custkey
        AND c.c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r.r_regionkey
        AND s.s_nationkey = n2.n_nationkey
        AND r.r_name = 'ASIA'
        AND p.p_type = 'LARGE PLATED TIN'
        AND o.o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
)
SELECT
    o_year,
    SUM(CASE WHEN nation = 'INDIA' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
FROM MarketData
GROUP BY o_year
ORDER BY o_year;

-- =================================================================
-- Query ID: ETPCH_Q11 (Mutated)
-- Description: Extended TPC-H Q11, identifying important stock in India.
-- Change: Changed nation from 'INDIA' to 'GERMANY'.
-- =================================================================
SELECT
    ps.ps_partkey,
    n.n_name,
    SUM(ps.ps_supplycost * ps.ps_availqty) AS total_value
FROM
    partsupp ps, supplier s, nation n
WHERE
    ps.ps_suppkey = s.s_suppkey
    AND s.s_nationkey = n.n_nationkey
    AND n.n_name = 'GERMANY'
GROUP BY
    ps.ps_partkey, n.n_name
HAVING
    SUM(ps.ps_supplycost * ps.ps_availqty) >
    (SELECT SUM(ps_inner.ps_supplycost * ps_inner.ps_availqty) * 0.00001
     FROM partsupp ps_inner, supplier s_inner, nation n_inner
     WHERE ps_inner.ps_suppkey = s_inner.s_suppkey
       AND s_inner.s_nationkey = n_inner.n_nationkey
       AND n_inner.n_name = 'GERMANY')
ORDER BY total_value DESC;

-- =================================================================
-- Query ID: ETPCH_Q16 (Mutated)
-- Description: Extended TPC-H Q16, analyzing parts/supplier relationships.
-- Change: Removed the NOT IN clause, broadening the supplier pool.
-- =================================================================
SELECT
    p.p_brand, p.p_type, p.p_size, COUNT(DISTINCT ps.ps_suppkey) AS supplier_cnt
FROM
    part p, partsupp ps
WHERE
    p.p_partkey = ps.ps_partkey
    AND p.p_brand <> 'Brand#23'
    AND p.p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p.p_size IN (1, 4, 7)
GROUP BY
    p.p_brand, p.p_type, p.p_size
ORDER BY
    supplier_cnt DESC, p.p_brand, p.p_type, p.p_size;

-- =================================================================
-- Query ID: ETPCH_Q17 (Mutated)
-- Description: Simplified ETPCH Q17, calculating revenue for small-quantity orders using the standard lineitem table.
-- Change: Changed the multiplier from 0.7 to 0.2.
-- =================================================================
WITH PartAvgQuantity AS (
    SELECT
        l_partkey,
        0.2 * AVG(l_quantity) AS avg_q
    FROM
        lineitem
    GROUP BY
        l_partkey
)
SELECT
    SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem l
JOIN
    part p ON p.p_partkey = l.l_partkey
JOIN
    PartAvgQuantity paq ON l.l_partkey = paq.l_partkey
WHERE
    p.p_brand = 'Brand#53'
    AND p.p_container = 'MED BAG'
    AND l.l_quantity < paq.avg_q;

-- =================================================================
-- Query ID: ETPCH_Q19 (Mutated)
-- Description: Simplified ETPCH Q19, Discounted Revenue Query using the standard lineitem table.
-- Change: Changed 'DELIVER IN PERSON' to 'TAKE BACK RETURN'.
-- =================================================================
SELECT
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    lineitem l, part p
WHERE
    l.l_partkey = p.p_partkey
    AND l.l_shipmode IN ('AIR', 'AIR REG')
    AND l.l_shipinstruct = 'TAKE BACK RETURN'
    AND (
        (p.p_brand = 'Brand#12' AND p.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l.l_quantity BETWEEN 1 AND 11 AND p.p_size BETWEEN 1 AND 5)
     OR (p.p_brand = 'Brand#23' AND p.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l.l_quantity BETWEEN 10 AND 20 AND p.p_size BETWEEN 1 AND 10)
     OR (p.p_brand = 'Brand#34' AND p.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l.l_quantity BETWEEN 20 AND 30 AND p.p_size BETWEEN 1 AND 15)
    );

-- =================================================================
-- Query ID: ETPCH_Q20 (Mutated)
-- Description: Simplified ETPCH Q20, Potential Part Promotion Query using the standard lineitem table.
-- Change: Changed part name pattern from '%ivory%' to '%steel%'.
-- =================================================================
WITH LineitemQuantity AS (
    SELECT
        l_partkey,
        l_suppkey,
        0.5 * SUM(l_quantity) AS half_sum_qty
    FROM
        lineitem
    WHERE
        l_shipdate >= '1995-01-01'
        AND l_shipdate < ('1995-01-01'::date + interval '1 year')
    GROUP BY
        l_partkey, l_suppkey
)
SELECT
    s.s_name,
    s.s_address
FROM
    supplier s
JOIN
    nation n ON s.s_nationkey = n.n_nationkey
JOIN
    partsupp ps ON s.s_suppkey = ps.ps_suppkey
JOIN
    part p ON ps.ps_partkey = p.p_partkey
JOIN
    LineitemQuantity lq ON ps.ps_partkey = lq.l_partkey AND ps.ps_suppkey = lq.l_suppkey
WHERE
    n.n_name = 'FRANCE'
    AND p.p_name LIKE '%steel%'
    AND ps.ps_availqty > lq.half_sum_qty
ORDER BY
    s.s_name;

-- =================================================================
-- Query ID: ETPCH_Q21 (Mutated)
-- Description: Simplified ETPCH Q21, finding suppliers in Argentina who kept orders waiting, using standard tables.
-- Change: Flipped EXISTS to NOT EXISTS.
-- =================================================================
SELECT
    s.s_name,
    COUNT(*) AS numwait
FROM
    supplier s
JOIN
    lineitem l1 ON s.s_suppkey = l1.l_suppkey
JOIN
    orders o ON l1.l_orderkey = o.o_orderkey
JOIN
    nation n ON s.s_nationkey = n.n_nationkey
WHERE
    o.o_orderstatus = 'F'
    AND n.n_name = 'ARGENTINA'
    AND l1.l_receiptdate > l1.l_commitdate
    AND NOT EXISTS (
        SELECT 1 FROM lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT 1 FROM lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate
    )
GROUP BY
    s.s_name
ORDER BY
    numwait DESC, s.s_name;

-- =================================================================
-- Query ID: ETPCH_Q23 (Mutated)
-- Description: A simplified version of a complex query analyzing customer returns using standard tables.
-- Change: Changed return flag from 'R' to 'A'.
-- =================================================================
SELECT
    SUBSTRING(c.c_address, LENGTH(c.c_address) - 4) AS city,
    p.p_brand
FROM
    customer c, orders o, lineitem l, part p
WHERE
    c.c_custkey = o.o_custkey
    AND o.o_orderkey = l.l_orderkey
    AND l.l_partkey = p.p_partkey
    AND l.l_returnflag = 'A'
    AND EXTRACT(YEAR FROM o.o_orderdate) = 1995
GROUP BY
    city, p.p_brand
ORDER BY
    city, p.p_brand;

-- =================================================================
-- Query ID: ETPCH_Q24 (Mutated)
-- Description: A simplified version of a complex query analyzing returns and supplier availability.
-- Change: Changed return flag from 'R' to 'N'.
-- =================================================================
SELECT DISTINCT
    c.c_address AS city
FROM
    customer c
JOIN
    orders o ON c.c_custkey = o.o_custkey
JOIN
    lineitem l ON o.o_orderkey = l.l_orderkey
WHERE
    l.l_returnflag = 'N'
    AND o.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31';
