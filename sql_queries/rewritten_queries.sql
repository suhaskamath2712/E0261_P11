-- =================================================================
-- Query ID: U1 (Rewritten)
-- Description: A UNION ALL query combining top parts by available quantity
-- with top suppliers by available quantity.
-- =================================================================
(SELECT
    S.s_name,
    S.s_suppkey
FROM
    supplier AS S
INNER JOIN
    partsupp AS PS ON S.s_suppkey = PS.ps_suppkey
WHERE
    PS.ps_availqty > 200
ORDER BY
    S.s_suppkey
LIMIT 7)
UNION ALL
(SELECT
    P.p_name,
    P.p_partkey
FROM
    part AS P
INNER JOIN
    partsupp AS PS ON P.p_partkey = PS.ps_partkey
WHERE
    PS.ps_availqty > 100
ORDER BY
    P.p_partkey
LIMIT 5);

-- =================================================================
-- Query ID: U2 (Rewritten)
-- Description: Combines top suppliers from Germany with top urgent priority customers.
-- =================================================================
(SELECT
    C.c_name,
    C.c_custkey
FROM
    customer AS C,
    orders AS O
WHERE
    O.o_custkey = C.c_custkey
    AND O.o_orderpriority = '1-URGENT'
ORDER BY
    C.c_custkey, C.c_name DESC
LIMIT 10)
UNION ALL
(SELECT
    S.s_name,
    S.s_suppkey
FROM
    supplier AS S,
    nation AS N
WHERE
    S.s_nationkey = N.n_nationkey
    AND N.n_name = 'GERMANY'
ORDER BY
    S.s_suppkey DESC, S.s_name
LIMIT 12);

-- =================================================================
-- Query ID: U3 (Rewritten)
-- Description: A three-part UNION ALL query fetching customers from the USA, parts with high quantity,
-- and nations starting with 'B'.
-- =================================================================
(SELECT
    part.p_partkey AS item_key,
    part.p_name AS item_name
FROM
    part
INNER JOIN
    lineitem ON part.p_partkey = lineitem.l_partkey
WHERE
    lineitem.l_quantity > 35
ORDER BY
    item_key
LIMIT 10)
UNION ALL
(SELECT
    customer.c_custkey AS item_key,
    customer.c_name AS item_name
FROM
    customer
INNER JOIN
    nation ON customer.c_nationkey = nation.n_nationkey
WHERE
    nation.n_name = 'UNITED STATES'
ORDER BY
    item_key
LIMIT 10)
UNION ALL
(SELECT
    N.n_nationkey AS item_key,
    R.r_name AS item_name
FROM
    nation AS N,
    region AS R
WHERE
    N.n_name LIKE 'B%'
ORDER BY
    item_key
LIMIT 5);

-- =================================================================
-- Query ID: U4 (Rewritten)
-- Description: A four-part UNION ALL query combining customers from the USA, suppliers from Canada,
-- parts with high quantity, and parts with high supply cost.
-- =================================================================
(SELECT
    part.p_partkey,
    part.p_name
FROM
    part,
    lineitem
WHERE
    part.p_partkey = lineitem.l_partkey
    AND lineitem.l_quantity > 20
ORDER BY
    part.p_partkey DESC
LIMIT 7)
UNION ALL
(SELECT
    supplier.s_suppkey,
    supplier.s_name
FROM
    supplier,
    nation
WHERE
    supplier.s_nationkey = nation.n_nationkey
    AND nation.n_name = 'CANADA'
ORDER BY
    supplier.s_suppkey
LIMIT 6)
UNION ALL
(SELECT
    customer.c_custkey,
    customer.c_name
FROM
    customer,
    nation
WHERE
    customer.c_nationkey = nation.n_nationkey
    AND nation.n_name = 'UNITED STATES'
ORDER BY
    customer.c_custkey DESC
LIMIT 5)
UNION ALL
(SELECT
    partsupp.ps_partkey,
    part.p_name
FROM
    part,
    partsupp
WHERE
    part.p_partkey = partsupp.ps_partkey
    AND partsupp.ps_supplycost >= 1000
ORDER BY
    partsupp.ps_partkey
LIMIT 8);

-- =================================================================
-- Query ID: U5 (Rewritten)
-- Description: Combines recent orders for a specific customer with older line items that have
-- high quantity and price.
-- =================================================================
(SELECT
    L.l_orderkey,
    L.l_shipdate,
    O.o_orderstatus
FROM
    lineitem AS L
JOIN
    orders AS O ON L.l_orderkey = O.o_orderkey
WHERE
    O.o_orderdate < '1994-01-01'
    AND L.l_quantity > 20
    AND L.l_extendedprice > 1000
ORDER BY
    L.l_orderkey
LIMIT 5)
UNION ALL
(SELECT
    O.o_orderkey,
    O.o_orderdate,
    N.n_name
FROM
    orders AS O,
    customer AS C,
    nation AS N
WHERE
    O.o_custkey = C.c_custkey
    AND C.c_nationkey = N.n_nationkey
    AND C.c_name LIKE '%0001248%'
    AND O.o_orderdate >= '1997-01-01'
ORDER BY
    O.o_orderkey
LIMIT 20);

-- =================================================================
-- Query ID: U6 (Rewritten)
-- Description: Combines top clerks by total price on older orders with total account balance for
-- nations with 'UNITED' in their name.
-- =================================================================
(SELECT
    N.n_name AS identifier,
    SUM(S.s_acctbal) AS total
FROM
    nation AS N,
    supplier AS S
WHERE
    N.n_nationkey = S.s_nationkey
    AND N.n_name LIKE '%UNITED%'
GROUP BY
    N.n_name
ORDER BY
    N.n_name DESC
LIMIT 10)
UNION ALL
(SELECT
    O.o_clerk AS identifier,
    SUM(L.l_extendedprice) AS total
FROM
    orders AS O,
    lineitem AS L
WHERE
    O.o_orderkey = L.l_orderkey
    AND O.o_orderdate <= '1995-01-01'
GROUP BY
    O.o_clerk
ORDER BY
    total DESC
LIMIT 10);

-- =================================================================
-- Query ID: U7 (Rewritten)
-- Description: Combines line items from a specific date range and quantity with parts having a low supply cost.
-- =================================================================
WITH LowCostParts AS (
    SELECT
        ps_partkey AS key,
        p_retailprice AS price,
        ps_suppkey AS s_key
    FROM
        partsupp
    JOIN
        supplier ON ps_suppkey = s_suppkey
    JOIN
        part ON ps_partkey = p_partkey
    WHERE
        ps_supplycost < 100
    ORDER BY
        price
    LIMIT 20
),
RecentLineItems AS (
    SELECT
        l_orderkey AS key,
        l_extendedprice AS price,
        l_partkey AS s_key
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '1994-01-01'
        AND l_shipdate < DATE '1995-01-01'
        AND l_quantity > 30
    ORDER BY
        key
    LIMIT 20
)
SELECT key, price, s_key FROM RecentLineItems
UNION ALL
SELECT key, price, s_key FROM LowCostParts;

-- =================================================================
-- Query ID: U8 (Rewritten)
-- Description: Combines customers with the fewest recent orders and line items with the highest average quantity.
-- =================================================================
(SELECT
    L.l_orderkey AS entity_id,
    AVG(L.l_quantity) AS metric
FROM
    orders AS O
INNER JOIN
    lineitem AS L ON O.o_orderkey = L.l_orderkey
WHERE
    O.o_orderdate < '1996-07-01'
GROUP BY
    L.l_orderkey
ORDER BY
    metric DESC
LIMIT 10)
UNION ALL
(SELECT
    C.c_custkey AS entity_id,
    COUNT(*) AS metric
FROM
    customer AS C
INNER JOIN
    orders AS O ON C.c_custkey = O.o_custkey
WHERE
    O.o_orderdate >= '1995-01-01'
GROUP BY
    C.c_custkey
ORDER BY
    metric ASC
LIMIT 10);

-- =================================================================
-- Query ID: U9 (Rewritten)
-- Description: Combines customers in the 'BUILDING' segment with high account balance and suppliers
-- with a very high account balance.
-- =================================================================
(SELECT
    S.s_name,
    N.n_name
FROM
    supplier AS S,
    nation AS N
WHERE
    S.s_acctbal > 4000
    AND S.s_nationkey = N.n_nationkey)
UNION ALL
(SELECT
    C.c_name,
    N.n_name
FROM
    customer AS C,
    nation AS N
WHERE
    C.c_mktsegment = 'BUILDING'
    AND C.c_acctbal > 100
    AND C.c_nationkey = N.n_nationkey);

-- =================================================================
-- Query ID: O1 (Rewritten)
-- Description: Uses a RIGHT OUTER JOIN to list customers and their nations, counting totals.
-- =================================================================
SELECT
    cust.c_name,
    nat.n_name,
    count(*) AS total_count
FROM
    nation AS nat
RIGHT JOIN
    customer AS cust ON cust.c_nationkey = nat.n_nationkey
WHERE
    cust.c_acctbal < 1000
GROUP BY
    cust.c_name,
    nat.n_name
ORDER BY
    cust.c_name ASC,
    nat.n_name DESC
LIMIT 10;

-- =================================================================
-- Query ID: O2 (Rewritten)
-- Description: Uses a LEFT OUTER JOIN to count low-quantity line items based on ship mode and priority.
-- =================================================================
SELECT
    L.l_shipmode,
    O.o_shippriority,
    count(*) AS low_line_count
FROM
    lineitem AS L
LEFT JOIN
    orders AS O ON L.l_orderkey = O.o_orderkey
WHERE
    L.l_linenumber = 4
    AND L.l_quantity < 30
    AND O.o_totalprice > 50000
GROUP BY
    L.l_shipmode,
    O.o_shippriority
ORDER BY
    L.l_shipmode
LIMIT 5;

-- =================================================================
-- Query ID: O3 (Rewritten)
-- Description: Uses a FULL OUTER JOIN to sum customer account balances for finished orders.
-- =================================================================
SELECT
    O.o_custkey AS customer_key,
    SUM(C.c_acctbal) AS balance_sum,
    O.o_clerk,
    C.c_name
FROM
    orders AS O
FULL OUTER JOIN
    customer AS C ON C.c_custkey = O.o_custkey
WHERE
    O.o_orderstatus = 'F'
GROUP BY
    O.o_custkey,
    O.o_clerk,
    C.c_name
ORDER BY
    customer_key
LIMIT 35;

-- =================================================================
-- Query ID: O4 (Rewritten)
-- Description: A complex query with multiple OUTER JOINs to retrieve part, supplier, and nation data.
-- =================================================================
SELECT
    P.p_size,
    S.s_phone,
    PS.ps_supplycost,
    N.n_name
FROM
    part AS P
RIGHT JOIN
    partsupp AS PS ON P.p_partkey = PS.ps_partkey AND P.p_size > 7
LEFT JOIN
    supplier AS S ON PS.ps_suppkey = S.s_suppkey AND S.s_acctbal < 2000
FULL JOIN
    nation AS N ON S.s_nationkey = N.n_nationkey AND N.n_regionkey > 3
ORDER BY
    PS.ps_supplycost ASC
LIMIT 50;

-- =================================================================
-- Query ID: O5 (Rewritten)
-- Description: Uses a RIGHT OUTER JOIN to get parts with specific size and availability.
-- =================================================================
SELECT
    ps.ps_suppkey,
    p.p_name,
    p.p_type
FROM
    part AS p
RIGHT JOIN
    partsupp AS ps ON p.p_partkey = ps.ps_partkey
WHERE
    p.p_size > 4 AND ps.ps_availqty > 3350
ORDER BY
    ps.ps_suppkey
LIMIT 100;

-- =================================================================
-- Query ID: O6 (Rewritten)
-- Description: Similar to O4, this query uses multiple OUTER JOINs with different ordering.
-- =================================================================
SELECT
    pa.p_name,
    su.s_phone,
    ps.ps_supplycost,
    na.n_name
FROM
    part AS pa
RIGHT OUTER JOIN
    partsupp AS ps ON pa.p_partkey = ps.ps_partkey AND pa.p_size > 7
LEFT OUTER JOIN
    supplier AS su ON ps.ps_suppkey = su.s_suppkey AND su.s_acctbal < 2000
FULL OUTER JOIN
    nation AS na ON su.s_nationkey = na.n_nationkey AND na.n_regionkey > 3
ORDER BY
    pa.p_name, su.s_phone, ps.ps_supplycost, na.n_name DESC
LIMIT 20;

-- =================================================================
-- Query ID: A1 (Rewritten)
-- Description: An aggregate query counting line items by ship mode based on date and price conditions.
-- =================================================================
SELECT
    L.l_shipmode,
    COUNT(*) AS item_count
FROM
    orders AS O,
    lineitem AS L
WHERE
    O.o_orderkey = L.l_orderkey
    AND L.l_commitdate < L.l_receiptdate
    AND L.l_shipdate < L.l_commitdate
    AND L.l_receiptdate BETWEEN '1994-01-01' AND '1994-12-31'
    AND L.l_extendedprice <= O.o_totalprice
    AND L.l_extendedprice <= 70000
    AND O.o_totalprice > 60000
GROUP BY
    L.l_shipmode
ORDER BY
    L.l_shipmode;

-- =================================================================
-- Query ID: A2 (Rewritten)
-- Description: An aggregate query counting orders by priority within a specific date range.
-- =================================================================
SELECT
    O.o_orderpriority,
    COUNT(*) AS order_count
FROM
    orders AS O
INNER JOIN
    lineitem AS L ON L.l_orderkey = O.o_orderkey
WHERE
    O.o_orderdate >= '1993-07-01'
    AND O.o_orderdate < '1993-10-01'
    AND L.l_commitdate <= L.l_receiptdate
GROUP BY
    O.o_orderpriority
ORDER BY
    O.o_orderpriority;

-- =================================================================
-- Query ID: A3 (Rewritten)
-- Description: Selects line items based on a complex set of date and availability conditions.
-- =================================================================
SELECT
    li.l_orderkey,
    li.l_linenumber
FROM
    orders AS ord
INNER JOIN
    lineitem AS li ON ord.o_orderkey = li.l_orderkey
INNER JOIN
    partsupp AS ps ON ps.ps_partkey = li.l_partkey AND ps.ps_suppkey = li.l_suppkey
WHERE
    ps.ps_availqty = li.l_linenumber
    AND li.l_shipdate >= ord.o_orderdate
    AND ord.o_orderdate >= '1990-01-01'
    AND li.l_commitdate <= li.l_receiptdate
    AND li.l_shipdate <= li.l_commitdate
    AND li.l_receiptdate > '1994-01-01'
ORDER BY
    li.l_orderkey
LIMIT 7;

-- =================================================================
-- Query ID: A4 (Rewritten)
-- Description: Counts the number of waiting line items for each supplier for finished orders.
-- =================================================================
SELECT
    S.s_name,
    COUNT(*) AS num_wait
FROM
    supplier AS S,
    lineitem AS L,
    orders AS O,
    nation AS N
WHERE
    S.s_suppkey = L.l_suppkey
    AND O.o_orderkey = L.l_orderkey
    AND O.o_orderstatus = 'F'
    AND L.l_receiptdate >= L.l_commitdate
    AND S.s_nationkey = N.n_nationkey
GROUP BY
    S.s_name
ORDER BY
    num_wait DESC
LIMIT 100;

-- =================================================================
-- Query ID: A5 (Rewritten)
-- Description: A summary query that calculates various aggregates (sum, avg, count) on line items.
-- =================================================================
SELECT
    l.l_returnflag,
    l.l_linestatus,
    SUM(l.l_quantity) AS sum_qty,
    SUM(l.l_extendedprice) AS sum_base_price,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS sum_disc_price,
    SUM(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) AS sum_charge,
    AVG(l.l_quantity) AS avg_qty,
    AVG(l.l_extendedprice) AS avg_price,
    AVG(l.l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    lineitem AS l
WHERE
    l.l_receiptdate BETWEEN l.l_shipdate AND l.l_commitdate
GROUP BY
    l.l_returnflag,
    l.l_linestatus
ORDER BY
    l.l_returnflag,
    l.l_linestatus;

-- =================================================================
-- Query ID: N1 (Rewritten)
-- Description: Counts suppliers for parts based on brand, type, and size.
-- =================================================================
SELECT
    P.p_brand,
    P.p_type,
    P.p_size,
    COUNT(*) AS supplier_count
FROM
    part AS P
INNER JOIN
    partsupp AS PS ON P.p_partkey = PS.ps_partkey
WHERE
    P.p_size >= 4
    AND P.p_type NOT LIKE 'SMALL PLATED%'
    AND P.p_brand <> 'Brand#45'
GROUP BY
    P.p_brand,
    P.p_type,
    P.p_size
ORDER BY
    supplier_count DESC,
    P.p_brand ASC,
    P.p_type ASC,
    P.p_size ASC;

-- =================================================================
-- Query ID: F1 (Rewritten)
-- Description: A UNION ALL query combining high-balance customers from India with suppliers from Argentina.
-- =================================================================
WITH IndianCustomers AS (
    SELECT c.c_name AS name, c.c_acctbal AS balance
    FROM orders o, customer c, nation n
    WHERE c.c_custkey = o.o_custkey
      AND c.c_nationkey = n.n_nationkey
      AND c.c_mktsegment = 'FURNITURE'
      AND n.n_name = 'INDIA'
      AND o.o_orderdate BETWEEN '1998-01-01' AND '1998-12-05'
      AND o.o_totalprice <= c.c_acctbal
),
ArgentinianSuppliers AS (
    SELECT s.s_name AS name, s.s_acctbal AS balance
    FROM supplier s, lineitem l, orders o, nation n
    WHERE l.l_suppkey = s.s_suppkey
      AND l.l_orderkey = o.o_orderkey
      AND s.s_nationkey = n.n_nationkey
      AND n.n_name = 'ARGENTINA'
      AND o.o_orderdate BETWEEN '1998-01-01' AND '1998-01-05'
      AND o.o_totalprice > s.s_acctbal
      AND o.o_totalprice >= 30000
      AND 50000 >= s.s_acctbal
)
SELECT * FROM IndianCustomers
UNION ALL
(SELECT * FROM ArgentinianSuppliers ORDER BY balance DESC LIMIT 20);

-- =================================================================
-- Query ID: F2 (Rewritten)
-- Description: Combines order details with supplier details based on various part and date conditions.
-- =================================================================
(SELECT
    PA.p_brand,
    SU.s_name,
    LI.l_shipmode
FROM
    lineitem AS LI
JOIN
    part AS PA ON LI.l_partkey = PA.p_partkey
JOIN
    supplier AS SU ON SU.s_suppkey = LI.l_suppkey
WHERE
    LI.l_shipdate > '1995-01-01'
    AND SU.s_acctbal >= LI.l_extendedprice
    AND PA.p_partkey < 15000
    AND LI.l_suppkey < 14000
    AND PA.p_container = 'LG CAN'
ORDER BY
    SU.s_name
LIMIT 10)
UNION ALL
(SELECT
    P.p_brand,
    O.o_clerk,
    L.l_shipmode
FROM
    orders AS O,
    lineitem AS L,
    part AS P
WHERE
    L.l_partkey = P.p_partkey
    AND O.o_orderkey = L.l_orderkey
    AND L.l_shipdate >= O.o_orderdate
    AND O.o_orderdate > '1994-01-01'
    AND L.l_shipdate > '1995-01-01'
    AND P.p_retailprice >= L.l_extendedprice
    AND P.p_partkey < 10000
    AND L.l_suppkey < 10000
    AND P.p_container = 'LG CAN'
ORDER BY
    O.o_clerk
LIMIT 5);

-- =================================================================
-- Query ID: F3 (Rewritten)
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
-- Query ID: F4 (Rewritten)
-- Description: Selects nation names and customer account balances using a LEFT OUTER JOIN with conditions.
-- =================================================================
SELECT
    N.n_name,
    C.c_acctbal
FROM
    nation AS N
LEFT OUTER JOIN
    customer AS C ON N.n_nationkey = C.c_nationkey
WHERE
    C.c_nationkey > 3
    AND N.n_nationkey < 20
    AND C.c_nationkey <> 10
LIMIT 200;

-- =================================================================
-- Query ID: MQ1 (Rewritten)
-- Description: Pricing Summary Report Query. Provides a summary of line item pricing.
-- =================================================================
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS quantity_sum,
    sum(l_extendedprice) AS base_price_sum,
    sum(l_extendedprice * (1 - l_discount)) AS discounted_price_sum,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS charge_sum,
    avg(l_quantity) AS quantity_avg,
    avg(l_extendedprice) AS price_avg,
    avg(l_discount) AS discount_avg,
    count(*) AS order_count
FROM
    lineitem
WHERE
    l_shipdate <= DATE '1998-12-01' - INTERVAL '71 day'
GROUP BY
    1, 2
ORDER BY
    1, 2;

-- =================================================================
-- Query ID: MQ2 (Rewritten)
-- Description: Minimum Cost Supplier Query. Finds suppliers in a region who can supply a specific part at minimum cost.
-- =================================================================
SELECT
    s.s_acctbal,
    s.s_name,
    n.n_name,
    p.p_partkey,
    p.p_mfgr,
    s.s_address,
    s.s_phone,
    s.s_comment
FROM
    part p,
    supplier s,
    partsupp ps,
    nation n,
    region r
WHERE
    p.p_partkey = ps.ps_partkey
    AND s.s_suppkey = ps.ps_suppkey
    AND s.s_nationkey = n.n_nationkey
    AND n.n_regionkey = r.r_regionkey
    AND p.p_size = 38
    AND p.p_type LIKE '%TIN'
    AND r.r_name = 'MIDDLE EAST'
ORDER BY
    s.s_acctbal DESC,
    n.n_name,
    s.s_name,
    p.p_partkey
LIMIT 100;

-- =================================================================
-- Query ID: MQ3 (Rewritten)
-- Description: Shipping Priority Query. Lists unshipped orders with the highest revenue.
-- =================================================================
SELECT
    L.l_orderkey,
    SUM(L.l_extendedprice * (1 - L.l_discount)) AS revenue,
    O.o_orderdate,
    O.o_shippriority
FROM
    customer C
    JOIN orders O ON C.c_custkey = O.o_custkey
    JOIN lineitem L ON L.l_orderkey = O.o_orderkey
WHERE
    C.c_mktsegment = 'BUILDING'
    AND O.o_orderdate < '1995-03-15'
    AND L.l_shipdate > '1995-03-15'
GROUP BY
    L.l_orderkey,
    O.o_orderdate,
    O.o_shippriority
ORDER BY
    revenue DESC,
    O.o_orderdate
LIMIT 10;

-- =================================================================
-- Query ID: MQ4 (Rewritten)
-- Description: Order Priority Checking Query. Counts orders by priority for a given quarter.
-- =================================================================
SELECT
    o_orderpriority,
    o_orderdate,
    COUNT(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= '1997-07-01'
    AND o_orderdate < ('1997-07-01'::date + interval '3 month')
GROUP BY
    o_orderpriority, o_orderdate
ORDER BY
    o_orderpriority
LIMIT 10;

-- =================================================================
-- Query ID: MQ5 (Rewritten)
-- Description: Local Supplier Volume Query. Lists revenue volume for suppliers in a specific region.
-- =================================================================
SELECT
    N.n_name,
    SUM(L.l_extendedprice * (1 - L.l_discount)) AS revenue
FROM
    region AS R
    JOIN nation AS N ON R.r_regionkey = N.n_regionkey
    JOIN supplier AS S ON N.n_nationkey = S.s_nationkey
    JOIN lineitem AS L ON S.s_suppkey = L.l_suppkey
    JOIN orders AS O ON L.l_orderkey = O.o_orderkey
    JOIN customer AS C ON O.o_custkey = C.c_custkey AND S.s_nationkey = C.c_nationkey
WHERE
    R.r_name = 'MIDDLE EAST'
    AND O.o_orderdate >= '1994-01-01'
    AND O.o_orderdate < ('1994-01-01'::date + interval '1 year')
GROUP BY
    N.n_name
ORDER BY
    revenue DESC
LIMIT 100;

-- =================================================================
-- Query ID: MQ6 (Rewritten)
-- Description: Forecasting Revenue Change Query. Quantifies revenue increase if discounts were eliminated.
-- =================================================================
SELECT
    l_shipmode,
    SUM(l_extendedprice * l_discount) AS total_revenue
FROM
    lineitem
WHERE
    l_shipdate BETWEEN '1994-01-01' AND '1994-12-31'
    AND l_quantity < 24
GROUP BY
    l_shipmode
ORDER BY l_shipmode
LIMIT 100;

-- =================================================================
-- Query ID: MQ10 (Rewritten)
-- Description: Returned Item Reporting Query. Identifies customers who have returned items.
-- =================================================================
SELECT
    C.c_name,
    SUM(L.l_extendedprice * (1 - L.l_discount)) AS revenue,
    C.c_acctbal,
    N.n_name,
    C.c_address,
    C.c_phone,
    C.c_comment
FROM
    lineitem AS L
    JOIN orders AS O ON L.l_orderkey = O.o_orderkey
    JOIN customer AS C ON O.o_custkey = C.c_custkey
    JOIN nation AS N ON C.c_nationkey = N.n_nationkey
WHERE
    L.l_returnflag = 'R'
    AND O.o_orderdate >= '1994-01-01'
    AND O.o_orderdate < ('1994-01-01'::date + interval '3 month')
GROUP BY
    C.c_name,
    C.c_acctbal,
    C.c_phone,
    N.n_name,
    C.c_address,
    C.c_comment
ORDER BY
    revenue DESC
LIMIT 20;

-- =================================================================
-- Query ID: MQ11 (Rewritten)
-- Description: Important Stock Identification Query. Finds the most important stock in a given nation.
-- =================================================================
SELECT
    PS.ps_comment,
    SUM(PS.ps_supplycost * PS.ps_availqty) AS stock_value
FROM
    partsupp AS PS
    INNER JOIN supplier AS S ON PS.ps_suppkey = S.s_suppkey
    INNER JOIN nation AS N ON S.s_nationkey = N.n_nationkey
WHERE
    N.n_name = 'ARGENTINA'
GROUP BY
    PS.ps_comment
ORDER BY
    stock_value DESC
LIMIT 100;

-- =================================================================
-- Query ID: MQ17 (Rewritten)
-- Description: Small-Quantity-Order Revenue Query. Determines average extended price for small-quantity orders.
-- =================================================================
SELECT
    AVG(L.l_extendedprice) AS average_total
FROM
    lineitem AS L
    JOIN part AS P ON P.p_partkey = L.l_partkey
WHERE
    P.p_brand = 'Brand#52'
    AND P.p_container = 'LG CAN';

-- =================================================================
-- Query ID: MQ18 (Rewritten)
-- Description: Large Volume Customer Query. Finds a large volume of orders for customers with a specific phone prefix.
-- =================================================================
SELECT
    cust.c_name,
    ord.o_orderdate,
    ord.o_totalprice,
    SUM(li.l_quantity)
FROM
    customer AS cust,
    orders AS ord,
    lineitem AS li
WHERE
    cust.c_phone LIKE '27-%'
    AND cust.c_custkey = ord.o_custkey
    AND ord.o_orderkey = li.l_orderkey
GROUP BY
    1, 2, 3
ORDER BY
    ord.o_orderdate,
    ord.o_totalprice DESC
LIMIT 100;

-- =================================================================
-- Query ID: MQ21 (Rewritten)
-- Description: Suppliers Who Kept Orders Waiting Query. Finds suppliers who had line items not shipped on time.
-- =================================================================
SELECT
    s.s_name,
    COUNT(*) AS numwait
FROM
    supplier AS s
    JOIN nation AS n ON s.s_nationkey = n.n_nationkey
    JOIN lineitem AS l1 ON s.s_suppkey = l1.l_suppkey
    JOIN orders AS o ON o.o_orderkey = l1.l_orderkey
WHERE
    n.n_name = 'GERMANY'
    AND o.o_orderstatus = 'F'
GROUP BY
    s.s_name
ORDER BY
    numwait DESC,
    s.s_name
LIMIT 100;

-- =================================================================
-- Query ID: Alaap (Rewritten)
-- Description: A custom query calculating revenue by market segment and shipping priority.
-- =================================================================
SELECT
    N.n_name,
    C.c_acctbal
FROM
    nation AS N
LEFT OUTER JOIN
    customer AS C
    ON N.n_nationkey = C.c_nationkey
   AND C.c_nationkey > 3
   AND N.n_nationkey < 20
   AND C.c_nationkey <> 10
LIMIT 200;
    AND o.o_orderdate <= '1995-10-13'
    AND l.l_extendedprice BETWEEN 212 AND 3000
    AND l.l_quantity <= 123
GROUP BY
    o.o_orderdate,
    o.o_shippriority,
    c.c_mktsegment
ORDER BY
    revenue DESC,
    o.o_orderdate ASC,
    o.o_shippriority ASC;

-- =================================================================
-- Query ID: TPCH_Q9 (Rewritten)
-- Description: Product Type Profit Measure Query. Computes profit for all parts with a certain name substring.
-- =================================================================
WITH profit_calc AS (
    SELECT
        n.n_name AS nation,
        EXTRACT(YEAR FROM o.o_orderdate) AS order_year,
        l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity AS amount
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
        AND p.p_name LIKE 'co%'
)
SELECT
    nation,
    order_year,
    SUM(amount) AS sum_profit
FROM
    profit_calc
GROUP BY
    nation,
    order_year
ORDER BY
    nation,
    order_year DESC;

-- =================================================================
-- Query ID: TPCH_Q11 (Rewritten)
-- Description: Important Stock Identification Query. Finds parts from a nation that represent a significant
-- fraction of the total stock value.
-- =================================================================
WITH IndiaStock AS (
    SELECT
        ps.ps_partkey,
        SUM(ps.ps_supplycost * ps.ps_availqty) AS total_value
    FROM
        partsupp ps
    JOIN
        supplier s ON ps.ps_suppkey = s.s_suppkey
    JOIN
        nation n ON s.s_nationkey = n.n_nationkey
    WHERE
        n.n_name = 'INDIA'
    GROUP BY
        ps.ps_partkey
)
SELECT
    I.ps_partkey,
    'INDIA' as n_name,
    I.total_value
FROM
    IndiaStock I
WHERE
    I.total_value > (
        SELECT SUM(total_value) * 0.00001 FROM IndiaStock
    )
ORDER BY
    I.total_value DESC;

-- =================================================================
-- Query ID: TPCH_Q12 (Rewritten)
-- Description: Shipping Modes and Order Priority Query. Determines if late shipping is related to order priority.
-- =================================================================
SELECT
    l.l_shipmode,
    SUM(CASE WHEN o.o_orderpriority = '1-URGENT' OR o.o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
    SUM(CASE WHEN o.o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
FROM
    orders o
JOIN
    lineitem l ON o.o_orderkey = l.l_orderkey
WHERE
    l.l_shipmode = 'SHIP'
    AND l.l_commitdate < l.l_receiptdate
    AND l.l_shipdate < l.l_commitdate
    AND l.l_receiptdate >= '1995-01-01'
    AND l.l_receiptdate < ('1995-01-01'::date + INTERVAL '1 year')
GROUP BY
    l.l_shipmode
ORDER BY
    l.l_shipmode;

-- =================================================================
-- Query ID: TPCH_Q13 (Rewritten)
-- Description: Customer Distribution Query. Counts customers by the number of orders they have placed.
-- =================================================================
WITH CustomerOrders AS (
    SELECT
        c.c_custkey,
        o.o_orderdate,
        COUNT(o.o_orderkey) AS order_count
    FROM
        customer c
    LEFT OUTER JOIN
        orders o ON c.c_custkey = o.o_custkey AND o.o_comment NOT LIKE '%among%regular%'
    GROUP BY
        c.c_custkey, o.o_orderdate
)
SELECT
    c_orders.order_count,
    c_orders.o_orderdate,
    COUNT(*) AS custdist
FROM
    CustomerOrders AS c_orders
GROUP BY
    c_orders.order_count,
    c_orders.o_orderdate
ORDER BY
    custdist DESC,
    c_orders.order_count DESC;

-- =================================================================
-- Query ID: TPCH_Q14 (Rewritten)
-- Description: Promotion Effect Query. Monitors market share of promotional parts.
-- =================================================================
SELECT
    100.00 * SUM(
        CASE
            WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount)
            ELSE 0
        END
    ) / SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
FROM
    lineitem l
JOIN
    part p ON l.l_partkey = p.p_partkey
WHERE
    l.l_shipdate >= '1995-01-01'
    AND l.l_shipdate < ('1995-01-01'::date + INTERVAL '1 month');

-- =================================================================
-- Query ID: ETPCH_Q15 (Rewritten)
-- Description: Top Supplier Query. Finds the supplier with the maximum total revenue for a given period.
-- =================================================================
WITH RevenueCTE AS (
    SELECT
        l_suppkey AS supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        lineitem
    WHERE
        l_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
    GROUP BY
        l_suppkey
)
SELECT
    s.s_suppkey,
    s.s_name,
    s.s_address,
    s.s_phone,
    r.total_revenue
FROM
    supplier s
JOIN
    RevenueCTE r ON s.s_suppkey = r.supplier_no
WHERE
    r.total_revenue = (SELECT MAX(total_revenue) FROM RevenueCTE)
ORDER BY
    s.s_suppkey;

-- =================================================================
-- Query ID: TPCH_Q16 (Rewritten)
-- Description: Parts/Supplier Relationship Query. Counts suppliers for parts matching certain criteria,
-- excluding suppliers with complaints.
-- =================================================================
SELECT
    p.p_brand,
    p.p_type,
    p.p_size,
    COUNT(DISTINCT ps.ps_suppkey) AS supplier_cnt
FROM
    partsupp ps
JOIN
    part p ON p.p_partkey = ps.ps_partkey
WHERE
    p.p_brand <> 'Brand#23'
    AND p.p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p.p_size IN (1, 4, 7)
    AND NOT EXISTS (
        SELECT 1
        FROM supplier s
        WHERE s.s_suppkey = ps.ps_suppkey
          AND s.s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY
    p.p_brand,
    p.p_type,
    p.p_size
ORDER BY
    supplier_cnt DESC,
    p.p_brand,
    p.p_type,
    p.p_size;

-- =================================================================
-- Query ID: TPCH_Q17 (Rewritten)
-- Description: Small-Quantity-Order Revenue Query. Calculates average yearly revenue for parts with low quantity.
-- =================================================================
WITH AvgQuantity AS (
    SELECT l_partkey, 0.7 * AVG(l_quantity) AS threshold
    FROM lineitem
    GROUP BY l_partkey
)
SELECT
    SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem l
JOIN
    part p ON p.p_partkey = l.l_partkey
JOIN
    AvgQuantity aq ON aq.l_partkey = l.l_partkey
WHERE
    p.p_brand = 'Brand#53'
    AND p.p_container = 'MED BAG'
    AND l.l_quantity < aq.threshold;

-- =================================================================
-- Query ID: ETPCH_Q18 (Rewritten)
-- Description: Large Volume Customer Query. Finds top customers who have placed a large volume of orders.
-- =================================================================
WITH LargeOrders AS (
    SELECT l_orderkey
    FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300
)
SELECT
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice,
    SUM(l.l_quantity)
FROM
    customer c
JOIN
    orders o ON c.c_custkey = o.o_custkey
JOIN
    lineitem l ON o.o_orderkey = l.l_orderkey
WHERE
    o.o_orderkey IN (SELECT l_orderkey FROM LargeOrders)
GROUP BY
    c.c_name,
    c.c_custkey,
    o.o_orderkey,
    o.o_orderdate,
    o.o_totalprice
ORDER BY
    o.o_totalprice DESC,
    o.o_orderdate;

-- =================================================================
-- Query ID: Nested_Test (Rewritten)
-- Description: A sample nested query to find suppliers from France for 'ivory' parts.
-- =================================================================
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
WHERE
    p.p_name LIKE '%ivory%'
    AND n.n_name = 'FRANCE'
    AND ps.ps_availqty > (SELECT MIN(c_acctbal) FROM customer)
ORDER BY
    s.s_name;

-- =================================================================
-- Query ID: paper_sample (Rewritten & Corrected)
-- Description: A custom UNION ALL query combining high-value customers with suppliers based on average price.
-- =================================================================
(SELECT
    s.s_name AS entity_name,
    n.n_name AS country,
    AVG(l.l_extendedprice * (1 - l.l_discount)) AS price
FROM
    lineitem l, nation n, orders o, region r, supplier s
WHERE
    l.l_suppkey = s.s_suppkey
    AND n.n_nationkey = s.s_nationkey
    AND l.l_orderkey = o.o_orderkey
    AND n.n_regionkey = r.r_regionkey
    AND s.s_acctbal <= o.o_totalprice
    AND o.o_totalprice <= 15000
GROUP BY
    n.n_name, s.s_name
ORDER BY
    price DESC, country DESC, entity_name)
UNION ALL
(SELECT
    c.c_name AS entity_name,
    n.n_name AS country,
    o.o_totalprice AS price
FROM
    orders o
JOIN
    customer c ON c.c_custkey = o.o_custkey
JOIN
    nation n ON c.c_nationkey = n.n_nationkey -- This line has been corrected
WHERE
    o.o_totalprice <= c.c_acctbal
    AND c.c_acctbal >= 9000
    AND o.o_totalprice <= 15000
    AND c.c_mktsegment IN ('HOUSEHOLD', 'MACHINERY')
GROUP BY
    c.c_name, n.n_name, o.o_totalprice
ORDER BY
    price ASC, country ASC, entity_name);

-- =================================================================
-- Query ID: ETPCH_Q1 (Rewritten)
-- Description: Simplified version of ETPCH Q1, using the standard lineitem table.
-- =================================================================
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS total_qty,
    AVG(l_extendedprice) AS avg_price,
    COUNT(*) AS num_orders,
    SUM(l_extendedprice) AS total_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS total_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS total_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_discount) AS avg_disc
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '3 day'
GROUP BY
    l_returnflag, l_linestatus
ORDER BY
    l_returnflag, l_linestatus;

-- =================================================================
-- Query ID: ETPCH_Q3 (Rewritten)
-- Description: Simplified version of ETPCH Q3, using the standard lineitem table for shipping priority analysis.
-- =================================================================
SELECT
    L.l_orderkey,
    SUM(L.l_extendedprice * (1 - L.l_discount)) AS revenue,
    O.o_orderdate,
    O.o_shippriority
FROM
    customer C, orders O, lineitem L
WHERE
    C.c_mktsegment = 'FURNITURE'
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
-- Query ID: ETPCH_Q4 (Rewritten)
-- Description: Simplified version of ETPCH Q4, using the standard lineitem table for order priority checking.
-- =================================================================
SELECT
    o.o_orderpriority,
    COUNT(DISTINCT o.o_orderkey) AS order_count
FROM
    orders o, lineitem l
WHERE
    o.o_orderkey = l.l_orderkey
    AND l.l_commitdate < l.l_receiptdate
    AND o.o_orderdate >= '1995-01-01' AND o.o_orderdate <= '1995-03-31'
GROUP BY
    o.o_orderpriority
ORDER BY
    o.o_orderpriority ASC;

-- =================================================================
-- Query ID: ETPCH_Q5 (Rewritten)
-- Description: Simplified version of ETPCH Q5, using standard tables for local supplier volume.
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
    r.r_name = 'ASIA'
    AND o.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31'
GROUP BY
    n.n_name
ORDER BY
    revenue DESC, n.n_name ASC;

-- =================================================================
-- Query ID: ETPCH_Q6 (Rewritten)
-- Description: Simplified version of ETPCH Q6, using the standard lineitem table for forecasting revenue change.
-- =================================================================
SELECT
    SUM(l_extendedprice * l_discount)
FROM
    lineitem
WHERE
    l_shipdate >= '1993-01-01'
    AND l_shipdate < '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;

-- =================================================================
-- Query ID: ETPCH_Q6_1 (Rewritten)
-- Description: A variation of ETPCH_Q6 focusing only on the standard lineitem table.
-- =================================================================
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate < '1995-01-01'
    AND l_shipdate >= '1993-01-01'
    AND l_quantity < 24;

-- =================================================================
-- Query ID: ETPCH_Q6_2 (Rewritten)
-- Description: Another variation of ETPCH_Q6, summing revenue from the standard lineitem table.
-- =================================================================
SELECT
    SUM(l_extendedprice) AS revenue
FROM
    lineitem
WHERE
    l_quantity < 24
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_shipdate >= '1993-01-01'
    AND l_shipdate < '1995-01-01';

-- =================================================================
-- Query ID: ETPCH_Q7 (Rewritten)
-- Description: Simplified version of ETPCH Q7, analyzing trade volume between two nations using standard tables.
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
    AND ((supp_nation.n_name = 'FRANCE' AND cust_nation.n_name = 'GERMANY')
         OR (supp_nation.n_name = 'GERMANY' AND cust_nation.n_name = 'FRANCE'))
GROUP BY
    supp_nation.n_name, cust_nation.n_name, l_year
ORDER BY
    supp_nation.n_name, cust_nation.n_name, l_year;

-- =================================================================
-- Query ID: ETPCH_Q9 (Rewritten)
-- Description: Simplified version of ETPCH Q9, calculating profit by nation and year using standard tables.
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
    AND p.p_name LIKE '%co%'
GROUP BY
    n.n_name, o_year
ORDER BY
    nation ASC, o_year DESC;

-- =================================================================
-- Query ID: ETPCH_Q10 (Rewritten)
-- Description: Simplified version of ETPCH Q10, reporting on returned items using the standard lineitem table.
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
    AND l.l_returnflag = 'R'
    AND o.o_orderdate >= '1995-01-01' AND o.o_orderdate < '1995-04-01'
GROUP BY
    c.c_custkey, c.c_name, c.c_acctbal, n.n_name, c.c_address, c.c_phone, c.c_comment
ORDER BY
    revenue DESC
LIMIT 20;

-- =================================================================
-- Query ID: ETPCH_Q12 (Rewritten)
-- Description: Simplified ETPCH Q12, analyzing shipping modes and order priority using the standard lineitem table.
-- =================================================================
SELECT
    l.l_shipmode,
    COUNT(CASE WHEN o.o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 END) AS high_line_count,
    COUNT(CASE WHEN o.o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 END) AS low_line_count
FROM
    orders o, lineitem l
WHERE
    o.o_orderkey = l.l_orderkey
    AND l.l_shipmode IN ('SHIP', 'TRUCK')
    AND l.l_commitdate < l.l_receiptdate
    AND l.l_shipdate < l.l_commitdate
    AND l.l_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
GROUP BY
    l.l_shipmode
ORDER BY
    l.l_shipmode;

-- =================================================================
-- Query ID: ETPCH_Q14 (Rewritten)
-- Description: Simplified ETPCH Q14, calculating promotion effect using the standard lineitem table.
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
    100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) /
    SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM PromoData;

-- =================================================================
-- Query ID: ETPCH_Q13 (Rewritten)
-- Description: Extended TPC-H Q13, analyzing customer distribution by order count.
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
        orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
    GROUP BY
        c_custkey, o_orderdate
) AS c_orders
GROUP BY
    c_orders.c_count, c_orders.o_orderdate
ORDER BY
    custdist DESC, c_count DESC;

-- =================================================================
-- Query ID: ETPCH_Q2 (Rewritten)
-- Description: Extended TPC-H Q2, finding the minimum cost supplier for a part in Europe.
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
    AND p.p_size = 15
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
-- Query ID: ETPCH_Q8 (Rewritten)
-- Description: Simplified ETPCH Q8, calculating market share using standard tables.
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
        AND p.p_type = 'ECONOMY ANODIZED STEEL'
        AND o.o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
)
SELECT
    o_year,
    SUM(CASE WHEN nation = 'INDIA' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
FROM MarketData
GROUP BY o_year
ORDER BY o_year;

-- =================================================================
-- Query ID: ETPCH_Q11 (Rewritten)
-- Description: Extended TPC-H Q11, identifying important stock in India.
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
    AND n.n_name = 'INDIA'
GROUP BY
    ps.ps_partkey, n.n_name
HAVING
    SUM(ps.ps_supplycost * ps.ps_availqty) >
    (SELECT SUM(ps_inner.ps_supplycost * ps_inner.ps_availqty) * 0.00001
     FROM partsupp ps_inner, supplier s_inner, nation n_inner
     WHERE ps_inner.ps_suppkey = s_inner.s_suppkey
       AND s_inner.s_nationkey = n_inner.n_nationkey
       AND n_inner.n_name = 'INDIA')
ORDER BY total_value DESC;

-- =================================================================
-- Query ID: ETPCH_Q16 (Rewritten)
-- Description: Extended TPC-H Q16, analyzing parts/supplier relationships.
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
    AND ps.ps_suppkey NOT IN (SELECT s.s_suppkey FROM supplier s WHERE s.s_comment LIKE '%Customer%Complaints%')
GROUP BY
    p.p_brand, p.p_type, p.p_size
ORDER BY
    supplier_cnt DESC, p.p_brand, p.p_type, p.p_size;

-- =================================================================
-- Query ID: ETPCH_Q19 (Rewritten)
-- Description: Simplified ETPCH Q19, Discounted Revenue Query using the standard lineitem table.
-- =================================================================
SELECT
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    lineitem l, part p
WHERE
    l.l_partkey = p.p_partkey
    AND l.l_shipmode IN ('AIR', 'AIR REG')
    AND l.l_shipinstruct = 'DELIVER IN PERSON'
    AND (
        (p.p_brand = 'Brand#12' AND p.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l.l_quantity BETWEEN 1 AND 11 AND p.p_size BETWEEN 1 AND 5)
     OR (p.p_brand = 'Brand#23' AND p.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l.l_quantity BETWEEN 10 AND 20 AND p.p_size BETWEEN 1 AND 10)
     OR (p.p_brand = 'Brand#34' AND p.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l.l_quantity BETWEEN 20 AND 30 AND p.p_size BETWEEN 1 AND 15)
    );

-- =================================================================
-- Query ID: ETPCH_Q21 (Rewritten)
-- Description: Simplified ETPCH Q21, finding suppliers in Argentina who kept orders waiting, using standard tables.
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
    AND EXISTS (
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
-- Query ID: ETPCH_Q23 (Rewritten)
-- Description: A simplified version of a complex query analyzing customer returns using standard tables.
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
    AND l.l_returnflag = 'R'
    AND EXTRACT(YEAR FROM o.o_orderdate) = 1995
GROUP BY
    city, p.p_brand
ORDER BY
    city, p.p_brand;

-- =================================================================
-- Query ID: ETPCH_Q24 (Rewritten)
-- Description: A simplified version of a complex query analyzing returns and supplier availability.
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
    l.l_returnflag = 'R'
    AND o.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31';

-- =================================================================
-- Query ID: ETPCH_Q22 (Optimized)
-- Description: Finds potential customers in specific countries who have high account balances but have not yet placed any orders.
-- Fix: Replaced a correlated subquery for the average account balance with a CTE (AvgBalance) to calculate the average only once.
--      Also replaced NOT IN with a more performant NOT EXISTS clause.
-- =================================================================
WITH AvgBalance AS (
    SELECT AVG(c_acctbal) as avg_bal
    FROM customer
    WHERE c_acctbal > 0.00
      AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
),
FilteredCustomers AS (
    SELECT
        SUBSTRING(c.c_phone, 1, 2) AS cntrycode,
        c.c_acctbal
    FROM
        customer c, AvgBalance ab
    WHERE
        SUBSTRING(c.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
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
-- Query ID: ETPCH_Q17 (Optimized)
-- Description: Calculates the average yearly revenue for parts with low order quantities.
-- Fix: Replaced the correlated subquery for average quantity with a CTE (PartAvgQuantity).
--      This pre-calculates the average quantity for every part in a single pass.
-- =================================================================
WITH PartAvgQuantity AS (
    SELECT
        l_partkey,
        0.7 * AVG(l_quantity) AS avg_q
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
-- Query ID: ETPCH_Q20 (Optimized)
-- Description: Finds suppliers in France for 'ivory' parts based on stock availability versus sales quantity.
-- Fix: Replaced the deeply nested correlated subquery with a CTE (LineitemQuantity) to pre-calculate
--      the sum of quantities for each part/supplier pair, avoiding row-by-row execution.
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
    AND p.p_name LIKE '%ivory%'
    AND ps.ps_availqty > lq.half_sum_qty
ORDER BY
    s.s_name;

