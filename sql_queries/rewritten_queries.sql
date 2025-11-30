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
    l_shipdate <= DATE '1998-12-01' - INTERVAL '71' DAY
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
    AND o_orderdate < DATE '1997-07-01' + INTERVAL '3' MONTH
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
    AND O.o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
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
    AND O.o_orderdate < DATE '1994-01-01' + INTERVAL '3' MONTH
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
    SUM(li.l_quantity) AS total_qty
FROM
    customer AS cust,
    orders AS ord,
    lineitem AS li
WHERE
    cust.c_phone LIKE '27-%'
    AND cust.c_custkey = ord.o_custkey
    AND ord.o_orderkey = li.l_orderkey
GROUP BY
    cust.c_name,
    ord.o_orderdate,
    ord.o_totalprice
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
    c.c_mktsegment,
    o.o_shippriority,
    o.o_orderdate,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE
    o.o_orderdate <= DATE '1995-10-13'
    AND l.l_extendedprice BETWEEN 212 AND 3000
    AND l.l_quantity <= 123
GROUP BY
    c.c_mktsegment,
    o.o_shippriority,
    o.o_orderdate
ORDER BY
    revenue DESC,
    o.o_orderdate ASC,
    o.o_shippriority ASC
LIMIT 200;

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
    l_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY
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
    n.n_name, EXTRACT(YEAR FROM o.o_orderdate)
ORDER BY
    nation ASC, EXTRACT(YEAR FROM o.o_orderdate) DESC;

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
-- Query ID: LITHE_1
-- Description: Standard query Q1 (pricing summary).
-- =================================================================
SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate < '1998-11-29' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

-- =================================================================
-- Query ID: LITHE_2
-- Description: Standard query Q2 (minimum cost supplier-like).
-- =================================================================
WITH min_supplycost AS ( SELECT ps_partkey, MIN(ps_supplycost) AS min_cost FROM partsupp JOIN supplier ON s_suppkey = ps_suppkey JOIN nation ON s_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey WHERE r_name = 'EUROPE' GROUP BY ps_partkey ) SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part JOIN partsupp ON p_partkey = ps_partkey JOIN supplier ON s_suppkey = ps_suppkey JOIN nation ON s_nationkey = n_nationkey JOIN region ON n_regionkey = r_regionkey JOIN min_supplycost ON part.p_partkey = min_supplycost.ps_partkey AND partsupp.ps_supplycost = min_supplycost.min_cost WHERE p_size = 15 AND p_type LIKE '%BRASS' AND r_name = 'EUROPE' ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;

-- =================================================================
-- Query ID: LITHE_3
-- Description: Standard query Q3 (shipping priority revenue).
-- =================================================================
with customer_orders_cte as ( select o_orderkey, o_orderdate, o_shippriority from customer, orders where c_mktsegment = 'FURNITURE' and c_custkey = o_custkey and o_orderdate < date '1995-01-01' ), lineitem_revenue_cte as ( select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue from lineitem where l_shipdate > date '1995-01-01' group by l_orderkey ) select co.o_orderkey, lr.revenue, co.o_orderdate, co.o_shippriority from customer_orders_cte co join lineitem_revenue_cte lr on co.o_orderkey = lr.l_orderkey order by lr.revenue desc, co.o_orderdate;

-- =================================================================
-- Query ID: LITHE_4
-- Description: Standard query Q4 (order priority counting).
-- =================================================================
select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + INTERVAL '3' MONTH and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority ;

-- =================================================================
-- Query ID: LITHE_5
-- Description: Standard query Q5 (local supplier volume-like).
-- =================================================================
with filtered_orders as ( select * from orders where o_orderkey in ( select l_orderkey from lineitem ) ) select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, filtered_orders as orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date '1995-01-01' and o_orderdate < date '1995-01-01' + INTERVAL '1' YEAR group by n_name order by revenue desc;

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
SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit FROM part JOIN partsupp ON p_partkey = ps_partkey JOIN lineitem ON l_partkey = p_partkey AND l_suppkey = ps_suppkey JOIN supplier ON s_suppkey = l_suppkey JOIN orders ON o_orderkey = l_orderkey JOIN nation ON s_nationkey = n_nationkey WHERE p_name LIKE 'co%' GROUP BY n_name, o_year ORDER BY n_name, o_year DESC;

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
SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, r.total_revenue FROM ( SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue, MAX(SUM(l_extendedprice * (1 - l_discount))) OVER () AS max_revenue FROM lineitem WHERE l_shipdate >= DATE '1995-01-01' AND l_shipdate < DATE '1995-01-01' + INTERVAL '3' MONTH GROUP BY l_suppkey ) r JOIN supplier s ON s.s_suppkey = r.supplier_no WHERE r.total_revenue = r.max_revenue ORDER BY s.s_suppkey;

-- =================================================================
-- Query ID: LITHE_16
-- Description: Standard query Q16 (parts/supplier relationship stats).
-- =================================================================
SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp JOIN part ON p_partkey = ps_partkey LEFT JOIN supplier ON ps_suppkey = s_suppkey AND s_comment LIKE '%Customer%Complaints%' WHERE p_brand <> 'Brand#23' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (1, 4, 7) AND s_suppkey IS NULL GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;

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
with filtered_supplier as ( select * from supplier where s_nationkey in ( select n_nationkey from nation where n_name = 'FRANCE' ) ) select s_name, s_address from filtered_supplier as supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like '%ivory%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1995-01-01' and l_shipdate < date '1995-01-01' + INTERVAL '1' YEAR ) ) and s_nationkey = n_nationkey and n_name = 'FRANCE' order by s_name; 

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

