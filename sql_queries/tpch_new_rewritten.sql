-- =================================================================
-- Query ID: TPCHN1
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
GROUP BY l_returnflag,
         l_linestatus
ORDER BY l_returnflag,
         l_linestatus;

-- =================================================================
-- Query ID: TPCHN2
-- =================================================================
WITH min_supplycost AS
  (SELECT ps_partkey,
          MIN(ps_supplycost) AS min_cost
   FROM partsupp
   JOIN supplier ON s_suppkey = ps_suppkey
   JOIN nation ON s_nationkey = n_nationkey
   JOIN region ON n_regionkey = r_regionkey
   WHERE r_name = 'EUROPE'
   GROUP BY ps_partkey)
SELECT s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
FROM part
JOIN partsupp ON p_partkey = ps_partkey
JOIN supplier ON s_suppkey = ps_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
JOIN min_supplycost ON part.p_partkey = min_supplycost.ps_partkey
AND partsupp.ps_supplycost = min_supplycost.min_cost
WHERE p_size = 15
  AND p_type LIKE '%BRASS'
  AND r_name = 'EUROPE'
ORDER BY s_acctbal DESC,
         n_name,
         s_name,
         p_partkey
LIMIT 100;

-- =================================================================
-- Query ID: TPCHN3
-- =================================================================
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate,
       o_shippriority
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON c_custkey = o_custkey
WHERE c_mktsegment = 'FURNITURE'
  AND o_orderdate < DATE '1995-01-01'
  AND l_shipdate > DATE '1995-01-01'
GROUP BY l_orderkey,
         o_orderdate,
         o_shippriority
ORDER BY revenue DESC,
         o_orderdate;

-- =================================================================
-- Query ID: TPCHN4
-- =================================================================
SELECT o_orderpriority,
       COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-04-01'
  AND EXISTS
    (SELECT 1
     FROM lineitem
     WHERE l_orderkey = o_orderkey
       AND l_commitdate < l_receiptdate )
GROUP BY o_orderpriority
ORDER BY o_orderpriority;

-- =================================================================
-- Query ID: TPCHN5
-- =================================================================
SELECT n_name,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM region
JOIN nation ON n_regionkey = r_regionkey
JOIN supplier ON s_nationkey = n_nationkey
JOIN lineitem ON l_suppkey = s_suppkey
JOIN orders ON o_orderkey = l_orderkey
JOIN customer ON c_custkey = o_custkey
WHERE r_name = 'ASIA'
  AND o_orderdate >= DATE '1995-01-01'
  AND o_orderdate < DATE '1996-01-01'
GROUP BY n_name
ORDER BY revenue DESC;

-- =================================================================
-- Query ID: TPCHN6
-- =================================================================
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1993-01-01'
  AND l_shipdate < DATE '1994-03-01' + INTERVAL '1 year'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 10;

-- =================================================================
-- Query ID: TPCHN7
-- =================================================================
SELECT n1.n_name AS supp_nation,
       n2.n_name AS cust_nation,
       EXTRACT(YEAR
               FROM l.l_shipdate) AS l_year,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l
JOIN supplier s ON s.s_suppkey = l.l_suppkey
JOIN orders o ON o.o_orderkey = l.l_orderkey
JOIN customer c ON c.c_custkey = o.o_custkey
JOIN nation n1 ON s.s_nationkey = n1.n_nationkey
JOIN nation n2 ON c.c_nationkey = n2.n_nationkey
WHERE ((n1.n_name = 'GERMANY'
        AND n2.n_name = 'FRANCE')
       OR (n1.n_name = 'FRANCE'
           AND n2.n_name = 'GERMANY'))
  AND l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
GROUP BY n1.n_name,
         n2.n_name,
         EXTRACT(YEAR
                 FROM l.l_shipdate)
ORDER BY n1.n_name,
         n2.n_name,
         l_year;

-- =================================================================
-- Query ID: TPCHN8
-- =================================================================
SELECT o_year,
       SUM(CASE
               WHEN nation = 'INDIA' THEN volume
               ELSE 0
           END) / SUM(volume) AS mkt_share
FROM
  (SELECT EXTRACT(YEAR
                  FROM o_orderdate) AS o_year,
          l_extendedprice * (1 - l_discount) AS volume,
          n2.n_name AS nation
   FROM lineitem
   JOIN orders ON l_orderkey = o_orderkey
   JOIN customer ON o_custkey = c_custkey
   JOIN nation n1 ON c_nationkey = n1.n_nationkey
   JOIN region ON n1.n_regionkey = r_regionkey
   JOIN supplier ON s_suppkey = l_suppkey
   JOIN nation n2 ON s_nationkey = n2.n_nationkey
   JOIN part ON p_partkey = l_partkey
   WHERE r_name = 'ASIA'
     AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
     AND p_type = 'ECONOMY ANODIZED STEEL' ) AS all_nations
GROUP BY o_year
ORDER BY o_year;

-- =================================================================
-- Query ID: TPCHN9
-- =================================================================
SELECT n_name AS nation,
       EXTRACT(YEAR
               FROM o_orderdate) AS o_year,
       SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit
FROM nation
JOIN supplier ON s_nationkey = n_nationkey
JOIN lineitem ON s_suppkey = l_suppkey
JOIN partsupp ON ps_suppkey = l_suppkey
AND ps_partkey = l_partkey
JOIN part ON p_partkey = l_partkey
JOIN orders ON o_orderkey = l_orderkey
WHERE p_name LIKE 'co%'
GROUP BY n_name,
         o_year
ORDER BY n_name,
         o_year DESC;

-- =================================================================
-- Query ID: TPCHN10
-- =================================================================
SELECT c.c_custkey,
       c.c_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
       c.c_acctbal,
       n.n_name,
       c.c_address,
       c.c_phone,
       c.c_comment
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
JOIN nation n ON c.c_nationkey = n.n_nationkey
WHERE o.o_orderdate >= DATE '1995-01-01'
  AND o.o_orderdate < DATE '1995-04-01'
  AND l.l_returnflag = 'R'
GROUP BY c.c_custkey,
         c.c_name,
         c.c_acctbal,
         c.c_phone,
         n.n_name,
         c.c_address,
         c.c_comment
ORDER BY revenue DESC;

-- =================================================================
-- Query ID: TPCHN11
-- =================================================================
WITH india_parts AS
  (SELECT ps_partkey,
          SUM(ps_supplycost * ps_availqty) AS total_value
   FROM partsupp
   JOIN supplier ON ps_suppkey = s_suppkey
   JOIN nation ON s_nationkey = n_nationkey
   WHERE n_name = 'INDIA'
   GROUP BY ps_partkey),
     threshold AS
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001 AS value_threshold
   FROM partsupp
   JOIN supplier ON ps_suppkey = s_suppkey
   JOIN nation ON s_nationkey = n_nationkey
   WHERE n_name = 'INDIA' )
SELECT ps_partkey,
       'INDIA' AS n_name,
       total_value
FROM india_parts,
     threshold
WHERE total_value > value_threshold
ORDER BY total_value DESC;

-- =================================================================
-- Query ID: TPCHN12
-- =================================================================
SELECT l_shipmode,
       SUM(CASE
               WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1
               ELSE 0
           END) AS high_line_count,
       SUM(CASE
               WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1
               ELSE 0
           END) AS low_line_count
FROM orders
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_shipmode = 'SHIP'
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1995-01-01'
  AND l_receiptdate < DATE '1996-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode;

-- =================================================================
-- Query ID: TPCHN13
-- =================================================================
SELECT c_count,
       o_orderdate,
       COUNT(*) AS custdist
FROM
  (SELECT c.c_custkey,
          o.o_orderdate,
          COUNT(o.o_orderkey) AS c_count
   FROM customer c
   LEFT JOIN orders o ON c.c_custkey = o.o_custkey
   AND o.o_comment NOT LIKE '%special%requests%'
   GROUP BY c.c_custkey,
            o.o_orderdate) AS c_orders
GROUP BY c_count,
         o_orderdate
ORDER BY custdist DESC,
         c_count DESC;

-- =================================================================
-- Query ID: TPCHN14
-- =================================================================
SELECT 100.00 * SUM(CASE
                        WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount)
                        ELSE 0
                    END) / SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
FROM lineitem l
JOIN part p ON l.l_partkey = p.p_partkey
WHERE l.l_shipdate >= DATE '1995-01-01'
  AND l.l_shipdate < DATE '1995-02-01';

-- =================================================================
-- Query ID: TPCHN15
-- =================================================================
SELECT s.s_suppkey,
       s.s_name,
       s.s_address,
       s.s_phone,
       r.total_revenue
FROM supplier s
JOIN
  (SELECT l_suppkey AS supplier_no,
          SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
   FROM lineitem
   WHERE l_shipdate >= DATE '1995-01-01'
     AND l_shipdate < DATE '1995-01-01' + INTERVAL '3' MONTH
   GROUP BY l_suppkey) r ON s.s_suppkey = r.supplier_no
WHERE r.total_revenue =
    (SELECT MAX(total_revenue)
     FROM
       (SELECT SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
        FROM lineitem
        WHERE l_shipdate >= DATE '1995-01-01'
          AND l_shipdate < DATE '1995-01-01' + INTERVAL '3' MONTH
        GROUP BY l_suppkey) AS subquery)
ORDER BY s.s_suppkey;

-- =================================================================
-- Query ID: TPCHN16
-- =================================================================
SELECT p_brand,
       p_type,
       p_size,
       COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM part
JOIN partsupp ON p_partkey = ps_partkey
LEFT JOIN
  (SELECT s_suppkey
   FROM supplier
   WHERE s_comment LIKE '%Customer%Complaints%' ) AS excluded_suppliers ON ps_suppkey = excluded_suppliers.s_suppkey
WHERE p_brand <> 'Brand#23'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (1,
                 4,
                 7)
  AND excluded_suppliers.s_suppkey IS NULL
GROUP BY p_brand,
         p_type,
         p_size
ORDER BY supplier_cnt DESC,
         p_brand,
         p_type,
         p_size;

-- =================================================================
-- Query ID: TPCHN17
-- =================================================================
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem
JOIN part ON p_partkey = l_partkey
WHERE p_brand = 'Brand#53'
  AND p_container = 'MED BAG'
  AND l_quantity <
    (SELECT 0.7 * AVG(l_quantity)
     FROM lineitem AS li
     WHERE li.l_partkey = lineitem.l_partkey );

-- =================================================================
-- Query ID: TPCHN18
-- =================================================================
SELECT c.c_name,
       c.c_custkey,
       o.o_orderkey,
       o.o_orderdate,
       o.o_totalprice,
       SUM(l.l_quantity)
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
JOIN
  (SELECT l_orderkey
   FROM lineitem
   GROUP BY l_orderkey
   HAVING SUM(l_quantity) > 300) subquery ON o.o_orderkey = subquery.l_orderkey
GROUP BY c.c_name,
         c.c_custkey,
         o.o_orderkey,
         o.o_orderdate,
         o.o_totalprice
ORDER BY o.o_totalprice DESC,
         o.o_orderdate;

-- =================================================================
-- Query ID: TPCHN19
-- =================================================================
SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN part ON p_partkey = l_partkey
WHERE l_shipmode IN ('AIR',
                     'AIR REG')
  AND l_shipinstruct = 'DELIVER IN PERSON'
  AND ((p_brand = 'Brand#12'
        AND p_container IN ('SM CASE',
                            'SM BOX',
                            'SM PACK',
                            'SM PKG')
        AND l_quantity BETWEEN 1 AND 11
        AND p_size BETWEEN 1 AND 5)
       OR (p_brand = 'Brand#23'
           AND p_container IN ('MED BAG',
                               'MED BOX',
                               'MED PKG',
                               'MED PACK')
           AND l_quantity BETWEEN 10 AND 20
           AND p_size BETWEEN 1 AND 10)
       OR (p_brand = 'Brand#34'
           AND p_container IN ('LG CASE',
                               'LG BOX',
                               'LG PACK',
                               'LG PKG')
           AND l_quantity BETWEEN 20 AND 30
           AND p_size BETWEEN 1 AND 15));

-- =================================================================
-- Query ID: TPCHN20
-- =================================================================
SELECT s.s_name,
       s.s_address
FROM supplier s
JOIN nation n ON s.s_nationkey = n.n_nationkey
JOIN partsupp ps ON s.s_suppkey = ps.ps_suppkey
JOIN part p ON ps.ps_partkey = p.p_partkey
WHERE p.p_name LIKE '%ivory%'
  AND ps.ps_availqty >
    (SELECT 0.5 * SUM(l.l_quantity)
     FROM lineitem l
     WHERE l.l_partkey = ps.ps_partkey
       AND l.l_suppkey = ps.ps_suppkey
       AND l.l_shipdate >= DATE '1995-01-01'
       AND l.l_shipdate < DATE '1996-01-01' )
  AND n.n_name = 'FRANCE'
ORDER BY s.s_name;

-- =================================================================
-- Query ID: TPCHN21
-- =================================================================
SELECT s_name,
       COUNT(*) AS numwait
FROM supplier
JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
JOIN orders ON o_orderkey = l1.l_orderkey
JOIN nation ON s_nationkey = n_nationkey
WHERE o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND n_name = 'ARGENTINA'
  AND EXISTS
    (SELECT 1
     FROM lineitem l2
     WHERE l2.l_orderkey = l1.l_orderkey
       AND l2.l_suppkey <> l1.l_suppkey )
  AND NOT EXISTS
    (SELECT 1
     FROM lineitem l3
     WHERE l3.l_orderkey = l1.l_orderkey
       AND l3.l_suppkey <> l1.l_suppkey
       AND l3.l_receiptdate > l3.l_commitdate )
GROUP BY s_name
ORDER BY numwait DESC,
         s_name;

-- =================================================================
-- Query ID: TPCHN22
-- =================================================================
WITH avg_acctbal AS
  (SELECT avg(c_acctbal) AS avg_bal
   FROM customer
   WHERE c_acctbal > 0.00
     AND substring(c_phone
                   from 1
                   for 2) IN ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17') ),
     filtered_customers AS
  (SELECT substring(c_phone
                    from 1
                    for 2) AS cntrycode,
          c_acctbal,
          c_custkey
   FROM customer
   WHERE substring(c_phone
                   from 1
                   for 2) IN ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17')
     AND c_acctbal >
       (SELECT avg_bal
        FROM avg_acctbal) ),
     non_order_customers AS
  (SELECT cntrycode,
          c_acctbal
   FROM filtered_customers fc
   WHERE NOT EXISTS
       (SELECT 1
        FROM orders
        WHERE o_custkey = fc.c_custkey ) )
SELECT cntrycode,
       count(*) AS numcust,
       sum(c_acctbal) AS totacctbal
FROM non_order_customers
GROUP BY cntrycode
ORDER BY cntrycode;

-- =================================================================
-- Query ID: TPCHN23
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
GROUP BY l_returnflag,
         l_linestatus
ORDER BY l_returnflag,
         l_linestatus;

-- =================================================================
-- Query ID: TPCHN24
-- =================================================================
WITH regional_suppliers AS
  (SELECT s_suppkey,
          s_acctbal,
          s_name,
          s_address,
          s_phone,
          s_comment,
          n_name
   FROM supplier
   JOIN nation ON s_nationkey = n_nationkey
   JOIN region ON n_regionkey = r_regionkey
   WHERE r_name = 'EUROPE' ),
     min_supplycost AS
  (SELECT ps_partkey,
          MIN(ps_supplycost) AS min_cost
   FROM partsupp
   JOIN regional_suppliers ON s_suppkey = ps_suppkey
   GROUP BY ps_partkey)
SELECT s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
FROM part
JOIN partsupp ON part.p_partkey = partsupp.ps_partkey
JOIN regional_suppliers ON regional_suppliers.s_suppkey = partsupp.ps_suppkey
JOIN min_supplycost ON partsupp.ps_partkey = min_supplycost.ps_partkey
AND partsupp.ps_supplycost = min_supplycost.min_cost
WHERE p_size = 15
  AND p_type LIKE '%BRASS'
ORDER BY s_acctbal DESC,
         n_name,
         s_name,
         p_partkey
LIMIT 100;

-- =================================================================
-- Query ID: TPCHN25
-- =================================================================
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate,
       o_shippriority
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON c_custkey = o_custkey
WHERE c_mktsegment = 'FURNITURE'
  AND o_orderdate < DATE '1995-01-01'
  AND l_shipdate > DATE '1995-01-01'
GROUP BY l_orderkey,
         o_orderdate,
         o_shippriority
ORDER BY revenue DESC,
         o_orderdate;

-- =================================================================
-- Query ID: TPCHN26
-- =================================================================
SELECT o_orderpriority,
       COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-04-01'
  AND EXISTS
    (SELECT 1
     FROM lineitem
     WHERE l_orderkey = o_orderkey
       AND l_commitdate < l_receiptdate )
GROUP BY o_orderpriority
ORDER BY o_orderpriority;

-- =================================================================
-- Query ID: TPCHN27
-- =================================================================
SELECT n_name,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM region
JOIN nation ON n_regionkey = r_regionkey
JOIN supplier ON s_nationkey = n_nationkey
JOIN lineitem ON l_suppkey = s_suppkey
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON c_custkey = o_custkey
WHERE r_name = 'ASIA'
  AND o_orderdate >= DATE '1995-01-01'
  AND o_orderdate < DATE '1996-01-01'
GROUP BY n_name
ORDER BY revenue DESC;

-- =================================================================
-- Query ID: TPCHN28
-- =================================================================
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1993-01-01'
  AND l_shipdate < DATE '1995-03-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 10;

-- =================================================================
-- Query ID: TPCHN29
-- =================================================================
SELECT n1.n_name AS supp_nation,
       n2.n_name AS cust_nation,
       EXTRACT(YEAR
               FROM l.l_shipdate) AS l_year,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM lineitem l
JOIN supplier s ON s.s_suppkey = l.l_suppkey
JOIN orders o ON o.o_orderkey = l.l_orderkey
JOIN customer c ON c.c_custkey = o.o_custkey
JOIN nation n1 ON s.s_nationkey = n1.n_nationkey
JOIN nation n2 ON c.c_nationkey = n2.n_nationkey
WHERE ((n1.n_name = 'GERMANY'
        AND n2.n_name = 'FRANCE')
       OR (n1.n_name = 'FRANCE'
           AND n2.n_name = 'GERMANY'))
  AND l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
GROUP BY n1.n_name,
         n2.n_name,
         EXTRACT(YEAR
                 FROM l.l_shipdate)
ORDER BY n1.n_name,
         n2.n_name,
         EXTRACT(YEAR
                 FROM l.l_shipdate);

-- =================================================================
-- Query ID: TPCHN30
-- =================================================================
SELECT o_year,
       SUM(CASE
               WHEN nation = 'INDIA' THEN volume
               ELSE 0
           END) / SUM(volume) AS mkt_share
FROM
  (SELECT EXTRACT(YEAR
                  FROM o_orderdate) AS o_year,
          l_extendedprice * (1 - l_discount) AS volume,
          n2.n_name AS nation
   FROM lineitem
   JOIN orders ON l_orderkey = o_orderkey
   JOIN customer ON o_custkey = c_custkey
   JOIN nation n1 ON c_nationkey = n1.n_nationkey
   JOIN region ON n1.n_regionkey = r_regionkey
   JOIN supplier ON s_suppkey = l_suppkey
   JOIN nation n2 ON s_nationkey = n2.n_nationkey
   JOIN part ON p_partkey = l_partkey
   WHERE r_name = 'ASIA'
     AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
     AND p_type = 'ECONOMY ANODIZED STEEL' ) AS all_nations
GROUP BY o_year
ORDER BY o_year;

-- =================================================================
-- Query ID: TPCHN31
-- =================================================================
SELECT n_name AS nation,
       EXTRACT(YEAR
               FROM o_orderdate) AS o_year,
       SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit
FROM part
JOIN partsupp ON p_partkey = ps_partkey
JOIN lineitem ON l_partkey = p_partkey
AND l_suppkey = ps_suppkey
JOIN supplier ON s_suppkey = l_suppkey
JOIN orders ON o_orderkey = l_orderkey
JOIN nation ON s_nationkey = n_nationkey
WHERE p_name LIKE 'co%'
GROUP BY n_name,
         o_year
ORDER BY n_name,
         o_year DESC;

-- =================================================================
-- Query ID: TPCHN32
-- =================================================================
SELECT c.c_custkey,
       c.c_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
       c.c_acctbal,
       n.n_name,
       c.c_address,
       c.c_phone,
       c.c_comment
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
JOIN nation n ON c.c_nationkey = n.n_nationkey
WHERE o.o_orderdate >= DATE '1995-01-01'
  AND o.o_orderdate < DATE '1995-04-01'
  AND l.l_returnflag = 'R'
GROUP BY c.c_custkey,
         c.c_name,
         c.c_acctbal,
         c.c_phone,
         n.n_name,
         c.c_address,
         c.c_comment
ORDER BY revenue DESC;

-- =================================================================
-- Query ID: TPCHN33
-- =================================================================
WITH total_value_cte AS
  (SELECT ps_partkey,
          SUM(ps_supplycost * ps_availqty) AS total_value
   FROM partsupp
   JOIN supplier ON ps_suppkey = s_suppkey
   JOIN nation ON s_nationkey = n_nationkey
   WHERE n_name = 'INDIA'
   GROUP BY ps_partkey),
     threshold AS
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001 AS threshold_value
   FROM partsupp
   JOIN supplier ON ps_suppkey = s_suppkey
   JOIN nation ON s_nationkey = n_nationkey
   WHERE n_name = 'INDIA' )
SELECT ps_partkey,
       'INDIA' AS n_name,
       total_value
FROM total_value_cte,
     threshold
WHERE total_value > threshold.threshold_value
ORDER BY total_value DESC;

-- =================================================================
-- Query ID: TPCHN34
-- =================================================================
SELECT l_shipmode,
       SUM(CASE
               WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1
               ELSE 0
           END) AS high_line_count,
       SUM(CASE
               WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1
               ELSE 0
           END) AS low_line_count
FROM orders
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_shipmode = 'SHIP'
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1995-01-01'
  AND l_receiptdate < DATE '1996-01-01'
GROUP BY l_shipmode
ORDER BY l_shipmode;

-- =================================================================
-- Query ID: TPCHN35
-- =================================================================
SELECT c_count,
       c_orderdate,
       COUNT(*) AS custdist
FROM
  (SELECT c.c_custkey,
          o.o_orderdate AS c_orderdate,
          COUNT(o.o_orderkey) AS c_count
   FROM customer c
   LEFT JOIN orders o ON c.c_custkey = o.o_custkey
   AND o.o_comment NOT LIKE '%special%requests%'
   GROUP BY c.c_custkey,
            o.o_orderdate) AS c_orders
GROUP BY c_count,
         c_orderdate
ORDER BY custdist DESC,
         c_count DESC;

-- =================================================================
-- Query ID: TPCHN36
-- =================================================================
SELECT 100.00 * SUM(CASE
                        WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
                        ELSE 0
                    END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem
JOIN part ON l_partkey = p_partkey
WHERE l_shipdate >= DATE '1995-01-01'
  AND l_shipdate < DATE '1995-02-01';

-- =================================================================
-- Query ID: TPCHN37
-- =================================================================
SELECT s.s_suppkey,
       s.s_name,
       s.s_address,
       s.s_phone,
       r.total_revenue
FROM supplier s
JOIN
  (SELECT l_suppkey AS supplier_no,
          SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
   FROM lineitem
   WHERE l_shipdate >= DATE '1995-01-01'
     AND l_shipdate < DATE '1995-04-01'
   GROUP BY l_suppkey) r ON s.s_suppkey = r.supplier_no
WHERE r.total_revenue =
    (SELECT MAX(total_revenue)
     FROM
       (SELECT SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
        FROM lineitem
        WHERE l_shipdate >= DATE '1995-01-01'
          AND l_shipdate < DATE '1995-04-01'
        GROUP BY l_suppkey) AS subquery)
ORDER BY s.s_suppkey;

-- =================================================================
-- Query ID: TPCHN38
-- =================================================================
SELECT p_brand,
       p_type,
       p_size,
       COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM part
JOIN partsupp ON p_partkey = ps_partkey
LEFT JOIN
  (SELECT s_suppkey
   FROM supplier
   WHERE s_comment LIKE '%Customer%Complaints%' ) AS filtered_suppliers ON ps_suppkey = filtered_suppliers.s_suppkey
WHERE p_brand <> 'Brand#23'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (1,
                 4,
                 7)
  AND filtered_suppliers.s_suppkey IS NULL
GROUP BY p_brand,
         p_type,
         p_size
ORDER BY supplier_cnt DESC,
         p_brand,
         p_type,
         p_size;

-- =================================================================
-- Query ID: TPCHN39
-- =================================================================
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem
JOIN part ON p_partkey = l_partkey
WHERE p_brand = 'Brand#53'
  AND p_container = 'MED BAG'
  AND l_quantity <
    (SELECT 0.7 * AVG(l_quantity)
     FROM lineitem AS sub_lineitem
     WHERE sub_lineitem.l_partkey = lineitem.l_partkey );

-- =================================================================
-- Query ID: TPCHN40
-- =================================================================
SELECT c.c_name,
       c.c_custkey,
       o.o_orderkey,
       o.o_orderdate,
       o.o_totalprice,
       SUM(l.l_quantity)
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
JOIN
  (SELECT l_orderkey
   FROM lineitem
   GROUP BY l_orderkey
   HAVING SUM(l_quantity) > 300) subquery ON o.o_orderkey = subquery.l_orderkey
GROUP BY c.c_name,
         c.c_custkey,
         o.o_orderkey,
         o.o_orderdate,
         o.o_totalprice
ORDER BY o.o_totalprice DESC,
         o.o_orderdate;

-- =================================================================
-- Query ID: TPCHN41
-- =================================================================
SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN part ON p_partkey = l_partkey
WHERE l_shipmode IN ('AIR',
                     'AIR REG')
  AND l_shipinstruct = 'DELIVER IN PERSON'
  AND ((p_brand = 'Brand#12'
        AND p_container IN ('SM CASE',
                            'SM BOX',
                            'SM PACK',
                            'SM PKG')
        AND l_quantity BETWEEN 1 AND 11
        AND p_size BETWEEN 1 AND 5)
       OR (p_brand = 'Brand#23'
           AND p_container IN ('MED BAG',
                               'MED BOX',
                               'MED PKG',
                               'MED PACK')
           AND l_quantity BETWEEN 10 AND 20
           AND p_size BETWEEN 1 AND 10)
       OR (p_brand = 'Brand#34'
           AND p_container IN ('LG CASE',
                               'LG BOX',
                               'LG PACK',
                               'LG PKG')
           AND l_quantity BETWEEN 20 AND 30
           AND p_size BETWEEN 1 AND 15));

-- =================================================================
-- Query ID: TPCHN42
-- =================================================================
SELECT s.s_name,
       s.s_address
FROM supplier s
JOIN nation n ON s.s_nationkey = n.n_nationkey
WHERE n.n_name = 'FRANCE'
  AND EXISTS
    (SELECT 1
     FROM partsupp ps
     JOIN part p ON ps.ps_partkey = p.p_partkey
     WHERE p.p_name LIKE '%ivory%'
       AND ps.ps_suppkey = s.s_suppkey
       AND ps.ps_availqty >
         (SELECT 0.5 * SUM(l.l_quantity)
          FROM lineitem l
          WHERE l.l_partkey = ps.ps_partkey
            AND l.l_suppkey = ps.ps_suppkey
            AND l.l_shipdate >= DATE '1995-01-01'
            AND l.l_shipdate < DATE '1996-01-01' ) )
ORDER BY s.s_name;

-- =================================================================
-- Query ID: TPCHN43
-- =================================================================
SELECT s_name,
       COUNT(*) AS numwait
FROM supplier
JOIN nation ON s_nationkey = n_nationkey
JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
JOIN orders ON o_orderkey = l1.l_orderkey
WHERE o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND n_name = 'ARGENTINA'
  AND EXISTS
    (SELECT 1
     FROM lineitem l2
     WHERE l2.l_orderkey = l1.l_orderkey
       AND l2.l_suppkey <> l1.l_suppkey )
  AND NOT EXISTS
    (SELECT 1
     FROM lineitem l3
     WHERE l3.l_orderkey = l1.l_orderkey
       AND l3.l_suppkey <> l1.l_suppkey
       AND l3.l_receiptdate > l3.l_commitdate )
GROUP BY s_name
ORDER BY numwait DESC,
         s_name;

-- =================================================================
-- Query ID: TPCHN44
-- =================================================================
WITH avg_acctbal AS
  (SELECT avg(c_acctbal) AS avg_balance
   FROM customer
   WHERE c_acctbal > 0.00
     AND substring(c_phone
                   from 1
                   for 2) IN ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17') ),
     filtered_customers AS
  (SELECT substring(c_phone
                    from 1
                    for 2) AS cntrycode,
          c_acctbal,
          c_custkey
   FROM customer
   WHERE substring(c_phone
                   from 1
                   for 2) IN ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17')
     AND c_acctbal >
       (SELECT avg_balance
        FROM avg_acctbal)
     AND NOT EXISTS
       (SELECT 1
        FROM orders
        WHERE o_custkey = c_custkey ) )
SELECT cntrycode,
       count(*) AS numcust,
       sum(c_acctbal) AS totacctbal
FROM filtered_customers
GROUP BY cntrycode
ORDER BY cntrycode;

-- =================================================================
-- Query ID: TPCHN45
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
GROUP BY l_returnflag,
         l_linestatus
ORDER BY l_returnflag,
         l_linestatus;

-- =================================================================
-- Query ID: TPCHN46
-- =================================================================
WITH MinSupplyCost AS
  (SELECT ps_partkey,
          MIN(ps_supplycost) AS min_supplycost
   FROM partsupp
   JOIN supplier ON supplier.s_suppkey = partsupp.ps_suppkey
   JOIN nation ON supplier.s_nationkey = nation.n_nationkey
   JOIN region ON nation.n_regionkey = region.r_regionkey
   WHERE region.r_name = 'EUROPE'
   GROUP BY ps_partkey)
SELECT s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
FROM part
JOIN partsupp ON part.p_partkey = partsupp.ps_partkey
JOIN supplier ON supplier.s_suppkey = partsupp.ps_suppkey
JOIN nation ON supplier.s_nationkey = nation.n_nationkey
JOIN region ON nation.n_regionkey = region.r_regionkey
JOIN MinSupplyCost ON partsupp.ps_partkey = MinSupplyCost.ps_partkey
AND partsupp.ps_supplycost = MinSupplyCost.min_supplycost
WHERE part.p_size = 15
  AND part.p_type LIKE '%BRASS'
  AND region.r_name = 'EUROPE'
ORDER BY s_acctbal DESC,
         n_name,
         s_name,
         p_partkey
LIMIT 100;

-- =================================================================
-- Query ID: TPCHN47
-- =================================================================
SELECT l.l_orderkey,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
       o.o_orderdate,
       o.o_shippriority
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
WHERE c.c_mktsegment = 'FURNITURE'
  AND o.o_orderdate < DATE '1995-01-01'
  AND l.l_shipdate > DATE '1995-01-01'
GROUP BY l.l_orderkey,
         o.o_orderdate,
         o.o_shippriority
ORDER BY revenue DESC,
         o.o_orderdate;

-- =================================================================
-- Query ID: TPCHN48
-- =================================================================
SELECT o_orderpriority,
       COUNT(*) AS order_count
FROM orders
JOIN lineitem ON l_orderkey = o_orderkey
AND l_commitdate < l_receiptdate
WHERE o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '3' MONTH
GROUP BY o_orderpriority
ORDER BY o_orderpriority;

-- =================================================================
-- Query ID: TPCHN49
-- =================================================================
SELECT n.n_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM region r
JOIN nation n ON n.n_regionkey = r.r_regionkey
JOIN supplier s ON s.s_nationkey = n.n_nationkey
JOIN lineitem l ON l.l_suppkey = s.s_suppkey
JOIN orders o ON o.o_orderkey = l.l_orderkey
JOIN customer c ON c.c_custkey = o.o_custkey
WHERE r.r_name = 'ASIA'
  AND o.o_orderdate >= DATE '1995-01-01'
  AND o.o_orderdate < DATE '1996-01-01'
GROUP BY n.n_name
ORDER BY revenue DESC;

-- =================================================================
-- Query ID: TPCHN50
-- =================================================================
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1993-01-01'
  AND l_shipdate < DATE '1995-03-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 10;

-- =================================================================
-- Query ID: TPCHN51
-- =================================================================
SELECT n1.n_name AS supp_nation,
       n2.n_name AS cust_nation,
       EXTRACT(YEAR
               FROM l_shipdate) AS l_year,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM supplier
JOIN lineitem ON s_suppkey = l_suppkey
JOIN orders ON o_orderkey = l_orderkey
JOIN customer ON c_custkey = o_custkey
JOIN nation n1 ON s_nationkey = n1.n_nationkey
JOIN nation n2 ON c_nationkey = n2.n_nationkey
WHERE ((n1.n_name = 'GERMANY'
        AND n2.n_name = 'FRANCE')
       OR (n1.n_name = 'FRANCE'
           AND n2.n_name = 'GERMANY'))
  AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
GROUP BY n1.n_name,
         n2.n_name,
         EXTRACT(YEAR
                 FROM l_shipdate)
ORDER BY n1.n_name,
         n2.n_name,
         EXTRACT(YEAR
                 FROM l_shipdate);

-- =================================================================
-- Query ID: TPCHN52
-- =================================================================
WITH filtered_orders AS
  (SELECT o_orderkey,
          EXTRACT(YEAR
                  FROM o_orderdate) AS o_year
   FROM orders
   WHERE o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' ),
     filtered_parts AS
  (SELECT p_partkey
   FROM part
   WHERE p_type = 'ECONOMY ANODIZED STEEL' ),
     filtered_customers AS
  (SELECT c_custkey
   FROM customer
   JOIN nation n1 ON c_nationkey = n1.n_nationkey
   JOIN region ON n1.n_regionkey = r_regionkey
   WHERE r_name = 'ASIA' ),
     lineitem_volume AS
  (SELECT l_orderkey,
          l_extendedprice * (1 - l_discount) AS volume,
          l_suppkey
   FROM lineitem
   JOIN filtered_parts ON l_partkey = p_partkey),
     nation_supplier AS
  (SELECT s_suppkey,
          n2.n_name AS nation
   FROM supplier
   JOIN nation n2 ON s_nationkey = n2.n_nationkey)
SELECT o_year,
       SUM(CASE
               WHEN nation = 'INDIA' THEN volume
               ELSE 0
           END) / SUM(volume) AS mkt_share
FROM filtered_orders
JOIN lineitem_volume ON filtered_orders.o_orderkey = lineitem_volume.l_orderkey
JOIN nation_supplier ON lineitem_volume.l_suppkey = nation_supplier.s_suppkey
JOIN filtered_customers ON filtered_orders.o_orderkey = filtered_customers.c_custkey
GROUP BY o_year
ORDER BY o_year;

-- =================================================================
-- Query ID: TPCHN53
-- =================================================================
SELECT n.n_name AS nation,
       EXTRACT(YEAR
               FROM o.o_orderdate) AS o_year,
       SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit
FROM nation n
JOIN supplier s ON s.s_nationkey = n.n_nationkey
JOIN lineitem l ON l.l_suppkey = s.s_suppkey
JOIN partsupp ps ON ps.ps_suppkey = l.l_suppkey
AND ps.ps_partkey = l.l_partkey
JOIN part p ON p.p_partkey = l.l_partkey
JOIN orders o ON o.o_orderkey = l.l_orderkey
WHERE p.p_name LIKE 'co%'
GROUP BY n.n_name,
         o_year
ORDER BY n.n_name,
         o_year DESC;

-- =================================================================
-- Query ID: TPCHN54
-- =================================================================
SELECT c.c_custkey,
       c.c_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
       c.c_acctbal,
       n.n_name,
       c.c_address,
       c.c_phone,
       c.c_comment
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
JOIN nation n ON c.c_nationkey = n.n_nationkey
WHERE o.o_orderdate >= DATE '1995-01-01'
  AND o.o_orderdate < DATE '1995-01-01' + INTERVAL '3' MONTH
  AND l.l_returnflag = 'R'
GROUP BY c.c_custkey,
         c.c_name,
         c.c_acctbal,
         c.c_phone,
         n.n_name,
         c.c_address,
         c.c_comment
ORDER BY revenue DESC;

-- =================================================================
-- Query ID: TPCHN55
-- =================================================================
WITH total_supplycost AS
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001 AS threshold_value
   FROM partsupp
   JOIN supplier ON ps_suppkey = s_suppkey
   JOIN nation ON s_nationkey = n_nationkey
   WHERE n_name = 'INDIA' )
SELECT ps_partkey,
       n_name,
       SUM(ps_supplycost * ps_availqty) AS total_value
FROM partsupp
JOIN supplier ON ps_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
WHERE n_name = 'INDIA'
GROUP BY ps_partkey,
         n_name
HAVING SUM(ps_supplycost * ps_availqty) >
  (SELECT threshold_value
   FROM total_supplycost)
ORDER BY total_value DESC;

-- =================================================================
-- Query ID: TPCHN56
-- =================================================================
SELECT l_shipmode,
       SUM(CASE
               WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1
               ELSE 0
           END) AS high_line_count,
       SUM(CASE
               WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1
               ELSE 0
           END) AS low_line_count
FROM orders
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_shipmode = 'SHIP'
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1995-01-01'
  AND l_receiptdate < DATE '1995-01-01' + INTERVAL '1' YEAR
GROUP BY l_shipmode
ORDER BY l_shipmode;

-- =================================================================
-- Query ID: TPCHN57
-- =================================================================
WITH filtered_orders AS
  (SELECT o_custkey,
          o_orderdate
   FROM orders
   WHERE o_comment NOT LIKE '%special%requests%' )
SELECT COUNT(o.o_orderdate) AS c_count,
       o.o_orderdate AS c_orderdate,
       COUNT(DISTINCT c.c_custkey) AS custdist
FROM customer c
LEFT JOIN filtered_orders o ON c.c_custkey = o.o_custkey
GROUP BY o.o_orderdate
ORDER BY custdist DESC,
         c_count DESC;

-- =================================================================
-- Query ID: TPCHN58
-- =================================================================
SELECT 100.00 * SUM(CASE
                        WHEN p_type LIKE 'PROMO%' THEN extended_price_discount
                        ELSE 0
                    END) / SUM(extended_price_discount) AS promo_revenue
FROM
  (SELECT l_extendedprice * (1 - l_discount) AS extended_price_discount,
          p_type
   FROM lineitem
   JOIN part ON l_partkey = p_partkey
   WHERE l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1995-01-01' + INTERVAL '1' MONTH - INTERVAL '1' DAY ) AS subquery;

-- =================================================================
-- Query ID: TPCHN59
-- =================================================================
WITH revenue AS
  (SELECT l_suppkey,
          SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
   FROM lineitem
   WHERE l_shipdate >= DATE '1995-01-01'
     AND l_shipdate < DATE '1995-01-01' + INTERVAL '3' MONTH
   GROUP BY l_suppkey),
     max_revenue AS
  (SELECT MAX(total_revenue) AS max_total_revenue
   FROM revenue)
SELECT s.s_suppkey,
       s.s_name,
       s.s_address,
       s.s_phone,
       r.total_revenue
FROM supplier s
JOIN revenue r ON s.s_suppkey = r.l_suppkey
JOIN max_revenue mr ON r.total_revenue = mr.max_total_revenue
ORDER BY s.s_suppkey;

-- =================================================================
-- Query ID: TPCHN60
-- =================================================================
SELECT p.p_brand,
       p.p_type,
       p.p_size,
       COUNT(DISTINCT ps.ps_suppkey) AS supplier_cnt
FROM part p
JOIN partsupp ps ON p.p_partkey = ps.ps_partkey
LEFT JOIN supplier s ON ps.ps_suppkey = s.s_suppkey
AND s.s_comment LIKE '%Customer%Complaints%'
WHERE p.p_brand <> 'Brand#23'
  AND p.p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p.p_size IN (1,
                   4,
                   7)
  AND s.s_suppkey IS NULL
GROUP BY p.p_brand,
         p.p_type,
         p.p_size
ORDER BY supplier_cnt DESC,
         p.p_brand,
         p.p_type,
         p.p_size;

-- =================================================================
-- Query ID: TPCHN61
-- =================================================================
WITH avg_quantity AS
  (SELECT l_partkey,
          0.7 * AVG(l_quantity) AS threshold
   FROM lineitem
   GROUP BY l_partkey)
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem
JOIN part ON part.p_partkey = lineitem.l_partkey
JOIN avg_quantity ON avg_quantity.l_partkey = lineitem.l_partkey
WHERE part.p_brand = 'Brand#53'
  AND part.p_container = 'MED BAG'
  AND lineitem.l_quantity < avg_quantity.threshold;

-- =================================================================
-- Query ID: TPCHN62
-- =================================================================
SELECT c.c_name,
       c.c_custkey,
       o.o_orderkey,
       o.o_orderdate,
       o.o_totalprice,
       SUM(l.l_quantity)
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
JOIN
  (SELECT l_orderkey
   FROM lineitem
   GROUP BY l_orderkey
   HAVING SUM(l_quantity) > 300) subquery ON o.o_orderkey = subquery.l_orderkey
GROUP BY c.c_name,
         c.c_custkey,
         o.o_orderkey,
         o.o_orderdate,
         o.o_totalprice
ORDER BY o.o_totalprice DESC,
         o.o_orderdate;

-- =================================================================
-- Query ID: TPCHN63
-- =================================================================
SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN part ON p_partkey = l_partkey
WHERE l_shipmode IN ('AIR',
                     'AIR REG')
  AND l_shipinstruct = 'DELIVER IN PERSON'
  AND ((p_brand = 'Brand#12'
        AND p_container IN ('SM CASE',
                            'SM BOX',
                            'SM PACK',
                            'SM PKG')
        AND l_quantity BETWEEN 1 AND 11
        AND p_size BETWEEN 1 AND 5)
       OR (p_brand = 'Brand#23'
           AND p_container IN ('MED BAG',
                               'MED BOX',
                               'MED PKG',
                               'MED PACK')
           AND l_quantity BETWEEN 10 AND 20
           AND p_size BETWEEN 1 AND 10)
       OR (p_brand = 'Brand#34'
           AND p_container IN ('LG CASE',
                               'LG BOX',
                               'LG PACK',
                               'LG PKG')
           AND l_quantity BETWEEN 20 AND 30
           AND p_size BETWEEN 1 AND 15));

-- =================================================================
-- Query ID: TPCHN64
-- =================================================================
WITH PartKeys AS
  (SELECT p_partkey
   FROM part
   WHERE p_name LIKE '%ivory%' ),
     PartSuppKeys AS
  (SELECT ps_suppkey
   FROM partsupp
   JOIN PartKeys ON partsupp.ps_partkey = PartKeys.p_partkey
   WHERE ps_availqty >
       (SELECT 0.5 * SUM(l_quantity)
        FROM lineitem
        WHERE l_partkey = partsupp.ps_partkey
          AND l_suppkey = partsupp.ps_suppkey
          AND l_shipdate >= DATE '1995-01-01'
          AND l_shipdate < DATE '1996-01-01' ) )
SELECT s_name,
       s_address
FROM supplier
JOIN PartSuppKeys ON supplier.s_suppkey = PartSuppKeys.ps_suppkey
JOIN nation ON supplier.s_nationkey = nation.n_nationkey
WHERE nation.n_name = 'FRANCE'
ORDER BY s_name;

-- =================================================================
-- Query ID: TPCHN65
-- =================================================================
SELECT s.s_name,
       COUNT(*) AS numwait
FROM supplier s
JOIN lineitem l1 ON s.s_suppkey = l1.l_suppkey
JOIN orders o ON o.o_orderkey = l1.l_orderkey
JOIN nation n ON s.s_nationkey = n.n_nationkey
LEFT JOIN lineitem l2 ON l2.l_orderkey = l1.l_orderkey
AND l2.l_suppkey <> l1.l_suppkey
LEFT JOIN lineitem l3 ON l3.l_orderkey = l1.l_orderkey
AND l3.l_suppkey <> l1.l_suppkey
AND l3.l_receiptdate > l3.l_commitdate
WHERE o.o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND n.n_name = 'ARGENTINA'
  AND l2.l_orderkey IS NOT NULL
  AND l3.l_orderkey IS NULL
GROUP BY s.s_name
ORDER BY numwait DESC,
         s.s_name;

-- =================================================================
-- Query ID: TPCHN66
-- =================================================================
WITH avg_acctbal AS
  (SELECT avg(c_acctbal) AS avg_bal
   FROM customer
   WHERE c_acctbal > 0.00
     AND substring(c_phone
                   from 1
                   for 2) IN ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17') ),
     filtered_customers AS
  (SELECT c_custkey,
          substring(c_phone
                    from 1
                    for 2) AS cntrycode,
          c_acctbal
   FROM customer
   WHERE substring(c_phone
                   from 1
                   for 2) IN ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17')
     AND c_acctbal >
       (SELECT avg_bal
        FROM avg_acctbal)
     AND NOT EXISTS
       (SELECT 1
        FROM orders
        WHERE o_custkey = c_custkey ) )
SELECT cntrycode,
       count(*) AS numcust,
       sum(c_acctbal) AS totacctbal
FROM filtered_customers
GROUP BY cntrycode
ORDER BY cntrycode;

-- =================================================================
-- Query ID: TPCHN67
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
GROUP BY l_returnflag,
         l_linestatus
ORDER BY l_returnflag,
         l_linestatus;

-- =================================================================
-- Query ID: TPCHN68
-- =================================================================
WITH MinSupplyCost AS
  (SELECT ps_partkey,
          MIN(ps_supplycost) AS min_supplycost
   FROM partsupp
   JOIN supplier ON s_suppkey = ps_suppkey
   JOIN nation ON s_nationkey = n_nationkey
   JOIN region ON n_regionkey = r_regionkey
   WHERE r_name = 'EUROPE'
   GROUP BY ps_partkey)
SELECT s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
FROM part
JOIN partsupp ON p_partkey = ps_partkey
JOIN supplier ON s_suppkey = ps_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
JOIN MinSupplyCost ON part.p_partkey = MinSupplyCost.ps_partkey
AND partsupp.ps_supplycost = MinSupplyCost.min_supplycost
WHERE p_size = 15
  AND p_type LIKE '%BRASS'
  AND r_name = 'EUROPE'
ORDER BY s_acctbal DESC,
         n_name,
         s_name,
         p_partkey
LIMIT 100;

-- =================================================================
-- Query ID: TPCHN69
-- =================================================================
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate,
       o_shippriority
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
WHERE c_mktsegment = 'FURNITURE'
  AND o_orderdate < DATE '1995-01-01'
  AND l_shipdate > DATE '1995-01-01'
GROUP BY l_orderkey,
         o_orderdate,
         o_shippriority
ORDER BY revenue DESC,
         o_orderdate;

-- =================================================================
-- Query ID: TPCHN70
-- =================================================================
SELECT o_orderpriority,
       COUNT(DISTINCT o_orderkey) AS order_count
FROM orders
JOIN lineitem ON l_orderkey = o_orderkey
AND l_commitdate < l_receiptdate
WHERE o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '3' MONTH
GROUP BY o_orderpriority
ORDER BY o_orderpriority;

-- =================================================================
-- Query ID: TPCHN71
-- =================================================================
WITH filtered_region AS
  (SELECT r_regionkey
   FROM region
   WHERE r_name = 'ASIA' ),
     filtered_orders AS
  (SELECT o_orderkey,
          o_custkey
   FROM orders
   WHERE o_orderdate >= DATE '1995-01-01'
     AND o_orderdate < DATE '1996-01-01' )
SELECT n.n_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM customer c
JOIN filtered_orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
JOIN supplier s ON l.l_suppkey = s.s_suppkey
JOIN nation n ON c.c_nationkey = n.n_nationkey
AND s.s_nationkey = n.n_nationkey
JOIN filtered_region r ON n.n_regionkey = r.r_regionkey
GROUP BY n.n_name
ORDER BY revenue DESC;

-- =================================================================
-- Query ID: TPCHN72
-- =================================================================
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1993-01-01'
  AND l_shipdate < DATE '1994-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 10;

-- =================================================================
-- Query ID: TPCHN73
-- =================================================================
WITH shipping AS
  (SELECT n1.n_name AS supp_nation,
          n2.n_name AS cust_nation,
          EXTRACT(YEAR
                  FROM l_shipdate) AS l_year,
          l_extendedprice * (1 - l_discount) AS volume
   FROM supplier
   JOIN lineitem ON s_suppkey = l_suppkey
   JOIN orders ON o_orderkey = l_orderkey
   JOIN customer ON c_custkey = o_custkey
   JOIN nation n1 ON s_nationkey = n1.n_nationkey
   JOIN nation n2 ON c_nationkey = n2.n_nationkey
   WHERE ((n1.n_name = 'GERMANY'
           AND n2.n_name = 'FRANCE')
          OR (n1.n_name = 'FRANCE'
              AND n2.n_name = 'GERMANY'))
     AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' )
SELECT supp_nation,
       cust_nation,
       l_year,
       SUM(volume) AS revenue
FROM shipping
GROUP BY supp_nation,
         cust_nation,
         l_year
ORDER BY supp_nation,
         cust_nation,
         l_year;

-- =================================================================
-- Query ID: TPCHN74
-- =================================================================
SELECT o_year,
       SUM(CASE
               WHEN nation = 'INDIA' THEN volume
               ELSE 0
           END) / SUM(volume) AS mkt_share
FROM
  (SELECT EXTRACT(YEAR
                  FROM o.o_orderdate) AS o_year,
          l.l_extendedprice * (1 - l.l_discount) AS volume,
          n2.n_name AS nation
   FROM lineitem l
   JOIN orders o ON l.l_orderkey = o.o_orderkey
   JOIN customer c ON o.o_custkey = c.c_custkey
   JOIN nation n1 ON c.c_nationkey = n1.n_nationkey
   JOIN region r ON n1.n_regionkey = r.r_regionkey
   JOIN supplier s ON l.l_suppkey = s.s_suppkey
   JOIN nation n2 ON s.s_nationkey = n2.n_nationkey
   JOIN part p ON l.l_partkey = p.p_partkey
   WHERE r.r_name = 'ASIA'
     AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
     AND p.p_type = 'ECONOMY ANODIZED STEEL' ) AS all_nations
GROUP BY o_year
ORDER BY o_year;

-- =================================================================
-- Query ID: TPCHN75
-- =================================================================
SELECT n.n_name AS nation,
       EXTRACT(YEAR
               FROM o.o_orderdate) AS o_year,
       SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit
FROM part p
JOIN lineitem l ON p.p_partkey = l.l_partkey
JOIN partsupp ps ON ps.ps_suppkey = l.l_suppkey
AND ps.ps_partkey = l.l_partkey
JOIN supplier s ON s.s_suppkey = l.l_suppkey
JOIN orders o ON o.o_orderkey = l.l_orderkey
JOIN nation n ON s.s_nationkey = n.n_nationkey
WHERE p.p_name LIKE 'co%'
GROUP BY n.n_name,
         o_year
ORDER BY n.n_name,
         o_year DESC;

-- =================================================================
-- Query ID: TPCHN76
-- =================================================================
SELECT c.c_custkey,
       c.c_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
       c.c_acctbal,
       n.n_name,
       c.c_address,
       c.c_phone,
       c.c_comment
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
JOIN nation n ON c.c_nationkey = n.n_nationkey
WHERE o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-03-31'
  AND l.l_returnflag = 'R'
GROUP BY c.c_custkey,
         c.c_name,
         c.c_acctbal,
         c.c_phone,
         n.n_name,
         c.c_address,
         c.c_comment
ORDER BY revenue DESC;

-- =================================================================
-- Query ID: TPCHN77
-- =================================================================
WITH total_value_cte AS
  (SELECT ps_partkey,
          SUM(ps_supplycost * ps_availqty) AS total_value
   FROM partsupp
   JOIN supplier ON ps_suppkey = s_suppkey
   JOIN nation ON s_nationkey = n_nationkey
   WHERE n_name = 'INDIA'
   GROUP BY ps_partkey),
     threshold AS
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001 AS threshold_value
   FROM partsupp
   JOIN supplier ON ps_suppkey = s_suppkey
   JOIN nation ON s_nationkey = n_nationkey
   WHERE n_name = 'INDIA' )
SELECT ps_partkey,
       'INDIA' AS n_name,
       total_value
FROM total_value_cte,
     threshold
WHERE total_value > threshold.threshold_value
ORDER BY total_value DESC;

-- =================================================================
-- Query ID: TPCHN78
-- =================================================================
SELECT l_shipmode,
       SUM(CASE
               WHEN o_orderpriority = '1-URGENT'
                    OR o_orderpriority = '2-HIGH' THEN 1
               ELSE 0
           END) AS high_line_count,
       SUM(CASE
               WHEN o_orderpriority <> '1-URGENT'
                    AND o_orderpriority <> '2-HIGH' THEN 1
               ELSE 0
           END) AS low_line_count
FROM orders
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_shipmode = 'SHIP'
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate BETWEEN DATE '1995-01-01' AND DATE '1995-12-31'
GROUP BY l_shipmode
ORDER BY l_shipmode;

-- =================================================================
-- Query ID: TPCHN79
-- =================================================================
SELECT c_count,
       o_orderdate,
       COUNT(*) AS custdist
FROM
  (SELECT c.c_custkey,
          o.o_orderdate,
          COUNT(o.o_orderkey) AS c_count
   FROM customer c
   LEFT JOIN orders o ON c.c_custkey = o.o_custkey
   WHERE o.o_comment NOT LIKE '%special%requests%'
   GROUP BY c.c_custkey,
            o.o_orderdate) AS c_orders
GROUP BY c_count,
         o_orderdate
ORDER BY custdist DESC,
         c_count DESC;

-- =================================================================
-- Query ID: TPCHN80
-- =================================================================
SELECT 100.00 * SUM(CASE
                        WHEN p_type LIKE 'PROMO%' THEN revenue
                        ELSE 0
                    END) / SUM(revenue) AS promo_revenue
FROM
  (SELECT l_extendedprice * (1 - l_discount) AS revenue,
          p_type
   FROM lineitem
   JOIN part ON l_partkey = p_partkey
   WHERE l_shipdate >= DATE '1995-01-01'
     AND l_shipdate < DATE '1995-02-01') AS subquery;

-- =================================================================
-- Query ID: TPCHN81
-- =================================================================
SELECT s.s_suppkey,
       s.s_name,
       s.s_address,
       s.s_phone,
       r.total_revenue
FROM
  (SELECT l_suppkey AS supplier_no,
          SUM(l_extendedprice * (1 - l_discount)) AS total_revenue,
          MAX(SUM(l_extendedprice * (1 - l_discount))) OVER () AS max_revenue
   FROM lineitem
   WHERE l_shipdate >= DATE '1995-01-01'
     AND l_shipdate < DATE '1995-01-01' + INTERVAL '3' MONTH
   GROUP BY l_suppkey) r
JOIN supplier s ON s.s_suppkey = r.supplier_no
WHERE r.total_revenue = r.max_revenue
ORDER BY s.s_suppkey;

-- =================================================================
-- Query ID: TPCHN82
-- =================================================================
SELECT p.p_brand,
       p.p_type,
       p.p_size,
       COUNT(DISTINCT ps.ps_suppkey) AS supplier_cnt
FROM part p
JOIN partsupp ps ON p.p_partkey = ps.ps_partkey
LEFT JOIN supplier s ON ps.ps_suppkey = s.s_suppkey
AND s.s_comment LIKE '%Customer%Complaints%'
WHERE p.p_brand <> 'Brand#23'
  AND p.p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p.p_size IN (1,
                   4,
                   7)
  AND s.s_suppkey IS NULL
GROUP BY p.p_brand,
         p.p_type,
         p.p_size
ORDER BY supplier_cnt DESC,
         p.p_brand,
         p.p_type,
         p.p_size;

-- =================================================================
-- Query ID: TPCHN83
-- =================================================================
WITH avg_quantity AS
  (SELECT l_partkey,
          0.7 * AVG(l_quantity) AS threshold_quantity
   FROM lineitem
   GROUP BY l_partkey)
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem
JOIN part ON p_partkey = l_partkey
JOIN avg_quantity ON lineitem.l_partkey = avg_quantity.l_partkey
WHERE p_brand = 'Brand#53'
  AND p_container = 'MED BAG'
  AND l_quantity < avg_quantity.threshold_quantity;

-- =================================================================
-- Query ID: TPCHN84
-- =================================================================
SELECT c.c_name,
       c.c_custkey,
       o.o_orderkey,
       o.o_orderdate,
       o.o_totalprice,
       SUM(l.l_quantity) AS total_quantity
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
JOIN
  (SELECT l_orderkey
   FROM lineitem
   GROUP BY l_orderkey
   HAVING SUM(l_quantity) > 300) subquery ON o.o_orderkey = subquery.l_orderkey
GROUP BY c.c_name,
         c.c_custkey,
         o.o_orderkey,
         o.o_orderdate,
         o.o_totalprice
ORDER BY o.o_totalprice DESC,
         o.o_orderdate;

-- =================================================================
-- Query ID: TPCHN85
-- =================================================================
SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN part ON p_partkey = l_partkey
WHERE l_shipmode IN ('AIR',
                     'AIR REG')
  AND l_shipinstruct = 'DELIVER IN PERSON'
  AND ((p_brand = 'Brand#12'
        AND p_container IN ('SM CASE',
                            'SM BOX',
                            'SM PACK',
                            'SM PKG')
        AND l_quantity BETWEEN 1 AND 11
        AND p_size BETWEEN 1 AND 5)
       OR (p_brand = 'Brand#23'
           AND p_container IN ('MED BAG',
                               'MED BOX',
                               'MED PKG',
                               'MED PACK')
           AND l_quantity BETWEEN 10 AND 20
           AND p_size BETWEEN 1 AND 10)
       OR (p_brand = 'Brand#34'
           AND p_container IN ('LG CASE',
                               'LG BOX',
                               'LG PACK',
                               'LG PKG')
           AND l_quantity BETWEEN 20 AND 30
           AND p_size BETWEEN 1 AND 15));

-- =================================================================
-- Query ID: TPCHN86
-- =================================================================
WITH PartKeys AS
  (SELECT p_partkey
   FROM part
   WHERE p_name LIKE '%ivory%' ),
     AvailableSuppliers AS
  (SELECT ps_suppkey
   FROM partsupp
   JOIN PartKeys ON partsupp.ps_partkey = PartKeys.p_partkey
   WHERE ps_availqty >
       (SELECT 0.5 * SUM(l_quantity)
        FROM lineitem
        WHERE l_partkey = partsupp.ps_partkey
          AND l_suppkey = partsupp.ps_suppkey
          AND l_shipdate >= DATE '1995-01-01'
          AND l_shipdate < DATE '1996-01-01' ) )
SELECT s_name,
       s_address
FROM supplier
JOIN nation ON supplier.s_nationkey = nation.n_nationkey
JOIN AvailableSuppliers ON supplier.s_suppkey = AvailableSuppliers.ps_suppkey
WHERE nation.n_name = 'FRANCE'
ORDER BY s_name;

-- =================================================================
-- Query ID: TPCHN87
-- =================================================================
SELECT s.s_name,
       COUNT(*) AS numwait
FROM supplier s
JOIN lineitem l1 ON s.s_suppkey = l1.l_suppkey
JOIN orders o ON o.o_orderkey = l1.l_orderkey
JOIN nation n ON s.s_nationkey = n.n_nationkey
WHERE o.o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND n.n_name = 'ARGENTINA'
  AND EXISTS
    (SELECT 1
     FROM lineitem l2
     WHERE l2.l_orderkey = l1.l_orderkey
       AND l2.l_suppkey <> l1.l_suppkey )
  AND NOT EXISTS
    (SELECT 1
     FROM lineitem l3
     WHERE l3.l_orderkey = l1.l_orderkey
       AND l3.l_suppkey <> l1.l_suppkey
       AND l3.l_receiptdate > l3.l_commitdate )
GROUP BY s.s_name
ORDER BY numwait DESC,
         s.s_name;

-- =================================================================
-- Query ID: TPCHN88
-- =================================================================
WITH avg_acctbal AS
  (SELECT AVG(c_acctbal) AS avg_bal
   FROM customer
   WHERE c_acctbal > 0.00
     AND substring(c_phone
                   from 1
                   for 2) IN ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17') ),
     filtered_customers AS
  (SELECT c_custkey,
          substring(c_phone
                    from 1
                    for 2) AS cntrycode,
          c_acctbal
   FROM customer
   WHERE substring(c_phone
                   from 1
                   for 2) IN ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17')
     AND c_acctbal >
       (SELECT avg_bal
        FROM avg_acctbal) )
SELECT cntrycode,
       COUNT(*) AS numcust,
       SUM(c_acctbal) AS totacctbal
FROM filtered_customers fc
LEFT JOIN orders o ON fc.c_custkey = o.o_custkey
WHERE o.o_custkey IS NULL
GROUP BY cntrycode
ORDER BY cntrycode;

-- =================================================================
-- Query ID: TPCHN89
-- =================================================================
select l_returnflag,
       l_linestatus,
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as sum_base_price,
       sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       avg(l_quantity) as avg_qty,
       avg(l_extendedprice) as avg_price,
       avg(l_discount) as avg_disc,
       count(*) as count_order
from lineitem
where l_shipdate <= date '1998-12-01' - interval '3' day
group by l_returnflag,
         l_linestatus
order by l_returnflag,
         l_linestatus ;

-- =================================================================
-- Query ID: TPCHN90
-- =================================================================
with filtered_partsupp as
  (select *
   from partsupp
   where ps_partkey in
       (select p_partkey
        from part
        where p_size = 15 ) )
select s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
from part,
     supplier,
     filtered_partsupp as partsupp,
     nation,
     region
where p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15
  and p_type like '%BRASS'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
  and ps_supplycost =
    (select min(ps_supplycost)
     from partsupp,
          supplier,
          nation,
          region
     where p_partkey = ps_partkey
       and s_suppkey = ps_suppkey
       and s_nationkey = n_nationkey
       and n_regionkey = r_regionkey
       and r_name = 'EUROPE' )
order by s_acctbal desc,
         n_name,
         s_name,
         p_partkey
limit 100;

-- =================================================================
-- Query ID: TPCHN91
-- =================================================================
with filtered_orders as
  (select *
   from orders
   where o_custkey in
       (select c_custkey
        from customer
        where c_mktsegment = 'FURNITURE') )
select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate,
       o_shippriority
from filtered_orders as orders,
     lineitem
where l_orderkey = o_orderkey
  and o_orderdate < date '1995-01-01'
  and l_shipdate > date '1995-01-01'
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc,
         o_orderdate;

-- =================================================================
-- Query ID: TPCHN92
-- =================================================================
select o_orderpriority,
       count(*) as order_count
from orders
where o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '3' month
  and exists
    (select *
     from lineitem
     where l_orderkey = o_orderkey
       and l_commitdate < l_receiptdate )
group by o_orderpriority
order by o_orderpriority;

-- =================================================================
-- Query ID: TPCHN93
-- =================================================================
with filtered_orders as
  (select *
   from orders
   where o_orderkey in
       (select l_orderkey
        from lineitem) )
select n_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue
from customer,
     filtered_orders as orders,
     lineitem,
     supplier,
     nation,
     region
where c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_orderdate >= date '1995-01-01'
  and o_orderdate < date '1995-01-01' + interval '1' year
group by n_name
order by revenue desc;

-- =================================================================
-- Query ID: TPCHN94
-- =================================================================
select sum(l_extendedprice * l_discount) as revenue
from lineitem
where l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-03-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 10;

-- =================================================================
-- Query ID: TPCHN95
-- =================================================================
with filtered_lineitem as
  (select *
   from lineitem
   where l_suppkey in
       (select s_suppkey
        from supplier
        where s_nationkey in
            (select n_nationkey
             from nation
             where n_name in ('FRANCE',
                              'GERMANY') ) )
     and l_orderkey in
       (select o_orderkey
        from orders
        where o_custkey in
            (select c_custkey
             from customer
             where c_nationkey in
                 (select n_nationkey
                  from nation
                  where n_name in ('FRANCE',
                                   'GERMANY') ) ) ) )
select supp_nation,
       cust_nation,
       l_year,
       sum(volume) as revenue
from
  (select n1.n_name as supp_nation,
          n2.n_name as cust_nation,
          extract(year
                  from l_shipdate) as l_year,
          l_extendedprice * (1 - l_discount) as volume
   from filtered_lineitem as lineitem,
        supplier,
        orders,
        customer,
        nation n1,
        nation n2
   where s_suppkey = lineitem.l_suppkey
     and o_orderkey = lineitem.l_orderkey
     and c_custkey = o_custkey
     and s_nationkey = n1.n_nationkey
     and c_nationkey = n2.n_nationkey
     and ((n1.n_name = 'GERMANY'
           and n2.n_name = 'FRANCE')
          or (n1.n_name = 'FRANCE'
              and n2.n_name = 'GERMANY'))
     and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year;

-- =================================================================
-- Query ID: TPCHN96
-- =================================================================
WITH filtered_orders AS
  (SELECT *
   FROM orders
   WHERE o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' )
SELECT o_year,
       SUM(CASE
               WHEN nation = 'INDIA' THEN volume
               ELSE 0
           END) / SUM(volume) AS mkt_share
FROM
  (SELECT EXTRACT(YEAR
                  FROM o_orderdate) AS o_year,
          l_extendedprice * (1 - l_discount) AS volume,
          n2.n_name AS nation
   FROM part,
        supplier,
        lineitem,
        filtered_orders AS orders,
        customer,
        nation n1,
        nation n2,
        region
   WHERE p_partkey = l_partkey
     AND s_suppkey = l_suppkey
     AND l_orderkey = o_orderkey
     AND o_custkey = c_custkey
     AND c_nationkey = n1.n_nationkey
     AND n1.n_regionkey = r_regionkey
     AND r_name = 'ASIA'
     AND s_nationkey = n2.n_nationkey
     AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
     AND p_type = 'ECONOMY ANODIZED STEEL' ) AS all_nations
GROUP BY o_year
ORDER BY o_year;

-- =================================================================
-- Query ID: TPCHN97
-- =================================================================
with filtered_lineitem as
  (select *
   from lineitem
   where l_suppkey in
       (select s_suppkey
        from supplier
        where s_nationkey in
            (select n_nationkey
             from nation))
     and l_partkey in
       (select p_partkey
        from part
        where p_name like 'co%') )
select nation,
       o_year,
       sum(amount) as sum_profit
from
  (select n_name as nation,
          p_name,
          extract(year
                  from o_orderdate) as o_year,
          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
   from part,
        supplier,
        filtered_lineitem as lineitem,
        partsupp,
        orders,
        nation
   where s_suppkey = l_suppkey
     and ps_suppkey = l_suppkey
     and ps_partkey = l_partkey
     and p_partkey = l_partkey
     and o_orderkey = l_orderkey
     and s_nationkey = n_nationkey
     and p_name like 'co%' ) as profit
group by nation,
         o_year
order by nation,
         o_year desc;

-- =================================================================
-- Query ID: TPCHN98
-- =================================================================
select c_custkey,
       c_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal,
       n_name,
       c_address,
       c_phone,
       c_comment
from customer,
     orders,
     lineitem,
     nation
where c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1995-01-01'
  and o_orderdate < date '1995-01-01' + interval '3' month
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by c_custkey,
         c_name,
         c_acctbal,
         c_phone,
         n_name,
         c_address,
         c_comment
order by revenue desc;

-- =================================================================
-- Query ID: TPCHN99
-- =================================================================
WITH filtered_partsupp AS
  (SELECT *
   FROM partsupp
   WHERE ps_suppkey IN
       (SELECT s_suppkey
        FROM supplier
        WHERE s_nationkey IN
            (SELECT n_nationkey
             FROM nation
             WHERE n_name = 'INDIA' ) ) )
SELECT ps_partkey,
       n_name,
       SUM(ps_supplycost * ps_availqty) AS total_value
FROM filtered_partsupp AS partsupp,
     supplier,
     nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'INDIA'
GROUP BY ps_partkey,
         n_name
HAVING SUM(ps_supplycost * ps_availqty) >
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
   FROM filtered_partsupp AS partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'INDIA' )
ORDER BY total_value DESC;

-- =================================================================
-- Query ID: TPCHN100
-- =================================================================
with filtered_orders as
  (select *
   from orders
   where o_orderkey in
       (select l_orderkey
        from lineitem
        where l_shipmode = 'SHIP' ) )
select l_shipmode,
       sum(case
               when o_orderpriority = '1-URGENT'
                    or o_orderpriority = '2-HIGH' then 1
               else 0
           end) as high_line_count,
       sum(case
               when o_orderpriority <> '1-URGENT'
                    and o_orderpriority <> '2-HIGH' then 1
               else 0
           end) as low_line_count
from filtered_orders as orders,
     lineitem
where o_orderkey = l_orderkey
  and l_shipmode = 'SHIP'
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1995-01-01'
  and l_receiptdate < date '1995-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode;

-- =================================================================
-- Query ID: TPCHN101
-- =================================================================
select c_count,
       c_orderdate,
       count(*) as custdist
from
  (select c_custkey,
          o_orderdate,
          count(o_orderkey)
   from customer
   left outer join orders on c_custkey = o_custkey
   and o_comment not like '%special%requests%'
   group by c_custkey,
            o_orderdate) as c_orders (c_custkey, c_count, c_orderdate)
group by c_count,
         c_orderdate
order by custdist desc,
         c_count desc ;

-- =================================================================
-- Query ID: TPCHN102
-- =================================================================
with filtered_lineitem as
  (select *
   from lineitem
   where l_partkey in
       (select p_partkey
        from part
        where p_type like 'PROMO%') )
select 100.00 * sum(case
                        when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
                        else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from filtered_lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1995-01-01' + interval '1' month;

-- =================================================================
-- Query ID: TPCHN103
-- =================================================================
with revenue(supplier_no, total_revenue) as
  (select l_suppkey,
          sum(l_extendedprice * (1 - l_discount))
   from lineitem
   where l_shipdate >= date '1995-01-01'
     and l_shipdate < date '1995-01-01' + interval '3' month
   group by l_suppkey)
select s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
from supplier,
     revenue
where s_suppkey = supplier_no
  and total_revenue =
    (select max(total_revenue)
     from revenue)
order by s_suppkey;

-- =================================================================
-- Query ID: TPCHN104
-- =================================================================
select p_brand,
       p_type,
       p_size,
       count(distinct ps_suppkey) as supplier_cnt
from partsupp,
     part
where p_partkey = ps_partkey
  and p_brand <> 'Brand#23'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  and p_size IN (1,
                 4,
                 7)
  and ps_suppkey not in
    (select s_suppkey
     from supplier
     where s_comment like '%Customer%Complaints%' )
group by p_brand,
         p_type,
         p_size
order by supplier_cnt desc,
         p_brand,
         p_type,
         p_size ;

-- =================================================================
-- Query ID: TPCHN105
-- =================================================================
with filtered_lineitem as
  (select *
   from lineitem
   where l_partkey in
       (select p_partkey
        from part
        where p_brand = 'Brand#53'
          and p_container = 'MED BAG' ) )
select sum(l_extendedprice) / 7.0 as avg_yearly
from filtered_lineitem
where l_quantity <
    (select 0.7 * avg(l_quantity)
     from lineitem
     where l_partkey = filtered_lineitem.l_partkey );

-- =================================================================
-- Query ID: TPCHN106
-- =================================================================
with filtered_orders as
  (select *
   from orders
   where o_orderkey in
       (select l_orderkey
        from lineitem
        group by l_orderkey
        having sum(l_quantity) > 300) )
select c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
from customer,
     filtered_orders as orders,
     lineitem
where c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
order by o_totalprice desc,
         o_orderdate;

-- =================================================================
-- Query ID: TPCHN107
-- =================================================================
with filtered_lineitem as
  (select *
   from lineitem
   where l_quantity >= 1
     and l_quantity <= 1 + 10
     and l_shipmode in ('AIR',
                        'AIR REG')
     and l_shipinstruct = 'DELIVER IN PERSON'
     and l_partkey in
       (select p_partkey
        from part
        where (p_brand = 'Brand#12'
               and p_container in ('SM CASE',
                                   'SM BOX',
                                   'SM PACK',
                                   'SM PKG')
               and p_size between 1 and 5)
          or (p_brand = 'Brand#23'
              and p_container in ('MED BAG',
                                  'MED BOX',
                                  'MED PKG',
                                  'MED PACK')
              and p_size between 1 and 10)
          or (p_brand = 'Brand#34'
              and p_container in ('LG CASE',
                                  'LG BOX',
                                  'LG PACK',
                                  'LG PKG')
              and p_size between 1 and 15) ) )
select sum(l_extendedprice * (1 - l_discount)) as revenue
from filtered_lineitem
where (l_partkey in
         (select p_partkey
          from part
          where p_brand = 'Brand#12'
            and p_container in ('SM CASE',
                                'SM BOX',
                                'SM PACK',
                                'SM PKG')
            and p_size between 1 and 5)
       and l_quantity >= 1
       and l_quantity <= 1 + 10)
  or (l_partkey in
        (select p_partkey
         from part
         where p_brand = 'Brand#23'
           and p_container in ('MED BAG',
                               'MED BOX',
                               'MED PKG',
                               'MED PACK')
           and p_size between 1 and 10)
      and l_quantity >= 10
      and l_quantity <= 10 + 10)
  or (l_partkey in
        (select p_partkey
         from part
         where p_brand = 'Brand#34'
           and p_container in ('LG CASE',
                               'LG BOX',
                               'LG PACK',
                               'LG PKG')
           and p_size between 1 and 15)
      and l_quantity >= 20
      and l_quantity <= 20 + 10);

-- =================================================================
-- Query ID: TPCHN108
-- =================================================================
with filtered_supplier as
  (select *
   from supplier
   where s_nationkey in
       (select n_nationkey
        from nation
        where n_name = 'FRANCE' ) )
select s_name,
       s_address
from filtered_supplier as supplier,
     nation
where s_suppkey in
    (select ps_suppkey
     from partsupp
     where ps_partkey in
         (select p_partkey
          from part
          where p_name like '%ivory%' )
       and ps_availqty >
         (select 0.5 * sum(l_quantity)
          from lineitem
          where l_partkey = ps_partkey
            and l_suppkey = ps_suppkey
            and l_shipdate >= date '1995-01-01'
            and l_shipdate < date '1995-01-01' + interval '1' year ) )
  and s_nationkey = n_nationkey
  and n_name = 'FRANCE'
order by s_name;

-- =================================================================
-- Query ID: TPCHN109
-- =================================================================
with filtered_orders as
  (select *
   from orders
   where o_orderstatus = 'F'
     and o_orderkey in
       (select l1.l_orderkey
        from lineitem l1
        where l1.l_receiptdate > l1.l_commitdate
          and exists
            (select *
             from lineitem l2
             where l2.l_orderkey = l1.l_orderkey
               and l2.l_suppkey <> l1.l_suppkey )
          and not exists
            (select *
             from lineitem l3
             where l3.l_orderkey = l1.l_orderkey
               and l3.l_suppkey <> l1.l_suppkey
               and l3.l_receiptdate > l3.l_commitdate ) ) )
select s_name,
       count(*) as numwait
from supplier,
     lineitem l1,
     filtered_orders,
     nation
where s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists
    (select *
     from lineitem l2
     where l2.l_orderkey = l1.l_orderkey
       and l2.l_suppkey <> l1.l_suppkey )
  and not exists
    (select *
     from lineitem l3
     where l3.l_orderkey = l1.l_orderkey
       and l3.l_suppkey <> l1.l_suppkey
       and l3.l_receiptdate > l3.l_commitdate )
  and s_nationkey = n_nationkey
  and n_name = 'ARGENTINA'
group by s_name
order by numwait desc,
         s_name;

-- =================================================================
-- Query ID: TPCHN110
-- =================================================================
with filtered_customer as
  (select *
   from customer
   where substring(c_phone
                   from 1
                   for 2) in ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17') )
select cntrycode,
       count(*) as numcust,
       sum(c_acctbal) as totacctbal
from
  (select substring(c_phone
                    from 1
                    for 2) as cntrycode,
          c_acctbal
   from filtered_customer
   where c_acctbal >
       (select avg(c_acctbal)
        from filtered_customer
        where c_acctbal > 0.00 )
     and not exists
       (select *
        from orders
        where o_custkey = c_custkey ) ) as custsale
group by cntrycode
order by cntrycode;

-- =================================================================
-- Query ID: TPCHN111
-- =================================================================
WITH min_supplycost_cte AS
  (SELECT MIN(ps_supplycost) AS min_supplycost
   FROM partsupp,
        supplier,
        nation,
        region,
        part
   WHERE part.p_partkey = partsupp.ps_partkey
     AND supplier.s_suppkey = partsupp.ps_suppkey
     AND supplier.s_nationkey = nation.n_nationkey
     AND nation.n_regionkey = region.r_regionkey
     AND region.r_name = 'EUROPE'
     AND part.p_size = 15
     AND part.p_type LIKE '%BRASS' )
SELECT s_acctbal,
       s_name,
       n_name,
       part.p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
FROM part,
     supplier,
     partsupp,
     nation,
     region,
     min_supplycost_cte
WHERE part.p_partkey = partsupp.ps_partkey
  AND supplier.s_suppkey = partsupp.ps_suppkey
  AND part.p_size = 15
  AND part.p_type LIKE '%BRASS'
  AND supplier.s_nationkey = nation.n_nationkey
  AND nation.n_regionkey = region.r_regionkey
  AND region.r_name = 'EUROPE'
  AND partsupp.ps_supplycost = min_supplycost_cte.min_supplycost
ORDER BY s_acctbal DESC,
         n_name,
         s_name,
         part.p_partkey
LIMIT 100;

-- =================================================================
-- Query ID: TPCHN112
-- =================================================================
with customer_orders_cte as
  (select o_orderkey,
          o_orderdate,
          o_shippriority
   from customer,
        orders
   where c_mktsegment = 'FURNITURE'
     and c_custkey = o_custkey
     and o_orderdate < date '1995-01-01' ),
     lineitem_revenue_cte as
  (select l_orderkey,
          sum(l_extendedprice * (1 - l_discount)) as revenue
   from lineitem
   where l_shipdate > date '1995-01-01'
   group by l_orderkey)
select co.o_orderkey,
       lr.revenue,
       co.o_orderdate,
       co.o_shippriority
from customer_orders_cte co
join lineitem_revenue_cte lr on co.o_orderkey = lr.l_orderkey
order by lr.revenue desc,
         co.o_orderdate;

-- =================================================================
-- Query ID: TPCHN113
-- =================================================================
with lineitem_cte as
  (select l_orderkey
   from lineitem
   where l_commitdate < l_receiptdate )
select o_orderpriority,
       count(*) as order_count
from orders
where o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '3' month
  and exists
    (select 1
     from lineitem_cte
     where lineitem_cte.l_orderkey = orders.o_orderkey )
group by o_orderpriority
order by o_orderpriority;

-- =================================================================
-- Query ID: TPCHN114
-- =================================================================
with date_filtered_lineitem as
  (select l_extendedprice,
          l_discount
   from lineitem
   where l_shipdate >= date '1993-01-01'
     and l_shipdate < date '1994-03-01' + interval '1' year
     and l_quantity < 10 ),
     discount_filtered_lineitem as
  (select l_extendedprice,
          l_discount
   from date_filtered_lineitem
   where l_discount between 0.06 - 0.01 and 0.06 + 0.01 )
select sum(l_extendedprice * l_discount) as revenue
from discount_filtered_lineitem;

-- =================================================================
-- Query ID: TPCHN115
-- =================================================================
with shipping as
  (select n1.n_name as supp_nation,
          n2.n_name as cust_nation,
          extract(year
                  from l_shipdate) as l_year,
          l_extendedprice * (1 - l_discount) as volume
   from supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
   where s_suppkey = l_suppkey
     and o_orderkey = l_orderkey
     and c_custkey = o_custkey
     and s_nationkey = n1.n_nationkey
     and c_nationkey = n2.n_nationkey
     and ((n1.n_name = 'GERMANY'
           and n2.n_name = 'FRANCE')
          or (n1.n_name = 'FRANCE'
              and n2.n_name = 'GERMANY'))
     and l_shipdate between date '1995-01-01' and date '1996-12-31' )
select supp_nation,
       cust_nation,
       l_year,
       sum(volume) as revenue
from shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year;

-- =================================================================
-- Query ID: TPCHN116
-- =================================================================
with cte as
  (select extract(year
                  from o_orderdate) as o_year,
          l_extendedprice * (1 - l_discount) as volume,
          n2.n_name as nation
   from part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
   where p_partkey = l_partkey
     and s_suppkey = l_suppkey
     and l_orderkey = o_orderkey
     and o_custkey = c_custkey
     and c_nationkey = n1.n_nationkey
     and n1.n_regionkey = r_regionkey
     and r_name = 'ASIA'
     and s_nationkey = n2.n_nationkey
     and o_orderdate between date '1995-01-01' and date '1996-12-31'
     and p_type = 'ECONOMY ANODIZED STEEL' )
select o_year,
       sum(case
               when nation = 'INDIA' then volume
               else 0
           end) / sum(volume) as mkt_share
from cte
group by o_year
order by o_year;

-- =================================================================
-- Query ID: TPCHN117
-- =================================================================
with order_filter_cte as
  (select o_orderkey,
          o_custkey
   from orders
   where o_orderdate >= date '1995-01-01'
     and o_orderdate < date '1995-01-01' + interval '3' month ),
     lineitem_filter_cte as
  (select l_orderkey,
          l_extendedprice,
          l_discount
   from lineitem
   where l_returnflag = 'R' )
select c.c_custkey,
       c.c_name,
       sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
       c.c_acctbal,
       n.n_name,
       c.c_address,
       c.c_phone,
       c.c_comment
from customer c
join order_filter_cte o on c.c_custkey = o.o_custkey
join lineitem_filter_cte l on l.l_orderkey = o.o_orderkey
join nation n on c.c_nationkey = n.n_nationkey
group by c.c_custkey,
         c.c_name,
         c.c_acctbal,
         c.c_phone,
         n.n_name,
         c.c_address,
         c.c_comment
order by revenue desc;

-- =================================================================
-- Query ID: TPCHN118
-- =================================================================
WITH total_supply_cost_cte AS
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001 AS threshold_value
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'INDIA' )
SELECT ps_partkey,
       n_name,
       SUM(ps_supplycost * ps_availqty) AS total_value
FROM partsupp,
     supplier,
     nation,
     total_supply_cost_cte
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'INDIA'
GROUP BY ps_partkey,
         n_name,
         total_supply_cost_cte.threshold_value
HAVING SUM(ps_supplycost * ps_availqty) > threshold_value
ORDER BY total_value DESC;

-- =================================================================
-- Query ID: TPCHN119
-- =================================================================
with filtered_lineitem as
  (select l_orderkey,
          l_shipmode
   from lineitem
   where l_shipmode = 'SHIP'
     and l_commitdate < l_receiptdate
     and l_shipdate < l_commitdate
     and l_receiptdate >= date '1995-01-01'
     and l_receiptdate < date '1995-01-01' + interval '1' year )
select l_shipmode,
       sum(case
               when o_orderpriority = '1-URGENT'
                    or o_orderpriority = '2-HIGH' then 1
               else 0
           end) as high_line_count,
       sum(case
               when o_orderpriority <> '1-URGENT'
                    and o_orderpriority <> '2-HIGH' then 1
               else 0
           end) as low_line_count
from orders
join filtered_lineitem on o_orderkey = l_orderkey
group by l_shipmode
order by l_shipmode;

-- =================================================================
-- Query ID: TPCHN120
-- =================================================================
WITH c_orders_cte AS
  (SELECT c_custkey,
          o_orderdate,
          COUNT(o_orderkey) AS c_count
   FROM customer
   LEFT OUTER JOIN orders ON c_custkey = o_custkey
   AND o_comment NOT LIKE '%special%requests%'
   GROUP BY c_custkey,
            o_orderdate)
SELECT c_count,
       o_orderdate,
       COUNT(*) AS custdist
FROM c_orders_cte
GROUP BY c_count,
         o_orderdate
ORDER BY custdist DESC,
         c_count DESC;

-- =================================================================
-- Query ID: TPCHN121
-- =================================================================
with promo_revenue_cte as
  (select l_extendedprice * (1 - l_discount) as discounted_price,
          case
              when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
              else 0
          end as promo_discounted_price
   from lineitem,
        part
   where l_partkey = p_partkey
     and l_shipdate >= date '1995-01-01'
     and l_shipdate < date '1995-01-01' + interval '1' month )
select 100.00 * sum(promo_discounted_price) / sum(discounted_price) as promo_revenue
from promo_revenue_cte;

-- =================================================================
-- Query ID: TPCHN122
-- =================================================================
with supplier_exclusion_cte as
  (select s_suppkey
   from supplier
   where s_comment like '%Customer%Complaints%' )
select p_brand,
       p_type,
       p_size,
       count(distinct ps_suppkey) as supplier_cnt
from partsupp,
     part
where p_partkey = ps_partkey
  and p_brand <> 'Brand#23'
  and p_type NOT LIKE 'MEDIUM POLISHED%'
  and p_size IN (1,
                 4,
                 7)
  and ps_suppkey not in
    (select s_suppkey
     from supplier_exclusion_cte)
group by p_brand,
         p_type,
         p_size
order by supplier_cnt desc,
         p_brand,
         p_type,
         p_size;

-- =================================================================
-- Query ID: TPCHN123
-- =================================================================
WITH avg_quantity_cte AS
  (SELECT l_partkey,
          0.7 * AVG(l_quantity) AS threshold_quantity
   FROM lineitem
   GROUP BY l_partkey)
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem
JOIN part ON part.p_partkey = lineitem.l_partkey
JOIN avg_quantity_cte ON avg_quantity_cte.l_partkey = lineitem.l_partkey
WHERE part.p_brand = 'Brand#53'
  AND part.p_container = 'MED BAG'
  AND lineitem.l_quantity < avg_quantity_cte.threshold_quantity;

-- =================================================================
-- Query ID: TPCHN124
-- =================================================================
WITH large_orders_cte AS
  (SELECT l_orderkey
   FROM lineitem
   GROUP BY l_orderkey
   HAVING SUM(l_quantity) > 300)
SELECT c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       SUM(l_quantity)
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON o_orderkey = lineitem.l_orderkey
JOIN large_orders_cte ON o_orderkey = large_orders_cte.l_orderkey
GROUP BY c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
ORDER BY o_totalprice DESC,
         o_orderdate;

-- =================================================================
-- Query ID: TPCHN125
-- =================================================================
with part_lineitem_cte as
  (select l_extendedprice,
          l_discount
   from lineitem
   join part on p_partkey = l_partkey
   where (p_brand = 'Brand#12'
          and p_container in ('SM CASE',
                              'SM BOX',
                              'SM PACK',
                              'SM PKG')
          and l_quantity >= 1
          and l_quantity <= 11
          and p_size between 1 and 5
          and l_shipmode in ('AIR',
                             'AIR REG')
          and l_shipinstruct = 'DELIVER IN PERSON')
     or (p_brand = 'Brand#23'
         and p_container in ('MED BAG',
                             'MED BOX',
                             'MED PKG',
                             'MED PACK')
         and l_quantity >= 10
         and l_quantity <= 20
         and p_size between 1 and 10
         and l_shipmode in ('AIR',
                            'AIR REG')
         and l_shipinstruct = 'DELIVER IN PERSON')
     or (p_brand = 'Brand#34'
         and p_container in ('LG CASE',
                             'LG BOX',
                             'LG PACK',
                             'LG PKG')
         and l_quantity >= 20
         and l_quantity <= 30
         and p_size between 1 and 15
         and l_shipmode in ('AIR',
                            'AIR REG')
         and l_shipinstruct = 'DELIVER IN PERSON') )
select sum(l_extendedprice * (1 - l_discount)) as revenue
from part_lineitem_cte;

-- =================================================================
-- Query ID: TPCHN126
-- =================================================================
WITH l2_exists_cte AS
  (SELECT l_orderkey
   FROM lineitem
   GROUP BY l_orderkey
   HAVING COUNT(DISTINCT l_suppkey) > 1),
     l3_not_exists_cte AS
  (SELECT l_orderkey
   FROM lineitem
   GROUP BY l_orderkey
   HAVING COUNT(DISTINCT l_suppkey) = 1
   AND MAX(l_receiptdate) <= MAX(l_commitdate))
SELECT s_name,
       COUNT(*) AS numwait
FROM supplier
JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
JOIN orders ON o_orderkey = l1.l_orderkey
JOIN nation ON s_nationkey = n_nationkey
WHERE o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND l1.l_orderkey IN
    (SELECT l_orderkey
     FROM l2_exists_cte)
  AND l1.l_orderkey NOT IN
    (SELECT l_orderkey
     FROM l3_not_exists_cte)
  AND n_name = 'ARGENTINA'
GROUP BY s_name
ORDER BY numwait DESC,
         s_name;
