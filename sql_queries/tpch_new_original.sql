-- =================================================================
-- Query ID: TPCHN1
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
-- Query ID: TPCHN2
-- =================================================================
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
     partsupp,
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
limit 100 ;

-- =================================================================
-- Query ID: TPCHN3
-- =================================================================
select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate,
       o_shippriority
from customer,
     orders,
     lineitem
where c_mktsegment = 'FURNITURE'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-01-01'
  and l_shipdate > date '1995-01-01'
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN4
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
order by o_orderpriority ;

-- =================================================================
-- Query ID: TPCHN5
-- =================================================================
select n_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue
from customer,
     orders,
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN6
-- =================================================================
select sum(l_extendedprice * l_discount) as revenue
from lineitem
where l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-03-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 10 ;

-- =================================================================
-- Query ID: TPCHN7
-- =================================================================
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
     and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year ;

-- =================================================================
-- Query ID: TPCHN8
-- =================================================================
select o_year,
       sum(case
               when nation = 'INDIA' then volume
               else 0
           end) / sum(volume) as mkt_share
from
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
     and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
group by o_year
order by o_year ;

-- =================================================================
-- Query ID: TPCHN9
-- =================================================================
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
        lineitem,
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
         o_year desc ;

-- =================================================================
-- Query ID: TPCHN10
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN11
-- =================================================================
SELECT ps_partkey,
       n_name,
       SUM(ps_supplycost * ps_availqty) AS total_value
FROM partsupp,
     supplier,
     nation
where ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'INDIA'
GROUP BY ps_partkey,
         n_name
HAVING SUM(ps_supplycost * ps_availqty) >
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     and s_nationkey = n_nationkey
     and n_name = 'INDIA' )
ORDER BY total_value DESC ;

-- =================================================================
-- Query ID: TPCHN12
-- =================================================================
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
from orders,
     lineitem
where o_orderkey = l_orderkey
  and l_shipmode = 'SHIP'
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1995-01-01'
  and l_receiptdate < date '1995-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode ;

-- =================================================================
-- Query ID: TPCHN13
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
-- Query ID: TPCHN14
-- =================================================================
select 100.00 * sum(case
                        when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
                        else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1995-01-01' + interval '1' month ;

-- =================================================================
-- Query ID: TPCHN15
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
order by s_suppkey ;

-- =================================================================
-- Query ID: TPCHN16
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
-- Query ID: TPCHN17
-- =================================================================
select sum(l_extendedprice) / 7.0 as avg_yearly
from lineitem,
     part
where p_partkey = l_partkey
  and p_brand = 'Brand#53'
  and p_container = 'MED BAG'
  and l_quantity <
    (select 0.7 * avg(l_quantity)
     from lineitem
     where l_partkey = p_partkey ) ;

-- =================================================================
-- Query ID: TPCHN18
-- =================================================================
select c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
from customer,
     orders,
     lineitem
where o_orderkey in
    (select l_orderkey
     from lineitem
     group by l_orderkey
     having sum(l_quantity) > 300)
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
order by o_totalprice desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN19
-- =================================================================
select sum(l_extendedprice * (1 - l_discount)) as revenue
from lineitem,
     part
where (p_partkey = l_partkey
       and p_brand = 'Brand#12'
       and p_container in ('SM CASE',
                           'SM BOX',
                           'SM PACK',
                           'SM PKG')
       and l_quantity >= 1
       and l_quantity <= 1 + 10
       and p_size between 1 and 5
       and l_shipmode in ('AIR',
                          'AIR REG')
       and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#23'
      and p_container in ('MED BAG',
                          'MED BOX',
                          'MED PKG',
                          'MED PACK')
      and l_quantity >= 10
      and l_quantity <= 10 + 10
      and p_size between 1 and 10
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#34'
      and p_container in ('LG CASE',
                          'LG BOX',
                          'LG PACK',
                          'LG PKG')
      and l_quantity >= 20
      and l_quantity <= 20 + 10
      and p_size between 1 and 15
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON') ;

-- =================================================================
-- Query ID: TPCHN20
-- =================================================================
select s_name,
       s_address
from supplier,
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
order by s_name ;

-- =================================================================
-- Query ID: TPCHN21
-- =================================================================
select s_name,
       count(*) as numwait
from supplier,
     lineitem l1,
     orders,
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
         s_name ;

-- =================================================================
-- Query ID: TPCHN22
-- =================================================================
select cntrycode,
       count(*) as numcust,
       sum(c_acctbal) as totacctbal
from
  (select substring(c_phone
                    from 1
                    for 2) as cntrycode,
          c_acctbal
   from customer
   where substring(c_phone
                   from 1
                   for 2) in ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17')
     and c_acctbal >
       (select avg(c_acctbal)
        from customer
        where c_acctbal > 0.00
          and substring(c_phone
                        from 1
                        for 2) in ('13',
                                   '31',
                                   '23',
                                   '29',
                                   '30',
                                   '18',
                                   '17') )
     and not exists
       (select *
        from orders
        where o_custkey = c_custkey ) ) as custsale
group by cntrycode
order by cntrycode ;

-- =================================================================
-- Query ID: TPCHN23
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
-- Query ID: TPCHN24
-- =================================================================
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
     partsupp,
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
limit 100 ;

-- =================================================================
-- Query ID: TPCHN25
-- =================================================================
select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate,
       o_shippriority
from customer,
     orders,
     lineitem
where c_mktsegment = 'FURNITURE'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-01-01'
  and l_shipdate > date '1995-01-01'
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN26
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
order by o_orderpriority ;

-- =================================================================
-- Query ID: TPCHN27
-- =================================================================
select n_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue
from customer,
     orders,
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN28
-- =================================================================
select sum(l_extendedprice * l_discount) as revenue
from lineitem
where l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-03-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 10 ;

-- =================================================================
-- Query ID: TPCHN29
-- =================================================================
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
     and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year ;

-- =================================================================
-- Query ID: TPCHN30
-- =================================================================
select o_year,
       sum(case
               when nation = 'INDIA' then volume
               else 0
           end) / sum(volume) as mkt_share
from
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
     and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
group by o_year
order by o_year ;

-- =================================================================
-- Query ID: TPCHN31
-- =================================================================
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
        lineitem,
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
         o_year desc ;

-- =================================================================
-- Query ID: TPCHN32
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN33
-- =================================================================
SELECT ps_partkey,
       n_name,
       SUM(ps_supplycost * ps_availqty) AS total_value
FROM partsupp,
     supplier,
     nation
where ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'INDIA'
GROUP BY ps_partkey,
         n_name
HAVING SUM(ps_supplycost * ps_availqty) >
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     and s_nationkey = n_nationkey
     and n_name = 'INDIA' )
ORDER BY total_value DESC ;

-- =================================================================
-- Query ID: TPCHN34
-- =================================================================
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
from orders,
     lineitem
where o_orderkey = l_orderkey
  and l_shipmode = 'SHIP'
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1995-01-01'
  and l_receiptdate < date '1995-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode ;

-- =================================================================
-- Query ID: TPCHN35
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
-- Query ID: TPCHN36
-- =================================================================
select 100.00 * sum(case
                        when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
                        else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1995-01-01' + interval '1' month ;

-- =================================================================
-- Query ID: TPCHN37
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
order by s_suppkey ;

-- =================================================================
-- Query ID: TPCHN38
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
-- Query ID: TPCHN39
-- =================================================================
select sum(l_extendedprice) / 7.0 as avg_yearly
from lineitem,
     part
where p_partkey = l_partkey
  and p_brand = 'Brand#53'
  and p_container = 'MED BAG'
  and l_quantity <
    (select 0.7 * avg(l_quantity)
     from lineitem
     where l_partkey = p_partkey ) ;

-- =================================================================
-- Query ID: TPCHN40
-- =================================================================
select c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
from customer,
     orders,
     lineitem
where o_orderkey in
    (select l_orderkey
     from lineitem
     group by l_orderkey
     having sum(l_quantity) > 300)
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
order by o_totalprice desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN41
-- =================================================================
select sum(l_extendedprice * (1 - l_discount)) as revenue
from lineitem,
     part
where (p_partkey = l_partkey
       and p_brand = 'Brand#12'
       and p_container in ('SM CASE',
                           'SM BOX',
                           'SM PACK',
                           'SM PKG')
       and l_quantity >= 1
       and l_quantity <= 1 + 10
       and p_size between 1 and 5
       and l_shipmode in ('AIR',
                          'AIR REG')
       and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#23'
      and p_container in ('MED BAG',
                          'MED BOX',
                          'MED PKG',
                          'MED PACK')
      and l_quantity >= 10
      and l_quantity <= 10 + 10
      and p_size between 1 and 10
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#34'
      and p_container in ('LG CASE',
                          'LG BOX',
                          'LG PACK',
                          'LG PKG')
      and l_quantity >= 20
      and l_quantity <= 20 + 10
      and p_size between 1 and 15
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON') ;

-- =================================================================
-- Query ID: TPCHN42
-- =================================================================
select s_name,
       s_address
from supplier,
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
order by s_name ;

-- =================================================================
-- Query ID: TPCHN43
-- =================================================================
select s_name,
       count(*) as numwait
from supplier,
     lineitem l1,
     orders,
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
         s_name ;

-- =================================================================
-- Query ID: TPCHN44
-- =================================================================
select cntrycode,
       count(*) as numcust,
       sum(c_acctbal) as totacctbal
from
  (select substring(c_phone
                    from 1
                    for 2) as cntrycode,
          c_acctbal
   from customer
   where substring(c_phone
                   from 1
                   for 2) in ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17')
     and c_acctbal >
       (select avg(c_acctbal)
        from customer
        where c_acctbal > 0.00
          and substring(c_phone
                        from 1
                        for 2) in ('13',
                                   '31',
                                   '23',
                                   '29',
                                   '30',
                                   '18',
                                   '17') )
     and not exists
       (select *
        from orders
        where o_custkey = c_custkey ) ) as custsale
group by cntrycode
order by cntrycode ;

-- =================================================================
-- Query ID: TPCHN45
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
-- Query ID: TPCHN46
-- =================================================================
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
     partsupp,
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
limit 100 ;

-- =================================================================
-- Query ID: TPCHN47
-- =================================================================
select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate,
       o_shippriority
from customer,
     orders,
     lineitem
where c_mktsegment = 'FURNITURE'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-01-01'
  and l_shipdate > date '1995-01-01'
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN48
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
order by o_orderpriority ;

-- =================================================================
-- Query ID: TPCHN49
-- =================================================================
select n_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue
from customer,
     orders,
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN50
-- =================================================================
select sum(l_extendedprice * l_discount) as revenue
from lineitem
where l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-03-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 10 ;

-- =================================================================
-- Query ID: TPCHN51
-- =================================================================
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
     and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year ;

-- =================================================================
-- Query ID: TPCHN52
-- =================================================================
select o_year,
       sum(case
               when nation = 'INDIA' then volume
               else 0
           end) / sum(volume) as mkt_share
from
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
     and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
group by o_year
order by o_year ;

-- =================================================================
-- Query ID: TPCHN53
-- =================================================================
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
        lineitem,
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
         o_year desc ;

-- =================================================================
-- Query ID: TPCHN54
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN55
-- =================================================================
SELECT ps_partkey,
       n_name,
       SUM(ps_supplycost * ps_availqty) AS total_value
FROM partsupp,
     supplier,
     nation
where ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'INDIA'
GROUP BY ps_partkey,
         n_name
HAVING SUM(ps_supplycost * ps_availqty) >
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     and s_nationkey = n_nationkey
     and n_name = 'INDIA' )
ORDER BY total_value DESC ;

-- =================================================================
-- Query ID: TPCHN56
-- =================================================================
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
from orders,
     lineitem
where o_orderkey = l_orderkey
  and l_shipmode = 'SHIP'
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1995-01-01'
  and l_receiptdate < date '1995-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode ;

-- =================================================================
-- Query ID: TPCHN57
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
-- Query ID: TPCHN58
-- =================================================================
select 100.00 * sum(case
                        when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
                        else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1995-01-01' + interval '1' month ;

-- =================================================================
-- Query ID: TPCHN59
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
order by s_suppkey ;

-- =================================================================
-- Query ID: TPCHN60
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
-- Query ID: TPCHN61
-- =================================================================
select sum(l_extendedprice) / 7.0 as avg_yearly
from lineitem,
     part
where p_partkey = l_partkey
  and p_brand = 'Brand#53'
  and p_container = 'MED BAG'
  and l_quantity <
    (select 0.7 * avg(l_quantity)
     from lineitem
     where l_partkey = p_partkey ) ;

-- =================================================================
-- Query ID: TPCHN62
-- =================================================================
select c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
from customer,
     orders,
     lineitem
where o_orderkey in
    (select l_orderkey
     from lineitem
     group by l_orderkey
     having sum(l_quantity) > 300)
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
order by o_totalprice desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN63
-- =================================================================
select sum(l_extendedprice * (1 - l_discount)) as revenue
from lineitem,
     part
where (p_partkey = l_partkey
       and p_brand = 'Brand#12'
       and p_container in ('SM CASE',
                           'SM BOX',
                           'SM PACK',
                           'SM PKG')
       and l_quantity >= 1
       and l_quantity <= 1 + 10
       and p_size between 1 and 5
       and l_shipmode in ('AIR',
                          'AIR REG')
       and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#23'
      and p_container in ('MED BAG',
                          'MED BOX',
                          'MED PKG',
                          'MED PACK')
      and l_quantity >= 10
      and l_quantity <= 10 + 10
      and p_size between 1 and 10
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#34'
      and p_container in ('LG CASE',
                          'LG BOX',
                          'LG PACK',
                          'LG PKG')
      and l_quantity >= 20
      and l_quantity <= 20 + 10
      and p_size between 1 and 15
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON') ;

-- =================================================================
-- Query ID: TPCHN64
-- =================================================================
select s_name,
       s_address
from supplier,
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
order by s_name ;

-- =================================================================
-- Query ID: TPCHN65
-- =================================================================
select s_name,
       count(*) as numwait
from supplier,
     lineitem l1,
     orders,
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
         s_name ;

-- =================================================================
-- Query ID: TPCHN66
-- =================================================================
select cntrycode,
       count(*) as numcust,
       sum(c_acctbal) as totacctbal
from
  (select substring(c_phone
                    from 1
                    for 2) as cntrycode,
          c_acctbal
   from customer
   where substring(c_phone
                   from 1
                   for 2) in ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17')
     and c_acctbal >
       (select avg(c_acctbal)
        from customer
        where c_acctbal > 0.00
          and substring(c_phone
                        from 1
                        for 2) in ('13',
                                   '31',
                                   '23',
                                   '29',
                                   '30',
                                   '18',
                                   '17') )
     and not exists
       (select *
        from orders
        where o_custkey = c_custkey ) ) as custsale
group by cntrycode
order by cntrycode ;

-- =================================================================
-- Query ID: TPCHN67
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
-- Query ID: TPCHN68
-- =================================================================
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
     partsupp,
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
limit 100 ;

-- =================================================================
-- Query ID: TPCHN69
-- =================================================================
select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate,
       o_shippriority
from customer,
     orders,
     lineitem
where c_mktsegment = 'FURNITURE'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-01-01'
  and l_shipdate > date '1995-01-01'
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN70
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
order by o_orderpriority ;

-- =================================================================
-- Query ID: TPCHN71
-- =================================================================
select n_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue
from customer,
     orders,
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN72
-- =================================================================
select sum(l_extendedprice * l_discount) as revenue
from lineitem
where l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-03-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 10 ;

-- =================================================================
-- Query ID: TPCHN73
-- =================================================================
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
     and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year ;

-- =================================================================
-- Query ID: TPCHN74
-- =================================================================
select o_year,
       sum(case
               when nation = 'INDIA' then volume
               else 0
           end) / sum(volume) as mkt_share
from
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
     and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
group by o_year
order by o_year ;

-- =================================================================
-- Query ID: TPCHN75
-- =================================================================
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
        lineitem,
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
         o_year desc ;

-- =================================================================
-- Query ID: TPCHN76
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN77
-- =================================================================
SELECT ps_partkey,
       n_name,
       SUM(ps_supplycost * ps_availqty) AS total_value
FROM partsupp,
     supplier,
     nation
where ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'INDIA'
GROUP BY ps_partkey,
         n_name
HAVING SUM(ps_supplycost * ps_availqty) >
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     and s_nationkey = n_nationkey
     and n_name = 'INDIA' )
ORDER BY total_value DESC ;

-- =================================================================
-- Query ID: TPCHN78
-- =================================================================
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
from orders,
     lineitem
where o_orderkey = l_orderkey
  and l_shipmode = 'SHIP'
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1995-01-01'
  and l_receiptdate < date '1995-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode ;

-- =================================================================
-- Query ID: TPCHN79
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
-- Query ID: TPCHN80
-- =================================================================
select 100.00 * sum(case
                        when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
                        else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1995-01-01' + interval '1' month ;

-- =================================================================
-- Query ID: TPCHN81
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
order by s_suppkey ;

-- =================================================================
-- Query ID: TPCHN82
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
-- Query ID: TPCHN83
-- =================================================================
select sum(l_extendedprice) / 7.0 as avg_yearly
from lineitem,
     part
where p_partkey = l_partkey
  and p_brand = 'Brand#53'
  and p_container = 'MED BAG'
  and l_quantity <
    (select 0.7 * avg(l_quantity)
     from lineitem
     where l_partkey = p_partkey ) ;

-- =================================================================
-- Query ID: TPCHN84
-- =================================================================
select c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
from customer,
     orders,
     lineitem
where o_orderkey in
    (select l_orderkey
     from lineitem
     group by l_orderkey
     having sum(l_quantity) > 300)
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
order by o_totalprice desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN85
-- =================================================================
select sum(l_extendedprice * (1 - l_discount)) as revenue
from lineitem,
     part
where (p_partkey = l_partkey
       and p_brand = 'Brand#12'
       and p_container in ('SM CASE',
                           'SM BOX',
                           'SM PACK',
                           'SM PKG')
       and l_quantity >= 1
       and l_quantity <= 1 + 10
       and p_size between 1 and 5
       and l_shipmode in ('AIR',
                          'AIR REG')
       and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#23'
      and p_container in ('MED BAG',
                          'MED BOX',
                          'MED PKG',
                          'MED PACK')
      and l_quantity >= 10
      and l_quantity <= 10 + 10
      and p_size between 1 and 10
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#34'
      and p_container in ('LG CASE',
                          'LG BOX',
                          'LG PACK',
                          'LG PKG')
      and l_quantity >= 20
      and l_quantity <= 20 + 10
      and p_size between 1 and 15
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON') ;

-- =================================================================
-- Query ID: TPCHN86
-- =================================================================
select s_name,
       s_address
from supplier,
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
order by s_name ;

-- =================================================================
-- Query ID: TPCHN87
-- =================================================================
select s_name,
       count(*) as numwait
from supplier,
     lineitem l1,
     orders,
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
         s_name ;

-- =================================================================
-- Query ID: TPCHN88
-- =================================================================
select cntrycode,
       count(*) as numcust,
       sum(c_acctbal) as totacctbal
from
  (select substring(c_phone
                    from 1
                    for 2) as cntrycode,
          c_acctbal
   from customer
   where substring(c_phone
                   from 1
                   for 2) in ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17')
     and c_acctbal >
       (select avg(c_acctbal)
        from customer
        where c_acctbal > 0.00
          and substring(c_phone
                        from 1
                        for 2) in ('13',
                                   '31',
                                   '23',
                                   '29',
                                   '30',
                                   '18',
                                   '17') )
     and not exists
       (select *
        from orders
        where o_custkey = c_custkey ) ) as custsale
group by cntrycode
order by cntrycode ;

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
     partsupp,
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
limit 100 ;

-- =================================================================
-- Query ID: TPCHN91
-- =================================================================
select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate,
       o_shippriority
from customer,
     orders,
     lineitem
where c_mktsegment = 'FURNITURE'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-01-01'
  and l_shipdate > date '1995-01-01'
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc,
         o_orderdate ;

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
order by o_orderpriority ;

-- =================================================================
-- Query ID: TPCHN93
-- =================================================================
select n_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue
from customer,
     orders,
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN94
-- =================================================================
select sum(l_extendedprice * l_discount) as revenue
from lineitem
where l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-03-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 10 ;

-- =================================================================
-- Query ID: TPCHN95
-- =================================================================
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
     and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year ;

-- =================================================================
-- Query ID: TPCHN96
-- =================================================================
select o_year,
       sum(case
               when nation = 'INDIA' then volume
               else 0
           end) / sum(volume) as mkt_share
from
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
     and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
group by o_year
order by o_year ;

-- =================================================================
-- Query ID: TPCHN97
-- =================================================================
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
        lineitem,
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
         o_year desc ;

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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN99
-- =================================================================
SELECT ps_partkey,
       n_name,
       SUM(ps_supplycost * ps_availqty) AS total_value
FROM partsupp,
     supplier,
     nation
where ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'INDIA'
GROUP BY ps_partkey,
         n_name
HAVING SUM(ps_supplycost * ps_availqty) >
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     and s_nationkey = n_nationkey
     and n_name = 'INDIA' )
ORDER BY total_value DESC ;

-- =================================================================
-- Query ID: TPCHN100
-- =================================================================
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
from orders,
     lineitem
where o_orderkey = l_orderkey
  and l_shipmode = 'SHIP'
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1995-01-01'
  and l_receiptdate < date '1995-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode ;

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
select 100.00 * sum(case
                        when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
                        else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1995-01-01' + interval '1' month ;

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
order by s_suppkey ;

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
select sum(l_extendedprice) / 7.0 as avg_yearly
from lineitem,
     part
where p_partkey = l_partkey
  and p_brand = 'Brand#53'
  and p_container = 'MED BAG'
  and l_quantity <
    (select 0.7 * avg(l_quantity)
     from lineitem
     where l_partkey = p_partkey ) ;

-- =================================================================
-- Query ID: TPCHN106
-- =================================================================
select c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
from customer,
     orders,
     lineitem
where o_orderkey in
    (select l_orderkey
     from lineitem
     group by l_orderkey
     having sum(l_quantity) > 300)
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
order by o_totalprice desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN107
-- =================================================================
select sum(l_extendedprice * (1 - l_discount)) as revenue
from lineitem,
     part
where (p_partkey = l_partkey
       and p_brand = 'Brand#12'
       and p_container in ('SM CASE',
                           'SM BOX',
                           'SM PACK',
                           'SM PKG')
       and l_quantity >= 1
       and l_quantity <= 1 + 10
       and p_size between 1 and 5
       and l_shipmode in ('AIR',
                          'AIR REG')
       and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#23'
      and p_container in ('MED BAG',
                          'MED BOX',
                          'MED PKG',
                          'MED PACK')
      and l_quantity >= 10
      and l_quantity <= 10 + 10
      and p_size between 1 and 10
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#34'
      and p_container in ('LG CASE',
                          'LG BOX',
                          'LG PACK',
                          'LG PKG')
      and l_quantity >= 20
      and l_quantity <= 20 + 10
      and p_size between 1 and 15
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON') ;

-- =================================================================
-- Query ID: TPCHN108
-- =================================================================
select s_name,
       s_address
from supplier,
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
order by s_name ;

-- =================================================================
-- Query ID: TPCHN109
-- =================================================================
select s_name,
       count(*) as numwait
from supplier,
     lineitem l1,
     orders,
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
         s_name ;

-- =================================================================
-- Query ID: TPCHN110
-- =================================================================
select cntrycode,
       count(*) as numcust,
       sum(c_acctbal) as totacctbal
from
  (select substring(c_phone
                    from 1
                    for 2) as cntrycode,
          c_acctbal
   from customer
   where substring(c_phone
                   from 1
                   for 2) in ('13',
                              '31',
                              '23',
                              '29',
                              '30',
                              '18',
                              '17')
     and c_acctbal >
       (select avg(c_acctbal)
        from customer
        where c_acctbal > 0.00
          and substring(c_phone
                        from 1
                        for 2) in ('13',
                                   '31',
                                   '23',
                                   '29',
                                   '30',
                                   '18',
                                   '17') )
     and not exists
       (select *
        from orders
        where o_custkey = c_custkey ) ) as custsale
group by cntrycode
order by cntrycode ;

-- =================================================================
-- Query ID: TPCHN111
-- =================================================================
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
     partsupp,
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
limit 100 ;

-- =================================================================
-- Query ID: TPCHN112
-- =================================================================
select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate,
       o_shippriority
from customer,
     orders,
     lineitem
where c_mktsegment = 'FURNITURE'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-01-01'
  and l_shipdate > date '1995-01-01'
group by l_orderkey,
         o_orderdate,
         o_shippriority
order by revenue desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN113
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
order by o_orderpriority ;

-- =================================================================
-- Query ID: TPCHN114
-- =================================================================
select sum(l_extendedprice * l_discount) as revenue
from lineitem
where l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-03-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 10 ;

-- =================================================================
-- Query ID: TPCHN115
-- =================================================================
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
     and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year ;

-- =================================================================
-- Query ID: TPCHN116
-- =================================================================
select o_year,
       sum(case
               when nation = 'INDIA' then volume
               else 0
           end) / sum(volume) as mkt_share
from
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
     and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations
group by o_year
order by o_year ;

-- =================================================================
-- Query ID: TPCHN117
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
order by revenue desc ;

-- =================================================================
-- Query ID: TPCHN118
-- =================================================================
SELECT ps_partkey,
       n_name,
       SUM(ps_supplycost * ps_availqty) AS total_value
FROM partsupp,
     supplier,
     nation
where ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'INDIA'
GROUP BY ps_partkey,
         n_name
HAVING SUM(ps_supplycost * ps_availqty) >
  (SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     and s_nationkey = n_nationkey
     and n_name = 'INDIA' )
ORDER BY total_value DESC ;

-- =================================================================
-- Query ID: TPCHN119
-- =================================================================
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
from orders,
     lineitem
where o_orderkey = l_orderkey
  and l_shipmode = 'SHIP'
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1995-01-01'
  and l_receiptdate < date '1995-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode ;

-- =================================================================
-- Query ID: TPCHN120
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
-- Query ID: TPCHN121
-- =================================================================
select 100.00 * sum(case
                        when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount)
                        else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1995-01-01' + interval '1' month ;

-- =================================================================
-- Query ID: TPCHN122
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
-- Query ID: TPCHN123
-- =================================================================
select sum(l_extendedprice) / 7.0 as avg_yearly
from lineitem,
     part
where p_partkey = l_partkey
  and p_brand = 'Brand#53'
  and p_container = 'MED BAG'
  and l_quantity <
    (select 0.7 * avg(l_quantity)
     from lineitem
     where l_partkey = p_partkey ) ;

-- =================================================================
-- Query ID: TPCHN124
-- =================================================================
select c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
from customer,
     orders,
     lineitem
where o_orderkey in
    (select l_orderkey
     from lineitem
     group by l_orderkey
     having sum(l_quantity) > 300)
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
order by o_totalprice desc,
         o_orderdate ;

-- =================================================================
-- Query ID: TPCHN125
-- =================================================================
select sum(l_extendedprice * (1 - l_discount)) as revenue
from lineitem,
     part
where (p_partkey = l_partkey
       and p_brand = 'Brand#12'
       and p_container in ('SM CASE',
                           'SM BOX',
                           'SM PACK',
                           'SM PKG')
       and l_quantity >= 1
       and l_quantity <= 1 + 10
       and p_size between 1 and 5
       and l_shipmode in ('AIR',
                          'AIR REG')
       and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#23'
      and p_container in ('MED BAG',
                          'MED BOX',
                          'MED PKG',
                          'MED PACK')
      and l_quantity >= 10
      and l_quantity <= 10 + 10
      and p_size between 1 and 10
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON')
  or (p_partkey = l_partkey
      and p_brand = 'Brand#34'
      and p_container in ('LG CASE',
                          'LG BOX',
                          'LG PACK',
                          'LG PKG')
      and l_quantity >= 20
      and l_quantity <= 20 + 10
      and p_size between 1 and 15
      and l_shipmode in ('AIR',
                         'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON') ;

-- =================================================================
-- Query ID: TPCHN126
-- =================================================================
select s_name,
       count(*) as numwait
from supplier,
     lineitem l1,
     orders,
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
         s_name ;
