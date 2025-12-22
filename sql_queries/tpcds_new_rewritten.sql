-- =================================================================
-- Query ID: TPCDSN1
-- =================================================================
SELECT c.c_customer_id
FROM
  (SELECT sr_customer_sk AS ctr_customer_sk,
          sr_store_sk AS ctr_store_sk,
          SUM(sr_fee) AS ctr_total_return
   FROM store_returns
   JOIN date_dim ON sr_returned_date_sk = d_date_sk
   WHERE d_year = 2000
   GROUP BY sr_customer_sk,
            sr_store_sk) AS ctr
JOIN store s ON s.s_store_sk = ctr.ctr_store_sk
JOIN customer c ON ctr.ctr_customer_sk = c.c_customer_sk
WHERE s.s_state = 'TN'
  AND ctr.ctr_total_return >
    (SELECT AVG(ctr_total_return) * 1.2
     FROM
       (SELECT sr_store_sk AS ctr_store_sk,
               SUM(sr_fee) AS ctr_total_return
        FROM store_returns
        JOIN date_dim ON sr_returned_date_sk = d_date_sk
        WHERE d_year = 2000
        GROUP BY sr_store_sk) AS subquery
     WHERE subquery.ctr_store_sk = ctr.ctr_store_sk )
ORDER BY c.c_customer_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN2
-- =================================================================
WITH wscs AS
  (SELECT ws_sold_date_sk AS sold_date_sk,
          ws_ext_sales_price AS sales_price
   FROM web_sales
   UNION ALL SELECT cs_sold_date_sk AS sold_date_sk,
                    cs_ext_sales_price AS sales_price
   FROM catalog_sales),
     wswscs AS
  (SELECT d_week_seq,
          SUM(CASE
                  WHEN d_day_name = 'Sunday' THEN sales_price
                  ELSE 0
              END) AS sun_sales,
          SUM(CASE
                  WHEN d_day_name = 'Monday' THEN sales_price
                  ELSE 0
              END) AS mon_sales,
          SUM(CASE
                  WHEN d_day_name = 'Tuesday' THEN sales_price
                  ELSE 0
              END) AS tue_sales,
          SUM(CASE
                  WHEN d_day_name = 'Wednesday' THEN sales_price
                  ELSE 0
              END) AS wed_sales,
          SUM(CASE
                  WHEN d_day_name = 'Thursday' THEN sales_price
                  ELSE 0
              END) AS thu_sales,
          SUM(CASE
                  WHEN d_day_name = 'Friday' THEN sales_price
                  ELSE 0
              END) AS fri_sales,
          SUM(CASE
                  WHEN d_day_name = 'Saturday' THEN sales_price
                  ELSE 0
              END) AS sat_sales
   FROM wscs
   JOIN date_dim ON d_date_sk = sold_date_sk
   GROUP BY d_week_seq),
     year_1998 AS
  (SELECT wswscs.d_week_seq AS d_week_seq1,
          sun_sales AS sun_sales1,
          mon_sales AS mon_sales1,
          tue_sales AS tue_sales1,
          wed_sales AS wed_sales1,
          thu_sales AS thu_sales1,
          fri_sales AS fri_sales1,
          sat_sales AS sat_sales1
   FROM wswscs
   JOIN date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
   WHERE d_year = 1998 ),
     year_1999 AS
  (SELECT wswscs.d_week_seq AS d_week_seq2,
          sun_sales AS sun_sales2,
          mon_sales AS mon_sales2,
          tue_sales AS tue_sales2,
          wed_sales AS wed_sales2,
          thu_sales AS thu_sales2,
          fri_sales AS fri_sales2,
          sat_sales AS sat_sales2
   FROM wswscs
   JOIN date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
   WHERE d_year = 1999 )
SELECT y.d_week_seq1,
       ROUND(y.sun_sales1 / NULLIF(z.sun_sales2, 0), 2) AS sun_sales_ratio,
       ROUND(y.mon_sales1 / NULLIF(z.mon_sales2, 0), 2) AS mon_sales_ratio,
       ROUND(y.tue_sales1 / NULLIF(z.tue_sales2, 0), 2) AS tue_sales_ratio,
       ROUND(y.wed_sales1 / NULLIF(z.wed_sales2, 0), 2) AS wed_sales_ratio,
       ROUND(y.thu_sales1 / NULLIF(z.thu_sales2, 0), 2) AS thu_sales_ratio,
       ROUND(y.fri_sales1 / NULLIF(z.fri_sales2, 0), 2) AS fri_sales_ratio,
       ROUND(y.sat_sales1 / NULLIF(z.sat_sales2, 0), 2) AS sat_sales_ratio
FROM year_1998 y
JOIN year_1999 z ON y.d_week_seq1 = z.d_week_seq2 - 53
ORDER BY y.d_week_seq1 ;

-- =================================================================
-- Query ID: TPCDSN3
-- =================================================================
SELECT dt.d_year,
       item.i_brand_id AS brand_id,
       item.i_brand AS brand,
       SUM(ss_sales_price) AS sum_agg
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manufact_id = 816
  AND dt.d_moy = 11
GROUP BY dt.d_year,
         item.i_brand_id,
         item.i_brand
ORDER BY dt.d_year,
         sum_agg DESC,
         brand_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN4
-- =================================================================
WITH year_total AS
  (SELECT c.c_customer_id AS customer_id,
          c.c_first_name AS customer_first_name,
          c.c_last_name AS customer_last_name,
          c.c_preferred_cust_flag AS customer_preferred_cust_flag,
          c.c_birth_country AS customer_birth_country,
          c.c_login AS customer_login,
          c.c_email_address AS customer_email_address,
          d.d_year AS dyear,
          SUM(((ss.ss_ext_list_price - ss.ss_ext_wholesale_cost - ss.ss_ext_discount_amt) + ss.ss_ext_sales_price) / 2) AS year_total,
          's' AS sale_type
   FROM customer c
   JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
   JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
   GROUP BY c.c_customer_id,
            c.c_first_name,
            c.c_last_name,
            c.c_preferred_cust_flag,
            c.c_birth_country,
            c.c_login,
            c.c_email_address,
            d.d_year
   UNION ALL SELECT c.c_customer_id AS customer_id,
                    c.c_first_name AS customer_first_name,
                    c.c_last_name AS customer_last_name,
                    c.c_preferred_cust_flag AS customer_preferred_cust_flag,
                    c.c_birth_country AS customer_birth_country,
                    c.c_login AS customer_login,
                    c.c_email_address AS customer_email_address,
                    d.d_year AS dyear,
                    SUM(((cs.cs_ext_list_price - cs.cs_ext_wholesale_cost - cs.cs_ext_discount_amt) + cs.cs_ext_sales_price) / 2) AS year_total,
                    'c' AS sale_type
   FROM customer c
   JOIN catalog_sales cs ON c.c_customer_sk = cs.cs_bill_customer_sk
   JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
   GROUP BY c.c_customer_id,
            c.c_first_name,
            c.c_last_name,
            c.c_preferred_cust_flag,
            c.c_birth_country,
            c.c_login,
            c.c_email_address,
            d.d_year
   UNION ALL SELECT c.c_customer_id AS customer_id,
                    c.c_first_name AS customer_first_name,
                    c.c_last_name AS customer_last_name,
                    c.c_preferred_cust_flag AS customer_preferred_cust_flag,
                    c.c_birth_country AS customer_birth_country,
                    c.c_login AS customer_login,
                    c.c_email_address AS customer_email_address,
                    d.d_year AS dyear,
                    SUM(((ws.ws_ext_list_price - ws.ws_ext_wholesale_cost - ws.ws_ext_discount_amt) + ws.ws_ext_sales_price) / 2) AS year_total,
                    'w' AS sale_type
   FROM customer c
   JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
   JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
   GROUP BY c.c_customer_id,
            c.c_first_name,
            c.c_last_name,
            c.c_preferred_cust_flag,
            c.c_birth_country,
            c.c_login,
            c.c_email_address,
            d.d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_birth_country
FROM year_total t_s_firstyear
JOIN year_total t_s_secyear ON t_s_secyear.customer_id = t_s_firstyear.customer_id
AND t_s_secyear.sale_type = 's'
AND t_s_secyear.dyear = 1999
JOIN year_total t_c_firstyear ON t_c_firstyear.customer_id = t_s_firstyear.customer_id
AND t_c_firstyear.sale_type = 'c'
AND t_c_firstyear.dyear = 1999
JOIN year_total t_c_secyear ON t_c_secyear.customer_id = t_c_firstyear.customer_id
AND t_c_secyear.sale_type = 'c'
AND t_c_secyear.dyear = 1999
JOIN year_total t_w_firstyear ON t_w_firstyear.customer_id = t_s_firstyear.customer_id
AND t_w_firstyear.sale_type = 'w'
AND t_w_firstyear.dyear = 1999
JOIN year_total t_w_secyear ON t_w_secyear.customer_id = t_w_firstyear.customer_id
AND t_w_secyear.sale_type = 'w'
AND t_w_secyear.dyear = 1999
WHERE t_s_firstyear.sale_type = 's'
  AND t_s_firstyear.dyear = 1999
  AND t_s_firstyear.year_total > 0
  AND t_c_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND COALESCE(t_c_secyear.year_total / NULLIF(t_c_firstyear.year_total, 0), 0) >= COALESCE(t_s_secyear.year_total / NULLIF(t_s_firstyear.year_total, 0), 0)
  AND COALESCE(t_c_secyear.year_total / NULLIF(t_c_firstyear.year_total, 0), 0) >= COALESCE(t_w_secyear.year_total / NULLIF(t_w_firstyear.year_total, 0), 0)
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_birth_country
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN5
-- =================================================================
WITH salesreturns AS
  (SELECT ss_store_sk AS store_sk,
          ss_sold_date_sk AS date_sk,
          ss_ext_sales_price AS sales_price,
          ss_net_profit AS profit,
          CAST(0 AS DECIMAL(7, 2)) AS return_amt,
          CAST(0 AS DECIMAL(7, 2)) AS net_loss
   FROM store_sales
   UNION ALL SELECT sr_store_sk AS store_sk,
                    sr_returned_date_sk AS date_sk,
                    CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                    CAST(0 AS DECIMAL(7, 2)) AS profit,
                    sr_return_amt AS return_amt,
                    sr_net_loss AS net_loss
   FROM store_returns),
     store_agg AS
  (SELECT s_store_id,
          SUM(sales_price) AS sales,
          SUM(profit) AS profit,
          SUM(return_amt) AS returns,
          SUM(net_loss) AS profit_loss
   FROM salesreturns
   JOIN date_dim ON date_sk = d_date_sk
   JOIN store ON store_sk = s_store_sk
   WHERE d_date BETWEEN '2000-08-19'::DATE AND '2000-09-02'::DATE
   GROUP BY s_store_id),
     catalog_salesreturns AS
  (SELECT cs_catalog_page_sk AS page_sk,
          cs_sold_date_sk AS date_sk,
          cs_ext_sales_price AS sales_price,
          cs_net_profit AS profit,
          CAST(0 AS DECIMAL(7, 2)) AS return_amt,
          CAST(0 AS DECIMAL(7, 2)) AS net_loss
   FROM catalog_sales
   UNION ALL SELECT cr_catalog_page_sk AS page_sk,
                    cr_returned_date_sk AS date_sk,
                    CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                    CAST(0 AS DECIMAL(7, 2)) AS profit,
                    cr_return_amount AS return_amt,
                    cr_net_loss AS net_loss
   FROM catalog_returns),
     catalog_agg AS
  (SELECT cp_catalog_page_id,
          SUM(sales_price) AS sales,
          SUM(profit) AS profit,
          SUM(return_amt) AS returns,
          SUM(net_loss) AS profit_loss
   FROM catalog_salesreturns
   JOIN date_dim ON date_sk = d_date_sk
   JOIN catalog_page ON page_sk = cp_catalog_page_sk
   WHERE d_date BETWEEN '2000-08-19'::DATE AND '2000-09-02'::DATE
   GROUP BY cp_catalog_page_id),
     web_salesreturns AS
  (SELECT ws_web_site_sk AS wsr_web_site_sk,
          ws_sold_date_sk AS date_sk,
          ws_ext_sales_price AS sales_price,
          ws_net_profit AS profit,
          CAST(0 AS DECIMAL(7, 2)) AS return_amt,
          CAST(0 AS DECIMAL(7, 2)) AS net_loss
   FROM web_sales
   UNION ALL SELECT ws_web_site_sk AS wsr_web_site_sk,
                    wr_returned_date_sk AS date_sk,
                    CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                    CAST(0 AS DECIMAL(7, 2)) AS profit,
                    wr_return_amt AS return_amt,
                    wr_net_loss AS net_loss
   FROM web_returns
   LEFT JOIN web_sales ON (wr_item_sk = ws_item_sk
                           AND wr_order_number = ws_order_number)),
     web_agg AS
  (SELECT web_site_id,
          SUM(sales_price) AS sales,
          SUM(profit) AS profit,
          SUM(return_amt) AS returns,
          SUM(net_loss) AS profit_loss
   FROM web_salesreturns
   JOIN date_dim ON date_sk = d_date_sk
   JOIN web_site ON wsr_web_site_sk = web_site_sk
   WHERE d_date BETWEEN '2000-08-19'::DATE AND '2000-09-02'::DATE
   GROUP BY web_site_id)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns) AS returns,
       SUM(profit) AS profit
FROM
  (SELECT 'store channel' AS channel,
          'store' || s_store_id AS id,
          sales,
          returns,
          (profit - profit_loss) AS profit
   FROM store_agg
   UNION ALL SELECT 'catalog channel' AS channel,
                    'catalog_page' || cp_catalog_page_id AS id,
                    sales,
                    returns,
                    (profit - profit_loss) AS profit
   FROM catalog_agg
   UNION ALL SELECT 'web channel' AS channel,
                    'web_site' || web_site_id AS id,
                    sales,
                    returns,
                    (profit - profit_loss) AS profit
   FROM web_agg) x
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel,
         id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN6
-- =================================================================
WITH date_seq AS
  (SELECT DISTINCT d_month_seq
   FROM date_dim
   WHERE d_year = 2002
     AND d_moy = 3 ),
     avg_price AS
  (SELECT i_category,
          AVG(i_current_price) AS avg_price
   FROM item
   GROUP BY i_category)
SELECT a.ca_state AS state,
       COUNT(*) AS cnt
FROM customer_address a
JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
JOIN item i ON s.ss_item_sk = i.i_item_sk
JOIN date_seq ds ON d.d_month_seq = ds.d_month_seq
JOIN avg_price ap ON i.i_category = ap.i_category
WHERE i.i_current_price > 1.2 * ap.avg_price
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt,
         a.ca_state
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN7
-- =================================================================
SELECT i_item_id,
       AVG(ss_quantity) AS agg1,
       AVG(ss_list_price) AS agg2,
       AVG(ss_coupon_amt) AS agg3,
       AVG(ss_sales_price) AS agg4
FROM store_sales
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN item ON ss_item_sk = i_item_sk
JOIN promotion ON ss_promo_sk = p_promo_sk
WHERE cd_gender = 'F'
  AND cd_marital_status = 'W'
  AND cd_education_status = 'College'
  AND (p_channel_email = 'N'
       OR p_channel_event = 'N')
  AND d_year = 2001
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN8
-- =================================================================
WITH valid_zips AS
  (SELECT DISTINCT substr(ca_zip, 1, 5) AS ca_zip
   FROM customer_address
   WHERE substr(ca_zip, 1, 5) IN ('47602',
                                  '16704',
                                  '35863',
                                  '28577',
                                  '83910',
                                  '36201',
                                  '58412',
                                  '48162',
                                  '28055',
                                  '41419',
                                  '80332',
                                  '38607',
                                  '77817',
                                  '24891',
                                  '16226',
                                  '18410',
                                  '21231',
                                  '59345',
                                  '13918',
                                  '51089',
                                  '20317',
                                  '17167',
                                  '54585',
                                  '67881',
                                  '78366',
                                  '47770',
                                  '18360',
                                  '51717',
                                  '73108',
                                  '14440',
                                  '21800',
                                  '89338',
                                  '45859',
                                  '65501',
                                  '34948',
                                  '25973',
                                  '73219',
                                  '25333',
                                  '17291',
                                  '10374',
                                  '18829',
                                  '60736',
                                  '82620',
                                  '41351',
                                  '52094',
                                  '19326',
                                  '25214',
                                  '54207',
                                  '40936',
                                  '21814',
                                  '79077',
                                  '25178',
                                  '75742',
                                  '77454',
                                  '30621',
                                  '89193',
                                  '27369',
                                  '41232',
                                  '48567',
                                  '83041',
                                  '71948',
                                  '37119',
                                  '68341',
                                  '14073',
                                  '16891',
                                  '62878',
                                  '49130',
                                  '19833',
                                  '24286',
                                  '27700',
                                  '40979',
                                  '50412',
                                  '81504',
                                  '94835',
                                  '84844',
                                  '71954',
                                  '39503',
                                  '57649',
                                  '18434',
                                  '24987',
                                  '12350',
                                  '86379',
                                  '27413',
                                  '44529',
                                  '98569',
                                  '16515',
                                  '27287',
                                  '24255',
                                  '21094',
                                  '16005',
                                  '56436',
                                  '91110',
                                  '68293',
                                  '56455',
                                  '54558',
                                  '10298',
                                  '83647',
                                  '32754',
                                  '27052',
                                  '51766',
                                  '19444',
                                  '13869',
                                  '45645',
                                  '94791',
                                  '57631',
                                  '20712',
                                  '37788',
                                  '41807',
                                  '46507',
                                  '21727',
                                  '71836',
                                  '81070',
                                  '50632',
                                  '88086',
                                  '63991',
                                  '20244',
                                  '31655',
                                  '51782',
                                  '29818',
                                  '63792',
                                  '68605',
                                  '94898',
                                  '36430',
                                  '57025',
                                  '20601',
                                  '82080',
                                  '33869',
                                  '22728',
                                  '35834',
                                  '29086',
                                  '92645',
                                  '98584',
                                  '98072',
                                  '11652',
                                  '78093',
                                  '57553',
                                  '43830',
                                  '71144',
                                  '53565',
                                  '18700',
                                  '90209',
                                  '71256',
                                  '38353',
                                  '54364',
                                  '28571',
                                  '96560',
                                  '57839',
                                  '56355',
                                  '50679',
                                  '45266',
                                  '84680',
                                  '34306',
                                  '34972',
                                  '48530',
                                  '30106',
                                  '15371',
                                  '92380',
                                  '84247',
                                  '92292',
                                  '68852',
                                  '13338',
                                  '34594',
                                  '82602',
                                  '70073',
                                  '98069',
                                  '85066',
                                  '47289',
                                  '11686',
                                  '98862',
                                  '26217',
                                  '47529',
                                  '63294',
                                  '51793',
                                  '35926',
                                  '24227',
                                  '14196',
                                  '24594',
                                  '32489',
                                  '99060',
                                  '49472',
                                  '43432',
                                  '49211',
                                  '14312',
                                  '88137',
                                  '47369',
                                  '56877',
                                  '20534',
                                  '81755',
                                  '15794',
                                  '12318',
                                  '21060',
                                  '73134',
                                  '41255',
                                  '63073',
                                  '81003',
                                  '73873',
                                  '66057',
                                  '51184',
                                  '51195',
                                  '45676',
                                  '92696',
                                  '70450',
                                  '90669',
                                  '98338',
                                  '25264',
                                  '38919',
                                  '59226',
                                  '58581',
                                  '60298',
                                  '17895',
                                  '19489',
                                  '52301',
                                  '80846',
                                  '95464',
                                  '68770',
                                  '51634',
                                  '19988',
                                  '18367',
                                  '18421',
                                  '11618',
                                  '67975',
                                  '25494',
                                  '41352',
                                  '95430',
                                  '15734',
                                  '62585',
                                  '97173',
                                  '33773',
                                  '10425',
                                  '75675',
                                  '53535',
                                  '17879',
                                  '41967',
                                  '12197',
                                  '67998',
                                  '79658',
                                  '59130',
                                  '72592',
                                  '14851',
                                  '43933',
                                  '68101',
                                  '50636',
                                  '25717',
                                  '71286',
                                  '24660',
                                  '58058',
                                  '72991',
                                  '95042',
                                  '15543',
                                  '33122',
                                  '69280',
                                  '11912',
                                  '59386',
                                  '27642',
                                  '65177',
                                  '17672',
                                  '33467',
                                  '64592',
                                  '36335',
                                  '54010',
                                  '18767',
                                  '63193',
                                  '42361',
                                  '49254',
                                  '33113',
                                  '33159',
                                  '36479',
                                  '59080',
                                  '11855',
                                  '81963',
                                  '31016',
                                  '49140',
                                  '29392',
                                  '41836',
                                  '32958',
                                  '53163',
                                  '13844',
                                  '73146',
                                  '23952',
                                  '65148',
                                  '93498',
                                  '14530',
                                  '46131',
                                  '58454',
                                  '13376',
                                  '13378',
                                  '83986',
                                  '12320',
                                  '17193',
                                  '59852',
                                  '46081',
                                  '98533',
                                  '52389',
                                  '13086',
                                  '68843',
                                  '31013',
                                  '13261',
                                  '60560',
                                  '13443',
                                  '45533',
                                  '83583',
                                  '11489',
                                  '58218',
                                  '19753',
                                  '22911',
                                  '25115',
                                  '86709',
                                  '27156',
                                  '32669',
                                  '13123',
                                  '51933',
                                  '39214',
                                  '41331',
                                  '66943',
                                  '14155',
                                  '69998',
                                  '49101',
                                  '70070',
                                  '35076',
                                  '14242',
                                  '73021',
                                  '59494',
                                  '15782',
                                  '29752',
                                  '37914',
                                  '74686',
                                  '83086',
                                  '34473',
                                  '15751',
                                  '81084',
                                  '49230',
                                  '91894',
                                  '60624',
                                  '17819',
                                  '28810',
                                  '63180',
                                  '56224',
                                  '39459',
                                  '55233',
                                  '75752',
                                  '43639',
                                  '55349',
                                  '86057',
                                  '62361',
                                  '50788',
                                  '31830',
                                  '58062',
                                  '18218',
                                  '85761',
                                  '60083',
                                  '45484',
                                  '21204',
                                  '90229',
                                  '70041',
                                  '41162',
                                  '35390',
                                  '16364',
                                  '39500',
                                  '68908',
                                  '26689',
                                  '52868',
                                  '81335',
                                  '40146',
                                  '11340',
                                  '61527',
                                  '61794',
                                  '71997',
                                  '30415',
                                  '59004',
                                  '29450',
                                  '58117',
                                  '69952',
                                  '33562',
                                  '83833',
                                  '27385',
                                  '61860',
                                  '96435',
                                  '48333',
                                  '23065',
                                  '32961',
                                  '84919',
                                  '61997',
                                  '99132',
                                  '22815',
                                  '56600',
                                  '68730',
                                  '48017',
                                  '95694',
                                  '32919',
                                  '88217',
                                  '27116',
                                  '28239',
                                  '58032',
                                  '18884',
                                  '16791',
                                  '21343',
                                  '97462',
                                  '18569',
                                  '75660',
                                  '15475') ),
     preferred_zips AS
  (SELECT substr(ca_zip, 1, 5) AS ca_zip
   FROM customer_address
   JOIN customer ON ca_address_sk = c_current_addr_sk
   WHERE c_preferred_cust_flag = 'Y'
   GROUP BY substr(ca_zip, 1, 5)
   HAVING COUNT(*) > 10),
     intersect_zips AS
  (SELECT ca_zip
   FROM valid_zips INTERSECT SELECT ca_zip
   FROM preferred_zips)
SELECT s_store_name,
       SUM(ss_net_profit) AS total_net_profit
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN intersect_zips V1 ON substr(s_zip, 1, 1) = substr(V1.ca_zip, 1, 1)
WHERE d_qoy = 2
  AND d_year = 1998
GROUP BY s_store_name
ORDER BY s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN9
-- =================================================================
WITH sales_data AS
  (SELECT ss_quantity,
          ss_ext_tax,
          ss_net_paid_inc_tax
   FROM store_sales),
     bucket_counts AS
  (SELECT SUM(CASE
                  WHEN ss_quantity BETWEEN 1 AND 20 THEN 1
                  ELSE 0
              END) AS count1,
          SUM(CASE
                  WHEN ss_quantity BETWEEN 21 AND 40 THEN 1
                  ELSE 0
              END) AS count2,
          SUM(CASE
                  WHEN ss_quantity BETWEEN 41 AND 60 THEN 1
                  ELSE 0
              END) AS count3,
          SUM(CASE
                  WHEN ss_quantity BETWEEN 61 AND 80 THEN 1
                  ELSE 0
              END) AS count4,
          SUM(CASE
                  WHEN ss_quantity BETWEEN 81 AND 100 THEN 1
                  ELSE 0
              END) AS count5
   FROM sales_data),
     bucket_averages AS
  (SELECT AVG(CASE
                  WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax1,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid1,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax2,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid2,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax3,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid3,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax4,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid4,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax5,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid5
   FROM sales_data)
SELECT CASE
           WHEN count1 > 1071 THEN avg_ext_tax1
           ELSE avg_net_paid1
       END AS bucket1,
       CASE
           WHEN count2 > 39161 THEN avg_ext_tax2
           ELSE avg_net_paid2
       END AS bucket2,
       CASE
           WHEN count3 > 29434 THEN avg_ext_tax3
           ELSE avg_net_paid3
       END AS bucket3,
       CASE
           WHEN count4 > 6568 THEN avg_ext_tax4
           ELSE avg_net_paid4
       END AS bucket4,
       CASE
           WHEN count5 > 21216 THEN avg_ext_tax5
           ELSE avg_net_paid5
       END AS bucket5
FROM bucket_counts,
     bucket_averages,
     reason
WHERE r_reason_sk = 1 ;

-- =================================================================
-- Query ID: TPCDSN10
-- =================================================================
WITH date_filter AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_year = 1999
     AND d_moy BETWEEN 1 AND 12 ),
     store_sales_filter AS
  (SELECT ss_customer_sk
   FROM store_sales
   JOIN date_filter ON ss_sold_date_sk = d_date_sk),
     web_sales_filter AS
  (SELECT ws_bill_customer_sk
   FROM web_sales
   JOIN date_filter ON ws_sold_date_sk = d_date_sk),
     catalog_sales_filter AS
  (SELECT cs_ship_customer_sk
   FROM catalog_sales
   JOIN date_filter ON cs_sold_date_sk = d_date_sk),
     customer_filter AS
  (SELECT DISTINCT c_customer_sk
   FROM customer
   WHERE EXISTS
       (SELECT 1
        FROM store_sales_filter
        WHERE ss_customer_sk = c_customer_sk)
     AND (EXISTS
            (SELECT 1
             FROM web_sales_filter
             WHERE ws_bill_customer_sk = c_customer_sk)
          OR EXISTS
            (SELECT 1
             FROM catalog_sales_filter
             WHERE cs_ship_customer_sk = c_customer_sk)) )
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       COUNT(*) AS cnt1,
       cd_purchase_estimate,
       COUNT(*) AS cnt2,
       cd_credit_rating,
       COUNT(*) AS cnt3,
       cd_dep_count,
       COUNT(*) AS cnt4,
       cd_dep_employed_count,
       COUNT(*) AS cnt5,
       cd_dep_college_count,
       COUNT(*) AS cnt6
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd_demo_sk = c.c_current_cdemo_sk
JOIN customer_filter cf ON c.c_customer_sk = cf.c_customer_sk
WHERE ca_county IN ('Fairfield County',
                    'Campbell County',
                    'Washtenaw County',
                    'Escambia County',
                    'Cleburne County',
                    'United States',
                    '1')
GROUP BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
ORDER BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN11
-- =================================================================
WITH year_total AS
  (SELECT c.c_customer_id AS customer_id,
          c.c_first_name AS customer_first_name,
          c.c_last_name AS customer_last_name,
          c.c_preferred_cust_flag AS customer_preferred_cust_flag,
          c.c_birth_country AS customer_birth_country,
          c.c_login AS customer_login,
          c.c_email_address AS customer_email_address,
          d.d_year AS dyear,
          SUM(ss.ss_ext_list_price - ss.ss_ext_discount_amt) AS year_total,
          's' AS sale_type
   FROM customer c
   JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
   JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
   GROUP BY c.c_customer_id,
            c.c_first_name,
            c.c_last_name,
            c.c_preferred_cust_flag,
            c.c_birth_country,
            c.c_login,
            c.c_email_address,
            d.d_year
   UNION ALL SELECT c.c_customer_id AS customer_id,
                    c.c_first_name AS customer_first_name,
                    c.c_last_name AS customer_last_name,
                    c.c_preferred_cust_flag AS customer_preferred_cust_flag,
                    c.c_birth_country AS customer_birth_country,
                    c.c_login AS customer_login,
                    c.c_email_address AS customer_email_address,
                    d.d_year AS dyear,
                    SUM(ws.ws_ext_list_price - ws.ws_ext_discount_amt) AS year_total,
                    'w' AS sale_type
   FROM customer c
   JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
   JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
   GROUP BY c.c_customer_id,
            c.c_first_name,
            c.c_last_name,
            c.c_preferred_cust_flag,
            c.c_birth_country,
            c.c_login,
            c.c_email_address,
            d.d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_email_address
FROM year_total t_s_firstyear
JOIN year_total t_s_secyear ON t_s_secyear.customer_id = t_s_firstyear.customer_id
JOIN year_total t_w_firstyear ON t_s_firstyear.customer_id = t_w_firstyear.customer_id
JOIN year_total t_w_secyear ON t_s_firstyear.customer_id = t_w_secyear.customer_id
WHERE t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 1999
  AND t_s_secyear.dyear = 1999
  AND t_w_firstyear.dyear = 1999
  AND t_w_secyear.dyear = 1999
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND COALESCE(t_w_secyear.year_total / NULLIF(t_w_firstyear.year_total, 0), 0) >= COALESCE(t_s_secyear.year_total / NULLIF(t_s_firstyear.year_total, 0), 0)
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_email_address
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN12
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ws_ext_sales_price) AS itemrevenue,
       SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
WHERE i_category IN ('Men',
                     'Books',
                     'Electronics')
  AND d_date BETWEEN '2001-06-15' AND '2001-07-15'
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN13
-- =================================================================
SELECT AVG(ss_quantity),
       AVG(ss_ext_sales_price),
       AVG(ss_ext_wholesale_cost),
       SUM(ss_ext_wholesale_cost)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2001
  AND ((cd_marital_status = 'M'
        AND cd_education_status = 'College'
        AND ss_sales_price BETWEEN 100.00 AND 150.00
        AND hd_dep_count = 3)
       OR (cd_marital_status = 'D'
           AND cd_education_status = 'Primary'
           AND ss_sales_price BETWEEN 50.00 AND 100.00
           AND hd_dep_count = 1)
       OR (cd_marital_status = 'W'
           AND cd_education_status = '2 yr Degree'
           AND ss_sales_price BETWEEN 150.00 AND 200.00
           AND hd_dep_count = 1))
  AND ca_country = 'United States'
  AND ((ca_state IN ('IL',
                     'TN',
                     'TX')
        AND ss_net_profit BETWEEN 100 AND 200)
       OR (ca_state IN ('WY',
                        'OH',
                        'ID')
           AND ss_net_profit BETWEEN 150 AND 300)
       OR (ca_state IN ('MS',
                        'SC',
                        'IA')
           AND ss_net_profit BETWEEN 50 AND 250)) ;

-- =================================================================
-- Query ID: TPCDSN14
-- =================================================================
SELECT *
FROM customer
LIMIT 1000 ;

-- =================================================================
-- Query ID: TPCDSN15
-- =================================================================
SELECT ca_zip,
       SUM(cs_sales_price)
FROM catalog_sales
JOIN customer ON cs_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE (LEFT(ca_zip, 5) IN ('85669',
                           '86197',
                           '88274',
                           '83405',
                           '86475',
                           '85392',
                           '85460',
                           '80348',
                           '81792')
       OR ca_state IN ('CA',
                       'WA',
                       'GA')
       OR cs_sales_price > 500)
  AND d_qoy = 2
  AND d_year = 2001
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN16
-- =================================================================
SELECT COUNT(DISTINCT cs1.cs_order_number) AS "order count",
       SUM(cs1.cs_ext_ship_cost) AS "total shipping cost",
       SUM(cs1.cs_net_profit) AS "total net profit"
FROM catalog_sales cs1
JOIN date_dim ON cs1.cs_ship_date_sk = date_dim.d_date_sk
JOIN customer_address ON cs1.cs_ship_addr_sk = customer_address.ca_address_sk
JOIN call_center ON cs1.cs_call_center_sk = call_center.cc_call_center_sk
WHERE date_dim.d_date BETWEEN '2002-04-01' AND '2002-05-31'
  AND customer_address.ca_state = 'PA'
  AND call_center.cc_county = 'Williamson County'
  AND EXISTS
    (SELECT 1
     FROM catalog_sales cs2
     WHERE cs1.cs_order_number = cs2.cs_order_number
       AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk )
  AND NOT EXISTS
    (SELECT 1
     FROM catalog_returns cr1
     WHERE cs1.cs_order_number = cr1.cr_order_number )
GROUP BY cs1.cs_order_number
ORDER BY "order count"
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN17
-- =================================================================
WITH filtered_store_sales AS
  (SELECT ss_item_sk,
          ss_store_sk,
          ss_customer_sk,
          ss_ticket_number,
          ss_quantity
   FROM store_sales
   JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
   WHERE d1.d_quarter_name = '2001Q1' ),
     filtered_store_returns AS
  (SELECT sr_item_sk,
          sr_customer_sk,
          sr_ticket_number,
          sr_return_quantity
   FROM store_returns
   JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
   WHERE d2.d_quarter_name IN ('2001Q1',
                               '2001Q2',
                               '2001Q3') ),
     filtered_catalog_sales AS
  (SELECT cs_item_sk,
          cs_bill_customer_sk,
          cs_quantity
   FROM catalog_sales
   JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
   WHERE d3.d_quarter_name IN ('2001Q1',
                               '2001Q2',
                               '2001Q3') )
SELECT i_item_id,
       i_item_desc,
       s_state,
       COUNT(ss.ss_quantity) AS store_sales_quantitycount,
       AVG(ss.ss_quantity) AS store_sales_quantityave,
       STDDEV_SAMP(ss.ss_quantity) AS store_sales_quantitystdev,
       STDDEV_SAMP(ss.ss_quantity) / NULLIF(AVG(ss.ss_quantity), 0) AS store_sales_quantitycov,
       COUNT(sr.sr_return_quantity) AS store_returns_quantitycount,
       AVG(sr.sr_return_quantity) AS store_returns_quantityave,
       STDDEV_SAMP(sr.sr_return_quantity) AS store_returns_quantitystdev,
       STDDEV_SAMP(sr.sr_return_quantity) / NULLIF(AVG(sr.sr_return_quantity), 0) AS store_returns_quantitycov,
       COUNT(cs.cs_quantity) AS catalog_sales_quantitycount,
       AVG(cs.cs_quantity) AS catalog_sales_quantityave,
       STDDEV_SAMP(cs.cs_quantity) AS catalog_sales_quantitystdev,
       STDDEV_SAMP(cs.cs_quantity) / NULLIF(AVG(cs.cs_quantity), 0) AS catalog_sales_quantitycov
FROM filtered_store_sales ss
JOIN filtered_store_returns sr ON ss.ss_customer_sk = sr.sr_customer_sk
AND ss.ss_item_sk = sr.sr_item_sk
AND ss.ss_ticket_number = sr.sr_ticket_number
JOIN filtered_catalog_sales cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk
AND sr.sr_item_sk = cs.cs_item_sk
JOIN item ON i_item_sk = ss.ss_item_sk
JOIN store ON s_store_sk = ss.ss_store_sk
GROUP BY i_item_id,
         i_item_desc,
         s_state
ORDER BY i_item_id,
         i_item_desc,
         s_state
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN18
-- =================================================================
SELECT i_item_id,
       ca_country,
       ca_state,
       ca_county,
       AVG(cs_quantity::decimal(12, 2)) AS agg1,
       AVG(cs_list_price::decimal(12, 2)) AS agg2,
       AVG(cs_coupon_amt::decimal(12, 2)) AS agg3,
       AVG(cs_sales_price::decimal(12, 2)) AS agg4,
       AVG(cs_net_profit::decimal(12, 2)) AS agg5,
       AVG(c_birth_year::decimal(12, 2)) AS agg6,
       AVG(cd1.cd_dep_count::decimal(12, 2)) AS agg7
FROM catalog_sales
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN item ON cs_item_sk = i_item_sk
JOIN customer_demographics cd1 ON cs_bill_cdemo_sk = cd1.cd_demo_sk
JOIN customer ON cs_bill_customer_sk = c_customer_sk
JOIN customer_demographics cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
WHERE cd1.cd_gender = 'F'
  AND cd1.cd_education_status = 'Primary'
  AND c_birth_month IN (1,
                        3,
                        7,
                        11,
                        10,
                        4)
  AND d_year = 2001
  AND ca_state IN ('AL',
                   'MO',
                   'TN',
                   'GA',
                   'MT',
                   'IN',
                   'CA')
GROUP BY ROLLUP (i_item_id,
                 ca_country,
                 ca_state,
                 ca_county)
ORDER BY ca_country,
         ca_state,
         ca_county,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN19
-- =================================================================
SELECT i_brand_id AS brand_id,
       i_brand AS brand,
       i_manufact_id,
       i_manufact,
       SUM(ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN date_dim ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE i_manager_id = 14
  AND d_moy = 11
  AND d_year = 2002
  AND LEFT(ca_zip, 5) <> LEFT(s_zip, 5)
GROUP BY i_brand_id,
         i_brand,
         i_manufact_id,
         i_manufact
ORDER BY ext_price DESC,
         i_brand,
         i_brand_id,
         i_manufact_id,
         i_manufact
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN20
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(cs_ext_sales_price) AS itemrevenue,
       SUM(cs_ext_sales_price) * 100.0 / SUM(SUM(cs_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM catalog_sales
JOIN item ON cs_item_sk = i_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE i_category IN ('Books',
                     'Music',
                     'Sports')
  AND d_date BETWEEN '2002-06-18' AND '2002-07-18'
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN21
-- =================================================================
SELECT w_warehouse_name,
       i_item_id,
       SUM(CASE
               WHEN d_date < '1999-06-22' THEN inv_quantity_on_hand
               ELSE 0
           END) AS inv_before,
       SUM(CASE
               WHEN d_date >= '1999-06-22' THEN inv_quantity_on_hand
               ELSE 0
           END) AS inv_after
FROM inventory
JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
JOIN item ON i_item_sk = inv_item_sk
JOIN date_dim ON inv_date_sk = d_date_sk
WHERE i_current_price BETWEEN 0.99 AND 1.49
  AND d_date BETWEEN '1999-05-23' AND '1999-07-22'
GROUP BY w_warehouse_name,
         i_item_id
HAVING COALESCE(SUM(CASE
                        WHEN d_date >= '1999-06-22' THEN inv_quantity_on_hand
                        ELSE 0
                    END) / NULLIF(SUM(CASE
                                          WHEN d_date < '1999-06-22' THEN inv_quantity_on_hand
                                          ELSE 0
                                      END), 0), NULL) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
ORDER BY w_warehouse_name,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN22
-- =================================================================
SELECT i_product_name,
       i_brand,
       i_class,
       i_category,
       AVG(inv_quantity_on_hand) AS qoh
FROM inventory
JOIN date_dim ON inv_date_sk = d_date_sk
JOIN item ON inv_item_sk = i_item_sk
WHERE d_month_seq BETWEEN 1200 AND 1211
GROUP BY ROLLUP(i_product_name, i_brand, i_class, i_category)
ORDER BY qoh,
         i_product_name,
         i_brand,
         i_class,
         i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN23
-- =================================================================
SELECT *
FROM customer ;

-- =================================================================
-- Query ID: TPCDSN24
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'khaki'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN25
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'seashell'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN26
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       MAX(ss_net_profit) AS store_sales_profit,
       MAX(sr_net_loss) AS store_returns_loss,
       MAX(cs_net_profit) AS catalog_sales_profit
FROM store_sales
JOIN item ON i_item_sk = ss_item_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
LEFT JOIN store_returns ON ss_customer_sk = sr_customer_sk
AND ss_item_sk = sr_item_sk
AND ss_ticket_number = sr_ticket_number
LEFT JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
AND d2.d_moy BETWEEN 4 AND 10
AND d2.d_year = 1999
LEFT JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
AND sr_item_sk = cs_item_sk
LEFT JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
AND d3.d_moy BETWEEN 4 AND 10
AND d3.d_year = 1999
WHERE d1.d_moy = 4
  AND d1.d_year = 1999
GROUP BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
ORDER BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN27
-- =================================================================
SELECT i.i_item_id,
       AVG(cs.cs_quantity) AS agg1,
       AVG(cs.cs_list_price) AS agg2,
       AVG(cs.cs_coupon_amt) AS agg3,
       AVG(cs.cs_sales_price) AS agg4
FROM catalog_sales cs
JOIN customer_demographics cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
JOIN item i ON cs.cs_item_sk = i.i_item_sk
JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk
WHERE cd.cd_gender = 'M'
  AND cd.cd_marital_status = 'W'
  AND cd.cd_education_status = 'Unknown'
  AND (p.p_channel_email = 'N'
       OR p.p_channel_event = 'N')
  AND d.d_year = 2002
GROUP BY i.i_item_id
ORDER BY i.i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN28
-- =================================================================
SELECT i_item_id,
       s_state,
       GROUPING(s_state) AS g_state,
       AVG(ss_quantity) AS agg1,
       AVG(ss_list_price) AS agg2,
       AVG(ss_coupon_amt) AS agg3,
       AVG(ss_sales_price) AS agg4
FROM store_sales
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE cd_gender = 'M'
  AND cd_marital_status = 'W'
  AND cd_education_status = 'Secondary'
  AND d_year = 1999
  AND s_state = 'TN'
GROUP BY ROLLUP (i_item_id,
                 s_state)
ORDER BY i_item_id,
         s_state
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN29
-- =================================================================
WITH B1 AS
  (SELECT AVG(ss_list_price) AS B1_LP,
          COUNT(ss_list_price) AS B1_CNT,
          COUNT(DISTINCT ss_list_price) AS B1_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 0 AND 5
     AND (ss_list_price BETWEEN 107 AND 117
          OR ss_coupon_amt BETWEEN 1319 AND 2319
          OR ss_wholesale_cost BETWEEN 60 AND 80) ),
     B2 AS
  (SELECT AVG(ss_list_price) AS B2_LP,
          COUNT(ss_list_price) AS B2_CNT,
          COUNT(DISTINCT ss_list_price) AS B2_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 6 AND 10
     AND (ss_list_price BETWEEN 23 AND 33
          OR ss_coupon_amt BETWEEN 825 AND 1825
          OR ss_wholesale_cost BETWEEN 43 AND 63) ),
     B3 AS
  (SELECT AVG(ss_list_price) AS B3_LP,
          COUNT(ss_list_price) AS B3_CNT,
          COUNT(DISTINCT ss_list_price) AS B3_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 11 AND 15
     AND (ss_list_price BETWEEN 74 AND 84
          OR ss_coupon_amt BETWEEN 4381 AND 5381
          OR ss_wholesale_cost BETWEEN 57 AND 77) ),
     B4 AS
  (SELECT AVG(ss_list_price) AS B4_LP,
          COUNT(ss_list_price) AS B4_CNT,
          COUNT(DISTINCT ss_list_price) AS B4_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 16 AND 20
     AND (ss_list_price BETWEEN 89 AND 99
          OR ss_coupon_amt BETWEEN 3117 AND 4117
          OR ss_wholesale_cost BETWEEN 68 AND 88) ),
     B5 AS
  (SELECT AVG(ss_list_price) AS B5_LP,
          COUNT(ss_list_price) AS B5_CNT,
          COUNT(DISTINCT ss_list_price) AS B5_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 21 AND 25
     AND (ss_list_price BETWEEN 58 AND 68
          OR ss_coupon_amt BETWEEN 9402 AND 10402
          OR ss_wholesale_cost BETWEEN 38 AND 58) ),
     B6 AS
  (SELECT AVG(ss_list_price) AS B6_LP,
          COUNT(ss_list_price) AS B6_CNT,
          COUNT(DISTINCT ss_list_price) AS B6_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 26 AND 30
     AND (ss_list_price BETWEEN 64 AND 74
          OR ss_coupon_amt BETWEEN 5792 AND 6792
          OR ss_wholesale_cost BETWEEN 73 AND 93) )
SELECT *
FROM B1,
     B2,
     B3,
     B4,
     B5,
     B6
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN30
-- =================================================================
SELECT i.i_item_id,
       i.i_item_desc,
       s.s_store_id,
       s.s_store_name,
       MAX(ss.ss_quantity) AS store_sales_quantity,
       MAX(sr.sr_return_quantity) AS store_returns_quantity,
       MAX(cs.cs_quantity) AS catalog_sales_quantity
FROM store_sales ss
JOIN date_dim d1 ON d1.d_date_sk = ss.ss_sold_date_sk
JOIN item i ON i.i_item_sk = ss.ss_item_sk
JOIN store s ON s.s_store_sk = ss.ss_store_sk
LEFT JOIN store_returns sr ON ss.ss_customer_sk = sr.sr_customer_sk
AND ss.ss_item_sk = sr.sr_item_sk
AND ss.ss_ticket_number = sr.sr_ticket_number
LEFT JOIN date_dim d2 ON sr.sr_returned_date_sk = d2.d_date_sk
LEFT JOIN catalog_sales cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk
AND sr.sr_item_sk = cs.cs_item_sk
LEFT JOIN date_dim d3 ON cs.cs_sold_date_sk = d3.d_date_sk
WHERE d1.d_moy = 4
  AND d1.d_year = 1998
  AND (d2.d_moy BETWEEN 4 AND 7
       AND d2.d_year = 1998
       OR d2.d_date_sk IS NULL)
  AND (d3.d_year IN (1998,
                     1999,
                     2000)
       OR d3.d_date_sk IS NULL)
GROUP BY i.i_item_id,
         i.i_item_desc,
         s.s_store_id,
         s.s_store_name
ORDER BY i.i_item_id,
         i.i_item_desc,
         s.s_store_id,
         s.s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN31
-- =================================================================
WITH customer_total_return AS
  (SELECT wr_returning_customer_sk AS ctr_customer_sk,
          ca_state AS ctr_state,
          SUM(wr_return_amt) AS ctr_total_return
   FROM web_returns
   JOIN date_dim ON wr_returned_date_sk = d_date_sk
   JOIN customer_address ON wr_returning_addr_sk = ca_address_sk
   WHERE d_year = 2000
   GROUP BY wr_returning_customer_sk,
            ca_state),
     avg_total_return AS
  (SELECT ctr_state,
          AVG(ctr_total_return) * 1.2 AS avg_return_threshold
   FROM customer_total_return
   GROUP BY ctr_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       c_preferred_cust_flag,
       c_birth_day,
       c_birth_month,
       c_birth_year,
       c_birth_country,
       c_login,
       c_email_address,
       c_last_review_date,
       ctr1.ctr_total_return
FROM customer_total_return ctr1
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
JOIN avg_total_return atr ON ctr1.ctr_state = atr.ctr_state
WHERE ctr1.ctr_total_return > atr.avg_return_threshold
  AND ca_state = 'IN'
ORDER BY c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         c_preferred_cust_flag,
         c_birth_day,
         c_birth_month,
         c_birth_year,
         c_birth_country,
         c_login,
         c_email_address,
         c_last_review_date,
         ctr_total_return
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN32
-- =================================================================
WITH ss AS
  (SELECT ca_county,
          d_qoy,
          d_year,
          SUM(ss_ext_sales_price) AS store_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer_address ON ss_addr_sk = ca_address_sk
   GROUP BY ca_county,
            d_qoy,
            d_year),
     ws AS
  (SELECT ca_county,
          d_qoy,
          d_year,
          SUM(ws_ext_sales_price) AS web_sales
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
   GROUP BY ca_county,
            d_qoy,
            d_year)
SELECT ss1.ca_county,
       ss1.d_year,
       ws2.web_sales / NULLIF(ws1.web_sales, 0) AS web_q1_q2_increase,
       ss2.store_sales / NULLIF(ss1.store_sales, 0) AS store_q1_q2_increase,
       ws3.web_sales / NULLIF(ws2.web_sales, 0) AS web_q2_q3_increase,
       ss3.store_sales / NULLIF(ss2.store_sales, 0) AS store_q2_q3_increase
FROM ss ss1
JOIN ss ss2 ON ss1.ca_county = ss2.ca_county
AND ss2.d_qoy = 2
AND ss2.d_year = 1999
JOIN ss ss3 ON ss2.ca_county = ss3.ca_county
AND ss3.d_qoy = 3
AND ss3.d_year = 1999
JOIN ws ws1 ON ss1.ca_county = ws1.ca_county
AND ws1.d_qoy = 1
AND ws1.d_year = 1999
JOIN ws ws2 ON ws1.ca_county = ws2.ca_county
AND ws2.d_qoy = 2
AND ws2.d_year = 1999
JOIN ws ws3 ON ws2.ca_county = ws3.ca_county
AND ws3.d_qoy = 3
AND ws3.d_year = 1999
WHERE ss1.d_qoy = 1
  AND ss1.d_year = 1999
  AND COALESCE(ws2.web_sales / NULLIF(ws1.web_sales, 0), 0) > COALESCE(ss2.store_sales / NULLIF(ss1.store_sales, 0), 0)
  AND COALESCE(ws3.web_sales / NULLIF(ws2.web_sales, 0), 0) > COALESCE(ss3.store_sales / NULLIF(ss2.store_sales, 0), 0)
ORDER BY store_q2_q3_increase ;

-- =================================================================
-- Query ID: TPCDSN33
-- =================================================================
WITH avg_discount AS
  (SELECT 1.3 * avg(cs_ext_discount_amt) AS threshold
   FROM catalog_sales
   JOIN date_dim ON d_date_sk = cs_sold_date_sk
   WHERE d_date BETWEEN '2001-03-09' AND (CAST('2001-03-09' AS date) + INTERVAL '90' DAY)
     AND cs_item_sk IN
       (SELECT i_item_sk
        FROM item
        WHERE i_manufact_id = 722) )
SELECT SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales
JOIN item ON i_item_sk = cs_item_sk
JOIN date_dim ON d_date_sk = cs_sold_date_sk
WHERE i_manufact_id = 722
  AND d_date BETWEEN '2001-03-09' AND (CAST('2001-03-09' AS date) + INTERVAL '90' DAY)
  AND cs_ext_discount_amt >
    (SELECT threshold
     FROM avg_discount)
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN34
-- =================================================================
WITH item_filtered AS
  (SELECT i_item_sk,
          i_manufact_id
   FROM item
   WHERE i_category = 'Books' ),
     date_filtered AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_year = 2001
     AND d_moy = 3 ),
     address_filtered AS
  (SELECT ca_address_sk
   FROM customer_address
   WHERE ca_gmt_offset = -5 ),
     ss AS
  (SELECT i_manufact_id,
          SUM(ss_ext_sales_price) AS total_sales
   FROM store_sales
   JOIN item_filtered ON store_sales.ss_item_sk = item_filtered.i_item_sk
   JOIN date_filtered ON store_sales.ss_sold_date_sk = date_filtered.d_date_sk
   JOIN address_filtered ON store_sales.ss_addr_sk = address_filtered.ca_address_sk
   GROUP BY i_manufact_id),
     cs AS
  (SELECT i_manufact_id,
          SUM(cs_ext_sales_price) AS total_sales
   FROM catalog_sales
   JOIN item_filtered ON catalog_sales.cs_item_sk = item_filtered.i_item_sk
   JOIN date_filtered ON catalog_sales.cs_sold_date_sk = date_filtered.d_date_sk
   JOIN address_filtered ON catalog_sales.cs_bill_addr_sk = address_filtered.ca_address_sk
   GROUP BY i_manufact_id),
     ws AS
  (SELECT i_manufact_id,
          SUM(ws_ext_sales_price) AS total_sales
   FROM web_sales
   JOIN item_filtered ON web_sales.ws_item_sk = item_filtered.i_item_sk
   JOIN date_filtered ON web_sales.ws_sold_date_sk = date_filtered.d_date_sk
   JOIN address_filtered ON web_sales.ws_bill_addr_sk = address_filtered.ca_address_sk
   GROUP BY i_manufact_id)
SELECT i_manufact_id,
       SUM(total_sales) AS total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) AS combined_sales
GROUP BY i_manufact_id
ORDER BY total_sales DESC
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN35
-- =================================================================
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          COUNT(*) AS cnt
   FROM store_sales
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   JOIN store ON store_sales.ss_store_sk = store.s_store_sk
   JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
   WHERE (date_dim.d_dom BETWEEN 1 AND 3
          OR date_dim.d_dom BETWEEN 25 AND 28)
     AND household_demographics.hd_buy_potential IN ('1001-5000',
                                                     '0-500')
     AND household_demographics.hd_vehicle_count > 0
     AND (household_demographics.hd_dep_count / NULLIF(household_demographics.hd_vehicle_count, 0)) > 1.2
     AND date_dim.d_year BETWEEN 2000 AND 2002
     AND store.s_county = 'Williamson County'
   GROUP BY ss_ticket_number,
            ss_customer_sk) AS dn
JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
WHERE cnt BETWEEN 15 AND 20
ORDER BY c_last_name,
         c_first_name,
         c_salutation,
         c_preferred_cust_flag DESC,
         ss_ticket_number ;

-- =================================================================
-- Query ID: TPCDSN36
-- =================================================================
WITH sales_1999 AS
  (SELECT ss_customer_sk AS customer_sk
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_year = 1999
     AND d_qoy < 4
   UNION SELECT ws_bill_customer_sk AS customer_sk
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE d_year = 1999
     AND d_qoy < 4
   UNION SELECT cs_ship_customer_sk AS customer_sk
   FROM catalog_sales
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE d_year = 1999
     AND d_qoy < 4 )
SELECT ca_state,
       cd_gender,
       cd_marital_status,
       cd_dep_count,
       COUNT(*) AS cnt1,
       AVG(cd_dep_count) AS avg_dep_count,
       STDDEV_SAMP(cd_dep_count) AS stddev_dep_count,
       SUM(cd_dep_count) AS sum_dep_count,
       cd_dep_employed_count,
       COUNT(*) AS cnt2,
       AVG(cd_dep_employed_count) AS avg_dep_employed_count,
       STDDEV_SAMP(cd_dep_employed_count) AS stddev_dep_employed_count,
       SUM(cd_dep_employed_count) AS sum_dep_employed_count,
       cd_dep_college_count,
       COUNT(*) AS cnt3,
       AVG(cd_dep_college_count) AS avg_dep_college_count,
       STDDEV_SAMP(cd_dep_college_count) AS stddev_dep_college_count,
       SUM(cd_dep_college_count) AS sum_dep_college_count
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd_demo_sk = c.c_current_cdemo_sk
WHERE c.c_customer_sk IN
    (SELECT customer_sk
     FROM sales_1999)
GROUP BY ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
ORDER BY ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN37
-- =================================================================
SELECT *
FROM customer ;

-- =================================================================
-- Query ID: TPCDSN38
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
JOIN catalog_sales ON cs_item_sk = i_item_sk
WHERE i_current_price BETWEEN 29 AND 59
  AND d_date BETWEEN DATE '2002-03-29' AND DATE '2002-05-28'
  AND i_manufact_id IN (393,
                        174,
                        251,
                        445)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN39
-- =================================================================
SELECT COUNT(*)
FROM
  (SELECT c_last_name,
          c_first_name,
          d_date
   FROM
     (SELECT c_last_name,
             c_first_name,
             d_date
      FROM store_sales
      JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1189 AND 1200
      UNION SELECT c_last_name,
                   c_first_name,
                   d_date
      FROM catalog_sales
      JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1189 AND 1200
      UNION SELECT c_last_name,
                   c_first_name,
                   d_date
      FROM web_sales
      JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1189 AND 1200 ) AS combined_sales
   GROUP BY c_last_name,
            c_first_name,
            d_date
   HAVING COUNT(*) = 3) AS hot_cust
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN40
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN41
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
  and inv1.cov > 1.5
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN42
-- =================================================================
SELECT w_state,
       i_item_id,
       SUM(CASE
               WHEN d_date < '2001-05-02' THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
               ELSE 0
           END) AS sales_before,
       SUM(CASE
               WHEN d_date >= '2001-05-02' THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
               ELSE 0
           END) AS sales_after
FROM catalog_sales
LEFT JOIN catalog_returns ON cs_order_number = cr_order_number
AND cs_item_sk = cr_item_sk
JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
JOIN item ON i_item_sk = cs_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE i_current_price BETWEEN 0.99 AND 1.49
  AND d_date BETWEEN '2001-04-02' AND '2001-06-01'
GROUP BY w_state,
         i_item_id
ORDER BY w_state,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN43
-- =================================================================
SELECT DISTINCT i_product_name
FROM item i1
WHERE i_manufact_id BETWEEN 704 AND 744
  AND EXISTS
    (SELECT 1
     FROM item
     WHERE i_manufact = i1.i_manufact
       AND ((i_category = 'Women'
             AND i_color IN ('forest',
                             'lime')
             AND i_units IN ('Pallet',
                             'Pound')
             AND i_size IN ('economy',
                            'small'))
            OR (i_category = 'Women'
                AND i_color IN ('navy',
                                'slate')
                AND i_units IN ('Gross',
                                'Bunch')
                AND i_size IN ('extra large',
                               'petite'))
            OR (i_category = 'Men'
                AND i_color IN ('powder',
                                'sky')
                AND i_units IN ('Dozen',
                                'Lb')
                AND i_size IN ('N/A',
                               'large'))
            OR (i_category = 'Men'
                AND i_color IN ('maroon',
                                'smoke')
                AND i_units IN ('Ounce',
                                'Case')
                AND i_size IN ('economy',
                               'small'))
            OR (i_category = 'Women'
                AND i_color IN ('dark',
                                'aquamarine')
                AND i_units IN ('Ton',
                                'Tbl')
                AND i_size IN ('economy',
                               'small'))
            OR (i_category = 'Women'
                AND i_color IN ('frosted',
                                'plum')
                AND i_units IN ('Dram',
                                'Box')
                AND i_size IN ('extra large',
                               'petite'))
            OR (i_category = 'Men'
                AND i_color IN ('papaya',
                                'peach')
                AND i_units IN ('Bundle',
                                'Carton')
                AND i_size IN ('N/A',
                               'large'))
            OR (i_category = 'Men'
                AND i_color IN ('firebrick',
                                'sienna')
                AND i_units IN ('Cup',
                                'Each')
                AND i_size IN ('economy',
                               'small'))) )
ORDER BY i_product_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN44
-- =================================================================
SELECT dt.d_year,
       item.i_category_id,
       item.i_category,
       SUM(ss_ext_sales_price) AS total_sales
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manager_id = 1
  AND dt.d_moy = 11
  AND dt.d_year = 1998
GROUP BY dt.d_year,
         item.i_category_id,
         item.i_category
ORDER BY total_sales DESC,
         dt.d_year,
         item.i_category_id,
         item.i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN45
-- =================================================================
SELECT s_store_name,
       s_store_id,
       SUM(CASE
               WHEN d_day_name = 'Sunday' THEN ss_sales_price
               ELSE 0
           END) AS sun_sales,
       SUM(CASE
               WHEN d_day_name = 'Monday' THEN ss_sales_price
               ELSE 0
           END) AS mon_sales,
       SUM(CASE
               WHEN d_day_name = 'Tuesday' THEN ss_sales_price
               ELSE 0
           END) AS tue_sales,
       SUM(CASE
               WHEN d_day_name = 'Wednesday' THEN ss_sales_price
               ELSE 0
           END) AS wed_sales,
       SUM(CASE
               WHEN d_day_name = 'Thursday' THEN ss_sales_price
               ELSE 0
           END) AS thu_sales,
       SUM(CASE
               WHEN d_day_name = 'Friday' THEN ss_sales_price
               ELSE 0
           END) AS fri_sales,
       SUM(CASE
               WHEN d_day_name = 'Saturday' THEN ss_sales_price
               ELSE 0
           END) AS sat_sales
FROM store_sales
JOIN date_dim ON d_date_sk = ss_sold_date_sk
JOIN store ON s_store_sk = ss_store_sk
WHERE s_gmt_offset = -5
  AND d_year = 2000
GROUP BY s_store_name,
         s_store_id
ORDER BY s_store_name,
         s_store_id,
         sun_sales,
         mon_sales,
         tue_sales,
         wed_sales,
         thu_sales,
         fri_sales,
         sat_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN46
-- =================================================================
WITH ranked_items AS
  (SELECT ss_item_sk AS item_sk,
          avg(ss_net_profit) AS avg_profit,
          RANK() OVER (
                       ORDER BY avg(ss_net_profit) DESC) AS desc_rnk,
                      RANK() OVER (
                                   ORDER BY avg(ss_net_profit) ASC) AS asc_rnk
   FROM store_sales
   WHERE ss_store_sk = 4
   GROUP BY ss_item_sk
   HAVING avg(ss_net_profit) > 0.9 *
     (SELECT avg(ss_net_profit)
      FROM store_sales
      WHERE ss_store_sk = 4
        AND ss_hdemo_sk IS NOT NULL
      GROUP BY ss_store_sk))
SELECT asc_items.rnk,
       i1.i_product_name AS best_performing,
       i2.i_product_name AS worst_performing
FROM
  (SELECT item_sk,
          asc_rnk AS rnk
   FROM ranked_items
   WHERE asc_rnk < 11) asc_items
JOIN
  (SELECT item_sk,
          desc_rnk AS rnk
   FROM ranked_items
   WHERE desc_rnk < 11) desc_items ON asc_items.rnk = desc_items.rnk
JOIN item i1 ON i1.i_item_sk = asc_items.item_sk
JOIN item i2 ON i2.i_item_sk = desc_items.item_sk
ORDER BY asc_items.rnk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN47
-- =================================================================
SELECT ca_zip,
       ca_city,
       SUM(ws_sales_price)
FROM web_sales
JOIN customer ON ws_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN item ON ws_item_sk = i_item_sk
WHERE (LEFT(ca_zip, 5) IN ('85669',
                           '86197',
                           '88274',
                           '83405',
                           '86475',
                           '85392',
                           '85460',
                           '80348',
                           '81792')
       OR i_item_id IN
         (SELECT i_item_id
          FROM item
          WHERE i_item_sk IN (2,
                              3,
                              5,
                              7,
                              11,
                              13,
                              17,
                              19,
                              23,
                              29)))
  AND d_qoy = 1
  AND d_year = 2000
GROUP BY ca_zip,
         ca_city
ORDER BY ca_zip,
         ca_city
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN48
-- =================================================================
SELECT c.c_last_name,
       c.c_first_name,
       ca.ca_city,
       dn.bought_city,
       dn.ss_ticket_number,
       dn.amt,
       dn.profit
FROM
  (SELECT ss.ss_ticket_number,
          ss.ss_customer_sk,
          ca.ca_city AS bought_city,
          SUM(ss.ss_coupon_amt) AS amt,
          SUM(ss.ss_net_profit) AS profit
   FROM store_sales ss
   JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
   JOIN store s ON ss.ss_store_sk = s.s_store_sk
   JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
   JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
   WHERE (hd.hd_dep_count = 8
          OR hd.hd_vehicle_count = 0)
     AND dd.d_dow IN (0,
                      4)
     AND dd.d_year BETWEEN 2000 AND 2002
     AND s.s_city IN ('Midway',
                      'Fairview')
   GROUP BY ss.ss_ticket_number,
            ss.ss_customer_sk,
            ca.ca_city) dn
JOIN customer c ON dn.ss_customer_sk = c.c_customer_sk
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
WHERE ca.ca_city <> dn.bought_city
ORDER BY c.c_last_name,
         c.c_first_name,
         ca.ca_city,
         dn.bought_city,
         dn.ss_ticket_number
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN49
-- =================================================================
WITH v1 AS
  (SELECT i_category,
          i_brand,
          s_store_name,
          s_company_name,
          d_year,
          d_moy,
          SUM(ss_sales_price) AS sum_sales,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_category,
                                                      i_brand,
                                                      s_store_name,
                                                      s_company_name,
                                                      d_year) AS avg_monthly_sales,
                                        ROW_NUMBER() OVER (PARTITION BY i_category,
                                                                        i_brand,
                                                                        s_store_name,
                                                                        s_company_name
                                                           ORDER BY d_year,
                                                                    d_moy) AS rn
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_year IN (1999,
                    2000,
                    2001)
     AND (d_year != 1999
          OR d_moy = 12)
     AND (d_year != 2001
          OR d_moy = 1)
   GROUP BY i_category,
            i_brand,
            s_store_name,
            s_company_name,
            d_year,
            d_moy),
     v2 AS
  (SELECT v1.s_store_name,
          v1.s_company_name,
          v1.d_year,
          v1.avg_monthly_sales,
          v1.sum_sales,
          LAG(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                               v1.i_brand,
                                               v1.s_store_name,
                                               v1.s_company_name
                                  ORDER BY v1.rn) AS psum,
                                 LEAD(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                                                       v1.i_brand,
                                                                       v1.s_store_name,
                                                                       v1.s_company_name
                                                          ORDER BY v1.rn) AS nsum
   FROM v1)
SELECT *
FROM v2
WHERE d_year = 2000
  AND avg_monthly_sales > 0
  AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         nsum
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN50
-- =================================================================
SELECT SUM(ss_quantity)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
WHERE d_year = 2001
  AND ((cd_marital_status = 'S'
        AND cd_education_status = 'Secondary'
        AND ss_sales_price BETWEEN 100.00 AND 150.00)
       OR (cd_marital_status = 'M'
           AND cd_education_status = '2 yr Degree'
           AND ss_sales_price BETWEEN 50.00 AND 100.00)
       OR (cd_marital_status = 'D'
           AND cd_education_status = 'Advanced Degree'
           AND ss_sales_price BETWEEN 150.00 AND 200.00))
  AND ca_country = 'United States'
  AND ((ca_state IN ('ND',
                     'NY',
                     'SD')
        AND ss_net_profit BETWEEN 0 AND 2000)
       OR (ca_state IN ('MD',
                        'GA',
                        'KS')
           AND ss_net_profit BETWEEN 150 AND 3000)
       OR (ca_state IN ('CO',
                        'MN',
                        'NC')
           AND ss_net_profit BETWEEN 50 AND 25000)) ;

-- =================================================================
-- Query ID: TPCDSN51
-- =================================================================
WITH ranked_sales AS
  (SELECT 'web' AS channel,
          ws.ws_item_sk AS item,
          (SUM(COALESCE(wr.wr_return_quantity, 0))::DECIMAL(15, 4) / NULLIF(SUM(ws.ws_quantity), 0)::DECIMAL(15, 4)) AS return_ratio,
          (SUM(COALESCE(wr.wr_return_amt, 0))::DECIMAL(15, 4) / NULLIF(SUM(ws.ws_net_paid), 0)::DECIMAL(15, 4)) AS currency_ratio
   FROM web_sales ws
   LEFT JOIN web_returns wr ON ws.ws_order_number = wr.wr_order_number
   AND ws.ws_item_sk = wr.wr_item_sk
   JOIN date_dim ON ws.ws_sold_date_sk = d_date_sk
   WHERE wr.wr_return_amt > 10000
     AND ws.ws_net_profit > 1
     AND ws.ws_net_paid > 0
     AND ws.ws_quantity > 0
     AND d_year = 1998
     AND d_moy = 11
   GROUP BY ws.ws_item_sk
   UNION ALL SELECT 'catalog' AS channel,
                    cs.cs_item_sk AS item,
                    (SUM(COALESCE(cr.cr_return_quantity, 0))::DECIMAL(15, 4) / NULLIF(SUM(cs.cs_quantity), 0)::DECIMAL(15, 4)) AS return_ratio,
                    (SUM(COALESCE(cr.cr_return_amount, 0))::DECIMAL(15, 4) / NULLIF(SUM(cs.cs_net_paid), 0)::DECIMAL(15, 4)) AS currency_ratio
   FROM catalog_sales cs
   LEFT JOIN catalog_returns cr ON cs.cs_order_number = cr.cr_order_number
   AND cs.cs_item_sk = cr.cr_item_sk
   JOIN date_dim ON cs.cs_sold_date_sk = d_date_sk
   WHERE cr.cr_return_amount > 10000
     AND cs.cs_net_profit > 1
     AND cs.cs_net_paid > 0
     AND cs.cs_quantity > 0
     AND d_year = 1998
     AND d_moy = 11
   GROUP BY cs.cs_item_sk
   UNION ALL SELECT 'store' AS channel,
                    sts.ss_item_sk AS item,
                    (SUM(COALESCE(sr.sr_return_quantity, 0))::DECIMAL(15, 4) / NULLIF(SUM(sts.ss_quantity), 0)::DECIMAL(15, 4)) AS return_ratio,
                    (SUM(COALESCE(sr.sr_return_amt, 0))::DECIMAL(15, 4) / NULLIF(SUM(sts.ss_net_paid), 0)::DECIMAL(15, 4)) AS currency_ratio
   FROM store_sales sts
   LEFT JOIN store_returns sr ON sts.ss_ticket_number = sr.sr_ticket_number
   AND sts.ss_item_sk = sr.sr_item_sk
   JOIN date_dim ON sts.ss_sold_date_sk = d_date_sk
   WHERE sr.sr_return_amt > 10000
     AND sts.ss_net_profit > 1
     AND sts.ss_net_paid > 0
     AND sts.ss_quantity > 0
     AND d_year = 1998
     AND d_moy = 11
   GROUP BY sts.ss_item_sk),
     ranked_results AS
  (SELECT channel,
          item,
          return_ratio,
          RANK() OVER (PARTITION BY channel
                       ORDER BY return_ratio) AS return_rank,
                      RANK() OVER (PARTITION BY channel
                                   ORDER BY currency_ratio) AS currency_rank
   FROM ranked_sales)
SELECT channel,
       item,
       return_ratio,
       return_rank,
       currency_rank
FROM ranked_results
WHERE return_rank <= 10
  OR currency_rank <= 10
ORDER BY channel,
         return_rank,
         currency_rank,
         item
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN52
-- =================================================================
SELECT s_store_name,
       s_company_id,
       s_street_number,
       s_street_name,
       s_street_type,
       s_suite_number,
       s_city,
       s_county,
       s_state,
       s_zip,
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS "30 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 30
                     AND sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS "31-60 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 60
                     AND sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS "61-90 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 90
                     AND sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS "91-120 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS ">120 days"
FROM store_sales
JOIN store_returns ON ss_ticket_number = sr_ticket_number
AND ss_item_sk = sr_item_sk
AND ss_customer_sk = sr_customer_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
WHERE d2.d_year = 2001
  AND d2.d_moy = 8
GROUP BY s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
ORDER BY s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN53
-- =================================================================
WITH web_v1 AS
  (SELECT ws_item_sk AS item_sk,
          d_date,
          SUM(ws_sales_price) OVER (PARTITION BY ws_item_sk
                                    ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1212 AND 1223
     AND ws_item_sk IS NOT NULL ),
     store_v1 AS
  (SELECT ss_item_sk AS item_sk,
          d_date,
          SUM(ss_sales_price) OVER (PARTITION BY ss_item_sk
                                    ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1212 AND 1223
     AND ss_item_sk IS NOT NULL )
SELECT item_sk,
       d_date,
       web_sales,
       store_sales,
       web_cumulative,
       store_cumulative
FROM
  (SELECT COALESCE(web.item_sk, store.item_sk) AS item_sk,
          COALESCE(web.d_date, store.d_date) AS d_date,
          web.cume_sales AS web_sales,
          store.cume_sales AS store_sales,
          MAX(web.cume_sales) OVER (PARTITION BY COALESCE(web.item_sk, store.item_sk)
                                    ORDER BY COALESCE(web.d_date, store.d_date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS web_cumulative,
                                   MAX(store.cume_sales) OVER (PARTITION BY COALESCE(web.item_sk, store.item_sk)
                                                               ORDER BY COALESCE(web.d_date, store.d_date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS store_cumulative
   FROM web_v1 web
   FULL OUTER JOIN store_v1 store ON web.item_sk = store.item_sk
   AND web.d_date = store.d_date) AS combined
WHERE web_cumulative > store_cumulative
ORDER BY item_sk,
         d_date
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN54
-- =================================================================
SELECT dt.d_year,
       item.i_brand_id AS brand_id,
       item.i_brand AS brand,
       SUM(ss_ext_sales_price) AS ext_price
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manager_id = 1
  AND dt.d_moy = 12
  AND dt.d_year = 2000
GROUP BY dt.d_year,
         item.i_brand_id,
         item.i_brand
ORDER BY dt.d_year,
         ext_price DESC,
         brand_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN55
-- =================================================================
WITH filtered_items AS
  (SELECT i_manufact_id,
          ss_sales_price,
          d_qoy
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_month_seq BETWEEN 1186 AND 1197
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('personal',
                           'portable',
                           'reference',
                           'self-help')
           AND i_brand IN ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9'))
          OR (i_category IN ('Women',
                             'Music',
                             'Men')
              AND i_class IN ('accessories',
                              'classical',
                              'fragrances',
                              'pants')
              AND i_brand IN ('amalgimporto #1',
                              'edu packscholar #1',
                              'exportiimporto #1',
                              'importoamalg #1'))) ),
     sales_aggregates AS
  (SELECT i_manufact_id,
          SUM(ss_sales_price) AS sum_sales,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) AS avg_quarterly_sales
   FROM filtered_items
   GROUP BY i_manufact_id,
            d_qoy)
SELECT *
FROM sales_aggregates
WHERE CASE
          WHEN avg_quarterly_sales > 0 THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
          ELSE NULL
      END > 0.1
ORDER BY avg_quarterly_sales,
         sum_sales,
         i_manufact_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN56
-- =================================================================
WITH cs_or_ws_sales AS
  (SELECT cs_sold_date_sk AS sold_date_sk,
          cs_bill_customer_sk AS customer_sk,
          cs_item_sk AS item_sk
   FROM catalog_sales
   UNION ALL SELECT ws_sold_date_sk AS sold_date_sk,
                    ws_bill_customer_sk AS customer_sk,
                    ws_item_sk AS item_sk
   FROM web_sales),
     my_customers AS
  (SELECT DISTINCT c.c_customer_sk,
                   c.c_current_addr_sk
   FROM cs_or_ws_sales
   JOIN item ON cs_or_ws_sales.item_sk = item.i_item_sk
   JOIN date_dim ON cs_or_ws_sales.sold_date_sk = date_dim.d_date_sk
   JOIN customer c ON c.c_customer_sk = cs_or_ws_sales.customer_sk
   WHERE item.i_category = 'Music'
     AND item.i_class = 'country'
     AND date_dim.d_moy = 1
     AND date_dim.d_year = 1999 ),
     my_revenue AS
  (SELECT c_customer_sk,
          SUM(ss_ext_sales_price) AS revenue
   FROM my_customers
   JOIN store_sales ON my_customers.c_customer_sk = store_sales.ss_customer_sk
   JOIN customer_address ON my_customers.c_current_addr_sk = customer_address.ca_address_sk
   JOIN store ON customer_address.ca_county = store.s_county
   AND customer_address.ca_state = store.s_state
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   WHERE date_dim.d_month_seq BETWEEN
       (SELECT DISTINCT d_month_seq + 1
        FROM date_dim
        WHERE d_year = 1999
          AND d_moy = 1 ) AND
       (SELECT DISTINCT d_month_seq + 3
        FROM date_dim
        WHERE d_year = 1999
          AND d_moy = 1 )
   GROUP BY c_customer_sk),
     segments AS
  (SELECT CAST((revenue / 50) AS INT) AS segment
   FROM my_revenue)
SELECT segment,
       COUNT(*) AS num_customers,
       segment * 50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment,
         num_customers
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN57
-- =================================================================
SELECT i_brand_id AS brand_id,
       i_brand AS brand,
       SUM(ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN date_dim ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE i_manager_id = 52
  AND d_moy = 11
  AND d_year = 2000
GROUP BY i_brand_id,
         i_brand
ORDER BY ext_price DESC,
         i_brand_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN58
-- =================================================================
WITH filtered_items AS
  (SELECT i_item_id
   FROM item
   WHERE i_color IN ('powder',
                     'orchid',
                     'pink') ),
     date_filter AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_year = 2000
     AND d_moy = 3 ),
     store_sales_agg AS
  (SELECT ss.ss_item_sk AS i_item_id,
          SUM(ss.ss_ext_sales_price) AS total_sales
   FROM store_sales ss
   JOIN filtered_items fi ON CAST(ss.ss_item_sk AS TEXT) = fi.i_item_id
   JOIN date_filter df ON ss.ss_sold_date_sk = df.d_date_sk
   JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
   WHERE ca.ca_gmt_offset = -6
   GROUP BY ss.ss_item_sk),
     catalog_sales_agg AS
  (SELECT cs.cs_item_sk AS i_item_id,
          SUM(cs.cs_ext_sales_price) AS total_sales
   FROM catalog_sales cs
   JOIN filtered_items fi ON CAST(cs.cs_item_sk AS TEXT) = fi.i_item_id
   JOIN date_filter df ON cs.cs_sold_date_sk = df.d_date_sk
   JOIN customer_address ca ON cs.cs_bill_addr_sk = ca.ca_address_sk
   WHERE ca.ca_gmt_offset = -6
   GROUP BY cs.cs_item_sk),
     web_sales_agg AS
  (SELECT ws.ws_item_sk AS i_item_id,
          SUM(ws.ws_ext_sales_price) AS total_sales
   FROM web_sales ws
   JOIN filtered_items fi ON CAST(ws.ws_item_sk AS TEXT) = fi.i_item_id
   JOIN date_filter df ON ws.ws_sold_date_sk = df.d_date_sk
   JOIN customer_address ca ON ws.ws_bill_addr_sk = ca.ca_address_sk
   WHERE ca.ca_gmt_offset = -6
   GROUP BY ws.ws_item_sk)
SELECT i_item_id,
       SUM(total_sales) AS total_sales
FROM
  (SELECT *
   FROM store_sales_agg
   UNION ALL SELECT *
   FROM catalog_sales_agg
   UNION ALL SELECT *
   FROM web_sales_agg) AS combined_sales
GROUP BY i_item_id
ORDER BY total_sales,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN59
-- =================================================================
WITH v1 AS
  (SELECT i_category,
          i_brand,
          cc_name,
          d_year,
          d_moy,
          SUM(cs_sales_price) AS sum_sales,
          AVG(SUM(cs_sales_price)) OVER (PARTITION BY i_category,
                                                      i_brand,
                                                      cc_name,
                                                      d_year) AS avg_monthly_sales,
                                        ROW_NUMBER() OVER (PARTITION BY i_category,
                                                                        i_brand,
                                                                        cc_name
                                                           ORDER BY d_year,
                                                                    d_moy) AS rn
   FROM item
   JOIN catalog_sales ON cs_item_sk = i_item_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN call_center ON cc_call_center_sk = cs_call_center_sk
   WHERE d_year IN (2000,
                    2001,
                    2002)
     AND (d_year != 2000
          OR d_moy = 12)
     AND (d_year != 2002
          OR d_moy = 1)
   GROUP BY i_category,
            i_brand,
            cc_name,
            d_year,
            d_moy),
     v2 AS
  (SELECT v1.i_category,
          v1.i_brand,
          v1.cc_name,
          v1.d_year,
          v1.avg_monthly_sales,
          v1.sum_sales,
          LAG(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                               v1.i_brand,
                                               v1.cc_name
                                  ORDER BY v1.rn) AS psum,
                                 LEAD(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                                                       v1.i_brand,
                                                                       v1.cc_name
                                                          ORDER BY v1.rn) AS nsum
   FROM v1)
SELECT *
FROM v2
WHERE d_year = 2001
  AND avg_monthly_sales > 0
  AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         avg_monthly_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN60
-- =================================================================
WITH date_filter AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_week_seq =
       (SELECT d_week_seq
        FROM date_dim
        WHERE d_date = '1998-11-19') ),
     ss_items AS
  (SELECT i_item_id AS item_id,
          SUM(ss_ext_sales_price) AS ss_item_rev
   FROM store_sales
   JOIN item ON ss_item_sk = i_item_sk
   JOIN date_filter ON ss_sold_date_sk = d_date_sk
   GROUP BY i_item_id),
     cs_items AS
  (SELECT i_item_id AS item_id,
          SUM(cs_ext_sales_price) AS cs_item_rev
   FROM catalog_sales
   JOIN item ON cs_item_sk = i_item_sk
   JOIN date_filter ON cs_sold_date_sk = d_date_sk
   GROUP BY i_item_id),
     ws_items AS
  (SELECT i_item_id AS item_id,
          SUM(ws_ext_sales_price) AS ws_item_rev
   FROM web_sales
   JOIN item ON ws_item_sk = i_item_sk
   JOIN date_filter ON ws_sold_date_sk = d_date_sk
   GROUP BY i_item_id)
SELECT ss_items.item_id,
       ss_item_rev,
       ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev,
       cs_item_rev,
       cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev,
       ws_item_rev,
       ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev,
       (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM ss_items
JOIN cs_items ON ss_items.item_id = cs_items.item_id
JOIN ws_items ON ss_items.item_id = ws_items.item_id
WHERE ss_item_rev BETWEEN 0.1 * cs_item_rev AND 2 * cs_item_rev
  AND ss_item_rev BETWEEN 0.1 * ws_item_rev AND 2 * ws_item_rev
  AND cs_item_rev BETWEEN 0.1 * ss_item_rev AND 2 * ss_item_rev
  AND cs_item_rev BETWEEN 0.1 * ws_item_rev AND 2 * ws_item_rev
  AND ws_item_rev BETWEEN 0.1 * ss_item_rev AND 2 * ss_item_rev
  AND ws_item_rev BETWEEN 0.1 * cs_item_rev AND 2 * cs_item_rev
ORDER BY item_id,
         ss_item_rev
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN61
-- =================================================================
WITH wss AS
  (SELECT d_week_seq,
          ss_store_sk,
          SUM(CASE
                  WHEN d_day_name = 'Sunday' THEN ss_sales_price
                  ELSE 0
              END) AS sun_sales,
          SUM(CASE
                  WHEN d_day_name = 'Monday' THEN ss_sales_price
                  ELSE 0
              END) AS mon_sales,
          SUM(CASE
                  WHEN d_day_name = 'Tuesday' THEN ss_sales_price
                  ELSE 0
              END) AS tue_sales,
          SUM(CASE
                  WHEN d_day_name = 'Wednesday' THEN ss_sales_price
                  ELSE 0
              END) AS wed_sales,
          SUM(CASE
                  WHEN d_day_name = 'Thursday' THEN ss_sales_price
                  ELSE 0
              END) AS thu_sales,
          SUM(CASE
                  WHEN d_day_name = 'Friday' THEN ss_sales_price
                  ELSE 0
              END) AS fri_sales,
          SUM(CASE
                  WHEN d_day_name = 'Saturday' THEN ss_sales_price
                  ELSE 0
              END) AS sat_sales
   FROM store_sales
   JOIN date_dim ON d_date_sk = ss_sold_date_sk
   GROUP BY d_week_seq,
            ss_store_sk)
SELECT s_store_name1,
       s_store_id1,
       d_week_seq1,
       sun_sales1 / NULLIF(sun_sales2, 0),
       mon_sales1 / NULLIF(mon_sales2, 0),
       tue_sales1 / NULLIF(tue_sales2, 0),
       wed_sales1 / NULLIF(wed_sales2, 0),
       thu_sales1 / NULLIF(thu_sales2, 0),
       fri_sales1 / NULLIF(fri_sales2, 0),
       sat_sales1 / NULLIF(sat_sales2, 0)
FROM
  (SELECT s_store_name AS s_store_name1,
          wss.d_week_seq AS d_week_seq1,
          s_store_id AS s_store_id1,
          sun_sales AS sun_sales1,
          mon_sales AS mon_sales1,
          tue_sales AS tue_sales1,
          wed_sales AS wed_sales1,
          thu_sales AS thu_sales1,
          fri_sales AS fri_sales1,
          sat_sales AS sat_sales1
   FROM wss
   JOIN store ON ss_store_sk = s_store_sk
   JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
   WHERE d_month_seq BETWEEN 1195 AND 1206 ) y
JOIN
  (SELECT s_store_name AS s_store_name2,
          wss.d_week_seq AS d_week_seq2,
          s_store_id AS s_store_id2,
          sun_sales AS sun_sales2,
          mon_sales AS mon_sales2,
          tue_sales AS tue_sales2,
          wed_sales AS wed_sales2,
          thu_sales AS thu_sales2,
          fri_sales AS fri_sales2,
          sat_sales AS sat_sales2
   FROM wss
   JOIN store ON ss_store_sk = s_store_sk
   JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
   WHERE d_month_seq BETWEEN 1207 AND 1218 ) x ON s_store_id1 = s_store_id2
AND d_week_seq1 = d_week_seq2 - 52
ORDER BY s_store_name1,
         s_store_id1,
         d_week_seq1
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN62
-- =================================================================
WITH item_ids AS
  (SELECT i_item_id::integer
   FROM item
   WHERE i_category = 'Jewelry' ),
     date_filter AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_year = 2000
     AND d_moy = 10 ),
     address_filter AS
  (SELECT ca_address_sk
   FROM customer_address
   WHERE ca_gmt_offset = -5 ),
     ss AS
  (SELECT ss_item_sk AS i_item_id,
          SUM(ss_ext_sales_price) AS total_sales
   FROM store_sales
   JOIN date_filter ON ss_sold_date_sk = d_date_sk
   JOIN address_filter ON ss_addr_sk = ca_address_sk
   WHERE ss_item_sk IN
       (SELECT i_item_id
        FROM item_ids)
   GROUP BY ss_item_sk),
     cs AS
  (SELECT cs_item_sk AS i_item_id,
          SUM(cs_ext_sales_price) AS total_sales
   FROM catalog_sales
   JOIN date_filter ON cs_sold_date_sk = d_date_sk
   JOIN address_filter ON cs_bill_addr_sk = ca_address_sk
   WHERE cs_item_sk IN
       (SELECT i_item_id
        FROM item_ids)
   GROUP BY cs_item_sk),
     ws AS
  (SELECT ws_item_sk AS i_item_id,
          SUM(ws_ext_sales_price) AS total_sales
   FROM web_sales
   JOIN date_filter ON ws_sold_date_sk = d_date_sk
   JOIN address_filter ON ws_bill_addr_sk = ca_address_sk
   WHERE ws_item_sk IN
       (SELECT i_item_id
        FROM item_ids)
   GROUP BY ws_item_sk)
SELECT i_item_id,
       SUM(total_sales) AS total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) AS combined_sales
GROUP BY i_item_id
ORDER BY i_item_id,
         total_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN63
-- =================================================================
WITH promotional_sales AS
  (SELECT SUM(ss_ext_sales_price) AS promotions
   FROM store_sales
   JOIN store ON ss_store_sk = s_store_sk
   JOIN promotion ON ss_promo_sk = p_promo_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer ON ss_customer_sk = c_customer_sk
   JOIN customer_address ON ca_address_sk = c_current_addr_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE ca_gmt_offset = -5
     AND i_category = 'Home'
     AND (p_channel_dmail = 'Y'
          OR p_channel_email = 'Y'
          OR p_channel_tv = 'Y')
     AND s_gmt_offset = -5
     AND d_year = 2000
     AND d_moy = 12 ),
     all_sales AS
  (SELECT SUM(ss_ext_sales_price) AS total
   FROM store_sales
   JOIN store ON ss_store_sk = s_store_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer ON ss_customer_sk = c_customer_sk
   JOIN customer_address ON ca_address_sk = c_current_addr_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE ca_gmt_offset = -5
     AND i_category = 'Home'
     AND s_gmt_offset = -5
     AND d_year = 2000
     AND d_moy = 12 )
SELECT ps.promotions,
       als.total,
       (CAST(ps.promotions AS DECIMAL(15, 4)) / CAST(als.total AS DECIMAL(15, 4)) * 100) AS percentage
FROM promotional_sales ps,
     all_sales als
ORDER BY ps.promotions,
         als.total
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN64
-- =================================================================
select substr(w_warehouse_name, 1, 20),
       sm_type,
       web_name,
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk <= 30) then 1
               else 0
           end) as "30 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 30)
                    and (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1
               else 0
           end) as "31-60 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 60)
                    and (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1
               else 0
           end) as "61-90 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 90)
                    and (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1
               else 0
           end) as "91-120 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1
               else 0
           end) as ">120 days"
from web_sales,
     warehouse,
     ship_mode,
     web_site,
     date_dim
where d_month_seq between 1223 and 1223 + 11
  and ws_ship_date_sk = d_date_sk
  and ws_warehouse_sk = w_warehouse_sk
  and ws_ship_mode_sk = sm_ship_mode_sk
  and ws_web_site_sk = web_site_sk
group by substr(w_warehouse_name, 1, 20),
         sm_type,
         web_name
order by substr(w_warehouse_name, 1, 20),
         sm_type,
         web_name
limit 100;

-- =================================================================
-- Query ID: TPCDSN65
-- =================================================================
WITH filtered_items AS
  (SELECT i_manager_id,
          ss_sales_price,
          d_month_seq
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_month_seq BETWEEN 1222 AND 1233
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('personal',
                           'portable',
                           'reference',
                           'self-help')
           AND i_brand IN ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9'))
          OR (i_category IN ('Women',
                             'Music',
                             'Men')
              AND i_class IN ('accessories',
                              'classical',
                              'fragrances',
                              'pants')
              AND i_brand IN ('amalgimporto #1',
                              'edu packscholar #1',
                              'exportiimporto #1',
                              'importoamalg #1'))) ),
     sales_aggregates AS
  (SELECT i_manager_id,
          SUM(ss_sales_price) AS sum_sales
   FROM filtered_items
   GROUP BY i_manager_id,
            d_month_seq),
     monthly_avg_sales AS
  (SELECT i_manager_id,
          AVG(sum_sales) AS avg_monthly_sales
   FROM sales_aggregates
   GROUP BY i_manager_id)
SELECT i_manager_id,
       sum_sales,
       avg_monthly_sales
FROM sales_aggregates
JOIN monthly_avg_sales USING (i_manager_id)
WHERE CASE
          WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
          ELSE NULL
      END > 0.1
ORDER BY i_manager_id,
         avg_monthly_sales,
         sum_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN66
-- =================================================================
WITH cs_ui AS
  (SELECT cs_item_sk,
          SUM(cs_ext_list_price) AS sale,
          SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
   FROM catalog_sales
   JOIN catalog_returns ON cs_item_sk = cr_item_sk
   AND cs_order_number = cr_order_number
   GROUP BY cs_item_sk
   HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)),
     cross_sales AS
  (SELECT i_product_name AS product_name,
          i_item_sk AS item_sk,
          s_store_name AS store_name,
          s_zip AS store_zip,
          ad1.ca_street_number AS b_street_number,
          ad1.ca_street_name AS b_street_name,
          ad1.ca_city AS b_city,
          ad1.ca_zip AS b_zip,
          ad2.ca_street_number AS c_street_number,
          ad2.ca_street_name AS c_street_name,
          ad2.ca_city AS c_city,
          ad2.ca_zip AS c_zip,
          d1.d_year AS syear,
          COUNT(*) AS cnt,
          SUM(ss_wholesale_cost) AS s1,
          SUM(ss_list_price) AS s2,
          SUM(ss_coupon_amt) AS s3
   FROM store_sales
   JOIN store_returns ON ss_item_sk = sr_item_sk
   AND ss_ticket_number = sr_ticket_number
   JOIN cs_ui ON ss_item_sk = cs_ui.cs_item_sk
   JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   JOIN customer ON ss_customer_sk = c_customer_sk
   JOIN customer_demographics cd1 ON ss_cdemo_sk = cd1.cd_demo_sk
   JOIN customer_demographics cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk
   JOIN promotion ON ss_promo_sk = p_promo_sk
   JOIN household_demographics hd1 ON ss_hdemo_sk = hd1.hd_demo_sk
   JOIN household_demographics hd2 ON c_current_hdemo_sk = hd2.hd_demo_sk
   JOIN customer_address ad1 ON ss_addr_sk = ad1.ca_address_sk
   JOIN customer_address ad2 ON c_current_addr_sk = ad2.ca_address_sk
   JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
   JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE cd1.cd_marital_status <> cd2.cd_marital_status
     AND i_color IN ('orange',
                     'lace',
                     'lawn',
                     'misty',
                     'blush',
                     'pink')
     AND i_current_price BETWEEN 48 AND 58
     AND i_current_price BETWEEN 49 AND 63
   GROUP BY i_product_name,
            i_item_sk,
            s_store_name,
            s_zip,
            ad1.ca_street_number,
            ad1.ca_street_name,
            ad1.ca_city,
            ad1.ca_zip,
            ad2.ca_street_number,
            ad2.ca_street_name,
            ad2.ca_city,
            ad2.ca_zip,
            d1.d_year)
SELECT cs1.product_name,
       cs1.store_name,
       cs1.store_zip,
       cs1.b_street_number,
       cs1.b_street_name,
       cs1.b_city,
       cs1.b_zip,
       cs1.c_street_number,
       cs1.c_street_name,
       cs1.c_city,
       cs1.c_zip,
       cs1.syear,
       cs1.cnt,
       cs1.s1 AS s11,
       cs1.s2 AS s21,
       cs1.s3 AS s31,
       cs2.s1 AS s12,
       cs2.s2 AS s22,
       cs2.s3 AS s32,
       cs2.syear,
       cs2.cnt
FROM cross_sales cs1
JOIN cross_sales cs2 ON cs1.item_sk = cs2.item_sk
AND cs1.syear = 1999
AND cs2.syear = 2000
AND cs2.cnt <= cs1.cnt
AND cs1.store_name = cs2.store_name
AND cs1.store_zip = cs2.store_zip
ORDER BY cs1.product_name,
         cs1.store_name,
         cs2.cnt,
         cs1.s1,
         cs2.s1 ;

-- =================================================================
-- Query ID: TPCDSN67
-- =================================================================
WITH sales_data AS
  (SELECT ss_store_sk,
          ss_item_sk,
          SUM(ss_sales_price) AS revenue
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1176 AND 1187
   GROUP BY ss_store_sk,
            ss_item_sk),
     average_revenue AS
  (SELECT ss_store_sk,
          AVG(revenue) AS ave
   FROM sales_data
   GROUP BY ss_store_sk)
SELECT s_store_name,
       i_item_desc,
       sc.revenue,
       i_current_price,
       i_wholesale_cost,
       i_brand
FROM store
JOIN sales_data sc ON s_store_sk = sc.ss_store_sk
JOIN average_revenue sb ON sb.ss_store_sk = sc.ss_store_sk
JOIN item ON i_item_sk = sc.ss_item_sk
WHERE sc.revenue <= 0.1 * sb.ave
ORDER BY s_store_name,
         i_item_desc
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN68
-- =================================================================
WITH sales_data AS
  (SELECT w_warehouse_name,
          w_warehouse_sq_ft,
          w_city,
          w_county,
          w_state,
          w_country,
          'ORIENTAL,BOXBUNDLES' AS ship_carriers,
          d_year AS year,
          SUM(CASE
                  WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS jan_sales,
          SUM(CASE
                  WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS feb_sales,
          SUM(CASE
                  WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS mar_sales,
          SUM(CASE
                  WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS apr_sales,
          SUM(CASE
                  WHEN d_moy = 5 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS may_sales,
          SUM(CASE
                  WHEN d_moy = 6 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS jun_sales,
          SUM(CASE
                  WHEN d_moy = 7 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS jul_sales,
          SUM(CASE
                  WHEN d_moy = 8 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS aug_sales,
          SUM(CASE
                  WHEN d_moy = 9 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS sep_sales,
          SUM(CASE
                  WHEN d_moy = 10 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS oct_sales,
          SUM(CASE
                  WHEN d_moy = 11 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS nov_sales,
          SUM(CASE
                  WHEN d_moy = 12 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS dec_sales,
          SUM(CASE
                  WHEN d_moy = 1 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS jan_net,
          SUM(CASE
                  WHEN d_moy = 2 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS feb_net,
          SUM(CASE
                  WHEN d_moy = 3 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS mar_net,
          SUM(CASE
                  WHEN d_moy = 4 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS apr_net,
          SUM(CASE
                  WHEN d_moy = 5 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS may_net,
          SUM(CASE
                  WHEN d_moy = 6 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS jun_net,
          SUM(CASE
                  WHEN d_moy = 7 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS jul_net,
          SUM(CASE
                  WHEN d_moy = 8 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS aug_net,
          SUM(CASE
                  WHEN d_moy = 9 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS sep_net,
          SUM(CASE
                  WHEN d_moy = 10 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS oct_net,
          SUM(CASE
                  WHEN d_moy = 11 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS nov_net,
          SUM(CASE
                  WHEN d_moy = 12 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS dec_net
   FROM web_sales
   JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN time_dim ON ws_sold_time_sk = t_time_sk
   JOIN ship_mode ON ws_ship_mode_sk = sm_ship_mode_sk
   WHERE d_year = 2001
     AND t_time BETWEEN 42970 AND 42970 + 28800
     AND sm_carrier IN ('ORIENTAL',
                        'BOXBUNDLES')
   GROUP BY w_warehouse_name,
            w_warehouse_sq_ft,
            w_city,
            w_county,
            w_state,
            w_country,
            d_year
   UNION ALL SELECT w_warehouse_name,
                    w_warehouse_sq_ft,
                    w_city,
                    w_county,
                    w_state,
                    w_country,
                    'ORIENTAL,BOXBUNDLES' AS ship_carriers,
                    d_year AS year,
                    SUM(CASE
                            WHEN d_moy = 1 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS jan_sales,
                    SUM(CASE
                            WHEN d_moy = 2 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS feb_sales,
                    SUM(CASE
                            WHEN d_moy = 3 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS mar_sales,
                    SUM(CASE
                            WHEN d_moy = 4 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS apr_sales,
                    SUM(CASE
                            WHEN d_moy = 5 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS may_sales,
                    SUM(CASE
                            WHEN d_moy = 6 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS jun_sales,
                    SUM(CASE
                            WHEN d_moy = 7 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS jul_sales,
                    SUM(CASE
                            WHEN d_moy = 8 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS aug_sales,
                    SUM(CASE
                            WHEN d_moy = 9 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS sep_sales,
                    SUM(CASE
                            WHEN d_moy = 10 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS oct_sales,
                    SUM(CASE
                            WHEN d_moy = 11 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS nov_sales,
                    SUM(CASE
                            WHEN d_moy = 12 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS dec_sales,
                    SUM(CASE
                            WHEN d_moy = 1 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS jan_net,
                    SUM(CASE
                            WHEN d_moy = 2 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS feb_net,
                    SUM(CASE
                            WHEN d_moy = 3 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS mar_net,
                    SUM(CASE
                            WHEN d_moy = 4 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS apr_net,
                    SUM(CASE
                            WHEN d_moy = 5 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS may_net,
                    SUM(CASE
                            WHEN d_moy = 6 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS jun_net,
                    SUM(CASE
                            WHEN d_moy = 7 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS jul_net,
                    SUM(CASE
                            WHEN d_moy = 8 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS aug_net,
                    SUM(CASE
                            WHEN d_moy = 9 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS sep_net,
                    SUM(CASE
                            WHEN d_moy = 10 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS oct_net,
                    SUM(CASE
                            WHEN d_moy = 11 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS nov_net,
                    SUM(CASE
                            WHEN d_moy = 12 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS dec_net
   FROM catalog_sales
   JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN time_dim ON cs_sold_time_sk = t_time_sk
   JOIN ship_mode ON cs_ship_mode_sk = sm_ship_mode_sk
   WHERE d_year = 2001
     AND t_time BETWEEN 42970 AND 42970 + 28800
     AND sm_carrier IN ('ORIENTAL',
                        'BOXBUNDLES')
   GROUP BY w_warehouse_name,
            w_warehouse_sq_ft,
            w_city,
            w_county,
            w_state,
            w_country,
            d_year)
SELECT w_warehouse_name,
       w_warehouse_sq_ft,
       w_city,
       w_county,
       w_state,
       w_country,
       ship_carriers,
       year,
       SUM(jan_sales) AS jan_sales,
       SUM(feb_sales) AS feb_sales,
       SUM(mar_sales) AS mar_sales,
       SUM(apr_sales) AS apr_sales,
       SUM(may_sales) AS may_sales,
       SUM(jun_sales) AS jun_sales,
       SUM(jul_sales) AS jul_sales,
       SUM(aug_sales) AS aug_sales,
       SUM(sep_sales) AS sep_sales,
       SUM(oct_sales) AS oct_sales,
       SUM(nov_sales) AS nov_sales,
       SUM(dec_sales) AS dec_sales,
       SUM(jan_sales) / w_warehouse_sq_ft AS jan_sales_per_sq_foot,
       SUM(feb_sales) / w_warehouse_sq_ft AS feb_sales_per_sq_foot,
       SUM(mar_sales) / w_warehouse_sq_ft AS mar_sales_per_sq_foot,
       SUM(apr_sales) / w_warehouse_sq_ft AS apr_sales_per_sq_foot,
       SUM(may_sales) / w_warehouse_sq_ft AS may_sales_per_sq_foot,
       SUM(jun_sales) / w_warehouse_sq_ft AS jun_sales_per_sq_foot,
       SUM(jul_sales) / w_warehouse_sq_ft AS jul_sales_per_sq_foot,
       SUM(aug_sales) / w_warehouse_sq_ft AS aug_sales_per_sq_foot,
       SUM(sep_sales) / w_warehouse_sq_ft AS sep_sales_per_sq_foot,
       SUM(oct_sales) / w_warehouse_sq_ft AS oct_sales_per_sq_foot,
       SUM(nov_sales) / w_warehouse_sq_ft AS nov_sales_per_sq_foot,
       SUM(dec_sales) / w_warehouse_sq_ft AS dec_sales_per_sq_foot,
       SUM(jan_net) AS jan_net,
       SUM(feb_net) AS feb_net,
       SUM(mar_net) AS mar_net,
       SUM(apr_net) AS apr_net,
       SUM(may_net) AS may_net,
       SUM(jun_net) AS jun_net,
       SUM(jul_net) AS jul_net,
       SUM(aug_net) AS aug_net,
       SUM(sep_net) AS sep_net,
       SUM(oct_net) AS oct_net,
       SUM(nov_net) AS nov_net,
       SUM(dec_net) AS dec_net
FROM sales_data
GROUP BY w_warehouse_name,
         w_warehouse_sq_ft,
         w_city,
         w_county,
         w_state,
         w_country,
         ship_carriers,
         year
ORDER BY w_warehouse_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN69
-- =================================================================
WITH sales_data AS
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sumsales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE d_month_seq BETWEEN 1217 AND 1217 + 11
   GROUP BY ROLLUP(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)),
     ranked_sales AS
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sumsales,
          RANK() OVER (PARTITION BY i_category
                       ORDER BY sumsales DESC) AS rk
   FROM sales_data)
SELECT *
FROM ranked_sales
WHERE rk <= 100
ORDER BY i_category,
         i_class,
         i_brand,
         i_product_name,
         d_year,
         d_qoy,
         d_moy,
         s_store_id,
         sumsales,
         rk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN70
-- =================================================================
SELECT c.c_last_name,
       c.c_first_name,
       current_addr.ca_city,
       dn.bought_city,
       dn.ss_ticket_number,
       dn.extended_price,
       dn.extended_tax,
       dn.list_price
FROM
  (SELECT ss.ss_ticket_number,
          ss.ss_customer_sk,
          ca.ca_city AS bought_city,
          SUM(ss.ss_ext_sales_price) AS extended_price,
          SUM(ss.ss_ext_list_price) AS list_price,
          SUM(ss.ss_ext_tax) AS extended_tax
   FROM store_sales ss
   JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
   JOIN store s ON ss.ss_store_sk = s.s_store_sk
   JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
   JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
   WHERE dd.d_dom BETWEEN 1 AND 2
     AND (hd.hd_dep_count = 3
          OR hd.hd_vehicle_count = 4)
     AND dd.d_year BETWEEN 1998 AND 2000
     AND s.s_city IN ('Fairview',
                      'Midway')
   GROUP BY ss.ss_ticket_number,
            ss.ss_customer_sk,
            ca.ca_city) dn
JOIN customer c ON dn.ss_customer_sk = c.c_customer_sk
JOIN customer_address current_addr ON c.c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> dn.bought_city
ORDER BY c.c_last_name,
         dn.ss_ticket_number
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN71
-- =================================================================
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       COUNT(*) AS cnt1,
       cd_purchase_estimate,
       COUNT(*) AS cnt2,
       cd_credit_rating,
       COUNT(*) AS cnt3
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE ca.ca_state IN ('IL',
                      'TX',
                      'ME')
  AND EXISTS
    (SELECT 1
     FROM store_sales ss
     JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
     WHERE c.c_customer_sk = ss.ss_customer_sk
       AND d.d_year = 2002
       AND d.d_moy BETWEEN 1 AND 3 )
  AND NOT EXISTS
    (SELECT 1
     FROM web_sales ws
     JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
     WHERE c.c_customer_sk = ws.ws_bill_customer_sk
       AND d.d_year = 2002
       AND d.d_moy BETWEEN 1 AND 3 )
  AND NOT EXISTS
    (SELECT 1
     FROM catalog_sales cs
     JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
     WHERE c.c_customer_sk = cs.cs_ship_customer_sk
       AND d.d_year = 2002
       AND d.d_moy BETWEEN 1 AND 3 )
GROUP BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
ORDER BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN72
-- =================================================================
SELECT *
FROM customer ;

-- =================================================================
-- Query ID: TPCDSN73
-- =================================================================
WITH sales_data AS
  (SELECT ws_item_sk AS sold_item_sk,
          ws_sold_time_sk AS time_sk,
          ws_ext_sales_price AS ext_price
   FROM web_sales
   JOIN date_dim ON d_date_sk = ws_sold_date_sk
   WHERE d_moy = 12
     AND d_year = 2002
   UNION ALL SELECT cs_item_sk AS sold_item_sk,
                    cs_sold_time_sk AS time_sk,
                    cs_ext_sales_price AS ext_price
   FROM catalog_sales
   JOIN date_dim ON d_date_sk = cs_sold_date_sk
   WHERE d_moy = 12
     AND d_year = 2002
   UNION ALL SELECT ss_item_sk AS sold_item_sk,
                    ss_sold_time_sk AS time_sk,
                    ss_ext_sales_price AS ext_price
   FROM store_sales
   JOIN date_dim ON d_date_sk = ss_sold_date_sk
   WHERE d_moy = 12
     AND d_year = 2002 )
SELECT i.i_brand_id AS brand_id,
       i.i_brand AS brand,
       t.t_hour,
       t.t_minute,
       SUM(s.ext_price) AS ext_price
FROM sales_data s
JOIN item i ON s.sold_item_sk = i.i_item_sk
JOIN time_dim t ON s.time_sk = t.t_time_sk
WHERE i.i_manager_id = 1
  AND (t.t_meal_time = 'breakfast'
       OR t.t_meal_time = 'dinner')
GROUP BY i.i_brand_id,
         i.i_brand,
         t.t_hour,
         t.t_minute
ORDER BY ext_price DESC,
         i.i_brand_id ;

-- =================================================================
-- Query ID: TPCDSN74
-- =================================================================
SELECT i_item_desc,
       w_warehouse_name,
       d1.d_week_seq,
       SUM(CASE
               WHEN p_promo_sk IS NULL THEN 1
               ELSE 0
           END) AS no_promo,
       SUM(CASE
               WHEN p_promo_sk IS NOT NULL THEN 1
               ELSE 0
           END) AS promo,
       COUNT(*) AS total_cnt
FROM catalog_sales
JOIN inventory ON cs_item_sk = inv_item_sk
JOIN warehouse ON w_warehouse_sk = inv_warehouse_sk
JOIN item ON i_item_sk = cs_item_sk
JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
JOIN household_demographics ON cs_bill_hdemo_sk = hd_demo_sk
JOIN date_dim d1 ON cs_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON inv_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_ship_date_sk = d3.d_date_sk
LEFT JOIN promotion ON cs_promo_sk = p_promo_sk
LEFT JOIN catalog_returns ON cr_item_sk = cs_item_sk
AND cr_order_number = cs_order_number
WHERE d1.d_week_seq = d2.d_week_seq
  AND inv_quantity_on_hand < cs_quantity
  AND d3.d_date > d1.d_date
  AND hd_buy_potential = '1001-5000'
  AND d1.d_year = 2002
  AND cd_marital_status = 'W'
GROUP BY i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
ORDER BY total_cnt DESC,
         i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN75
-- =================================================================
SELECT c.c_last_name,
       c.c_first_name,
       c.c_salutation,
       c.c_preferred_cust_flag,
       dj.ss_ticket_number,
       dj.cnt
FROM
  (SELECT ss.ss_ticket_number,
          ss.ss_customer_sk,
          COUNT(*) AS cnt
   FROM store_sales ss
   JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
   JOIN store s ON ss.ss_store_sk = s.s_store_sk
   JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
   WHERE dd.d_dom BETWEEN 1 AND 2
     AND hd.hd_buy_potential IN ('1001-5000',
                                 '5001-10000')
     AND hd.hd_vehicle_count > 0
     AND COALESCE(NULLIF(hd.hd_vehicle_count, 0), 1) < hd.hd_dep_count
     AND dd.d_year BETWEEN 2000 AND 2002
     AND s.s_county = 'Williamson County'
   GROUP BY ss.ss_ticket_number,
            ss.ss_customer_sk) dj
JOIN customer c ON dj.ss_customer_sk = c.c_customer_sk
WHERE dj.cnt BETWEEN 1 AND 5
ORDER BY dj.cnt DESC,
         c.c_last_name ASC ;

-- =================================================================
-- Query ID: TPCDSN76
-- =================================================================
WITH year_total AS
  (SELECT c_customer_id AS customer_id,
          c_first_name AS customer_first_name,
          c_last_name AS customer_last_name,
          d_year AS year,
          MAX(ss_net_paid) AS year_total,
          's' AS sale_type
   FROM customer
   JOIN store_sales ON c_customer_sk = ss_customer_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_year IN (1999,
                    2000)
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            d_year
   UNION ALL SELECT c_customer_id AS customer_id,
                    c_first_name AS customer_first_name,
                    c_last_name AS customer_last_name,
                    d_year AS year,
                    MAX(ws_net_paid) AS year_total,
                    'w' AS sale_type
   FROM customer
   JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE d_year IN (1999,
                    2000)
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name
FROM year_total t_s_firstyear
JOIN year_total t_s_secyear ON t_s_secyear.customer_id = t_s_firstyear.customer_id
JOIN year_total t_w_firstyear ON t_s_firstyear.customer_id = t_w_firstyear.customer_id
JOIN year_total t_w_secyear ON t_s_firstyear.customer_id = t_w_secyear.customer_id
WHERE t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.year = 1999
  AND t_s_secyear.year = 2000
  AND t_w_firstyear.year = 1999
  AND t_w_secyear.year = 2000
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND COALESCE(t_w_secyear.year_total / NULLIF(t_w_firstyear.year_total, 0), 0) >= COALESCE(t_s_secyear.year_total / NULLIF(t_s_firstyear.year_total, 0), 0)
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_first_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN77
-- =================================================================
WITH sales_detail AS
  (SELECT d_year,
          i_brand_id,
          i_class_id,
          i_category_id,
          i_manufact_id,
          SUM(sales_cnt) AS sales_cnt,
          SUM(sales_amt) AS sales_amt
   FROM
     (SELECT d_year,
             i_brand_id,
             i_class_id,
             i_category_id,
             i_manufact_id,
             cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
             cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt
      FROM catalog_sales
      JOIN item ON i_item_sk = cs_item_sk
      JOIN date_dim ON d_date_sk = cs_sold_date_sk
      LEFT JOIN catalog_returns ON cs_order_number = cr_order_number
      AND cs_item_sk = cr_item_sk
      WHERE i_category = 'Sports'
      UNION ALL SELECT d_year,
                       i_brand_id,
                       i_class_id,
                       i_category_id,
                       i_manufact_id,
                       ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,
                       ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt
      FROM store_sales
      JOIN item ON i_item_sk = ss_item_sk
      JOIN date_dim ON d_date_sk = ss_sold_date_sk
      LEFT JOIN store_returns ON ss_ticket_number = sr_ticket_number
      AND ss_item_sk = sr_item_sk
      WHERE i_category = 'Sports'
      UNION ALL SELECT d_year,
                       i_brand_id,
                       i_class_id,
                       i_category_id,
                       i_manufact_id,
                       ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,
                       ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt
      FROM web_sales
      JOIN item ON i_item_sk = ws_item_sk
      JOIN date_dim ON d_date_sk = ws_sold_date_sk
      LEFT JOIN web_returns ON ws_order_number = wr_order_number
      AND ws_item_sk = wr_item_sk
      WHERE i_category = 'Sports' ) AS sales
   GROUP BY d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id),
     filtered_sales AS
  (SELECT curr_yr.d_year AS year,
          prev_yr.d_year AS prev_year,
          curr_yr.i_brand_id,
          curr_yr.i_class_id,
          curr_yr.i_category_id,
          curr_yr.i_manufact_id,
          prev_yr.sales_cnt AS prev_yr_cnt,
          curr_yr.sales_cnt AS curr_yr_cnt,
          curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
          curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
   FROM sales_detail curr_yr
   JOIN sales_detail prev_yr ON curr_yr.i_brand_id = prev_yr.i_brand_id
   AND curr_yr.i_class_id = prev_yr.i_class_id
   AND curr_yr.i_category_id = prev_yr.i_category_id
   AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
   AND curr_yr.d_year = 2002
   AND prev_yr.d_year = 2001
   WHERE curr_yr.sales_cnt::DECIMAL(17, 2) / NULLIF(prev_yr.sales_cnt::DECIMAL(17, 2), 0) < 0.9 )
SELECT *
FROM filtered_sales
ORDER BY sales_cnt_diff,
         sales_amt_diff
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN78
-- =================================================================
WITH sales_data AS
  (SELECT 'store' AS channel,
          'ss_customer_sk' AS col_name,
          d_year,
          d_qoy,
          i_category,
          ss_ext_sales_price AS ext_sales_price
   FROM store_sales
   JOIN item ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE ss_customer_sk IS NOT NULL
   UNION ALL SELECT 'web' AS channel,
                    'ws_promo_sk' AS col_name,
                    d_year,
                    d_qoy,
                    i_category,
                    ws_ext_sales_price AS ext_sales_price
   FROM web_sales
   JOIN item ON ws_item_sk = i_item_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE ws_promo_sk IS NOT NULL
   UNION ALL SELECT 'catalog' AS channel,
                    'cs_bill_customer_sk' AS col_name,
                    d_year,
                    d_qoy,
                    i_category,
                    cs_ext_sales_price AS ext_sales_price
   FROM catalog_sales
   JOIN item ON cs_item_sk = i_item_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE cs_bill_customer_sk IS NOT NULL )
SELECT channel,
       col_name,
       d_year,
       d_qoy,
       i_category,
       COUNT(*) AS sales_cnt,
       SUM(ext_sales_price) AS sales_amt
FROM sales_data
GROUP BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
ORDER BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN79
-- =================================================================
WITH date_range AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_date BETWEEN '2000-08-10'::date AND '2000-09-09'::date ),
     ss AS
  (SELECT ss.ss_store_sk,
          SUM(ss.ss_ext_sales_price) AS sales,
          SUM(ss.ss_net_profit) AS profit
   FROM store_sales ss
   JOIN date_range dr ON ss.ss_sold_date_sk = dr.d_date_sk
   GROUP BY ss.ss_store_sk),
     sr AS
  (SELECT sr.sr_store_sk,
          SUM(sr.sr_return_amt) AS returns,
          SUM(sr.sr_net_loss) AS profit_loss
   FROM store_returns sr
   JOIN date_range dr ON sr.sr_returned_date_sk = dr.d_date_sk
   GROUP BY sr.sr_store_sk),
     cs AS
  (SELECT cs.cs_call_center_sk,
          SUM(cs.cs_ext_sales_price) AS sales,
          SUM(cs.cs_net_profit) AS profit
   FROM catalog_sales cs
   JOIN date_range dr ON cs.cs_sold_date_sk = dr.d_date_sk
   GROUP BY cs.cs_call_center_sk),
     cr AS
  (SELECT cr.cr_call_center_sk,
          SUM(cr.cr_return_amount) AS returns,
          SUM(cr.cr_net_loss) AS profit_loss
   FROM catalog_returns cr
   JOIN date_range dr ON cr.cr_returned_date_sk = dr.d_date_sk
   GROUP BY cr.cr_call_center_sk),
     ws AS
  (SELECT ws.ws_web_page_sk,
          SUM(ws.ws_ext_sales_price) AS sales,
          SUM(ws.ws_net_profit) AS profit
   FROM web_sales ws
   JOIN date_range dr ON ws.ws_sold_date_sk = dr.d_date_sk
   GROUP BY ws.ws_web_page_sk),
     wr AS
  (SELECT wr.wr_web_page_sk,
          SUM(wr.wr_return_amt) AS returns,
          SUM(wr.wr_net_loss) AS profit_loss
   FROM web_returns wr
   JOIN date_range dr ON wr.wr_returned_date_sk = dr.d_date_sk
   GROUP BY wr.wr_web_page_sk)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns) AS returns,
       SUM(profit) AS profit
FROM
  (SELECT 'store channel' AS channel,
          ss.ss_store_sk AS id,
          ss.sales,
          COALESCE(sr.returns, 0) AS returns,
          (ss.profit - COALESCE(sr.profit_loss, 0)) AS profit
   FROM ss
   LEFT JOIN sr ON ss.ss_store_sk = sr.sr_store_sk
   UNION ALL SELECT 'catalog channel' AS channel,
                    cs.cs_call_center_sk AS id,
                    cs.sales,
                    cr.returns,
                    (cs.profit - cr.profit_loss) AS profit
   FROM cs
   LEFT JOIN cr ON cs.cs_call_center_sk = cr.cr_call_center_sk
   UNION ALL SELECT 'web channel' AS channel,
                    ws.ws_web_page_sk AS id,
                    ws.sales,
                    COALESCE(wr.returns, 0) AS returns,
                    (ws.profit - COALESCE(wr.profit_loss, 0)) AS profit
   FROM ws
   LEFT JOIN wr ON ws.ws_web_page_sk = wr.wr_web_page_sk) x
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel,
         id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN80
-- =================================================================
WITH ws AS
  (SELECT d_year AS ws_sold_year,
          ws_item_sk,
          ws_bill_customer_sk AS ws_customer_sk,
          SUM(ws_quantity) AS ws_qty,
          SUM(ws_wholesale_cost) AS ws_wc,
          SUM(ws_sales_price) AS ws_sp
   FROM web_sales
   LEFT JOIN web_returns ON wr_order_number = ws_order_number
   AND ws_item_sk = wr_item_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE wr_order_number IS NULL
   GROUP BY d_year,
            ws_item_sk,
            ws_bill_customer_sk),
     cs AS
  (SELECT d_year AS cs_sold_year,
          cs_item_sk,
          cs_bill_customer_sk AS cs_customer_sk,
          SUM(cs_quantity) AS cs_qty,
          SUM(cs_wholesale_cost) AS cs_wc,
          SUM(cs_sales_price) AS cs_sp
   FROM catalog_sales
   LEFT JOIN catalog_returns ON cr_order_number = cs_order_number
   AND cs_item_sk = cr_item_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE cr_order_number IS NULL
   GROUP BY d_year,
            cs_item_sk,
            cs_bill_customer_sk),
     ss AS
  (SELECT d_year AS ss_sold_year,
          ss_item_sk,
          ss_customer_sk,
          SUM(ss_quantity) AS ss_qty,
          SUM(ss_wholesale_cost) AS ss_wc,
          SUM(ss_sales_price) AS ss_sp
   FROM store_sales
   LEFT JOIN store_returns ON sr_ticket_number = ss_ticket_number
   AND ss_item_sk = sr_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE sr_ticket_number IS NULL
   GROUP BY d_year,
            ss_item_sk,
            ss_customer_sk)
SELECT ss.ss_customer_sk,
       ROUND(ss.ss_qty / NULLIF((COALESCE(ws.ws_qty, 0) + COALESCE(cs.cs_qty, 0)), 0), 2) AS ratio,
       ss.ss_qty AS store_qty,
       ss.ss_wc AS store_wholesale_cost,
       ss.ss_sp AS store_sales_price,
       COALESCE(ws.ws_qty, 0) + COALESCE(cs.cs_qty, 0) AS other_chan_qty,
       COALESCE(ws.ws_wc, 0) + COALESCE(cs.cs_wc, 0) AS other_chan_wholesale_cost,
       COALESCE(ws.ws_sp, 0) + COALESCE(cs.cs_sp, 0) AS other_chan_sales_price
FROM ss
LEFT JOIN ws ON ws.ws_sold_year = ss.ss_sold_year
AND ws.ws_item_sk = ss.ss_item_sk
AND ws.ws_customer_sk = ss.ss_customer_sk
LEFT JOIN cs ON cs.cs_sold_year = ss.ss_sold_year
AND cs.cs_item_sk = ss.ss_item_sk
AND cs.cs_customer_sk = ss.ss_customer_sk
WHERE (COALESCE(ws.ws_qty, 0) > 0
       OR COALESCE(cs.cs_qty, 0) > 0)
  AND ss.ss_sold_year = 1998
ORDER BY ss.ss_customer_sk,
         ss.ss_qty DESC,
         ss.ss_wc DESC,
         ss.ss_sp DESC,
         other_chan_qty,
         other_chan_wholesale_cost,
         other_chan_sales_price,
         ratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN81
-- =================================================================
SELECT c.c_last_name,
       c.c_first_name,
       LEFT(ms.s_city, 30) AS s_city,
       ms.ss_ticket_number,
       ms.amt,
       ms.profit
FROM
  (SELECT ss.ss_ticket_number,
          ss.ss_customer_sk,
          s.s_city,
          SUM(ss.ss_coupon_amt) AS amt,
          SUM(ss.ss_net_profit) AS profit
   FROM store_sales ss
   JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
   JOIN store s ON ss.ss_store_sk = s.s_store_sk
   JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
   WHERE (hd.hd_dep_count = 7
          OR hd.hd_vehicle_count > -1)
     AND d.d_dow = 4
     AND d.d_year BETWEEN 2000 AND 2002
     AND s.s_number_employees BETWEEN 200 AND 295
   GROUP BY ss.ss_ticket_number,
            ss.ss_customer_sk,
            s.s_city) ms
JOIN customer c ON ms.ss_customer_sk = c.c_customer_sk
ORDER BY c.c_last_name,
         c.c_first_name,
         LEFT(ms.s_city, 30),
         ms.profit
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN82
-- =================================================================
WITH date_range AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_date BETWEEN '2002-08-14'::date AND '2002-09-13'::date ),
     ssr AS
  (SELECT s_store_id AS store_id,
          SUM(ss_ext_sales_price) AS sales,
          SUM(COALESCE(sr_return_amt, 0)) AS returns,
          SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
   FROM store_sales
   LEFT JOIN store_returns ON ss_item_sk = sr_item_sk
   AND ss_ticket_number = sr_ticket_number
   JOIN date_range ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   JOIN item ON ss_item_sk = i_item_sk
   JOIN promotion ON ss_promo_sk = p_promo_sk
   WHERE i_current_price > 50
     AND p_channel_tv = 'N'
   GROUP BY s_store_id),
     csr AS
  (SELECT cp_catalog_page_id AS catalog_page_id,
          SUM(cs_ext_sales_price) AS sales,
          SUM(COALESCE(cr_return_amount, 0)) AS returns,
          SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
   FROM catalog_sales
   LEFT JOIN catalog_returns ON cs_item_sk = cr_item_sk
   AND cs_order_number = cr_order_number
   JOIN date_range ON cs_sold_date_sk = d_date_sk
   JOIN catalog_page ON cs_catalog_page_sk = cp_catalog_page_sk
   JOIN item ON cs_item_sk = i_item_sk
   JOIN promotion ON cs_promo_sk = p_promo_sk
   WHERE i_current_price > 50
     AND p_channel_tv = 'N'
   GROUP BY cp_catalog_page_id),
     wsr AS
  (SELECT web_site_id,
          SUM(ws_ext_sales_price) AS sales,
          SUM(COALESCE(wr_return_amt, 0)) AS returns,
          SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
   FROM web_sales
   LEFT JOIN web_returns ON ws_item_sk = wr_item_sk
   AND ws_order_number = wr_order_number
   JOIN date_range ON ws_sold_date_sk = d_date_sk
   JOIN web_site ON ws_web_site_sk = web_site_sk
   JOIN item ON ws_item_sk = i_item_sk
   JOIN promotion ON ws_promo_sk = p_promo_sk
   WHERE i_current_price > 50
     AND p_channel_tv = 'N'
   GROUP BY web_site_id)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns) AS returns,
       SUM(profit) AS profit
FROM
  (SELECT 'store channel' AS channel,
          'store' || store_id AS id,
          sales,
          returns,
          profit
   FROM ssr
   UNION ALL SELECT 'catalog channel' AS channel,
                    'catalog_page' || catalog_page_id AS id,
                    sales,
                    returns,
                    profit
   FROM csr
   UNION ALL SELECT 'web channel' AS channel,
                    'web_site' || web_site_id AS id,
                    sales,
                    returns,
                    profit
   FROM wsr) x
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel,
         id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN83
-- =================================================================
WITH customer_total_return AS
  (SELECT cr_returning_customer_sk AS ctr_customer_sk,
          ca_state AS ctr_state,
          SUM(cr_return_amt_inc_tax) AS ctr_total_return
   FROM catalog_returns
   JOIN date_dim ON cr_returned_date_sk = d_date_sk
   JOIN customer_address ON cr_returning_addr_sk = ca_address_sk
   WHERE d_year = 2001
   GROUP BY cr_returning_customer_sk,
            ca_state),
     state_avg_return AS
  (SELECT ctr_state,
          AVG(ctr_total_return) * 1.2 AS avg_return_threshold
   FROM customer_total_return
   GROUP BY ctr_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       ca_street_number,
       ca_street_name,
       ca_street_type,
       ca_suite_number,
       ca_city,
       ca_county,
       ca_state,
       ca_zip,
       ca_country,
       ca_gmt_offset,
       ca_location_type,
       ctr1.ctr_total_return
FROM customer_total_return ctr1
JOIN state_avg_return sar ON ctr1.ctr_state = sar.ctr_state
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
WHERE ctr1.ctr_total_return > sar.avg_return_threshold
  AND ca_state = 'TN'
ORDER BY c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         ca_street_number,
         ca_street_name,
         ca_street_type,
         ca_suite_number,
         ca_city,
         ca_county,
         ca_state,
         ca_zip,
         ca_country,
         ca_gmt_offset,
         ca_location_type,
         ctr_total_return
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN84
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
JOIN store_sales ON ss_item_sk = i_item_sk
WHERE i_current_price BETWEEN 58 AND 88
  AND d_date BETWEEN DATE '2001-01-13' AND DATE '2001-03-14'
  AND i_manufact_id IN (259,
                        559,
                        580,
                        485)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN85
-- =================================================================
WITH date_filter AS
  (SELECT d_week_seq
   FROM date_dim
   WHERE d_date IN ('2001-07-13',
                    '2001-09-10',
                    '2001-11-16',
                    '2000-12-14') ),
     filtered_dates AS
  (SELECT d_date
   FROM date_dim
   WHERE d_week_seq IN
       (SELECT d_week_seq
        FROM date_filter) ),
     sr_items AS
  (SELECT i_item_id AS item_id,
          SUM(sr_return_quantity) AS sr_item_qty
   FROM store_returns
   JOIN item ON sr_item_sk = i_item_sk
   JOIN date_dim ON sr_returned_date_sk = d_date_sk
   WHERE d_date IN
       (SELECT d_date
        FROM filtered_dates)
   GROUP BY i_item_id),
     cr_items AS
  (SELECT i_item_id AS item_id,
          SUM(cr_return_quantity) AS cr_item_qty
   FROM catalog_returns
   JOIN item ON cr_item_sk = i_item_sk
   JOIN date_dim ON cr_returned_date_sk = d_date_sk
   WHERE d_date IN
       (SELECT d_date
        FROM filtered_dates)
   GROUP BY i_item_id),
     wr_items AS
  (SELECT i_item_id AS item_id,
          SUM(wr_return_quantity) AS wr_item_qty
   FROM web_returns
   JOIN item ON wr_item_sk = i_item_sk
   JOIN date_dim ON wr_returned_date_sk = d_date_sk
   WHERE d_date IN
       (SELECT d_date
        FROM filtered_dates)
   GROUP BY i_item_id)
SELECT sr_items.item_id,
       sr_item_qty,
       sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS sr_dev,
       cr_item_qty,
       cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS cr_dev,
       wr_item_qty,
       wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS wr_dev,
       (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 AS average
FROM sr_items
JOIN cr_items ON sr_items.item_id = cr_items.item_id
JOIN wr_items ON sr_items.item_id = wr_items.item_id
ORDER BY sr_items.item_id,
         sr_item_qty
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN86
-- =================================================================
SELECT c.c_customer_id AS customer_id,
       COALESCE(c.c_last_name, '') || ',' || COALESCE(c.c_first_name, '') AS customername
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
JOIN household_demographics hd ON hd.hd_demo_sk = c.c_current_hdemo_sk
JOIN income_band ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
JOIN store_returns sr ON sr.sr_cdemo_sk = cd.cd_demo_sk
WHERE ca.ca_city = 'Woodland'
  AND ib.ib_lower_bound >= 60306
  AND ib.ib_upper_bound <= 60306 + 50000
ORDER BY c.c_customer_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN87
-- =================================================================
SELECT substr(r_reason_desc, 1, 20),
       avg(ws_quantity),
       avg(wr_refunded_cash),
       avg(wr_fee)
FROM web_sales
JOIN web_returns ON ws_item_sk = wr_item_sk
AND ws_order_number = wr_order_number
JOIN web_page ON ws_web_page_sk = wp_web_page_sk
JOIN customer_demographics cd1 ON cd1.cd_demo_sk = wr_refunded_cdemo_sk
JOIN customer_demographics cd2 ON cd2.cd_demo_sk = wr_returning_cdemo_sk
JOIN customer_address ON ca_address_sk = wr_refunded_addr_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN reason ON r_reason_sk = wr_reason_sk
WHERE d_year = 1998
  AND ((cd1.cd_marital_status = 'D'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Primary'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws_sales_price BETWEEN 100.00 AND 150.00)
       OR (cd1.cd_marital_status = 'S'
           AND cd1.cd_marital_status = cd2.cd_marital_status
           AND cd1.cd_education_status = 'College'
           AND cd1.cd_education_status = cd2.cd_education_status
           AND ws_sales_price BETWEEN 50.00 AND 100.00)
       OR (cd1.cd_marital_status = 'U'
           AND cd1.cd_marital_status = cd2.cd_marital_status
           AND cd1.cd_education_status = 'Advanced Degree'
           AND cd1.cd_education_status = cd2.cd_education_status
           AND ws_sales_price BETWEEN 150.00 AND 200.00))
  AND ca_country = 'United States'
  AND ((ca_state IN ('NC',
                     'TX',
                     'IA')
        AND ws_net_profit BETWEEN 100 AND 200)
       OR (ca_state IN ('WI',
                        'WV',
                        'GA')
           AND ws_net_profit BETWEEN 150 AND 300)
       OR (ca_state IN ('OK',
                        'VA',
                        'KY')
           AND ws_net_profit BETWEEN 50 AND 250))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 20),
         avg(ws_quantity),
         avg(wr_refunded_cash),
         avg(wr_fee)
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN88
-- =================================================================
WITH sales_data AS
  (SELECT ws_net_paid,
          i_category,
          i_class,
          d_date_sk
   FROM web_sales
   JOIN date_dim d1 ON d1.d_date_sk = ws_sold_date_sk
   JOIN item ON i_item_sk = ws_item_sk
   WHERE d1.d_month_seq BETWEEN 1186 AND 1197 ),
     aggregated_data AS
  (SELECT SUM(ws_net_paid) AS total_sum,
          i_category,
          i_class,
          GROUPING(i_category) + GROUPING(i_class) AS lochierarchy
   FROM sales_data
   GROUP BY ROLLUP(i_category, i_class)),
     ranked_data AS
  (SELECT total_sum,
          i_category,
          i_class,
          lochierarchy,
          RANK() OVER (PARTITION BY lochierarchy,
                                    CASE
                                        WHEN lochierarchy = 0 THEN i_category
                                    END
                       ORDER BY total_sum DESC) AS rank_within_parent
   FROM aggregated_data)
SELECT *
FROM ranked_data
ORDER BY lochierarchy DESC,
         CASE
             WHEN lochierarchy = 0 THEN i_category
         END,
         rank_within_parent
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN89
-- =================================================================
WITH store_customers AS
  (SELECT DISTINCT c_last_name,
                   c_first_name,
                   d_date
   FROM store_sales
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
   WHERE d_month_seq BETWEEN 1202 AND 1213 ),
     catalog_customers AS
  (SELECT DISTINCT c_last_name,
                   c_first_name,
                   d_date
   FROM catalog_sales
   JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
   JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
   WHERE d_month_seq BETWEEN 1202 AND 1213 ),
     web_customers AS
  (SELECT DISTINCT c_last_name,
                   c_first_name,
                   d_date
   FROM web_sales
   JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
   JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
   WHERE d_month_seq BETWEEN 1202 AND 1213 )
SELECT COUNT(*)
FROM
  (SELECT *
   FROM store_customers
   EXCEPT SELECT *
   FROM catalog_customers
   EXCEPT SELECT *
   FROM web_customers) AS cool_cust ;

-- =================================================================
-- Query ID: TPCDSN90
-- =================================================================
SELECT SUM(CASE
               WHEN t_hour = 8
                    AND t_minute >= 30 THEN 1
               ELSE 0
           END) AS h8_30_to_9,
       SUM(CASE
               WHEN t_hour = 9
                    AND t_minute < 30 THEN 1
               ELSE 0
           END) AS h9_to_9_30,
       SUM(CASE
               WHEN t_hour = 9
                    AND t_minute >= 30 THEN 1
               ELSE 0
           END) AS h9_30_to_10,
       SUM(CASE
               WHEN t_hour = 10
                    AND t_minute < 30 THEN 1
               ELSE 0
           END) AS h10_to_10_30,
       SUM(CASE
               WHEN t_hour = 10
                    AND t_minute >= 30 THEN 1
               ELSE 0
           END) AS h10_30_to_11,
       SUM(CASE
               WHEN t_hour = 11
                    AND t_minute < 30 THEN 1
               ELSE 0
           END) AS h11_to_11_30,
       SUM(CASE
               WHEN t_hour = 11
                    AND t_minute >= 30 THEN 1
               ELSE 0
           END) AS h11_30_to_12,
       SUM(CASE
               WHEN t_hour = 12
                    AND t_minute < 30 THEN 1
               ELSE 0
           END) AS h12_to_12_30
FROM store_sales
JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
JOIN time_dim ON ss_sold_time_sk = t_time_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE ((hd_dep_count = 0
        AND hd_vehicle_count <= 2)
       OR (hd_dep_count = -1
           AND hd_vehicle_count <= 1)
       OR (hd_dep_count = 3
           AND hd_vehicle_count <= 5))
  AND s_store_name = 'ese' ;

-- =================================================================
-- Query ID: TPCDSN91
-- =================================================================
WITH sales_data AS
  (SELECT i_category,
          i_class,
          i_brand,
          s_store_name,
          s_company_name,
          d_moy,
          SUM(ss_sales_price) AS sum_sales
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_year = 2001
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('history',
                           'school-uniforms',
                           'audio'))
          OR (i_category IN ('Men',
                             'Sports',
                             'Shoes')
              AND i_class IN ('pants',
                              'tennis',
                              'womens')))
   GROUP BY i_category,
            i_class,
            i_brand,
            s_store_name,
            s_company_name,
            d_moy),
     avg_sales_data AS
  (SELECT i_category,
          i_brand,
          s_store_name,
          s_company_name,
          AVG(sum_sales) AS avg_monthly_sales
   FROM sales_data
   GROUP BY i_category,
            i_brand,
            s_store_name,
            s_company_name)
SELECT sd.i_category,
       sd.i_class,
       sd.i_brand,
       sd.s_store_name,
       sd.s_company_name,
       sd.d_moy,
       sd.sum_sales,
       asd.avg_monthly_sales
FROM sales_data sd
JOIN avg_sales_data asd ON sd.i_category = asd.i_category
AND sd.i_brand = asd.i_brand
AND sd.s_store_name = asd.s_store_name
AND sd.s_company_name = asd.s_company_name
WHERE CASE
          WHEN (asd.avg_monthly_sales <> 0) THEN (ABS(sd.sum_sales - asd.avg_monthly_sales) / asd.avg_monthly_sales)
          ELSE NULL
      END > 0.1
ORDER BY sd.sum_sales - asd.avg_monthly_sales,
         sd.s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN92
-- =================================================================
WITH amc_cte AS
  (SELECT COUNT(*) AS amc
   FROM web_sales
   JOIN household_demographics ON ws_ship_hdemo_sk = hd_demo_sk
   JOIN time_dim ON ws_sold_time_sk = t_time_sk
   JOIN web_page ON ws_web_page_sk = wp_web_page_sk
   WHERE t_hour = 12
     AND hd_dep_count = 6
     AND wp_char_count BETWEEN 5000 AND 5200 ),
     pmc_cte AS
  (SELECT COUNT(*) AS pmc
   FROM web_sales
   JOIN household_demographics ON ws_ship_hdemo_sk = hd_demo_sk
   JOIN time_dim ON ws_sold_time_sk = t_time_sk
   JOIN web_page ON ws_web_page_sk = wp_web_page_sk
   WHERE t_hour = 14
     AND hd_dep_count = 6
     AND wp_char_count BETWEEN 5000 AND 5200 )
SELECT CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio
FROM amc_cte,
     pmc_cte
ORDER BY am_pm_ratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN93
-- =================================================================
SELECT cc.cc_call_center_id AS Call_Center,
       cc.cc_name AS Call_Center_Name,
       cc.cc_manager AS Manager,
       SUM(cr.cr_net_loss) AS Returns_Loss
FROM call_center cc
JOIN catalog_returns cr ON cr.cr_call_center_sk = cc.cc_call_center_sk
JOIN date_dim dd ON cr.cr_returned_date_sk = dd.d_date_sk
JOIN customer c ON cr.cr_returning_customer_sk = c.c_customer_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
JOIN household_demographics hd ON hd.hd_demo_sk = c.c_current_hdemo_sk
JOIN customer_address ca ON ca.ca_address_sk = c.c_current_addr_sk
WHERE dd.d_year = 2000
  AND dd.d_moy = 12
  AND ((cd.cd_marital_status = 'M'
        AND cd.cd_education_status = 'Advanced Degree ')
       OR (cd.cd_marital_status = 'W'
           AND cd.cd_education_status = 'Unnknown'))
  AND hd.hd_buy_potential LIKE 'Unknown%'
  AND ca.ca_gmt_offset = -7
GROUP BY cc.cc_call_center_id,
         cc.cc_name,
         cc.cc_manager
ORDER BY Returns_Loss DESC ;

-- =================================================================
-- Query ID: TPCDSN94
-- =================================================================
WITH avg_discount AS
  (SELECT 1.3 * avg(ws_ext_discount_amt) AS threshold
   FROM web_sales
   JOIN date_dim ON d_date_sk = ws_sold_date_sk
   WHERE d_date BETWEEN '2000-02-01' AND '2000-05-01'
     AND ws_item_sk IN
       (SELECT i_item_sk
        FROM item
        WHERE i_manufact_id = 393) )
SELECT sum(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales
JOIN item ON i_item_sk = ws_item_sk
JOIN date_dim ON d_date_sk = ws_sold_date_sk
WHERE i_manufact_id = 393
  AND d_date BETWEEN '2000-02-01' AND '2000-05-01'
  AND ws_ext_discount_amt >
    (SELECT threshold
     FROM avg_discount)
GROUP BY i_item_sk
ORDER BY "Excess Discount Amount" DESC
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN95
-- =================================================================
SELECT ss_customer_sk,
       SUM(COALESCE((ss_quantity - sr_return_quantity) * ss_sales_price, ss_quantity * ss_sales_price)) AS sumsales
FROM store_sales
LEFT JOIN store_returns ON sr_item_sk = ss_item_sk
AND sr_ticket_number = ss_ticket_number
JOIN reason ON sr_reason_sk = r_reason_sk
WHERE r_reason_desc = 'Package was damaged'
GROUP BY ss_customer_sk
ORDER BY sumsales,
         ss_customer_sk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN96
-- =================================================================
WITH valid_orders AS
  (SELECT ws_order_number
   FROM web_sales ws1
   WHERE EXISTS
       (SELECT 1
        FROM web_sales ws2
        WHERE ws1.ws_order_number = ws2.ws_order_number
          AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk )
     AND NOT EXISTS
       (SELECT 1
        FROM web_returns wr1
        WHERE ws1.ws_order_number = wr1.wr_order_number ) ),
     filtered_sales AS
  (SELECT ws_order_number,
          ws_ext_ship_cost,
          ws_net_profit
   FROM web_sales ws
   JOIN date_dim ON ws.ws_ship_date_sk = d_date_sk
   JOIN customer_address ON ws.ws_ship_addr_sk = ca_address_sk
   JOIN web_site ON ws.ws_web_site_sk = web_site_sk
   WHERE d_date BETWEEN '2002-5-01' AND (CAST('2002-5-01' AS DATE) + INTERVAL '60' DAY)
     AND ca_state = 'OK'
     AND web_company_name = 'pri'
     AND ws_order_number IN
       (SELECT ws_order_number
        FROM valid_orders) )
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws_net_profit) AS "total net profit"
FROM filtered_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN97
-- =================================================================
WITH ws_wh AS
  (SELECT ws1.ws_order_number
   FROM web_sales ws1
   JOIN web_sales ws2 ON ws1.ws_order_number = ws2.ws_order_number
   WHERE ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk ),
     ws_orders AS
  (SELECT wr_order_number
   FROM web_returns
   WHERE wr_order_number IN
       (SELECT ws_order_number
        FROM ws_wh) )
SELECT COUNT(DISTINCT ws1.ws_order_number) AS "order count",
       SUM(ws1.ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws1.ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_dim ON ws1.ws_ship_date_sk = d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
WHERE d_date BETWEEN '2001-04-01' AND (CAST('2001-04-01' AS DATE) + INTERVAL '60' DAY)
  AND ca_state = 'VA'
  AND web_company_name = 'pri'
  AND ws1.ws_order_number IN
    (SELECT ws_order_number
     FROM ws_wh)
  AND ws1.ws_order_number IN
    (SELECT wr_order_number
     FROM ws_orders)
GROUP BY ws1.ws_order_number
ORDER BY "order count" DESC
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN98
-- =================================================================
SELECT COUNT(*)
FROM store_sales
JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk
JOIN household_demographics ON ss_hdemo_sk = household_demographics.hd_demo_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE time_dim.t_hour = 8
  AND time_dim.t_minute >= 30
  AND household_demographics.hd_dep_count = 0
  AND store.s_store_name = 'ese' ;

-- =================================================================
-- Query ID: TPCDSN99
-- =================================================================
with ssci as
  (select ss_customer_sk as customer_sk,
          ss_item_sk as item_sk
   from store_sales
   join date_dim on ss_sold_date_sk = d_date_sk
   where d_month_seq between 1199 and 1210
   group by ss_customer_sk,
            ss_item_sk),
     csci as
  (select cs_bill_customer_sk as customer_sk,
          cs_item_sk as item_sk
   from catalog_sales
   join date_dim on cs_sold_date_sk = d_date_sk
   where d_month_seq between 1199 and 1210
   group by cs_bill_customer_sk,
            cs_item_sk)
select sum(case
               when ssci.customer_sk is not null
                    and csci.customer_sk is null then 1
               else 0
           end) as store_only,
       sum(case
               when ssci.customer_sk is null
                    and csci.customer_sk is not null then 1
               else 0
           end) as catalog_only,
       sum(case
               when ssci.customer_sk is not null
                    and csci.customer_sk is not null then 1
               else 0
           end) as store_and_catalog
from ssci
full outer join csci on ssci.customer_sk = csci.customer_sk
and ssci.item_sk = csci.item_sk
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN100
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ss_ext_sales_price) AS itemrevenue,
       SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM store_sales
JOIN item ON ss_item_sk = i_item_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE i_category IN ('Men',
                     'Sports',
                     'Jewelry')
  AND d_date BETWEEN '1999-02-05' AND '1999-03-07'
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio ;

-- =================================================================
-- Query ID: TPCDSN101
-- =================================================================
SELECT substr(w.w_warehouse_name, 1, 20),
       sm.sm_type,
       cc.cc_name,
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS "30 days",
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 30)
                    AND (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS "31-60 days",
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 60)
                    AND (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS "61-90 days",
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 90)
                    AND (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS "91-120 days",
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS ">120 days"
FROM catalog_sales cs
JOIN warehouse w ON cs.cs_warehouse_sk = w.w_warehouse_sk
JOIN ship_mode sm ON cs.cs_ship_mode_sk = sm.sm_ship_mode_sk
JOIN call_center cc ON cs.cs_call_center_sk = cc.cc_call_center_sk
JOIN date_dim d ON cs.cs_ship_date_sk = d.d_date_sk
WHERE d.d_month_seq BETWEEN 1194 AND 1205
GROUP BY substr(w.w_warehouse_name, 1, 20),
         sm.sm_type,
         cc.cc_name
ORDER BY substr(w.w_warehouse_name, 1, 20),
         sm.sm_type,
         cc.cc_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN102
-- =================================================================
WITH customer_total_return AS
  (SELECT sr_customer_sk AS ctr_customer_sk,
          sr_store_sk AS ctr_store_sk,
          SUM(sr_fee) AS ctr_total_return
   FROM store_returns
   JOIN date_dim ON sr_returned_date_sk = d_date_sk
   WHERE d_year = 2000
   GROUP BY sr_customer_sk,
            sr_store_sk),
     average_return AS
  (SELECT ctr_store_sk,
          AVG(ctr_total_return) * 1.2 AS threshold
   FROM customer_total_return
   GROUP BY ctr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1
JOIN store ON s_store_sk = ctr1.ctr_store_sk
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN average_return ar ON ctr1.ctr_store_sk = ar.ctr_store_sk
WHERE ctr1.ctr_total_return > ar.threshold
  AND s_state = 'TN'
ORDER BY c_customer_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN103
-- =================================================================
WITH wscs AS
  (SELECT ws_sold_date_sk AS sold_date_sk,
          ws_ext_sales_price AS sales_price
   FROM web_sales
   UNION ALL SELECT cs_sold_date_sk AS sold_date_sk,
                    cs_ext_sales_price AS sales_price
   FROM catalog_sales),
     wswscs AS
  (SELECT d_week_seq,
          SUM(CASE
                  WHEN d_day_name = 'Sunday' THEN sales_price
                  ELSE 0
              END) AS sun_sales,
          SUM(CASE
                  WHEN d_day_name = 'Monday' THEN sales_price
                  ELSE 0
              END) AS mon_sales,
          SUM(CASE
                  WHEN d_day_name = 'Tuesday' THEN sales_price
                  ELSE 0
              END) AS tue_sales,
          SUM(CASE
                  WHEN d_day_name = 'Wednesday' THEN sales_price
                  ELSE 0
              END) AS wed_sales,
          SUM(CASE
                  WHEN d_day_name = 'Thursday' THEN sales_price
                  ELSE 0
              END) AS thu_sales,
          SUM(CASE
                  WHEN d_day_name = 'Friday' THEN sales_price
                  ELSE 0
              END) AS fri_sales,
          SUM(CASE
                  WHEN d_day_name = 'Saturday' THEN sales_price
                  ELSE 0
              END) AS sat_sales
   FROM wscs
   JOIN date_dim ON d_date_sk = sold_date_sk
   GROUP BY d_week_seq),
     year_sales AS
  (SELECT wswscs.d_week_seq,
          sun_sales,
          mon_sales,
          tue_sales,
          wed_sales,
          thu_sales,
          fri_sales,
          sat_sales,
          d_year
   FROM wswscs
   JOIN date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
   WHERE d_year IN (1998,
                    1999) )
SELECT y.d_week_seq AS d_week_seq1,
       ROUND(y.sun_sales::NUMERIC / NULLIF(z.sun_sales, 0), 2) AS sun_sales_ratio,
       ROUND(y.mon_sales::NUMERIC / NULLIF(z.mon_sales, 0), 2) AS mon_sales_ratio,
       ROUND(y.tue_sales::NUMERIC / NULLIF(z.tue_sales, 0), 2) AS tue_sales_ratio,
       ROUND(y.wed_sales::NUMERIC / NULLIF(z.wed_sales, 0), 2) AS wed_sales_ratio,
       ROUND(y.thu_sales::NUMERIC / NULLIF(z.thu_sales, 0), 2) AS thu_sales_ratio,
       ROUND(y.fri_sales::NUMERIC / NULLIF(z.fri_sales, 0), 2) AS fri_sales_ratio,
       ROUND(y.sat_sales::NUMERIC / NULLIF(z.sat_sales, 0), 2) AS sat_sales_ratio
FROM year_sales y
JOIN year_sales z ON y.d_week_seq = z.d_week_seq - 53
AND y.d_year = 1998
AND z.d_year = 1999
ORDER BY y.d_week_seq ;

-- =================================================================
-- Query ID: TPCDSN104
-- =================================================================
SELECT dt.d_year,
       item.i_brand_id AS brand_id,
       item.i_brand AS brand,
       SUM(ss.ss_sales_price) AS sum_agg
FROM date_dim dt
JOIN store_sales ss ON dt.d_date_sk = ss.ss_sold_date_sk
JOIN item ON ss.ss_item_sk = item.i_item_sk
WHERE item.i_manufact_id = 816
  AND dt.d_moy = 11
GROUP BY dt.d_year,
         item.i_brand_id,
         item.i_brand
ORDER BY dt.d_year,
         sum_agg DESC,
         brand_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN105
-- =================================================================
WITH year_total AS
  (SELECT c_customer_id AS customer_id,
          c_first_name AS customer_first_name,
          c_last_name AS customer_last_name,
          c_preferred_cust_flag AS customer_preferred_cust_flag,
          c_birth_country AS customer_birth_country,
          c_login AS customer_login,
          c_email_address AS customer_email_address,
          d_year AS dyear,
          SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total,
          's' AS sale_type
   FROM customer
   JOIN store_sales ON c_customer_sk = ss_customer_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   UNION ALL SELECT c_customer_id AS customer_id,
                    c_first_name AS customer_first_name,
                    c_last_name AS customer_last_name,
                    c_preferred_cust_flag AS customer_preferred_cust_flag,
                    c_birth_country AS customer_birth_country,
                    c_login AS customer_login,
                    c_email_address AS customer_email_address,
                    d_year AS dyear,
                    SUM(((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2) AS year_total,
                    'c' AS sale_type
   FROM customer
   JOIN catalog_sales ON c_customer_sk = cs_bill_customer_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   UNION ALL SELECT c_customer_id AS customer_id,
                    c_first_name AS customer_first_name,
                    c_last_name AS customer_last_name,
                    c_preferred_cust_flag AS customer_preferred_cust_flag,
                    c_birth_country AS customer_birth_country,
                    c_login AS customer_login,
                    c_email_address AS customer_email_address,
                    d_year AS dyear,
                    SUM(((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2) AS year_total,
                    'w' AS sale_type
   FROM customer
   JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_birth_country
FROM year_total t_s_firstyear
JOIN year_total t_s_secyear ON t_s_secyear.customer_id = t_s_firstyear.customer_id
AND t_s_secyear.sale_type = 's'
AND t_s_secyear.dyear = 1999
JOIN year_total t_c_firstyear ON t_c_firstyear.customer_id = t_s_firstyear.customer_id
AND t_c_firstyear.sale_type = 'c'
AND t_c_firstyear.dyear = 1999
JOIN year_total t_c_secyear ON t_c_secyear.customer_id = t_s_firstyear.customer_id
AND t_c_secyear.sale_type = 'c'
AND t_c_secyear.dyear = 1999
JOIN year_total t_w_firstyear ON t_w_firstyear.customer_id = t_s_firstyear.customer_id
AND t_w_firstyear.sale_type = 'w'
AND t_w_firstyear.dyear = 1999
JOIN year_total t_w_secyear ON t_w_secyear.customer_id = t_s_firstyear.customer_id
AND t_w_secyear.sale_type = 'w'
AND t_w_secyear.dyear = 1999
WHERE t_s_firstyear.sale_type = 's'
  AND t_s_firstyear.dyear = 1999
  AND t_s_firstyear.year_total > 0
  AND t_c_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND COALESCE(t_c_secyear.year_total / NULLIF(t_c_firstyear.year_total, 0), 0) >= COALESCE(t_s_secyear.year_total / NULLIF(t_s_firstyear.year_total, 0), 0)
  AND COALESCE(t_c_secyear.year_total / NULLIF(t_c_firstyear.year_total, 0), 0) >= COALESCE(t_w_secyear.year_total / NULLIF(t_w_firstyear.year_total, 0), 0)
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_birth_country
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN106
-- =================================================================
WITH salesreturns AS
  (SELECT ss_store_sk AS store_sk,
          ss_sold_date_sk AS date_sk,
          ss_ext_sales_price AS sales_price,
          ss_net_profit AS profit,
          CAST(0 AS DECIMAL(7, 2)) AS return_amt,
          CAST(0 AS DECIMAL(7, 2)) AS net_loss
   FROM store_sales
   UNION ALL SELECT sr_store_sk AS store_sk,
                    sr_returned_date_sk AS date_sk,
                    CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                    CAST(0 AS DECIMAL(7, 2)) AS profit,
                    sr_return_amt AS return_amt,
                    sr_net_loss AS net_loss
   FROM store_returns),
     catalog_salesreturns AS
  (SELECT cs_catalog_page_sk AS page_sk,
          cs_sold_date_sk AS date_sk,
          cs_ext_sales_price AS sales_price,
          cs_net_profit AS profit,
          CAST(0 AS DECIMAL(7, 2)) AS return_amt,
          CAST(0 AS DECIMAL(7, 2)) AS net_loss
   FROM catalog_sales
   UNION ALL SELECT cr_catalog_page_sk AS page_sk,
                    cr_returned_date_sk AS date_sk,
                    CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                    CAST(0 AS DECIMAL(7, 2)) AS profit,
                    cr_return_amount AS return_amt,
                    cr_net_loss AS net_loss
   FROM catalog_returns),
     web_salesreturns AS
  (SELECT ws_web_site_sk AS wsr_web_site_sk,
          ws_sold_date_sk AS date_sk,
          ws_ext_sales_price AS sales_price,
          ws_net_profit AS profit,
          CAST(0 AS DECIMAL(7, 2)) AS return_amt,
          CAST(0 AS DECIMAL(7, 2)) AS net_loss
   FROM web_sales
   UNION ALL SELECT ws_web_site_sk AS wsr_web_site_sk,
                    wr_returned_date_sk AS date_sk,
                    CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                    CAST(0 AS DECIMAL(7, 2)) AS profit,
                    wr_return_amt AS return_amt,
                    wr_net_loss AS net_loss
   FROM web_returns
   LEFT JOIN web_sales ON (wr_item_sk = ws_item_sk
                           AND wr_order_number = ws_order_number)),
     ssr AS
  (SELECT s_store_id,
          SUM(sales_price) AS sales,
          SUM(profit) AS profit,
          SUM(return_amt) AS returns,
          SUM(net_loss) AS profit_loss
   FROM salesreturns
   JOIN date_dim ON date_sk = d_date_sk
   JOIN store ON store_sk = s_store_sk
   WHERE d_date BETWEEN '2000-08-19'::DATE AND '2000-09-02'::DATE
   GROUP BY s_store_id),
     csr AS
  (SELECT cp_catalog_page_id,
          SUM(sales_price) AS sales,
          SUM(profit) AS profit,
          SUM(return_amt) AS returns,
          SUM(net_loss) AS profit_loss
   FROM catalog_salesreturns
   JOIN date_dim ON date_sk = d_date_sk
   JOIN catalog_page ON page_sk = cp_catalog_page_sk
   WHERE d_date BETWEEN '2000-08-19'::DATE AND '2000-09-02'::DATE
   GROUP BY cp_catalog_page_id),
     wsr AS
  (SELECT web_site_id,
          SUM(sales_price) AS sales,
          SUM(profit) AS profit,
          SUM(return_amt) AS returns,
          SUM(net_loss) AS profit_loss
   FROM web_salesreturns
   JOIN date_dim ON date_sk = d_date_sk
   JOIN web_site ON wsr_web_site_sk = web_site_sk
   WHERE d_date BETWEEN '2000-08-19'::DATE AND '2000-09-02'::DATE
   GROUP BY web_site_id)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns) AS returns,
       SUM(profit) AS profit
FROM
  (SELECT 'store channel' AS channel,
          'store' || s_store_id AS id,
          sales,
          returns,
          (profit - profit_loss) AS profit
   FROM ssr
   UNION ALL SELECT 'catalog channel' AS channel,
                    'catalog_page' || cp_catalog_page_id AS id,
                    sales,
                    returns,
                    (profit - profit_loss) AS profit
   FROM csr
   UNION ALL SELECT 'web channel' AS channel,
                    'web_site' || web_site_id AS id,
                    sales,
                    returns,
                    (profit - profit_loss) AS profit
   FROM wsr) x
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel,
         id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN107
-- =================================================================
SELECT a.ca_state AS state,
       COUNT(*) AS cnt
FROM customer_address a
JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
JOIN item i ON s.ss_item_sk = i.i_item_sk
WHERE d.d_month_seq =
    (SELECT d_month_seq
     FROM date_dim
     WHERE d_year = 2002
       AND d_moy = 3
     LIMIT 1)
  AND i.i_current_price > 1.2 *
    (SELECT AVG(j.i_current_price)
     FROM item j
     WHERE j.i_category = i.i_category )
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt,
         a.ca_state
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN108
-- =================================================================
SELECT i.i_item_id,
       AVG(ss.ss_quantity) AS agg1,
       AVG(ss.ss_list_price) AS agg2,
       AVG(ss.ss_coupon_amt) AS agg3,
       AVG(ss.ss_sales_price) AS agg4
FROM store_sales ss
JOIN customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN item i ON ss.ss_item_sk = i.i_item_sk
JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
WHERE cd.cd_gender = 'F'
  AND cd.cd_marital_status = 'W'
  AND cd.cd_education_status = 'College'
  AND (p.p_channel_email = 'N'
       OR p.p_channel_event = 'N')
  AND d.d_year = 2001
GROUP BY i.i_item_id
ORDER BY i.i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN109
-- =================================================================
WITH valid_zips AS
  (SELECT DISTINCT substr(ca_zip, 1, 5) AS ca_zip
   FROM customer_address
   WHERE substr(ca_zip, 1, 5) IN ('47602',
                                  '16704',
                                  '35863',
                                  '28577',
                                  '83910',
                                  '36201',
                                  '58412',
                                  '48162',
                                  '28055',
                                  '41419',
                                  '80332',
                                  '38607',
                                  '77817',
                                  '24891',
                                  '16226',
                                  '18410',
                                  '21231',
                                  '59345',
                                  '13918',
                                  '51089',
                                  '20317',
                                  '17167',
                                  '54585',
                                  '67881',
                                  '78366',
                                  '47770',
                                  '18360',
                                  '51717',
                                  '73108',
                                  '14440',
                                  '21800',
                                  '89338',
                                  '45859',
                                  '65501',
                                  '34948',
                                  '25973',
                                  '73219',
                                  '25333',
                                  '17291',
                                  '10374',
                                  '18829',
                                  '60736',
                                  '82620',
                                  '41351',
                                  '52094',
                                  '19326',
                                  '25214',
                                  '54207',
                                  '40936',
                                  '21814',
                                  '79077',
                                  '25178',
                                  '75742',
                                  '77454',
                                  '30621',
                                  '89193',
                                  '27369',
                                  '41232',
                                  '48567',
                                  '83041',
                                  '71948',
                                  '37119',
                                  '68341',
                                  '14073',
                                  '16891',
                                  '62878',
                                  '49130',
                                  '19833',
                                  '24286',
                                  '27700',
                                  '40979',
                                  '50412',
                                  '81504',
                                  '94835',
                                  '84844',
                                  '71954',
                                  '39503',
                                  '57649',
                                  '18434',
                                  '24987',
                                  '12350',
                                  '86379',
                                  '27413',
                                  '44529',
                                  '98569',
                                  '16515',
                                  '27287',
                                  '24255',
                                  '21094',
                                  '16005',
                                  '56436',
                                  '91110',
                                  '68293',
                                  '56455',
                                  '54558',
                                  '10298',
                                  '83647',
                                  '32754',
                                  '27052',
                                  '51766',
                                  '19444',
                                  '13869',
                                  '45645',
                                  '94791',
                                  '57631',
                                  '20712',
                                  '37788',
                                  '41807',
                                  '46507',
                                  '21727',
                                  '71836',
                                  '81070',
                                  '50632',
                                  '88086',
                                  '63991',
                                  '20244',
                                  '31655',
                                  '51782',
                                  '29818',
                                  '63792',
                                  '68605',
                                  '94898',
                                  '36430',
                                  '57025',
                                  '20601',
                                  '82080',
                                  '33869',
                                  '22728',
                                  '35834',
                                  '29086',
                                  '92645',
                                  '98584',
                                  '98072',
                                  '11652',
                                  '78093',
                                  '57553',
                                  '43830',
                                  '71144',
                                  '53565',
                                  '18700',
                                  '90209',
                                  '71256',
                                  '38353',
                                  '54364',
                                  '28571',
                                  '96560',
                                  '57839',
                                  '56355',
                                  '50679',
                                  '45266',
                                  '84680',
                                  '34306',
                                  '34972',
                                  '48530',
                                  '30106',
                                  '15371',
                                  '92380',
                                  '84247',
                                  '92292',
                                  '68852',
                                  '13338',
                                  '34594',
                                  '82602',
                                  '70073',
                                  '98069',
                                  '85066',
                                  '47289',
                                  '11686',
                                  '98862',
                                  '26217',
                                  '47529',
                                  '63294',
                                  '51793',
                                  '35926',
                                  '24227',
                                  '14196',
                                  '24594',
                                  '32489',
                                  '99060',
                                  '49472',
                                  '43432',
                                  '49211',
                                  '14312',
                                  '88137',
                                  '47369',
                                  '56877',
                                  '20534',
                                  '81755',
                                  '15794',
                                  '12318',
                                  '21060',
                                  '73134',
                                  '41255',
                                  '63073',
                                  '81003',
                                  '73873',
                                  '66057',
                                  '51184',
                                  '51195',
                                  '45676',
                                  '92696',
                                  '70450',
                                  '90669',
                                  '98338',
                                  '25264',
                                  '38919',
                                  '59226',
                                  '58581',
                                  '60298',
                                  '17895',
                                  '19489',
                                  '52301',
                                  '80846',
                                  '95464',
                                  '68770',
                                  '51634',
                                  '19988',
                                  '18367',
                                  '18421',
                                  '11618',
                                  '67975',
                                  '25494',
                                  '41352',
                                  '95430',
                                  '15734',
                                  '62585',
                                  '97173',
                                  '33773',
                                  '10425',
                                  '75675',
                                  '53535',
                                  '17879',
                                  '41967',
                                  '12197',
                                  '67998',
                                  '79658',
                                  '59130',
                                  '72592',
                                  '14851',
                                  '43933',
                                  '68101',
                                  '50636',
                                  '25717',
                                  '71286',
                                  '24660',
                                  '58058',
                                  '72991',
                                  '95042',
                                  '15543',
                                  '33122',
                                  '69280',
                                  '11912',
                                  '59386',
                                  '27642',
                                  '65177',
                                  '17672',
                                  '33467',
                                  '64592',
                                  '36335',
                                  '54010',
                                  '18767',
                                  '63193',
                                  '42361',
                                  '49254',
                                  '33113',
                                  '33159',
                                  '36479',
                                  '59080',
                                  '11855',
                                  '81963',
                                  '31016',
                                  '49140',
                                  '29392',
                                  '41836',
                                  '32958',
                                  '53163',
                                  '13844',
                                  '73146',
                                  '23952',
                                  '65148',
                                  '93498',
                                  '14530',
                                  '46131',
                                  '58454',
                                  '13376',
                                  '13378',
                                  '83986',
                                  '12320',
                                  '17193',
                                  '59852',
                                  '46081',
                                  '98533',
                                  '52389',
                                  '13086',
                                  '68843',
                                  '31013',
                                  '13261',
                                  '60560',
                                  '13443',
                                  '45533',
                                  '83583',
                                  '11489',
                                  '58218',
                                  '19753',
                                  '22911',
                                  '25115',
                                  '86709',
                                  '27156',
                                  '32669',
                                  '13123',
                                  '51933',
                                  '39214',
                                  '41331',
                                  '66943',
                                  '14155',
                                  '69998',
                                  '49101',
                                  '70070',
                                  '35076',
                                  '14242',
                                  '73021',
                                  '59494',
                                  '15782',
                                  '29752',
                                  '37914',
                                  '74686',
                                  '83086',
                                  '34473',
                                  '15751',
                                  '81084',
                                  '49230',
                                  '91894',
                                  '60624',
                                  '17819',
                                  '28810',
                                  '63180',
                                  '56224',
                                  '39459',
                                  '55233',
                                  '75752',
                                  '43639',
                                  '55349',
                                  '86057',
                                  '62361',
                                  '50788',
                                  '31830',
                                  '58062',
                                  '18218',
                                  '85761',
                                  '60083',
                                  '45484',
                                  '21204',
                                  '90229',
                                  '70041',
                                  '41162',
                                  '35390',
                                  '16364',
                                  '39500',
                                  '68908',
                                  '26689',
                                  '52868',
                                  '81335',
                                  '40146',
                                  '11340',
                                  '61527',
                                  '61794',
                                  '71997',
                                  '30415',
                                  '59004',
                                  '29450',
                                  '58117',
                                  '69952',
                                  '33562',
                                  '83833',
                                  '27385',
                                  '61860',
                                  '96435',
                                  '48333',
                                  '23065',
                                  '32961',
                                  '84919',
                                  '61997',
                                  '99132',
                                  '22815',
                                  '56600',
                                  '68730',
                                  '48017',
                                  '95694',
                                  '32919',
                                  '88217',
                                  '27116',
                                  '28239',
                                  '58032',
                                  '18884',
                                  '16791',
                                  '21343',
                                  '97462',
                                  '18569',
                                  '75660',
                                  '15475') ),
     preferred_zips AS
  (SELECT substr(ca_zip, 1, 5) AS ca_zip
   FROM customer_address
   JOIN customer ON ca_address_sk = c_current_addr_sk
   WHERE c_preferred_cust_flag = 'Y'
   GROUP BY substr(ca_zip, 1, 5)
   HAVING COUNT(*) > 10)
SELECT s_store_name,
       SUM(ss_net_profit) AS total_net_profit
FROM store_sales
JOIN store ON ss_store_sk = s_store_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN valid_zips V1 ON substr(s_zip, 1, 1) = substr(V1.ca_zip, 1, 1)
WHERE d_qoy = 2
  AND d_year = 1998
  AND EXISTS
    (SELECT 1
     FROM preferred_zips
     WHERE ca_zip = V1.ca_zip )
GROUP BY s_store_name
ORDER BY s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN110
-- =================================================================
WITH sales_summary AS
  (SELECT ss_quantity,
          COUNT(*) AS cnt,
          AVG(ss_ext_tax) AS avg_ext_tax,
          AVG(ss_net_paid_inc_tax) AS avg_net_paid_inc_tax
   FROM store_sales
   GROUP BY ss_quantity),
     bucket_conditions AS
  (SELECT SUM(CASE
                  WHEN ss_quantity BETWEEN 1 AND 20 THEN cnt
                  ELSE 0
              END) AS cnt1,
          SUM(CASE
                  WHEN ss_quantity BETWEEN 21 AND 40 THEN cnt
                  ELSE 0
              END) AS cnt2,
          SUM(CASE
                  WHEN ss_quantity BETWEEN 41 AND 60 THEN cnt
                  ELSE 0
              END) AS cnt3,
          SUM(CASE
                  WHEN ss_quantity BETWEEN 61 AND 80 THEN cnt
                  ELSE 0
              END) AS cnt4,
          SUM(CASE
                  WHEN ss_quantity BETWEEN 81 AND 100 THEN cnt
                  ELSE 0
              END) AS cnt5,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 1 AND 20 THEN avg_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax1,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 21 AND 40 THEN avg_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax2,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 41 AND 60 THEN avg_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax3,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 61 AND 80 THEN avg_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax4,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 81 AND 100 THEN avg_ext_tax
                  ELSE NULL
              END) AS avg_ext_tax5,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 1 AND 20 THEN avg_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid_inc_tax1,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 21 AND 40 THEN avg_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid_inc_tax2,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 41 AND 60 THEN avg_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid_inc_tax3,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 61 AND 80 THEN avg_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid_inc_tax4,
          AVG(CASE
                  WHEN ss_quantity BETWEEN 81 AND 100 THEN avg_net_paid_inc_tax
                  ELSE NULL
              END) AS avg_net_paid_inc_tax5
   FROM sales_summary)
SELECT CASE
           WHEN cnt1 > 1071 THEN avg_ext_tax1
           ELSE avg_net_paid_inc_tax1
       END AS bucket1,
       CASE
           WHEN cnt2 > 39161 THEN avg_ext_tax2
           ELSE avg_net_paid_inc_tax2
       END AS bucket2,
       CASE
           WHEN cnt3 > 29434 THEN avg_ext_tax3
           ELSE avg_net_paid_inc_tax3
       END AS bucket3,
       CASE
           WHEN cnt4 > 6568 THEN avg_ext_tax4
           ELSE avg_net_paid_inc_tax4
       END AS bucket4,
       CASE
           WHEN cnt5 > 21216 THEN avg_ext_tax5
           ELSE avg_net_paid_inc_tax5
       END AS bucket5
FROM bucket_conditions
JOIN reason ON r_reason_sk = 1 ;

-- =================================================================
-- Query ID: TPCDSN111
-- =================================================================
WITH sales_data AS
  (SELECT c.c_customer_sk
   FROM store_sales ss
   JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
   JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
   WHERE d.d_year = 1999
     AND d.d_moy BETWEEN 1 AND 12
   UNION SELECT c.c_customer_sk
   FROM web_sales ws
   JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
   JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
   WHERE d.d_year = 1999
     AND d.d_moy BETWEEN 1 AND 12
   UNION SELECT c.c_customer_sk
   FROM catalog_sales cs
   JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
   JOIN customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
   WHERE d.d_year = 1999
     AND d.d_moy BETWEEN 1 AND 12 )
SELECT cd.cd_gender,
       cd.cd_marital_status,
       cd.cd_education_status,
       COUNT(*) AS cnt1,
       cd.cd_purchase_estimate,
       COUNT(*) AS cnt2,
       cd.cd_credit_rating,
       COUNT(*) AS cnt3,
       cd.cd_dep_count,
       COUNT(*) AS cnt4,
       cd.cd_dep_employed_count,
       COUNT(*) AS cnt5,
       cd.cd_dep_college_count,
       COUNT(*) AS cnt6
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE ca.ca_county IN ('Fairfield County',
                       'Campbell County',
                       'Washtenaw County',
                       'Escambia County',
                       'Cleburne County',
                       'United States',
                       '1')
  AND c.c_customer_sk IN
    (SELECT c_customer_sk
     FROM sales_data)
GROUP BY cd.cd_gender,
         cd.cd_marital_status,
         cd.cd_education_status,
         cd.cd_purchase_estimate,
         cd.cd_credit_rating,
         cd.cd_dep_count,
         cd.cd_dep_employed_count,
         cd.cd_dep_college_count
ORDER BY cd.cd_gender,
         cd.cd_marital_status,
         cd.cd_education_status,
         cd.cd_purchase_estimate,
         cd.cd_credit_rating,
         cd.cd_dep_count,
         cd.cd_dep_employed_count,
         cd.cd_dep_college_count
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN112
-- =================================================================
WITH year_total AS
  (SELECT c.c_customer_id AS customer_id,
          c.c_first_name AS customer_first_name,
          c.c_last_name AS customer_last_name,
          c.c_preferred_cust_flag AS customer_preferred_cust_flag,
          c.c_birth_country AS customer_birth_country,
          c.c_login AS customer_login,
          c.c_email_address AS customer_email_address,
          d.d_year AS dyear,
          SUM(ss.ss_ext_list_price - ss.ss_ext_discount_amt) AS year_total,
          's' AS sale_type
   FROM customer c
   JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
   JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
   GROUP BY c.c_customer_id,
            c.c_first_name,
            c.c_last_name,
            c.c_preferred_cust_flag,
            c.c_birth_country,
            c.c_login,
            c.c_email_address,
            d.d_year
   UNION ALL SELECT c.c_customer_id AS customer_id,
                    c.c_first_name AS customer_first_name,
                    c.c_last_name AS customer_last_name,
                    c.c_preferred_cust_flag AS customer_preferred_cust_flag,
                    c.c_birth_country AS customer_birth_country,
                    c.c_login AS customer_login,
                    c.c_email_address AS customer_email_address,
                    d.d_year AS dyear,
                    SUM(ws.ws_ext_list_price - ws.ws_ext_discount_amt) AS year_total,
                    'w' AS sale_type
   FROM customer c
   JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
   JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
   GROUP BY c.c_customer_id,
            c.c_first_name,
            c.c_last_name,
            c.c_preferred_cust_flag,
            c.c_birth_country,
            c.c_login,
            c.c_email_address,
            d.d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_email_address
FROM year_total t_s_firstyear
JOIN year_total t_s_secyear ON t_s_secyear.customer_id = t_s_firstyear.customer_id
JOIN year_total t_w_firstyear ON t_s_firstyear.customer_id = t_w_firstyear.customer_id
JOIN year_total t_w_secyear ON t_s_firstyear.customer_id = t_w_secyear.customer_id
WHERE t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 1999
  AND t_s_secyear.dyear = 1999
  AND t_w_firstyear.dyear = 1999
  AND t_w_secyear.dyear = 1999
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND COALESCE(t_w_secyear.year_total / NULLIF(t_w_firstyear.year_total, 0), 0.0) >= COALESCE(t_s_secyear.year_total / NULLIF(t_s_firstyear.year_total, 0), 0.0)
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_email_address
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN113
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ws_ext_sales_price) AS itemrevenue,
       SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
WHERE i_category IN ('Men',
                     'Books',
                     'Electronics')
  AND d_date BETWEEN '2001-06-15' AND '2001-07-15'
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN114
-- =================================================================
SELECT AVG(ss_quantity),
       AVG(ss_ext_sales_price),
       AVG(ss_ext_wholesale_cost),
       SUM(ss_ext_wholesale_cost)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
WHERE d_year = 2001
  AND ca_country = 'United States'
  AND ((cd_marital_status = 'M'
        AND cd_education_status = 'College'
        AND ss_sales_price BETWEEN 100.00 AND 150.00
        AND hd_dep_count = 3)
       OR (cd_marital_status = 'D'
           AND cd_education_status = 'Primary'
           AND ss_sales_price BETWEEN 50.00 AND 100.00
           AND hd_dep_count = 1)
       OR (cd_marital_status = 'W'
           AND cd_education_status = '2 yr Degree'
           AND ss_sales_price BETWEEN 150.00 AND 200.00
           AND hd_dep_count = 1))
  AND ((ca_state IN ('IL',
                     'TN',
                     'TX')
        AND ss_net_profit BETWEEN 100 AND 200)
       OR (ca_state IN ('WY',
                        'OH',
                        'ID')
           AND ss_net_profit BETWEEN 150 AND 300)
       OR (ca_state IN ('MS',
                        'SC',
                        'IA')
           AND ss_net_profit BETWEEN 50 AND 250)) ;

-- =================================================================
-- Query ID: TPCDSN115
-- =================================================================
select *
from customer ;

-- =================================================================
-- Query ID: TPCDSN116
-- =================================================================
SELECT ca_zip,
       SUM(cs_sales_price)
FROM catalog_sales
JOIN customer ON cs_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE (LEFT(ca_zip, 5) IN ('85669',
                           '86197',
                           '88274',
                           '83405',
                           '86475',
                           '85392',
                           '85460',
                           '80348',
                           '81792')
       OR ca_state IN ('CA',
                       'WA',
                       'GA')
       OR cs_sales_price > 500)
  AND d_qoy = 2
  AND d_year = 2001
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN117
-- =================================================================
SELECT COUNT(DISTINCT cs_order_number) AS "order count",
       SUM(cs_ext_ship_cost) AS "total shipping cost",
       SUM(cs_net_profit) AS "total net profit"
FROM catalog_sales cs1
JOIN date_dim ON cs1.cs_ship_date_sk = d_date_sk
JOIN customer_address ON cs1.cs_ship_addr_sk = ca_address_sk
JOIN call_center ON cs1.cs_call_center_sk = cc_call_center_sk
WHERE d_date BETWEEN '2002-04-01' AND '2002-06-01'
  AND ca_state = 'PA'
  AND cc_county = 'Williamson County'
  AND EXISTS
    (SELECT 1
     FROM catalog_sales cs2
     WHERE cs1.cs_order_number = cs2.cs_order_number
       AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk )
  AND NOT EXISTS
    (SELECT 1
     FROM catalog_returns cr1
     WHERE cs1.cs_order_number = cr1.cr_order_number )
GROUP BY cs1.cs_order_number
ORDER BY "order count"
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN118
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       s_state,
       COUNT(ss_quantity) AS store_sales_quantitycount,
       AVG(ss_quantity) AS store_sales_quantityave,
       STDDEV_SAMP(ss_quantity) AS store_sales_quantitystdev,
       STDDEV_SAMP(ss_quantity) / NULLIF(AVG(ss_quantity), 0) AS store_sales_quantitycov,
       COUNT(sr_return_quantity) AS store_returns_quantitycount,
       AVG(sr_return_quantity) AS store_returns_quantityave,
       STDDEV_SAMP(sr_return_quantity) AS store_returns_quantitystdev,
       STDDEV_SAMP(sr_return_quantity) / NULLIF(AVG(sr_return_quantity), 0) AS store_returns_quantitycov,
       COUNT(cs_quantity) AS catalog_sales_quantitycount,
       AVG(cs_quantity) AS catalog_sales_quantityave,
       STDDEV_SAMP(cs_quantity) AS catalog_sales_quantitystdev,
       STDDEV_SAMP(cs_quantity) / NULLIF(AVG(cs_quantity), 0) AS catalog_sales_quantitycov
FROM store_sales
JOIN store_returns ON ss_customer_sk = sr_customer_sk
AND ss_item_sk = sr_item_sk
AND ss_ticket_number = sr_ticket_number
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
AND sr_item_sk = cs_item_sk
JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN item ON i_item_sk = ss_item_sk
WHERE d1.d_quarter_name = '2001Q1'
  AND d2.d_quarter_name IN ('2001Q1',
                            '2001Q2',
                            '2001Q3')
  AND d3.d_quarter_name IN ('2001Q1',
                            '2001Q2',
                            '2001Q3')
GROUP BY i_item_id,
         i_item_desc,
         s_state
ORDER BY i_item_id,
         i_item_desc,
         s_state
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN119
-- =================================================================
SELECT i_item_id,
       ca_country,
       ca_state,
       ca_county,
       AVG(cs_quantity::decimal(12, 2)) AS agg1,
       AVG(cs_list_price::decimal(12, 2)) AS agg2,
       AVG(cs_coupon_amt::decimal(12, 2)) AS agg3,
       AVG(cs_sales_price::decimal(12, 2)) AS agg4,
       AVG(cs_net_profit::decimal(12, 2)) AS agg5,
       AVG(c_birth_year::decimal(12, 2)) AS agg6,
       AVG(cd1.cd_dep_count::decimal(12, 2)) AS agg7
FROM catalog_sales
JOIN customer_demographics cd1 ON cs_bill_cdemo_sk = cd1.cd_demo_sk
JOIN customer ON cs_bill_customer_sk = c_customer_sk
JOIN customer_demographics cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN item ON cs_item_sk = i_item_sk
WHERE cd1.cd_gender = 'F'
  AND cd1.cd_education_status = 'Primary'
  AND c_birth_month IN (1,
                        3,
                        7,
                        11,
                        10,
                        4)
  AND d_year = 2001
  AND ca_state IN ('AL',
                   'MO',
                   'TN',
                   'GA',
                   'MT',
                   'IN',
                   'CA')
GROUP BY ROLLUP (i_item_id,
                 ca_country,
                 ca_state,
                 ca_county)
ORDER BY ca_country,
         ca_state,
         ca_county,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN120
-- =================================================================
SELECT i_brand_id AS brand_id,
       i_brand AS brand,
       i_manufact_id,
       i_manufact,
       SUM(ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN date_dim ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE i_manager_id = 14
  AND d_moy = 11
  AND d_year = 2002
  AND LEFT(ca_zip, 5) <> LEFT(s_zip, 5)
GROUP BY i_brand_id,
         i_brand,
         i_manufact_id,
         i_manufact
ORDER BY ext_price DESC,
         i_brand,
         i_brand_id,
         i_manufact_id,
         i_manufact
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN121
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(cs_ext_sales_price) AS itemrevenue,
       SUM(cs_ext_sales_price) * 100.0 / SUM(SUM(cs_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM catalog_sales
JOIN item ON cs_item_sk = i_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE i_category IN ('Books',
                     'Music',
                     'Sports')
  AND d_date BETWEEN '2002-06-18' AND '2002-07-18'
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN122
-- =================================================================
SELECT w_warehouse_name,
       i_item_id,
       inv_before,
       inv_after
FROM
  (SELECT w_warehouse_name,
          i_item_id,
          SUM(CASE
                  WHEN d_date < '1999-06-22' THEN inv_quantity_on_hand
                  ELSE 0
              END) AS inv_before,
          SUM(CASE
                  WHEN d_date >= '1999-06-22' THEN inv_quantity_on_hand
                  ELSE 0
              END) AS inv_after
   FROM inventory
   JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
   JOIN item ON i_item_sk = inv_item_sk
   JOIN date_dim ON inv_date_sk = d_date_sk
   WHERE i_current_price BETWEEN 0.99 AND 1.49
     AND d_date BETWEEN '1999-05-23' AND '1999-07-22'
   GROUP BY w_warehouse_name,
            i_item_id) AS subquery
WHERE COALESCE(NULLIF(inv_before, 0), NULL) IS NOT NULL
  AND inv_after / inv_before BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
ORDER BY w_warehouse_name,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN123
-- =================================================================
SELECT i_product_name,
       i_brand,
       i_class,
       i_category,
       AVG(inv_quantity_on_hand) AS qoh
FROM inventory
JOIN date_dim ON inv_date_sk = d_date_sk
JOIN item ON inv_item_sk = i_item_sk
WHERE d_month_seq BETWEEN 1200 AND 1211
GROUP BY ROLLUP(i_product_name, i_brand, i_class, i_category)
ORDER BY qoh,
         i_product_name,
         i_brand,
         i_class,
         i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN124
-- =================================================================
select *
from customer ;

-- =================================================================
-- Query ID: TPCDSN125
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'khaki'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN126
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'seashell'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN127
-- =================================================================
SELECT i.i_item_id,
       i.i_item_desc,
       s.s_store_id,
       s.s_store_name,
       MAX(ss.ss_net_profit) AS store_sales_profit,
       MAX(sr.sr_net_loss) AS store_returns_loss,
       MAX(cs.cs_net_profit) AS catalog_sales_profit
FROM store_sales ss
JOIN item i ON i.i_item_sk = ss.ss_item_sk
JOIN store s ON s.s_store_sk = ss.ss_store_sk
JOIN date_dim d1 ON d1.d_date_sk = ss.ss_sold_date_sk
LEFT JOIN store_returns sr ON ss.ss_customer_sk = sr.sr_customer_sk
AND ss.ss_item_sk = sr.sr_item_sk
AND ss.ss_ticket_number = sr.sr_ticket_number
LEFT JOIN date_dim d2 ON sr.sr_returned_date_sk = d2.d_date_sk
LEFT JOIN catalog_sales cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk
AND sr.sr_item_sk = cs.cs_item_sk
LEFT JOIN date_dim d3 ON cs.cs_sold_date_sk = d3.d_date_sk
WHERE d1.d_moy = 4
  AND d1.d_year = 1999
  AND (d2.d_moy BETWEEN 4 AND 10
       AND d2.d_year = 1999
       OR d2.d_date_sk IS NULL)
  AND (d3.d_moy BETWEEN 4 AND 10
       AND d3.d_year = 1999
       OR d3.d_date_sk IS NULL)
GROUP BY i.i_item_id,
         i.i_item_desc,
         s.s_store_id,
         s.s_store_name
ORDER BY i.i_item_id,
         i.i_item_desc,
         s.s_store_id,
         s.s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN128
-- =================================================================
SELECT i.i_item_id,
       AVG(cs.cs_quantity) AS agg1,
       AVG(cs.cs_list_price) AS agg2,
       AVG(cs.cs_coupon_amt) AS agg3,
       AVG(cs.cs_sales_price) AS agg4
FROM catalog_sales cs
JOIN customer_demographics cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
JOIN item i ON cs.cs_item_sk = i.i_item_sk
JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk
WHERE cd.cd_gender = 'M'
  AND cd.cd_marital_status = 'W'
  AND cd.cd_education_status = 'Unknown'
  AND (p.p_channel_email = 'N'
       OR p.p_channel_event = 'N')
  AND d.d_year = 2002
GROUP BY i.i_item_id
ORDER BY i.i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN129
-- =================================================================
SELECT i_item_id,
       s_state,
       GROUPING(s_state) AS g_state,
       AVG(ss_quantity) AS agg1,
       AVG(ss_list_price) AS agg2,
       AVG(ss_coupon_amt) AS agg3,
       AVG(ss_sales_price) AS agg4
FROM store_sales
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE cd_gender = 'M'
  AND cd_marital_status = 'W'
  AND cd_education_status = 'Secondary'
  AND d_year = 1999
  AND s_state = 'TN'
GROUP BY ROLLUP (i_item_id,
                 s_state)
ORDER BY i_item_id,
         s_state
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN130
-- =================================================================
WITH B1 AS
  (SELECT avg(ss_list_price) AS B1_LP,
          count(ss_list_price) AS B1_CNT,
          count(distinct ss_list_price) AS B1_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 0 AND 5
     AND (ss_list_price BETWEEN 107 AND 117
          OR ss_coupon_amt BETWEEN 1319 AND 2319
          OR ss_wholesale_cost BETWEEN 60 AND 80) ),
     B2 AS
  (SELECT avg(ss_list_price) AS B2_LP,
          count(ss_list_price) AS B2_CNT,
          count(distinct ss_list_price) AS B2_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 6 AND 10
     AND (ss_list_price BETWEEN 23 AND 33
          OR ss_coupon_amt BETWEEN 825 AND 1825
          OR ss_wholesale_cost BETWEEN 43 AND 63) ),
     B3 AS
  (SELECT avg(ss_list_price) AS B3_LP,
          count(ss_list_price) AS B3_CNT,
          count(distinct ss_list_price) AS B3_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 11 AND 15
     AND (ss_list_price BETWEEN 74 AND 84
          OR ss_coupon_amt BETWEEN 4381 AND 5381
          OR ss_wholesale_cost BETWEEN 57 AND 77) ),
     B4 AS
  (SELECT avg(ss_list_price) AS B4_LP,
          count(ss_list_price) AS B4_CNT,
          count(distinct ss_list_price) AS B4_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 16 AND 20
     AND (ss_list_price BETWEEN 89 AND 99
          OR ss_coupon_amt BETWEEN 3117 AND 4117
          OR ss_wholesale_cost BETWEEN 68 AND 88) ),
     B5 AS
  (SELECT avg(ss_list_price) AS B5_LP,
          count(ss_list_price) AS B5_CNT,
          count(distinct ss_list_price) AS B5_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 21 AND 25
     AND (ss_list_price BETWEEN 58 AND 68
          OR ss_coupon_amt BETWEEN 9402 AND 10402
          OR ss_wholesale_cost BETWEEN 38 AND 58) ),
     B6 AS
  (SELECT avg(ss_list_price) AS B6_LP,
          count(ss_list_price) AS B6_CNT,
          count(distinct ss_list_price) AS B6_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 26 AND 30
     AND (ss_list_price BETWEEN 64 AND 74
          OR ss_coupon_amt BETWEEN 5792 AND 6792
          OR ss_wholesale_cost BETWEEN 73 AND 93) )
SELECT *
FROM B1,
     B2,
     B3,
     B4,
     B5,
     B6
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN131
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       MAX(ss_quantity) AS store_sales_quantity,
       MAX(sr_return_quantity) AS store_returns_quantity,
       MAX(cs_quantity) AS catalog_sales_quantity
FROM store_sales
JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
JOIN item ON i_item_sk = ss_item_sk
JOIN store ON s_store_sk = ss_store_sk
LEFT JOIN store_returns ON ss_customer_sk = sr_customer_sk
AND ss_item_sk = sr_item_sk
AND ss_ticket_number = sr_ticket_number
LEFT JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
LEFT JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
AND sr_item_sk = cs_item_sk
LEFT JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
WHERE d1.d_moy = 4
  AND d1.d_year = 1998
  AND (d2.d_moy BETWEEN 4 AND 7
       OR d2.d_moy IS NULL)
  AND (d2.d_year = 1998
       OR d2.d_year IS NULL)
  AND (d3.d_year IN (1998,
                     1999,
                     2000)
       OR d3.d_year IS NULL)
GROUP BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
ORDER BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN132
-- =================================================================
WITH customer_total_return AS
  (SELECT wr_returning_customer_sk AS ctr_customer_sk,
          ca_state AS ctr_state,
          SUM(wr_return_amt) AS ctr_total_return
   FROM web_returns
   JOIN date_dim ON wr_returned_date_sk = d_date_sk
   JOIN customer_address ON wr_returning_addr_sk = ca_address_sk
   WHERE d_year = 2000
   GROUP BY wr_returning_customer_sk,
            ca_state),
     state_avg_return AS
  (SELECT ctr_state,
          AVG(ctr_total_return) * 1.2 AS avg_return_threshold
   FROM customer_total_return
   GROUP BY ctr_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       c_preferred_cust_flag,
       c_birth_day,
       c_birth_month,
       c_birth_year,
       c_birth_country,
       c_login,
       c_email_address,
       c_last_review_date,
       ctr1.ctr_total_return
FROM customer_total_return ctr1
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
JOIN state_avg_return sar ON ctr1.ctr_state = sar.ctr_state
WHERE ctr1.ctr_total_return > sar.avg_return_threshold
  AND ca_state = 'IN'
ORDER BY c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         c_preferred_cust_flag,
         c_birth_day,
         c_birth_month,
         c_birth_year,
         c_birth_country,
         c_login,
         c_email_address,
         c_last_review_date,
         ctr_total_return
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN133
-- =================================================================
WITH ss AS
  (SELECT ca_county,
          d_qoy,
          d_year,
          SUM(ss_ext_sales_price) AS store_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer_address ON ss_addr_sk = ca_address_sk
   GROUP BY ca_county,
            d_qoy,
            d_year),
     ws AS
  (SELECT ca_county,
          d_qoy,
          d_year,
          SUM(ws_ext_sales_price) AS web_sales
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
   GROUP BY ca_county,
            d_qoy,
            d_year)
SELECT ss1.ca_county,
       ss1.d_year,
       ws2.web_sales / NULLIF(ws1.web_sales, 0) AS web_q1_q2_increase,
       ss2.store_sales / NULLIF(ss1.store_sales, 0) AS store_q1_q2_increase,
       ws3.web_sales / NULLIF(ws2.web_sales, 0) AS web_q2_q3_increase,
       ss3.store_sales / NULLIF(ss2.store_sales, 0) AS store_q2_q3_increase
FROM ss ss1
JOIN ss ss2 ON ss1.ca_county = ss2.ca_county
AND ss2.d_qoy = 2
AND ss2.d_year = 1999
JOIN ss ss3 ON ss2.ca_county = ss3.ca_county
AND ss3.d_qoy = 3
AND ss3.d_year = 1999
JOIN ws ws1 ON ss1.ca_county = ws1.ca_county
AND ws1.d_qoy = 1
AND ws1.d_year = 1999
JOIN ws ws2 ON ws1.ca_county = ws2.ca_county
AND ws2.d_qoy = 2
AND ws2.d_year = 1999
JOIN ws ws3 ON ws2.ca_county = ws3.ca_county
AND ws3.d_qoy = 3
AND ws3.d_year = 1999
WHERE ss1.d_qoy = 1
  AND ss1.d_year = 1999
  AND COALESCE(ws2.web_sales / NULLIF(ws1.web_sales, 0), 0) > COALESCE(ss2.store_sales / NULLIF(ss1.store_sales, 0), 0)
  AND COALESCE(ws3.web_sales / NULLIF(ws2.web_sales, 0), 0) > COALESCE(ss3.store_sales / NULLIF(ss2.store_sales, 0), 0)
ORDER BY store_q2_q3_increase ;

-- =================================================================
-- Query ID: TPCDSN134
-- =================================================================
WITH avg_discount AS
  (SELECT 1.3 * AVG(cs_ext_discount_amt) AS threshold
   FROM catalog_sales
   JOIN date_dim ON d_date_sk = cs_sold_date_sk
   WHERE d_date BETWEEN '2001-03-09' AND '2001-06-07'
     AND cs_item_sk =
       (SELECT i_item_sk
        FROM item
        WHERE i_manufact_id = 722
        LIMIT 1) )
SELECT SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales
JOIN item ON i_item_sk = cs_item_sk
JOIN date_dim ON d_date_sk = cs_sold_date_sk
WHERE i_manufact_id = 722
  AND d_date BETWEEN '2001-03-09' AND '2001-06-07'
  AND cs_ext_discount_amt >
    (SELECT threshold
     FROM avg_discount)
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN135
-- =================================================================
WITH filtered_items AS
  (SELECT i_item_sk,
          i_manufact_id
   FROM item
   WHERE i_category = 'Books' ),
     filtered_dates AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_year = 2001
     AND d_moy = 3 ),
     filtered_addresses AS
  (SELECT ca_address_sk
   FROM customer_address
   WHERE ca_gmt_offset = -5 ),
     ss AS
  (SELECT i.i_manufact_id,
          SUM(ss.ss_ext_sales_price) AS total_sales
   FROM store_sales ss
   JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
   JOIN filtered_addresses ca ON ss.ss_addr_sk = ca.ca_address_sk
   JOIN filtered_items i ON ss.ss_item_sk = i.i_item_sk
   GROUP BY i.i_manufact_id),
     cs AS
  (SELECT i.i_manufact_id,
          SUM(cs.cs_ext_sales_price) AS total_sales
   FROM catalog_sales cs
   JOIN filtered_dates d ON cs.cs_sold_date_sk = d.d_date_sk
   JOIN filtered_addresses ca ON cs.cs_bill_addr_sk = ca.ca_address_sk
   JOIN filtered_items i ON cs.cs_item_sk = i.i_item_sk
   GROUP BY i.i_manufact_id),
     ws AS
  (SELECT i.i_manufact_id,
          SUM(ws.ws_ext_sales_price) AS total_sales
   FROM web_sales ws
   JOIN filtered_dates d ON ws.ws_sold_date_sk = d.d_date_sk
   JOIN filtered_addresses ca ON ws.ws_bill_addr_sk = ca.ca_address_sk
   JOIN filtered_items i ON ws.ws_item_sk = i.i_item_sk
   GROUP BY i.i_manufact_id)
SELECT i_manufact_id,
       SUM(total_sales) AS total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) AS combined_sales
GROUP BY i_manufact_id
ORDER BY total_sales DESC
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN136
-- =================================================================
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          COUNT(*) AS cnt
   FROM store_sales
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   JOIN store ON store_sales.ss_store_sk = store.s_store_sk
   JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
   WHERE (date_dim.d_dom BETWEEN 1 AND 3
          OR date_dim.d_dom BETWEEN 25 AND 28)
     AND household_demographics.hd_buy_potential IN ('1001-5000',
                                                     '0-500')
     AND household_demographics.hd_vehicle_count > 0
     AND (household_demographics.hd_dep_count / NULLIF(household_demographics.hd_vehicle_count, 0)) > 1.2
     AND date_dim.d_year BETWEEN 2000 AND 2002
     AND store.s_county = 'Williamson County'
   GROUP BY ss_ticket_number,
            ss_customer_sk) AS dn
JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
WHERE cnt BETWEEN 15 AND 20
ORDER BY c_last_name,
         c_first_name,
         c_salutation,
         c_preferred_cust_flag DESC,
         ss_ticket_number ;

-- =================================================================
-- Query ID: TPCDSN137
-- =================================================================
WITH sales_1999 AS
  (SELECT ss_customer_sk AS customer_sk
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_year = 1999
     AND d_qoy < 4
   UNION SELECT ws_bill_customer_sk AS customer_sk
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE d_year = 1999
     AND d_qoy < 4
   UNION SELECT cs_ship_customer_sk AS customer_sk
   FROM catalog_sales
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE d_year = 1999
     AND d_qoy < 4 )
SELECT ca_state,
       cd_gender,
       cd_marital_status,
       cd_dep_count,
       COUNT(*) AS cnt1,
       AVG(cd_dep_count) AS avg_dep_count,
       STDDEV_SAMP(cd_dep_count) AS stddev_dep_count,
       SUM(cd_dep_count) AS sum_dep_count,
       cd_dep_employed_count,
       COUNT(*) AS cnt2,
       AVG(cd_dep_employed_count) AS avg_dep_employed_count,
       STDDEV_SAMP(cd_dep_employed_count) AS stddev_dep_employed_count,
       SUM(cd_dep_employed_count) AS sum_dep_employed_count,
       cd_dep_college_count,
       COUNT(*) AS cnt3,
       AVG(cd_dep_college_count) AS avg_dep_college_count,
       STDDEV_SAMP(cd_dep_college_count) AS stddev_dep_college_count,
       SUM(cd_dep_college_count) AS sum_dep_college_count
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd_demo_sk = c.c_current_cdemo_sk
WHERE c.c_customer_sk IN
    (SELECT customer_sk
     FROM sales_1999)
GROUP BY ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
ORDER BY ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN138
-- =================================================================
select *
from customer ;

-- =================================================================
-- Query ID: TPCDSN139
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
JOIN catalog_sales ON cs_item_sk = i_item_sk
WHERE i_current_price BETWEEN 29 AND 59
  AND d_date BETWEEN DATE '2002-03-29' AND DATE '2002-05-28'
  AND i_manufact_id IN (393,
                        174,
                        251,
                        445)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN140
-- =================================================================
SELECT COUNT(*)
FROM
  (SELECT c_last_name,
          c_first_name,
          d_date
   FROM
     (SELECT c_last_name,
             c_first_name,
             d_date
      FROM store_sales
      JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1189 AND 1200
      UNION SELECT c_last_name,
                   c_first_name,
                   d_date
      FROM catalog_sales
      JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1189 AND 1200
      UNION SELECT c_last_name,
                   c_first_name,
                   d_date
      FROM web_sales
      JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1189 AND 1200 ) AS combined_sales
   GROUP BY c_last_name,
            c_first_name,
            d_date
   HAVING COUNT(*) = 3) AS hot_cust
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN141
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN142
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
  and inv1.cov > 1.5
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN143
-- =================================================================
SELECT w_state,
       i_item_id,
       SUM(CASE
               WHEN d_date < '2001-05-02' THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
               ELSE 0
           END) AS sales_before,
       SUM(CASE
               WHEN d_date >= '2001-05-02' THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
               ELSE 0
           END) AS sales_after
FROM catalog_sales
LEFT JOIN catalog_returns ON cs_order_number = cr_order_number
AND cs_item_sk = cr_item_sk
JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
JOIN item ON i_item_sk = cs_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE i_current_price BETWEEN 0.99 AND 1.49
  AND d_date BETWEEN '2001-04-02' AND '2001-06-01'
GROUP BY w_state,
         i_item_id
ORDER BY w_state,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN144
-- =================================================================
SELECT DISTINCT i_product_name
FROM item i1
WHERE i_manufact_id BETWEEN 704 AND 744
  AND EXISTS
    (SELECT 1
     FROM item
     WHERE i_manufact = i1.i_manufact
       AND ((i_category = 'Women'
             AND i_color IN ('forest',
                             'lime')
             AND i_units IN ('Pallet',
                             'Pound')
             AND i_size IN ('economy',
                            'small'))
            OR (i_category = 'Women'
                AND i_color IN ('navy',
                                'slate')
                AND i_units IN ('Gross',
                                'Bunch')
                AND i_size IN ('extra large',
                               'petite'))
            OR (i_category = 'Men'
                AND i_color IN ('powder',
                                'sky')
                AND i_units IN ('Dozen',
                                'Lb')
                AND i_size IN ('N/A',
                               'large'))
            OR (i_category = 'Men'
                AND i_color IN ('maroon',
                                'smoke')
                AND i_units IN ('Ounce',
                                'Case')
                AND i_size IN ('economy',
                               'small'))
            OR (i_category = 'Women'
                AND i_color IN ('dark',
                                'aquamarine')
                AND i_units IN ('Ton',
                                'Tbl')
                AND i_size IN ('economy',
                               'small'))
            OR (i_category = 'Women'
                AND i_color IN ('frosted',
                                'plum')
                AND i_units IN ('Dram',
                                'Box')
                AND i_size IN ('extra large',
                               'petite'))
            OR (i_category = 'Men'
                AND i_color IN ('papaya',
                                'peach')
                AND i_units IN ('Bundle',
                                'Carton')
                AND i_size IN ('N/A',
                               'large'))
            OR (i_category = 'Men'
                AND i_color IN ('firebrick',
                                'sienna')
                AND i_units IN ('Cup',
                                'Each')
                AND i_size IN ('economy',
                               'small'))) )
ORDER BY i_product_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN145
-- =================================================================
SELECT dt.d_year,
       item.i_category_id,
       item.i_category,
       SUM(ss.ss_ext_sales_price) AS total_sales
FROM store_sales ss
JOIN date_dim dt ON dt.d_date_sk = ss.ss_sold_date_sk
JOIN item ON ss.ss_item_sk = item.i_item_sk
WHERE item.i_manager_id = 1
  AND dt.d_moy = 11
  AND dt.d_year = 1998
GROUP BY dt.d_year,
         item.i_category_id,
         item.i_category
ORDER BY total_sales DESC,
         dt.d_year,
         item.i_category_id,
         item.i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN146
-- =================================================================
SELECT s_store_name,
       s_store_id,
       SUM(CASE
               WHEN d_day_name = 'Sunday' THEN ss_sales_price
               ELSE 0
           END) AS sun_sales,
       SUM(CASE
               WHEN d_day_name = 'Monday' THEN ss_sales_price
               ELSE 0
           END) AS mon_sales,
       SUM(CASE
               WHEN d_day_name = 'Tuesday' THEN ss_sales_price
               ELSE 0
           END) AS tue_sales,
       SUM(CASE
               WHEN d_day_name = 'Wednesday' THEN ss_sales_price
               ELSE 0
           END) AS wed_sales,
       SUM(CASE
               WHEN d_day_name = 'Thursday' THEN ss_sales_price
               ELSE 0
           END) AS thu_sales,
       SUM(CASE
               WHEN d_day_name = 'Friday' THEN ss_sales_price
               ELSE 0
           END) AS fri_sales,
       SUM(CASE
               WHEN d_day_name = 'Saturday' THEN ss_sales_price
               ELSE 0
           END) AS sat_sales
FROM store_sales
JOIN date_dim ON d_date_sk = ss_sold_date_sk
JOIN store ON s_store_sk = ss_store_sk
WHERE s_gmt_offset = -5
  AND d_year = 2000
GROUP BY s_store_name,
         s_store_id
ORDER BY s_store_name,
         s_store_id,
         sun_sales,
         mon_sales,
         tue_sales,
         wed_sales,
         thu_sales,
         fri_sales,
         sat_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN147
-- =================================================================
WITH ranked_items AS
  (SELECT ss_item_sk AS item_sk,
          avg(ss_net_profit) AS rank_col
   FROM store_sales
   WHERE ss_store_sk = 4
   GROUP BY ss_item_sk
   HAVING avg(ss_net_profit) > 0.9 *
     (SELECT avg(ss_net_profit)
      FROM store_sales
      WHERE ss_store_sk = 4
        AND ss_hdemo_sk IS NOT NULL
      GROUP BY ss_store_sk)),
     ranked_asc AS
  (SELECT item_sk,
          RANK() OVER (
                       ORDER BY rank_col ASC) AS rnk
   FROM ranked_items),
     ranked_desc AS
  (SELECT item_sk,
          RANK() OVER (
                       ORDER BY rank_col DESC) AS rnk
   FROM ranked_items)
SELECT asc_items.rnk,
       i1.i_product_name AS best_performing,
       i2.i_product_name AS worst_performing
FROM ranked_asc AS asc_items
JOIN ranked_desc AS desc_items ON asc_items.rnk = desc_items.rnk
JOIN item i1 ON i1.i_item_sk = asc_items.item_sk
JOIN item i2 ON i2.i_item_sk = desc_items.item_sk
WHERE asc_items.rnk < 11
ORDER BY asc_items.rnk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN148
-- =================================================================
SELECT ca_zip,
       ca_city,
       SUM(ws_sales_price)
FROM web_sales
JOIN customer ON ws_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN item ON ws_item_sk = i_item_sk
WHERE (LEFT(ca_zip, 5) IN ('85669',
                           '86197',
                           '88274',
                           '83405',
                           '86475',
                           '85392',
                           '85460',
                           '80348',
                           '81792')
       OR i_item_id IN
         (SELECT i_item_id
          FROM item
          WHERE i_item_sk IN (2,
                              3,
                              5,
                              7,
                              11,
                              13,
                              17,
                              19,
                              23,
                              29)))
  AND d_qoy = 1
  AND d_year = 2000
GROUP BY ca_zip,
         ca_city
ORDER BY ca_zip,
         ca_city
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN149
-- =================================================================
SELECT c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       amt,
       profit
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          ca_city AS bought_city,
          SUM(ss_coupon_amt) AS amt,
          SUM(ss_net_profit) AS profit
   FROM store_sales
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   JOIN store ON store_sales.ss_store_sk = store.s_store_sk
   JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
   JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
   WHERE (household_demographics.hd_dep_count = 8
          OR household_demographics.hd_vehicle_count = 0)
     AND date_dim.d_dow IN (0,
                            4)
     AND date_dim.d_year BETWEEN 2000 AND 2002
     AND store.s_city IN ('Midway',
                          'Fairview')
   GROUP BY ss_ticket_number,
            ss_customer_sk,
            ca_city) AS dn
JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
JOIN customer_address AS current_addr ON customer.c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> dn.bought_city
ORDER BY c_last_name,
         c_first_name,
         ca_city,
         bought_city,
         ss_ticket_number
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN150
-- =================================================================
WITH v1 AS
  (SELECT i_category,
          i_brand,
          s_store_name,
          s_company_name,
          d_year,
          d_moy,
          SUM(ss_sales_price) AS sum_sales,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_category,
                                                      i_brand,
                                                      s_store_name,
                                                      s_company_name,
                                                      d_year) AS avg_monthly_sales,
                                        LAG(SUM(ss_sales_price)) OVER (PARTITION BY i_category,
                                                                                    i_brand,
                                                                                    s_store_name,
                                                                                    s_company_name
                                                                       ORDER BY d_year,
                                                                                d_moy) AS psum,
                                                                      LEAD(SUM(ss_sales_price)) OVER (PARTITION BY i_category,
                                                                                                                   i_brand,
                                                                                                                   s_store_name,
                                                                                                                   s_company_name
                                                                                                      ORDER BY d_year,
                                                                                                               d_moy) AS nsum
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_year = 2000
     OR (d_year = 1999
         AND d_moy = 12)
     OR (d_year = 2001
         AND d_moy = 1)
   GROUP BY i_category,
            i_brand,
            s_store_name,
            s_company_name,
            d_year,
            d_moy)
SELECT s_store_name,
       s_company_name,
       d_year,
       avg_monthly_sales,
       sum_sales,
       psum,
       nsum
FROM v1
WHERE d_year = 2000
  AND avg_monthly_sales > 0
  AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         nsum
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN151
-- =================================================================
SELECT SUM(ss_quantity)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
WHERE d_year = 2001
  AND ((cd_marital_status = 'S'
        AND cd_education_status = 'Secondary'
        AND ss_sales_price BETWEEN 100.00 AND 150.00)
       OR (cd_marital_status = 'M'
           AND cd_education_status = '2 yr Degree'
           AND ss_sales_price BETWEEN 50.00 AND 100.00)
       OR (cd_marital_status = 'D'
           AND cd_education_status = 'Advanced Degree'
           AND ss_sales_price BETWEEN 150.00 AND 200.00))
  AND ca_country = 'United States'
  AND ((ca_state IN ('ND',
                     'NY',
                     'SD')
        AND ss_net_profit BETWEEN 0 AND 2000)
       OR (ca_state IN ('MD',
                        'GA',
                        'KS')
           AND ss_net_profit BETWEEN 150 AND 3000)
       OR (ca_state IN ('CO',
                        'MN',
                        'NC')
           AND ss_net_profit BETWEEN 50 AND 25000)) ;

-- =================================================================
-- Query ID: TPCDSN152
-- =================================================================
WITH ranked_web AS
  (SELECT 'web' AS channel,
          ws.ws_item_sk AS item,
          (SUM(COALESCE(wr.wr_return_quantity, 0))::decimal(15, 4) / NULLIF(SUM(COALESCE(ws.ws_quantity, 0)), 0)::decimal(15, 4)) AS return_ratio,
          (SUM(COALESCE(wr.wr_return_amt, 0))::decimal(15, 4) / NULLIF(SUM(COALESCE(ws.ws_net_paid, 0)), 0)::decimal(15, 4)) AS currency_ratio
   FROM web_sales ws
   LEFT JOIN web_returns wr ON ws.ws_order_number = wr.wr_order_number
   AND ws.ws_item_sk = wr.wr_item_sk
   JOIN date_dim ON ws.ws_sold_date_sk = d_date_sk
   WHERE wr.wr_return_amt > 10000
     AND ws.ws_net_profit > 1
     AND ws.ws_net_paid > 0
     AND ws.ws_quantity > 0
     AND d_year = 1998
     AND d_moy = 11
   GROUP BY ws.ws_item_sk),
     ranked_catalog AS
  (SELECT 'catalog' AS channel,
          cs.cs_item_sk AS item,
          (SUM(COALESCE(cr.cr_return_quantity, 0))::decimal(15, 4) / NULLIF(SUM(COALESCE(cs.cs_quantity, 0)), 0)::decimal(15, 4)) AS return_ratio,
          (SUM(COALESCE(cr.cr_return_amount, 0))::decimal(15, 4) / NULLIF(SUM(COALESCE(cs.cs_net_paid, 0)), 0)::decimal(15, 4)) AS currency_ratio
   FROM catalog_sales cs
   LEFT JOIN catalog_returns cr ON cs.cs_order_number = cr.cr_order_number
   AND cs.cs_item_sk = cr.cr_item_sk
   JOIN date_dim ON cs.cs_sold_date_sk = d_date_sk
   WHERE cr.cr_return_amount > 10000
     AND cs.cs_net_profit > 1
     AND cs.cs_net_paid > 0
     AND cs.cs_quantity > 0
     AND d_year = 1998
     AND d_moy = 11
   GROUP BY cs.cs_item_sk),
     ranked_store AS
  (SELECT 'store' AS channel,
          sts.ss_item_sk AS item,
          (SUM(COALESCE(sr.sr_return_quantity, 0))::decimal(15, 4) / NULLIF(SUM(COALESCE(sts.ss_quantity, 0)), 0)::decimal(15, 4)) AS return_ratio,
          (SUM(COALESCE(sr.sr_return_amt, 0))::decimal(15, 4) / NULLIF(SUM(COALESCE(sts.ss_net_paid, 0)), 0)::decimal(15, 4)) AS currency_ratio
   FROM store_sales sts
   LEFT JOIN store_returns sr ON sts.ss_ticket_number = sr.sr_ticket_number
   AND sts.ss_item_sk = sr.sr_item_sk
   JOIN date_dim ON sts.ss_sold_date_sk = d_date_sk
   WHERE sr.sr_return_amt > 10000
     AND sts.ss_net_profit > 1
     AND sts.ss_net_paid > 0
     AND sts.ss_quantity > 0
     AND d_year = 1998
     AND d_moy = 11
   GROUP BY sts.ss_item_sk),
     ranked_combined AS
  (SELECT channel,
          item,
          return_ratio,
          currency_ratio,
          RANK() OVER (PARTITION BY channel
                       ORDER BY return_ratio) AS return_rank,
                      RANK() OVER (PARTITION BY channel
                                   ORDER BY currency_ratio) AS currency_rank
   FROM
     (SELECT *
      FROM ranked_web
      UNION ALL SELECT *
      FROM ranked_catalog
      UNION ALL SELECT *
      FROM ranked_store) AS combined)
SELECT channel,
       item,
       return_ratio,
       return_rank,
       currency_rank
FROM ranked_combined
WHERE return_rank <= 10
  OR currency_rank <= 10
ORDER BY channel,
         return_rank,
         currency_rank,
         item
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN153
-- =================================================================
SELECT s_store_name,
       s_company_id,
       s_street_number,
       s_street_name,
       s_street_type,
       s_suite_number,
       s_city,
       s_county,
       s_state,
       s_zip,
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS "30 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 30)
                    AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS "31-60 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 60)
                    AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS "61-90 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 90)
                    AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS "91-120 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS ">120 days"
FROM store_sales
JOIN store_returns ON ss_ticket_number = sr_ticket_number
AND ss_item_sk = sr_item_sk
AND ss_customer_sk = sr_customer_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
WHERE d2.d_year = 2001
  AND d2.d_moy = 8
GROUP BY s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
ORDER BY s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN154
-- =================================================================
WITH web_v1 AS
  (SELECT ws_item_sk AS item_sk,
          d_date,
          SUM(ws_sales_price) OVER (PARTITION BY ws_item_sk
                                    ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1212 AND 1223
     AND ws_item_sk IS NOT NULL ),
     store_v1 AS
  (SELECT ss_item_sk AS item_sk,
          d_date,
          SUM(ss_sales_price) OVER (PARTITION BY ss_item_sk
                                    ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1212 AND 1223
     AND ss_item_sk IS NOT NULL )
SELECT item_sk,
       d_date,
       web_sales,
       store_sales,
       web_cumulative,
       store_cumulative
FROM
  (SELECT COALESCE(web.item_sk, store.item_sk) AS item_sk,
          COALESCE(web.d_date, store.d_date) AS d_date,
          web.cume_sales AS web_sales,
          store.cume_sales AS store_sales,
          MAX(web.cume_sales) OVER (PARTITION BY COALESCE(web.item_sk, store.item_sk)
                                    ORDER BY COALESCE(web.d_date, store.d_date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS web_cumulative,
                                   MAX(store.cume_sales) OVER (PARTITION BY COALESCE(web.item_sk, store.item_sk)
                                                               ORDER BY COALESCE(web.d_date, store.d_date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS store_cumulative
   FROM web_v1 web
   FULL OUTER JOIN store_v1 store ON web.item_sk = store.item_sk
   AND web.d_date = store.d_date) AS combined
WHERE web_cumulative > store_cumulative
ORDER BY item_sk,
         d_date
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN155
-- =================================================================
SELECT dt.d_year,
       item.i_brand_id AS brand_id,
       item.i_brand AS brand,
       SUM(ss_ext_sales_price) AS ext_price
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manager_id = 1
  AND dt.d_moy = 12
  AND dt.d_year = 2000
GROUP BY dt.d_year,
         item.i_brand_id,
         item.i_brand
ORDER BY dt.d_year,
         ext_price DESC,
         brand_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN156
-- =================================================================
WITH filtered_items AS
  (SELECT i_manufact_id,
          ss_sales_price,
          d_qoy
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_month_seq BETWEEN 1186 AND 1197
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('personal',
                           'portable',
                           'reference',
                           'self-help')
           AND i_brand IN ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9'))
          OR (i_category IN ('Women',
                             'Music',
                             'Men')
              AND i_class IN ('accessories',
                              'classical',
                              'fragrances',
                              'pants')
              AND i_brand IN ('amalgimporto #1',
                              'edu packscholar #1',
                              'exportiimporto #1',
                              'importoamalg #1'))) ),
     sales_aggregates AS
  (SELECT i_manufact_id,
          SUM(ss_sales_price) AS sum_sales,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) AS avg_quarterly_sales
   FROM filtered_items
   GROUP BY i_manufact_id,
            d_qoy)
SELECT *
FROM sales_aggregates
WHERE CASE
          WHEN avg_quarterly_sales > 0 THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
          ELSE NULL
      END > 0.1
ORDER BY avg_quarterly_sales,
         sum_sales,
         i_manufact_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN157
-- =================================================================
WITH cs_or_ws_sales AS
  (SELECT cs_sold_date_sk AS sold_date_sk,
          cs_bill_customer_sk AS customer_sk,
          cs_item_sk AS item_sk
   FROM catalog_sales
   UNION ALL SELECT ws_sold_date_sk AS sold_date_sk,
                    ws_bill_customer_sk AS customer_sk,
                    ws_item_sk AS item_sk
   FROM web_sales),
     my_customers AS
  (SELECT DISTINCT c.c_customer_sk,
                   c.c_current_addr_sk
   FROM cs_or_ws_sales
   JOIN item ON cs_or_ws_sales.item_sk = item.i_item_sk
   JOIN date_dim ON cs_or_ws_sales.sold_date_sk = date_dim.d_date_sk
   JOIN customer c ON c.c_customer_sk = cs_or_ws_sales.customer_sk
   WHERE item.i_category = 'Music'
     AND item.i_class = 'country'
     AND date_dim.d_moy = 1
     AND date_dim.d_year = 1999 ),
     my_revenue AS
  (SELECT c_customer_sk,
          SUM(ss_ext_sales_price) AS revenue
   FROM my_customers
   JOIN store_sales ON my_customers.c_customer_sk = store_sales.ss_customer_sk
   JOIN customer_address ON my_customers.c_current_addr_sk = customer_address.ca_address_sk
   JOIN store ON customer_address.ca_county = store.s_county
   AND customer_address.ca_state = store.s_state
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   WHERE date_dim.d_month_seq BETWEEN
       (SELECT DISTINCT d_month_seq + 1
        FROM date_dim
        WHERE d_year = 1999
          AND d_moy = 1 ) AND
       (SELECT DISTINCT d_month_seq + 3
        FROM date_dim
        WHERE d_year = 1999
          AND d_moy = 1 )
   GROUP BY c_customer_sk),
     segments AS
  (SELECT CAST((revenue / 50) AS INT) AS segment
   FROM my_revenue)
SELECT segment,
       COUNT(*) AS num_customers,
       segment * 50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment,
         num_customers
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN158
-- =================================================================
SELECT i_brand_id AS brand_id,
       i_brand,
       SUM(ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN date_dim ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE i_manager_id = 52
  AND d_moy = 11
  AND d_year = 2000
GROUP BY i_brand_id,
         i_brand
ORDER BY ext_price DESC,
         i_brand_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN159
-- =================================================================
WITH filtered_items AS
  (SELECT i_item_id::integer
   FROM item
   WHERE i_color IN ('powder',
                     'orchid',
                     'pink') ),
     filtered_dates AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_year = 2000
     AND d_moy = 3 ),
     filtered_addresses AS
  (SELECT ca_address_sk
   FROM customer_address
   WHERE ca_gmt_offset = -6 ),
     ss AS
  (SELECT ss_item_sk AS i_item_id,
          SUM(ss_ext_sales_price) AS total_sales
   FROM store_sales
   JOIN filtered_items ON ss_item_sk = filtered_items.i_item_id
   JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
   JOIN filtered_addresses ON ss_addr_sk = ca_address_sk
   GROUP BY ss_item_sk),
     cs AS
  (SELECT cs_item_sk AS i_item_id,
          SUM(cs_ext_sales_price) AS total_sales
   FROM catalog_sales
   JOIN filtered_items ON cs_item_sk = filtered_items.i_item_id
   JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
   JOIN filtered_addresses ON cs_bill_addr_sk = ca_address_sk
   GROUP BY cs_item_sk),
     ws AS
  (SELECT ws_item_sk AS i_item_id,
          SUM(ws_ext_sales_price) AS total_sales
   FROM web_sales
   JOIN filtered_items ON ws_item_sk = filtered_items.i_item_id
   JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
   JOIN filtered_addresses ON ws_bill_addr_sk = ca_address_sk
   GROUP BY ws_item_sk)
SELECT i_item_id,
       SUM(total_sales) AS total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) AS combined_sales
GROUP BY i_item_id
ORDER BY total_sales,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN160
-- =================================================================
WITH v1 AS
  (SELECT i_category,
          i_brand,
          cc_name,
          d_year,
          d_moy,
          SUM(cs_sales_price) AS sum_sales,
          AVG(SUM(cs_sales_price)) OVER (PARTITION BY i_category,
                                                      i_brand,
                                                      cc_name,
                                                      d_year) AS avg_monthly_sales,
                                        ROW_NUMBER() OVER (PARTITION BY i_category,
                                                                        i_brand,
                                                                        cc_name
                                                           ORDER BY d_year,
                                                                    d_moy) AS rn
   FROM item
   JOIN catalog_sales ON cs_item_sk = i_item_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN call_center ON cc_call_center_sk = cs_call_center_sk
   WHERE d_year IN (2000,
                    2001,
                    2002)
     AND (d_year != 2000
          OR d_moy = 12)
     AND (d_year != 2002
          OR d_moy = 1)
   GROUP BY i_category,
            i_brand,
            cc_name,
            d_year,
            d_moy),
     v2 AS
  (SELECT v1.i_category,
          v1.i_brand,
          v1.cc_name,
          v1.d_year,
          v1.avg_monthly_sales,
          v1.sum_sales,
          LAG(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                               v1.i_brand,
                                               v1.cc_name
                                  ORDER BY v1.rn) AS psum,
                                 LEAD(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                                                       v1.i_brand,
                                                                       v1.cc_name
                                                          ORDER BY v1.rn) AS nsum
   FROM v1)
SELECT *
FROM v2
WHERE d_year = 2001
  AND avg_monthly_sales > 0
  AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         avg_monthly_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN161
-- =================================================================
WITH week_dates AS
  (SELECT d_date
   FROM date_dim
   WHERE d_week_seq =
       (SELECT d_week_seq
        FROM date_dim
        WHERE d_date = '1998-11-19') ),
     ss_items AS
  (SELECT i_item_id AS item_id,
          SUM(ss_ext_sales_price) AS ss_item_rev
   FROM store_sales
   JOIN item ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_date IN
       (SELECT d_date
        FROM week_dates)
   GROUP BY i_item_id),
     cs_items AS
  (SELECT i_item_id AS item_id,
          SUM(cs_ext_sales_price) AS cs_item_rev
   FROM catalog_sales
   JOIN item ON cs_item_sk = i_item_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE d_date IN
       (SELECT d_date
        FROM week_dates)
   GROUP BY i_item_id),
     ws_items AS
  (SELECT i_item_id AS item_id,
          SUM(ws_ext_sales_price) AS ws_item_rev
   FROM web_sales
   JOIN item ON ws_item_sk = i_item_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE d_date IN
       (SELECT d_date
        FROM week_dates)
   GROUP BY i_item_id)
SELECT ss_items.item_id,
       ss_item_rev,
       ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev,
       cs_item_rev,
       cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev,
       ws_item_rev,
       ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev,
       (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM ss_items
JOIN cs_items ON ss_items.item_id = cs_items.item_id
JOIN ws_items ON ss_items.item_id = ws_items.item_id
WHERE ss_item_rev BETWEEN 0.1 * cs_item_rev AND 2 * cs_item_rev
  AND ss_item_rev BETWEEN 0.1 * ws_item_rev AND 2 * ws_item_rev
  AND cs_item_rev BETWEEN 0.1 * ss_item_rev AND 2 * ss_item_rev
  AND cs_item_rev BETWEEN 0.1 * ws_item_rev AND 2 * ws_item_rev
  AND ws_item_rev BETWEEN 0.1 * ss_item_rev AND 2 * ss_item_rev
  AND ws_item_rev BETWEEN 0.1 * cs_item_rev AND 2 * cs_item_rev
ORDER BY item_id,
         ss_item_rev
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN162
-- =================================================================
WITH wss AS
  (SELECT d_week_seq,
          ss_store_sk,
          SUM(CASE
                  WHEN d_day_name = 'Sunday' THEN ss_sales_price
                  ELSE 0
              END) AS sun_sales,
          SUM(CASE
                  WHEN d_day_name = 'Monday' THEN ss_sales_price
                  ELSE 0
              END) AS mon_sales,
          SUM(CASE
                  WHEN d_day_name = 'Tuesday' THEN ss_sales_price
                  ELSE 0
              END) AS tue_sales,
          SUM(CASE
                  WHEN d_day_name = 'Wednesday' THEN ss_sales_price
                  ELSE 0
              END) AS wed_sales,
          SUM(CASE
                  WHEN d_day_name = 'Thursday' THEN ss_sales_price
                  ELSE 0
              END) AS thu_sales,
          SUM(CASE
                  WHEN d_day_name = 'Friday' THEN ss_sales_price
                  ELSE 0
              END) AS fri_sales,
          SUM(CASE
                  WHEN d_day_name = 'Saturday' THEN ss_sales_price
                  ELSE 0
              END) AS sat_sales
   FROM store_sales
   JOIN date_dim ON d_date_sk = ss_sold_date_sk
   GROUP BY d_week_seq,
            ss_store_sk)
SELECT s_store_name1,
       s_store_id1,
       d_week_seq1,
       sun_sales1 / NULLIF(sun_sales2, 0) AS sun_sales_ratio,
       mon_sales1 / NULLIF(mon_sales2, 0) AS mon_sales_ratio,
       tue_sales1 / NULLIF(tue_sales2, 0) AS tue_sales_ratio,
       wed_sales1 / NULLIF(wed_sales2, 0) AS wed_sales_ratio,
       thu_sales1 / NULLIF(thu_sales2, 0) AS thu_sales_ratio,
       fri_sales1 / NULLIF(fri_sales2, 0) AS fri_sales_ratio,
       sat_sales1 / NULLIF(sat_sales2, 0) AS sat_sales_ratio
FROM
  (SELECT s_store_name AS s_store_name1,
          wss.d_week_seq AS d_week_seq1,
          s_store_id AS s_store_id1,
          sun_sales AS sun_sales1,
          mon_sales AS mon_sales1,
          tue_sales AS tue_sales1,
          wed_sales AS wed_sales1,
          thu_sales AS thu_sales1,
          fri_sales AS fri_sales1,
          sat_sales AS sat_sales1
   FROM wss
   JOIN store ON ss_store_sk = s_store_sk
   JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
   WHERE d_month_seq BETWEEN 1195 AND 1206 ) y
JOIN
  (SELECT s_store_name AS s_store_name2,
          wss.d_week_seq AS d_week_seq2,
          s_store_id AS s_store_id2,
          sun_sales AS sun_sales2,
          mon_sales AS mon_sales2,
          tue_sales AS tue_sales2,
          wed_sales AS wed_sales2,
          thu_sales AS thu_sales2,
          fri_sales AS fri_sales2,
          sat_sales AS sat_sales2
   FROM wss
   JOIN store ON ss_store_sk = s_store_sk
   JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
   WHERE d_month_seq BETWEEN 1207 AND 1218 ) x ON s_store_id1 = s_store_id2
AND d_week_seq1 = d_week_seq2 - 52
ORDER BY s_store_name1,
         s_store_id1,
         d_week_seq1
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN163
-- =================================================================
WITH filtered_items AS
  (SELECT i_item_id::integer
   FROM item
   WHERE i_category = 'Jewelry' ),
     filtered_dates AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_year = 2000
     AND d_moy = 10 ),
     filtered_addresses AS
  (SELECT ca_address_sk
   FROM customer_address
   WHERE ca_gmt_offset = -5 ),
     ss AS
  (SELECT ss_item_sk AS i_item_id,
          SUM(ss_ext_sales_price) AS total_sales
   FROM store_sales
   JOIN filtered_items ON ss_item_sk = i_item_id
   JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
   JOIN filtered_addresses ON ss_addr_sk = ca_address_sk
   GROUP BY ss_item_sk),
     cs AS
  (SELECT cs_item_sk AS i_item_id,
          SUM(cs_ext_sales_price) AS total_sales
   FROM catalog_sales
   JOIN filtered_items ON cs_item_sk = i_item_id
   JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
   JOIN filtered_addresses ON cs_bill_addr_sk = ca_address_sk
   GROUP BY cs_item_sk),
     ws AS
  (SELECT ws_item_sk AS i_item_id,
          SUM(ws_ext_sales_price) AS total_sales
   FROM web_sales
   JOIN filtered_items ON ws_item_sk = i_item_id
   JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
   JOIN filtered_addresses ON ws_bill_addr_sk = ca_address_sk
   GROUP BY ws_item_sk)
SELECT i_item_id,
       SUM(total_sales) AS total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) AS combined_sales
GROUP BY i_item_id
ORDER BY i_item_id,
         total_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN164
-- =================================================================
WITH filtered_sales AS
  (SELECT ss_ext_sales_price,
          ss_promo_sk
   FROM store_sales
   JOIN store ON ss_store_sk = s_store_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer ON ss_customer_sk = c_customer_sk
   JOIN customer_address ON ca_address_sk = c_current_addr_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE ca_gmt_offset = -5
     AND i_category = 'Home'
     AND s_gmt_offset = -5
     AND d_year = 2000
     AND d_moy = 12 ),
     promotional_sales AS
  (SELECT SUM(ss_ext_sales_price) AS promotions
   FROM filtered_sales
   JOIN promotion ON ss_promo_sk = p_promo_sk
   WHERE p_channel_dmail = 'Y'
     OR p_channel_email = 'Y'
     OR p_channel_tv = 'Y' ),
     all_sales AS
  (SELECT SUM(ss_ext_sales_price) AS total
   FROM filtered_sales)
SELECT ps.promotions,
       asales.total,
       CAST(ps.promotions AS decimal(15, 4)) / CAST(asales.total AS decimal(15, 4)) * 100 AS percentage
FROM promotional_sales ps,
     all_sales asales
ORDER BY ps.promotions,
         asales.total
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN165
-- =================================================================
SELECT substr(w.w_warehouse_name, 1, 20),
       sm.sm_type,
       wsit.web_name,
       SUM(CASE
               WHEN (ws.ws_ship_date_sk - ws.ws_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS "30 days",
       SUM(CASE
               WHEN (ws.ws_ship_date_sk - ws.ws_sold_date_sk > 30)
                    AND (ws.ws_ship_date_sk - ws.ws_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS "31-60 days",
       SUM(CASE
               WHEN (ws.ws_ship_date_sk - ws.ws_sold_date_sk > 60)
                    AND (ws.ws_ship_date_sk - ws.ws_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS "61-90 days",
       SUM(CASE
               WHEN (ws.ws_ship_date_sk - ws.ws_sold_date_sk > 90)
                    AND (ws.ws_ship_date_sk - ws.ws_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS "91-120 days",
       SUM(CASE
               WHEN (ws.ws_ship_date_sk - ws.ws_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS ">120 days"
FROM web_sales ws
JOIN warehouse w ON ws.ws_warehouse_sk = w.w_warehouse_sk
JOIN ship_mode sm ON ws.ws_ship_mode_sk = sm.sm_ship_mode_sk
JOIN web_site wsit ON ws.ws_web_site_sk = wsit.web_site_sk
JOIN date_dim dd ON ws.ws_ship_date_sk = dd.d_date_sk
WHERE dd.d_month_seq BETWEEN 1223 AND 1234
GROUP BY substr(w.w_warehouse_name, 1, 20),
         sm.sm_type,
         wsit.web_name
ORDER BY substr(w.w_warehouse_name, 1, 20),
         sm.sm_type,
         wsit.web_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN166
-- =================================================================
SELECT i_manager_id,
       sum_sales,
       avg_monthly_sales
FROM
  (SELECT i_manager_id,
          SUM(ss_sales_price) AS sum_sales,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manager_id) AS avg_monthly_sales
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_month_seq BETWEEN 1222 AND 1233
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('personal',
                           'portable',
                           'reference',
                           'self-help')
           AND i_brand IN ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9'))
          OR (i_category IN ('Women',
                             'Music',
                             'Men')
              AND i_class IN ('accessories',
                              'classical',
                              'fragrances',
                              'pants')
              AND i_brand IN ('amalgimporto #1',
                              'edu packscholar #1',
                              'exportiimporto #1',
                              'importoamalg #1')))
   GROUP BY i_manager_id,
            d_moy) AS tmp1
WHERE CASE
          WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
          ELSE NULL
      END > 0.1
ORDER BY i_manager_id,
         avg_monthly_sales,
         sum_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN167
-- =================================================================
WITH cs_ui AS
  (SELECT cs_item_sk,
          SUM(cs_ext_list_price) AS sale,
          SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
   FROM catalog_sales
   JOIN catalog_returns ON cs_item_sk = cr_item_sk
   AND cs_order_number = cr_order_number
   GROUP BY cs_item_sk
   HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)),
     cross_sales AS
  (SELECT i_product_name AS product_name,
          i_item_sk AS item_sk,
          s_store_name AS store_name,
          s_zip AS store_zip,
          ad1.ca_street_number AS b_street_number,
          ad1.ca_street_name AS b_street_name,
          ad1.ca_city AS b_city,
          ad1.ca_zip AS b_zip,
          ad2.ca_street_number AS c_street_number,
          ad2.ca_street_name AS c_street_name,
          ad2.ca_city AS c_city,
          ad2.ca_zip AS c_zip,
          d1.d_year AS syear,
          d2.d_year AS fsyear,
          d3.d_year AS s2year,
          COUNT(*) AS cnt,
          SUM(ss_wholesale_cost) AS s1,
          SUM(ss_list_price) AS s2,
          SUM(ss_coupon_amt) AS s3
   FROM store_sales
   JOIN store ON ss_store_sk = s_store_sk
   JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
   JOIN customer ON ss_customer_sk = c_customer_sk
   JOIN customer_demographics cd1 ON ss_cdemo_sk = cd1.cd_demo_sk
   JOIN household_demographics hd1 ON ss_hdemo_sk = hd1.hd_demo_sk
   JOIN customer_address ad1 ON ss_addr_sk = ad1.ca_address_sk
   JOIN item ON ss_item_sk = i_item_sk
   JOIN store_returns ON ss_item_sk = sr_item_sk
   AND ss_ticket_number = sr_ticket_number
   JOIN cs_ui ON ss_item_sk = cs_ui.cs_item_sk
   JOIN customer_demographics cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk
   JOIN household_demographics hd2 ON c_current_hdemo_sk = hd2.hd_demo_sk
   JOIN customer_address ad2 ON c_current_addr_sk = ad2.ca_address_sk
   JOIN date_dim d2 ON c_first_sales_date_sk = d2.d_date_sk
   JOIN date_dim d3 ON c_first_shipto_date_sk = d3.d_date_sk
   JOIN promotion ON ss_promo_sk = p_promo_sk
   JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
   JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
   WHERE cd1.cd_marital_status <> cd2.cd_marital_status
     AND i_color IN ('orange',
                     'lace',
                     'lawn',
                     'misty',
                     'blush',
                     'pink')
     AND i_current_price BETWEEN 48 AND 58
     AND i_current_price BETWEEN 49 AND 63
   GROUP BY i_product_name,
            i_item_sk,
            s_store_name,
            s_zip,
            ad1.ca_street_number,
            ad1.ca_street_name,
            ad1.ca_city,
            ad1.ca_zip,
            ad2.ca_street_number,
            ad2.ca_street_name,
            ad2.ca_city,
            ad2.ca_zip,
            d1.d_year,
            d2.d_year,
            d3.d_year)
SELECT cs1.product_name,
       cs1.store_name,
       cs1.store_zip,
       cs1.b_street_number,
       cs1.b_street_name,
       cs1.b_city,
       cs1.b_zip,
       cs1.c_street_number,
       cs1.c_street_name,
       cs1.c_city,
       cs1.c_zip,
       cs1.syear,
       cs1.cnt,
       cs1.s1 AS s11,
       cs1.s2 AS s21,
       cs1.s3 AS s31,
       cs2.s1 AS s12,
       cs2.s2 AS s22,
       cs2.s3 AS s32,
       cs2.syear,
       cs2.cnt
FROM cross_sales cs1
JOIN cross_sales cs2 ON cs1.item_sk = cs2.item_sk
AND cs1.syear = 1999
AND cs2.syear = 2000
AND cs2.cnt <= cs1.cnt
AND cs1.store_name = cs2.store_name
AND cs1.store_zip = cs2.store_zip
ORDER BY cs1.product_name,
         cs1.store_name,
         cs2.cnt,
         cs1.s1,
         cs2.s1 ;

-- =================================================================
-- Query ID: TPCDSN168
-- =================================================================
WITH sales_data AS
  (SELECT ss_store_sk,
          ss_item_sk,
          SUM(ss_sales_price) AS revenue
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1176 AND 1187
   GROUP BY ss_store_sk,
            ss_item_sk),
     average_revenue AS
  (SELECT ss_store_sk,
          AVG(revenue) AS ave
   FROM sales_data
   GROUP BY ss_store_sk)
SELECT s_store_name,
       i_item_desc,
       sc.revenue,
       i_current_price,
       i_wholesale_cost,
       i_brand
FROM store
JOIN sales_data sc ON s_store_sk = sc.ss_store_sk
JOIN average_revenue sb ON sb.ss_store_sk = sc.ss_store_sk
JOIN item ON i_item_sk = sc.ss_item_sk
WHERE sc.revenue <= 0.1 * sb.ave
ORDER BY s_store_name,
         i_item_desc
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN169
-- =================================================================
WITH sales_data AS
  (SELECT w_warehouse_name,
          w_warehouse_sq_ft,
          w_city,
          w_county,
          w_state,
          w_country,
          'ORIENTAL,BOXBUNDLES' AS ship_carriers,
          d_year AS year,
          SUM(CASE
                  WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS jan_sales,
          SUM(CASE
                  WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS feb_sales,
          SUM(CASE
                  WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS mar_sales,
          SUM(CASE
                  WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS apr_sales,
          SUM(CASE
                  WHEN d_moy = 5 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS may_sales,
          SUM(CASE
                  WHEN d_moy = 6 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS jun_sales,
          SUM(CASE
                  WHEN d_moy = 7 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS jul_sales,
          SUM(CASE
                  WHEN d_moy = 8 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS aug_sales,
          SUM(CASE
                  WHEN d_moy = 9 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS sep_sales,
          SUM(CASE
                  WHEN d_moy = 10 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS oct_sales,
          SUM(CASE
                  WHEN d_moy = 11 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS nov_sales,
          SUM(CASE
                  WHEN d_moy = 12 THEN ws_ext_sales_price * ws_quantity
                  ELSE 0
              END) AS dec_sales,
          SUM(CASE
                  WHEN d_moy = 1 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS jan_net,
          SUM(CASE
                  WHEN d_moy = 2 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS feb_net,
          SUM(CASE
                  WHEN d_moy = 3 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS mar_net,
          SUM(CASE
                  WHEN d_moy = 4 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS apr_net,
          SUM(CASE
                  WHEN d_moy = 5 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS may_net,
          SUM(CASE
                  WHEN d_moy = 6 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS jun_net,
          SUM(CASE
                  WHEN d_moy = 7 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS jul_net,
          SUM(CASE
                  WHEN d_moy = 8 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS aug_net,
          SUM(CASE
                  WHEN d_moy = 9 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS sep_net,
          SUM(CASE
                  WHEN d_moy = 10 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS oct_net,
          SUM(CASE
                  WHEN d_moy = 11 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS nov_net,
          SUM(CASE
                  WHEN d_moy = 12 THEN ws_net_paid_inc_ship * ws_quantity
                  ELSE 0
              END) AS dec_net
   FROM web_sales
   JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN time_dim ON ws_sold_time_sk = t_time_sk
   JOIN ship_mode ON ws_ship_mode_sk = sm_ship_mode_sk
   WHERE d_year = 2001
     AND t_time BETWEEN 42970 AND 42970 + 28800
     AND sm_carrier IN ('ORIENTAL',
                        'BOXBUNDLES')
   GROUP BY w_warehouse_name,
            w_warehouse_sq_ft,
            w_city,
            w_county,
            w_state,
            w_country,
            d_year
   UNION ALL SELECT w_warehouse_name,
                    w_warehouse_sq_ft,
                    w_city,
                    w_county,
                    w_state,
                    w_country,
                    'ORIENTAL,BOXBUNDLES' AS ship_carriers,
                    d_year AS year,
                    SUM(CASE
                            WHEN d_moy = 1 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS jan_sales,
                    SUM(CASE
                            WHEN d_moy = 2 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS feb_sales,
                    SUM(CASE
                            WHEN d_moy = 3 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS mar_sales,
                    SUM(CASE
                            WHEN d_moy = 4 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS apr_sales,
                    SUM(CASE
                            WHEN d_moy = 5 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS may_sales,
                    SUM(CASE
                            WHEN d_moy = 6 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS jun_sales,
                    SUM(CASE
                            WHEN d_moy = 7 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS jul_sales,
                    SUM(CASE
                            WHEN d_moy = 8 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS aug_sales,
                    SUM(CASE
                            WHEN d_moy = 9 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS sep_sales,
                    SUM(CASE
                            WHEN d_moy = 10 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS oct_sales,
                    SUM(CASE
                            WHEN d_moy = 11 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS nov_sales,
                    SUM(CASE
                            WHEN d_moy = 12 THEN cs_ext_list_price * cs_quantity
                            ELSE 0
                        END) AS dec_sales,
                    SUM(CASE
                            WHEN d_moy = 1 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS jan_net,
                    SUM(CASE
                            WHEN d_moy = 2 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS feb_net,
                    SUM(CASE
                            WHEN d_moy = 3 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS mar_net,
                    SUM(CASE
                            WHEN d_moy = 4 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS apr_net,
                    SUM(CASE
                            WHEN d_moy = 5 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS may_net,
                    SUM(CASE
                            WHEN d_moy = 6 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS jun_net,
                    SUM(CASE
                            WHEN d_moy = 7 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS jul_net,
                    SUM(CASE
                            WHEN d_moy = 8 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS aug_net,
                    SUM(CASE
                            WHEN d_moy = 9 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS sep_net,
                    SUM(CASE
                            WHEN d_moy = 10 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS oct_net,
                    SUM(CASE
                            WHEN d_moy = 11 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS nov_net,
                    SUM(CASE
                            WHEN d_moy = 12 THEN cs_net_paid * cs_quantity
                            ELSE 0
                        END) AS dec_net
   FROM catalog_sales
   JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN time_dim ON cs_sold_time_sk = t_time_sk
   JOIN ship_mode ON cs_ship_mode_sk = sm_ship_mode_sk
   WHERE d_year = 2001
     AND t_time BETWEEN 42970 AND 42970 + 28800
     AND sm_carrier IN ('ORIENTAL',
                        'BOXBUNDLES')
   GROUP BY w_warehouse_name,
            w_warehouse_sq_ft,
            w_city,
            w_county,
            w_state,
            w_country,
            d_year)
SELECT w_warehouse_name,
       w_warehouse_sq_ft,
       w_city,
       w_county,
       w_state,
       w_country,
       ship_carriers,
       year,
       SUM(jan_sales) AS jan_sales,
       SUM(feb_sales) AS feb_sales,
       SUM(mar_sales) AS mar_sales,
       SUM(apr_sales) AS apr_sales,
       SUM(may_sales) AS may_sales,
       SUM(jun_sales) AS jun_sales,
       SUM(jul_sales) AS jul_sales,
       SUM(aug_sales) AS aug_sales,
       SUM(sep_sales) AS sep_sales,
       SUM(oct_sales) AS oct_sales,
       SUM(nov_sales) AS nov_sales,
       SUM(dec_sales) AS dec_sales,
       SUM(jan_sales) / w_warehouse_sq_ft AS jan_sales_per_sq_foot,
       SUM(feb_sales) / w_warehouse_sq_ft AS feb_sales_per_sq_foot,
       SUM(mar_sales) / w_warehouse_sq_ft AS mar_sales_per_sq_foot,
       SUM(apr_sales) / w_warehouse_sq_ft AS apr_sales_per_sq_foot,
       SUM(may_sales) / w_warehouse_sq_ft AS may_sales_per_sq_foot,
       SUM(jun_sales) / w_warehouse_sq_ft AS jun_sales_per_sq_foot,
       SUM(jul_sales) / w_warehouse_sq_ft AS jul_sales_per_sq_foot,
       SUM(aug_sales) / w_warehouse_sq_ft AS aug_sales_per_sq_foot,
       SUM(sep_sales) / w_warehouse_sq_ft AS sep_sales_per_sq_foot,
       SUM(oct_sales) / w_warehouse_sq_ft AS oct_sales_per_sq_foot,
       SUM(nov_sales) / w_warehouse_sq_ft AS nov_sales_per_sq_foot,
       SUM(dec_sales) / w_warehouse_sq_ft AS dec_sales_per_sq_foot,
       SUM(jan_net) AS jan_net,
       SUM(feb_net) AS feb_net,
       SUM(mar_net) AS mar_net,
       SUM(apr_net) AS apr_net,
       SUM(may_net) AS may_net,
       SUM(jun_net) AS jun_net,
       SUM(jul_net) AS jul_net,
       SUM(aug_net) AS aug_net,
       SUM(sep_net) AS sep_net,
       SUM(oct_net) AS oct_net,
       SUM(nov_net) AS nov_net,
       SUM(dec_net) AS dec_net
FROM sales_data
GROUP BY w_warehouse_name,
         w_warehouse_sq_ft,
         w_city,
         w_county,
         w_state,
         w_country,
         ship_carriers,
         year
ORDER BY w_warehouse_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN170
-- =================================================================
WITH sales_data AS
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sumsales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE d_month_seq BETWEEN 1217 AND 1228
   GROUP BY ROLLUP(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)),
     ranked_sales AS
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sumsales,
          RANK() OVER (PARTITION BY i_category
                       ORDER BY sumsales DESC) AS rk
   FROM sales_data)
SELECT *
FROM ranked_sales
WHERE rk <= 100
ORDER BY i_category,
         i_class,
         i_brand,
         i_product_name,
         d_year,
         d_qoy,
         d_moy,
         s_store_id,
         sumsales,
         rk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN171
-- =================================================================
SELECT c.c_last_name,
       c.c_first_name,
       dn.bought_city,
       current_addr.ca_city,
       dn.ss_ticket_number,
       dn.extended_price,
       dn.extended_tax,
       dn.list_price
FROM
  (SELECT ss.ss_ticket_number,
          ss.ss_customer_sk,
          ca.ca_city AS bought_city,
          SUM(ss.ss_ext_sales_price) AS extended_price,
          SUM(ss.ss_ext_list_price) AS list_price,
          SUM(ss.ss_ext_tax) AS extended_tax
   FROM store_sales ss
   JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
   JOIN store s ON ss.ss_store_sk = s.s_store_sk
   JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
   JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
   WHERE dd.d_dom BETWEEN 1 AND 2
     AND (hd.hd_dep_count = 3
          OR hd.hd_vehicle_count = 4)
     AND dd.d_year BETWEEN 1998 AND 2000
     AND s.s_city IN ('Fairview',
                      'Midway')
   GROUP BY ss.ss_ticket_number,
            ss.ss_customer_sk,
            ca.ca_city) dn
JOIN customer c ON dn.ss_customer_sk = c.c_customer_sk
JOIN customer_address current_addr ON c.c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> dn.bought_city
ORDER BY c.c_last_name,
         dn.ss_ticket_number
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN172
-- =================================================================
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       COUNT(*) AS cnt1,
       cd_purchase_estimate,
       COUNT(*) AS cnt2,
       cd_credit_rating,
       COUNT(*) AS cnt3
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE ca.ca_state IN ('IL',
                      'TX',
                      'ME')
  AND EXISTS
    (SELECT 1
     FROM store_sales ss
     JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
     WHERE c.c_customer_sk = ss.ss_customer_sk
       AND d.d_year = 2002
       AND d.d_moy BETWEEN 1 AND 3 )
  AND NOT EXISTS
    (SELECT 1
     FROM web_sales ws
     JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
     WHERE c.c_customer_sk = ws.ws_bill_customer_sk
       AND d.d_year = 2002
       AND d.d_moy BETWEEN 1 AND 3 )
  AND NOT EXISTS
    (SELECT 1
     FROM catalog_sales cs
     JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
     WHERE c.c_customer_sk = cs.cs_ship_customer_sk
       AND d.d_year = 2002
       AND d.d_moy BETWEEN 1 AND 3 )
GROUP BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
ORDER BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN173
-- =================================================================
select *
from customer ;

-- =================================================================
-- Query ID: TPCDSN174
-- =================================================================
SELECT i_brand_id AS brand_id,
       i_brand AS brand,
       t_hour,
       t_minute,
       SUM(ext_price) AS ext_price
FROM item
JOIN
  (SELECT ws_ext_sales_price AS ext_price,
          ws_sold_date_sk AS sold_date_sk,
          ws_item_sk AS sold_item_sk,
          ws_sold_time_sk AS time_sk
   FROM web_sales
   JOIN date_dim ON d_date_sk = ws_sold_date_sk
   WHERE d_moy = 12
     AND d_year = 2002
   UNION ALL SELECT cs_ext_sales_price AS ext_price,
                    cs_sold_date_sk AS sold_date_sk,
                    cs_item_sk AS sold_item_sk,
                    cs_sold_time_sk AS time_sk
   FROM catalog_sales
   JOIN date_dim ON d_date_sk = cs_sold_date_sk
   WHERE d_moy = 12
     AND d_year = 2002
   UNION ALL SELECT ss_ext_sales_price AS ext_price,
                    ss_sold_date_sk AS sold_date_sk,
                    ss_item_sk AS sold_item_sk,
                    ss_sold_time_sk AS time_sk
   FROM store_sales
   JOIN date_dim ON d_date_sk = ss_sold_date_sk
   WHERE d_moy = 12
     AND d_year = 2002 ) AS tmp ON sold_item_sk = i_item_sk
JOIN time_dim ON time_sk = t_time_sk
WHERE i_manager_id = 1
  AND (t_meal_time = 'breakfast'
       OR t_meal_time = 'dinner')
GROUP BY i_brand_id,
         i_brand,
         t_hour,
         t_minute
ORDER BY ext_price DESC,
         i_brand_id ;

-- =================================================================
-- Query ID: TPCDSN175
-- =================================================================
SELECT i_item_desc,
       w_warehouse_name,
       d1.d_week_seq,
       SUM(CASE
               WHEN p_promo_sk IS NULL THEN 1
               ELSE 0
           END) AS no_promo,
       SUM(CASE
               WHEN p_promo_sk IS NOT NULL THEN 1
               ELSE 0
           END) AS promo,
       COUNT(*) AS total_cnt
FROM catalog_sales
JOIN inventory ON cs_item_sk = inv_item_sk
JOIN warehouse ON w_warehouse_sk = inv_warehouse_sk
JOIN item ON i_item_sk = cs_item_sk
JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
JOIN household_demographics ON cs_bill_hdemo_sk = hd_demo_sk
JOIN date_dim d1 ON cs_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON inv_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_ship_date_sk = d3.d_date_sk
LEFT JOIN promotion ON cs_promo_sk = p_promo_sk
LEFT JOIN catalog_returns ON cr_item_sk = cs_item_sk
AND cr_order_number = cs_order_number
WHERE d1.d_week_seq = d2.d_week_seq
  AND inv_quantity_on_hand < cs_quantity
  AND d3.d_date > d1.d_date
  AND hd_buy_potential = '1001-5000'
  AND d1.d_year = 2002
  AND cd_marital_status = 'W'
GROUP BY i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
ORDER BY total_cnt DESC,
         i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN176
-- =================================================================
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          COUNT(*) AS cnt
   FROM store_sales
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   JOIN store ON store_sales.ss_store_sk = store.s_store_sk
   JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
   WHERE date_dim.d_dom BETWEEN 1 AND 2
     AND household_demographics.hd_buy_potential IN ('1001-5000',
                                                     '5001-10000')
     AND household_demographics.hd_vehicle_count > 0
     AND (household_demographics.hd_dep_count / NULLIF(household_demographics.hd_vehicle_count, 0)) > 1
     AND date_dim.d_year BETWEEN 2000 AND 2002
     AND store.s_county = 'Williamson County'
   GROUP BY ss_ticket_number,
            ss_customer_sk) AS dj
JOIN customer ON dj.ss_customer_sk = customer.c_customer_sk
WHERE cnt BETWEEN 1 AND 5
ORDER BY cnt DESC,
         c_last_name ASC ;

-- =================================================================
-- Query ID: TPCDSN177
-- =================================================================
WITH year_total AS
  (SELECT c_customer_id AS customer_id,
          c_first_name AS customer_first_name,
          c_last_name AS customer_last_name,
          d_year AS year,
          MAX(ss_net_paid) AS year_total,
          's' AS sale_type
   FROM customer
   JOIN store_sales ON c_customer_sk = ss_customer_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_year IN (1999,
                    2000)
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            d_year
   UNION ALL SELECT c_customer_id AS customer_id,
                    c_first_name AS customer_first_name,
                    c_last_name AS customer_last_name,
                    d_year AS year,
                    MAX(ws_net_paid) AS year_total,
                    'w' AS sale_type
   FROM customer
   JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE d_year IN (1999,
                    2000)
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name
FROM year_total t_s_firstyear
JOIN year_total t_s_secyear ON t_s_secyear.customer_id = t_s_firstyear.customer_id
JOIN year_total t_w_firstyear ON t_s_firstyear.customer_id = t_w_firstyear.customer_id
JOIN year_total t_w_secyear ON t_s_firstyear.customer_id = t_w_secyear.customer_id
WHERE t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.year = 1999
  AND t_s_secyear.year = 2000
  AND t_w_firstyear.year = 1999
  AND t_w_secyear.year = 2000
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND COALESCE(t_w_secyear.year_total / NULLIF(t_w_firstyear.year_total, 0), 0) >= COALESCE(t_s_secyear.year_total / NULLIF(t_s_firstyear.year_total, 0), 0)
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_first_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN178
-- =================================================================
WITH sales_detail AS
  (SELECT d_year,
          i_brand_id,
          i_class_id,
          i_category_id,
          i_manufact_id,
          SUM(sales_cnt) AS sales_cnt,
          SUM(sales_amt) AS sales_amt
   FROM
     (SELECT d_year,
             i_brand_id,
             i_class_id,
             i_category_id,
             i_manufact_id,
             cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
             cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt
      FROM catalog_sales
      JOIN item ON i_item_sk = cs_item_sk
      JOIN date_dim ON d_date_sk = cs_sold_date_sk
      LEFT JOIN catalog_returns ON cs_order_number = cr_order_number
      AND cs_item_sk = cr_item_sk
      WHERE i_category = 'Sports'
      UNION ALL SELECT d_year,
                       i_brand_id,
                       i_class_id,
                       i_category_id,
                       i_manufact_id,
                       ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,
                       ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt
      FROM store_sales
      JOIN item ON i_item_sk = ss_item_sk
      JOIN date_dim ON d_date_sk = ss_sold_date_sk
      LEFT JOIN store_returns ON ss_ticket_number = sr_ticket_number
      AND ss_item_sk = sr_item_sk
      WHERE i_category = 'Sports'
      UNION ALL SELECT d_year,
                       i_brand_id,
                       i_class_id,
                       i_category_id,
                       i_manufact_id,
                       ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,
                       ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt
      FROM web_sales
      JOIN item ON i_item_sk = ws_item_sk
      JOIN date_dim ON d_date_sk = ws_sold_date_sk
      LEFT JOIN web_returns ON ws_order_number = wr_order_number
      AND ws_item_sk = wr_item_sk
      WHERE i_category = 'Sports' ) AS sales
   GROUP BY d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id)
SELECT prev_yr.d_year AS prev_year,
       curr_yr.d_year AS year,
       curr_yr.i_brand_id,
       curr_yr.i_class_id,
       curr_yr.i_category_id,
       curr_yr.i_manufact_id,
       prev_yr.sales_cnt AS prev_yr_cnt,
       curr_yr.sales_cnt AS curr_yr_cnt,
       curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
       curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM sales_detail curr_yr
JOIN sales_detail prev_yr ON curr_yr.i_brand_id = prev_yr.i_brand_id
AND curr_yr.i_class_id = prev_yr.i_class_id
AND curr_yr.i_category_id = prev_yr.i_category_id
AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
AND curr_yr.d_year = 2002
AND prev_yr.d_year = 2001
WHERE curr_yr.sales_cnt::DECIMAL(17, 2) / NULLIF(prev_yr.sales_cnt::DECIMAL(17, 2), 0) < 0.9
ORDER BY sales_cnt_diff,
         sales_amt_diff
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN179
-- =================================================================
SELECT channel,
       col_name,
       d_year,
       d_qoy,
       i_category,
       COUNT(*) AS sales_cnt,
       SUM(ext_sales_price) AS sales_amt
FROM
  (SELECT 'store' AS channel,
          'ss_customer_sk' AS col_name,
          d_year,
          d_qoy,
          i_category,
          ss_ext_sales_price AS ext_sales_price
   FROM store_sales
   JOIN item ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE ss_customer_sk IS NOT NULL
   UNION ALL SELECT 'web' AS channel,
                    'ws_promo_sk' AS col_name,
                    d_year,
                    d_qoy,
                    i_category,
                    ws_ext_sales_price AS ext_sales_price
   FROM web_sales
   JOIN item ON ws_item_sk = i_item_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE ws_promo_sk IS NOT NULL
   UNION ALL SELECT 'catalog' AS channel,
                    'cs_bill_customer_sk' AS col_name,
                    d_year,
                    d_qoy,
                    i_category,
                    cs_ext_sales_price AS ext_sales_price
   FROM catalog_sales
   JOIN item ON cs_item_sk = i_item_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE cs_bill_customer_sk IS NOT NULL ) AS foo
GROUP BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
ORDER BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN180
-- =================================================================
WITH date_range AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_date BETWEEN '2000-08-10'::date AND '2000-09-09'::date ),
     ss AS
  (SELECT ss_store_sk,
          SUM(ss_ext_sales_price) AS sales,
          SUM(ss_net_profit) AS profit
   FROM store_sales
   JOIN date_range ON ss_sold_date_sk = d_date_sk
   GROUP BY ss_store_sk),
     sr AS
  (SELECT sr_store_sk,
          SUM(sr_return_amt) AS returns,
          SUM(sr_net_loss) AS profit_loss
   FROM store_returns
   JOIN date_range ON sr_returned_date_sk = d_date_sk
   GROUP BY sr_store_sk),
     cs AS
  (SELECT cs_call_center_sk,
          SUM(cs_ext_sales_price) AS sales,
          SUM(cs_net_profit) AS profit
   FROM catalog_sales
   JOIN date_range ON cs_sold_date_sk = d_date_sk
   GROUP BY cs_call_center_sk),
     cr AS
  (SELECT cr_call_center_sk,
          SUM(cr_return_amount) AS returns,
          SUM(cr_net_loss) AS profit_loss
   FROM catalog_returns
   JOIN date_range ON cr_returned_date_sk = d_date_sk
   GROUP BY cr_call_center_sk),
     ws AS
  (SELECT ws_web_page_sk,
          SUM(ws_ext_sales_price) AS sales,
          SUM(ws_net_profit) AS profit
   FROM web_sales
   JOIN date_range ON ws_sold_date_sk = d_date_sk
   GROUP BY ws_web_page_sk),
     wr AS
  (SELECT wr_web_page_sk,
          SUM(wr_return_amt) AS returns,
          SUM(wr_net_loss) AS profit_loss
   FROM web_returns
   JOIN date_range ON wr_returned_date_sk = d_date_sk
   GROUP BY wr_web_page_sk)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns) AS returns,
       SUM(profit) AS profit
FROM
  (SELECT 'store channel' AS channel,
          ss.ss_store_sk AS id,
          sales,
          COALESCE(returns, 0) AS returns,
          (profit - COALESCE(profit_loss, 0)) AS profit
   FROM ss
   LEFT JOIN sr ON ss.ss_store_sk = sr.sr_store_sk
   UNION ALL SELECT 'catalog channel' AS channel,
                    cs.cs_call_center_sk AS id,
                    sales,
                    COALESCE(returns, 0) AS returns,
                    (profit - COALESCE(profit_loss, 0)) AS profit
   FROM cs
   LEFT JOIN cr ON cs.cs_call_center_sk = cr.cr_call_center_sk
   UNION ALL SELECT 'web channel' AS channel,
                    ws.ws_web_page_sk AS id,
                    sales,
                    COALESCE(returns, 0) AS returns,
                    (profit - COALESCE(profit_loss, 0)) AS profit
   FROM ws
   LEFT JOIN wr ON ws.ws_web_page_sk = wr.wr_web_page_sk) x
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel,
         id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN181
-- =================================================================
WITH ws AS
  (SELECT d_year AS ws_sold_year,
          ws_item_sk,
          ws_bill_customer_sk AS ws_customer_sk,
          SUM(ws_quantity) AS ws_qty,
          SUM(ws_wholesale_cost) AS ws_wc,
          SUM(ws_sales_price) AS ws_sp
   FROM web_sales
   LEFT JOIN web_returns ON wr_order_number = ws_order_number
   AND ws_item_sk = wr_item_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE wr_order_number IS NULL
   GROUP BY d_year,
            ws_item_sk,
            ws_bill_customer_sk),
     cs AS
  (SELECT d_year AS cs_sold_year,
          cs_item_sk,
          cs_bill_customer_sk AS cs_customer_sk,
          SUM(cs_quantity) AS cs_qty,
          SUM(cs_wholesale_cost) AS cs_wc,
          SUM(cs_sales_price) AS cs_sp
   FROM catalog_sales
   LEFT JOIN catalog_returns ON cr_order_number = cs_order_number
   AND cs_item_sk = cr_item_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE cr_order_number IS NULL
   GROUP BY d_year,
            cs_item_sk,
            cs_bill_customer_sk),
     ss AS
  (SELECT d_year AS ss_sold_year,
          ss_item_sk,
          ss_customer_sk,
          SUM(ss_quantity) AS ss_qty,
          SUM(ss_wholesale_cost) AS ss_wc,
          SUM(ss_sales_price) AS ss_sp
   FROM store_sales
   LEFT JOIN store_returns ON sr_ticket_number = ss_ticket_number
   AND ss_item_sk = sr_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE sr_ticket_number IS NULL
   GROUP BY d_year,
            ss_item_sk,
            ss_customer_sk)
SELECT ss.ss_customer_sk,
       ROUND(ss.ss_qty / NULLIF((COALESCE(ws.ws_qty, 0) + COALESCE(cs.cs_qty, 0)), 0), 2) AS ratio,
       ss.ss_qty AS store_qty,
       ss.ss_wc AS store_wholesale_cost,
       ss.ss_sp AS store_sales_price,
       COALESCE(ws.ws_qty, 0) + COALESCE(cs.cs_qty, 0) AS other_chan_qty,
       COALESCE(ws.ws_wc, 0) + COALESCE(cs.cs_wc, 0) AS other_chan_wholesale_cost,
       COALESCE(ws.ws_sp, 0) + COALESCE(cs.cs_sp, 0) AS other_chan_sales_price
FROM ss
LEFT JOIN ws ON ws.ws_sold_year = ss.ss_sold_year
AND ws.ws_item_sk = ss.ss_item_sk
AND ws.ws_customer_sk = ss.ss_customer_sk
LEFT JOIN cs ON cs.cs_sold_year = ss.ss_sold_year
AND cs.cs_item_sk = ss.ss_item_sk
AND cs.cs_customer_sk = ss.ss_customer_sk
WHERE (COALESCE(ws.ws_qty, 0) > 0
       OR COALESCE(cs.cs_qty, 0) > 0)
  AND ss.ss_sold_year = 1998
ORDER BY ss.ss_customer_sk,
         ss.ss_qty DESC,
         ss.ss_wc DESC,
         ss.ss_sp DESC,
         other_chan_qty,
         other_chan_wholesale_cost,
         other_chan_sales_price,
         ratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN182
-- =================================================================
SELECT c.c_last_name,
       c.c_first_name,
       substr(ms.s_city, 1, 30),
       ms.ss_ticket_number,
       ms.amt,
       ms.profit
FROM
  (SELECT ss.ss_ticket_number,
          ss.ss_customer_sk,
          s.s_city,
          SUM(ss.ss_coupon_amt) AS amt,
          SUM(ss.ss_net_profit) AS profit
   FROM store_sales ss
   JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
   JOIN store s ON ss.ss_store_sk = s.s_store_sk
   JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
   WHERE (hd.hd_dep_count = 7
          OR hd.hd_vehicle_count > -1)
     AND dd.d_dow = 4
     AND dd.d_year BETWEEN 2000 AND 2002
     AND s.s_number_employees BETWEEN 200 AND 295
   GROUP BY ss.ss_ticket_number,
            ss.ss_customer_sk,
            s.s_city) ms
JOIN customer c ON ms.ss_customer_sk = c.c_customer_sk
ORDER BY c.c_last_name,
         c.c_first_name,
         substr(ms.s_city, 1, 30),
         ms.profit
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN183
-- =================================================================
WITH date_filtered AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_date BETWEEN '2002-08-14'::date AND '2002-09-13'::date ),
     ssr AS
  (SELECT s_store_id AS store_id,
          SUM(ss_ext_sales_price) AS sales,
          SUM(COALESCE(sr_return_amt, 0)) AS returns,
          SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
   FROM store_sales
   LEFT JOIN store_returns ON ss_item_sk = sr_item_sk
   AND ss_ticket_number = sr_ticket_number
   JOIN date_filtered ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   JOIN item ON ss_item_sk = i_item_sk
   JOIN promotion ON ss_promo_sk = p_promo_sk
   WHERE i_current_price > 50
     AND p_channel_tv = 'N'
   GROUP BY s_store_id),
     csr AS
  (SELECT cp_catalog_page_id AS catalog_page_id,
          SUM(cs_ext_sales_price) AS sales,
          SUM(COALESCE(cr_return_amount, 0)) AS returns,
          SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
   FROM catalog_sales
   LEFT JOIN catalog_returns ON cs_item_sk = cr_item_sk
   AND cs_order_number = cr_order_number
   JOIN date_filtered ON cs_sold_date_sk = d_date_sk
   JOIN catalog_page ON cs_catalog_page_sk = cp_catalog_page_sk
   JOIN item ON cs_item_sk = i_item_sk
   JOIN promotion ON cs_promo_sk = p_promo_sk
   WHERE i_current_price > 50
     AND p_channel_tv = 'N'
   GROUP BY cp_catalog_page_id),
     wsr AS
  (SELECT web_site_id,
          SUM(ws_ext_sales_price) AS sales,
          SUM(COALESCE(wr_return_amt, 0)) AS returns,
          SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
   FROM web_sales
   LEFT JOIN web_returns ON ws_item_sk = wr_item_sk
   AND ws_order_number = wr_order_number
   JOIN date_filtered ON ws_sold_date_sk = d_date_sk
   JOIN web_site ON ws_web_site_sk = web_site_sk
   JOIN item ON ws_item_sk = i_item_sk
   JOIN promotion ON ws_promo_sk = p_promo_sk
   WHERE i_current_price > 50
     AND p_channel_tv = 'N'
   GROUP BY web_site_id)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns) AS returns,
       SUM(profit) AS profit
FROM
  (SELECT 'store channel' AS channel,
          'store' || store_id AS id,
          sales,
          returns,
          profit
   FROM ssr
   UNION ALL SELECT 'catalog channel' AS channel,
                    'catalog_page' || catalog_page_id AS id,
                    sales,
                    returns,
                    profit
   FROM csr
   UNION ALL SELECT 'web channel' AS channel,
                    'web_site' || web_site_id AS id,
                    sales,
                    returns,
                    profit
   FROM wsr) x
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel,
         id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN184
-- =================================================================
WITH customer_total_return AS
  (SELECT cr_returning_customer_sk AS ctr_customer_sk,
          ca_state AS ctr_state,
          SUM(cr_return_amt_inc_tax) AS ctr_total_return
   FROM catalog_returns
   JOIN date_dim ON cr_returned_date_sk = d_date_sk
   JOIN customer_address ON cr_returning_addr_sk = ca_address_sk
   WHERE d_year = 2001
   GROUP BY cr_returning_customer_sk,
            ca_state),
     state_avg_return AS
  (SELECT ctr_state,
          AVG(ctr_total_return) * 1.2 AS avg_return_threshold
   FROM customer_total_return
   GROUP BY ctr_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       ca_street_number,
       ca_street_name,
       ca_street_type,
       ca_suite_number,
       ca_city,
       ca_county,
       ca_state,
       ca_zip,
       ca_country,
       ca_gmt_offset,
       ca_location_type,
       ctr1.ctr_total_return
FROM customer_total_return ctr1
JOIN state_avg_return sar ON ctr1.ctr_state = sar.ctr_state
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
WHERE ctr1.ctr_total_return > sar.avg_return_threshold
  AND ca_state = 'TN'
ORDER BY c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         ca_street_number,
         ca_street_name,
         ca_street_type,
         ca_suite_number,
         ca_city,
         ca_county,
         ca_state,
         ca_zip,
         ca_country,
         ca_gmt_offset,
         ca_location_type,
         ctr_total_return
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN185
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
JOIN store_sales ON ss_item_sk = i_item_sk
WHERE i_current_price BETWEEN 58 AND 88
  AND d_date BETWEEN DATE '2001-01-13' AND DATE '2001-03-14'
  AND i_manufact_id IN (259,
                        559,
                        580,
                        485)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN186
-- =================================================================
WITH date_filter AS
  (SELECT d_week_seq
   FROM date_dim
   WHERE d_date IN ('2001-07-13',
                    '2001-09-10',
                    '2001-11-16',
                    '2000-12-14') ),
     filtered_dates AS
  (SELECT d_date
   FROM date_dim
   WHERE d_week_seq IN
       (SELECT d_week_seq
        FROM date_filter) ),
     sr_items AS
  (SELECT i_item_id AS item_id,
          SUM(sr_return_quantity) AS sr_item_qty
   FROM store_returns
   JOIN item ON sr_item_sk = i_item_sk
   JOIN date_dim ON sr_returned_date_sk = d_date_sk
   WHERE d_date IN
       (SELECT d_date
        FROM filtered_dates)
   GROUP BY i_item_id),
     cr_items AS
  (SELECT i_item_id AS item_id,
          SUM(cr_return_quantity) AS cr_item_qty
   FROM catalog_returns
   JOIN item ON cr_item_sk = i_item_sk
   JOIN date_dim ON cr_returned_date_sk = d_date_sk
   WHERE d_date IN
       (SELECT d_date
        FROM filtered_dates)
   GROUP BY i_item_id),
     wr_items AS
  (SELECT i_item_id AS item_id,
          SUM(wr_return_quantity) AS wr_item_qty
   FROM web_returns
   JOIN item ON wr_item_sk = i_item_sk
   JOIN date_dim ON wr_returned_date_sk = d_date_sk
   WHERE d_date IN
       (SELECT d_date
        FROM filtered_dates)
   GROUP BY i_item_id)
SELECT sr_items.item_id,
       sr_item_qty,
       sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS sr_dev,
       cr_item_qty,
       cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS cr_dev,
       wr_item_qty,
       wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS wr_dev,
       (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 AS average
FROM sr_items
JOIN cr_items ON sr_items.item_id = cr_items.item_id
JOIN wr_items ON sr_items.item_id = wr_items.item_id
ORDER BY sr_items.item_id,
         sr_item_qty
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN187
-- =================================================================
SELECT c.c_customer_id AS customer_id,
       COALESCE(c.c_last_name, '') || ',' || COALESCE(c.c_first_name, '') AS customername
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
JOIN household_demographics hd ON hd.hd_demo_sk = c.c_current_hdemo_sk
JOIN income_band ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
JOIN store_returns sr ON sr.sr_cdemo_sk = cd.cd_demo_sk
WHERE ca.ca_city = 'Woodland'
  AND ib.ib_lower_bound >= 60306
  AND ib.ib_upper_bound <= 60306 + 50000
ORDER BY c.c_customer_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN188
-- =================================================================
SELECT substr(r_reason_desc, 1, 20),
       AVG(ws_quantity),
       AVG(wr_refunded_cash),
       AVG(wr_fee)
FROM web_sales
JOIN web_returns ON ws_item_sk = wr_item_sk
AND ws_order_number = wr_order_number
JOIN web_page ON ws_web_page_sk = wp_web_page_sk
JOIN customer_demographics cd1 ON cd1.cd_demo_sk = wr_refunded_cdemo_sk
JOIN customer_demographics cd2 ON cd2.cd_demo_sk = wr_returning_cdemo_sk
JOIN customer_address ON ca_address_sk = wr_refunded_addr_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN reason ON r_reason_sk = wr_reason_sk
WHERE d_year = 1998
  AND ((cd1.cd_marital_status = 'D'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Primary'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws_sales_price BETWEEN 100.00 AND 150.00)
       OR (cd1.cd_marital_status = 'S'
           AND cd1.cd_marital_status = cd2.cd_marital_status
           AND cd1.cd_education_status = 'College'
           AND cd1.cd_education_status = cd2.cd_education_status
           AND ws_sales_price BETWEEN 50.00 AND 100.00)
       OR (cd1.cd_marital_status = 'U'
           AND cd1.cd_marital_status = cd2.cd_marital_status
           AND cd1.cd_education_status = 'Advanced Degree'
           AND cd1.cd_education_status = cd2.cd_education_status
           AND ws_sales_price BETWEEN 150.00 AND 200.00))
  AND ((ca_country = 'United States'
        AND ca_state IN ('NC',
                         'TX',
                         'IA')
        AND ws_net_profit BETWEEN 100 AND 200)
       OR (ca_country = 'United States'
           AND ca_state IN ('WI',
                            'WV',
                            'GA')
           AND ws_net_profit BETWEEN 150 AND 300)
       OR (ca_country = 'United States'
           AND ca_state IN ('OK',
                            'VA',
                            'KY')
           AND ws_net_profit BETWEEN 50 AND 250))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 20),
         AVG(ws_quantity),
         AVG(wr_refunded_cash),
         AVG(wr_fee)
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN189
-- =================================================================
SELECT total_sum,
       i_category,
       i_class,
       lochierarchy,
       rank_within_parent
FROM
  (SELECT SUM(ws_net_paid) AS total_sum,
          i_category,
          i_class,
          GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
          RANK() OVER (PARTITION BY GROUPING(i_category) + GROUPING(i_class),
                                    CASE
                                        WHEN GROUPING(i_class) = 0 THEN i_category
                                    END
                       ORDER BY SUM(ws_net_paid) DESC) AS rank_within_parent
   FROM web_sales
   JOIN date_dim d1 ON d1.d_date_sk = ws_sold_date_sk
   JOIN item ON i_item_sk = ws_item_sk
   WHERE d1.d_month_seq BETWEEN 1186 AND 1197
   GROUP BY ROLLUP(i_category, i_class)) AS tmp
ORDER BY lochierarchy DESC,
         CASE
             WHEN lochierarchy = 0 THEN i_category
         END,
         rank_within_parent
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN190
-- =================================================================
WITH store_customers AS
  (SELECT DISTINCT c_last_name,
                   c_first_name,
                   d_date
   FROM store_sales
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
   WHERE d_month_seq BETWEEN 1202 AND 1213 ),
     catalog_customers AS
  (SELECT DISTINCT c_last_name,
                   c_first_name,
                   d_date
   FROM catalog_sales
   JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
   JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
   WHERE d_month_seq BETWEEN 1202 AND 1213 ),
     web_customers AS
  (SELECT DISTINCT c_last_name,
                   c_first_name,
                   d_date
   FROM web_sales
   JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
   JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
   WHERE d_month_seq BETWEEN 1202 AND 1213 )
SELECT COUNT(*)
FROM
  (SELECT c_last_name,
          c_first_name,
          d_date
   FROM store_customers
   EXCEPT SELECT c_last_name,
                 c_first_name,
                 d_date
   FROM catalog_customers
   EXCEPT SELECT c_last_name,
                 c_first_name,
                 d_date
   FROM web_customers) AS unique_store_customers ;

-- =================================================================
-- Query ID: TPCDSN191
-- =================================================================
WITH filtered_sales AS
  (SELECT ss_sold_time_sk
   FROM store_sales
   JOIN household_demographics ON ss_hdemo_sk = household_demographics.hd_demo_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE store.s_store_name = 'ese'
     AND ((household_demographics.hd_dep_count = 0
           AND household_demographics.hd_vehicle_count <= 2)
          OR (household_demographics.hd_dep_count = -1
              AND household_demographics.hd_vehicle_count <= 1)
          OR (household_demographics.hd_dep_count = 3
              AND household_demographics.hd_vehicle_count <= 5)) ),
     time_intervals AS
  (SELECT t_time_sk,
          t_hour,
          t_minute
   FROM time_dim
   WHERE (t_hour = 8
          AND t_minute >= 30)
     OR (t_hour = 9
         AND t_minute < 30)
     OR (t_hour = 9
         AND t_minute >= 30)
     OR (t_hour = 10
         AND t_minute < 30)
     OR (t_hour = 10
         AND t_minute >= 30)
     OR (t_hour = 11
         AND t_minute < 30)
     OR (t_hour = 11
         AND t_minute >= 30)
     OR (t_hour = 12
         AND t_minute < 30) )
SELECT COUNT(CASE
                 WHEN t_hour = 8
                      AND t_minute >= 30 THEN 1
             END) AS h8_30_to_9,
       COUNT(CASE
                 WHEN t_hour = 9
                      AND t_minute < 30 THEN 1
             END) AS h9_to_9_30,
       COUNT(CASE
                 WHEN t_hour = 9
                      AND t_minute >= 30 THEN 1
             END) AS h9_30_to_10,
       COUNT(CASE
                 WHEN t_hour = 10
                      AND t_minute < 30 THEN 1
             END) AS h10_to_10_30,
       COUNT(CASE
                 WHEN t_hour = 10
                      AND t_minute >= 30 THEN 1
             END) AS h10_30_to_11,
       COUNT(CASE
                 WHEN t_hour = 11
                      AND t_minute < 30 THEN 1
             END) AS h11_to_11_30,
       COUNT(CASE
                 WHEN t_hour = 11
                      AND t_minute >= 30 THEN 1
             END) AS h11_30_to_12,
       COUNT(CASE
                 WHEN t_hour = 12
                      AND t_minute < 30 THEN 1
             END) AS h12_to_12_30
FROM filtered_sales
JOIN time_intervals ON filtered_sales.ss_sold_time_sk = time_intervals.t_time_sk ;

-- =================================================================
-- Query ID: TPCDSN192
-- =================================================================
WITH sales_data AS
  (SELECT i_category,
          i_class,
          i_brand,
          s_store_name,
          s_company_name,
          d_moy,
          SUM(ss_sales_price) AS sum_sales,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_category,
                                                      i_brand,
                                                      s_store_name,
                                                      s_company_name) AS avg_monthly_sales
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_year = 2001
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('history',
                           'school-uniforms',
                           'audio'))
          OR (i_category IN ('Men',
                             'Sports',
                             'Shoes')
              AND i_class IN ('pants',
                              'tennis',
                              'womens')))
   GROUP BY i_category,
            i_class,
            i_brand,
            s_store_name,
            s_company_name,
            d_moy)
SELECT *
FROM sales_data
WHERE CASE
          WHEN avg_monthly_sales <> 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
          ELSE NULL
      END > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN193
-- =================================================================
WITH amc_cte AS
  (SELECT COUNT(*) AS amc
   FROM web_sales
   JOIN household_demographics ON ws_ship_hdemo_sk = hd_demo_sk
   JOIN time_dim ON ws_sold_time_sk = t_time_sk
   JOIN web_page ON ws_web_page_sk = wp_web_page_sk
   WHERE t_hour = 12
     AND hd_dep_count = 6
     AND wp_char_count BETWEEN 5000 AND 5200 ),
     pmc_cte AS
  (SELECT COUNT(*) AS pmc
   FROM web_sales
   JOIN household_demographics ON ws_ship_hdemo_sk = hd_demo_sk
   JOIN time_dim ON ws_sold_time_sk = t_time_sk
   JOIN web_page ON ws_web_page_sk = wp_web_page_sk
   WHERE t_hour = 14
     AND hd_dep_count = 6
     AND wp_char_count BETWEEN 5000 AND 5200 )
SELECT CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio
FROM amc_cte,
     pmc_cte
ORDER BY am_pm_ratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN194
-- =================================================================
SELECT cc.cc_call_center_id AS Call_Center,
       cc.cc_name AS Call_Center_Name,
       cc.cc_manager AS Manager,
       SUM(cr.cr_net_loss) AS Returns_Loss
FROM call_center cc
JOIN catalog_returns cr ON cr.cr_call_center_sk = cc.cc_call_center_sk
JOIN date_dim dd ON cr.cr_returned_date_sk = dd.d_date_sk
JOIN customer c ON cr.cr_returning_customer_sk = c.c_customer_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
JOIN household_demographics hd ON hd.hd_demo_sk = c.c_current_hdemo_sk
JOIN customer_address ca ON ca.ca_address_sk = c.c_current_addr_sk
WHERE dd.d_year = 2000
  AND dd.d_moy = 12
  AND ((cd.cd_marital_status = 'M'
        AND cd.cd_education_status = 'Advanced Degree ')
       OR (cd.cd_marital_status = 'W'
           AND cd.cd_education_status = 'Unnknown'))
  AND hd.hd_buy_potential LIKE 'Unknown%'
  AND ca.ca_gmt_offset = -7
GROUP BY cc.cc_call_center_id,
         cc.cc_name,
         cc.cc_manager
ORDER BY Returns_Loss DESC ;

-- =================================================================
-- Query ID: TPCDSN195
-- =================================================================
SELECT SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales
JOIN item ON i_item_sk = ws_item_sk
JOIN date_dim ON d_date_sk = ws_sold_date_sk
WHERE i_manufact_id = 393
  AND d_date BETWEEN '2000-02-01' AND '2000-05-01'
  AND ws_ext_discount_amt >
    (SELECT 1.3 * AVG(ws_ext_discount_amt)
     FROM web_sales
     JOIN date_dim ON d_date_sk = ws_sold_date_sk
     WHERE ws_item_sk = i_item_sk
       AND d_date BETWEEN '2000-02-01' AND '2000-05-01' )
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN196
-- =================================================================
SELECT ss_customer_sk,
       SUM(CASE
               WHEN sr_return_quantity IS NOT NULL THEN (ss_quantity - sr_return_quantity) * ss_sales_price
               ELSE ss_quantity * ss_sales_price
           END) AS sumsales
FROM store_sales
LEFT JOIN store_returns ON sr_item_sk = ss_item_sk
AND sr_ticket_number = ss_ticket_number
JOIN reason ON sr_reason_sk = r_reason_sk
WHERE r_reason_desc = 'Package was damaged'
GROUP BY ss_customer_sk
ORDER BY sumsales,
         ss_customer_sk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN197
-- =================================================================
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_dim ON ws1.ws_ship_date_sk = d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
WHERE d_date BETWEEN '2002-05-01' AND '2002-06-30'
  AND ca_state = 'OK'
  AND web_company_name = 'pri'
  AND EXISTS
    (SELECT 1
     FROM web_sales ws2
     WHERE ws1.ws_order_number = ws2.ws_order_number
       AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk )
  AND NOT EXISTS
    (SELECT 1
     FROM web_returns wr1
     WHERE ws1.ws_order_number = wr1.wr_order_number )
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN198
-- =================================================================
WITH ws_wh AS
  (SELECT ws1.ws_order_number
   FROM web_sales ws1
   JOIN web_sales ws2 ON ws1.ws_order_number = ws2.ws_order_number
   WHERE ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk ),
     ws_orders AS
  (SELECT wr_order_number
   FROM web_returns
   WHERE wr_order_number IN
       (SELECT ws_order_number
        FROM ws_wh) )
SELECT COUNT(DISTINCT ws1.ws_order_number) AS "order count",
       SUM(ws1.ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws1.ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_dim ON ws1.ws_ship_date_sk = d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
WHERE d_date BETWEEN '2001-04-01' AND (CAST('2001-04-01' AS DATE) + INTERVAL '60' DAY)
  AND ca_state = 'VA'
  AND web_company_name = 'pri'
  AND ws1.ws_order_number IN
    (SELECT ws_order_number
     FROM ws_wh)
  AND ws1.ws_order_number IN
    (SELECT wr_order_number
     FROM ws_orders)
GROUP BY ws1.ws_order_number
ORDER BY "order count"
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN199
-- =================================================================
SELECT count(*)
FROM store_sales
JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk
JOIN household_demographics ON ss_hdemo_sk = household_demographics.hd_demo_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE time_dim.t_hour = 8
  AND time_dim.t_minute >= 30
  AND household_demographics.hd_dep_count = 0
  AND store.s_store_name = 'ese' ;

-- =================================================================
-- Query ID: TPCDSN200
-- =================================================================
WITH ssci AS
  (SELECT ss_customer_sk AS customer_sk,
          ss_item_sk AS item_sk
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1199 AND 1210
   GROUP BY ss_customer_sk,
            ss_item_sk),
     csci AS
  (SELECT cs_bill_customer_sk AS customer_sk,
          cs_item_sk AS item_sk
   FROM catalog_sales
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1199 AND 1210
   GROUP BY cs_bill_customer_sk,
            cs_item_sk)
SELECT SUM(CASE
               WHEN ssci.customer_sk IS NOT NULL
                    AND csci.customer_sk IS NULL THEN 1
               ELSE 0
           END) AS store_only,
       SUM(CASE
               WHEN ssci.customer_sk IS NULL
                    AND csci.customer_sk IS NOT NULL THEN 1
               ELSE 0
           END) AS catalog_only,
       SUM(CASE
               WHEN ssci.customer_sk IS NOT NULL
                    AND csci.customer_sk IS NOT NULL THEN 1
               ELSE 0
           END) AS store_and_catalog
FROM ssci
FULL OUTER JOIN csci ON ssci.customer_sk = csci.customer_sk
AND ssci.item_sk = csci.item_sk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN201
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ss_ext_sales_price) AS itemrevenue,
       SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM store_sales
JOIN item ON ss_item_sk = i_item_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE i_category IN ('Men',
                     'Sports',
                     'Jewelry')
  AND d_date BETWEEN '1999-02-05' AND '1999-03-07'
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio ;

-- =================================================================
-- Query ID: TPCDSN202
-- =================================================================
SELECT substr(w.w_warehouse_name, 1, 20),
       sm.sm_type,
       cc.cc_name,
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS "30 days",
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 30)
                    AND (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS "31-60 days",
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 60)
                    AND (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS "61-90 days",
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 90)
                    AND (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS "91-120 days",
       SUM(CASE
               WHEN (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS ">120 days"
FROM catalog_sales cs
JOIN warehouse w ON cs.cs_warehouse_sk = w.w_warehouse_sk
JOIN ship_mode sm ON cs.cs_ship_mode_sk = sm.sm_ship_mode_sk
JOIN call_center cc ON cs.cs_call_center_sk = cc.cc_call_center_sk
JOIN date_dim d ON cs.cs_ship_date_sk = d.d_date_sk
WHERE d.d_month_seq BETWEEN 1194 AND 1205
GROUP BY substr(w.w_warehouse_name, 1, 20),
         sm.sm_type,
         cc.cc_name
ORDER BY substr(w.w_warehouse_name, 1, 20),
         sm.sm_type,
         cc.cc_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN203
-- =================================================================
with customer_total_return as
  (select sr_customer_sk as ctr_customer_sk,
          sr_store_sk as ctr_store_sk,
          sum(SR_FEE) as ctr_total_return
   from store_returns,
        date_dim
   where sr_returned_date_sk = d_date_sk
     and d_year = 2000
   group by sr_customer_sk,
            sr_store_sk),
     avg_total_return_cte as
  (select ctr_store_sk,
          avg(ctr_total_return) * 1.2 as avg_total_return
   from customer_total_return
   group by ctr_store_sk)
select c_customer_id
from customer_total_return ctr1
join store on s_store_sk = ctr1.ctr_store_sk
join customer on ctr1.ctr_customer_sk = c_customer_sk
join avg_total_return_cte on ctr1.ctr_store_sk = avg_total_return_cte.ctr_store_sk
where ctr1.ctr_total_return > avg_total_return_cte.avg_total_return
  and s_state = 'TN'
order by c_customer_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN204
-- =================================================================
with wscs as
  (select sold_date_sk,
          sales_price
   from
     (select ws_sold_date_sk sold_date_sk,
             ws_ext_sales_price sales_price
      from web_sales
      union all select cs_sold_date_sk sold_date_sk,
                       cs_ext_sales_price sales_price
      from catalog_sales) as tmp),
     wswscs as
  (select d_week_seq,
          sum(case
                  when (d_day_name = 'Sunday') then sales_price
                  else null
              end) sun_sales,
          sum(case
                  when (d_day_name = 'Monday') then sales_price
                  else null
              end) mon_sales,
          sum(case
                  when (d_day_name = 'Tuesday') then sales_price
                  else null
              end) tue_sales,
          sum(case
                  when (d_day_name = 'Wednesday') then sales_price
                  else null
              end) wed_sales,
          sum(case
                  when (d_day_name = 'Thursday') then sales_price
                  else null
              end) thu_sales,
          sum(case
                  when (d_day_name = 'Friday') then sales_price
                  else null
              end) fri_sales,
          sum(case
                  when (d_day_name = 'Saturday') then sales_price
                  else null
              end) sat_sales
   from wscs,
        date_dim
   where d_date_sk = sold_date_sk
   group by d_week_seq),
     wswscs_1998 as
  (select wswscs.d_week_seq d_week_seq1,
          sun_sales sun_sales1,
          mon_sales mon_sales1,
          tue_sales tue_sales1,
          wed_sales wed_sales1,
          thu_sales thu_sales1,
          fri_sales fri_sales1,
          sat_sales sat_sales1
   from wswscs,
        date_dim
   where date_dim.d_week_seq = wswscs.d_week_seq
     and d_year = 1998 ),
     wswscs_1999 as
  (select wswscs.d_week_seq d_week_seq2,
          sun_sales sun_sales2,
          mon_sales mon_sales2,
          tue_sales tue_sales2,
          wed_sales wed_sales2,
          thu_sales thu_sales2,
          fri_sales fri_sales2,
          sat_sales sat_sales2
   from wswscs,
        date_dim
   where date_dim.d_week_seq = wswscs.d_week_seq
     and d_year = 1999 )
select d_week_seq1,
       round(sun_sales1 / sun_sales2, 2),
       round(mon_sales1 / mon_sales2, 2),
       round(tue_sales1 / tue_sales2, 2),
       round(wed_sales1 / wed_sales2, 2),
       round(thu_sales1 / thu_sales2, 2),
       round(fri_sales1 / fri_sales2, 2),
       round(sat_sales1 / sat_sales2, 2)
from wswscs_1998 y,
     wswscs_1999 z
where d_week_seq1 = d_week_seq2 - 53
order by d_week_seq1 ;

-- =================================================================
-- Query ID: TPCDSN205
-- =================================================================
with filtered_items as
  (select i_item_sk,
          i_brand_id,
          i_brand
   from item
   where i_manufact_id = 816 ),
     filtered_dates as
  (select d_date_sk,
          d_year
   from date_dim
   where d_moy = 11 )
select dt.d_year,
       fi.i_brand_id as brand_id,
       fi.i_brand as brand,
       sum(ss.ss_sales_price) as sum_agg
from store_sales ss
join filtered_dates dt on dt.d_date_sk = ss.ss_sold_date_sk
join filtered_items fi on ss.ss_item_sk = fi.i_item_sk
group by dt.d_year,
         fi.i_brand,
         fi.i_brand_id
order by dt.d_year,
         sum_agg desc,
         brand_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN206
-- =================================================================
with store_sales_cte as
  (select c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          c_preferred_cust_flag customer_preferred_cust_flag,
          c_birth_country customer_birth_country,
          c_login customer_login,
          c_email_address customer_email_address,
          d_year dyear,
          sum(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) year_total,
          's' sale_type
   from customer,
        store_sales,
        date_dim
   where c_customer_sk = ss_customer_sk
     and ss_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year),
     catalog_sales_cte as
  (select c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          c_preferred_cust_flag customer_preferred_cust_flag,
          c_birth_country customer_birth_country,
          c_login customer_login,
          c_email_address customer_email_address,
          d_year dyear,
          sum((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) year_total,
          'c' sale_type
   from customer,
        catalog_sales,
        date_dim
   where c_customer_sk = cs_bill_customer_sk
     and cs_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year),
     web_sales_cte as
  (select c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          c_preferred_cust_flag customer_preferred_cust_flag,
          c_birth_country customer_birth_country,
          c_login customer_login,
          c_email_address customer_email_address,
          d_year dyear,
          sum((((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2)) year_total,
          'w' sale_type
   from customer,
        web_sales,
        date_dim
   where c_customer_sk = ws_bill_customer_sk
     and ws_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year),
     year_total as
  (select *
   from store_sales_cte
   union all select *
   from catalog_sales_cte
   union all select *
   from web_sales_cte)
select t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_birth_country
from year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_c_firstyear,
     year_total t_c_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_c_secyear.customer_id
  and t_s_firstyear.customer_id = t_c_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_c_firstyear.sale_type = 'c'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_c_secyear.sale_type = 'c'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 1999
  and t_s_secyear.dyear = 1999
  and t_c_firstyear.dyear = 1999
  and t_c_secyear.dyear = 1999
  and t_w_firstyear.dyear = 1999
  and t_w_secyear.dyear = 1999
  and t_s_firstyear.year_total > 0
  and t_c_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case
          when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total
          else null
      end >= case
                 when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total
                 else null
             end
  and case
          when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total
          else null
      end >= case
                 when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total
                 else null
             end
order by t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_birth_country
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN207
-- =================================================================
with salesreturns_store as
  (select ss_store_sk as store_sk,
          ss_sold_date_sk as date_sk,
          ss_ext_sales_price as sales_price,
          ss_net_profit as profit,
          cast(0 as decimal(7, 2)) as return_amt,
          cast(0 as decimal(7, 2)) as net_loss
   from store_sales
   union all select sr_store_sk as store_sk,
                    sr_returned_date_sk as date_sk,
                    cast(0 as decimal(7, 2)) as sales_price,
                    cast(0 as decimal(7, 2)) as profit,
                    sr_return_amt as return_amt,
                    sr_net_loss as net_loss
   from store_returns),
     salesreturns_catalog as
  (select cs_catalog_page_sk as page_sk,
          cs_sold_date_sk as date_sk,
          cs_ext_sales_price as sales_price,
          cs_net_profit as profit,
          cast(0 as decimal(7, 2)) as return_amt,
          cast(0 as decimal(7, 2)) as net_loss
   from catalog_sales
   union all select cr_catalog_page_sk as page_sk,
                    cr_returned_date_sk as date_sk,
                    cast(0 as decimal(7, 2)) as sales_price,
                    cast(0 as decimal(7, 2)) as profit,
                    cr_return_amount as return_amt,
                    cr_net_loss as net_loss
   from catalog_returns),
     salesreturns_web as
  (select ws_web_site_sk as wsr_web_site_sk,
          ws_sold_date_sk as date_sk,
          ws_ext_sales_price as sales_price,
          ws_net_profit as profit,
          cast(0 as decimal(7, 2)) as return_amt,
          cast(0 as decimal(7, 2)) as net_loss
   from web_sales
   union all select ws_web_site_sk as wsr_web_site_sk,
                    wr_returned_date_sk as date_sk,
                    cast(0 as decimal(7, 2)) as sales_price,
                    cast(0 as decimal(7, 2)) as profit,
                    wr_return_amt as return_amt,
                    wr_net_loss as net_loss
   from web_returns
   left outer join web_sales on (wr_item_sk = ws_item_sk
                                 and wr_order_number = ws_order_number)),
     ssr as
  (select s_store_id,
          sum(sales_price) as sales,
          sum(profit) as profit,
          sum(return_amt) as returns,
          sum(net_loss) as profit_loss
   from salesreturns_store,
        date_dim,
        store
   where date_sk = d_date_sk
     and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day)
     and store_sk = s_store_sk
   group by s_store_id),
     csr as
  (select cp_catalog_page_id,
          sum(sales_price) as sales,
          sum(profit) as profit,
          sum(return_amt) as returns,
          sum(net_loss) as profit_loss
   from salesreturns_catalog,
        date_dim,
        catalog_page
   where date_sk = d_date_sk
     and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day)
     and page_sk = cp_catalog_page_sk
   group by cp_catalog_page_id),
     wsr as
  (select web_site_id,
          sum(sales_price) as sales,
          sum(profit) as profit,
          sum(return_amt) as returns,
          sum(net_loss) as profit_loss
   from salesreturns_web,
        date_dim,
        web_site
   where date_sk = d_date_sk
     and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day)
     and wsr_web_site_sk = web_site_sk
   group by web_site_id)
select channel,
       id,
       sum(sales) as sales,
       sum(returns) as returns,
       sum(profit) as profit
from
  (select 'store channel' as channel,
          'store' || s_store_id as id,
          sales,
          returns,
          (profit - profit_loss) as profit
   from ssr
   union all select 'catalog channel' as channel,
                    'catalog_page' || cp_catalog_page_id as id,
                    sales,
                    returns,
                    (profit - profit_loss) as profit
   from csr
   union all select 'web channel' as channel,
                    'web_site' || web_site_id as id,
                    sales,
                    returns,
                    (profit - profit_loss) as profit
   from wsr) x
group by rollup (channel,
                 id)
order by channel,
         id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN208
-- =================================================================
with month_seq_cte as
  (select distinct d_month_seq
   from date_dim
   where d_year = 2002
     and d_moy = 3 ),
     avg_price_cte as
  (select i_category,
          avg(i_current_price) as avg_price
   from item
   group by i_category)
select a.ca_state state,
       count(*) cnt
from customer_address a
join customer c on a.ca_address_sk = c.c_current_addr_sk
join store_sales s on c.c_customer_sk = s.ss_customer_sk
join date_dim d on s.ss_sold_date_sk = d.d_date_sk
join item i on s.ss_item_sk = i.i_item_sk
join month_seq_cte on d.d_month_seq = month_seq_cte.d_month_seq
join avg_price_cte on i.i_category = avg_price_cte.i_category
where i.i_current_price > 1.2 * avg_price_cte.avg_price
group by a.ca_state
having count(*) >= 10
order by cnt,
         a.ca_state
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN209
-- =================================================================
with filtered_data as
  (select ss_item_sk,
          ss_quantity,
          ss_list_price,
          ss_coupon_amt,
          ss_sales_price
   from store_sales
   join customer_demographics on ss_cdemo_sk = cd_demo_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   join promotion on ss_promo_sk = p_promo_sk
   where cd_gender = 'F'
     and cd_marital_status = 'W'
     and cd_education_status = 'College'
     and (p_channel_email = 'N'
          or p_channel_event = 'N')
     and d_year = 2001 )
select i_item_id,
       avg(ss_quantity) as agg1,
       avg(ss_list_price) as agg2,
       avg(ss_coupon_amt) as agg3,
       avg(ss_sales_price) as agg4
from filtered_data
join item on filtered_data.ss_item_sk = i_item_sk
group by i_item_id
order by i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN210
-- =================================================================
with ca_zip_cte as
  (select ca_zip
   from
     (select substr(ca_zip, 1, 5) as ca_zip
      from customer_address
      where substr(ca_zip, 1, 5) in ('47602',
                                     '16704',
                                     '35863',
                                     '28577',
                                     '83910',
                                     '36201',
                                     '58412',
                                     '48162',
                                     '28055',
                                     '41419',
                                     '80332',
                                     '38607',
                                     '77817',
                                     '24891',
                                     '16226',
                                     '18410',
                                     '21231',
                                     '59345',
                                     '13918',
                                     '51089',
                                     '20317',
                                     '17167',
                                     '54585',
                                     '67881',
                                     '78366',
                                     '47770',
                                     '18360',
                                     '51717',
                                     '73108',
                                     '14440',
                                     '21800',
                                     '89338',
                                     '45859',
                                     '65501',
                                     '34948',
                                     '25973',
                                     '73219',
                                     '25333',
                                     '17291',
                                     '10374',
                                     '18829',
                                     '60736',
                                     '82620',
                                     '41351',
                                     '52094',
                                     '19326',
                                     '25214',
                                     '54207',
                                     '40936',
                                     '21814',
                                     '79077',
                                     '25178',
                                     '75742',
                                     '77454',
                                     '30621',
                                     '89193',
                                     '27369',
                                     '41232',
                                     '48567',
                                     '83041',
                                     '71948',
                                     '37119',
                                     '68341',
                                     '14073',
                                     '16891',
                                     '62878',
                                     '49130',
                                     '19833',
                                     '24286',
                                     '27700',
                                     '40979',
                                     '50412',
                                     '81504',
                                     '94835',
                                     '84844',
                                     '71954',
                                     '39503',
                                     '57649',
                                     '18434',
                                     '24987',
                                     '12350',
                                     '86379',
                                     '27413',
                                     '44529',
                                     '98569',
                                     '16515',
                                     '27287',
                                     '24255',
                                     '21094',
                                     '16005',
                                     '56436',
                                     '91110',
                                     '68293',
                                     '56455',
                                     '54558',
                                     '10298',
                                     '83647',
                                     '32754',
                                     '27052',
                                     '51766',
                                     '19444',
                                     '13869',
                                     '45645',
                                     '94791',
                                     '57631',
                                     '20712',
                                     '37788',
                                     '41807',
                                     '46507',
                                     '21727',
                                     '71836',
                                     '81070',
                                     '50632',
                                     '88086',
                                     '63991',
                                     '20244',
                                     '31655',
                                     '51782',
                                     '29818',
                                     '63792',
                                     '68605',
                                     '94898',
                                     '36430',
                                     '57025',
                                     '20601',
                                     '82080',
                                     '33869',
                                     '22728',
                                     '35834',
                                     '29086',
                                     '92645',
                                     '98584',
                                     '98072',
                                     '11652',
                                     '78093',
                                     '57553',
                                     '43830',
                                     '71144',
                                     '53565',
                                     '18700',
                                     '90209',
                                     '71256',
                                     '38353',
                                     '54364',
                                     '28571',
                                     '96560',
                                     '57839',
                                     '56355',
                                     '50679',
                                     '45266',
                                     '84680',
                                     '34306',
                                     '34972',
                                     '48530',
                                     '30106',
                                     '15371',
                                     '92380',
                                     '84247',
                                     '92292',
                                     '68852',
                                     '13338',
                                     '34594',
                                     '82602',
                                     '70073',
                                     '98069',
                                     '85066',
                                     '47289',
                                     '11686',
                                     '98862',
                                     '26217',
                                     '47529',
                                     '63294',
                                     '51793',
                                     '35926',
                                     '24227',
                                     '14196',
                                     '24594',
                                     '32489',
                                     '99060',
                                     '49472',
                                     '43432',
                                     '49211',
                                     '14312',
                                     '88137',
                                     '47369',
                                     '56877',
                                     '20534',
                                     '81755',
                                     '15794',
                                     '12318',
                                     '21060',
                                     '73134',
                                     '41255',
                                     '63073',
                                     '81003',
                                     '73873',
                                     '66057',
                                     '51184',
                                     '51195',
                                     '45676',
                                     '92696',
                                     '70450',
                                     '90669',
                                     '98338',
                                     '25264',
                                     '38919',
                                     '59226',
                                     '58581',
                                     '60298',
                                     '17895',
                                     '19489',
                                     '52301',
                                     '80846',
                                     '95464',
                                     '68770',
                                     '51634',
                                     '19988',
                                     '18367',
                                     '18421',
                                     '11618',
                                     '67975',
                                     '25494',
                                     '41352',
                                     '95430',
                                     '15734',
                                     '62585',
                                     '97173',
                                     '33773',
                                     '10425',
                                     '75675',
                                     '53535',
                                     '17879',
                                     '41967',
                                     '12197',
                                     '67998',
                                     '79658',
                                     '59130',
                                     '72592',
                                     '14851',
                                     '43933',
                                     '68101',
                                     '50636',
                                     '25717',
                                     '71286',
                                     '24660',
                                     '58058',
                                     '72991',
                                     '95042',
                                     '15543',
                                     '33122',
                                     '69280',
                                     '11912',
                                     '59386',
                                     '27642',
                                     '65177',
                                     '17672',
                                     '33467',
                                     '64592',
                                     '36335',
                                     '54010',
                                     '18767',
                                     '63193',
                                     '42361',
                                     '49254',
                                     '33113',
                                     '33159',
                                     '36479',
                                     '59080',
                                     '11855',
                                     '81963',
                                     '31016',
                                     '49140',
                                     '29392',
                                     '41836',
                                     '32958',
                                     '53163',
                                     '13844',
                                     '73146',
                                     '23952',
                                     '65148',
                                     '93498',
                                     '14530',
                                     '46131',
                                     '58454',
                                     '13376',
                                     '13378',
                                     '83986',
                                     '12320',
                                     '17193',
                                     '59852',
                                     '46081',
                                     '98533',
                                     '52389',
                                     '13086',
                                     '68843',
                                     '31013',
                                     '13261',
                                     '60560',
                                     '13443',
                                     '45533',
                                     '83583',
                                     '11489',
                                     '58218',
                                     '19753',
                                     '22911',
                                     '25115',
                                     '86709',
                                     '27156',
                                     '32669',
                                     '13123',
                                     '51933',
                                     '39214',
                                     '41331',
                                     '66943',
                                     '14155',
                                     '69998',
                                     '49101',
                                     '70070',
                                     '35076',
                                     '14242',
                                     '73021',
                                     '59494',
                                     '15782',
                                     '29752',
                                     '37914',
                                     '74686',
                                     '83086',
                                     '34473',
                                     '15751',
                                     '81084',
                                     '49230',
                                     '91894',
                                     '60624',
                                     '17819',
                                     '28810',
                                     '63180',
                                     '56224',
                                     '39459',
                                     '55233',
                                     '75752',
                                     '43639',
                                     '55349',
                                     '86057',
                                     '62361',
                                     '50788',
                                     '31830',
                                     '58062',
                                     '18218',
                                     '85761',
                                     '60083',
                                     '45484',
                                     '21204',
                                     '90229',
                                     '70041',
                                     '41162',
                                     '35390',
                                     '16364',
                                     '39500',
                                     '68908',
                                     '26689',
                                     '52868',
                                     '81335',
                                     '40146',
                                     '11340',
                                     '61527',
                                     '61794',
                                     '71997',
                                     '30415',
                                     '59004',
                                     '29450',
                                     '58117',
                                     '69952',
                                     '33562',
                                     '83833',
                                     '27385',
                                     '61860',
                                     '96435',
                                     '48333',
                                     '23065',
                                     '32961',
                                     '84919',
                                     '61997',
                                     '99132',
                                     '22815',
                                     '56600',
                                     '68730',
                                     '48017',
                                     '95694',
                                     '32919',
                                     '88217',
                                     '27116',
                                     '28239',
                                     '58032',
                                     '18884',
                                     '16791',
                                     '21343',
                                     '97462',
                                     '18569',
                                     '75660',
                                     '15475') intersect
        select ca_zip
        from
          (select substr(ca_zip, 1, 5) as ca_zip,
                  count(*) as cnt
           from customer_address,
                customer
           where ca_address_sk = c_current_addr_sk
             and c_preferred_cust_flag = 'Y'
           group by ca_zip
           having count(*) > 10) A1 ) A2)
select s_store_name,
       sum(ss_net_profit)
from store_sales,
     date_dim,
     store,
     ca_zip_cte
where ss_store_sk = s_store_sk
  and ss_sold_date_sk = d_date_sk
  and d_qoy = 2
  and d_year = 1998
  and substr(s_zip, 0, 1) = substr(ca_zip_cte.ca_zip, 0, 1)
group by s_store_name
order by s_store_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN211
-- =================================================================
with store_sales_cte as
  (select ss_quantity,
          count(*) as cnt,
          avg(ss_ext_tax) as avg_ext_tax,
          avg(ss_net_paid_inc_tax) as avg_net_paid_inc_tax
   from store_sales
   where ss_quantity between 1 and 100
   group by ss_quantity)
select case
           when
                  (select sum(cnt)
                   from store_sales_cte
                   where ss_quantity between 1 and 20) > 1071 then
                  (select avg(avg_ext_tax)
                   from store_sales_cte
                   where ss_quantity between 1 and 20)
           else
                  (select avg(avg_net_paid_inc_tax)
                   from store_sales_cte
                   where ss_quantity between 1 and 20)
       end as bucket1,
       case
           when
                  (select sum(cnt)
                   from store_sales_cte
                   where ss_quantity between 21 and 40) > 39161 then
                  (select avg(avg_ext_tax)
                   from store_sales_cte
                   where ss_quantity between 21 and 40)
           else
                  (select avg(avg_net_paid_inc_tax)
                   from store_sales_cte
                   where ss_quantity between 21 and 40)
       end as bucket2,
       case
           when
                  (select sum(cnt)
                   from store_sales_cte
                   where ss_quantity between 41 and 60) > 29434 then
                  (select avg(avg_ext_tax)
                   from store_sales_cte
                   where ss_quantity between 41 and 60)
           else
                  (select avg(avg_net_paid_inc_tax)
                   from store_sales_cte
                   where ss_quantity between 41 and 60)
       end as bucket3,
       case
           when
                  (select sum(cnt)
                   from store_sales_cte
                   where ss_quantity between 61 and 80) > 6568 then
                  (select avg(avg_ext_tax)
                   from store_sales_cte
                   where ss_quantity between 61 and 80)
           else
                  (select avg(avg_net_paid_inc_tax)
                   from store_sales_cte
                   where ss_quantity between 61 and 80)
       end as bucket4,
       case
           when
                  (select sum(cnt)
                   from store_sales_cte
                   where ss_quantity between 81 and 100) > 21216 then
                  (select avg(avg_ext_tax)
                   from store_sales_cte
                   where ss_quantity between 81 and 100)
           else
                  (select avg(avg_net_paid_inc_tax)
                   from store_sales_cte
                   where ss_quantity between 81 and 100)
       end as bucket5
from reason
where r_reason_sk = 1 ;

-- =================================================================
-- Query ID: TPCDSN212
-- =================================================================
with store_sales_cte as
  (select ss_customer_sk
   from store_sales
   join date_dim on ss_sold_date_sk = d_date_sk
   where d_year = 1999
     and d_moy between 1 and 12 ),
     web_sales_cte as
  (select ws_bill_customer_sk
   from web_sales
   join date_dim on ws_sold_date_sk = d_date_sk
   where d_year = 1999
     and d_moy between 1 and 12 ),
     catalog_sales_cte as
  (select cs_ship_customer_sk
   from catalog_sales
   join date_dim on cs_sold_date_sk = d_date_sk
   where d_year = 1999
     and d_moy between 1 and 12 )
select cd_gender,
       cd_marital_status,
       cd_education_status,
       count(*) cnt1,
       cd_purchase_estimate,
       count(*) cnt2,
       cd_credit_rating,
       count(*) cnt3,
       cd_dep_count,
       count(*) cnt4,
       cd_dep_employed_count,
       count(*) cnt5,
       cd_dep_college_count,
       count(*) cnt6
from customer c
join customer_address ca on c.c_current_addr_sk = ca.ca_address_sk
join customer_demographics on cd_demo_sk = c.c_current_cdemo_sk
where ca_county in ('Fairfield County',
                    'Campbell County',
                    'Washtenaw County',
                    'Escambia County',
                    'Cleburne County',
                    'United States',
                    '1')
  and exists
    (select 1
     from store_sales_cte
     where c.c_customer_sk = store_sales_cte.ss_customer_sk )
  and (exists
         (select 1
          from web_sales_cte
          where c.c_customer_sk = web_sales_cte.ws_bill_customer_sk )
       or exists
         (select 1
          from catalog_sales_cte
          where c.c_customer_sk = catalog_sales_cte.cs_ship_customer_sk ))
group by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
order by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN213
-- =================================================================
with store_sales_year_total as
  (select c_customer_id as customer_id,
          c_first_name as customer_first_name,
          c_last_name as customer_last_name,
          c_preferred_cust_flag as customer_preferred_cust_flag,
          c_birth_country as customer_birth_country,
          c_login as customer_login,
          c_email_address as customer_email_address,
          d_year as dyear,
          sum(ss_ext_list_price - ss_ext_discount_amt) as year_total,
          's' as sale_type
   from customer,
        store_sales,
        date_dim
   where c_customer_sk = ss_customer_sk
     and ss_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year),
     web_sales_year_total as
  (select c_customer_id as customer_id,
          c_first_name as customer_first_name,
          c_last_name as customer_last_name,
          c_preferred_cust_flag as customer_preferred_cust_flag,
          c_birth_country as customer_birth_country,
          c_login as customer_login,
          c_email_address as customer_email_address,
          d_year as dyear,
          sum(ws_ext_list_price - ws_ext_discount_amt) as year_total,
          'w' as sale_type
   from customer,
        web_sales,
        date_dim
   where c_customer_sk = ws_bill_customer_sk
     and ws_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year),
     year_total as
  (select *
   from store_sales_year_total
   union all select *
   from web_sales_year_total)
select t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_email_address
from year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 1999
  and t_s_secyear.dyear = 1999
  and t_w_firstyear.dyear = 1999
  and t_w_secyear.dyear = 1999
  and t_s_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case
          when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total
          else 0.0
      end >= case
                 when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total
                 else 0.0
             end
order by t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_email_address
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN214
-- =================================================================
WITH sales_data AS
  (SELECT ws_item_sk,
          ws_ext_sales_price
   FROM web_sales,
        date_dim
   WHERE ws_sold_date_sk = d_date_sk
     AND d_date BETWEEN CAST('2001-06-15' AS DATE) AND (CAST('2001-06-15' AS DATE) + INTERVAL '30' DAY) ),
     item_revenue AS
  (SELECT i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price,
          SUM(sales_data.ws_ext_sales_price) AS itemrevenue
   FROM sales_data,
        item
   WHERE sales_data.ws_item_sk = i_item_sk
     AND i_category IN ('Men',
                        'Books',
                        'Electronics')
   GROUP BY i_item_id,
            i_item_desc,
            i_category,
            i_class,
            i_current_price),
     class_revenue AS
  (SELECT i_class,
          SUM(itemrevenue) AS total_class_revenue
   FROM item_revenue
   GROUP BY i_class)
SELECT item_revenue.i_item_id,
       item_revenue.i_item_desc,
       item_revenue.i_category,
       item_revenue.i_class,
       item_revenue.i_current_price,
       item_revenue.itemrevenue,
       item_revenue.itemrevenue * 100 / class_revenue.total_class_revenue AS revenueratio
FROM item_revenue
JOIN class_revenue ON item_revenue.i_class = class_revenue.i_class
ORDER BY item_revenue.i_category,
         item_revenue.i_class,
         item_revenue.i_item_id,
         item_revenue.i_item_desc,
         revenueratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN215
-- =================================================================
with filtered_sales as
  (select ss_quantity,
          ss_ext_sales_price,
          ss_ext_wholesale_cost
   from store_sales
   join store on s_store_sk = ss_store_sk
   join customer_demographics on cd_demo_sk = ss_cdemo_sk
   join household_demographics on ss_hdemo_sk = hd_demo_sk
   join customer_address on ss_addr_sk = ca_address_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   where d_year = 2001
     and ((cd_marital_status = 'M'
           and cd_education_status = 'College'
           and ss_sales_price between 100.00 and 150.00
           and hd_dep_count = 3)
          or (cd_marital_status = 'D'
              and cd_education_status = 'Primary'
              and ss_sales_price between 50.00 and 100.00
              and hd_dep_count = 1)
          or (cd_marital_status = 'W'
              and cd_education_status = '2 yr Degree'
              and ss_sales_price between 150.00 and 200.00
              and hd_dep_count = 1))
     and ((ca_country = 'United States'
           and ca_state in ('IL',
                            'TN',
                            'TX')
           and ss_net_profit between 100 and 200)
          or (ca_country = 'United States'
              and ca_state in ('WY',
                               'OH',
                               'ID')
              and ss_net_profit between 150 and 300)
          or (ca_country = 'United States'
              and ca_state in ('MS',
                               'SC',
                               'IA')
              and ss_net_profit between 50 and 250)) )
select avg(ss_quantity),
       avg(ss_ext_sales_price),
       avg(ss_ext_wholesale_cost),
       sum(ss_ext_wholesale_cost)
from filtered_sales ;

-- =================================================================
-- Query ID: TPCDSN216
-- =================================================================
select *
from customer ;

-- =================================================================
-- Query ID: TPCDSN217
-- =================================================================
with filtered_customers as
  (select c_customer_sk,
          c_current_addr_sk
   from customer
   where c_current_addr_sk in
       (select ca_address_sk
        from customer_address
        where substr(ca_zip, 1, 5) in ('85669',
                                       '86197',
                                       '88274',
                                       '83405',
                                       '86475',
                                       '85392',
                                       '85460',
                                       '80348',
                                       '81792')
          or ca_state in ('CA',
                          'WA',
                          'GA') ) ),
     filtered_sales as
  (select cs_bill_customer_sk,
          cs_sales_price,
          cs_sold_date_sk
   from catalog_sales
   where cs_sales_price > 500 )
select ca_zip,
       sum(cs_sales_price)
from filtered_sales
join filtered_customers on filtered_sales.cs_bill_customer_sk = filtered_customers.c_customer_sk
join customer_address on filtered_customers.c_current_addr_sk = customer_address.ca_address_sk
join date_dim on filtered_sales.cs_sold_date_sk = date_dim.d_date_sk
where d_qoy = 2
  and d_year = 2001
group by ca_zip
order by ca_zip
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN218
-- =================================================================
with catalog_sales_warehouse_diff as
  (select cs_order_number
   from catalog_sales cs2
   group by cs_order_number
   having count(distinct cs_warehouse_sk) > 1),
     catalog_returns_exclusion as
  (select distinct cr_order_number
   from catalog_returns)
select count(distinct cs1.cs_order_number) as "order count",
       sum(cs1.cs_ext_ship_cost) as "total shipping cost",
       sum(cs1.cs_net_profit) as "total net profit"
from catalog_sales cs1
join date_dim on cs1.cs_ship_date_sk = d_date_sk
join customer_address on cs1.cs_ship_addr_sk = ca_address_sk
join call_center on cs1.cs_call_center_sk = cc_call_center_sk
where d_date between '2002-4-01' and (cast('2002-4-01' as date) + interval '60' day)
  and ca_state = 'PA'
  and cc_county in ('Williamson County')
  and cs1.cs_order_number in
    (select cs_order_number
     from catalog_sales_warehouse_diff)
  and cs1.cs_order_number not in
    (select cr_order_number
     from catalog_returns_exclusion)
order by count(distinct cs1.cs_order_number)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN219
-- =================================================================
with date_dim_cte as
  (select d_date_sk,
          d_quarter_name
   from date_dim
   where d_quarter_name in ('2001Q1',
                            '2001Q2',
                            '2001Q3') ),
     store_sales_cte as
  (select ss_sold_date_sk,
          ss_item_sk,
          ss_store_sk,
          ss_customer_sk,
          ss_ticket_number,
          ss_quantity
   from store_sales
   join date_dim_cte on d_date_sk = ss_sold_date_sk
   where d_quarter_name = '2001Q1' ),
     store_returns_cte as
  (select sr_customer_sk,
          sr_item_sk,
          sr_ticket_number,
          sr_returned_date_sk,
          sr_return_quantity
   from store_returns
   join date_dim_cte on d_date_sk = sr_returned_date_sk),
     catalog_sales_cte as
  (select cs_bill_customer_sk,
          cs_item_sk,
          cs_sold_date_sk,
          cs_quantity
   from catalog_sales
   join date_dim_cte on d_date_sk = cs_sold_date_sk)
select i_item_id,
       i_item_desc,
       s_state,
       count(ss_quantity) as store_sales_quantitycount,
       avg(ss_quantity) as store_sales_quantityave,
       stddev_samp(ss_quantity) as store_sales_quantitystdev,
       stddev_samp(ss_quantity) / avg(ss_quantity) as store_sales_quantitycov,
       count(sr_return_quantity) as store_returns_quantitycount,
       avg(sr_return_quantity) as store_returns_quantityave,
       stddev_samp(sr_return_quantity) as store_returns_quantitystdev,
       stddev_samp(sr_return_quantity) / avg(sr_return_quantity) as store_returns_quantitycov,
       count(cs_quantity) as catalog_sales_quantitycount,
       avg(cs_quantity) as catalog_sales_quantityave,
       stddev_samp(cs_quantity) as catalog_sales_quantitystdev,
       stddev_samp(cs_quantity) / avg(cs_quantity) as catalog_sales_quantitycov
from store_sales_cte
join store_returns_cte on store_sales_cte.ss_customer_sk = store_returns_cte.sr_customer_sk
and store_sales_cte.ss_item_sk = store_returns_cte.sr_item_sk
and store_sales_cte.ss_ticket_number = store_returns_cte.sr_ticket_number
join catalog_sales_cte on store_returns_cte.sr_customer_sk = catalog_sales_cte.cs_bill_customer_sk
and store_returns_cte.sr_item_sk = catalog_sales_cte.cs_item_sk
join store on store_sales_cte.ss_store_sk = s_store_sk
join item on store_sales_cte.ss_item_sk = i_item_sk
group by i_item_id,
         i_item_desc,
         s_state
order by i_item_id,
         i_item_desc,
         s_state
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN220
-- =================================================================
with filtered_customers as
  (select c_customer_sk,
          c_current_cdemo_sk,
          c_current_addr_sk,
          c_birth_year,
          c_birth_month
   from customer
   where c_birth_month in (1,
                           3,
                           7,
                           11,
                           10,
                           4) ),
     filtered_customer_demographics as
  (select cd_demo_sk,
          cd_dep_count
   from customer_demographics
   where cd_gender = 'F'
     and cd_education_status = 'Primary' ),
     filtered_customer_address as
  (select ca_address_sk,
          ca_country,
          ca_state,
          ca_county
   from customer_address
   where ca_state in ('AL',
                      'MO',
                      'TN',
                      'GA',
                      'MT',
                      'IN',
                      'CA') ),
     filtered_date_dim as
  (select d_date_sk
   from date_dim
   where d_year = 2001 )
select i_item_id,
       ca_country,
       ca_state,
       ca_county,
       avg(cast(cs_quantity as decimal(12, 2))) as agg1,
       avg(cast(cs_list_price as decimal(12, 2))) as agg2,
       avg(cast(cs_coupon_amt as decimal(12, 2))) as agg3,
       avg(cast(cs_sales_price as decimal(12, 2))) as agg4,
       avg(cast(cs_net_profit as decimal(12, 2))) as agg5,
       avg(cast(c_birth_year as decimal(12, 2))) as agg6,
       avg(cast(cd1.cd_dep_count as decimal(12, 2))) as agg7
from catalog_sales
join filtered_date_dim on cs_sold_date_sk = d_date_sk
join item on cs_item_sk = i_item_sk
join filtered_customer_demographics cd1 on cs_bill_cdemo_sk = cd1.cd_demo_sk
join filtered_customers on cs_bill_customer_sk = c_customer_sk
join filtered_customer_demographics cd2 on c_current_cdemo_sk = cd2.cd_demo_sk
join filtered_customer_address on c_current_addr_sk = ca_address_sk
group by rollup (i_item_id,
                 ca_country,
                 ca_state,
                 ca_county)
order by ca_country,
         ca_state,
         ca_county,
         i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN221
-- =================================================================
with filtered_date_dim as
  (select d_date_sk
   from date_dim
   where d_moy = 11
     and d_year = 2002 ),
     filtered_item as
  (select i_item_sk,
          i_brand_id,
          i_brand,
          i_manufact_id,
          i_manufact
   from item
   where i_manager_id = 14 ),
     filtered_customer as
  (select c_customer_sk,
          c_current_addr_sk
   from customer),
     filtered_customer_address as
  (select ca_address_sk,
          ca_zip
   from customer_address),
     filtered_store as
  (select s_store_sk,
          s_zip
   from store)
select fi.i_brand_id as brand_id,
       fi.i_brand as brand,
       fi.i_manufact_id,
       fi.i_manufact,
       sum(ss.ss_ext_sales_price) as ext_price
from store_sales ss
join filtered_date_dim fdd on fdd.d_date_sk = ss.ss_sold_date_sk
join filtered_item fi on ss.ss_item_sk = fi.i_item_sk
join filtered_customer fc on ss.ss_customer_sk = fc.c_customer_sk
join filtered_customer_address fca on fc.c_current_addr_sk = fca.ca_address_sk
join filtered_store fs on ss.ss_store_sk = fs.s_store_sk
where substr(fca.ca_zip, 1, 5) <> substr(fs.s_zip, 1, 5)
group by fi.i_brand,
         fi.i_brand_id,
         fi.i_manufact_id,
         fi.i_manufact
order by ext_price desc,
         fi.i_brand,
         fi.i_brand_id,
         fi.i_manufact_id,
         fi.i_manufact
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN222
-- =================================================================
WITH date_filtered_sales AS
  (SELECT cs_item_sk,
          cs_ext_sales_price
   FROM catalog_sales
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE d_date BETWEEN CAST('2002-06-18' AS DATE) AND (CAST('2002-06-18' AS DATE) + INTERVAL '30' DAY) ),
     category_filtered_items AS
  (SELECT i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price,
          i_item_sk
   FROM item
   WHERE i_category IN ('Books',
                        'Music',
                        'Sports') ),
     sales_with_items AS
  (SELECT i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price,
          cs_ext_sales_price
   FROM date_filtered_sales
   JOIN category_filtered_items ON date_filtered_sales.cs_item_sk = category_filtered_items.i_item_sk),
     class_revenue AS
  (SELECT i_class,
          SUM(cs_ext_sales_price) AS total_class_revenue
   FROM sales_with_items
   GROUP BY i_class)
SELECT sales_with_items.i_item_id,
       sales_with_items.i_item_desc,
       sales_with_items.i_category,
       sales_with_items.i_class,
       sales_with_items.i_current_price,
       SUM(sales_with_items.cs_ext_sales_price) AS itemrevenue,
       SUM(sales_with_items.cs_ext_sales_price) * 100 / class_revenue.total_class_revenue AS revenueratio
FROM sales_with_items
JOIN class_revenue ON sales_with_items.i_class = class_revenue.i_class
GROUP BY sales_with_items.i_item_id,
         sales_with_items.i_item_desc,
         sales_with_items.i_category,
         sales_with_items.i_class,
         sales_with_items.i_current_price,
         class_revenue.total_class_revenue
ORDER BY sales_with_items.i_category,
         sales_with_items.i_class,
         sales_with_items.i_item_id,
         sales_with_items.i_item_desc,
         revenueratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN223
-- =================================================================
with inventory_summary as
  (select w_warehouse_name,
          i_item_id,
          sum(case
                  when (cast(d_date as date) < cast ('1999-06-22' as date)) then inv_quantity_on_hand
                  else 0
              end) as inv_before,
          sum(case
                  when (cast(d_date as date) >= cast ('1999-06-22' as date)) then inv_quantity_on_hand
                  else 0
              end) as inv_after
   from inventory,
        warehouse,
        item,
        date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk = inv_item_sk
     and inv_warehouse_sk = w_warehouse_sk
     and inv_date_sk = d_date_sk
     and d_date between (cast ('1999-06-22' as date) - interval '30' day) and (cast ('1999-06-22' as date) + interval '30' day)
   group by w_warehouse_name,
            i_item_id)
select *
from inventory_summary
where (case
           when inv_before > 0 then inv_after / inv_before
           else null
       end) between 2.0 / 3.0 and 3.0 / 2.0
order by w_warehouse_name,
         i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN224
-- =================================================================
with date_range_cte as
  (select d_date_sk
   from date_dim
   where d_month_seq between 1200 and 1200 + 11 )
select i_product_name,
       i_brand,
       i_class,
       i_category,
       avg(inv_quantity_on_hand) as qoh
from inventory
join date_range_cte on inventory.inv_date_sk = date_range_cte.d_date_sk
join item on inventory.inv_item_sk = item.i_item_sk
group by rollup(i_product_name, i_brand, i_class, i_category)
order by qoh,
         i_product_name,
         i_brand,
         i_class,
         i_category
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN225
-- =================================================================
select *
from customer ;

-- =================================================================
-- Query ID: TPCDSN226
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'khaki'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN227
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'seashell'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN228
-- =================================================================
with ss_cte as
  (select ss_sold_date_sk,
          ss_item_sk,
          ss_store_sk,
          ss_customer_sk,
          ss_ticket_number,
          ss_net_profit
   from store_sales
   join date_dim d1 on d1.d_date_sk = ss_sold_date_sk
   where d1.d_moy = 4
     and d1.d_year = 1999 ),
     sr_cte as
  (select sr_customer_sk,
          sr_item_sk,
          sr_ticket_number,
          sr_returned_date_sk,
          sr_net_loss
   from store_returns
   join date_dim d2 on sr_returned_date_sk = d2.d_date_sk
   where d2.d_moy between 4 and 10
     and d2.d_year = 1999 ),
     cs_cte as
  (select cs_bill_customer_sk,
          cs_item_sk,
          cs_sold_date_sk,
          cs_net_profit
   from catalog_sales
   join date_dim d3 on cs_sold_date_sk = d3.d_date_sk
   where d3.d_moy between 4 and 10
     and d3.d_year = 1999 )
select i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       max(ss_cte.ss_net_profit) as store_sales_profit,
       max(sr_cte.sr_net_loss) as store_returns_loss,
       max(cs_cte.cs_net_profit) as catalog_sales_profit
from ss_cte
join sr_cte on ss_cte.ss_customer_sk = sr_cte.sr_customer_sk
and ss_cte.ss_item_sk = sr_cte.sr_item_sk
and ss_cte.ss_ticket_number = sr_cte.sr_ticket_number
join cs_cte on sr_cte.sr_customer_sk = cs_cte.cs_bill_customer_sk
and sr_cte.sr_item_sk = cs_cte.cs_item_sk
join store on ss_cte.ss_store_sk = s_store_sk
join item on i_item_sk = ss_cte.ss_item_sk
group by i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
order by i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN229
-- =================================================================
with filtered_data as
  (select cs_item_sk,
          cs_quantity,
          cs_list_price,
          cs_coupon_amt,
          cs_sales_price
   from catalog_sales
   join customer_demographics on cs_bill_cdemo_sk = cd_demo_sk
   join date_dim on cs_sold_date_sk = d_date_sk
   join promotion on cs_promo_sk = p_promo_sk
   where cd_gender = 'M'
     and cd_marital_status = 'W'
     and cd_education_status = 'Unknown'
     and (p_channel_email = 'N'
          or p_channel_event = 'N')
     and d_year = 2002 )
select i_item_id,
       avg(cs_quantity) as agg1,
       avg(cs_list_price) as agg2,
       avg(cs_coupon_amt) as agg3,
       avg(cs_sales_price) as agg4
from filtered_data
join item on filtered_data.cs_item_sk = i_item_sk
group by i_item_id
order by i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN230
-- =================================================================
with filtered_data as
  (select ss_quantity,
          ss_list_price,
          ss_coupon_amt,
          ss_sales_price,
          i_item_id,
          s_state
   from store_sales
   join customer_demographics on ss_cdemo_sk = cd_demo_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   join store on ss_store_sk = s_store_sk
   join item on ss_item_sk = i_item_sk
   where cd_gender = 'M'
     and cd_marital_status = 'W'
     and cd_education_status = 'Secondary'
     and d_year = 1999
     and s_state in ('TN',
                     'TN',
                     'TN',
                     'TN',
                     'TN',
                     'TN') )
select i_item_id,
       s_state,
       grouping(s_state) g_state,
       avg(ss_quantity) agg1,
       avg(ss_list_price) agg2,
       avg(ss_coupon_amt) agg3,
       avg(ss_sales_price) agg4
from filtered_data
group by rollup (i_item_id,
                 s_state)
order by i_item_id,
         s_state
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN231
-- =================================================================
with B1 as
  (select avg(ss_list_price) as B1_LP,
          count(ss_list_price) as B1_CNT,
          count(distinct ss_list_price) as B1_CNTD
   from store_sales
   where ss_quantity between 0 and 5
     and (ss_list_price between 107 and 117
          or ss_coupon_amt between 1319 and 2319
          or ss_wholesale_cost between 60 and 80) ),
     B2 as
  (select avg(ss_list_price) as B2_LP,
          count(ss_list_price) as B2_CNT,
          count(distinct ss_list_price) as B2_CNTD
   from store_sales
   where ss_quantity between 6 and 10
     and (ss_list_price between 23 and 33
          or ss_coupon_amt between 825 and 1825
          or ss_wholesale_cost between 43 and 63) ),
     B3 as
  (select avg(ss_list_price) as B3_LP,
          count(ss_list_price) as B3_CNT,
          count(distinct ss_list_price) as B3_CNTD
   from store_sales
   where ss_quantity between 11 and 15
     and (ss_list_price between 74 and 84
          or ss_coupon_amt between 4381 and 5381
          or ss_wholesale_cost between 57 and 77) ),
     B4 as
  (select avg(ss_list_price) as B4_LP,
          count(ss_list_price) as B4_CNT,
          count(distinct ss_list_price) as B4_CNTD
   from store_sales
   where ss_quantity between 16 and 20
     and (ss_list_price between 89 and 99
          or ss_coupon_amt between 3117 and 4117
          or ss_wholesale_cost between 68 and 88) ),
     B5 as
  (select avg(ss_list_price) as B5_LP,
          count(ss_list_price) as B5_CNT,
          count(distinct ss_list_price) as B5_CNTD
   from store_sales
   where ss_quantity between 21 and 25
     and (ss_list_price between 58 and 68
          or ss_coupon_amt between 9402 and 10402
          or ss_wholesale_cost between 38 and 58) ),
     B6 as
  (select avg(ss_list_price) as B6_LP,
          count(ss_list_price) as B6_CNT,
          count(distinct ss_list_price) as B6_CNTD
   from store_sales
   where ss_quantity between 26 and 30
     and (ss_list_price between 64 and 74
          or ss_coupon_amt between 5792 and 6792
          or ss_wholesale_cost between 73 and 93) )
select *
from B1,
     B2,
     B3,
     B4,
     B5,
     B6
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN232
-- =================================================================
with date_dim_cte as
  (select d_date_sk,
          d_moy,
          d_year
   from date_dim
   where (d_moy = 4
          and d_year = 1998)
     or (d_moy between 4 and 4 + 3
         and d_year = 1998)
     or (d_year in (1998,
                    1998 + 1,
                    1998 + 2)) )
select i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       max(ss_quantity) as store_sales_quantity,
       max(sr_return_quantity) as store_returns_quantity,
       max(cs_quantity) as catalog_sales_quantity
from store_sales
join store_returns on ss_customer_sk = sr_customer_sk
and ss_item_sk = sr_item_sk
and ss_ticket_number = sr_ticket_number
join catalog_sales on sr_customer_sk = cs_bill_customer_sk
and sr_item_sk = cs_item_sk
join date_dim_cte d1 on d1.d_date_sk = ss_sold_date_sk
and d1.d_moy = 4
and d1.d_year = 1998
join date_dim_cte d2 on sr_returned_date_sk = d2.d_date_sk
and d2.d_moy between 4 and 4 + 3
and d2.d_year = 1998
join date_dim_cte d3 on cs_sold_date_sk = d3.d_date_sk
and d3.d_year in (1998,
                  1998 + 1,
                  1998 + 2)
join store on s_store_sk = ss_store_sk
join item on i_item_sk = ss_item_sk
group by i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
order by i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN233
-- =================================================================
WITH customer_total_return AS
  (SELECT wr_returning_customer_sk AS ctr_customer_sk,
          ca_state AS ctr_state,
          SUM(wr_return_amt) AS ctr_total_return
   FROM web_returns
   JOIN date_dim ON wr_returned_date_sk = d_date_sk
   JOIN customer_address ON wr_returning_addr_sk = ca_address_sk
   WHERE d_year = 2000
   GROUP BY wr_returning_customer_sk,
            ca_state),
     avg_total_return_cte AS
  (SELECT ctr_state,
          AVG(ctr_total_return) * 1.2 AS avg_total_return
   FROM customer_total_return
   GROUP BY ctr_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       c_preferred_cust_flag,
       c_birth_day,
       c_birth_month,
       c_birth_year,
       c_birth_country,
       c_login,
       c_email_address,
       c_last_review_date,
       ctr1.ctr_total_return
FROM customer_total_return ctr1
JOIN customer_address ON ctr1.ctr_customer_sk = ca_address_sk
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN avg_total_return_cte ON ctr1.ctr_state = avg_total_return_cte.ctr_state
WHERE ctr1.ctr_total_return > avg_total_return_cte.avg_total_return
  AND ca_state = 'IN'
ORDER BY c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         c_preferred_cust_flag,
         c_birth_day,
         c_birth_month,
         c_birth_year,
         c_birth_country,
         c_login,
         c_email_address,
         c_last_review_date,
         ctr_total_return
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN234
-- =================================================================
with ss as
  (select ca_county,
          d_qoy,
          d_year,
          sum(ss_ext_sales_price) as store_sales
   from store_sales,
        date_dim,
        customer_address
   where ss_sold_date_sk = d_date_sk
     and ss_addr_sk = ca_address_sk
   group by ca_county,
            d_qoy,
            d_year),
     ws as
  (select ca_county,
          d_qoy,
          d_year,
          sum(ws_ext_sales_price) as web_sales
   from web_sales,
        date_dim,
        customer_address
   where ws_sold_date_sk = d_date_sk
     and ws_bill_addr_sk = ca_address_sk
   group by ca_county,
            d_qoy,
            d_year),
     ss_combined as
  (select ss1.ca_county,
          ss1.d_year,
          ss1.store_sales as store_sales_q1,
          ss2.store_sales as store_sales_q2,
          ss3.store_sales as store_sales_q3
   from ss ss1,
        ss ss2,
        ss ss3
   where ss1.d_qoy = 1
     and ss1.d_year = 1999
     and ss1.ca_county = ss2.ca_county
     and ss2.d_qoy = 2
     and ss2.d_year = 1999
     and ss2.ca_county = ss3.ca_county
     and ss3.d_qoy = 3
     and ss3.d_year = 1999 ),
     ws_combined as
  (select ws1.ca_county,
          ws1.d_year,
          ws1.web_sales as web_sales_q1,
          ws2.web_sales as web_sales_q2,
          ws3.web_sales as web_sales_q3
   from ws ws1,
        ws ws2,
        ws ws3
   where ws1.d_qoy = 1
     and ws1.d_year = 1999
     and ws1.ca_county = ws2.ca_county
     and ws2.d_qoy = 2
     and ws2.d_year = 1999
     and ws1.ca_county = ws3.ca_county
     and ws3.d_qoy = 3
     and ws3.d_year = 1999 )
select ss_combined.ca_county,
       ss_combined.d_year,
       ws_combined.web_sales_q2 / ws_combined.web_sales_q1 as web_q1_q2_increase,
       ss_combined.store_sales_q2 / ss_combined.store_sales_q1 as store_q1_q2_increase,
       ws_combined.web_sales_q3 / ws_combined.web_sales_q2 as web_q2_q3_increase,
       ss_combined.store_sales_q3 / ss_combined.store_sales_q2 as store_q2_q3_increase
from ss_combined,
     ws_combined
where ss_combined.ca_county = ws_combined.ca_county
  and case
          when ws_combined.web_sales_q1 > 0 then ws_combined.web_sales_q2 / ws_combined.web_sales_q1
          else null
      end > case
                when ss_combined.store_sales_q1 > 0 then ss_combined.store_sales_q2 / ss_combined.store_sales_q1
                else null
            end
  and case
          when ws_combined.web_sales_q2 > 0 then ws_combined.web_sales_q3 / ws_combined.web_sales_q2
          else null
      end > case
                when ss_combined.store_sales_q2 > 0 then ss_combined.store_sales_q3 / ss_combined.store_sales_q2
                else null
            end
order by store_q2_q3_increase ;

-- =================================================================
-- Query ID: TPCDSN235
-- =================================================================
with avg_discount_cte as
  (select 1.3 * avg(cs_ext_discount_amt) as avg_discount_amt
   from catalog_sales
   join date_dim on d_date_sk = cs_sold_date_sk
   join item on cs_item_sk = i_item_sk
   where d_date between '2001-03-09' and (cast('2001-03-09' as date) + interval '90' day) )
select sum(cs_ext_discount_amt) as "excess discount amount"
from catalog_sales
join item on i_item_sk = cs_item_sk
join date_dim on d_date_sk = cs_sold_date_sk
join avg_discount_cte on cs_ext_discount_amt > avg_discount_cte.avg_discount_amt
where i_manufact_id = 722
  and d_date between '2001-03-09' and (cast('2001-03-09' as date) + interval '90' day)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN236
-- =================================================================
WITH item_cte AS
  (SELECT i_item_sk,
          i_manufact_id
   FROM item
   WHERE i_category IN ('Books') ),
     date_customer_cte AS
  (SELECT d_date_sk,
          ca_address_sk
   FROM date_dim,
        customer_address
   WHERE d_year = 2001
     AND d_moy = 3
     AND ca_gmt_offset = -5 ),
     ss AS
  (SELECT i_manufact_id,
          SUM(ss_ext_sales_price) total_sales
   FROM store_sales
   JOIN item_cte ON store_sales.ss_item_sk = item_cte.i_item_sk
   JOIN date_customer_cte ON store_sales.ss_sold_date_sk = date_customer_cte.d_date_sk
   AND store_sales.ss_addr_sk = date_customer_cte.ca_address_sk
   GROUP BY i_manufact_id),
     cs AS
  (SELECT i_manufact_id,
          SUM(cs_ext_sales_price) total_sales
   FROM catalog_sales
   JOIN item_cte ON catalog_sales.cs_item_sk = item_cte.i_item_sk
   JOIN date_customer_cte ON catalog_sales.cs_sold_date_sk = date_customer_cte.d_date_sk
   AND catalog_sales.cs_bill_addr_sk = date_customer_cte.ca_address_sk
   GROUP BY i_manufact_id),
     ws AS
  (SELECT i_manufact_id,
          SUM(ws_ext_sales_price) total_sales
   FROM web_sales
   JOIN item_cte ON web_sales.ws_item_sk = item_cte.i_item_sk
   JOIN date_customer_cte ON web_sales.ws_sold_date_sk = date_customer_cte.d_date_sk
   AND web_sales.ws_bill_addr_sk = date_customer_cte.ca_address_sk
   GROUP BY i_manufact_id)
SELECT i_manufact_id,
       SUM(total_sales) total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) tmp1
GROUP BY i_manufact_id
ORDER BY total_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN237
-- =================================================================
with sales_data_cte as
  (select ss_ticket_number,
          ss_customer_sk,
          count(*) as cnt
   from store_sales
   join date_dim on store_sales.ss_sold_date_sk = date_dim.d_date_sk
   join store on store_sales.ss_store_sk = store.s_store_sk
   join household_demographics on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
   where (date_dim.d_dom between 1 and 3
          or date_dim.d_dom between 25 and 28)
     and (household_demographics.hd_buy_potential = '1001-5000'
          or household_demographics.hd_buy_potential = '0-500')
     and household_demographics.hd_vehicle_count > 0
     and (case
              when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
              else null
          end) > 1.2
     and date_dim.d_year in (2000,
                             2001,
                             2002)
     and store.s_county in ('Williamson County')
   group by ss_ticket_number,
            ss_customer_sk)
select c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
from sales_data_cte
join customer on sales_data_cte.ss_customer_sk = customer.c_customer_sk
where cnt between 15 and 20
order by c_last_name,
         c_first_name,
         c_salutation,
         c_preferred_cust_flag desc,
         ss_ticket_number ;

-- =================================================================
-- Query ID: TPCDSN238
-- =================================================================
with store_sales_cte as
  (select ss_customer_sk
   from store_sales
   join date_dim on ss_sold_date_sk = d_date_sk
   where d_year = 1999
     and d_qoy < 4 ),
     web_sales_cte as
  (select ws_bill_customer_sk
   from web_sales
   join date_dim on ws_sold_date_sk = d_date_sk
   where d_year = 1999
     and d_qoy < 4 ),
     catalog_sales_cte as
  (select cs_ship_customer_sk
   from catalog_sales
   join date_dim on cs_sold_date_sk = d_date_sk
   where d_year = 1999
     and d_qoy < 4 )
select ca_state,
       cd_gender,
       cd_marital_status,
       cd_dep_count,
       count(*) cnt1,
       avg(cd_dep_count),
       stddev_samp(cd_dep_count),
       sum(cd_dep_count),
       cd_dep_employed_count,
       count(*) cnt2,
       avg(cd_dep_employed_count),
       stddev_samp(cd_dep_employed_count),
       sum(cd_dep_employed_count),
       cd_dep_college_count,
       count(*) cnt3,
       avg(cd_dep_college_count),
       stddev_samp(cd_dep_college_count),
       sum(cd_dep_college_count)
from customer c
join customer_address ca on c.c_current_addr_sk = ca.ca_address_sk
join customer_demographics on cd_demo_sk = c.c_current_cdemo_sk
where exists
    (select 1
     from store_sales_cte
     where c.c_customer_sk = store_sales_cte.ss_customer_sk )
  and (exists
         (select 1
          from web_sales_cte
          where c.c_customer_sk = web_sales_cte.ws_bill_customer_sk )
       or exists
         (select 1
          from catalog_sales_cte
          where c.c_customer_sk = catalog_sales_cte.cs_ship_customer_sk ))
group by ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
order by ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN239
-- =================================================================
select *
from customer ;

-- =================================================================
-- Query ID: TPCDSN240
-- =================================================================
with date_range_cte as
  (select d_date_sk
   from date_dim
   where d_date between cast('2002-03-29' as date) and (cast('2002-03-29' as date) + interval '60' day) ),
     filtered_items_cte as
  (select i_item_id,
          i_item_desc,
          i_current_price,
          i_item_sk
   from item
   where i_current_price between 29 and 59
     and i_manufact_id in (393,
                           174,
                           251,
                           445) )
select i_item_id,
       i_item_desc,
       i_current_price
from filtered_items_cte
join inventory on inv_item_sk = i_item_sk
join date_range_cte on d_date_sk = inv_date_sk
join catalog_sales on cs_item_sk = i_item_sk
where inv_quantity_on_hand between 100 and 500
group by i_item_id,
         i_item_desc,
         i_current_price
order by i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN241
-- =================================================================
with store_sales_cte as
  (select distinct c_last_name,
                   c_first_name,
                   d_date
   from store_sales,
        date_dim,
        customer
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_customer_sk = customer.c_customer_sk
     and d_month_seq between 1189 and 1189 + 11 ),
     catalog_sales_cte as
  (select distinct c_last_name,
                   c_first_name,
                   d_date
   from catalog_sales,
        date_dim,
        customer
   where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
     and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
     and d_month_seq between 1189 and 1189 + 11 ),
     web_sales_cte as
  (select distinct c_last_name,
                   c_first_name,
                   d_date
   from web_sales,
        date_dim,
        customer
   where web_sales.ws_sold_date_sk = date_dim.d_date_sk
     and web_sales.ws_bill_customer_sk = customer.c_customer_sk
     and d_month_seq between 1189 and 1189 + 11 )
select count(*)
from
  (select *
   from store_sales_cte intersect select *
   from catalog_sales_cte intersect select *
   from web_sales_cte) hot_cust
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN242
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN243
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
  and inv1.cov > 1.5
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN244
-- =================================================================
with date_filtered_sales as
  (select cs_order_number,
          cs_item_sk,
          cs_sales_price,
          coalesce(cr_refunded_cash, 0) as refunded_cash,
          cs_warehouse_sk,
          cs_sold_date_sk
   from catalog_sales
   left outer join catalog_returns on (cs_order_number = cr_order_number
                                       and cs_item_sk = cr_item_sk)
   where cs_sold_date_sk in
       (select d_date_sk
        from date_dim
        where d_date between (cast('2001-05-02' as date) - interval '30' day) and (cast('2001-05-02' as date) + interval '30' day) ) ),
     filtered_items as
  (select i_item_id,
          i_item_sk
   from item
   where i_current_price between 0.99 and 1.49 )
select w_state,
       i_item_id,
       sum(case
               when (cast(d_date as date) < cast('2001-05-02' as date)) then cs_sales_price - refunded_cash
               else 0
           end) as sales_before,
       sum(case
               when (cast(d_date as date) >= cast('2001-05-02' as date)) then cs_sales_price - refunded_cash
               else 0
           end) as sales_after
from date_filtered_sales
join filtered_items on date_filtered_sales.cs_item_sk = filtered_items.i_item_sk
join warehouse on date_filtered_sales.cs_warehouse_sk = w_warehouse_sk
join date_dim on date_filtered_sales.cs_sold_date_sk = d_date_sk
group by w_state,
         i_item_id
order by w_state,
         i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN245
-- =================================================================
with item_count_cte as
  (select i_manufact,
          count(*) as item_cnt
   from item
   where ((i_category = 'Women'
           and (i_color = 'forest'
                or i_color = 'lime')
           and (i_units = 'Pallet'
                or i_units = 'Pound')
           and (i_size = 'economy'
                or i_size = 'small'))
          or (i_category = 'Women'
              and (i_color = 'navy'
                   or i_color = 'slate')
              and (i_units = 'Gross'
                   or i_units = 'Bunch')
              and (i_size = 'extra large'
                   or i_size = 'petite'))
          or (i_category = 'Men'
              and (i_color = 'powder'
                   or i_color = 'sky')
              and (i_units = 'Dozen'
                   or i_units = 'Lb')
              and (i_size = 'N/A'
                   or i_size = 'large'))
          or (i_category = 'Men'
              and (i_color = 'maroon'
                   or i_color = 'smoke')
              and (i_units = 'Ounce'
                   or i_units = 'Case')
              and (i_size = 'economy'
                   or i_size = 'small'))
          or (i_category = 'Women'
              and (i_color = 'dark'
                   or i_color = 'aquamarine')
              and (i_units = 'Ton'
                   or i_units = 'Tbl')
              and (i_size = 'economy'
                   or i_size = 'small'))
          or (i_category = 'Women'
              and (i_color = 'frosted'
                   or i_color = 'plum')
              and (i_units = 'Dram'
                   or i_units = 'Box')
              and (i_size = 'extra large'
                   or i_size = 'petite'))
          or (i_category = 'Men'
              and (i_color = 'papaya'
                   or i_color = 'peach')
              and (i_units = 'Bundle'
                   or i_units = 'Carton')
              and (i_size = 'N/A'
                   or i_size = 'large'))
          or (i_category = 'Men'
              and (i_color = 'firebrick'
                   or i_color = 'sienna')
              and (i_units = 'Cup'
                   or i_units = 'Each')
              and (i_size = 'economy'
                   or i_size = 'small')))
   group by i_manufact)
select distinct(i_product_name)
from item i1
join item_count_cte ic on i1.i_manufact = ic.i_manufact
where i1.i_manufact_id between 704 and 744
  and ic.item_cnt > 0
order by i_product_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN246
-- =================================================================
WITH filtered_data AS
  (SELECT dt.d_year,
          item.i_category_id,
          item.i_category,
          ss_ext_sales_price
   FROM date_dim dt
   JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
   JOIN item ON store_sales.ss_item_sk = item.i_item_sk
   WHERE item.i_manager_id = 1
     AND dt.d_moy = 11
     AND dt.d_year = 1998 )
SELECT d_year,
       i_category_id,
       i_category,
       SUM(ss_ext_sales_price)
FROM filtered_data
GROUP BY d_year,
         i_category_id,
         i_category
ORDER BY SUM(ss_ext_sales_price) DESC, d_year,
                                       i_category_id,
                                       i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN247
-- =================================================================
with filtered_data as
  (select d_day_name,
          ss_sales_price,
          s_store_name,
          s_store_id
   from date_dim
   join store_sales on d_date_sk = ss_sold_date_sk
   join store on s_store_sk = ss_store_sk
   where s_gmt_offset = -5
     and d_year = 2000 )
select s_store_name,
       s_store_id,
       sum(case
               when d_day_name = 'Sunday' then ss_sales_price
               else null
           end) as sun_sales,
       sum(case
               when d_day_name = 'Monday' then ss_sales_price
               else null
           end) as mon_sales,
       sum(case
               when d_day_name = 'Tuesday' then ss_sales_price
               else null
           end) as tue_sales,
       sum(case
               when d_day_name = 'Wednesday' then ss_sales_price
               else null
           end) as wed_sales,
       sum(case
               when d_day_name = 'Thursday' then ss_sales_price
               else null
           end) as thu_sales,
       sum(case
               when d_day_name = 'Friday' then ss_sales_price
               else null
           end) as fri_sales,
       sum(case
               when d_day_name = 'Saturday' then ss_sales_price
               else null
           end) as sat_sales
from filtered_data
group by s_store_name,
         s_store_id
order by s_store_name,
         s_store_id,
         sun_sales,
         mon_sales,
         tue_sales,
         wed_sales,
         thu_sales,
         fri_sales,
         sat_sales
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN248
-- =================================================================
with avg_net_profit_cte as
  (select avg(ss_net_profit) as rank_col
   from store_sales
   where ss_store_sk = 4
     and ss_hdemo_sk is not null
   group by ss_store_sk),
     ranked_items_asc as
  (select item_sk,
          rank() over (
                       order by rank_col asc) as rnk
   from
     (select ss_item_sk as item_sk,
             avg(ss_net_profit) as rank_col
      from store_sales
      where ss_store_sk = 4
      group by ss_item_sk
      having avg(ss_net_profit) > 0.9 *
        (select rank_col
         from avg_net_profit_cte)) V1),
     ranked_items_desc as
  (select item_sk,
          rank() over (
                       order by rank_col desc) as rnk
   from
     (select ss_item_sk as item_sk,
             avg(ss_net_profit) as rank_col
      from store_sales
      where ss_store_sk = 4
      group by ss_item_sk
      having avg(ss_net_profit) > 0.9 *
        (select rank_col
         from avg_net_profit_cte)) V2)
select asceding.rnk,
       i1.i_product_name as best_performing,
       i2.i_product_name as worst_performing
from
  (select *
   from ranked_items_asc
   where rnk < 11 ) asceding,

  (select *
   from ranked_items_desc
   where rnk < 11 ) descending,
     item i1,
     item i2
where asceding.rnk = descending.rnk
  and i1.i_item_sk = asceding.item_sk
  and i2.i_item_sk = descending.item_sk
order by asceding.rnk
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN249
-- =================================================================
with item_cte as
  (select i_item_id
   from item
   where i_item_sk in (2,
                       3,
                       5,
                       7,
                       11,
                       13,
                       17,
                       19,
                       23,
                       29) )
select ca_zip,
       ca_city,
       sum(ws_sales_price)
from web_sales
join customer on ws_bill_customer_sk = c_customer_sk
join customer_address on c_current_addr_sk = ca_address_sk
join date_dim on ws_sold_date_sk = d_date_sk
join item on ws_item_sk = i_item_sk
where (substr(ca_zip, 1, 5) in ('85669',
                                '86197',
                                '88274',
                                '83405',
                                '86475',
                                '85392',
                                '85460',
                                '80348',
                                '81792')
       or i_item_id in
         (select i_item_id
          from item_cte))
  and d_qoy = 1
  and d_year = 2000
group by ca_zip,
         ca_city
order by ca_zip,
         ca_city
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN250
-- =================================================================
with precomputed_sales as
  (select ss_ticket_number,
          ss_customer_sk,
          ss_addr_sk,
          ca_city as bought_city,
          sum(ss_coupon_amt) as amt,
          sum(ss_net_profit) as profit
   from store_sales,
        date_dim,
        store,
        household_demographics,
        customer_address
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_store_sk = store.s_store_sk
     and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     and store_sales.ss_addr_sk = customer_address.ca_address_sk
     and (household_demographics.hd_dep_count = 8
          or household_demographics.hd_vehicle_count = 0)
     and date_dim.d_dow in (4,
                            0)
     and date_dim.d_year in (2000,
                             2001,
                             2002)
     and store.s_city in ('Midway',
                          'Fairview',
                          'Fairview',
                          'Midway',
                          'Fairview')
   group by ss_ticket_number,
            ss_customer_sk,
            ss_addr_sk,
            ca_city)
select c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       amt,
       profit
from precomputed_sales dn,
     customer,
     customer_address current_addr
where dn.ss_customer_sk = customer.c_customer_sk
  and customer.c_current_addr_sk = current_addr.ca_address_sk
  and current_addr.ca_city <> dn.bought_city
order by c_last_name,
         c_first_name,
         ca_city,
         bought_city,
         ss_ticket_number
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN251
-- =================================================================
with v1 as
  (select i_category,
          i_brand,
          s_store_name,
          s_company_name,
          d_year,
          d_moy,
          sum(ss_sales_price) as sum_sales,
          avg(sum(ss_sales_price)) over (partition by i_category,
                                                      i_brand,
                                                      s_store_name,
                                                      s_company_name,
                                                      d_year) as avg_monthly_sales,
                                        rank() over (partition by i_category,
                                                                  i_brand,
                                                                  s_store_name,
                                                                  s_company_name
                                                     order by d_year,
                                                              d_moy) as rn
   from item,
        store_sales,
        date_dim,
        store
   where ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and ss_store_sk = s_store_sk
     and (d_year = 2000
          or (d_year = 2000-1
              and d_moy = 12)
          or (d_year = 2000 + 1
              and d_moy = 1))
   group by i_category,
            i_brand,
            s_store_name,
            s_company_name,
            d_year,
            d_moy),
     v1_lag_lead as
  (select v1.i_category,
          v1.i_brand,
          v1.s_store_name,
          v1.s_company_name,
          v1.d_year,
          v1.avg_monthly_sales,
          v1.sum_sales,
          lag(v1.sum_sales) over (partition by v1.i_category,
                                               v1.i_brand,
                                               v1.s_store_name,
                                               v1.s_company_name
                                  order by v1.rn) as psum,
                                 lead(v1.sum_sales) over (partition by v1.i_category,
                                                                       v1.i_brand,
                                                                       v1.s_store_name,
                                                                       v1.s_company_name
                                                          order by v1.rn) as nsum
   from v1)
select s_store_name,
       s_company_name,
       d_year,
       avg_monthly_sales,
       sum_sales,
       psum,
       nsum
from v1_lag_lead
where d_year = 2000
  and avg_monthly_sales > 0
  and case
          when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
          else null
      end > 0.1
order by sum_sales - avg_monthly_sales,
         nsum
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN252
-- =================================================================
WITH customer_conditions AS
  (SELECT cd_demo_sk,
          cd_marital_status,
          cd_education_status
   FROM customer_demographics
   WHERE (cd_marital_status = 'S'
          AND cd_education_status = 'Secondary')
     OR (cd_marital_status = 'M'
         AND cd_education_status = '2 yr Degree')
     OR (cd_marital_status = 'D'
         AND cd_education_status = 'Advanced Degree') ),
     address_conditions AS
  (SELECT ca_address_sk,
          ca_state
   FROM customer_address
   WHERE ca_country = 'United States' )
SELECT SUM(ss_quantity)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN customer_conditions ON customer_conditions.cd_demo_sk = ss_cdemo_sk
JOIN address_conditions ON address_conditions.ca_address_sk = ss_addr_sk
WHERE d_year = 2001
  AND ((ss_sales_price BETWEEN 100.00 AND 150.00
        AND customer_conditions.cd_marital_status = 'S'
        AND customer_conditions.cd_education_status = 'Secondary')
       OR (ss_sales_price BETWEEN 50.00 AND 100.00
           AND customer_conditions.cd_marital_status = 'M'
           AND customer_conditions.cd_education_status = '2 yr Degree')
       OR (ss_sales_price BETWEEN 150.00 AND 200.00
           AND customer_conditions.cd_marital_status = 'D'
           AND customer_conditions.cd_education_status = 'Advanced Degree'))
  AND ((ss_net_profit BETWEEN 0 AND 2000
        AND address_conditions.ca_state IN ('ND',
                                            'NY',
                                            'SD'))
       OR (ss_net_profit BETWEEN 150 AND 3000
           AND address_conditions.ca_state IN ('MD',
                                               'GA',
                                               'KS'))
       OR (ss_net_profit BETWEEN 50 AND 25000
           AND address_conditions.ca_state IN ('CO',
                                               'MN',
                                               'NC'))) ;

-- =================================================================
-- Query ID: TPCDSN253
-- =================================================================
with web_sales_cte as
  (select ws.ws_item_sk as item,
          (cast(sum(coalesce(wr.wr_return_quantity, 0)) as decimal(15, 4)) / cast(sum(coalesce(ws.ws_quantity, 0)) as decimal(15, 4))) as return_ratio,
          (cast(sum(coalesce(wr.wr_return_amt, 0)) as decimal(15, 4)) / cast(sum(coalesce(ws.ws_net_paid, 0)) as decimal(15, 4))) as currency_ratio
   from web_sales ws
   left outer join web_returns wr on (ws.ws_order_number = wr.wr_order_number
                                      and ws.ws_item_sk = wr.wr_item_sk), date_dim
   where wr.wr_return_amt > 10000
     and ws.ws_net_profit > 1
     and ws.ws_net_paid > 0
     and ws.ws_quantity > 0
     and ws_sold_date_sk = d_date_sk
     and d_year = 1998
     and d_moy = 11
   group by ws.ws_item_sk),
     catalog_sales_cte as
  (select cs.cs_item_sk as item,
          (cast(sum(coalesce(cr.cr_return_quantity, 0)) as decimal(15, 4)) / cast(sum(coalesce(cs.cs_quantity, 0)) as decimal(15, 4))) as return_ratio,
          (cast(sum(coalesce(cr.cr_return_amount, 0)) as decimal(15, 4)) / cast(sum(coalesce(cs.cs_net_paid, 0)) as decimal(15, 4))) as currency_ratio
   from catalog_sales cs
   left outer join catalog_returns cr on (cs.cs_order_number = cr.cr_order_number
                                          and cs.cs_item_sk = cr.cr_item_sk), date_dim
   where cr.cr_return_amount > 10000
     and cs.cs_net_profit > 1
     and cs.cs_net_paid > 0
     and cs.cs_quantity > 0
     and cs_sold_date_sk = d_date_sk
     and d_year = 1998
     and d_moy = 11
   group by cs.cs_item_sk),
     store_sales_cte as
  (select sts.ss_item_sk as item,
          (cast(sum(coalesce(sr.sr_return_quantity, 0)) as decimal(15, 4)) / cast(sum(coalesce(sts.ss_quantity, 0)) as decimal(15, 4))) as return_ratio,
          (cast(sum(coalesce(sr.sr_return_amt, 0)) as decimal(15, 4)) / cast(sum(coalesce(sts.ss_net_paid, 0)) as decimal(15, 4))) as currency_ratio
   from store_sales sts
   left outer join store_returns sr on (sts.ss_ticket_number = sr.sr_ticket_number
                                        and sts.ss_item_sk = sr.sr_item_sk), date_dim
   where sr.sr_return_amt > 10000
     and sts.ss_net_profit > 1
     and sts.ss_net_paid > 0
     and sts.ss_quantity > 0
     and ss_sold_date_sk = d_date_sk
     and d_year = 1998
     and d_moy = 11
   group by sts.ss_item_sk),
     web as
  (select item,
          return_ratio,
          currency_ratio,
          rank() over (
                       order by return_ratio) as return_rank,
                      rank() over (
                                   order by currency_ratio) as currency_rank
   from web_sales_cte),
     catalog as
  (select item,
          return_ratio,
          currency_ratio,
          rank() over (
                       order by return_ratio) as return_rank,
                      rank() over (
                                   order by currency_ratio) as currency_rank
   from catalog_sales_cte), store as
  (select item,
          return_ratio,
          currency_ratio,
          rank() over (
                       order by return_ratio) as return_rank,
                      rank() over (
                                   order by currency_ratio) as currency_rank
   from store_sales_cte)
select channel,
       item,
       return_ratio,
       return_rank,
       currency_rank
from
  (select 'web' as channel,
          web.item,
          web.return_ratio,
          web.return_rank,
          web.currency_rank
   from web
   where (web.return_rank <= 10
          or web.currency_rank <= 10)
   union select 'catalog' as channel,
                catalog.item,
                catalog.return_ratio,
                catalog.return_rank,
                catalog.currency_rank
   from catalog
   where (catalog.return_rank <= 10
          or catalog.currency_rank <= 10)
   union select 'store' as channel,
                store.item,
                store.return_ratio,
                store.return_rank,
                store.currency_rank
   from store
   where (store.return_rank <= 10
          or store.currency_rank <= 10) ) as tmp
order by 1,
         4,
         5,
         2
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN254
-- =================================================================
with date_dim_2001_aug as
  (select d_date_sk
   from date_dim
   where d_year = 2001
     and d_moy = 8 )
select s_store_name,
       s_company_id,
       s_street_number,
       s_street_name,
       s_street_type,
       s_suite_number,
       s_city,
       s_county,
       s_state,
       s_zip,
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk <= 30) then 1
               else 0
           end) as "30 days",
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk > 30)
                    and (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1
               else 0
           end) as "31-60 days",
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk > 60)
                    and (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1
               else 0
           end) as "61-90 days",
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk > 90)
                    and (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1
               else 0
           end) as "91-120 days",
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk > 120) then 1
               else 0
           end) as ">120 days"
from store_sales
join store_returns on ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and ss_customer_sk = sr_customer_sk
join store on ss_store_sk = s_store_sk
join date_dim d1 on ss_sold_date_sk = d1.d_date_sk
join date_dim_2001_aug d2 on sr_returned_date_sk = d2.d_date_sk
group by s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
order by s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN255
-- =================================================================
WITH web_v1 AS
  (SELECT ws_item_sk AS item_sk,
          d_date,
          SUM(SUM(ws_sales_price)) OVER (PARTITION BY ws_item_sk
                                         ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
   FROM web_sales,
        date_dim
   WHERE ws_sold_date_sk = d_date_sk
     AND d_month_seq BETWEEN 1212 AND 1212 + 11
     AND ws_item_sk IS NOT NULL
   GROUP BY ws_item_sk,
            d_date),
     store_v1 AS
  (SELECT ss_item_sk AS item_sk,
          d_date,
          SUM(SUM(ss_sales_price)) OVER (PARTITION BY ss_item_sk
                                         ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
   FROM store_sales,
        date_dim
   WHERE ss_sold_date_sk = d_date_sk
     AND d_month_seq BETWEEN 1212 AND 1212 + 11
     AND ss_item_sk IS NOT NULL
   GROUP BY ss_item_sk,
            d_date),
     combined_sales AS
  (SELECT COALESCE(web.item_sk, store.item_sk) AS item_sk,
          COALESCE(web.d_date, store.d_date) AS d_date,
          web.cume_sales AS web_sales,
          store.cume_sales AS store_sales
   FROM web_v1 web
   FULL OUTER JOIN store_v1 store ON web.item_sk = store.item_sk
   AND web.d_date = store.d_date),
     cumulative_sales AS
  (SELECT item_sk,
          d_date,
          web_sales,
          store_sales,
          MAX(web_sales) OVER (PARTITION BY item_sk
                               ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS web_cumulative,
                              MAX(store_sales) OVER (PARTITION BY item_sk
                                                     ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS store_cumulative
   FROM combined_sales)
SELECT *
FROM cumulative_sales
WHERE web_cumulative > store_cumulative
ORDER BY item_sk,
         d_date
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN256
-- =================================================================
with filtered_date_dim as
  (select d_date_sk,
          d_year
   from date_dim
   where d_moy = 12
     and d_year = 2000 ),
     filtered_item as
  (select i_item_sk,
          i_brand_id,
          i_brand
   from item
   where i_manager_id = 1 )
select dt.d_year,
       fi.i_brand_id as brand_id,
       fi.i_brand as brand,
       sum(ss_ext_sales_price) as ext_price
from store_sales
join filtered_date_dim dt on dt.d_date_sk = store_sales.ss_sold_date_sk
join filtered_item fi on store_sales.ss_item_sk = fi.i_item_sk
group by dt.d_year,
         fi.i_brand,
         fi.i_brand_id
order by dt.d_year,
         ext_price desc,
         brand_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN257
-- =================================================================
with sales_data_cte as
  (select i_manufact_id,
          sum(ss_sales_price) as sum_sales,
          avg(sum(ss_sales_price)) over (partition by i_manufact_id) as avg_quarterly_sales
   from item,
        store_sales,
        date_dim,
        store
   where ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and ss_store_sk = s_store_sk
     and d_month_seq in (1186,
                         1186 + 1,
                         1186 + 2,
                         1186 + 3,
                         1186 + 4,
                         1186 + 5,
                         1186 + 6,
                         1186 + 7,
                         1186 + 8,
                         1186 + 9,
                         1186 + 10,
                         1186 + 11)
     and ((i_category in ('Books',
                          'Children',
                          'Electronics')
           and i_class in ('personal',
                           'portable',
                           'reference',
                           'self-help')
           and i_brand in ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9'))
          or (i_category in ('Women',
                             'Music',
                             'Men')
              and i_class in ('accessories',
                              'classical',
                              'fragrances',
                              'pants')
              and i_brand in ('amalgimporto #1',
                              'edu packscholar #1',
                              'exportiimporto #1',
                              'importoamalg #1')))
   group by i_manufact_id,
            d_qoy)
select *
from sales_data_cte
where case
          when avg_quarterly_sales > 0 then abs(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
          else null
      end > 0.1
order by avg_quarterly_sales,
         sum_sales,
         i_manufact_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN258
-- =================================================================
with my_customers as
  (select distinct c_customer_sk,
                   c_current_addr_sk
   from
     (select cs_sold_date_sk sold_date_sk,
             cs_bill_customer_sk customer_sk,
             cs_item_sk item_sk
      from catalog_sales
      union all select ws_sold_date_sk sold_date_sk,
                       ws_bill_customer_sk customer_sk,
                       ws_item_sk item_sk
      from web_sales) cs_or_ws_sales,
        item,
        date_dim,
        customer
   where sold_date_sk = d_date_sk
     and item_sk = i_item_sk
     and i_category = 'Music'
     and i_class = 'country'
     and c_customer_sk = cs_or_ws_sales.customer_sk
     and d_moy = 1
     and d_year = 1999 ),
     month_seq_bounds as
  (select distinct
     (select d_month_seq + 1
      from date_dim
      where d_year = 1999
        and d_moy = 1) as start_month_seq,

     (select d_month_seq + 3
      from date_dim
      where d_year = 1999
        and d_moy = 1) as end_month_seq),
     my_revenue as
  (select c_customer_sk,
          sum(ss_ext_sales_price) as revenue
   from my_customers,
        store_sales,
        customer_address,
        store,
        date_dim,
        month_seq_bounds
   where c_current_addr_sk = ca_address_sk
     and ca_county = s_county
     and ca_state = s_state
     and ss_sold_date_sk = d_date_sk
     and c_customer_sk = ss_customer_sk
     and d_month_seq between month_seq_bounds.start_month_seq and month_seq_bounds.end_month_seq
   group by c_customer_sk),
     segments as
  (select cast((revenue / 50) as int) as segment
   from my_revenue)
select segment,
       count(*) as num_customers,
       segment * 50 as segment_base
from segments
group by segment
order by segment,
         num_customers
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN259
-- =================================================================
with filtered_date_dim as
  (select d_date_sk
   from date_dim
   where d_moy = 11
     and d_year = 2000 ),
     filtered_item as
  (select i_brand_id,
          i_brand,
          i_item_sk
   from item
   where i_manager_id = 52 )
select fi.i_brand_id as brand_id,
       fi.i_brand as brand,
       sum(ss.ss_ext_sales_price) as ext_price
from filtered_date_dim dd
join store_sales ss on dd.d_date_sk = ss.ss_sold_date_sk
join filtered_item fi on ss.ss_item_sk = fi.i_item_sk
group by fi.i_brand,
         fi.i_brand_id
order by ext_price desc,
         fi.i_brand_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN260
-- =================================================================
WITH item_color_cte AS
  (SELECT i_item_id,
          i_item_sk
   FROM item
   WHERE i_color IN ('powder',
                     'orchid',
                     'pink') ),
     ss AS
  (SELECT i.i_item_id,
          SUM(ss_ext_sales_price) total_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer_address ON ss_addr_sk = ca_address_sk
   JOIN item_color_cte i ON ss_item_sk = i.i_item_sk
   WHERE d_year = 2000
     AND d_moy = 3
     AND ca_gmt_offset = -6
   GROUP BY i.i_item_id),
     cs AS
  (SELECT i.i_item_id,
          SUM(cs_ext_sales_price) total_sales
   FROM catalog_sales
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN customer_address ON cs_bill_addr_sk = ca_address_sk
   JOIN item_color_cte i ON cs_item_sk = i.i_item_sk
   WHERE d_year = 2000
     AND d_moy = 3
     AND ca_gmt_offset = -6
   GROUP BY i.i_item_id),
     ws AS
  (SELECT i.i_item_id,
          SUM(ws_ext_sales_price) total_sales
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
   JOIN item_color_cte i ON ws_item_sk = i.i_item_sk
   WHERE d_year = 2000
     AND d_moy = 3
     AND ca_gmt_offset = -6
   GROUP BY i.i_item_id)
SELECT i_item_id,
       SUM(total_sales) total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) tmp1
GROUP BY i_item_id
ORDER BY total_sales,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN261
-- =================================================================
with v1 as
  (select i_category,
          i_brand,
          cc_name,
          d_year,
          d_moy,
          sum(cs_sales_price) as sum_sales,
          avg(sum(cs_sales_price)) over (partition by i_category,
                                                      i_brand,
                                                      cc_name,
                                                      d_year) as avg_monthly_sales,
                                        rank() over (partition by i_category,
                                                                  i_brand,
                                                                  cc_name
                                                     order by d_year,
                                                              d_moy) as rn
   from item,
        catalog_sales,
        date_dim,
        call_center
   where cs_item_sk = i_item_sk
     and cs_sold_date_sk = d_date_sk
     and cc_call_center_sk = cs_call_center_sk
     and (d_year = 2001
          or (d_year = 2000
              and d_moy = 12)
          or (d_year = 2002
              and d_moy = 1))
   group by i_category,
            i_brand,
            cc_name,
            d_year,
            d_moy),
     v1_lag_lead as
  (select v1.i_category,
          v1.i_brand,
          v1.cc_name,
          v1.d_year,
          v1.avg_monthly_sales,
          v1.sum_sales,
          lag(v1.sum_sales) over (partition by v1.i_category,
                                               v1.i_brand,
                                               v1.cc_name
                                  order by v1.d_year,
                                           v1.d_moy) as psum,
                                 lead(v1.sum_sales) over (partition by v1.i_category,
                                                                       v1.i_brand,
                                                                       v1.cc_name
                                                          order by v1.d_year,
                                                                   v1.d_moy) as nsum
   from v1)
select *
from v1_lag_lead
where d_year = 2001
  and avg_monthly_sales > 0
  and case
          when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
          else null
      end > 0.1
order by sum_sales - avg_monthly_sales,
         avg_monthly_sales
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN262
-- =================================================================
with week_dates as
  (select d_date
   from date_dim
   where d_week_seq =
       (select d_week_seq
        from date_dim
        where d_date = '1998-11-19') ),
     ss_items as
  (select i_item_id item_id,
          sum(ss_ext_sales_price) ss_item_rev
   from store_sales,
        item,
        date_dim
   where ss_item_sk = i_item_sk
     and d_date in
       (select d_date
        from week_dates)
     and ss_sold_date_sk = d_date_sk
   group by i_item_id),
     cs_items as
  (select i_item_id item_id,
          sum(cs_ext_sales_price) cs_item_rev
   from catalog_sales,
        item,
        date_dim
   where cs_item_sk = i_item_sk
     and d_date in
       (select d_date
        from week_dates)
     and cs_sold_date_sk = d_date_sk
   group by i_item_id),
     ws_items as
  (select i_item_id item_id,
          sum(ws_ext_sales_price) ws_item_rev
   from web_sales,
        item,
        date_dim
   where ws_item_sk = i_item_sk
     and d_date in
       (select d_date
        from week_dates)
     and ws_sold_date_sk = d_date_sk
   group by i_item_id)
select ss_items.item_id,
       ss_item_rev,
       ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 ss_dev,
       cs_item_rev,
       cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 cs_dev,
       ws_item_rev,
       ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 ws_dev,
       (ss_item_rev + cs_item_rev + ws_item_rev) / 3 average
from ss_items,
     cs_items,
     ws_items
where ss_items.item_id = cs_items.item_id
  and ss_items.item_id = ws_items.item_id
  and ss_item_rev between 0.1 * cs_item_rev and 2 * cs_item_rev
  and ss_item_rev between 0.1 * ws_item_rev and 2 * ws_item_rev
  and cs_item_rev between 0.1 * ss_item_rev and 2 * ss_item_rev
  and cs_item_rev between 0.1 * ws_item_rev and 2 * ws_item_rev
  and ws_item_rev between 0.1 * ss_item_rev and 2 * ss_item_rev
  and ws_item_rev between 0.1 * cs_item_rev and 2 * cs_item_rev
order by item_id,
         ss_item_rev
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN263
-- =================================================================
with wss as
  (select d_week_seq,
          ss_store_sk,
          sum(case
                  when (d_day_name = 'Sunday') then ss_sales_price
                  else null
              end) sun_sales,
          sum(case
                  when (d_day_name = 'Monday') then ss_sales_price
                  else null
              end) mon_sales,
          sum(case
                  when (d_day_name = 'Tuesday') then ss_sales_price
                  else null
              end) tue_sales,
          sum(case
                  when (d_day_name = 'Wednesday') then ss_sales_price
                  else null
              end) wed_sales,
          sum(case
                  when (d_day_name = 'Thursday') then ss_sales_price
                  else null
              end) thu_sales,
          sum(case
                  when (d_day_name = 'Friday') then ss_sales_price
                  else null
              end) fri_sales,
          sum(case
                  when (d_day_name = 'Saturday') then ss_sales_price
                  else null
              end) sat_sales
   from store_sales,
        date_dim
   where d_date_sk = ss_sold_date_sk
   group by d_week_seq,
            ss_store_sk),
     cte_y as
  (select s_store_name s_store_name1,
          wss.d_week_seq d_week_seq1,
          s_store_id s_store_id1,
          sun_sales sun_sales1,
          mon_sales mon_sales1,
          tue_sales tue_sales1,
          wed_sales wed_sales1,
          thu_sales thu_sales1,
          fri_sales fri_sales1,
          sat_sales sat_sales1
   from wss,
        store,
        date_dim d
   where d.d_week_seq = wss.d_week_seq
     and ss_store_sk = s_store_sk
     and d_month_seq between 1195 and 1195 + 11 ),
     cte_x as
  (select s_store_name s_store_name2,
          wss.d_week_seq d_week_seq2,
          s_store_id s_store_id2,
          sun_sales sun_sales2,
          mon_sales mon_sales2,
          tue_sales tue_sales2,
          wed_sales wed_sales2,
          thu_sales thu_sales2,
          fri_sales fri_sales2,
          sat_sales sat_sales2
   from wss,
        store,
        date_dim d
   where d.d_week_seq = wss.d_week_seq
     and ss_store_sk = s_store_sk
     and d_month_seq between 1195 + 12 and 1195 + 23 )
select s_store_name1,
       s_store_id1,
       d_week_seq1,
       sun_sales1 / sun_sales2,
       mon_sales1 / mon_sales2,
       tue_sales1 / tue_sales2,
       wed_sales1 / wed_sales2,
       thu_sales1 / thu_sales2,
       fri_sales1 / fri_sales2,
       sat_sales1 / sat_sales2
from cte_y
join cte_x on s_store_id1 = s_store_id2
and d_week_seq1 = d_week_seq2 - 52
order by s_store_name1,
         s_store_id1,
         d_week_seq1
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN264
-- =================================================================
WITH item_cte AS
  (SELECT i_item_id::integer
   FROM item
   WHERE i_category IN ('Jewelry') ),
     date_cte AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_year = 2000
     AND d_moy = 10 ),
     address_cte AS
  (SELECT ca_address_sk
   FROM customer_address
   WHERE ca_gmt_offset = -5 ),
     ss AS
  (SELECT ss_item_sk AS i_item_id,
          SUM(ss_ext_sales_price) total_sales
   FROM store_sales
   JOIN item_cte ON ss_item_sk = i_item_id
   JOIN date_cte ON ss_sold_date_sk = d_date_sk
   JOIN address_cte ON ss_addr_sk = ca_address_sk
   GROUP BY ss_item_sk),
     cs AS
  (SELECT cs_item_sk AS i_item_id,
          SUM(cs_ext_sales_price) total_sales
   FROM catalog_sales
   JOIN item_cte ON cs_item_sk = i_item_id
   JOIN date_cte ON cs_sold_date_sk = d_date_sk
   JOIN address_cte ON cs_bill_addr_sk = ca_address_sk
   GROUP BY cs_item_sk),
     ws AS
  (SELECT ws_item_sk AS i_item_id,
          SUM(ws_ext_sales_price) total_sales
   FROM web_sales
   JOIN item_cte ON ws_item_sk = i_item_id
   JOIN date_cte ON ws_sold_date_sk = d_date_sk
   JOIN address_cte ON ws_bill_addr_sk = ca_address_sk
   GROUP BY ws_item_sk)
SELECT i_item_id,
       SUM(total_sales) total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) tmp1
GROUP BY i_item_id
ORDER BY i_item_id,
         total_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN265
-- =================================================================
with promotional_sales_cte as
  (select sum(ss_ext_sales_price) as promotions
   from store_sales
   join store on ss_store_sk = s_store_sk
   join promotion on ss_promo_sk = p_promo_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   join customer on ss_customer_sk = c_customer_sk
   join customer_address on ca_address_sk = c_current_addr_sk
   join item on ss_item_sk = i_item_sk
   where ca_gmt_offset = -5
     and i_category = 'Home'
     and (p_channel_dmail = 'Y'
          or p_channel_email = 'Y'
          or p_channel_tv = 'Y')
     and s_gmt_offset = -5
     and d_year = 2000
     and d_moy = 12 ),
     all_sales_cte as
  (select sum(ss_ext_sales_price) as total
   from store_sales
   join store on ss_store_sk = s_store_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   join customer on ss_customer_sk = c_customer_sk
   join customer_address on ca_address_sk = c_current_addr_sk
   join item on ss_item_sk = i_item_sk
   where ca_gmt_offset = -5
     and i_category = 'Home'
     and s_gmt_offset = -5
     and d_year = 2000
     and d_moy = 12 )
select promotional_sales_cte.promotions,
       all_sales_cte.total,
       cast(promotional_sales_cte.promotions as decimal(15, 4)) / cast(all_sales_cte.total as decimal(15, 4)) * 100 as percentage
from promotional_sales_cte,
     all_sales_cte
order by promotional_sales_cte.promotions,
         all_sales_cte.total
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN266
-- =================================================================
with date_filtered as
  (select ws_ship_date_sk,
          ws_sold_date_sk,
          ws_warehouse_sk,
          ws_ship_mode_sk,
          ws_web_site_sk
   from web_sales
   join date_dim on ws_ship_date_sk = d_date_sk
   where d_month_seq between 1223 and 1223 + 11 )
select substr(w_warehouse_name, 1, 20),
       sm_type,
       web_name,
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk <= 30) then 1
               else 0
           end) as "30 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 30)
                    and (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1
               else 0
           end) as "31-60 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 60)
                    and (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1
               else 0
           end) as "61-90 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 90)
                    and (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1
               else 0
           end) as "91-120 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1
               else 0
           end) as ">120 days"
from date_filtered
join warehouse on date_filtered.ws_warehouse_sk = w_warehouse_sk
join ship_mode on date_filtered.ws_ship_mode_sk = sm_ship_mode_sk
join web_site on date_filtered.ws_web_site_sk = web_site_sk
group by substr(w_warehouse_name, 1, 20),
         sm_type,
         web_name
order by substr(w_warehouse_name, 1, 20),
         sm_type,
         web_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN267
-- =================================================================
with sales_data_cte as
  (select i_manager_id,
          sum(ss_sales_price) as sum_sales,
          avg(sum(ss_sales_price)) over (partition by i_manager_id) as avg_monthly_sales
   from item,
        store_sales,
        date_dim,
        store
   where ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and ss_store_sk = s_store_sk
     and d_month_seq in (1222,
                         1222 + 1,
                         1222 + 2,
                         1222 + 3,
                         1222 + 4,
                         1222 + 5,
                         1222 + 6,
                         1222 + 7,
                         1222 + 8,
                         1222 + 9,
                         1222 + 10,
                         1222 + 11)
     and ((i_category in ('Books',
                          'Children',
                          'Electronics')
           and i_class in ('personal',
                           'portable',
                           'reference',
                           'self-help')
           and i_brand in ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9'))
          or (i_category in ('Women',
                             'Music',
                             'Men')
              and i_class in ('accessories',
                              'classical',
                              'fragrances',
                              'pants')
              and i_brand in ('amalgimporto #1',
                              'edu packscholar #1',
                              'exportiimporto #1',
                              'importoamalg #1')))
   group by i_manager_id,
            d_moy)
select *
from sales_data_cte
where case
          when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
          else null
      end > 0.1
order by i_manager_id,
         avg_monthly_sales,
         sum_sales
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN268
-- =================================================================
WITH cs_ui AS
  (SELECT cs_item_sk,
          SUM(cs_ext_list_price) AS sale,
          SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
   FROM catalog_sales
   JOIN catalog_returns ON cs_item_sk = cr_item_sk
   AND cs_order_number = cr_order_number
   GROUP BY cs_item_sk
   HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)),
     cross_sales AS
  (SELECT i_product_name AS product_name,
          i_item_sk AS item_sk,
          s_store_name AS store_name,
          s_zip AS store_zip,
          ad1.ca_street_number AS b_street_number,
          ad1.ca_street_name AS b_street_name,
          ad1.ca_city AS b_city,
          ad1.ca_zip AS b_zip,
          ad2.ca_street_number AS c_street_number,
          ad2.ca_street_name AS c_street_name,
          ad2.ca_city AS c_city,
          ad2.ca_zip AS c_zip,
          d1.d_year AS syear,
          d2.d_year AS fsyear,
          COUNT(*) AS cnt,
          SUM(ss_wholesale_cost) AS s1,
          SUM(ss_list_price) AS s2,
          SUM(ss_coupon_amt) AS s3
   FROM store_sales
   JOIN store_returns ON ss_item_sk = sr_item_sk
   AND ss_ticket_number = sr_ticket_number
   JOIN cs_ui ON ss_item_sk = cs_ui.cs_item_sk
   JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
   JOIN date_dim d2 ON ss_sold_date_sk = d2.d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   JOIN customer ON ss_customer_sk = c_customer_sk
   JOIN customer_demographics cd1 ON ss_cdemo_sk = cd1.cd_demo_sk
   JOIN customer_demographics cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk
   JOIN promotion ON ss_promo_sk = p_promo_sk
   JOIN household_demographics hd1 ON ss_hdemo_sk = hd1.hd_demo_sk
   JOIN household_demographics hd2 ON c_current_hdemo_sk = hd2.hd_demo_sk
   JOIN customer_address ad1 ON ss_addr_sk = ad1.ca_address_sk
   JOIN customer_address ad2 ON c_current_addr_sk = ad2.ca_address_sk
   JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
   JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE cd1.cd_marital_status <> cd2.cd_marital_status
     AND i_color IN ('orange',
                     'lace',
                     'lawn',
                     'misty',
                     'blush',
                     'pink')
     AND i_current_price BETWEEN 49 AND 58
   GROUP BY i_product_name,
            i_item_sk,
            s_store_name,
            s_zip,
            ad1.ca_street_number,
            ad1.ca_street_name,
            ad1.ca_city,
            ad1.ca_zip,
            ad2.ca_street_number,
            ad2.ca_street_name,
            ad2.ca_city,
            ad2.ca_zip,
            d1.d_year,
            d2.d_year)
SELECT cs1.product_name,
       cs1.store_name,
       cs1.store_zip,
       cs1.b_street_number,
       cs1.b_street_name,
       cs1.b_city,
       cs1.b_zip,
       cs1.c_street_number,
       cs1.c_street_name,
       cs1.c_city,
       cs1.c_zip,
       cs1.syear,
       cs1.cnt,
       cs1.s1 AS s11,
       cs1.s2 AS s21,
       cs1.s3 AS s31,
       cs2.s1 AS s12,
       cs2.s2 AS s22,
       cs2.s3 AS s32,
       cs2.syear,
       cs2.cnt
FROM cross_sales cs1
JOIN cross_sales cs2 ON cs1.item_sk = cs2.item_sk
WHERE cs1.syear = 1999
  AND cs2.syear = 2000
  AND cs2.cnt <= cs1.cnt
  AND cs1.store_name = cs2.store_name
  AND cs1.store_zip = cs2.store_zip
ORDER BY cs1.product_name,
         cs1.store_name,
         cs2.cnt,
         cs1.s1,
         cs2.s1 ;

-- =================================================================
-- Query ID: TPCDSN269
-- =================================================================
with sa as
  (select ss_store_sk,
          ss_item_sk,
          sum(ss_sales_price) as revenue
   from store_sales,
        date_dim
   where ss_sold_date_sk = d_date_sk
     and d_month_seq between 1176 and 1176 + 11
   group by ss_store_sk,
            ss_item_sk),
     sb as
  (select ss_store_sk,
          avg(revenue) as ave
   from sa
   group by ss_store_sk)
select s_store_name,
       i_item_desc,
       sc.revenue,
       i_current_price,
       i_wholesale_cost,
       i_brand
from store,
     item,
     sb,
     sa as sc
where sb.ss_store_sk = sc.ss_store_sk
  and sc.revenue <= 0.1 * sb.ave
  and s_store_sk = sc.ss_store_sk
  and i_item_sk = sc.ss_item_sk
order by s_store_name,
         i_item_desc
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN270
-- =================================================================
with sales_data as
  (select w_warehouse_name,
          w_warehouse_sq_ft,
          w_city,
          w_county,
          w_state,
          w_country,
          'ORIENTAL' || ',' || 'BOXBUNDLES' as ship_carriers,
          d_year as year,
          sum(case
                  when d_moy = 1 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as jan_sales,
          sum(case
                  when d_moy = 2 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as feb_sales,
          sum(case
                  when d_moy = 3 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as mar_sales,
          sum(case
                  when d_moy = 4 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as apr_sales,
          sum(case
                  when d_moy = 5 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as may_sales,
          sum(case
                  when d_moy = 6 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as jun_sales,
          sum(case
                  when d_moy = 7 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as jul_sales,
          sum(case
                  when d_moy = 8 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as aug_sales,
          sum(case
                  when d_moy = 9 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as sep_sales,
          sum(case
                  when d_moy = 10 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as oct_sales,
          sum(case
                  when d_moy = 11 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as nov_sales,
          sum(case
                  when d_moy = 12 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as dec_sales,
          sum(case
                  when d_moy = 1 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as jan_net,
          sum(case
                  when d_moy = 2 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as feb_net,
          sum(case
                  when d_moy = 3 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as mar_net,
          sum(case
                  when d_moy = 4 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as apr_net,
          sum(case
                  when d_moy = 5 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as may_net,
          sum(case
                  when d_moy = 6 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as jun_net,
          sum(case
                  when d_moy = 7 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as jul_net,
          sum(case
                  when d_moy = 8 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as aug_net,
          sum(case
                  when d_moy = 9 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as sep_net,
          sum(case
                  when d_moy = 10 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as oct_net,
          sum(case
                  when d_moy = 11 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as nov_net,
          sum(case
                  when d_moy = 12 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as dec_net
   from web_sales,
        warehouse,
        date_dim,
        time_dim,
        ship_mode
   where ws_warehouse_sk = w_warehouse_sk
     and ws_sold_date_sk = d_date_sk
     and ws_sold_time_sk = t_time_sk
     and ws_ship_mode_sk = sm_ship_mode_sk
     and d_year = 2001
     and t_time between 42970 and 42970 + 28800
     and sm_carrier in ('ORIENTAL',
                        'BOXBUNDLES')
   group by w_warehouse_name,
            w_warehouse_sq_ft,
            w_city,
            w_county,
            w_state,
            w_country,
            d_year
   union all select w_warehouse_name,
                    w_warehouse_sq_ft,
                    w_city,
                    w_county,
                    w_state,
                    w_country,
                    'ORIENTAL' || ',' || 'BOXBUNDLES' as ship_carriers,
                    d_year as year,
                    sum(case
                            when d_moy = 1 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as jan_sales,
                    sum(case
                            when d_moy = 2 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as feb_sales,
                    sum(case
                            when d_moy = 3 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as mar_sales,
                    sum(case
                            when d_moy = 4 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as apr_sales,
                    sum(case
                            when d_moy = 5 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as may_sales,
                    sum(case
                            when d_moy = 6 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as jun_sales,
                    sum(case
                            when d_moy = 7 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as jul_sales,
                    sum(case
                            when d_moy = 8 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as aug_sales,
                    sum(case
                            when d_moy = 9 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as sep_sales,
                    sum(case
                            when d_moy = 10 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as oct_sales,
                    sum(case
                            when d_moy = 11 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as nov_sales,
                    sum(case
                            when d_moy = 12 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as dec_sales,
                    sum(case
                            when d_moy = 1 then cs_net_paid * cs_quantity
                            else 0
                        end) as jan_net,
                    sum(case
                            when d_moy = 2 then cs_net_paid * cs_quantity
                            else 0
                        end) as feb_net,
                    sum(case
                            when d_moy = 3 then cs_net_paid * cs_quantity
                            else 0
                        end) as mar_net,
                    sum(case
                            when d_moy = 4 then cs_net_paid * cs_quantity
                            else 0
                        end) as apr_net,
                    sum(case
                            when d_moy = 5 then cs_net_paid * cs_quantity
                            else 0
                        end) as may_net,
                    sum(case
                            when d_moy = 6 then cs_net_paid * cs_quantity
                            else 0
                        end) as jun_net,
                    sum(case
                            when d_moy = 7 then cs_net_paid * cs_quantity
                            else 0
                        end) as jul_net,
                    sum(case
                            when d_moy = 8 then cs_net_paid * cs_quantity
                            else 0
                        end) as aug_net,
                    sum(case
                            when d_moy = 9 then cs_net_paid * cs_quantity
                            else 0
                        end) as sep_net,
                    sum(case
                            when d_moy = 10 then cs_net_paid * cs_quantity
                            else 0
                        end) as oct_net,
                    sum(case
                            when d_moy = 11 then cs_net_paid * cs_quantity
                            else 0
                        end) as nov_net,
                    sum(case
                            when d_moy = 12 then cs_net_paid * cs_quantity
                            else 0
                        end) as dec_net
   from catalog_sales,
        warehouse,
        date_dim,
        time_dim,
        ship_mode
   where cs_warehouse_sk = w_warehouse_sk
     and cs_sold_date_sk = d_date_sk
     and cs_sold_time_sk = t_time_sk
     and cs_ship_mode_sk = sm_ship_mode_sk
     and d_year = 2001
     and t_time between 42970 and 42970 + 28800
     and sm_carrier in ('ORIENTAL',
                        'BOXBUNDLES')
   group by w_warehouse_name,
            w_warehouse_sq_ft,
            w_city,
            w_county,
            w_state,
            w_country,
            d_year)
select w_warehouse_name,
       w_warehouse_sq_ft,
       w_city,
       w_county,
       w_state,
       w_country,
       ship_carriers,
       year,
       sum(jan_sales) as jan_sales,
       sum(feb_sales) as feb_sales,
       sum(mar_sales) as mar_sales,
       sum(apr_sales) as apr_sales,
       sum(may_sales) as may_sales,
       sum(jun_sales) as jun_sales,
       sum(jul_sales) as jul_sales,
       sum(aug_sales) as aug_sales,
       sum(sep_sales) as sep_sales,
       sum(oct_sales) as oct_sales,
       sum(nov_sales) as nov_sales,
       sum(dec_sales) as dec_sales,
       sum(jan_sales / w_warehouse_sq_ft) as jan_sales_per_sq_foot,
       sum(feb_sales / w_warehouse_sq_ft) as feb_sales_per_sq_foot,
       sum(mar_sales / w_warehouse_sq_ft) as mar_sales_per_sq_foot,
       sum(apr_sales / w_warehouse_sq_ft) as apr_sales_per_sq_foot,
       sum(may_sales / w_warehouse_sq_ft) as may_sales_per_sq_foot,
       sum(jun_sales / w_warehouse_sq_ft) as jun_sales_per_sq_foot,
       sum(jul_sales / w_warehouse_sq_ft) as jul_sales_per_sq_foot,
       sum(aug_sales / w_warehouse_sq_ft) as aug_sales_per_sq_foot,
       sum(sep_sales / w_warehouse_sq_ft) as sep_sales_per_sq_foot,
       sum(oct_sales / w_warehouse_sq_ft) as oct_sales_per_sq_foot,
       sum(nov_sales / w_warehouse_sq_ft) as nov_sales_per_sq_foot,
       sum(dec_sales / w_warehouse_sq_ft) as dec_sales_per_sq_foot,
       sum(jan_net) as jan_net,
       sum(feb_net) as feb_net,
       sum(mar_net) as mar_net,
       sum(apr_net) as apr_net,
       sum(may_net) as may_net,
       sum(jun_net) as jun_net,
       sum(jul_net) as jul_net,
       sum(aug_net) as aug_net,
       sum(sep_net) as sep_net,
       sum(oct_net) as oct_net,
       sum(nov_net) as nov_net,
       sum(dec_net) as dec_net
from sales_data
group by w_warehouse_name,
         w_warehouse_sq_ft,
         w_city,
         w_county,
         w_state,
         w_country,
         ship_carriers,
         year
order by w_warehouse_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN271
-- =================================================================
with sales_cte as
  (select i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sum(coalesce(ss_sales_price * ss_quantity, 0)) as sumsales
   from store_sales,
        date_dim,
        store,
        item
   where ss_sold_date_sk = d_date_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and d_month_seq between 1217 and 1217 + 11
   group by rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)),
     ranked_sales as
  (select i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sumsales,
          rank() over (partition by i_category
                       order by sumsales desc) as rk
   from sales_cte)
select *
from ranked_sales
where rk <= 100
order by i_category,
         i_class,
         i_brand,
         i_product_name,
         d_year,
         d_qoy,
         d_moy,
         s_store_id,
         sumsales,
         rk
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN272
-- =================================================================
with sales_data as
  (select ss_ticket_number,
          ss_customer_sk,
          ca_city as bought_city,
          sum(ss_ext_sales_price) as extended_price,
          sum(ss_ext_list_price) as list_price,
          sum(ss_ext_tax) as extended_tax
   from store_sales,
        date_dim,
        store,
        household_demographics,
        customer_address
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_store_sk = store.s_store_sk
     and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     and store_sales.ss_addr_sk = customer_address.ca_address_sk
     and date_dim.d_dom between 1 and 2
     and (household_demographics.hd_dep_count = 3
          or household_demographics.hd_vehicle_count = 4)
     and date_dim.d_year in (1998,
                             1998 + 1,
                             1998 + 2)
     and store.s_city in ('Fairview',
                          'Midway')
   group by ss_ticket_number,
            ss_customer_sk,
            ss_addr_sk,
            ca_city)
select c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       extended_price,
       extended_tax,
       list_price
from sales_data dn,
     customer,
     customer_address current_addr
where dn.ss_customer_sk = customer.c_customer_sk
  and customer.c_current_addr_sk = current_addr.ca_address_sk
  and current_addr.ca_city <> dn.bought_city
order by c_last_name,
         ss_ticket_number
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN273
-- =================================================================
with store_sales_cte as
  (select ss_customer_sk
   from store_sales,
        date_dim
   where ss_sold_date_sk = d_date_sk
     and d_year = 2002
     and d_moy between 1 and 1 + 2 ),
     web_sales_cte as
  (select ws_bill_customer_sk
   from web_sales,
        date_dim
   where ws_sold_date_sk = d_date_sk
     and d_year = 2002
     and d_moy between 1 and 1 + 2 ),
     catalog_sales_cte as
  (select cs_ship_customer_sk
   from catalog_sales,
        date_dim
   where cs_sold_date_sk = d_date_sk
     and d_year = 2002
     and d_moy between 1 and 1 + 2 )
select cd_gender,
       cd_marital_status,
       cd_education_status,
       count(*) cnt1,
       cd_purchase_estimate,
       count(*) cnt2,
       cd_credit_rating,
       count(*) cnt3
from customer c,
     customer_address ca,
     customer_demographics
where c.c_current_addr_sk = ca.ca_address_sk
  and ca_state in ('IL',
                   'TX',
                   'ME')
  and cd_demo_sk = c.c_current_cdemo_sk
  and exists
    (select 1
     from store_sales_cte
     where c.c_customer_sk = store_sales_cte.ss_customer_sk )
  and not exists
    (select 1
     from web_sales_cte
     where c.c_customer_sk = web_sales_cte.ws_bill_customer_sk )
  and not exists
    (select 1
     from catalog_sales_cte
     where c.c_customer_sk = catalog_sales_cte.cs_ship_customer_sk )
group by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
order by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN274
-- =================================================================
select *
from customer ;

-- =================================================================
-- Query ID: TPCDSN275
-- =================================================================
with sales_data_cte as
  (select ws_ext_sales_price as ext_price,
          ws_sold_date_sk as sold_date_sk,
          ws_item_sk as sold_item_sk,
          ws_sold_time_sk as time_sk
   from web_sales,
        date_dim
   where d_date_sk = ws_sold_date_sk
     and d_moy = 12
     and d_year = 2002
   union all select cs_ext_sales_price as ext_price,
                    cs_sold_date_sk as sold_date_sk,
                    cs_item_sk as sold_item_sk,
                    cs_sold_time_sk as time_sk
   from catalog_sales,
        date_dim
   where d_date_sk = cs_sold_date_sk
     and d_moy = 12
     and d_year = 2002
   union all select ss_ext_sales_price as ext_price,
                    ss_sold_date_sk as sold_date_sk,
                    ss_item_sk as sold_item_sk,
                    ss_sold_time_sk as time_sk
   from store_sales,
        date_dim
   where d_date_sk = ss_sold_date_sk
     and d_moy = 12
     and d_year = 2002 )
select i_brand_id as brand_id,
       i_brand as brand,
       t_hour,
       t_minute,
       sum(ext_price) as ext_price
from item,
     sales_data_cte,
     time_dim
where sold_item_sk = i_item_sk
  and i_manager_id = 1
  and time_sk = t_time_sk
  and (t_meal_time = 'breakfast'
       or t_meal_time = 'dinner')
group by i_brand,
         i_brand_id,
         t_hour,
         t_minute
order by ext_price desc,
         i_brand_id ;

-- =================================================================
-- Query ID: TPCDSN276
-- =================================================================
WITH date_dim_cte AS
  (SELECT d_date_sk,
          d_week_seq,
          d_year,
          d_date
   FROM date_dim),
     promo_cte AS
  (SELECT cs_item_sk,
          cs_order_number,
          cs_promo_sk,
          p_promo_sk
   FROM catalog_sales
   LEFT OUTER JOIN promotion ON (cs_promo_sk = p_promo_sk)),
     catalog_returns_cte AS
  (SELECT cr_item_sk,
          cr_order_number
   FROM catalog_returns),
     inventory_cte AS
  (SELECT inv_item_sk,
          inv_warehouse_sk,
          inv_date_sk,
          inv_quantity_on_hand
   FROM inventory),
     warehouse_cte AS
  (SELECT w_warehouse_sk,
          w_warehouse_name
   FROM warehouse),
     item_cte AS
  (SELECT i_item_sk,
          i_item_desc
   FROM item),
     customer_demographics_cte AS
  (SELECT cd_demo_sk,
          cd_marital_status
   FROM customer_demographics),
     household_demographics_cte AS
  (SELECT hd_demo_sk,
          hd_buy_potential
   FROM household_demographics)
SELECT i_item_desc,
       w_warehouse_name,
       d1.d_week_seq,
       SUM(CASE
               WHEN promo_cte.p_promo_sk IS NULL THEN 1
               ELSE 0
           END) AS no_promo,
       SUM(CASE
               WHEN promo_cte.p_promo_sk IS NOT NULL THEN 1
               ELSE 0
           END) AS promo,
       COUNT(*) AS total_cnt
FROM catalog_sales
JOIN inventory_cte ON (catalog_sales.cs_item_sk = inventory_cte.inv_item_sk)
JOIN warehouse_cte ON (warehouse_cte.w_warehouse_sk = inventory_cte.inv_warehouse_sk)
JOIN item_cte ON (item_cte.i_item_sk = catalog_sales.cs_item_sk)
JOIN customer_demographics_cte ON (catalog_sales.cs_bill_cdemo_sk = customer_demographics_cte.cd_demo_sk)
JOIN household_demographics_cte ON (catalog_sales.cs_bill_hdemo_sk = household_demographics_cte.hd_demo_sk)
JOIN date_dim_cte d1 ON (catalog_sales.cs_sold_date_sk = d1.d_date_sk)
JOIN date_dim_cte d2 ON (inventory_cte.inv_date_sk = d2.d_date_sk)
JOIN date_dim_cte d3 ON (catalog_sales.cs_ship_date_sk = d3.d_date_sk)
LEFT OUTER JOIN promo_cte ON (catalog_sales.cs_item_sk = promo_cte.cs_item_sk
                              AND catalog_sales.cs_order_number = promo_cte.cs_order_number)
LEFT OUTER JOIN catalog_returns_cte ON (catalog_returns_cte.cr_item_sk = catalog_sales.cs_item_sk
                                        AND catalog_returns_cte.cr_order_number = catalog_sales.cs_order_number)
WHERE d1.d_week_seq = d2.d_week_seq
  AND inventory_cte.inv_quantity_on_hand < catalog_sales.cs_quantity
  AND d3.d_date > d1.d_date
  AND household_demographics_cte.hd_buy_potential = '1001-5000'
  AND d1.d_year = 2002
  AND customer_demographics_cte.cd_marital_status = 'W'
GROUP BY i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
ORDER BY total_cnt DESC,
         i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN277
-- =================================================================
with sales_data_cte as
  (select ss_ticket_number,
          ss_customer_sk,
          count(*) as cnt
   from store_sales,
        date_dim,
        store,
        household_demographics
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_store_sk = store.s_store_sk
     and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     and date_dim.d_dom between 1 and 2
     and (household_demographics.hd_buy_potential = '1001-5000'
          or household_demographics.hd_buy_potential = '5001-10000')
     and household_demographics.hd_vehicle_count > 0
     and case
             when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
             else null
         end > 1
     and date_dim.d_year in (2000,
                             2001,
                             2002)
     and store.s_county in ('Williamson County',
                            'Williamson County',
                            'Williamson County',
                            'Williamson County')
   group by ss_ticket_number,
            ss_customer_sk)
select c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
from sales_data_cte dj,
     customer
where dj.ss_customer_sk = customer.c_customer_sk
  and cnt between 1 and 5
order by cnt desc,
         c_last_name asc ;

-- =================================================================
-- Query ID: TPCDSN278
-- =================================================================
with store_sales_cte as
  (select c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          d_year as year,
          max(ss_net_paid) year_total
   from customer,
        store_sales,
        date_dim
   where c_customer_sk = ss_customer_sk
     and ss_sold_date_sk = d_date_sk
     and d_year in (1999,
                    2000)
   group by c_customer_id,
            c_first_name,
            c_last_name,
            d_year),
     web_sales_cte as
  (select c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          d_year as year,
          max(ws_net_paid) year_total
   from customer,
        web_sales,
        date_dim
   where c_customer_sk = ws_bill_customer_sk
     and ws_sold_date_sk = d_date_sk
     and d_year in (1999,
                    2000)
   group by c_customer_id,
            c_first_name,
            c_last_name,
            d_year),
     year_total as
  (select customer_id,
          customer_first_name,
          customer_last_name,
          year,
          year_total,
          's' as sale_type
   from store_sales_cte
   union all select customer_id,
                    customer_first_name,
                    customer_last_name,
                    year,
                    year_total,
                    'w' as sale_type
   from web_sales_cte)
select t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name
from year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.year = 1999
  and t_s_secyear.year = 2000
  and t_w_firstyear.year = 1999
  and t_w_secyear.year = 2000
  and t_s_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case
          when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total
          else null
      end >= case
                 when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total
                 else null
             end
order by 1,
         3,
         2
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN279
-- =================================================================
WITH sales_detail AS
  (SELECT d_year,
          i_brand_id,
          i_class_id,
          i_category_id,
          i_manufact_id,
          cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
          cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt
   FROM catalog_sales
   JOIN item ON i_item_sk = cs_item_sk
   JOIN date_dim ON d_date_sk = cs_sold_date_sk
   LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number
                                 AND cs_item_sk = cr_item_sk)
   WHERE i_category = 'Sports'
   UNION SELECT d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,
                ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt
   FROM store_sales
   JOIN item ON i_item_sk = ss_item_sk
   JOIN date_dim ON d_date_sk = ss_sold_date_sk
   LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number
                               AND ss_item_sk = sr_item_sk)
   WHERE i_category = 'Sports'
   UNION SELECT d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,
                ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt
   FROM web_sales
   JOIN item ON i_item_sk = ws_item_sk
   JOIN date_dim ON d_date_sk = ws_sold_date_sk
   LEFT JOIN web_returns ON (ws_order_number = wr_order_number
                             AND ws_item_sk = wr_item_sk)
   WHERE i_category = 'Sports' ),
     all_sales AS
  (SELECT d_year,
          i_brand_id,
          i_class_id,
          i_category_id,
          i_manufact_id,
          SUM(sales_cnt) AS sales_cnt,
          SUM(sales_amt) AS sales_amt
   FROM sales_detail
   GROUP BY d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id)
SELECT prev_yr.d_year AS prev_year,
       curr_yr.d_year AS year,
       curr_yr.i_brand_id,
       curr_yr.i_class_id,
       curr_yr.i_category_id,
       curr_yr.i_manufact_id,
       prev_yr.sales_cnt AS prev_yr_cnt,
       curr_yr.sales_cnt AS curr_yr_cnt,
       curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
       curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM all_sales curr_yr
JOIN all_sales prev_yr ON curr_yr.i_brand_id = prev_yr.i_brand_id
AND curr_yr.i_class_id = prev_yr.i_class_id
AND curr_yr.i_category_id = prev_yr.i_category_id
AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
AND prev_yr.d_year = curr_yr.d_year - 1
WHERE curr_yr.d_year = 2002
  AND CAST(curr_yr.sales_cnt AS DECIMAL(17, 2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17, 2)) < 0.9
ORDER BY sales_cnt_diff,
         sales_amt_diff
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN280
-- =================================================================
with store_sales_cte as
  (SELECT 'store' as channel,
          'ss_customer_sk' col_name,
                           d_year,
                           d_qoy,
                           i_category,
                           ss_ext_sales_price ext_sales_price
   FROM store_sales,
        item,
        date_dim
   WHERE ss_customer_sk IS not NULL
     AND ss_sold_date_sk = d_date_sk
     AND ss_item_sk = i_item_sk ),
     web_sales_cte as
  (SELECT 'web' as channel,
          'ws_promo_sk' col_name,
                        d_year,
                        d_qoy,
                        i_category,
                        ws_ext_sales_price ext_sales_price
   FROM web_sales,
        item,
        date_dim
   WHERE ws_promo_sk IS not NULL
     AND ws_sold_date_sk = d_date_sk
     AND ws_item_sk = i_item_sk ),
     catalog_sales_cte as
  (SELECT 'catalog' as channel,
          'cs_bill_customer_sk' col_name,
                                d_year,
                                d_qoy,
                                i_category,
                                cs_ext_sales_price ext_sales_price
   FROM catalog_sales,
        item,
        date_dim
   WHERE cs_bill_customer_sk IS not NULL
     AND cs_sold_date_sk = d_date_sk
     AND cs_item_sk = i_item_sk )
select channel,
       col_name,
       d_year,
       d_qoy,
       i_category,
       COUNT(*) sales_cnt,
       SUM(ext_sales_price) sales_amt
from
  (select *
   from store_sales_cte
   union all select *
   from web_sales_cte
   union all select *
   from catalog_sales_cte) foo
group by channel,
         col_name,
         d_year,
         d_qoy,
         i_category
order by channel,
         col_name,
         d_year,
         d_qoy,
         i_category
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN281
-- =================================================================
with date_range as
  (select d_date_sk
   from date_dim
   where d_date between cast('2000-08-10' as date) and (cast('2000-08-10' as date) + interval '30' day) ),
     ss as
  (select s_store_sk,
          sum(ss_ext_sales_price) as sales,
          sum(ss_net_profit) as profit
   from store_sales
   join date_range on ss_sold_date_sk = d_date_sk
   join store on ss_store_sk = s_store_sk
   group by s_store_sk),
     sr as
  (select s_store_sk,
          sum(sr_return_amt) as returns,
          sum(sr_net_loss) as profit_loss
   from store_returns
   join date_range on sr_returned_date_sk = d_date_sk
   join store on sr_store_sk = s_store_sk
   group by s_store_sk),
     cs as
  (select cs_call_center_sk,
          sum(cs_ext_sales_price) as sales,
          sum(cs_net_profit) as profit
   from catalog_sales
   join date_range on cs_sold_date_sk = d_date_sk
   group by cs_call_center_sk),
     cr as
  (select cr_call_center_sk,
          sum(cr_return_amount) as returns,
          sum(cr_net_loss) as profit_loss
   from catalog_returns
   join date_range on cr_returned_date_sk = d_date_sk
   group by cr_call_center_sk),
     ws as
  (select wp_web_page_sk,
          sum(ws_ext_sales_price) as sales,
          sum(ws_net_profit) as profit
   from web_sales
   join date_range on ws_sold_date_sk = d_date_sk
   join web_page on ws_web_page_sk = wp_web_page_sk
   group by wp_web_page_sk),
     wr as
  (select wp_web_page_sk,
          sum(wr_return_amt) as returns,
          sum(wr_net_loss) as profit_loss
   from web_returns
   join date_range on wr_returned_date_sk = d_date_sk
   join web_page on wr_web_page_sk = wp_web_page_sk
   group by wp_web_page_sk)
select channel,
       id,
       sum(sales) as sales,
       sum(returns) as returns,
       sum(profit) as profit
from
  (select 'store channel' as channel,
          ss.s_store_sk as id,
          sales,
          coalesce(returns, 0) as returns,
          (profit - coalesce(profit_loss, 0)) as profit
   from ss
   left join sr on ss.s_store_sk = sr.s_store_sk
   union all select 'catalog channel' as channel,
                    cs_call_center_sk as id,
                    sales,
                    returns,
                    (profit - profit_loss) as profit
   from cs
   join cr on cs.cs_call_center_sk = cr.cr_call_center_sk
   union all select 'web channel' as channel,
                    ws.wp_web_page_sk as id,
                    sales,
                    coalesce(returns, 0) as returns,
                    (profit - coalesce(profit_loss, 0)) as profit
   from ws
   left join wr on ws.wp_web_page_sk = wr.wp_web_page_sk) x
group by rollup (channel,
                 id)
order by channel,
         id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN282
-- =================================================================
with ws_precomputed as
  (select d_year AS ws_sold_year,
          ws_item_sk,
          ws_bill_customer_sk ws_customer_sk,
          sum(ws_quantity) ws_qty,
          sum(ws_wholesale_cost) ws_wc,
          sum(ws_sales_price) ws_sp
   from web_sales
   left join web_returns on wr_order_number = ws_order_number
   and ws_item_sk = wr_item_sk
   join date_dim on ws_sold_date_sk = d_date_sk
   where wr_order_number is null
   group by d_year,
            ws_item_sk,
            ws_bill_customer_sk),
     cs_precomputed as
  (select d_year AS cs_sold_year,
          cs_item_sk,
          cs_bill_customer_sk cs_customer_sk,
          sum(cs_quantity) cs_qty,
          sum(cs_wholesale_cost) cs_wc,
          sum(cs_sales_price) cs_sp
   from catalog_sales
   left join catalog_returns on cr_order_number = cs_order_number
   and cs_item_sk = cr_item_sk
   join date_dim on cs_sold_date_sk = d_date_sk
   where cr_order_number is null
   group by d_year,
            cs_item_sk,
            cs_bill_customer_sk),
     ss_precomputed as
  (select d_year AS ss_sold_year,
          ss_item_sk,
          ss_customer_sk,
          sum(ss_quantity) ss_qty,
          sum(ss_wholesale_cost) ss_wc,
          sum(ss_sales_price) ss_sp
   from store_sales
   left join store_returns on sr_ticket_number = ss_ticket_number
   and ss_item_sk = sr_item_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   where sr_ticket_number is null
   group by d_year,
            ss_item_sk,
            ss_customer_sk)
select ss_customer_sk,
       round(ss_qty / (coalesce(ws_qty, 0) + coalesce(cs_qty, 0)), 2) ratio,
       ss_qty store_qty,
       ss_wc store_wholesale_cost,
       ss_sp store_sales_price,
       coalesce(ws_qty, 0) + coalesce(cs_qty, 0) other_chan_qty,
       coalesce(ws_wc, 0) + coalesce(cs_wc, 0) other_chan_wholesale_cost,
       coalesce(ws_sp, 0) + coalesce(cs_sp, 0) other_chan_sales_price
from ss_precomputed
left join ws_precomputed on (ws_sold_year = ss_sold_year
                             and ws_item_sk = ss_item_sk
                             and ws_customer_sk = ss_customer_sk)
left join cs_precomputed on (cs_sold_year = ss_sold_year
                             and cs_item_sk = ss_item_sk
                             and cs_customer_sk = ss_customer_sk)
where (coalesce(ws_qty, 0) > 0
       or coalesce(cs_qty, 0) > 0)
  and ss_sold_year = 1998
order by ss_customer_sk,
         ss_qty desc,
         ss_wc desc,
         ss_sp desc,
         other_chan_qty,
         other_chan_wholesale_cost,
         other_chan_sales_price,
         ratio
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN283
-- =================================================================
with sales_data_cte as
  (select ss_ticket_number,
          ss_customer_sk,
          store.s_city,
          sum(ss_coupon_amt) as amt,
          sum(ss_net_profit) as profit
   from store_sales,
        date_dim,
        store,
        household_demographics
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_store_sk = store.s_store_sk
     and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     and (household_demographics.hd_dep_count = 7
          or household_demographics.hd_vehicle_count > -1)
     and date_dim.d_dow = 4
     and date_dim.d_year in (2000,
                             2001,
                             2002)
     and store.s_number_employees between 200 and 295
   group by ss_ticket_number,
            ss_customer_sk,
            ss_addr_sk,
            store.s_city)
select c_last_name,
       c_first_name,
       substr(s_city, 1, 30),
       ss_ticket_number,
       amt,
       profit
from sales_data_cte
join customer on sales_data_cte.ss_customer_sk = customer.c_customer_sk
order by c_last_name,
         c_first_name,
         substr(s_city, 1, 30),
         profit
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN284
-- =================================================================
with date_range as
  (select d_date_sk
   from date_dim
   where d_date between cast('2002-08-14' as date) and (cast('2002-08-14' as date) + interval '30' day) ),
     ssr as
  (select s_store_id as store_id,
          sum(ss_ext_sales_price) as sales,
          sum(coalesce(sr_return_amt, 0)) as returns,
          sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
   from store_sales
   left outer join store_returns on (ss_item_sk = sr_item_sk
                                     and ss_ticket_number = sr_ticket_number), date_range,
                                                                               store,
                                                                               item,
                                                                               promotion
   where ss_sold_date_sk = date_range.d_date_sk
     and ss_store_sk = s_store_sk
     and ss_item_sk = i_item_sk
     and i_current_price > 50
     and ss_promo_sk = p_promo_sk
     and p_channel_tv = 'N'
   group by s_store_id),
     csr as
  (select cp_catalog_page_id as catalog_page_id,
          sum(cs_ext_sales_price) as sales,
          sum(coalesce(cr_return_amount, 0)) as returns,
          sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
   from catalog_sales
   left outer join catalog_returns on (cs_item_sk = cr_item_sk
                                       and cs_order_number = cr_order_number), date_range,
                                                                               catalog_page,
                                                                               item,
                                                                               promotion
   where cs_sold_date_sk = date_range.d_date_sk
     and cs_catalog_page_sk = cp_catalog_page_sk
     and cs_item_sk = i_item_sk
     and i_current_price > 50
     and cs_promo_sk = p_promo_sk
     and p_channel_tv = 'N'
   group by cp_catalog_page_id),
     wsr as
  (select web_site_id,
          sum(ws_ext_sales_price) as sales,
          sum(coalesce(wr_return_amt, 0)) as returns,
          sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
   from web_sales
   left outer join web_returns on (ws_item_sk = wr_item_sk
                                   and ws_order_number = wr_order_number), date_range,
                                                                           web_site,
                                                                           item,
                                                                           promotion
   where ws_sold_date_sk = date_range.d_date_sk
     and ws_web_site_sk = web_site_sk
     and ws_item_sk = i_item_sk
     and i_current_price > 50
     and ws_promo_sk = p_promo_sk
     and p_channel_tv = 'N'
   group by web_site_id)
select channel,
       id,
       sum(sales) as sales,
       sum(returns) as returns,
       sum(profit) as profit
from
  (select 'store channel' as channel,
          'store' || store_id as id,
          sales,
          returns,
          profit
   from ssr
   union all select 'catalog channel' as channel,
                    'catalog_page' || catalog_page_id as id,
                    sales,
                    returns,
                    profit
   from csr
   union all select 'web channel' as channel,
                    'web_site' || web_site_id as id,
                    sales,
                    returns,
                    profit
   from wsr) x
group by rollup (channel,
                 id)
order by channel,
         id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN285
-- =================================================================
WITH customer_total_return AS
  (SELECT cr_returning_customer_sk AS ctr_customer_sk,
          ca_state AS ctr_state,
          SUM(cr_return_amt_inc_tax) AS ctr_total_return
   FROM catalog_returns
   JOIN date_dim ON cr_returned_date_sk = d_date_sk
   JOIN customer_address ON cr_returning_addr_sk = ca_address_sk
   WHERE d_year = 2001
   GROUP BY cr_returning_customer_sk,
            ca_state),
     state_avg_return AS
  (SELECT ctr_state,
          AVG(ctr_total_return) * 1.2 AS avg_return_threshold
   FROM customer_total_return
   GROUP BY ctr_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       ca_street_number,
       ca_street_name,
       ca_street_type,
       ca_suite_number,
       ca_city,
       ca_county,
       ca_state,
       ca_zip,
       ca_country,
       ca_gmt_offset,
       ca_location_type,
       ctr1.ctr_total_return
FROM customer_total_return ctr1
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
JOIN state_avg_return sar ON ctr1.ctr_state = sar.ctr_state
WHERE ctr1.ctr_total_return > sar.avg_return_threshold
  AND ca_state = 'TN'
ORDER BY c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         ca_street_number,
         ca_street_name,
         ca_street_type,
         ca_suite_number,
         ca_city,
         ca_county,
         ca_state,
         ca_zip,
         ca_country,
         ca_gmt_offset,
         ca_location_type,
         ctr1.ctr_total_return
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN286
-- =================================================================
with date_range_cte as
  (select d_date_sk
   from date_dim
   where d_date between cast('2001-01-13' as date) and (cast('2001-01-13' as date) + interval '60' day) ),
     inventory_cte as
  (select inv_item_sk,
          inv_date_sk,
          inv_quantity_on_hand
   from inventory
   where inv_quantity_on_hand between 100 and 500 ),
     store_sales_cte as
  (select ss_item_sk
   from store_sales)
select i_item_id,
       i_item_desc,
       i_current_price
from item
join inventory_cte on inv_item_sk = i_item_sk
join date_range_cte on d_date_sk = inv_date_sk
join store_sales_cte on ss_item_sk = i_item_sk
where i_current_price between 58 and 58 + 30
  and i_manufact_id in (259,
                        559,
                        580,
                        485)
group by i_item_id,
         i_item_desc,
         i_current_price
order by i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN287
-- =================================================================
with date_cte as
  (select d_date,
          d_week_seq
   from date_dim
   where d_date in ('2001-07-13',
                    '2001-09-10',
                    '2001-11-16',
                    '2000-12-14') ),
     week_seq_cte as
  (select d_week_seq
   from date_cte),
     dates_in_weeks_cte as
  (select d_date
   from date_dim
   where d_week_seq in
       (select d_week_seq
        from week_seq_cte) ),
     sr_items as
  (select i_item_id item_id,
          sum(sr_return_quantity) sr_item_qty
   from store_returns,
        item,
        date_dim
   where sr_item_sk = i_item_sk
     and d_date in
       (select d_date
        from dates_in_weeks_cte)
     and sr_returned_date_sk = d_date_sk
   group by i_item_id),
     cr_items as
  (select i_item_id item_id,
          sum(cr_return_quantity) cr_item_qty
   from catalog_returns,
        item,
        date_dim
   where cr_item_sk = i_item_sk
     and d_date in
       (select d_date
        from dates_in_weeks_cte)
     and cr_returned_date_sk = d_date_sk
   group by i_item_id),
     wr_items as
  (select i_item_id item_id,
          sum(wr_return_quantity) wr_item_qty
   from web_returns,
        item,
        date_dim
   where wr_item_sk = i_item_sk
     and d_date in
       (select d_date
        from dates_in_weeks_cte)
     and wr_returned_date_sk = d_date_sk
   group by i_item_id)
select sr_items.item_id,
       sr_item_qty,
       sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 sr_dev,
       cr_item_qty,
       cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 cr_dev,
       wr_item_qty,
       wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 wr_dev,
       (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 average
from sr_items,
     cr_items,
     wr_items
where sr_items.item_id = cr_items.item_id
  and sr_items.item_id = wr_items.item_id
order by sr_items.item_id,
         sr_item_qty
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN288
-- =================================================================
with filtered_customer_address as
  (select ca_address_sk
   from customer_address
   where ca_city = 'Woodland' ),
     filtered_income_band as
  (select ib_income_band_sk
   from income_band
   where ib_lower_bound >= 60306
     and ib_upper_bound <= 60306 + 50000 ),
     filtered_customer as
  (select c_customer_id,
          c_last_name,
          c_first_name,
          c_current_addr_sk,
          c_current_cdemo_sk,
          c_current_hdemo_sk
   from customer
   where c_current_addr_sk in
       (select ca_address_sk
        from filtered_customer_address) ),
     filtered_customer_demographics as
  (select cd_demo_sk
   from customer_demographics
   where cd_demo_sk in
       (select c_current_cdemo_sk
        from filtered_customer) ),
     filtered_household_demographics as
  (select hd_demo_sk
   from household_demographics
   where hd_income_band_sk in
       (select ib_income_band_sk
        from filtered_income_band) ),
     filtered_store_returns as
  (select sr_cdemo_sk
   from store_returns
   where sr_cdemo_sk in
       (select cd_demo_sk
        from filtered_customer_demographics) )
select c_customer_id as customer_id,
       coalesce(c_last_name, '') || ',' || coalesce(c_first_name, '') as customername
from filtered_customer
where c_current_cdemo_sk in
    (select sr_cdemo_sk
     from filtered_store_returns)
  and c_current_hdemo_sk in
    (select hd_demo_sk
     from filtered_household_demographics)
order by c_customer_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN289
-- =================================================================
with filtered_web_sales as
  (select *
   from web_sales
   where ws_sales_price between 50.00 and 200.00 ),
     filtered_customer_demographics as
  (select *
   from customer_demographics
   where (cd_marital_status = 'D'
          and cd_education_status = 'Primary')
     or (cd_marital_status = 'S'
         and cd_education_status = 'College')
     or (cd_marital_status = 'U'
         and cd_education_status = 'Advanced Degree') ),
     filtered_customer_address as
  (select *
   from customer_address
   where ca_country = 'United States'
     and ca_state in ('NC',
                      'TX',
                      'IA',
                      'WI',
                      'WV',
                      'GA',
                      'OK',
                      'VA',
                      'KY') ),
     filtered_web_returns as
  (select *
   from web_returns
   where wr_refunded_cdemo_sk in
       (select cd_demo_sk
        from filtered_customer_demographics)
     and wr_returning_cdemo_sk in
       (select cd_demo_sk
        from filtered_customer_demographics)
     and wr_refunded_addr_sk in
       (select ca_address_sk
        from filtered_customer_address) ),
     filtered_date_dim as
  (select *
   from date_dim
   where d_year = 1998 )
select substr(r_reason_desc, 1, 20),
       avg(ws_quantity),
       avg(wr_refunded_cash),
       avg(wr_fee)
from filtered_web_sales ws
join filtered_web_returns wr on ws.ws_item_sk = wr.wr_item_sk
and ws.ws_order_number = wr.wr_order_number
join web_page wp on ws.ws_web_page_sk = wp.wp_web_page_sk
join filtered_date_dim dd on ws.ws_sold_date_sk = dd.d_date_sk
join reason r on r.r_reason_sk = wr.wr_reason_sk
where (ws.ws_sales_price between 100.00 and 150.00
       and ws.ws_net_profit between 100 and 200)
  or (ws.ws_sales_price between 50.00 and 100.00
      and ws.ws_net_profit between 150 and 300)
  or (ws.ws_sales_price between 150.00 and 200.00
      and ws.ws_net_profit between 50 and 250)
group by r_reason_desc
order by substr(r_reason_desc, 1, 20),
         avg(ws_quantity),
         avg(wr_refunded_cash),
         avg(wr_fee)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN290
-- =================================================================
WITH sales_summary_cte AS
  (SELECT SUM(ws_net_paid) AS total_sum,
          i_category,
          i_class,
          GROUPING(i_category) + GROUPING(i_class) AS lochierarchy
   FROM web_sales
   JOIN date_dim d1 ON d1.d_date_sk = ws_sold_date_sk
   JOIN item ON i_item_sk = ws_item_sk
   WHERE d1.d_month_seq BETWEEN 1186 AND 1186 + 11
   GROUP BY ROLLUP(i_category, i_class))
SELECT *,
       RANK() OVER (PARTITION BY lochierarchy,
                                 CASE
                                     WHEN lochierarchy = 1 THEN i_category
                                 END
                    ORDER BY total_sum DESC) AS rank_within_parent
FROM sales_summary_cte
ORDER BY lochierarchy DESC,
         CASE
             WHEN lochierarchy = 0 THEN i_category
         END,
         rank_within_parent
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN291
-- =================================================================
with store_sales_cte as
  (select distinct c_last_name,
                   c_first_name,
                   d_date
   from store_sales,
        date_dim,
        customer
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_customer_sk = customer.c_customer_sk
     and d_month_seq between 1202 and 1202 + 11 ),
     catalog_sales_cte as
  (select distinct c_last_name,
                   c_first_name,
                   d_date
   from catalog_sales,
        date_dim,
        customer
   where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
     and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
     and d_month_seq between 1202 and 1202 + 11 ),
     web_sales_cte as
  (select distinct c_last_name,
                   c_first_name,
                   d_date
   from web_sales,
        date_dim,
        customer
   where web_sales.ws_sold_date_sk = date_dim.d_date_sk
     and web_sales.ws_bill_customer_sk = customer.c_customer_sk
     and d_month_seq between 1202 and 1202 + 11 )
select count(*)
from
  (select *
   from store_sales_cte
   except select *
   from catalog_sales_cte
   except select *
   from web_sales_cte) cool_cust ;

-- =================================================================
-- Query ID: TPCDSN292
-- =================================================================
with base_data as
  (select ss_sold_time_sk,
          ss_hdemo_sk,
          ss_store_sk
   from store_sales
   join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ss_sold_time_sk = time_dim.t_time_sk
   join store on ss_store_sk = s_store_sk
   where store.s_store_name = 'ese'
     and ((household_demographics.hd_dep_count = 0
           and household_demographics.hd_vehicle_count <= 2)
          or (household_demographics.hd_dep_count = -1
              and household_demographics.hd_vehicle_count <= 1)
          or (household_demographics.hd_dep_count = 3
              and household_demographics.hd_vehicle_count <= 5)) )
select *
from
  (select count(*) as h8_30_to_9
   from base_data
   join time_dim on base_data.ss_sold_time_sk = time_dim.t_time_sk
   where time_dim.t_hour = 8
     and time_dim.t_minute >= 30 ) s1,

  (select count(*) as h9_to_9_30
   from base_data
   join time_dim on base_data.ss_sold_time_sk = time_dim.t_time_sk
   where time_dim.t_hour = 9
     and time_dim.t_minute < 30 ) s2,

  (select count(*) as h9_30_to_10
   from base_data
   join time_dim on base_data.ss_sold_time_sk = time_dim.t_time_sk
   where time_dim.t_hour = 9
     and time_dim.t_minute >= 30 ) s3,

  (select count(*) as h10_to_10_30
   from base_data
   join time_dim on base_data.ss_sold_time_sk = time_dim.t_time_sk
   where time_dim.t_hour = 10
     and time_dim.t_minute < 30 ) s4,

  (select count(*) as h10_30_to_11
   from base_data
   join time_dim on base_data.ss_sold_time_sk = time_dim.t_time_sk
   where time_dim.t_hour = 10
     and time_dim.t_minute >= 30 ) s5,

  (select count(*) as h11_to_11_30
   from base_data
   join time_dim on base_data.ss_sold_time_sk = time_dim.t_time_sk
   where time_dim.t_hour = 11
     and time_dim.t_minute < 30 ) s6,

  (select count(*) as h11_30_to_12
   from base_data
   join time_dim on base_data.ss_sold_time_sk = time_dim.t_time_sk
   where time_dim.t_hour = 11
     and time_dim.t_minute >= 30 ) s7,

  (select count(*) as h12_to_12_30
   from base_data
   join time_dim on base_data.ss_sold_time_sk = time_dim.t_time_sk
   where time_dim.t_hour = 12
     and time_dim.t_minute < 30 ) s8 ;

-- =================================================================
-- Query ID: TPCDSN293
-- =================================================================
with sales_data_cte as
  (select i_category,
          i_class,
          i_brand,
          s_store_name,
          s_company_name,
          d_moy,
          sum(ss_sales_price) as sum_sales,
          avg(sum(ss_sales_price)) over (partition by i_category,
                                                      i_brand,
                                                      s_store_name,
                                                      s_company_name) as avg_monthly_sales
   from item,
        store_sales,
        date_dim,
        store
   where ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and ss_store_sk = s_store_sk
     and d_year in (2001)
     and ((i_category in ('Books',
                          'Children',
                          'Electronics')
           and i_class in ('history',
                           'school-uniforms',
                           'audio'))
          or (i_category in ('Men',
                             'Sports',
                             'Shoes')
              and i_class in ('pants',
                              'tennis',
                              'womens')))
   group by i_category,
            i_class,
            i_brand,
            s_store_name,
            s_company_name,
            d_moy)
select *
from sales_data_cte
where case
          when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales)
          else null
      end > 0.1
order by sum_sales - avg_monthly_sales,
         s_store_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN294
-- =================================================================
with amc_cte as
  (select count(*) as amc
   from web_sales
   join household_demographics on ws_ship_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ws_sold_time_sk = time_dim.t_time_sk
   join web_page on ws_web_page_sk = web_page.wp_web_page_sk
   where time_dim.t_hour between 12 and 13
     and household_demographics.hd_dep_count = 6
     and web_page.wp_char_count between 5000 and 5200 ),
     pmc_cte as
  (select count(*) as pmc
   from web_sales
   join household_demographics on ws_ship_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ws_sold_time_sk = time_dim.t_time_sk
   join web_page on ws_web_page_sk = web_page.wp_web_page_sk
   where time_dim.t_hour between 14 and 15
     and household_demographics.hd_dep_count = 6
     and web_page.wp_char_count between 5000 and 5200 )
select cast(amc_cte.amc as decimal(15, 4)) / cast(pmc_cte.pmc as decimal(15, 4)) as am_pm_ratio
from amc_cte,
     pmc_cte
order by am_pm_ratio
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN295
-- =================================================================
WITH filtered_date_dim AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_year = 2000
     AND d_moy = 12 ),
     filtered_customer_demographics AS
  (SELECT cd_demo_sk
   FROM customer_demographics
   WHERE (cd_marital_status = 'M'
          AND cd_education_status = 'Advanced Degree')
     OR (cd_marital_status = 'W'
         AND cd_education_status = 'Unnknown') ),
     filtered_household_demographics AS
  (SELECT hd_demo_sk
   FROM household_demographics
   WHERE hd_buy_potential LIKE 'Unknown%' ),
     filtered_customer_address AS
  (SELECT ca_address_sk
   FROM customer_address
   WHERE ca_gmt_offset = -7 )
SELECT cc_call_center_id AS Call_Center,
       cc_name AS Call_Center_Name,
       cc_manager AS Manager,
       SUM(cr_net_loss) AS Returns_Loss
FROM call_center
JOIN catalog_returns ON cr_call_center_sk = cc_call_center_sk
JOIN filtered_date_dim ON cr_returned_date_sk = filtered_date_dim.d_date_sk
JOIN customer ON cr_returning_customer_sk = c_customer_sk
JOIN filtered_customer_demographics ON cd_demo_sk = c_current_cdemo_sk
JOIN filtered_household_demographics ON hd_demo_sk = c_current_hdemo_sk
JOIN filtered_customer_address ON ca_address_sk = c_current_addr_sk
GROUP BY cc_call_center_id,
         cc_name,
         cc_manager
ORDER BY Returns_Loss DESC ;

-- =================================================================
-- Query ID: TPCDSN296
-- =================================================================
WITH avg_discount_cte AS
  (SELECT 1.3 * AVG(ws_ext_discount_amt) AS avg_discount_amt
   FROM web_sales
   JOIN date_dim ON d_date_sk = ws_sold_date_sk
   JOIN item ON ws_item_sk = i_item_sk
   WHERE d_date BETWEEN '2000-02-01' AND (CAST('2000-02-01' AS DATE) + INTERVAL '90' DAY) )
SELECT SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales
JOIN item ON i_item_sk = ws_item_sk
JOIN date_dim ON d_date_sk = ws_sold_date_sk
JOIN avg_discount_cte ON ws_ext_discount_amt > avg_discount_cte.avg_discount_amt
WHERE i_manufact_id = 393
  AND d_date BETWEEN '2000-02-01' AND (CAST('2000-02-01' AS DATE) + INTERVAL '90' DAY)
GROUP BY i_manufact_id
ORDER BY SUM(ws_ext_discount_amt)
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN297
-- =================================================================
with sales_cte as
  (select ss_item_sk,
          ss_ticket_number,
          ss_customer_sk,
          case
              when sr_return_quantity is not null then (ss_quantity - sr_return_quantity) * ss_sales_price
              else (ss_quantity * ss_sales_price)
          end as act_sales
   from store_sales
   left outer join store_returns on (sr_item_sk = ss_item_sk
                                     and sr_ticket_number = ss_ticket_number)
   join reason on sr_reason_sk = r_reason_sk
   where r_reason_desc = 'Package was damaged' )
select ss_customer_sk,
       sum(act_sales) as sumsales
from sales_cte
group by ss_customer_sk
order by sumsales,
         ss_customer_sk
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN298
-- =================================================================
WITH web_sales_cte AS
  (SELECT ws_order_number
   FROM web_sales ws1
   WHERE EXISTS
       (SELECT 1
        FROM web_sales ws2
        WHERE ws1.ws_order_number = ws2.ws_order_number
          AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk ) ),
     web_returns_cte AS
  (SELECT wr_order_number
   FROM web_returns wr1
   WHERE EXISTS
       (SELECT 1
        FROM web_sales ws1
        WHERE ws1.ws_order_number = wr1.wr_order_number ) )
SELECT COUNT(DISTINCT ws1.ws_order_number) AS "order count",
       SUM(ws1.ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws1.ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_dim ON ws1.ws_ship_date_sk = d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
WHERE d_date BETWEEN '2002-05-01' AND (CAST('2002-05-01' AS DATE) + INTERVAL '60' DAY)
  AND ca_state = 'OK'
  AND web_company_name = 'pri'
  AND EXISTS
    (SELECT 1
     FROM web_sales_cte
     WHERE ws1.ws_order_number = web_sales_cte.ws_order_number)
  AND NOT EXISTS
    (SELECT 1
     FROM web_returns_cte
     WHERE ws1.ws_order_number = web_returns_cte.wr_order_number)
GROUP BY ws1.ws_order_number
ORDER BY COUNT(DISTINCT ws1.ws_order_number)
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN299
-- =================================================================
with ws_wh as
  (select ws1.ws_order_number,
          ws1.ws_warehouse_sk wh1,
          ws2.ws_warehouse_sk wh2
   from web_sales ws1,
        web_sales ws2
   where ws1.ws_order_number = ws2.ws_order_number
     and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk ),
     ws_wh_orders as
  (select ws_order_number
   from ws_wh),
     ws_wh_returns as
  (select wr_order_number
   from web_returns,
        ws_wh
   where wr_order_number = ws_wh.ws_order_number )
select count(distinct ws_order_number) as "order count",
       sum(ws_ext_ship_cost) as "total shipping cost",
       sum(ws_net_profit) as "total net profit"
from web_sales ws1,
     date_dim,
     customer_address,
     web_site
where d_date between '2001-4-01' and (cast('2001-4-01' as date) + interval '60' day)
  and ws1.ws_ship_date_sk = d_date_sk
  and ws1.ws_ship_addr_sk = ca_address_sk
  and ca_state = 'VA'
  and ws1.ws_web_site_sk = web_site_sk
  and web_company_name = 'pri'
  and ws1.ws_order_number in
    (select ws_order_number
     from ws_wh_orders)
  and ws1.ws_order_number in
    (select wr_order_number
     from ws_wh_returns)
order by count(distinct ws_order_number)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN300
-- =================================================================
with filtered_time_dim as
  (select t_time_sk
   from time_dim
   where t_hour = 8
     and t_minute >= 30 ),
     filtered_household_demographics as
  (select hd_demo_sk
   from household_demographics
   where hd_dep_count = 0 ),
     filtered_store as
  (select s_store_sk
   from store
   where s_store_name = 'ese' )
select count(*)
from store_sales
join filtered_time_dim on ss_sold_time_sk = filtered_time_dim.t_time_sk
join filtered_household_demographics on ss_hdemo_sk = filtered_household_demographics.hd_demo_sk
join filtered_store on ss_store_sk = filtered_store.s_store_sk
order by count(*)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN301
-- =================================================================
with ssci as
  (select ss_customer_sk customer_sk,
          ss_item_sk item_sk
   from store_sales,
        date_dim
   where ss_sold_date_sk = d_date_sk
     and d_month_seq between 1199 and 1199 + 11
   group by ss_customer_sk,
            ss_item_sk),
     csci as
  (select cs_bill_customer_sk customer_sk,
          cs_item_sk item_sk
   from catalog_sales,
        date_dim
   where cs_sold_date_sk = d_date_sk
     and d_month_seq between 1199 and 1199 + 11
   group by cs_bill_customer_sk,
            cs_item_sk),
     store_only_cte as
  (select ssci.customer_sk,
          ssci.item_sk
   from ssci
   left join csci on ssci.customer_sk = csci.customer_sk
   and ssci.item_sk = csci.item_sk
   where csci.customer_sk is null ),
     catalog_only_cte as
  (select csci.customer_sk,
          csci.item_sk
   from csci
   left join ssci on csci.customer_sk = ssci.customer_sk
   and csci.item_sk = ssci.item_sk
   where ssci.customer_sk is null ),
     store_and_catalog_cte as
  (select ssci.customer_sk,
          ssci.item_sk
   from ssci
   join csci on ssci.customer_sk = csci.customer_sk
   and ssci.item_sk = csci.item_sk)
select
  (select count(*)
   from store_only_cte) as store_only,

  (select count(*)
   from catalog_only_cte) as catalog_only,

  (select count(*)
   from store_and_catalog_cte) as store_and_catalog
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN302
-- =================================================================
WITH sales_data AS
  (SELECT ss_item_sk,
          ss_ext_sales_price,
          i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price
   FROM store_sales
   JOIN item ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE i_category IN ('Men',
                        'Sports',
                        'Jewelry')
     AND d_date BETWEEN CAST('1999-02-05' AS DATE) AND (CAST('1999-02-05' AS DATE) + INTERVAL '30' DAY) ),
     category_sales AS
  (SELECT i_class,
          SUM(ss_ext_sales_price) AS total_class_sales
   FROM sales_data
   GROUP BY i_class)
SELECT sd.i_item_id,
       sd.i_item_desc,
       sd.i_category,
       sd.i_class,
       sd.i_current_price,
       SUM(sd.ss_ext_sales_price) AS itemrevenue,
       SUM(sd.ss_ext_sales_price) * 100 / cs.total_class_sales AS revenueratio
FROM sales_data sd
JOIN category_sales cs ON sd.i_class = cs.i_class
GROUP BY sd.i_item_id,
         sd.i_item_desc,
         sd.i_category,
         sd.i_class,
         sd.i_current_price,
         cs.total_class_sales
ORDER BY sd.i_category,
         sd.i_class,
         sd.i_item_id,
         sd.i_item_desc,
         revenueratio ;

-- =================================================================
-- Query ID: TPCDSN303
-- =================================================================
WITH date_diff_cte AS
  (SELECT cs_ship_date_sk - cs_sold_date_sk AS date_diff,
          cs_ship_date_sk,
          cs_warehouse_sk,
          cs_ship_mode_sk,
          cs_call_center_sk
   FROM catalog_sales)
SELECT SUBSTR(w_warehouse_name, 1, 20),
       sm_type,
       cc_name,
       SUM(CASE
               WHEN date_diff <= 30 THEN 1
               ELSE 0
           END) AS "30 days",
       SUM(CASE
               WHEN date_diff > 30
                    AND date_diff <= 60 THEN 1
               ELSE 0
           END) AS "31-60 days",
       SUM(CASE
               WHEN date_diff > 60
                    AND date_diff <= 90 THEN 1
               ELSE 0
           END) AS "61-90 days",
       SUM(CASE
               WHEN date_diff > 90
                    AND date_diff <= 120 THEN 1
               ELSE 0
           END) AS "91-120 days",
       SUM(CASE
               WHEN date_diff > 120 THEN 1
               ELSE 0
           END) AS ">120 days"
FROM date_diff_cte
JOIN warehouse ON date_diff_cte.cs_warehouse_sk = w_warehouse_sk
JOIN ship_mode ON date_diff_cte.cs_ship_mode_sk = sm_ship_mode_sk
JOIN call_center ON date_diff_cte.cs_call_center_sk = cc_call_center_sk
JOIN date_dim ON date_diff_cte.cs_ship_date_sk = d_date_sk
WHERE d_month_seq BETWEEN 1194 AND 1194 + 11
GROUP BY SUBSTR(w_warehouse_name, 1, 20),
         sm_type,
         cc_name
ORDER BY SUBSTR(w_warehouse_name, 1, 20),
         sm_type,
         cc_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN304
-- =================================================================
WITH customer_total_return AS
  (SELECT sr_customer_sk AS ctr_customer_sk,
          sr_store_sk AS ctr_store_sk,
          SUM(sr_fee) AS ctr_total_return
   FROM store_returns
   JOIN date_dim ON sr_returned_date_sk = d_date_sk
   WHERE d_year = 2000
   GROUP BY sr_customer_sk,
            sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1
JOIN store ON s_store_sk = ctr1.ctr_store_sk
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
WHERE s_state = 'TN'
  AND ctr1.ctr_total_return >
    (SELECT AVG(ctr_total_return) * 1.2
     FROM customer_total_return
     WHERE ctr1.ctr_store_sk = ctr_store_sk )
ORDER BY c_customer_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN305
-- =================================================================
WITH wscs AS
  (SELECT sold_date_sk,
          sales_price
   FROM
     (SELECT ws_sold_date_sk AS sold_date_sk,
             ws_ext_sales_price AS sales_price
      FROM web_sales
      UNION ALL SELECT cs_sold_date_sk AS sold_date_sk,
                       cs_ext_sales_price AS sales_price
      FROM catalog_sales) AS tmp),
     wswscs AS
  (SELECT d_week_seq,
          SUM(CASE
                  WHEN d_day_name = 'Sunday' THEN sales_price
                  ELSE NULL
              END) AS sun_sales,
          SUM(CASE
                  WHEN d_day_name = 'Monday' THEN sales_price
                  ELSE NULL
              END) AS mon_sales,
          SUM(CASE
                  WHEN d_day_name = 'Tuesday' THEN sales_price
                  ELSE NULL
              END) AS tue_sales,
          SUM(CASE
                  WHEN d_day_name = 'Wednesday' THEN sales_price
                  ELSE NULL
              END) AS wed_sales,
          SUM(CASE
                  WHEN d_day_name = 'Thursday' THEN sales_price
                  ELSE NULL
              END) AS thu_sales,
          SUM(CASE
                  WHEN d_day_name = 'Friday' THEN sales_price
                  ELSE NULL
              END) AS fri_sales,
          SUM(CASE
                  WHEN d_day_name = 'Saturday' THEN sales_price
                  ELSE NULL
              END) AS sat_sales
   FROM wscs
   JOIN date_dim ON d_date_sk = sold_date_sk
   GROUP BY d_week_seq)
SELECT y.d_week_seq1,
       ROUND(y.sun_sales1 / z.sun_sales2, 2),
       ROUND(y.mon_sales1 / z.mon_sales2, 2),
       ROUND(y.tue_sales1 / z.tue_sales2, 2),
       ROUND(y.wed_sales1 / z.wed_sales2, 2),
       ROUND(y.thu_sales1 / z.thu_sales2, 2),
       ROUND(y.fri_sales1 / z.fri_sales2, 2),
       ROUND(y.sat_sales1 / z.sat_sales2, 2)
FROM
  (SELECT wswscs.d_week_seq AS d_week_seq1,
          sun_sales AS sun_sales1,
          mon_sales AS mon_sales1,
          tue_sales AS tue_sales1,
          wed_sales AS wed_sales1,
          thu_sales AS thu_sales1,
          fri_sales AS fri_sales1,
          sat_sales AS sat_sales1
   FROM wswscs
   JOIN date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
   WHERE d_year = 1998 ) y
JOIN
  (SELECT wswscs.d_week_seq AS d_week_seq2,
          sun_sales AS sun_sales2,
          mon_sales AS mon_sales2,
          tue_sales AS tue_sales2,
          wed_sales AS wed_sales2,
          thu_sales AS thu_sales2,
          fri_sales AS fri_sales2,
          sat_sales AS sat_sales2
   FROM wswscs
   JOIN date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
   WHERE d_year = 1999 ) z ON y.d_week_seq1 = z.d_week_seq2 - 53
ORDER BY y.d_week_seq1 ;

-- =================================================================
-- Query ID: TPCDSN306
-- =================================================================
SELECT dt.d_year,
       item.i_brand_id AS brand_id,
       item.i_brand AS brand,
       SUM(ss_sales_price) AS sum_agg
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manufact_id = 816
  AND dt.d_moy = 11
GROUP BY dt.d_year,
         item.i_brand,
         item.i_brand_id
ORDER BY dt.d_year,
         sum_agg DESC,
         brand_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN307
-- =================================================================
WITH year_total AS
  (SELECT c_customer_id AS customer_id,
          c_first_name AS customer_first_name,
          c_last_name AS customer_last_name,
          c_preferred_cust_flag AS customer_preferred_cust_flag,
          c_birth_country AS customer_birth_country,
          c_login AS customer_login,
          c_email_address AS customer_email_address,
          d_year AS dyear,
          SUM(CASE
                  WHEN ss_customer_sk IS NOT NULL THEN ((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2
                  WHEN cs_bill_customer_sk IS NOT NULL THEN ((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2
                  WHEN ws_bill_customer_sk IS NOT NULL THEN ((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2
                  ELSE 0
              END) AS year_total,
          MAX(CASE
                  WHEN ss_customer_sk IS NOT NULL THEN 's'
                  WHEN cs_bill_customer_sk IS NOT NULL THEN 'c'
                  WHEN ws_bill_customer_sk IS NOT NULL THEN 'w'
              END) AS sale_type
   FROM customer
   LEFT JOIN store_sales ON c_customer_sk = ss_customer_sk
   LEFT JOIN catalog_sales ON c_customer_sk = cs_bill_customer_sk
   LEFT JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   OR cs_sold_date_sk = d_date_sk
   OR ws_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_birth_country
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_c_firstyear,
     year_total t_c_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_c_secyear.customer_id
  AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_c_firstyear.sale_type = 'c'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_c_secyear.sale_type = 'c'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 1999
  AND t_s_secyear.dyear = 1999
  AND t_c_firstyear.dyear = 1999
  AND t_c_secyear.dyear = 1999
  AND t_w_firstyear.dyear = 1999
  AND t_w_secyear.dyear = 1999
  AND t_s_firstyear.year_total > 0
  AND t_c_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
      END >= CASE
                 WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                 ELSE NULL
             END
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
      END >= CASE
                 WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                 ELSE NULL
             END
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_birth_country
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN308
-- =================================================================
WITH salesreturns AS
  (SELECT 'store' AS channel_type,
          ss_store_sk AS id,
          ss_sold_date_sk AS date_sk,
          ss_ext_sales_price AS sales_price,
          ss_net_profit AS profit,
          CAST(0 AS DECIMAL(7, 2)) AS return_amt,
          CAST(0 AS DECIMAL(7, 2)) AS net_loss
   FROM store_sales
   UNION ALL SELECT 'store' AS channel_type,
                    sr_store_sk AS id,
                    sr_returned_date_sk AS date_sk,
                    CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                    CAST(0 AS DECIMAL(7, 2)) AS profit,
                    sr_return_amt AS return_amt,
                    sr_net_loss AS net_loss
   FROM store_returns
   UNION ALL SELECT 'catalog' AS channel_type,
                    cs_catalog_page_sk AS id,
                    cs_sold_date_sk AS date_sk,
                    cs_ext_sales_price AS sales_price,
                    cs_net_profit AS profit,
                    CAST(0 AS DECIMAL(7, 2)) AS return_amt,
                    CAST(0 AS DECIMAL(7, 2)) AS net_loss
   FROM catalog_sales
   UNION ALL SELECT 'catalog' AS channel_type,
                    cr_catalog_page_sk AS id,
                    cr_returned_date_sk AS date_sk,
                    CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                    CAST(0 AS DECIMAL(7, 2)) AS profit,
                    cr_return_amount AS return_amt,
                    cr_net_loss AS net_loss
   FROM catalog_returns
   UNION ALL SELECT 'web' AS channel_type,
                    ws_web_site_sk AS id,
                    ws_sold_date_sk AS date_sk,
                    ws_ext_sales_price AS sales_price,
                    ws_net_profit AS profit,
                    CAST(0 AS DECIMAL(7, 2)) AS return_amt,
                    CAST(0 AS DECIMAL(7, 2)) AS net_loss
   FROM web_sales
   UNION ALL SELECT 'web' AS channel_type,
                    ws_web_site_sk AS id,
                    wr_returned_date_sk AS date_sk,
                    CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                    CAST(0 AS DECIMAL(7, 2)) AS profit,
                    wr_return_amt AS return_amt,
                    wr_net_loss AS net_loss
   FROM web_returns
   LEFT OUTER JOIN web_sales ON (wr_item_sk = ws_item_sk
                                 AND wr_order_number = ws_order_number)),
     filtered_salesreturns AS
  (SELECT channel_type,
          id,
          sales_price,
          profit,
          return_amt,
          net_loss
   FROM salesreturns
   JOIN date_dim ON date_sk = d_date_sk
   WHERE d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + INTERVAL '14' DAY) ),
     aggregated_sales AS
  (SELECT channel_type,
          id,
          SUM(sales_price) AS sales,
          SUM(profit) AS profit,
          SUM(return_amt) AS returns,
          SUM(net_loss) AS profit_loss
   FROM filtered_salesreturns
   GROUP BY channel_type,
            id)
SELECT CASE
           WHEN channel_type = 'store' THEN 'store channel'
           WHEN channel_type = 'catalog' THEN 'catalog channel'
           WHEN channel_type = 'web' THEN 'web channel'
       END AS channel,
       CASE
           WHEN channel_type = 'store' THEN 'store' || id
           WHEN channel_type = 'catalog' THEN 'catalog_page' || id
           WHEN channel_type = 'web' THEN 'web_site' || id
       END AS id,
       SUM(sales) AS sales,
       SUM(returns) AS returns,
       SUM(profit - profit_loss) AS profit
FROM aggregated_sales
GROUP BY ROLLUP (channel_type,
                 id)
ORDER BY channel,
         id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN309
-- =================================================================
SELECT a.ca_state AS state,
       COUNT(*) AS cnt
FROM customer_address a
JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
JOIN item i ON s.ss_item_sk = i.i_item_sk
WHERE d.d_month_seq =
    (SELECT DISTINCT d_month_seq
     FROM date_dim
     WHERE d_year = 2002
       AND d_moy = 3)
  AND i.i_current_price > 1.2 *
    (SELECT AVG(j.i_current_price)
     FROM item j
     WHERE j.i_category = i.i_category)
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt,
         a.ca_state
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN310
-- =================================================================
SELECT i_item_id,
       AVG(ss_quantity) AS agg1,
       AVG(ss_list_price) AS agg2,
       AVG(ss_coupon_amt) AS agg3,
       AVG(ss_sales_price) AS agg4
FROM store_sales
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN item ON ss_item_sk = i_item_sk
JOIN promotion ON ss_promo_sk = p_promo_sk
WHERE cd_gender = 'F'
  AND cd_marital_status = 'W'
  AND cd_education_status = 'College'
  AND (p_channel_email = 'N'
       OR p_channel_event = 'N')
  AND d_year = 2001
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN311
-- =================================================================
SELECT s_store_name,
       SUM(ss_net_profit)
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN
  (SELECT substr(ca_zip, 1, 5) AS ca_zip
   FROM customer_address
   WHERE substr(ca_zip, 1, 5) IN ('47602',
                                  '16704',
                                  '35863',
                                  '28577',
                                  '83910',
                                  '36201',
                                  '58412',
                                  '48162',
                                  '28055',
                                  '41419',
                                  '80332',
                                  '38607',
                                  '77817',
                                  '24891',
                                  '16226',
                                  '18410',
                                  '21231',
                                  '59345',
                                  '13918',
                                  '51089',
                                  '20317',
                                  '17167',
                                  '54585',
                                  '67881',
                                  '78366',
                                  '47770',
                                  '18360',
                                  '51717',
                                  '73108',
                                  '14440',
                                  '21800',
                                  '89338',
                                  '45859',
                                  '65501',
                                  '34948',
                                  '25973',
                                  '73219',
                                  '25333',
                                  '17291',
                                  '10374',
                                  '18829',
                                  '60736',
                                  '82620',
                                  '41351',
                                  '52094',
                                  '19326',
                                  '25214',
                                  '54207',
                                  '40936',
                                  '21814',
                                  '79077',
                                  '25178',
                                  '75742',
                                  '77454',
                                  '30621',
                                  '89193',
                                  '27369',
                                  '41232',
                                  '48567',
                                  '83041',
                                  '71948',
                                  '37119',
                                  '68341',
                                  '14073',
                                  '16891',
                                  '62878',
                                  '49130',
                                  '19833',
                                  '24286',
                                  '27700',
                                  '40979',
                                  '50412',
                                  '81504',
                                  '94835',
                                  '84844',
                                  '71954',
                                  '39503',
                                  '57649',
                                  '18434',
                                  '24987',
                                  '12350',
                                  '86379',
                                  '27413',
                                  '44529',
                                  '98569',
                                  '16515',
                                  '27287',
                                  '24255',
                                  '21094',
                                  '16005',
                                  '56436',
                                  '91110',
                                  '68293',
                                  '56455',
                                  '54558',
                                  '10298',
                                  '83647',
                                  '32754',
                                  '27052',
                                  '51766',
                                  '19444',
                                  '13869',
                                  '45645',
                                  '94791',
                                  '57631',
                                  '20712',
                                  '37788',
                                  '41807',
                                  '46507',
                                  '21727',
                                  '71836',
                                  '81070',
                                  '50632',
                                  '88086',
                                  '63991',
                                  '20244',
                                  '31655',
                                  '51782',
                                  '29818',
                                  '63792',
                                  '68605',
                                  '94898',
                                  '36430',
                                  '57025',
                                  '20601',
                                  '82080',
                                  '33869',
                                  '22728',
                                  '35834',
                                  '29086',
                                  '92645',
                                  '98584',
                                  '98072',
                                  '11652',
                                  '78093',
                                  '57553',
                                  '43830',
                                  '71144',
                                  '53565',
                                  '18700',
                                  '90209',
                                  '71256',
                                  '38353',
                                  '54364',
                                  '28571',
                                  '96560',
                                  '57839',
                                  '56355',
                                  '50679',
                                  '45266',
                                  '84680',
                                  '34306',
                                  '34972',
                                  '48530',
                                  '30106',
                                  '15371',
                                  '92380',
                                  '84247',
                                  '92292',
                                  '68852',
                                  '13338',
                                  '34594',
                                  '82602',
                                  '70073',
                                  '98069',
                                  '85066',
                                  '47289',
                                  '11686',
                                  '98862',
                                  '26217',
                                  '47529',
                                  '63294',
                                  '51793',
                                  '35926',
                                  '24227',
                                  '14196',
                                  '24594',
                                  '32489',
                                  '99060',
                                  '49472',
                                  '43432',
                                  '49211',
                                  '14312',
                                  '88137',
                                  '47369',
                                  '56877',
                                  '20534',
                                  '81755',
                                  '15794',
                                  '12318',
                                  '21060',
                                  '73134',
                                  '41255',
                                  '63073',
                                  '81003',
                                  '73873',
                                  '66057',
                                  '51184',
                                  '51195',
                                  '45676',
                                  '92696',
                                  '70450',
                                  '90669',
                                  '98338',
                                  '25264',
                                  '38919',
                                  '59226',
                                  '58581',
                                  '60298',
                                  '17895',
                                  '19489',
                                  '52301',
                                  '80846',
                                  '95464',
                                  '68770',
                                  '51634',
                                  '19988',
                                  '18367',
                                  '18421',
                                  '11618',
                                  '67975',
                                  '25494',
                                  '41352',
                                  '95430',
                                  '15734',
                                  '62585',
                                  '97173',
                                  '33773',
                                  '10425',
                                  '75675',
                                  '53535',
                                  '17879',
                                  '41967',
                                  '12197',
                                  '67998',
                                  '79658',
                                  '59130',
                                  '72592',
                                  '14851',
                                  '43933',
                                  '68101',
                                  '50636',
                                  '25717',
                                  '71286',
                                  '24660',
                                  '58058',
                                  '72991',
                                  '95042',
                                  '15543',
                                  '33122',
                                  '69280',
                                  '11912',
                                  '59386',
                                  '27642',
                                  '65177',
                                  '17672',
                                  '33467',
                                  '64592',
                                  '36335',
                                  '54010',
                                  '18767',
                                  '63193',
                                  '42361',
                                  '49254',
                                  '33113',
                                  '33159',
                                  '36479',
                                  '59080',
                                  '11855',
                                  '81963',
                                  '31016',
                                  '49140',
                                  '29392',
                                  '41836',
                                  '32958',
                                  '53163',
                                  '13844',
                                  '73146',
                                  '23952',
                                  '65148',
                                  '93498',
                                  '14530',
                                  '46131',
                                  '58454',
                                  '13376',
                                  '13378',
                                  '83986',
                                  '12320',
                                  '17193',
                                  '59852',
                                  '46081',
                                  '98533',
                                  '52389',
                                  '13086',
                                  '68843',
                                  '31013',
                                  '13261',
                                  '60560',
                                  '13443',
                                  '45533',
                                  '83583',
                                  '11489',
                                  '58218',
                                  '19753',
                                  '22911',
                                  '25115',
                                  '86709',
                                  '27156',
                                  '32669',
                                  '13123',
                                  '51933',
                                  '39214',
                                  '41331',
                                  '66943',
                                  '14155',
                                  '69998',
                                  '49101',
                                  '70070',
                                  '35076',
                                  '14242',
                                  '73021',
                                  '59494',
                                  '15782',
                                  '29752',
                                  '37914',
                                  '74686',
                                  '83086',
                                  '34473',
                                  '15751',
                                  '81084',
                                  '49230',
                                  '91894',
                                  '60624',
                                  '17819',
                                  '28810',
                                  '63180',
                                  '56224',
                                  '39459',
                                  '55233',
                                  '75752',
                                  '43639',
                                  '55349',
                                  '86057',
                                  '62361',
                                  '50788',
                                  '31830',
                                  '58062',
                                  '18218',
                                  '85761',
                                  '60083',
                                  '45484',
                                  '21204',
                                  '90229',
                                  '70041',
                                  '41162',
                                  '35390',
                                  '16364',
                                  '39500',
                                  '68908',
                                  '26689',
                                  '52868',
                                  '81335',
                                  '40146',
                                  '11340',
                                  '61527',
                                  '61794',
                                  '71997',
                                  '30415',
                                  '59004',
                                  '29450',
                                  '58117',
                                  '69952',
                                  '33562',
                                  '83833',
                                  '27385',
                                  '61860',
                                  '96435',
                                  '48333',
                                  '23065',
                                  '32961',
                                  '84919',
                                  '61997',
                                  '99132',
                                  '22815',
                                  '56600',
                                  '68730',
                                  '48017',
                                  '95694',
                                  '32919',
                                  '88217',
                                  '27116',
                                  '28239',
                                  '58032',
                                  '18884',
                                  '16791',
                                  '21343',
                                  '97462',
                                  '18569',
                                  '75660',
                                  '15475') INTERSECT
     SELECT substr(ca_zip, 1, 5) AS ca_zip
     FROM customer_address
     JOIN customer ON ca_address_sk = c_current_addr_sk WHERE c_preferred_cust_flag = 'Y'
   GROUP BY substr(ca_zip, 1, 5)
   HAVING COUNT(*) > 10) AS V1 ON substr(s_zip, 0, 1) = substr(V1.ca_zip, 0, 1)
WHERE d_qoy = 2
  AND d_year = 1998
GROUP BY s_store_name
ORDER BY s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN312
-- =================================================================
SELECT AVG(CASE
               WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_ext_tax
               ELSE NULL
           END) AS bucket1_ext_tax,
       AVG(CASE
               WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_net_paid_inc_tax
               ELSE NULL
           END) AS bucket1_net_paid_inc_tax,
       AVG(CASE
               WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_ext_tax
               ELSE NULL
           END) AS bucket2_ext_tax,
       AVG(CASE
               WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_net_paid_inc_tax
               ELSE NULL
           END) AS bucket2_net_paid_inc_tax,
       AVG(CASE
               WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_ext_tax
               ELSE NULL
           END) AS bucket3_ext_tax,
       AVG(CASE
               WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_net_paid_inc_tax
               ELSE NULL
           END) AS bucket3_net_paid_inc_tax,
       AVG(CASE
               WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_ext_tax
               ELSE NULL
           END) AS bucket4_ext_tax,
       AVG(CASE
               WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_net_paid_inc_tax
               ELSE NULL
           END) AS bucket4_net_paid_inc_tax,
       AVG(CASE
               WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_ext_tax
               ELSE NULL
           END) AS bucket5_ext_tax,
       AVG(CASE
               WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_net_paid_inc_tax
               ELSE NULL
           END) AS bucket5_net_paid_inc_tax
FROM store_sales
WHERE ss_quantity BETWEEN 1 AND 20
  OR ss_quantity BETWEEN 21 AND 40
  OR ss_quantity BETWEEN 41 AND 60
  OR ss_quantity BETWEEN 61 AND 80
  OR ss_quantity BETWEEN 81 AND 100
HAVING COUNT(CASE
                 WHEN ss_quantity BETWEEN 1 AND 20 THEN 1
                 ELSE NULL
             END) > 1071
OR COUNT(CASE
             WHEN ss_quantity BETWEEN 21 AND 40 THEN 1
             ELSE NULL
         END) > 39161
OR COUNT(CASE
             WHEN ss_quantity BETWEEN 41 AND 60 THEN 1
             ELSE NULL
         END) > 29434
OR COUNT(CASE
             WHEN ss_quantity BETWEEN 61 AND 80 THEN 1
             ELSE NULL
         END) > 6568
OR COUNT(CASE
             WHEN ss_quantity BETWEEN 81 AND 100 THEN 1
             ELSE NULL
         END) > 21216 ;

-- =================================================================
-- Query ID: TPCDSN313
-- =================================================================
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       COUNT(*) AS cnt1,
       cd_purchase_estimate,
       COUNT(*) AS cnt2,
       cd_credit_rating,
       COUNT(*) AS cnt3,
       cd_dep_count,
       COUNT(*) AS cnt4,
       cd_dep_employed_count,
       COUNT(*) AS cnt5,
       cd_dep_college_count,
       COUNT(*) AS cnt6
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE ca.ca_county IN ('Fairfield County',
                       'Campbell County',
                       'Washtenaw County',
                       'Escambia County',
                       'Cleburne County',
                       'United States',
                       '1')
  AND (EXISTS
         (SELECT 1
          FROM store_sales ss
          JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
          WHERE c.c_customer_sk = ss.ss_customer_sk
            AND d.d_year = 1999
            AND d.d_moy BETWEEN 1 AND 12 )
       OR EXISTS
         (SELECT 1
          FROM web_sales ws
          JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
          WHERE c.c_customer_sk = ws.ws_bill_customer_sk
            AND d.d_year = 1999
            AND d.d_moy BETWEEN 1 AND 12 )
       OR EXISTS
         (SELECT 1
          FROM catalog_sales cs
          JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
          WHERE c.c_customer_sk = cs.cs_ship_customer_sk
            AND d.d_year = 1999
            AND d.d_moy BETWEEN 1 AND 12 ))
GROUP BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
ORDER BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN314
-- =================================================================
WITH year_total AS
  (SELECT c_customer_id AS customer_id,
          c_first_name AS customer_first_name,
          c_last_name AS customer_last_name,
          c_preferred_cust_flag AS customer_preferred_cust_flag,
          c_birth_country AS customer_birth_country,
          c_login AS customer_login,
          c_email_address AS customer_email_address,
          d_year AS dyear,
          SUM(CASE
                  WHEN ss_customer_sk IS NOT NULL THEN ss_ext_list_price - ss_ext_discount_amt
                  ELSE ws_ext_list_price - ws_ext_discount_amt
              END) AS year_total,
          CASE
              WHEN ss_customer_sk IS NOT NULL THEN 's'
              ELSE 'w'
          END AS sale_type
   FROM customer
   LEFT JOIN store_sales ON c_customer_sk = ss_customer_sk
   LEFT JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
   JOIN date_dim ON COALESCE(ss_sold_date_sk, ws_sold_date_sk) = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year,
            CASE
                WHEN ss_customer_sk IS NOT NULL THEN 's'
                ELSE 'w'
            END)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_email_address
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 1999
  AND t_s_secyear.dyear = 1999
  AND t_w_firstyear.dyear = 1999
  AND t_w_secyear.dyear = 1999
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE
          WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
          ELSE 0.0
      END >= CASE
                 WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                 ELSE 0.0
             END
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_email_address
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN315
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ws_ext_sales_price) AS itemrevenue,
       SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
WHERE i_category IN ('Men',
                     'Books',
                     'Electronics')
  AND d_date BETWEEN CAST('2001-06-15' AS DATE) AND (CAST('2001-06-15' AS DATE) + INTERVAL '30' DAY)
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN316
-- =================================================================
SELECT AVG(ss_quantity),
       AVG(ss_ext_sales_price),
       AVG(ss_ext_wholesale_cost),
       SUM(ss_ext_wholesale_cost)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2001
  AND ((cd_marital_status = 'M'
        AND cd_education_status = 'College'
        AND ss_sales_price BETWEEN 100.00 AND 150.00
        AND hd_dep_count = 3)
       OR (cd_marital_status = 'D'
           AND cd_education_status = 'Primary'
           AND ss_sales_price BETWEEN 50.00 AND 100.00
           AND hd_dep_count = 1)
       OR (cd_marital_status = 'W'
           AND cd_education_status = '2 yr Degree'
           AND ss_sales_price BETWEEN 150.00 AND 200.00
           AND hd_dep_count = 1))
  AND ((ca_country = 'United States'
        AND ca_state IN ('IL',
                         'TN',
                         'TX')
        AND ss_net_profit BETWEEN 100 AND 200)
       OR (ca_country = 'United States'
           AND ca_state IN ('WY',
                            'OH',
                            'ID')
           AND ss_net_profit BETWEEN 150 AND 300)
       OR (ca_country = 'United States'
           AND ca_state IN ('MS',
                            'SC',
                            'IA')
           AND ss_net_profit BETWEEN 50 AND 250)) ;

-- =================================================================
-- Query ID: TPCDSN317
-- =================================================================
SELECT *
FROM customer ;

-- =================================================================
-- Query ID: TPCDSN318
-- =================================================================
SELECT ca_zip,
       SUM(cs_sales_price)
FROM catalog_sales
JOIN customer ON cs_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE (SUBSTR(ca_zip, 1, 5) IN ('85669',
                                '86197',
                                '88274',
                                '83405',
                                '86475',
                                '85392',
                                '85460',
                                '80348',
                                '81792')
       OR ca_state IN ('CA',
                       'WA',
                       'GA')
       OR cs_sales_price > 500)
  AND d_qoy = 2
  AND d_year = 2001
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN319
-- =================================================================
SELECT COUNT(DISTINCT cs_order_number) AS "order count",
       SUM(cs_ext_ship_cost) AS "total shipping cost",
       SUM(cs_net_profit) AS "total net profit"
FROM catalog_sales cs1
JOIN date_dim ON cs1.cs_ship_date_sk = d_date_sk
JOIN customer_address ON cs1.cs_ship_addr_sk = ca_address_sk
JOIN call_center ON cs1.cs_call_center_sk = cc_call_center_sk
WHERE d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS date) + INTERVAL '60' day)
  AND ca_state = 'PA'
  AND cc_county IN ('Williamson County')
  AND EXISTS
    (SELECT 1
     FROM catalog_sales cs2
     WHERE cs1.cs_order_number = cs2.cs_order_number
       AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk )
  AND NOT EXISTS
    (SELECT 1
     FROM catalog_returns cr1
     WHERE cs1.cs_order_number = cr1.cr_order_number )
ORDER BY "order count"
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN320
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       s_state,
       COUNT(ss_quantity) AS store_sales_quantitycount,
       AVG(ss_quantity) AS store_sales_quantityave,
       STDDEV_SAMP(ss_quantity) AS store_sales_quantitystdev,
       STDDEV_SAMP(ss_quantity) / AVG(ss_quantity) AS store_sales_quantitycov,
       COUNT(sr_return_quantity) AS store_returns_quantitycount,
       AVG(sr_return_quantity) AS store_returns_quantityave,
       STDDEV_SAMP(sr_return_quantity) AS store_returns_quantitystdev,
       STDDEV_SAMP(sr_return_quantity) / AVG(sr_return_quantity) AS store_returns_quantitycov,
       COUNT(cs_quantity) AS catalog_sales_quantitycount,
       AVG(cs_quantity) AS catalog_sales_quantityave,
       STDDEV_SAMP(cs_quantity) AS catalog_sales_quantitystdev,
       STDDEV_SAMP(cs_quantity) / AVG(cs_quantity) AS catalog_sales_quantitycov
FROM store_sales
JOIN store_returns ON ss_customer_sk = sr_customer_sk
AND ss_item_sk = sr_item_sk
AND ss_ticket_number = sr_ticket_number
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
AND sr_item_sk = cs_item_sk
JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN item ON i_item_sk = ss_item_sk
WHERE d1.d_quarter_name = '2001Q1'
  AND d2.d_quarter_name IN ('2001Q1',
                            '2001Q2',
                            '2001Q3')
  AND d3.d_quarter_name IN ('2001Q1',
                            '2001Q2',
                            '2001Q3')
GROUP BY i_item_id,
         i_item_desc,
         s_state
ORDER BY i_item_id,
         i_item_desc,
         s_state
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN321
-- =================================================================
SELECT i_item_id,
       ca_country,
       ca_state,
       ca_county,
       AVG(CAST(cs_quantity AS decimal(12, 2))) AS agg1,
       AVG(CAST(cs_list_price AS decimal(12, 2))) AS agg2,
       AVG(CAST(cs_coupon_amt AS decimal(12, 2))) AS agg3,
       AVG(CAST(cs_sales_price AS decimal(12, 2))) AS agg4,
       AVG(CAST(cs_net_profit AS decimal(12, 2))) AS agg5,
       AVG(CAST(c_birth_year AS decimal(12, 2))) AS agg6,
       AVG(CAST(cd1.cd_dep_count AS decimal(12, 2))) AS agg7
FROM catalog_sales
JOIN customer_demographics cd1 ON cs_bill_cdemo_sk = cd1.cd_demo_sk
JOIN customer ON cs_bill_customer_sk = c_customer_sk
JOIN customer_demographics cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN item ON cs_item_sk = i_item_sk
WHERE cd1.cd_gender = 'F'
  AND cd1.cd_education_status = 'Primary'
  AND c_birth_month IN (1,
                        3,
                        7,
                        11,
                        10,
                        4)
  AND d_year = 2001
  AND ca_state IN ('AL',
                   'MO',
                   'TN',
                   'GA',
                   'MT',
                   'IN',
                   'CA')
GROUP BY ROLLUP (i_item_id,
                 ca_country,
                 ca_state,
                 ca_county)
ORDER BY ca_country,
         ca_state,
         ca_county,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN322
-- =================================================================
SELECT i_brand_id AS brand_id,
       i_brand,
       i_manufact_id,
       i_manufact,
       SUM(ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN date_dim ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE i_manager_id = 14
  AND d_moy = 11
  AND d_year = 2002
  AND substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
GROUP BY i_brand,
         i_brand_id,
         i_manufact_id,
         i_manufact
ORDER BY ext_price DESC,
         i_brand,
         i_brand_id,
         i_manufact_id,
         i_manufact
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN323
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(cs_ext_sales_price) AS itemrevenue,
       SUM(cs_ext_sales_price) * 100 / SUM(SUM(cs_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM catalog_sales
JOIN item ON cs_item_sk = i_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE i_category IN ('Books',
                     'Music',
                     'Sports')
  AND d_date BETWEEN CAST('2002-06-18' AS date) AND (CAST('2002-06-18' AS date) + INTERVAL '30' DAY)
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN324
-- =================================================================
SELECT w_warehouse_name,
       i_item_id,
       SUM(CASE
               WHEN (CAST(d_date AS date) < CAST('1999-06-22' AS date)) THEN inv_quantity_on_hand
               ELSE 0
           END) AS inv_before,
       SUM(CASE
               WHEN (CAST(d_date AS date) >= CAST('1999-06-22' AS date)) THEN inv_quantity_on_hand
               ELSE 0
           END) AS inv_after
FROM inventory
JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
JOIN item ON i_item_sk = inv_item_sk
JOIN date_dim ON inv_date_sk = d_date_sk
WHERE i_current_price BETWEEN 0.99 AND 1.49
  AND d_date BETWEEN (CAST('1999-06-22' AS date) - INTERVAL '30' DAY) AND (CAST('1999-06-22' AS date) + INTERVAL '30' DAY)
GROUP BY w_warehouse_name,
         i_item_id
HAVING (CASE
            WHEN SUM(CASE
                         WHEN (CAST(d_date AS date) < CAST('1999-06-22' AS date)) THEN inv_quantity_on_hand
                         ELSE 0
                     END) > 0 THEN SUM(CASE
                                           WHEN (CAST(d_date AS date) >= CAST('1999-06-22' AS date)) THEN inv_quantity_on_hand
                                           ELSE 0
                                       END) / SUM(CASE
                                                      WHEN (CAST(d_date AS date) < CAST('1999-06-22' AS date)) THEN inv_quantity_on_hand
                                                      ELSE 0
                                                  END)
            ELSE NULL
        END) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
ORDER BY w_warehouse_name,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN325
-- =================================================================
SELECT i_product_name,
       i_brand,
       i_class,
       i_category,
       AVG(inv_quantity_on_hand) AS qoh
FROM inventory
JOIN date_dim ON inv_date_sk = d_date_sk
JOIN item ON inv_item_sk = i_item_sk
WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
GROUP BY ROLLUP(i_product_name, i_brand, i_class, i_category)
ORDER BY qoh,
         i_product_name,
         i_brand,
         i_class,
         i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN326
-- =================================================================
SELECT *
FROM customer ;

-- =================================================================
-- Query ID: TPCDSN327
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'khaki'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN328
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'seashell'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN329
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       MAX(ss_net_profit) AS store_sales_profit,
       MAX(sr_net_loss) AS store_returns_loss,
       MAX(cs_net_profit) AS catalog_sales_profit
FROM store_sales
JOIN store_returns ON ss_customer_sk = sr_customer_sk
AND ss_item_sk = sr_item_sk
AND ss_ticket_number = sr_ticket_number
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
AND sr_item_sk = cs_item_sk
JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN item ON i_item_sk = ss_item_sk
WHERE d1.d_moy = 4
  AND d1.d_year = 1999
  AND d2.d_moy BETWEEN 4 AND 10
  AND d2.d_year = 1999
  AND d3.d_moy BETWEEN 4 AND 10
  AND d3.d_year = 1999
GROUP BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
ORDER BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN330
-- =================================================================
SELECT i_item_id,
       AVG(cs_quantity) AS agg1,
       AVG(cs_list_price) AS agg2,
       AVG(cs_coupon_amt) AS agg3,
       AVG(cs_sales_price) AS agg4
FROM catalog_sales
JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN item ON cs_item_sk = i_item_sk
JOIN promotion ON cs_promo_sk = p_promo_sk
WHERE cd_gender = 'M'
  AND cd_marital_status = 'W'
  AND cd_education_status = 'Unknown'
  AND (p_channel_email = 'N'
       OR p_channel_event = 'N')
  AND d_year = 2002
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN331
-- =================================================================
SELECT i_item_id,
       s_state,
       GROUPING(s_state) AS g_state,
       AVG(ss_quantity) AS agg1,
       AVG(ss_list_price) AS agg2,
       AVG(ss_coupon_amt) AS agg3,
       AVG(ss_sales_price) AS agg4
FROM store_sales
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE cd_gender = 'M'
  AND cd_marital_status = 'W'
  AND cd_education_status = 'Secondary'
  AND d_year = 1999
  AND s_state = 'TN'
GROUP BY ROLLUP (i_item_id,
                 s_state)
ORDER BY i_item_id,
         s_state
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN332
-- =================================================================
SELECT AVG(CASE
               WHEN ss_quantity BETWEEN 0 AND 5
                    AND (ss_list_price BETWEEN 107 AND 117
                         OR ss_coupon_amt BETWEEN 1319 AND 2319
                         OR ss_wholesale_cost BETWEEN 60 AND 80) THEN ss_list_price
           END) AS B1_LP,
       COUNT(CASE
                 WHEN ss_quantity BETWEEN 0 AND 5
                      AND (ss_list_price BETWEEN 107 AND 117
                           OR ss_coupon_amt BETWEEN 1319 AND 2319
                           OR ss_wholesale_cost BETWEEN 60 AND 80) THEN ss_list_price
             END) AS B1_CNT,
       COUNT(DISTINCT CASE
                          WHEN ss_quantity BETWEEN 0 AND 5
                               AND (ss_list_price BETWEEN 107 AND 117
                                    OR ss_coupon_amt BETWEEN 1319 AND 2319
                                    OR ss_wholesale_cost BETWEEN 60 AND 80) THEN ss_list_price
                      END) AS B1_CNTD,
       AVG(CASE
               WHEN ss_quantity BETWEEN 6 AND 10
                    AND (ss_list_price BETWEEN 23 AND 33
                         OR ss_coupon_amt BETWEEN 825 AND 1825
                         OR ss_wholesale_cost BETWEEN 43 AND 63) THEN ss_list_price
           END) AS B2_LP,
       COUNT(CASE
                 WHEN ss_quantity BETWEEN 6 AND 10
                      AND (ss_list_price BETWEEN 23 AND 33
                           OR ss_coupon_amt BETWEEN 825 AND 1825
                           OR ss_wholesale_cost BETWEEN 43 AND 63) THEN ss_list_price
             END) AS B2_CNT,
       COUNT(DISTINCT CASE
                          WHEN ss_quantity BETWEEN 6 AND 10
                               AND (ss_list_price BETWEEN 23 AND 33
                                    OR ss_coupon_amt BETWEEN 825 AND 1825
                                    OR ss_wholesale_cost BETWEEN 43 AND 63) THEN ss_list_price
                      END) AS B2_CNTD,
       AVG(CASE
               WHEN ss_quantity BETWEEN 11 AND 15
                    AND (ss_list_price BETWEEN 74 AND 84
                         OR ss_coupon_amt BETWEEN 4381 AND 5381
                         OR ss_wholesale_cost BETWEEN 57 AND 77) THEN ss_list_price
           END) AS B3_LP,
       COUNT(CASE
                 WHEN ss_quantity BETWEEN 11 AND 15
                      AND (ss_list_price BETWEEN 74 AND 84
                           OR ss_coupon_amt BETWEEN 4381 AND 5381
                           OR ss_wholesale_cost BETWEEN 57 AND 77) THEN ss_list_price
             END) AS B3_CNT,
       COUNT(DISTINCT CASE
                          WHEN ss_quantity BETWEEN 11 AND 15
                               AND (ss_list_price BETWEEN 74 AND 84
                                    OR ss_coupon_amt BETWEEN 4381 AND 5381
                                    OR ss_wholesale_cost BETWEEN 57 AND 77) THEN ss_list_price
                      END) AS B3_CNTD,
       AVG(CASE
               WHEN ss_quantity BETWEEN 16 AND 20
                    AND (ss_list_price BETWEEN 89 AND 99
                         OR ss_coupon_amt BETWEEN 3117 AND 4117
                         OR ss_wholesale_cost BETWEEN 68 AND 88) THEN ss_list_price
           END) AS B4_LP,
       COUNT(CASE
                 WHEN ss_quantity BETWEEN 16 AND 20
                      AND (ss_list_price BETWEEN 89 AND 99
                           OR ss_coupon_amt BETWEEN 3117 AND 4117
                           OR ss_wholesale_cost BETWEEN 68 AND 88) THEN ss_list_price
             END) AS B4_CNT,
       COUNT(DISTINCT CASE
                          WHEN ss_quantity BETWEEN 16 AND 20
                               AND (ss_list_price BETWEEN 89 AND 99
                                    OR ss_coupon_amt BETWEEN 3117 AND 4117
                                    OR ss_wholesale_cost BETWEEN 68 AND 88) THEN ss_list_price
                      END) AS B4_CNTD,
       AVG(CASE
               WHEN ss_quantity BETWEEN 21 AND 25
                    AND (ss_list_price BETWEEN 58 AND 68
                         OR ss_coupon_amt BETWEEN 9402 AND 10402
                         OR ss_wholesale_cost BETWEEN 38 AND 58) THEN ss_list_price
           END) AS B5_LP,
       COUNT(CASE
                 WHEN ss_quantity BETWEEN 21 AND 25
                      AND (ss_list_price BETWEEN 58 AND 68
                           OR ss_coupon_amt BETWEEN 9402 AND 10402
                           OR ss_wholesale_cost BETWEEN 38 AND 58) THEN ss_list_price
             END) AS B5_CNT,
       COUNT(DISTINCT CASE
                          WHEN ss_quantity BETWEEN 21 AND 25
                               AND (ss_list_price BETWEEN 58 AND 68
                                    OR ss_coupon_amt BETWEEN 9402 AND 10402
                                    OR ss_wholesale_cost BETWEEN 38 AND 58) THEN ss_list_price
                      END) AS B5_CNTD,
       AVG(CASE
               WHEN ss_quantity BETWEEN 26 AND 30
                    AND (ss_list_price BETWEEN 64 AND 74
                         OR ss_coupon_amt BETWEEN 5792 AND 6792
                         OR ss_wholesale_cost BETWEEN 73 AND 93) THEN ss_list_price
           END) AS B6_LP,
       COUNT(CASE
                 WHEN ss_quantity BETWEEN 26 AND 30
                      AND (ss_list_price BETWEEN 64 AND 74
                           OR ss_coupon_amt BETWEEN 5792 AND 6792
                           OR ss_wholesale_cost BETWEEN 73 AND 93) THEN ss_list_price
             END) AS B6_CNT,
       COUNT(DISTINCT CASE
                          WHEN ss_quantity BETWEEN 26 AND 30
                               AND (ss_list_price BETWEEN 64 AND 74
                                    OR ss_coupon_amt BETWEEN 5792 AND 6792
                                    OR ss_wholesale_cost BETWEEN 73 AND 93) THEN ss_list_price
                      END) AS B6_CNTD
FROM store_sales
WHERE (ss_quantity BETWEEN 0 AND 5
       AND (ss_list_price BETWEEN 107 AND 117
            OR ss_coupon_amt BETWEEN 1319 AND 2319
            OR ss_wholesale_cost BETWEEN 60 AND 80))
  OR (ss_quantity BETWEEN 6 AND 10
      AND (ss_list_price BETWEEN 23 AND 33
           OR ss_coupon_amt BETWEEN 825 AND 1825
           OR ss_wholesale_cost BETWEEN 43 AND 63))
  OR (ss_quantity BETWEEN 11 AND 15
      AND (ss_list_price BETWEEN 74 AND 84
           OR ss_coupon_amt BETWEEN 4381 AND 5381
           OR ss_wholesale_cost BETWEEN 57 AND 77))
  OR (ss_quantity BETWEEN 16 AND 20
      AND (ss_list_price BETWEEN 89 AND 99
           OR ss_coupon_amt BETWEEN 3117 AND 4117
           OR ss_wholesale_cost BETWEEN 68 AND 88))
  OR (ss_quantity BETWEEN 21 AND 25
      AND (ss_list_price BETWEEN 58 AND 68
           OR ss_coupon_amt BETWEEN 9402 AND 10402
           OR ss_wholesale_cost BETWEEN 38 AND 58))
  OR (ss_quantity BETWEEN 26 AND 30
      AND (ss_list_price BETWEEN 64 AND 74
           OR ss_coupon_amt BETWEEN 5792 AND 6792
           OR ss_wholesale_cost BETWEEN 73 AND 93))
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN333
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       MAX(ss_quantity) AS store_sales_quantity,
       MAX(sr_return_quantity) AS store_returns_quantity,
       MAX(cs_quantity) AS catalog_sales_quantity
FROM store_sales
JOIN store_returns ON ss_customer_sk = sr_customer_sk
AND ss_item_sk = sr_item_sk
AND ss_ticket_number = sr_ticket_number
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
AND sr_item_sk = cs_item_sk
JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN item ON i_item_sk = ss_item_sk
WHERE d1.d_moy = 4
  AND d1.d_year = 1998
  AND d2.d_moy BETWEEN 4 AND 4 + 3
  AND d2.d_year = 1998
  AND d3.d_year IN (1998,
                    1999,
                    2000)
GROUP BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
ORDER BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN334
-- =================================================================
WITH customer_total_return AS
  (SELECT wr_returning_customer_sk AS ctr_customer_sk,
          ca_state AS ctr_state,
          SUM(wr_return_amt) AS ctr_total_return
   FROM web_returns
   JOIN date_dim ON wr_returned_date_sk = d_date_sk
   JOIN customer_address ON wr_returning_addr_sk = ca_address_sk
   WHERE d_year = 2000
   GROUP BY wr_returning_customer_sk,
            ca_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       c_preferred_cust_flag,
       c_birth_day,
       c_birth_month,
       c_birth_year,
       c_birth_country,
       c_login,
       c_email_address,
       c_last_review_date,
       ctr_total_return
FROM customer_total_return ctr1
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
WHERE ctr1.ctr_total_return >
    (SELECT AVG(ctr_total_return) * 1.2
     FROM customer_total_return ctr2
     WHERE ctr1.ctr_state = ctr2.ctr_state )
  AND ca_state = 'IN'
ORDER BY c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         c_preferred_cust_flag,
         c_birth_day,
         c_birth_month,
         c_birth_year,
         c_birth_country,
         c_login,
         c_email_address,
         c_last_review_date,
         ctr_total_return
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN335
-- =================================================================
SELECT ca_county,
       d_year,
       SUM(CASE
               WHEN d_qoy = 2 THEN web_sales
           END) / SUM(CASE
                          WHEN d_qoy = 1 THEN web_sales
                      END) AS web_q1_q2_increase,
       SUM(CASE
               WHEN d_qoy = 2 THEN store_sales
           END) / SUM(CASE
                          WHEN d_qoy = 1 THEN store_sales
                      END) AS store_q1_q2_increase,
       SUM(CASE
               WHEN d_qoy = 3 THEN web_sales
           END) / SUM(CASE
                          WHEN d_qoy = 2 THEN web_sales
                      END) AS web_q2_q3_increase,
       SUM(CASE
               WHEN d_qoy = 3 THEN store_sales
           END) / SUM(CASE
                          WHEN d_qoy = 2 THEN store_sales
                      END) AS store_q2_q3_increase
FROM
  (SELECT ca_county,
          d_qoy,
          d_year,
          SUM(ss_ext_sales_price) AS store_sales,
          0 AS web_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer_address ON ss_addr_sk = ca_address_sk
   WHERE d_year = 1999
     AND d_qoy IN (1,
                   2,
                   3)
   GROUP BY ca_county,
            d_qoy,
            d_year
   UNION ALL SELECT ca_county,
                    d_qoy,
                    d_year,
                    0 AS store_sales,
                    SUM(ws_ext_sales_price) AS web_sales
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
   WHERE d_year = 1999
     AND d_qoy IN (1,
                   2,
                   3)
   GROUP BY ca_county,
            d_qoy,
            d_year) AS sales_data
GROUP BY ca_county,
         d_year
HAVING SUM(CASE
               WHEN d_qoy = 1 THEN web_sales
           END) > 0
AND SUM(CASE
            WHEN d_qoy = 1 THEN store_sales
        END) > 0
AND SUM(CASE
            WHEN d_qoy = 2 THEN web_sales
        END) > 0
AND SUM(CASE
            WHEN d_qoy = 2 THEN store_sales
        END) > 0
AND SUM(CASE
            WHEN d_qoy = 3 THEN web_sales
        END) > 0
AND SUM(CASE
            WHEN d_qoy = 3 THEN store_sales
        END) > 0
AND SUM(CASE
            WHEN d_qoy = 2 THEN web_sales
        END) / SUM(CASE
                       WHEN d_qoy = 1 THEN web_sales
                   END) > SUM(CASE
                                  WHEN d_qoy = 2 THEN store_sales
                              END) / SUM(CASE
                                             WHEN d_qoy = 1 THEN store_sales
                                         END)
AND SUM(CASE
            WHEN d_qoy = 3 THEN web_sales
        END) / SUM(CASE
                       WHEN d_qoy = 2 THEN web_sales
                   END) > SUM(CASE
                                  WHEN d_qoy = 3 THEN store_sales
                              END) / SUM(CASE
                                             WHEN d_qoy = 2 THEN store_sales
                                         END)
ORDER BY store_q2_q3_increase ;

-- =================================================================
-- Query ID: TPCDSN336
-- =================================================================
SELECT SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales,
     item,
     date_dim
WHERE i_manufact_id = 722
  AND i_item_sk = cs_item_sk
  AND d_date BETWEEN '2001-03-09' AND (CAST('2001-03-09' AS date) + INTERVAL '90' day)
  AND d_date_sk = cs_sold_date_sk
  AND cs_ext_discount_amt >
    (SELECT 1.3 * AVG(cs_ext_discount_amt)
     FROM catalog_sales,
          date_dim
     WHERE cs_item_sk = i_item_sk
       AND d_date BETWEEN '2001-03-09' AND (CAST('2001-03-09' AS date) + INTERVAL '90' day)
       AND d_date_sk = cs_sold_date_sk )
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN337
-- =================================================================
SELECT i_manufact_id,
       SUM(total_sales) total_sales
FROM
  (SELECT i_manufact_id,
          SUM(ss_ext_sales_price) total_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer_address ON ss_addr_sk = ca_address_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE i_manufact_id IN
       (SELECT i_manufact_id
        FROM item
        WHERE i_category IN ('Books'))
     AND d_year = 2001
     AND d_moy = 3
     AND ca_gmt_offset = -5
   GROUP BY i_manufact_id
   UNION ALL SELECT i_manufact_id,
                    SUM(cs_ext_sales_price) total_sales
   FROM catalog_sales
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN customer_address ON cs_bill_addr_sk = ca_address_sk
   JOIN item ON cs_item_sk = i_item_sk
   WHERE i_manufact_id IN
       (SELECT i_manufact_id
        FROM item
        WHERE i_category IN ('Books'))
     AND d_year = 2001
     AND d_moy = 3
     AND ca_gmt_offset = -5
   GROUP BY i_manufact_id
   UNION ALL SELECT i_manufact_id,
                    SUM(ws_ext_sales_price) total_sales
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
   JOIN item ON ws_item_sk = i_item_sk
   WHERE i_manufact_id IN
       (SELECT i_manufact_id
        FROM item
        WHERE i_category IN ('Books'))
     AND d_year = 2001
     AND d_moy = 3
     AND ca_gmt_offset = -5
   GROUP BY i_manufact_id) tmp1
GROUP BY i_manufact_id
ORDER BY total_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN338
-- =================================================================
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          COUNT(*) AS cnt
   FROM store_sales
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   JOIN store ON store_sales.ss_store_sk = store.s_store_sk
   JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
   WHERE (date_dim.d_dom BETWEEN 1 AND 3
          OR date_dim.d_dom BETWEEN 25 AND 28)
     AND (household_demographics.hd_buy_potential = '1001-5000'
          OR household_demographics.hd_buy_potential = '0-500')
     AND household_demographics.hd_vehicle_count > 0
     AND (CASE
              WHEN household_demographics.hd_vehicle_count > 0 THEN household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
              ELSE NULL
          END) > 1.2
     AND date_dim.d_year IN (2000,
                             2001,
                             2002)
     AND store.s_county = 'Williamson County'
   GROUP BY ss_ticket_number,
            ss_customer_sk) dn
JOIN customer ON ss_customer_sk = c_customer_sk
WHERE cnt BETWEEN 15 AND 20
ORDER BY c_last_name,
         c_first_name,
         c_salutation,
         c_preferred_cust_flag DESC,
         ss_ticket_number ;

-- =================================================================
-- Query ID: TPCDSN339
-- =================================================================
SELECT ca_state,
       cd_gender,
       cd_marital_status,
       cd_dep_count,
       COUNT(*) AS cnt1,
       AVG(cd_dep_count),
       STDDEV_SAMP(cd_dep_count),
       SUM(cd_dep_count),
       cd_dep_employed_count,
       COUNT(*) AS cnt2,
       AVG(cd_dep_employed_count),
       STDDEV_SAMP(cd_dep_employed_count),
       SUM(cd_dep_employed_count),
       cd_dep_college_count,
       COUNT(*) AS cnt3,
       AVG(cd_dep_college_count),
       STDDEV_SAMP(cd_dep_college_count),
       SUM(cd_dep_college_count)
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS
    (SELECT 1
     FROM store_sales
     JOIN date_dim ON ss_sold_date_sk = d_date_sk
     WHERE c.c_customer_sk = ss_customer_sk
       AND d_year = 1999
       AND d_qoy < 4 )
  AND (EXISTS
         (SELECT 1
          FROM web_sales
          JOIN date_dim ON ws_sold_date_sk = d_date_sk
          WHERE c.c_customer_sk = ws_bill_customer_sk
            AND d_year = 1999
            AND d_qoy < 4 )
       OR EXISTS
         (SELECT 1
          FROM catalog_sales
          JOIN date_dim ON cs_sold_date_sk = d_date_sk
          WHERE c.c_customer_sk = cs_ship_customer_sk
            AND d_year = 1999
            AND d_qoy < 4 ))
GROUP BY ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
ORDER BY ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN340
-- =================================================================
SELECT *
FROM customer ;

-- =================================================================
-- Query ID: TPCDSN341
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
JOIN catalog_sales ON cs_item_sk = i_item_sk
WHERE i_current_price BETWEEN 29 AND 59
  AND d_date BETWEEN CAST('2002-03-29' AS date) AND (CAST('2002-03-29' AS date) + INTERVAL '60' day)
  AND i_manufact_id IN (393,
                        174,
                        251,
                        445)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN342
-- =================================================================
SELECT COUNT(*)
FROM
  (SELECT DISTINCT c_last_name,
                   c_first_name,
                   d_date
   FROM
     (SELECT c_last_name,
             c_first_name,
             d_date
      FROM store_sales
      JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1189 AND 1189 + 11
      UNION SELECT c_last_name,
                   c_first_name,
                   d_date
      FROM catalog_sales
      JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1189 AND 1189 + 11
      UNION SELECT c_last_name,
                   c_first_name,
                   d_date
      FROM web_sales
      JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1189 AND 1189 + 11 ) AS combined_sales) AS hot_cust
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN343
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN344
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
  and inv1.cov > 1.5
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN345
-- =================================================================
SELECT w_state,
       i_item_id,
       SUM(CASE
               WHEN (CAST(d_date AS date) < CAST('2001-05-02' AS date)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
               ELSE 0
           END) AS sales_before,
       SUM(CASE
               WHEN (CAST(d_date AS date) >= CAST('2001-05-02' AS date)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
               ELSE 0
           END) AS sales_after
FROM catalog_sales
LEFT OUTER JOIN catalog_returns ON (cs_order_number = cr_order_number
                                    AND cs_item_sk = cr_item_sk)
JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
JOIN item ON i_item_sk = cs_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE i_current_price BETWEEN 0.99 AND 1.49
  AND d_date BETWEEN (CAST('2001-05-02' AS date) - INTERVAL '30' DAY) AND (CAST('2001-05-02' AS date) + INTERVAL '30' DAY)
GROUP BY w_state,
         i_item_id
ORDER BY w_state,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN346
-- =================================================================
SELECT DISTINCT(i_product_name)
FROM item
WHERE i_manufact_id BETWEEN 704 AND 744
  AND ((i_category = 'Women'
        AND ((i_color IN ('forest',
                          'lime')
              AND i_units IN ('Pallet',
                              'Pound')
              AND i_size IN ('economy',
                             'small'))
             OR (i_color IN ('navy',
                             'slate')
                 AND i_units IN ('Gross',
                                 'Bunch')
                 AND i_size IN ('extra large',
                                'petite'))))
       OR (i_category = 'Men'
           AND ((i_color IN ('powder',
                             'sky')
                 AND i_units IN ('Dozen',
                                 'Lb')
                 AND i_size IN ('N/A',
                                'large'))
                OR (i_color IN ('maroon',
                                'smoke')
                    AND i_units IN ('Ounce',
                                    'Case')
                    AND i_size IN ('economy',
                                   'small'))))
       OR (i_category = 'Women'
           AND ((i_color IN ('dark',
                             'aquamarine')
                 AND i_units IN ('Ton',
                                 'Tbl')
                 AND i_size IN ('economy',
                                'small'))
                OR (i_color IN ('frosted',
                                'plum')
                    AND i_units IN ('Dram',
                                    'Box')
                    AND i_size IN ('extra large',
                                   'petite'))))
       OR (i_category = 'Men'
           AND ((i_color IN ('papaya',
                             'peach')
                 AND i_units IN ('Bundle',
                                 'Carton')
                 AND i_size IN ('N/A',
                                'large'))
                OR (i_color IN ('firebrick',
                                'sienna')
                    AND i_units IN ('Cup',
                                    'Each')
                    AND i_size IN ('economy',
                                   'small')))))
ORDER BY i_product_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN347
-- =================================================================
SELECT dt.d_year,
       item.i_category_id,
       item.i_category,
       SUM(ss_ext_sales_price)
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manager_id = 1
  AND dt.d_moy = 11
  AND dt.d_year = 1998
GROUP BY dt.d_year,
         item.i_category_id,
         item.i_category
ORDER BY SUM(ss_ext_sales_price) DESC, dt.d_year,
                                       item.i_category_id,
                                       item.i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN348
-- =================================================================
SELECT s_store_name,
       s_store_id,
       SUM(CASE
               WHEN d_day_name = 'Sunday' THEN ss_sales_price
               ELSE NULL
           END) AS sun_sales,
       SUM(CASE
               WHEN d_day_name = 'Monday' THEN ss_sales_price
               ELSE NULL
           END) AS mon_sales,
       SUM(CASE
               WHEN d_day_name = 'Tuesday' THEN ss_sales_price
               ELSE NULL
           END) AS tue_sales,
       SUM(CASE
               WHEN d_day_name = 'Wednesday' THEN ss_sales_price
               ELSE NULL
           END) AS wed_sales,
       SUM(CASE
               WHEN d_day_name = 'Thursday' THEN ss_sales_price
               ELSE NULL
           END) AS thu_sales,
       SUM(CASE
               WHEN d_day_name = 'Friday' THEN ss_sales_price
               ELSE NULL
           END) AS fri_sales,
       SUM(CASE
               WHEN d_day_name = 'Saturday' THEN ss_sales_price
               ELSE NULL
           END) AS sat_sales
FROM date_dim
JOIN store_sales ON d_date_sk = ss_sold_date_sk
JOIN store ON s_store_sk = ss_store_sk
WHERE s_gmt_offset = -5
  AND d_year = 2000
GROUP BY s_store_name,
         s_store_id
ORDER BY s_store_name,
         s_store_id,
         sun_sales,
         mon_sales,
         tue_sales,
         wed_sales,
         thu_sales,
         fri_sales,
         sat_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN349
-- =================================================================
SELECT asceding.rnk,
       i1.i_product_name AS best_performing,
       i2.i_product_name AS worst_performing
FROM
  (SELECT item_sk,
          RANK() OVER (
                       ORDER BY rank_col ASC) AS rnk
   FROM
     (SELECT ss_item_sk AS item_sk,
             AVG(ss_net_profit) AS rank_col
      FROM store_sales
      WHERE ss_store_sk = 4
      GROUP BY ss_item_sk
      HAVING AVG(ss_net_profit) > 0.9 *
        (SELECT AVG(ss_net_profit)
         FROM store_sales
         WHERE ss_store_sk = 4
           AND ss_hdemo_sk IS NOT NULL
         GROUP BY ss_store_sk)) AS V1) AS asceding
JOIN
  (SELECT item_sk,
          RANK() OVER (
                       ORDER BY rank_col DESC) AS rnk
   FROM
     (SELECT ss_item_sk AS item_sk,
             AVG(ss_net_profit) AS rank_col
      FROM store_sales
      WHERE ss_store_sk = 4
      GROUP BY ss_item_sk
      HAVING AVG(ss_net_profit) > 0.9 *
        (SELECT AVG(ss_net_profit)
         FROM store_sales
         WHERE ss_store_sk = 4
           AND ss_hdemo_sk IS NOT NULL
         GROUP BY ss_store_sk)) AS V2) AS descending ON asceding.rnk = descending.rnk
JOIN item i1 ON i1.i_item_sk = asceding.item_sk
JOIN item i2 ON i2.i_item_sk = descending.item_sk
ORDER BY asceding.rnk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN350
-- =================================================================
SELECT ca_zip,
       ca_city,
       SUM(ws_sales_price)
FROM web_sales
JOIN customer ON ws_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN item ON ws_item_sk = i_item_sk
WHERE (SUBSTR(ca_zip, 1, 5) IN ('85669',
                                '86197',
                                '88274',
                                '83405',
                                '86475',
                                '85392',
                                '85460',
                                '80348',
                                '81792')
       OR i_item_sk IN (2,
                        3,
                        5,
                        7,
                        11,
                        13,
                        17,
                        19,
                        23,
                        29))
  AND d_qoy = 1
  AND d_year = 2000
GROUP BY ca_zip,
         ca_city
ORDER BY ca_zip,
         ca_city
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN351
-- =================================================================
SELECT c_last_name,
       c_first_name,
       current_addr.ca_city,
       dn.bought_city,
       dn.ss_ticket_number,
       dn.amt,
       dn.profit
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          ca_city AS bought_city,
          SUM(ss_coupon_amt) AS amt,
          SUM(ss_net_profit) AS profit
   FROM store_sales
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   JOIN store ON store_sales.ss_store_sk = store.s_store_sk
   JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
   JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
   WHERE (household_demographics.hd_dep_count = 8
          OR household_demographics.hd_vehicle_count = 0)
     AND date_dim.d_dow IN (4,
                            0)
     AND date_dim.d_year IN (2000,
                             2001,
                             2002)
     AND store.s_city IN ('Midway',
                          'Fairview')
   GROUP BY ss_ticket_number,
            ss_customer_sk,
            ca_city) dn
JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
JOIN customer_address current_addr ON customer.c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> dn.bought_city
ORDER BY c_last_name,
         c_first_name,
         current_addr.ca_city,
         dn.bought_city,
         dn.ss_ticket_number
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN352
-- =================================================================
WITH v1 AS
  (SELECT i_category,
          i_brand,
          s_store_name,
          s_company_name,
          d_year,
          d_moy,
          SUM(ss_sales_price) AS sum_sales,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_category,
                                                      i_brand,
                                                      s_store_name,
                                                      s_company_name,
                                                      d_year) AS avg_monthly_sales,
                                        RANK() OVER (PARTITION BY i_category,
                                                                  i_brand,
                                                                  s_store_name,
                                                                  s_company_name
                                                     ORDER BY d_year,
                                                              d_moy) AS rn
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE (d_year = 2000
          OR (d_year = 1999
              AND d_moy = 12)
          OR (d_year = 2001
              AND d_moy = 1))
   GROUP BY i_category,
            i_brand,
            s_store_name,
            s_company_name,
            d_year,
            d_moy),
     v2 AS
  (SELECT v1.s_store_name,
          v1.s_company_name,
          v1.d_year,
          v1.avg_monthly_sales,
          v1.sum_sales,
          LAG(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                               v1.i_brand,
                                               v1.s_store_name,
                                               v1.s_company_name
                                  ORDER BY v1.d_year,
                                           v1.d_moy) AS psum,
                                 LEAD(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                                                       v1.i_brand,
                                                                       v1.s_store_name,
                                                                       v1.s_company_name
                                                          ORDER BY v1.d_year,
                                                                   v1.d_moy) AS nsum
   FROM v1)
SELECT *
FROM v2
WHERE d_year = 2000
  AND avg_monthly_sales > 0
  AND CASE
          WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
          ELSE NULL
      END > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         nsum
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN353
-- =================================================================
SELECT SUM(ss_quantity)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2001
  AND ((cd_marital_status = 'S'
        AND cd_education_status = 'Secondary'
        AND ss_sales_price BETWEEN 100.00 AND 150.00)
       OR (cd_marital_status = 'M'
           AND cd_education_status = '2 yr Degree'
           AND ss_sales_price BETWEEN 50.00 AND 100.00)
       OR (cd_marital_status = 'D'
           AND cd_education_status = 'Advanced Degree'
           AND ss_sales_price BETWEEN 150.00 AND 200.00))
  AND ((ca_country = 'United States'
        AND ca_state IN ('ND',
                         'NY',
                         'SD')
        AND ss_net_profit BETWEEN 0 AND 2000)
       OR (ca_country = 'United States'
           AND ca_state IN ('MD',
                            'GA',
                            'KS')
           AND ss_net_profit BETWEEN 150 AND 3000)
       OR (ca_country = 'United States'
           AND ca_state IN ('CO',
                            'MN',
                            'NC')
           AND ss_net_profit BETWEEN 50 AND 25000)) ;

-- =================================================================
-- Query ID: TPCDSN354
-- =================================================================
WITH ranked_web_sales AS
  (SELECT 'web' AS channel,
          ws.ws_item_sk AS item,
          (CAST(SUM(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15, 4))) AS return_ratio,
          (CAST(SUM(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15, 4))) AS currency_ratio,
          RANK() OVER (
                       ORDER BY (CAST(SUM(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15, 4)))) AS return_rank,
                      RANK() OVER (
                                   ORDER BY (CAST(SUM(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15, 4)))) AS currency_rank
   FROM web_sales ws
   LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number
                                      AND ws.ws_item_sk = wr.wr_item_sk), date_dim
   WHERE wr.wr_return_amt > 10000
     AND ws.ws_net_profit > 1
     AND ws.ws_net_paid > 0
     AND ws.ws_quantity > 0
     AND ws_sold_date_sk = d_date_sk
     AND d_year = 1998
     AND d_moy = 11
   GROUP BY ws.ws_item_sk),
     ranked_catalog_sales AS
  (SELECT 'catalog' AS channel,
          cs.cs_item_sk AS item,
          (CAST(SUM(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15, 4))) AS return_ratio,
          (CAST(SUM(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15, 4))) AS currency_ratio,
          RANK() OVER (
                       ORDER BY (CAST(SUM(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15, 4)))) AS return_rank,
                      RANK() OVER (
                                   ORDER BY (CAST(SUM(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15, 4)))) AS currency_rank
   FROM catalog_sales cs
   LEFT OUTER JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number
                                          AND cs.cs_item_sk = cr.cr_item_sk), date_dim
   WHERE cr.cr_return_amount > 10000
     AND cs.cs_net_profit > 1
     AND cs.cs_net_paid > 0
     AND cs.cs_quantity > 0
     AND cs_sold_date_sk = d_date_sk
     AND d_year = 1998
     AND d_moy = 11
   GROUP BY cs.cs_item_sk),
     ranked_store_sales AS
  (SELECT 'store' AS channel,
          sts.ss_item_sk AS item,
          (CAST(SUM(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15, 4))) AS return_ratio,
          (CAST(SUM(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15, 4))) AS currency_ratio,
          RANK() OVER (
                       ORDER BY (CAST(SUM(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15, 4)))) AS return_rank,
                      RANK() OVER (
                                   ORDER BY (CAST(SUM(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15, 4)))) AS currency_rank
   FROM store_sales sts
   LEFT OUTER JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number
                                        AND sts.ss_item_sk = sr.sr_item_sk), date_dim
   WHERE sr.sr_return_amt > 10000
     AND sts.ss_net_profit > 1
     AND sts.ss_net_paid > 0
     AND sts.ss_quantity > 0
     AND ss_sold_date_sk = d_date_sk
     AND d_year = 1998
     AND d_moy = 11
   GROUP BY sts.ss_item_sk)
SELECT channel,
       item,
       return_ratio,
       return_rank,
       currency_rank
FROM
  (SELECT *
   FROM ranked_web_sales
   WHERE return_rank <= 10
     OR currency_rank <= 10
   UNION ALL SELECT *
   FROM ranked_catalog_sales
   WHERE return_rank <= 10
     OR currency_rank <= 10
   UNION ALL SELECT *
   FROM ranked_store_sales
   WHERE return_rank <= 10
     OR currency_rank <= 10 ) AS tmp
ORDER BY 1,
         4,
         5,
         2
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN355
-- =================================================================
SELECT s_store_name,
       s_company_id,
       s_street_number,
       s_street_name,
       s_street_type,
       s_suite_number,
       s_city,
       s_county,
       s_state,
       s_zip,
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS "30 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 30)
                    AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS "31-60 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 60)
                    AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS "61-90 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 90)
                    AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS "91-120 days",
       SUM(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS ">120 days"
FROM store_sales
JOIN store_returns ON ss_ticket_number = sr_ticket_number
AND ss_item_sk = sr_item_sk
AND ss_customer_sk = sr_customer_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
WHERE d2.d_year = 2001
  AND d2.d_moy = 8
GROUP BY s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
ORDER BY s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN356
-- =================================================================
WITH sales_data AS
  (SELECT COALESCE(web.ws_item_sk, store.ss_item_sk) AS item_sk,
          COALESCE(web.d_date, store.d_date) AS d_date,
          SUM(web.ws_sales_price) OVER (PARTITION BY web.ws_item_sk
                                        ORDER BY web.d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS web_cume_sales,
                                       SUM(store.ss_sales_price) OVER (PARTITION BY store.ss_item_sk
                                                                       ORDER BY store.d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS store_cume_sales
   FROM
     (SELECT ws_item_sk,
             d_date,
             ws_sales_price
      FROM web_sales
      JOIN date_dim ON ws_sold_date_sk = d_date_sk
      WHERE d_month_seq BETWEEN 1212 AND 1212 + 11
        AND ws_item_sk IS NOT NULL) web
   FULL OUTER JOIN
     (SELECT ss_item_sk,
             d_date,
             ss_sales_price
      FROM store_sales
      JOIN date_dim ON ss_sold_date_sk = d_date_sk
      WHERE d_month_seq BETWEEN 1212 AND 1212 + 11
        AND ss_item_sk IS NOT NULL) store ON web.ws_item_sk = store.ss_item_sk
   AND web.d_date = store.d_date)
SELECT item_sk,
       d_date,
       web_cume_sales AS web_sales,
       store_cume_sales AS store_sales,
       MAX(web_cume_sales) OVER (PARTITION BY item_sk
                                 ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS web_cumulative,
                                MAX(store_cume_sales) OVER (PARTITION BY item_sk
                                                            ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS store_cumulative
FROM sales_data
WHERE web_cume_sales > store_cume_sales
ORDER BY item_sk,
         d_date
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN357
-- =================================================================
SELECT dt.d_year,
       item.i_brand_id AS brand_id,
       item.i_brand AS brand,
       SUM(ss_ext_sales_price) AS ext_price
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manager_id = 1
  AND dt.d_moy = 12
  AND dt.d_year = 2000
GROUP BY dt.d_year,
         item.i_brand,
         item.i_brand_id
ORDER BY dt.d_year,
         ext_price DESC,
         brand_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN358
-- =================================================================
WITH sales_data AS
  (SELECT i_manufact_id,
          d_qoy,
          SUM(ss_sales_price) AS sum_sales,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) AS avg_quarterly_sales
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_month_seq IN (1186,
                         1186 + 1,
                         1186 + 2,
                         1186 + 3,
                         1186 + 4,
                         1186 + 5,
                         1186 + 6,
                         1186 + 7,
                         1186 + 8,
                         1186 + 9,
                         1186 + 10,
                         1186 + 11)
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('personal',
                           'portable',
                           'reference',
                           'self-help')
           AND i_brand IN ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9'))
          OR (i_category IN ('Women',
                             'Music',
                             'Men')
              AND i_class IN ('accessories',
                              'classical',
                              'fragrances',
                              'pants')
              AND i_brand IN ('amalgimporto #1',
                              'edu packscholar #1',
                              'exportiimporto #1',
                              'importoamalg #1')))
   GROUP BY i_manufact_id,
            d_qoy)
SELECT i_manufact_id,
       sum_sales,
       avg_quarterly_sales
FROM sales_data
WHERE CASE
          WHEN avg_quarterly_sales > 0 THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
          ELSE NULL
      END > 0.1
ORDER BY avg_quarterly_sales,
         sum_sales,
         i_manufact_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN359
-- =================================================================
WITH cs_or_ws_sales AS
  (SELECT cs_sold_date_sk AS sold_date_sk,
          cs_bill_customer_sk AS customer_sk,
          cs_item_sk AS item_sk
   FROM catalog_sales
   UNION ALL SELECT ws_sold_date_sk AS sold_date_sk,
                    ws_bill_customer_sk AS customer_sk,
                    ws_item_sk AS item_sk
   FROM web_sales),
     my_customers AS
  (SELECT DISTINCT c_customer_sk,
                   c_current_addr_sk
   FROM cs_or_ws_sales
   JOIN item ON item_sk = i_item_sk
   JOIN date_dim ON sold_date_sk = d_date_sk
   JOIN customer ON c_customer_sk = cs_or_ws_sales.customer_sk
   WHERE i_category = 'Music'
     AND i_class = 'country'
     AND d_moy = 1
     AND d_year = 1999 ),
     my_revenue AS
  (SELECT c_customer_sk,
          SUM(ss_ext_sales_price) AS revenue
   FROM my_customers
   JOIN store_sales ON c_customer_sk = ss_customer_sk
   JOIN customer_address ON c_current_addr_sk = ca_address_sk
   JOIN store ON ca_county = s_county
   AND ca_state = s_state
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN
       (SELECT DISTINCT d_month_seq + 1
        FROM date_dim
        WHERE d_year = 1999
          AND d_moy = 1 ) AND
       (SELECT DISTINCT d_month_seq + 3
        FROM date_dim
        WHERE d_year = 1999
          AND d_moy = 1 )
   GROUP BY c_customer_sk),
     segments AS
  (SELECT CAST((revenue / 50) AS INT) AS segment
   FROM my_revenue)
SELECT segment,
       COUNT(*) AS num_customers,
       segment * 50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment,
         num_customers
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN360
-- =================================================================
SELECT i_brand_id AS brand_id,
       i_brand,
       SUM(ss_ext_sales_price) AS ext_price
FROM date_dim
JOIN store_sales ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE i_manager_id = 52
  AND d_moy = 11
  AND d_year = 2000
GROUP BY i_brand,
         i_brand_id
ORDER BY ext_price DESC,
         i_brand_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN361
-- =================================================================
SELECT i_item_id,
       SUM(total_sales) total_sales
FROM
  (SELECT i_item_id,
          SUM(ss_ext_sales_price) total_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer_address ON ss_addr_sk = ca_address_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE i_item_id IN
       (SELECT i_item_id
        FROM item
        WHERE i_color IN ('powder',
                          'orchid',
                          'pink'))
     AND d_year = 2000
     AND d_moy = 3
     AND ca_gmt_offset = -6
   GROUP BY i_item_id
   UNION ALL SELECT i_item_id,
                    SUM(cs_ext_sales_price) total_sales
   FROM catalog_sales
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN customer_address ON cs_bill_addr_sk = ca_address_sk
   JOIN item ON cs_item_sk = i_item_sk
   WHERE i_item_id IN
       (SELECT i_item_id
        FROM item
        WHERE i_color IN ('powder',
                          'orchid',
                          'pink'))
     AND d_year = 2000
     AND d_moy = 3
     AND ca_gmt_offset = -6
   GROUP BY i_item_id
   UNION ALL SELECT i_item_id,
                    SUM(ws_ext_sales_price) total_sales
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
   JOIN item ON ws_item_sk = i_item_sk
   WHERE i_item_id IN
       (SELECT i_item_id
        FROM item
        WHERE i_color IN ('powder',
                          'orchid',
                          'pink'))
     AND d_year = 2000
     AND d_moy = 3
     AND ca_gmt_offset = -6
   GROUP BY i_item_id) tmp1
GROUP BY i_item_id
ORDER BY total_sales,
         i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN362
-- =================================================================
WITH v1 AS
  (SELECT i_category,
          i_brand,
          cc_name,
          d_year,
          d_moy,
          SUM(cs_sales_price) AS sum_sales,
          AVG(SUM(cs_sales_price)) OVER (PARTITION BY i_category,
                                                      i_brand,
                                                      cc_name,
                                                      d_year) AS avg_monthly_sales,
                                        RANK() OVER (PARTITION BY i_category,
                                                                  i_brand,
                                                                  cc_name
                                                     ORDER BY d_year,
                                                              d_moy) AS rn
   FROM item
   JOIN catalog_sales ON cs_item_sk = i_item_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN call_center ON cc_call_center_sk = cs_call_center_sk
   WHERE (d_year = 2001
          OR (d_year = 2000
              AND d_moy = 12)
          OR (d_year = 2002
              AND d_moy = 1))
   GROUP BY i_category,
            i_brand,
            cc_name,
            d_year,
            d_moy),
     v2 AS
  (SELECT v1.i_category,
          v1.i_brand,
          v1.cc_name,
          v1.d_year,
          v1.avg_monthly_sales,
          v1.sum_sales,
          LAG(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                               v1.i_brand,
                                               v1.cc_name
                                  ORDER BY v1.rn) AS psum,
                                 LEAD(v1.sum_sales) OVER (PARTITION BY v1.i_category,
                                                                       v1.i_brand,
                                                                       v1.cc_name
                                                          ORDER BY v1.rn) AS nsum
   FROM v1)
SELECT *
FROM v2
WHERE d_year = 2001
  AND avg_monthly_sales > 0
  AND CASE
          WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
          ELSE NULL
      END > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         avg_monthly_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN363
-- =================================================================
WITH all_sales AS
  (SELECT i_item_id AS item_id,
          SUM(CASE
                  WHEN ss_sold_date_sk IS NOT NULL THEN ss_ext_sales_price
                  ELSE 0
              END) AS ss_item_rev,
          SUM(CASE
                  WHEN cs_sold_date_sk IS NOT NULL THEN cs_ext_sales_price
                  ELSE 0
              END) AS cs_item_rev,
          SUM(CASE
                  WHEN ws_sold_date_sk IS NOT NULL THEN ws_ext_sales_price
                  ELSE 0
              END) AS ws_item_rev
   FROM item
   LEFT JOIN store_sales ON ss_item_sk = i_item_sk
   LEFT JOIN catalog_sales ON cs_item_sk = i_item_sk
   LEFT JOIN web_sales ON ws_item_sk = i_item_sk
   JOIN date_dim ON ((ss_sold_date_sk = d_date_sk
                      OR cs_sold_date_sk = d_date_sk
                      OR ws_sold_date_sk = d_date_sk)
                     AND d_date IN
                       (SELECT d_date
                        FROM date_dim
                        WHERE d_week_seq =
                            (SELECT d_week_seq
                             FROM date_dim
                             WHERE d_date = '1998-11-19' ) ))
   GROUP BY i_item_id)
SELECT item_id,
       ss_item_rev,
       ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev,
       cs_item_rev,
       cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev,
       ws_item_rev,
       ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev,
       (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM all_sales
WHERE ss_item_rev BETWEEN 0.1 * cs_item_rev AND 2 * cs_item_rev
  AND ss_item_rev BETWEEN 0.1 * ws_item_rev AND 2 * ws_item_rev
  AND cs_item_rev BETWEEN 0.1 * ss_item_rev AND 2 * ss_item_rev
  AND cs_item_rev BETWEEN 0.1 * ws_item_rev AND 2 * ws_item_rev
  AND ws_item_rev BETWEEN 0.1 * ss_item_rev AND 2 * ss_item_rev
  AND ws_item_rev BETWEEN 0.1 * cs_item_rev AND 2 * cs_item_rev
ORDER BY item_id,
         ss_item_rev
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN364
-- =================================================================
WITH wss AS
  (SELECT d_week_seq,
          ss_store_sk,
          SUM(CASE
                  WHEN d_day_name = 'Sunday' THEN ss_sales_price
                  ELSE NULL
              END) AS sun_sales,
          SUM(CASE
                  WHEN d_day_name = 'Monday' THEN ss_sales_price
                  ELSE NULL
              END) AS mon_sales,
          SUM(CASE
                  WHEN d_day_name = 'Tuesday' THEN ss_sales_price
                  ELSE NULL
              END) AS tue_sales,
          SUM(CASE
                  WHEN d_day_name = 'Wednesday' THEN ss_sales_price
                  ELSE NULL
              END) AS wed_sales,
          SUM(CASE
                  WHEN d_day_name = 'Thursday' THEN ss_sales_price
                  ELSE NULL
              END) AS thu_sales,
          SUM(CASE
                  WHEN d_day_name = 'Friday' THEN ss_sales_price
                  ELSE NULL
              END) AS fri_sales,
          SUM(CASE
                  WHEN d_day_name = 'Saturday' THEN ss_sales_price
                  ELSE NULL
              END) AS sat_sales
   FROM store_sales
   JOIN date_dim ON d_date_sk = ss_sold_date_sk
   GROUP BY d_week_seq,
            ss_store_sk)
SELECT y.s_store_name1,
       y.s_store_id1,
       y.d_week_seq1,
       y.sun_sales1 / x.sun_sales2 AS sun_sales_ratio,
       y.mon_sales1 / x.mon_sales2 AS mon_sales_ratio,
       y.tue_sales1 / x.tue_sales2 AS tue_sales_ratio,
       y.wed_sales1 / x.wed_sales2 AS wed_sales_ratio,
       y.thu_sales1 / x.thu_sales2 AS thu_sales_ratio,
       y.fri_sales1 / x.fri_sales2 AS fri_sales_ratio,
       y.sat_sales1 / x.sat_sales2 AS sat_sales_ratio
FROM
  (SELECT s_store_name AS s_store_name1,
          wss.d_week_seq AS d_week_seq1,
          s_store_id AS s_store_id1,
          sun_sales AS sun_sales1,
          mon_sales AS mon_sales1,
          tue_sales AS tue_sales1,
          wed_sales AS wed_sales1,
          thu_sales AS thu_sales1,
          fri_sales AS fri_sales1,
          sat_sales AS sat_sales1
   FROM wss
   JOIN store ON ss_store_sk = s_store_sk
   JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
   WHERE d_month_seq BETWEEN 1195 AND 1195 + 11) y
JOIN
  (SELECT s_store_name AS s_store_name2,
          wss.d_week_seq AS d_week_seq2,
          s_store_id AS s_store_id2,
          sun_sales AS sun_sales2,
          mon_sales AS mon_sales2,
          tue_sales AS tue_sales2,
          wed_sales AS wed_sales2,
          thu_sales AS thu_sales2,
          fri_sales AS fri_sales2,
          sat_sales AS sat_sales2
   FROM wss
   JOIN store ON ss_store_sk = s_store_sk
   JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
   WHERE d_month_seq BETWEEN 1195 + 12 AND 1195 + 23) x ON y.s_store_id1 = x.s_store_id2
AND y.d_week_seq1 = x.d_week_seq2 - 52
ORDER BY y.s_store_name1,
         y.s_store_id1,
         y.d_week_seq1
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN365
-- =================================================================
SELECT i_item_id,
       SUM(total_sales) total_sales
FROM
  (SELECT i_item_id,
          SUM(ss_ext_sales_price) total_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN customer_address ON ss_addr_sk = ca_address_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE i_item_id IN
       (SELECT i_item_id
        FROM item
        WHERE i_category IN ('Jewelry'))
     AND d_year = 2000
     AND d_moy = 10
     AND ca_gmt_offset = -5
   GROUP BY i_item_id
   UNION ALL SELECT i_item_id,
                    SUM(cs_ext_sales_price) total_sales
   FROM catalog_sales
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN customer_address ON cs_bill_addr_sk = ca_address_sk
   JOIN item ON cs_item_sk = i_item_sk
   WHERE i_item_id IN
       (SELECT i_item_id
        FROM item
        WHERE i_category IN ('Jewelry'))
     AND d_year = 2000
     AND d_moy = 10
     AND ca_gmt_offset = -5
   GROUP BY i_item_id
   UNION ALL SELECT i_item_id,
                    SUM(ws_ext_sales_price) total_sales
   FROM web_sales
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
   JOIN item ON ws_item_sk = i_item_sk
   WHERE i_item_id IN
       (SELECT i_item_id
        FROM item
        WHERE i_category IN ('Jewelry'))
     AND d_year = 2000
     AND d_moy = 10
     AND ca_gmt_offset = -5
   GROUP BY i_item_id) tmp1
GROUP BY i_item_id
ORDER BY i_item_id,
         total_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN366
-- =================================================================
SELECT SUM(CASE
               WHEN (p_channel_dmail = 'Y'
                     OR p_channel_email = 'Y'
                     OR p_channel_tv = 'Y') THEN ss_ext_sales_price
               ELSE 0
           END) AS promotions,
       SUM(ss_ext_sales_price) AS total,
       CAST(SUM(CASE
                    WHEN (p_channel_dmail = 'Y'
                          OR p_channel_email = 'Y'
                          OR p_channel_tv = 'Y') THEN ss_ext_sales_price
                    ELSE 0
                END) AS DECIMAL(15, 4)) / CAST(SUM(ss_ext_sales_price) AS DECIMAL(15, 4)) * 100 AS percentage
FROM store_sales
JOIN store ON ss_store_sk = s_store_sk
JOIN promotion ON ss_promo_sk = p_promo_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE ca_gmt_offset = -5
  AND i_category = 'Home'
  AND s_gmt_offset = -5
  AND d_year = 2000
  AND d_moy = 12
ORDER BY promotions,
         total
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN367
-- =================================================================
SELECT substr(w_warehouse_name, 1, 20),
       sm_type,
       web_name,
       SUM(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS "30 days",
       SUM(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk > 30)
                    AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS "31-60 days",
       SUM(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk > 60)
                    AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS "61-90 days",
       SUM(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk > 90)
                    AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS "91-120 days",
       SUM(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS ">120 days"
FROM web_sales
JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk
JOIN ship_mode ON ws_ship_mode_sk = sm_ship_mode_sk
JOIN web_site ON ws_web_site_sk = web_site_sk
JOIN date_dim ON ws_ship_date_sk = d_date_sk
WHERE d_month_seq BETWEEN 1223 AND 1223 + 11
GROUP BY substr(w_warehouse_name, 1, 20),
         sm_type,
         web_name
ORDER BY substr(w_warehouse_name, 1, 20),
         sm_type,
         web_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN368
-- =================================================================
WITH sales_data AS
  (SELECT i_manager_id,
          d_moy,
          SUM(ss_sales_price) AS sum_sales,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manager_id) AS avg_monthly_sales
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_month_seq IN (1222,
                         1223,
                         1224,
                         1225,
                         1226,
                         1227,
                         1228,
                         1229,
                         1230,
                         1231,
                         1232,
                         1233)
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('personal',
                           'portable',
                           'reference',
                           'self-help')
           AND i_brand IN ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9'))
          OR (i_category IN ('Women',
                             'Music',
                             'Men')
              AND i_class IN ('accessories',
                              'classical',
                              'fragrances',
                              'pants')
              AND i_brand IN ('amalgimporto #1',
                              'edu packscholar #1',
                              'exportiimporto #1',
                              'importoamalg #1')))
   GROUP BY i_manager_id,
            d_moy)
SELECT i_manager_id,
       sum_sales,
       avg_monthly_sales
FROM sales_data
WHERE CASE
          WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
          ELSE NULL
      END > 0.1
ORDER BY i_manager_id,
         avg_monthly_sales,
         sum_sales
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN369
-- =================================================================
WITH cs_ui AS
  (SELECT cs_item_sk,
          SUM(cs_ext_list_price) AS sale,
          SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
   FROM catalog_sales
   JOIN catalog_returns ON cs_item_sk = cr_item_sk
   AND cs_order_number = cr_order_number
   GROUP BY cs_item_sk
   HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)),
     cross_sales AS
  (SELECT i_product_name AS product_name,
          i_item_sk AS item_sk,
          s_store_name AS store_name,
          s_zip AS store_zip,
          ad1.ca_street_number AS b_street_number,
          ad1.ca_street_name AS b_street_name,
          ad1.ca_city AS b_city,
          ad1.ca_zip AS b_zip,
          ad2.ca_street_number AS c_street_number,
          ad2.ca_street_name AS c_street_name,
          ad2.ca_city AS c_city,
          ad2.ca_zip AS c_zip,
          d1.d_year AS syear,
          COUNT(*) AS cnt,
          SUM(ss_wholesale_cost) AS s1,
          SUM(ss_list_price) AS s2,
          SUM(ss_coupon_amt) AS s3
   FROM store_sales
   JOIN store_returns ON ss_item_sk = sr_item_sk
   AND ss_ticket_number = sr_ticket_number
   JOIN cs_ui ON ss_item_sk = cs_ui.cs_item_sk
   JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   JOIN customer ON ss_customer_sk = c_customer_sk
   JOIN customer_demographics cd1 ON ss_cdemo_sk = cd1.cd_demo_sk
   JOIN customer_demographics cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk
   JOIN promotion ON ss_promo_sk = p_promo_sk
   JOIN household_demographics hd1 ON ss_hdemo_sk = hd1.hd_demo_sk
   JOIN household_demographics hd2 ON c_current_hdemo_sk = hd2.hd_demo_sk
   JOIN customer_address ad1 ON ss_addr_sk = ad1.ca_address_sk
   JOIN customer_address ad2 ON c_current_addr_sk = ad2.ca_address_sk
   JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
   JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE cd1.cd_marital_status <> cd2.cd_marital_status
     AND i_color IN ('orange',
                     'lace',
                     'lawn',
                     'misty',
                     'blush',
                     'pink')
     AND i_current_price BETWEEN 48 AND 58
     AND i_current_price BETWEEN 49 AND 63
   GROUP BY i_product_name,
            i_item_sk,
            s_store_name,
            s_zip,
            ad1.ca_street_number,
            ad1.ca_street_name,
            ad1.ca_city,
            ad1.ca_zip,
            ad2.ca_street_number,
            ad2.ca_street_name,
            ad2.ca_city,
            ad2.ca_zip,
            d1.d_year)
SELECT cs1.product_name,
       cs1.store_name,
       cs1.store_zip,
       cs1.b_street_number,
       cs1.b_street_name,
       cs1.b_city,
       cs1.b_zip,
       cs1.c_street_number,
       cs1.c_street_name,
       cs1.c_city,
       cs1.c_zip,
       cs1.syear,
       cs1.cnt,
       cs1.s1 AS s11,
       cs1.s2 AS s21,
       cs1.s3 AS s31,
       cs2.s1 AS s12,
       cs2.s2 AS s22,
       cs2.s3 AS s32,
       cs2.syear,
       cs2.cnt
FROM cross_sales cs1
JOIN cross_sales cs2 ON cs1.item_sk = cs2.item_sk
AND cs1.syear = 1999
AND cs2.syear = 2000
AND cs2.cnt <= cs1.cnt
AND cs1.store_name = cs2.store_name
AND cs1.store_zip = cs2.store_zip
ORDER BY cs1.product_name,
         cs1.store_name,
         cs2.cnt,
         cs1.s1,
         cs2.s1 ;

-- =================================================================
-- Query ID: TPCDSN370
-- =================================================================
SELECT s_store_name,
       i_item_desc,
       sc.revenue,
       i_current_price,
       i_wholesale_cost,
       i_brand
FROM store,
     item,

  (SELECT ss_store_sk,
          ss_item_sk,
          SUM(ss_sales_price) AS revenue,
          AVG(SUM(ss_sales_price)) OVER (PARTITION BY ss_store_sk) AS ave
   FROM store_sales,
        date_dim
   WHERE ss_sold_date_sk = d_date_sk
     AND d_month_seq BETWEEN 1176 AND 1176 + 11
   GROUP BY ss_store_sk,
            ss_item_sk) sc
WHERE sc.revenue <= 0.1 * sc.ave
  AND s_store_sk = sc.ss_store_sk
  AND i_item_sk = sc.ss_item_sk
ORDER BY s_store_name,
         i_item_desc
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN371
-- =================================================================
SELECT w_warehouse_name,
       w_warehouse_sq_ft,
       w_city,
       w_county,
       w_state,
       w_country,
       'ORIENTAL,BOXBUNDLES' AS ship_carriers,
       d_year AS year,
       SUM(CASE
               WHEN d_moy = 1 THEN sales_price * quantity
               ELSE 0
           END) AS jan_sales,
       SUM(CASE
               WHEN d_moy = 2 THEN sales_price * quantity
               ELSE 0
           END) AS feb_sales,
       SUM(CASE
               WHEN d_moy = 3 THEN sales_price * quantity
               ELSE 0
           END) AS mar_sales,
       SUM(CASE
               WHEN d_moy = 4 THEN sales_price * quantity
               ELSE 0
           END) AS apr_sales,
       SUM(CASE
               WHEN d_moy = 5 THEN sales_price * quantity
               ELSE 0
           END) AS may_sales,
       SUM(CASE
               WHEN d_moy = 6 THEN sales_price * quantity
               ELSE 0
           END) AS jun_sales,
       SUM(CASE
               WHEN d_moy = 7 THEN sales_price * quantity
               ELSE 0
           END) AS jul_sales,
       SUM(CASE
               WHEN d_moy = 8 THEN sales_price * quantity
               ELSE 0
           END) AS aug_sales,
       SUM(CASE
               WHEN d_moy = 9 THEN sales_price * quantity
               ELSE 0
           END) AS sep_sales,
       SUM(CASE
               WHEN d_moy = 10 THEN sales_price * quantity
               ELSE 0
           END) AS oct_sales,
       SUM(CASE
               WHEN d_moy = 11 THEN sales_price * quantity
               ELSE 0
           END) AS nov_sales,
       SUM(CASE
               WHEN d_moy = 12 THEN sales_price * quantity
               ELSE 0
           END) AS dec_sales,
       SUM(CASE
               WHEN d_moy = 1 THEN net_paid * quantity
               ELSE 0
           END) AS jan_net,
       SUM(CASE
               WHEN d_moy = 2 THEN net_paid * quantity
               ELSE 0
           END) AS feb_net,
       SUM(CASE
               WHEN d_moy = 3 THEN net_paid * quantity
               ELSE 0
           END) AS mar_net,
       SUM(CASE
               WHEN d_moy = 4 THEN net_paid * quantity
               ELSE 0
           END) AS apr_net,
       SUM(CASE
               WHEN d_moy = 5 THEN net_paid * quantity
               ELSE 0
           END) AS may_net,
       SUM(CASE
               WHEN d_moy = 6 THEN net_paid * quantity
               ELSE 0
           END) AS jun_net,
       SUM(CASE
               WHEN d_moy = 7 THEN net_paid * quantity
               ELSE 0
           END) AS jul_net,
       SUM(CASE
               WHEN d_moy = 8 THEN net_paid * quantity
               ELSE 0
           END) AS aug_net,
       SUM(CASE
               WHEN d_moy = 9 THEN net_paid * quantity
               ELSE 0
           END) AS sep_net,
       SUM(CASE
               WHEN d_moy = 10 THEN net_paid * quantity
               ELSE 0
           END) AS oct_net,
       SUM(CASE
               WHEN d_moy = 11 THEN net_paid * quantity
               ELSE 0
           END) AS nov_net,
       SUM(CASE
               WHEN d_moy = 12 THEN net_paid * quantity
               ELSE 0
           END) AS dec_net
FROM
  (SELECT w_warehouse_name,
          w_warehouse_sq_ft,
          w_city,
          w_county,
          w_state,
          w_country,
          d_year,
          d_moy,
          ws_ext_sales_price AS sales_price,
          ws_quantity AS quantity,
          ws_net_paid_inc_ship AS net_paid
   FROM web_sales
   JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN time_dim ON ws_sold_time_sk = t_time_sk
   JOIN ship_mode ON ws_ship_mode_sk = sm_ship_mode_sk
   WHERE d_year = 2001
     AND t_time BETWEEN 42970 AND 42970 + 28800
     AND sm_carrier IN ('ORIENTAL',
                        'BOXBUNDLES')
   UNION ALL SELECT w_warehouse_name,
                    w_warehouse_sq_ft,
                    w_city,
                    w_county,
                    w_state,
                    w_country,
                    d_year,
                    d_moy,
                    cs_ext_list_price AS sales_price,
                    cs_quantity AS quantity,
                    cs_net_paid AS net_paid
   FROM catalog_sales
   JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN time_dim ON cs_sold_time_sk = t_time_sk
   JOIN ship_mode ON cs_ship_mode_sk = sm_ship_mode_sk
   WHERE d_year = 2001
     AND t_time BETWEEN 42970 AND 42970 + 28800
     AND sm_carrier IN ('ORIENTAL',
                        'BOXBUNDLES') ) AS sales_data
GROUP BY w_warehouse_name,
         w_warehouse_sq_ft,
         w_city,
         w_county,
         w_state,
         w_country,
         d_year
ORDER BY w_warehouse_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN372
-- =================================================================
SELECT *
FROM
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sumsales,
          RANK() OVER (PARTITION BY i_category
                       ORDER BY SUM(COALESCE(ss_sales_price * ss_quantity, 0)) DESC) AS rk
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   JOIN item ON ss_item_sk = i_item_sk
   WHERE d_month_seq BETWEEN 1217 AND 1217 + 11
   GROUP BY ROLLUP(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)) dw2
WHERE rk <= 100
ORDER BY i_category,
         i_class,
         i_brand,
         i_product_name,
         d_year,
         d_qoy,
         d_moy,
         s_store_id,
         sumsales,
         rk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN373
-- =================================================================
SELECT c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       extended_price,
       extended_tax,
       list_price
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          ca_city AS bought_city,
          SUM(ss_ext_sales_price) AS extended_price,
          SUM(ss_ext_list_price) AS list_price,
          SUM(ss_ext_tax) AS extended_tax
   FROM store_sales
   JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
   JOIN store ON store_sales.ss_store_sk = store.s_store_sk
   JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
   JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
   WHERE date_dim.d_dom BETWEEN 1 AND 2
     AND (household_demographics.hd_dep_count = 3
          OR household_demographics.hd_vehicle_count = 4)
     AND date_dim.d_year IN (1998,
                             1999,
                             2000)
     AND store.s_city IN ('Fairview',
                          'Midway')
   GROUP BY ss_ticket_number,
            ss_customer_sk,
            ca_city) dn
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN customer_address current_addr ON customer.c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> bought_city
ORDER BY c_last_name,
         ss_ticket_number
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN374
-- =================================================================
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       COUNT(*) AS cnt1,
       cd_purchase_estimate,
       COUNT(*) AS cnt2,
       cd_credit_rating,
       COUNT(*) AS cnt3
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
WHERE ca_state IN ('IL',
                   'TX',
                   'ME')
  AND EXISTS
    (SELECT 1
     FROM store_sales ss
     JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
     WHERE c.c_customer_sk = ss.ss_customer_sk
       AND d.d_year = 2002
       AND d.d_moy BETWEEN 1 AND 3 )
  AND NOT EXISTS
    (SELECT 1
     FROM web_sales ws
     JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
     WHERE c.c_customer_sk = ws.ws_bill_customer_sk
       AND d.d_year = 2002
       AND d.d_moy BETWEEN 1 AND 3 )
  AND NOT EXISTS
    (SELECT 1
     FROM catalog_sales cs
     JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
     WHERE c.c_customer_sk = cs.cs_ship_customer_sk
       AND d.d_year = 2002
       AND d.d_moy BETWEEN 1 AND 3 )
GROUP BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
ORDER BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN375
-- =================================================================
select *
from customer ;

-- =================================================================
-- Query ID: TPCDSN376
-- =================================================================
SELECT i_brand_id AS brand_id,
       i_brand AS brand,
       t_hour,
       t_minute,
       SUM(ext_price) AS ext_price
FROM item
JOIN
  (SELECT ws_item_sk AS sold_item_sk,
          ws_sold_time_sk AS time_sk,
          ws_ext_sales_price AS ext_price
   FROM web_sales
   JOIN date_dim ON d_date_sk = ws_sold_date_sk
   WHERE d_moy = 12
     AND d_year = 2002
   UNION ALL SELECT cs_item_sk AS sold_item_sk,
                    cs_sold_time_sk AS time_sk,
                    cs_ext_sales_price AS ext_price
   FROM catalog_sales
   JOIN date_dim ON d_date_sk = cs_sold_date_sk
   WHERE d_moy = 12
     AND d_year = 2002
   UNION ALL SELECT ss_item_sk AS sold_item_sk,
                    ss_sold_time_sk AS time_sk,
                    ss_ext_sales_price AS ext_price
   FROM store_sales
   JOIN date_dim ON d_date_sk = ss_sold_date_sk
   WHERE d_moy = 12
     AND d_year = 2002 ) AS tmp ON sold_item_sk = i_item_sk
JOIN time_dim ON time_sk = t_time_sk
WHERE i_manager_id = 1
  AND (t_meal_time = 'breakfast'
       OR t_meal_time = 'dinner')
GROUP BY i_brand,
         i_brand_id,
         t_hour,
         t_minute
ORDER BY ext_price DESC,
         i_brand_id ;

-- =================================================================
-- Query ID: TPCDSN377
-- =================================================================
SELECT i_item_desc,
       w_warehouse_name,
       d1.d_week_seq,
       SUM(CASE
               WHEN p_promo_sk IS NULL THEN 1
               ELSE 0
           END) AS no_promo,
       SUM(CASE
               WHEN p_promo_sk IS NOT NULL THEN 1
               ELSE 0
           END) AS promo,
       COUNT(*) AS total_cnt
FROM catalog_sales
JOIN inventory ON (cs_item_sk = inv_item_sk)
JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk)
JOIN item ON (i_item_sk = cs_item_sk)
JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
LEFT OUTER JOIN promotion ON (cs_promo_sk = p_promo_sk)
LEFT OUTER JOIN catalog_returns ON (cr_item_sk = cs_item_sk
                                    AND cr_order_number = cs_order_number)
WHERE d1.d_week_seq = d2.d_week_seq
  AND inv_quantity_on_hand < cs_quantity
  AND d3.d_date > d1.d_date
  AND hd_buy_potential = '1001-5000'
  AND d1.d_year = 2002
  AND cd_marital_status = 'W'
GROUP BY i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
ORDER BY total_cnt DESC,
         i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN378
-- =================================================================
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       COUNT(*) AS cnt
FROM store_sales
JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
JOIN store ON store_sales.ss_store_sk = store.s_store_sk
JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
WHERE date_dim.d_dom BETWEEN 1 AND 2
  AND (household_demographics.hd_buy_potential = '1001-5000'
       OR household_demographics.hd_buy_potential = '5001-10000')
  AND household_demographics.hd_vehicle_count > 0
  AND CASE
          WHEN household_demographics.hd_vehicle_count > 0 THEN household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
          ELSE NULL
      END > 1
  AND date_dim.d_year IN (2000,
                          2001,
                          2002)
  AND store.s_county IN ('Williamson County')
GROUP BY ss_ticket_number,
         ss_customer_sk,
         c_last_name,
         c_first_name,
         c_salutation,
         c_preferred_cust_flag
HAVING COUNT(*) BETWEEN 1 AND 5
ORDER BY cnt DESC,
         c_last_name ASC ;

-- =================================================================
-- Query ID: TPCDSN379
-- =================================================================
WITH year_total AS
  (SELECT c_customer_id AS customer_id,
          c_first_name AS customer_first_name,
          c_last_name AS customer_last_name,
          d_year AS year,
          MAX(CASE
                  WHEN ss_customer_sk IS NOT NULL THEN ss_net_paid
                  ELSE ws_net_paid
              END) AS year_total,
          CASE
              WHEN ss_customer_sk IS NOT NULL THEN 's'
              ELSE 'w'
          END AS sale_type
   FROM customer
   LEFT JOIN store_sales ON c_customer_sk = ss_customer_sk
   LEFT JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
   JOIN date_dim ON (ss_sold_date_sk = d_date_sk
                     OR ws_sold_date_sk = d_date_sk)
   WHERE d_year IN (1999,
                    2000)
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            d_year,
            sale_type)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.year = 1999
  AND t_s_secyear.year = 2000
  AND t_w_firstyear.year = 1999
  AND t_w_secyear.year = 2000
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE
          WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
          ELSE NULL
      END >= CASE
                 WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                 ELSE NULL
             END
ORDER BY 1,
         3,
         2
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN380
-- =================================================================
WITH sales_detail AS
  (SELECT d_year,
          i_brand_id,
          i_class_id,
          i_category_id,
          i_manufact_id,
          SUM(sales_cnt) AS sales_cnt,
          SUM(sales_amt) AS sales_amt
   FROM
     (SELECT 'catalog' AS sales_type,
             d_year,
             i_brand_id,
             i_class_id,
             i_category_id,
             i_manufact_id,
             cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
             cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt
      FROM catalog_sales
      JOIN item ON i_item_sk = cs_item_sk
      JOIN date_dim ON d_date_sk = cs_sold_date_sk
      LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number
                                    AND cs_item_sk = cr_item_sk)
      WHERE i_category = 'Sports'
      UNION ALL SELECT 'store' AS sales_type,
                       d_year,
                       i_brand_id,
                       i_class_id,
                       i_category_id,
                       i_manufact_id,
                       ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,
                       ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt
      FROM store_sales
      JOIN item ON i_item_sk = ss_item_sk
      JOIN date_dim ON d_date_sk = ss_sold_date_sk
      LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number
                                  AND ss_item_sk = sr_item_sk)
      WHERE i_category = 'Sports'
      UNION ALL SELECT 'web' AS sales_type,
                       d_year,
                       i_brand_id,
                       i_class_id,
                       i_category_id,
                       i_manufact_id,
                       ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,
                       ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt
      FROM web_sales
      JOIN item ON i_item_sk = ws_item_sk
      JOIN date_dim ON d_date_sk = ws_sold_date_sk
      LEFT JOIN web_returns ON (ws_order_number = wr_order_number
                                AND ws_item_sk = wr_item_sk)
      WHERE i_category = 'Sports' ) AS sales
   GROUP BY d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id)
SELECT prev_yr.d_year AS prev_year,
       curr_yr.d_year AS year,
       curr_yr.i_brand_id,
       curr_yr.i_class_id,
       curr_yr.i_category_id,
       curr_yr.i_manufact_id,
       prev_yr.sales_cnt AS prev_yr_cnt,
       curr_yr.sales_cnt AS curr_yr_cnt,
       curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
       curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM sales_detail curr_yr
JOIN sales_detail prev_yr ON curr_yr.i_brand_id = prev_yr.i_brand_id
AND curr_yr.i_class_id = prev_yr.i_class_id
AND curr_yr.i_category_id = prev_yr.i_category_id
AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
AND curr_yr.d_year = 2002
AND prev_yr.d_year = 2001
WHERE CAST(curr_yr.sales_cnt AS DECIMAL(17, 2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17, 2)) < 0.9
ORDER BY sales_cnt_diff,
         sales_amt_diff
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN381
-- =================================================================
SELECT channel,
       col_name,
       d_year,
       d_qoy,
       i_category,
       COUNT(*) sales_cnt,
       SUM(ext_sales_price) sales_amt
FROM
  (SELECT CASE
              WHEN ss_customer_sk IS NOT NULL THEN 'store'
              WHEN ws_promo_sk IS NOT NULL THEN 'web'
              WHEN cs_bill_customer_sk IS NOT NULL THEN 'catalog'
          END AS channel,
          CASE
              WHEN ss_customer_sk IS NOT NULL THEN 'ss_customer_sk'
              WHEN ws_promo_sk IS NOT NULL THEN 'ws_promo_sk'
              WHEN cs_bill_customer_sk IS NOT NULL THEN 'cs_bill_customer_sk'
          END AS col_name,
          d_year,
          d_qoy,
          i_category,
          CASE
              WHEN ss_customer_sk IS NOT NULL THEN ss_ext_sales_price
              WHEN ws_promo_sk IS NOT NULL THEN ws_ext_sales_price
              WHEN cs_bill_customer_sk IS NOT NULL THEN cs_ext_sales_price
          END AS ext_sales_price
   FROM store_sales
   FULL OUTER JOIN web_sales ON ss_sold_date_sk = ws_sold_date_sk
   AND ss_item_sk = ws_item_sk
   FULL OUTER JOIN catalog_sales ON ss_sold_date_sk = cs_sold_date_sk
   AND ss_item_sk = cs_item_sk
   JOIN item ON ss_item_sk = i_item_sk
   OR ws_item_sk = i_item_sk
   OR cs_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   OR ws_sold_date_sk = d_date_sk
   OR cs_sold_date_sk = d_date_sk
   WHERE (ss_customer_sk IS NOT NULL
          OR ws_promo_sk IS NOT NULL
          OR cs_bill_customer_sk IS NOT NULL) ) foo
GROUP BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
ORDER BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN382
-- =================================================================
WITH date_filtered AS
  (SELECT d_date_sk
   FROM date_dim
   WHERE d_date BETWEEN CAST('2000-08-10' AS date) AND (CAST('2000-08-10' AS date) + INTERVAL '30' day) ),
     store_sales_aggregated AS
  (SELECT 'store channel' AS channel,
          ss_store_sk AS store_sales_id,
          SUM(ss_ext_sales_price) AS sales,
          SUM(ss_net_profit) AS profit
   FROM store_sales
   JOIN date_filtered ON ss_sold_date_sk = d_date_sk
   GROUP BY ss_store_sk),
     store_returns_aggregated AS
  (SELECT sr_store_sk AS store_returns_id,
          SUM(sr_return_amt) AS returns,
          SUM(sr_net_loss) AS profit_loss
   FROM store_returns
   JOIN date_filtered ON sr_returned_date_sk = d_date_sk
   GROUP BY sr_store_sk),
     catalog_sales_aggregated AS
  (SELECT 'catalog channel' AS channel,
          cs_call_center_sk AS catalog_sales_id,
          SUM(cs_ext_sales_price) AS sales,
          SUM(cs_net_profit) AS profit
   FROM catalog_sales
   JOIN date_filtered ON cs_sold_date_sk = d_date_sk
   GROUP BY cs_call_center_sk),
     catalog_returns_aggregated AS
  (SELECT cr_call_center_sk AS catalog_returns_id,
          SUM(cr_return_amount) AS returns,
          SUM(cr_net_loss) AS profit_loss
   FROM catalog_returns
   JOIN date_filtered ON cr_returned_date_sk = d_date_sk
   GROUP BY cr_call_center_sk),
     web_sales_aggregated AS
  (SELECT 'web channel' AS channel,
          ws_web_page_sk AS web_sales_id,
          SUM(ws_ext_sales_price) AS sales,
          SUM(ws_net_profit) AS profit
   FROM web_sales
   JOIN date_filtered ON ws_sold_date_sk = d_date_sk
   GROUP BY ws_web_page_sk),
     web_returns_aggregated AS
  (SELECT wr_web_page_sk AS web_returns_id,
          SUM(wr_return_amt) AS returns,
          SUM(wr_net_loss) AS profit_loss
   FROM web_returns
   JOIN date_filtered ON wr_returned_date_sk = d_date_sk
   GROUP BY wr_web_page_sk)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns) AS returns,
       SUM(profit) AS profit
FROM
  (SELECT channel,
          store_sales_id AS id,
          sales,
          COALESCE(returns, 0) AS returns,
          (profit - COALESCE(profit_loss, 0)) AS profit
   FROM store_sales_aggregated
   LEFT JOIN store_returns_aggregated ON store_sales_aggregated.store_sales_id = store_returns_aggregated.store_returns_id
   UNION ALL SELECT channel,
                    catalog_sales_id AS id,
                    sales,
                    COALESCE(returns, 0) AS returns,
                    (profit - COALESCE(profit_loss, 0)) AS profit
   FROM catalog_sales_aggregated
   LEFT JOIN catalog_returns_aggregated ON catalog_sales_aggregated.catalog_sales_id = catalog_returns_aggregated.catalog_returns_id
   UNION ALL SELECT channel,
                    web_sales_id AS id,
                    sales,
                    COALESCE(returns, 0) AS returns,
                    (profit - COALESCE(profit_loss, 0)) AS profit
   FROM web_sales_aggregated
   LEFT JOIN web_returns_aggregated ON web_sales_aggregated.web_sales_id = web_returns_aggregated.web_returns_id) AS combined
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel,
         id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN383
-- =================================================================
SELECT ss.ss_customer_sk,
       ROUND(ss.ss_qty / (COALESCE(ws.ws_qty, 0) + COALESCE(cs.cs_qty, 0)), 2) AS ratio,
       ss.ss_qty AS store_qty,
       ss.ss_wc AS store_wholesale_cost,
       ss.ss_sp AS store_sales_price,
       COALESCE(ws.ws_qty, 0) + COALESCE(cs.cs_qty, 0) AS other_chan_qty,
       COALESCE(ws.ws_wc, 0) + COALESCE(cs.cs_wc, 0) AS other_chan_wholesale_cost,
       COALESCE(ws.ws_sp, 0) + COALESCE(cs.cs_sp, 0) AS other_chan_sales_price
FROM
  (SELECT d_year AS ss_sold_year,
          ss_item_sk,
          ss_customer_sk,
          SUM(ss_quantity) AS ss_qty,
          SUM(ss_wholesale_cost) AS ss_wc,
          SUM(ss_sales_price) AS ss_sp
   FROM store_sales
   LEFT JOIN store_returns ON sr_ticket_number = ss_ticket_number
   AND ss_item_sk = sr_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE sr_ticket_number IS NULL
   GROUP BY d_year,
            ss_item_sk,
            ss_customer_sk) ss
LEFT JOIN
  (SELECT d_year AS ws_sold_year,
          ws_item_sk,
          ws_bill_customer_sk AS ws_customer_sk,
          SUM(ws_quantity) AS ws_qty,
          SUM(ws_wholesale_cost) AS ws_wc,
          SUM(ws_sales_price) AS ws_sp
   FROM web_sales
   LEFT JOIN web_returns ON wr_order_number = ws_order_number
   AND ws_item_sk = wr_item_sk
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   WHERE wr_order_number IS NULL
   GROUP BY d_year,
            ws_item_sk,
            ws_bill_customer_sk) ws ON ws.ws_sold_year = ss.ss_sold_year
AND ws.ws_item_sk = ss.ss_item_sk
AND ws.ws_customer_sk = ss.ss_customer_sk
LEFT JOIN
  (SELECT d_year AS cs_sold_year,
          cs_item_sk,
          cs_bill_customer_sk AS cs_customer_sk,
          SUM(cs_quantity) AS cs_qty,
          SUM(cs_wholesale_cost) AS cs_wc,
          SUM(cs_sales_price) AS cs_sp
   FROM catalog_sales
   LEFT JOIN catalog_returns ON cr_order_number = cs_order_number
   AND cs_item_sk = cr_item_sk
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE cr_order_number IS NULL
   GROUP BY d_year,
            cs_item_sk,
            cs_bill_customer_sk) cs ON cs.cs_sold_year = ss.ss_sold_year
AND cs.cs_item_sk = ss.ss_item_sk
AND cs.cs_customer_sk = ss.ss_customer_sk
WHERE (COALESCE(ws.ws_qty, 0) > 0
       OR COALESCE(cs.cs_qty, 0) > 0)
  AND ss.ss_sold_year = 1998
ORDER BY ss.ss_customer_sk,
         ss.ss_qty DESC,
         ss.ss_wc DESC,
         ss.ss_sp DESC,
         other_chan_qty,
         other_chan_wholesale_cost,
         other_chan_sales_price,
         ratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN384
-- =================================================================
SELECT c_last_name,
       c_first_name,
       substr(s_city, 1, 30),
       ss_ticket_number,
       SUM(ss_coupon_amt) AS amt,
       SUM(ss_net_profit) AS profit
FROM store_sales
JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
JOIN store ON store_sales.ss_store_sk = store.s_store_sk
JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
WHERE (household_demographics.hd_dep_count = 7
       OR household_demographics.hd_vehicle_count > -1)
  AND date_dim.d_dow = 4
  AND date_dim.d_year IN (2000,
                          2001,
                          2002)
  AND store.s_number_employees BETWEEN 200 AND 295
GROUP BY c_last_name,
         c_first_name,
         ss_ticket_number,
         store.s_city
ORDER BY c_last_name,
         c_first_name,
         substr(s_city, 1, 30),
         profit
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN385
-- =================================================================
WITH sales_data AS
  (SELECT 'store channel' AS channel,
          'store' || s_store_id AS id,
          ss_ext_sales_price AS sales,
          COALESCE(sr_return_amt, 0) AS returns,
          (ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
   FROM store_sales
   LEFT OUTER JOIN store_returns ON (ss_item_sk = sr_item_sk
                                     AND ss_ticket_number = sr_ticket_number)
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   JOIN item ON ss_item_sk = i_item_sk
   JOIN promotion ON ss_promo_sk = p_promo_sk
   WHERE d_date BETWEEN CAST('2002-08-14' AS DATE) AND (CAST('2002-08-14' AS DATE) + INTERVAL '30' DAY)
     AND i_current_price > 50
     AND p_channel_tv = 'N'
   UNION ALL SELECT 'catalog channel' AS channel,
                    'catalog_page' || cp_catalog_page_id AS id,
                    cs_ext_sales_price AS sales,
                    COALESCE(cr_return_amount, 0) AS returns,
                    (cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
   FROM catalog_sales
   LEFT OUTER JOIN catalog_returns ON (cs_item_sk = cr_item_sk
                                       AND cs_order_number = cr_order_number)
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   JOIN catalog_page ON cs_catalog_page_sk = cp_catalog_page_sk
   JOIN item ON cs_item_sk = i_item_sk
   JOIN promotion ON cs_promo_sk = p_promo_sk
   WHERE d_date BETWEEN CAST('2002-08-14' AS DATE) AND (CAST('2002-08-14' AS DATE) + INTERVAL '30' DAY)
     AND i_current_price > 50
     AND p_channel_tv = 'N'
   UNION ALL SELECT 'web channel' AS channel,
                    'web_site' || web_site_id AS id,
                    ws_ext_sales_price AS sales,
                    COALESCE(wr_return_amt, 0) AS returns,
                    (ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
   FROM web_sales
   LEFT OUTER JOIN web_returns ON (ws_item_sk = wr_item_sk
                                   AND ws_order_number = wr_order_number)
   JOIN date_dim ON ws_sold_date_sk = d_date_sk
   JOIN web_site ON ws_web_site_sk = web_site_sk
   JOIN item ON ws_item_sk = i_item_sk
   JOIN promotion ON ws_promo_sk = p_promo_sk
   WHERE d_date BETWEEN CAST('2002-08-14' AS DATE) AND (CAST('2002-08-14' AS DATE) + INTERVAL '30' DAY)
     AND i_current_price > 50
     AND p_channel_tv = 'N' )
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM(returns) AS returns,
       SUM(profit) AS profit
FROM sales_data
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel,
         id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN386
-- =================================================================
WITH customer_total_return AS
  (SELECT cr_returning_customer_sk AS ctr_customer_sk,
          ca_state AS ctr_state,
          SUM(cr_return_amt_inc_tax) AS ctr_total_return
   FROM catalog_returns
   JOIN date_dim ON cr_returned_date_sk = d_date_sk
   JOIN customer_address ON cr_returning_addr_sk = ca_address_sk
   WHERE d_year = 2001
   GROUP BY cr_returning_customer_sk,
            ca_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       ca_street_number,
       ca_street_name,
       ca_street_type,
       ca_suite_number,
       ca_city,
       ca_county,
       ca_state,
       ca_zip,
       ca_country,
       ca_gmt_offset,
       ca_location_type,
       ctr_total_return
FROM customer
JOIN customer_address ON ca_address_sk = c_current_addr_sk
JOIN customer_total_return ctr1 ON ctr1.ctr_customer_sk = c_customer_sk
WHERE ca_state = 'TN'
  AND ctr1.ctr_total_return >
    (SELECT AVG(ctr_total_return) * 1.2
     FROM customer_total_return ctr2
     WHERE ctr1.ctr_state = ctr2.ctr_state )
ORDER BY c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         ca_street_number,
         ca_street_name,
         ca_street_type,
         ca_suite_number,
         ca_city,
         ca_county,
         ca_state,
         ca_zip,
         ca_country,
         ca_gmt_offset,
         ca_location_type,
         ctr_total_return
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN387
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
JOIN store_sales ON ss_item_sk = i_item_sk
WHERE i_current_price BETWEEN 58 AND 88
  AND d_date BETWEEN CAST('2001-01-13' AS date) AND (CAST('2001-01-13' AS date) + INTERVAL '60' day)
  AND i_manufact_id IN (259,
                        559,
                        580,
                        485)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY i_item_id,
         i_item_desc,
         i_current_price
ORDER BY i_item_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN388
-- =================================================================
WITH all_returns AS
  (SELECT i_item_id AS item_id,
          SUM(CASE
                  WHEN sr_returned_date_sk IS NOT NULL THEN sr_return_quantity
                  ELSE 0
              END) AS sr_item_qty,
          SUM(CASE
                  WHEN cr_returned_date_sk IS NOT NULL THEN cr_return_quantity
                  ELSE 0
              END) AS cr_item_qty,
          SUM(CASE
                  WHEN wr_returned_date_sk IS NOT NULL THEN wr_return_quantity
                  ELSE 0
              END) AS wr_item_qty
   FROM item
   LEFT JOIN store_returns ON sr_item_sk = i_item_sk
   LEFT JOIN catalog_returns ON cr_item_sk = i_item_sk
   LEFT JOIN web_returns ON wr_item_sk = i_item_sk
   JOIN date_dim ON ((sr_returned_date_sk = d_date_sk
                      OR cr_returned_date_sk = d_date_sk
                      OR wr_returned_date_sk = d_date_sk)
                     AND d_date IN
                       (SELECT d_date
                        FROM date_dim
                        WHERE d_week_seq IN
                            (SELECT d_week_seq
                             FROM date_dim
                             WHERE d_date IN ('2001-07-13',
                                              '2001-09-10',
                                              '2001-11-16',
                                              '2000-12-14') ) ))
   GROUP BY i_item_id)
SELECT item_id,
       sr_item_qty,
       sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS sr_dev,
       cr_item_qty,
       cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS cr_dev,
       wr_item_qty,
       wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS wr_dev,
       (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 AS average
FROM all_returns
WHERE sr_item_qty IS NOT NULL
  AND cr_item_qty IS NOT NULL
  AND wr_item_qty IS NOT NULL
ORDER BY item_id,
         sr_item_qty
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN389
-- =================================================================
SELECT c_customer_id AS customer_id,
       COALESCE(c_last_name, '') || ',' || COALESCE(c_first_name, '') AS customername
FROM customer
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
JOIN household_demographics ON hd_demo_sk = c_current_hdemo_sk
JOIN income_band ON ib_income_band_sk = hd_income_band_sk
JOIN store_returns ON sr_cdemo_sk = cd_demo_sk
WHERE ca_city = 'Woodland'
  AND ib_lower_bound >= 60306
  AND ib_upper_bound <= 60306 + 50000
ORDER BY c_customer_id
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN390
-- =================================================================
SELECT substr(r_reason_desc, 1, 20),
       AVG(CASE
               WHEN cd1.cd_marital_status = 'D'
                    AND cd1.cd_education_status = 'Primary'
                    AND ws_sales_price BETWEEN 100.00 AND 150.00 THEN ws_quantity
           END) AS avg_ws_quantity_1,
       AVG(CASE
               WHEN cd1.cd_marital_status = 'S'
                    AND cd1.cd_education_status = 'College'
                    AND ws_sales_price BETWEEN 50.00 AND 100.00 THEN ws_quantity
           END) AS avg_ws_quantity_2,
       AVG(CASE
               WHEN cd1.cd_marital_status = 'U'
                    AND cd1.cd_education_status = 'Advanced Degree'
                    AND ws_sales_price BETWEEN 150.00 AND 200.00 THEN ws_quantity
           END) AS avg_ws_quantity_3,
       AVG(CASE
               WHEN cd1.cd_marital_status = 'D'
                    AND cd1.cd_education_status = 'Primary'
                    AND ws_sales_price BETWEEN 100.00 AND 150.00 THEN wr_refunded_cash
           END) AS avg_wr_refunded_cash_1,
       AVG(CASE
               WHEN cd1.cd_marital_status = 'S'
                    AND cd1.cd_education_status = 'College'
                    AND ws_sales_price BETWEEN 50.00 AND 100.00 THEN wr_refunded_cash
           END) AS avg_wr_refunded_cash_2,
       AVG(CASE
               WHEN cd1.cd_marital_status = 'U'
                    AND cd1.cd_education_status = 'Advanced Degree'
                    AND ws_sales_price BETWEEN 150.00 AND 200.00 THEN wr_refunded_cash
           END) AS avg_wr_refunded_cash_3,
       AVG(CASE
               WHEN cd1.cd_marital_status = 'D'
                    AND cd1.cd_education_status = 'Primary'
                    AND ws_sales_price BETWEEN 100.00 AND 150.00 THEN wr_fee
           END) AS avg_wr_fee_1,
       AVG(CASE
               WHEN cd1.cd_marital_status = 'S'
                    AND cd1.cd_education_status = 'College'
                    AND ws_sales_price BETWEEN 50.00 AND 100.00 THEN wr_fee
           END) AS avg_wr_fee_2,
       AVG(CASE
               WHEN cd1.cd_marital_status = 'U'
                    AND cd1.cd_education_status = 'Advanced Degree'
                    AND ws_sales_price BETWEEN 150.00 AND 200.00 THEN wr_fee
           END) AS avg_wr_fee_3
FROM web_sales
JOIN web_returns ON ws_item_sk = wr_item_sk
AND ws_order_number = wr_order_number
JOIN web_page ON ws_web_page_sk = wp_web_page_sk
JOIN customer_demographics cd1 ON cd1.cd_demo_sk = wr_refunded_cdemo_sk
JOIN customer_demographics cd2 ON cd2.cd_demo_sk = wr_returning_cdemo_sk
JOIN customer_address ON ca_address_sk = wr_refunded_addr_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN reason ON r_reason_sk = wr_reason_sk
WHERE d_year = 1998
  AND cd1.cd_marital_status = cd2.cd_marital_status
  AND cd1.cd_education_status = cd2.cd_education_status
  AND ca_country = 'United States'
  AND ((ca_state IN ('NC',
                     'TX',
                     'IA')
        AND ws_net_profit BETWEEN 100 AND 200)
       OR (ca_state IN ('WI',
                        'WV',
                        'GA')
           AND ws_net_profit BETWEEN 150 AND 300)
       OR (ca_state IN ('OK',
                        'VA',
                        'KY')
           AND ws_net_profit BETWEEN 50 AND 250))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 20),
         avg_ws_quantity_1,
         avg_ws_quantity_2,
         avg_ws_quantity_3,
         avg_wr_refunded_cash_1,
         avg_wr_refunded_cash_2,
         avg_wr_refunded_cash_3,
         avg_wr_fee_1,
         avg_wr_fee_2,
         avg_wr_fee_3
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN391
-- =================================================================
SELECT *
FROM
  (SELECT SUM(ws_net_paid) AS total_sum,
          i_category,
          i_class,
          GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
          RANK() OVER (PARTITION BY GROUPING(i_category) + GROUPING(i_class),
                                    CASE
                                        WHEN GROUPING(i_class) = 0 THEN i_category
                                    END
                       ORDER BY SUM(ws_net_paid) DESC) AS rank_within_parent
   FROM web_sales
   JOIN date_dim d1 ON d1.d_date_sk = ws_sold_date_sk
   JOIN item ON i_item_sk = ws_item_sk
   WHERE d1.d_month_seq BETWEEN 1186 AND 1186 + 11
   GROUP BY ROLLUP(i_category, i_class)) AS tmp
ORDER BY lochierarchy DESC,
         CASE
             WHEN lochierarchy = 0 THEN i_category
         END,
         rank_within_parent
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN392
-- =================================================================
SELECT COUNT(*)
FROM
  (SELECT DISTINCT c_last_name,
                   c_first_name,
                   d_date
   FROM
     (SELECT c_last_name,
             c_first_name,
             d_date
      FROM store_sales
      JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1202 AND 1202 + 11
      EXCEPT SELECT c_last_name,
                    c_first_name,
                    d_date
      FROM catalog_sales
      JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1202 AND 1202 + 11
      EXCEPT SELECT c_last_name,
                    c_first_name,
                    d_date
      FROM web_sales
      JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
      JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
      WHERE d_month_seq BETWEEN 1202 AND 1202 + 11 ) AS combined_sales) AS cool_cust ;

-- =================================================================
-- Query ID: TPCDSN393
-- =================================================================
SELECT COUNT(CASE
                 WHEN time_dim.t_hour = 8
                      AND time_dim.t_minute >= 30 THEN 1
             END) AS h8_30_to_9,
       COUNT(CASE
                 WHEN time_dim.t_hour = 9
                      AND time_dim.t_minute < 30 THEN 1
             END) AS h9_to_9_30,
       COUNT(CASE
                 WHEN time_dim.t_hour = 9
                      AND time_dim.t_minute >= 30 THEN 1
             END) AS h9_30_to_10,
       COUNT(CASE
                 WHEN time_dim.t_hour = 10
                      AND time_dim.t_minute < 30 THEN 1
             END) AS h10_to_10_30,
       COUNT(CASE
                 WHEN time_dim.t_hour = 10
                      AND time_dim.t_minute >= 30 THEN 1
             END) AS h10_30_to_11,
       COUNT(CASE
                 WHEN time_dim.t_hour = 11
                      AND time_dim.t_minute < 30 THEN 1
             END) AS h11_to_11_30,
       COUNT(CASE
                 WHEN time_dim.t_hour = 11
                      AND time_dim.t_minute >= 30 THEN 1
             END) AS h11_30_to_12,
       COUNT(CASE
                 WHEN time_dim.t_hour = 12
                      AND time_dim.t_minute < 30 THEN 1
             END) AS h12_to_12_30
FROM store_sales
JOIN household_demographics ON ss_hdemo_sk = household_demographics.hd_demo_sk
JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE store.s_store_name = 'ese'
  AND ((household_demographics.hd_dep_count = 0
        AND household_demographics.hd_vehicle_count <= 2)
       OR (household_demographics.hd_dep_count = -1
           AND household_demographics.hd_vehicle_count <= 1)
       OR (household_demographics.hd_dep_count = 3
           AND household_demographics.hd_vehicle_count <= 5)) ;

-- =================================================================
-- Query ID: TPCDSN394
-- =================================================================
WITH sales_data AS
  (SELECT i_category,
          i_class,
          i_brand,
          s_store_name,
          s_company_name,
          d_moy,
          sum(ss_sales_price) AS sum_sales,
          avg(sum(ss_sales_price)) OVER (PARTITION BY i_category,
                                                      i_brand,
                                                      s_store_name,
                                                      s_company_name) AS avg_monthly_sales
   FROM item
   JOIN store_sales ON ss_item_sk = i_item_sk
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   JOIN store ON ss_store_sk = s_store_sk
   WHERE d_year = 2001
     AND ((i_category IN ('Books',
                          'Children',
                          'Electronics')
           AND i_class IN ('history',
                           'school-uniforms',
                           'audio'))
          OR (i_category IN ('Men',
                             'Sports',
                             'Shoes')
              AND i_class IN ('pants',
                              'tennis',
                              'womens')))
   GROUP BY i_category,
            i_class,
            i_brand,
            s_store_name,
            s_company_name,
            d_moy)
SELECT i_category,
       i_class,
       i_brand,
       s_store_name,
       s_company_name,
       d_moy,
       sum_sales,
       avg_monthly_sales
FROM sales_data
WHERE CASE
          WHEN avg_monthly_sales <> 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
          ELSE NULL
      END > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN395
-- =================================================================
SELECT CAST(SUM(CASE
                    WHEN t_hour BETWEEN 12 AND 13 THEN 1
                    ELSE 0
                END) AS DECIMAL(15, 4)) / CAST(SUM(CASE
                                                       WHEN t_hour BETWEEN 14 AND 15 THEN 1
                                                       ELSE 0
                                                   END) AS DECIMAL(15, 4)) AS am_pm_ratio
FROM web_sales
JOIN household_demographics ON ws_ship_hdemo_sk = household_demographics.hd_demo_sk
JOIN time_dim ON ws_sold_time_sk = time_dim.t_time_sk
JOIN web_page ON ws_web_page_sk = web_page.wp_web_page_sk
WHERE household_demographics.hd_dep_count = 6
  AND web_page.wp_char_count BETWEEN 5000 AND 5200
ORDER BY am_pm_ratio
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN396
-- =================================================================
SELECT cc_call_center_id AS Call_Center,
       cc_name AS Call_Center_Name,
       cc_manager AS Manager,
       SUM(cr_net_loss) AS Returns_Loss
FROM call_center
JOIN catalog_returns ON cr_call_center_sk = cc_call_center_sk
JOIN date_dim ON cr_returned_date_sk = d_date_sk
JOIN customer ON cr_returning_customer_sk = c_customer_sk
JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
JOIN household_demographics ON hd_demo_sk = c_current_hdemo_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
WHERE d_year = 2000
  AND d_moy = 12
  AND ((cd_marital_status = 'M'
        AND cd_education_status = 'Advanced Degree ')
       OR (cd_marital_status = 'W'
           AND cd_education_status = 'Unnknown'))
  AND hd_buy_potential LIKE 'Unknown%'
  AND ca_gmt_offset = -7
GROUP BY cc_call_center_id,
         cc_name,
         cc_manager,
         cd_marital_status,
         cd_education_status
ORDER BY SUM(cr_net_loss) DESC ;

-- =================================================================
-- Query ID: TPCDSN397
-- =================================================================
SELECT SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales,
     item,
     date_dim
WHERE i_manufact_id = 393
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN '2000-02-01' AND (CAST('2000-02-01' AS date) + INTERVAL '90' day)
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt >
    (SELECT 1.3 * AVG(ws_ext_discount_amt)
     FROM web_sales,
          date_dim
     WHERE ws_item_sk = i_item_sk
       AND d_date BETWEEN '2000-02-01' AND (CAST('2000-02-01' AS date) + INTERVAL '90' day)
       AND d_date_sk = ws_sold_date_sk )
GROUP BY ws_item_sk,
         d_date_sk
ORDER BY SUM(ws_ext_discount_amt)
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN398
-- =================================================================
SELECT ss_customer_sk,
       SUM(CASE
               WHEN sr_return_quantity IS NOT NULL THEN (ss_quantity - sr_return_quantity) * ss_sales_price
               ELSE (ss_quantity * ss_sales_price)
           END) AS sumsales
FROM store_sales
LEFT OUTER JOIN store_returns ON (sr_item_sk = ss_item_sk
                                  AND sr_ticket_number = ss_ticket_number)
JOIN reason ON sr_reason_sk = r_reason_sk
WHERE r_reason_desc = 'Package was damaged'
GROUP BY ss_customer_sk
ORDER BY sumsales,
         ss_customer_sk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN399
-- =================================================================
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws_net_profit) AS "total net profit"
FROM web_sales
JOIN date_dim ON ws_ship_date_sk = d_date_sk
JOIN customer_address ON ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws_web_site_sk = web_site_sk
WHERE d_date BETWEEN '2002-5-01' AND (CAST('2002-5-01' AS date) + INTERVAL '60' DAY)
  AND ca_state = 'OK'
  AND web_company_name = 'pri'
  AND EXISTS
    (SELECT 1
     FROM web_sales ws2
     WHERE web_sales.ws_order_number = ws2.ws_order_number
       AND web_sales.ws_warehouse_sk <> ws2.ws_warehouse_sk )
  AND NOT EXISTS
    (SELECT 1
     FROM web_returns wr1
     WHERE web_sales.ws_order_number = wr1.wr_order_number )
ORDER BY COUNT(DISTINCT ws_order_number)
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN400
-- =================================================================
WITH ws_wh AS
  (SELECT ws1.ws_order_number,
          ws1.ws_warehouse_sk wh1,
          ws2.ws_warehouse_sk wh2
   FROM web_sales ws1,
        web_sales ws2
   WHERE ws1.ws_order_number = ws2.ws_order_number
     AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk )
SELECT COUNT(DISTINCT ws1.ws_order_number) AS "order count",
       SUM(ws1.ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws1.ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_dim ON ws1.ws_ship_date_sk = d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
LEFT JOIN web_returns ON ws1.ws_order_number = web_returns.wr_order_number
WHERE d_date BETWEEN '2001-4-01' AND (CAST('2001-4-01' AS DATE) + INTERVAL '60' DAY)
  AND ca_state = 'VA'
  AND web_company_name = 'pri'
  AND ws1.ws_order_number IN
    (SELECT ws_order_number
     FROM ws_wh)
ORDER BY COUNT(DISTINCT ws1.ws_order_number)
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN401
-- =================================================================
SELECT COUNT(*)
FROM store_sales
JOIN household_demographics ON ss_hdemo_sk = household_demographics.hd_demo_sk
JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE time_dim.t_hour = 8
  AND time_dim.t_minute >= 30
  AND household_demographics.hd_dep_count = 0
  AND store.s_store_name = 'ese'
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN402
-- =================================================================
SELECT SUM(CASE
               WHEN is_store_sales = 1
                    AND is_catalog_sales = 0 THEN 1
               ELSE 0
           END) AS store_only,
       SUM(CASE
               WHEN is_store_sales = 0
                    AND is_catalog_sales = 1 THEN 1
               ELSE 0
           END) AS catalog_only,
       SUM(CASE
               WHEN is_store_sales = 1
                    AND is_catalog_sales = 1 THEN 1
               ELSE 0
           END) AS store_and_catalog
FROM
  (SELECT ss_customer_sk AS customer_sk,
          ss_item_sk AS item_sk,
          1 AS is_store_sales,
          0 AS is_catalog_sales
   FROM store_sales
   JOIN date_dim ON ss_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1199 AND 1199 + 11
   GROUP BY ss_customer_sk,
            ss_item_sk
   UNION ALL SELECT cs_bill_customer_sk AS customer_sk,
                    cs_item_sk AS item_sk,
                    0 AS is_store_sales,
                    1 AS is_catalog_sales
   FROM catalog_sales
   JOIN date_dim ON cs_sold_date_sk = d_date_sk
   WHERE d_month_seq BETWEEN 1199 AND 1199 + 11
   GROUP BY cs_bill_customer_sk,
            cs_item_sk) AS combined_sales
GROUP BY customer_sk,
         item_sk
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN403
-- =================================================================
SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ss_ext_sales_price) AS itemrevenue,
       SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM store_sales
JOIN item ON ss_item_sk = i_item_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE i_category IN ('Men',
                     'Sports',
                     'Jewelry')
  AND d_date BETWEEN CAST('1999-02-05' AS DATE) AND (CAST('1999-02-05' AS DATE) + INTERVAL '30' DAY)
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio ;

-- =================================================================
-- Query ID: TPCDSN404
-- =================================================================
SELECT substr(w_warehouse_name, 1, 20),
       sm_type,
       cc_name,
       SUM(CASE
               WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS "30 days",
       SUM(CASE
               WHEN (cs_ship_date_sk - cs_sold_date_sk > 30)
                    AND (cs_ship_date_sk - cs_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS "31-60 days",
       SUM(CASE
               WHEN (cs_ship_date_sk - cs_sold_date_sk > 60)
                    AND (cs_ship_date_sk - cs_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS "61-90 days",
       SUM(CASE
               WHEN (cs_ship_date_sk - cs_sold_date_sk > 90)
                    AND (cs_ship_date_sk - cs_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS "91-120 days",
       SUM(CASE
               WHEN (cs_ship_date_sk - cs_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS ">120 days"
FROM catalog_sales
JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
JOIN ship_mode ON cs_ship_mode_sk = sm_ship_mode_sk
JOIN call_center ON cs_call_center_sk = cc_call_center_sk
JOIN date_dim ON cs_ship_date_sk = d_date_sk
WHERE d_month_seq BETWEEN 1194 AND 1194 + 11
GROUP BY substr(w_warehouse_name, 1, 20),
         sm_type,
         cc_name
ORDER BY substr(w_warehouse_name, 1, 20),
         sm_type,
         cc_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN405
-- =================================================================
with customer_total_return as
  (select sr_customer_sk as ctr_customer_sk,
          sr_store_sk as ctr_store_sk,
          sum(SR_FEE) as ctr_total_return
   from store_returns,
        date_dim
   where sr_returned_date_sk = d_date_sk
     and d_year = 2000
   group by sr_customer_sk,
            sr_store_sk)
select c_customer_id
from customer_total_return ctr1,
     store,
     customer
where ctr1.ctr_total_return >
    (select avg(ctr_total_return) * 1.2
     from customer_total_return ctr2
     where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  and s_store_sk = ctr1.ctr_store_sk
  and s_state = 'TN'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN406
-- =================================================================
with wscs as
  (select sold_date_sk,
          sales_price
   from
     (select ws_sold_date_sk sold_date_sk,
             ws_ext_sales_price sales_price
      from web_sales
      union all select cs_sold_date_sk sold_date_sk,
                       cs_ext_sales_price sales_price
      from catalog_sales) as tmp),
     wswscs as
  (select d_week_seq,
          sum(case
                  when (d_day_name = 'Sunday') then sales_price
                  else null
              end) sun_sales,
          sum(case
                  when (d_day_name = 'Monday') then sales_price
                  else null
              end) mon_sales,
          sum(case
                  when (d_day_name = 'Tuesday') then sales_price
                  else null
              end) tue_sales,
          sum(case
                  when (d_day_name = 'Wednesday') then sales_price
                  else null
              end) wed_sales,
          sum(case
                  when (d_day_name = 'Thursday') then sales_price
                  else null
              end) thu_sales,
          sum(case
                  when (d_day_name = 'Friday') then sales_price
                  else null
              end) fri_sales,
          sum(case
                  when (d_day_name = 'Saturday') then sales_price
                  else null
              end) sat_sales
   from wscs,
        date_dim
   where d_date_sk = sold_date_sk
   group by d_week_seq)
select d_week_seq1,
       round(sun_sales1 / sun_sales2, 2),
       round(mon_sales1 / mon_sales2, 2),
       round(tue_sales1 / tue_sales2, 2),
       round(wed_sales1 / wed_sales2, 2),
       round(thu_sales1 / thu_sales2, 2),
       round(fri_sales1 / fri_sales2, 2),
       round(sat_sales1 / sat_sales2, 2)
from
  (select wswscs.d_week_seq d_week_seq1,
          sun_sales sun_sales1,
          mon_sales mon_sales1,
          tue_sales tue_sales1,
          wed_sales wed_sales1,
          thu_sales thu_sales1,
          fri_sales fri_sales1,
          sat_sales sat_sales1
   from wswscs,
        date_dim
   where date_dim.d_week_seq = wswscs.d_week_seq
     and d_year = 1998 ) y,

  (select wswscs.d_week_seq d_week_seq2,
          sun_sales sun_sales2,
          mon_sales mon_sales2,
          tue_sales tue_sales2,
          wed_sales wed_sales2,
          thu_sales thu_sales2,
          fri_sales fri_sales2,
          sat_sales sat_sales2
   from wswscs,
        date_dim
   where date_dim.d_week_seq = wswscs.d_week_seq
     and d_year = 1999 ) z
where d_week_seq1 = d_week_seq2 - 53
order by d_week_seq1 ;

-- =================================================================
-- Query ID: TPCDSN407
-- =================================================================
select dt.d_year,
       item.i_brand_id brand_id,
       item.i_brand brand,
       sum(ss_sales_price) sum_agg
from date_dim dt,
     store_sales,
     item
where dt.d_date_sk = store_sales.ss_sold_date_sk
  and store_sales.ss_item_sk = item.i_item_sk
  and item.i_manufact_id = 816
  and dt.d_moy = 11
group by dt.d_year,
         item.i_brand,
         item.i_brand_id
order by dt.d_year,
         sum_agg desc,
         brand_id
limit 100;

-- =================================================================
-- Query ID: TPCDSN408
-- =================================================================
with year_total as
  (select c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          c_preferred_cust_flag customer_preferred_cust_flag,
          c_birth_country customer_birth_country,
          c_login customer_login,
          c_email_address customer_email_address,
          d_year dyear,
          sum(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) year_total,
          's' sale_type
   from customer,
        store_sales,
        date_dim
   where c_customer_sk = ss_customer_sk
     and ss_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   union all select c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    c_preferred_cust_flag customer_preferred_cust_flag,
                    c_birth_country customer_birth_country,
                    c_login customer_login,
                    c_email_address customer_email_address,
                    d_year dyear,
                    sum((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) year_total,
                    'c' sale_type
   from customer,
        catalog_sales,
        date_dim
   where c_customer_sk = cs_bill_customer_sk
     and cs_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   union all select c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    c_preferred_cust_flag customer_preferred_cust_flag,
                    c_birth_country customer_birth_country,
                    c_login customer_login,
                    c_email_address customer_email_address,
                    d_year dyear,
                    sum((((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2)) year_total,
                    'w' sale_type
   from customer,
        web_sales,
        date_dim
   where c_customer_sk = ws_bill_customer_sk
     and ws_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year)
select t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_birth_country
from year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_c_firstyear,
     year_total t_c_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_c_secyear.customer_id
  and t_s_firstyear.customer_id = t_c_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_c_firstyear.sale_type = 'c'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_c_secyear.sale_type = 'c'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 1999
  and t_s_secyear.dyear = 1999
  and t_c_firstyear.dyear = 1999
  and t_c_secyear.dyear = 1999
  and t_w_firstyear.dyear = 1999
  and t_w_secyear.dyear = 1999
  and t_s_firstyear.year_total > 0
  and t_c_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case
          when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total
          else null
      end >= case
                 when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total
                 else null
             end
  and case
          when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total
          else null
      end >= case
                 when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total
                 else null
             end
order by t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_birth_country
limit 100;

-- =================================================================
-- Query ID: TPCDSN409
-- =================================================================
with ssr as
  (select s_store_id,
          sum(sales_price) as sales,
          sum(profit) as profit,
          sum(return_amt) as returns,
          sum(net_loss) as profit_loss
   from
     (select ss_store_sk as store_sk,
             ss_sold_date_sk as date_sk,
             ss_ext_sales_price as sales_price,
             ss_net_profit as profit,
             cast(0 as decimal(7, 2)) as return_amt,
             cast(0 as decimal(7, 2)) as net_loss
      from store_sales
      union all select sr_store_sk as store_sk,
                       sr_returned_date_sk as date_sk,
                       cast(0 as decimal(7, 2)) as sales_price,
                       cast(0 as decimal(7, 2)) as profit,
                       sr_return_amt as return_amt,
                       sr_net_loss as net_loss
      from store_returns) salesreturns,
        date_dim,
        store
   where date_sk = d_date_sk
     and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day)
     and store_sk = s_store_sk
   group by s_store_id),
     csr as
  (select cp_catalog_page_id,
          sum(sales_price) as sales,
          sum(profit) as profit,
          sum(return_amt) as returns,
          sum(net_loss) as profit_loss
   from
     (select cs_catalog_page_sk as page_sk,
             cs_sold_date_sk as date_sk,
             cs_ext_sales_price as sales_price,
             cs_net_profit as profit,
             cast(0 as decimal(7, 2)) as return_amt,
             cast(0 as decimal(7, 2)) as net_loss
      from catalog_sales
      union all select cr_catalog_page_sk as page_sk,
                       cr_returned_date_sk as date_sk,
                       cast(0 as decimal(7, 2)) as sales_price,
                       cast(0 as decimal(7, 2)) as profit,
                       cr_return_amount as return_amt,
                       cr_net_loss as net_loss
      from catalog_returns) salesreturns,
        date_dim,
        catalog_page
   where date_sk = d_date_sk
     and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day)
     and page_sk = cp_catalog_page_sk
   group by cp_catalog_page_id),
     wsr as
  (select web_site_id,
          sum(sales_price) as sales,
          sum(profit) as profit,
          sum(return_amt) as returns,
          sum(net_loss) as profit_loss
   from
     (select ws_web_site_sk as wsr_web_site_sk,
             ws_sold_date_sk as date_sk,
             ws_ext_sales_price as sales_price,
             ws_net_profit as profit,
             cast(0 as decimal(7, 2)) as return_amt,
             cast(0 as decimal(7, 2)) as net_loss
      from web_sales
      union all select ws_web_site_sk as wsr_web_site_sk,
                       wr_returned_date_sk as date_sk,
                       cast(0 as decimal(7, 2)) as sales_price,
                       cast(0 as decimal(7, 2)) as profit,
                       wr_return_amt as return_amt,
                       wr_net_loss as net_loss
      from web_returns
      left outer join web_sales on (wr_item_sk = ws_item_sk
                                    and wr_order_number = ws_order_number)) salesreturns,
        date_dim,
        web_site
   where date_sk = d_date_sk
     and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + interval '14' day)
     and wsr_web_site_sk = web_site_sk
   group by web_site_id)
select channel,
       id,
       sum(sales) as sales,
       sum(returns) as returns,
       sum(profit) as profit
from
  (select 'store channel' as channel,
          'store' || s_store_id as id,
          sales,
          returns,
          (profit - profit_loss) as profit
   from ssr
   union all select 'catalog channel' as channel,
                    'catalog_page' || cp_catalog_page_id as id,
                    sales,
                    returns,
                    (profit - profit_loss) as profit
   from csr
   union all select 'web channel' as channel,
                    'web_site' || web_site_id as id,
                    sales,
                    returns,
                    (profit - profit_loss) as profit
   from wsr) x
group by rollup (channel,
                 id)
order by channel,
         id
limit 100;

-- =================================================================
-- Query ID: TPCDSN410
-- =================================================================
select a.ca_state state,
       count(*) cnt
from customer_address a
join customer c on a.ca_address_sk = c.c_current_addr_sk
join store_sales s on c.c_customer_sk = s.ss_customer_sk
join date_dim d on s.ss_sold_date_sk = d.d_date_sk
join item i on s.ss_item_sk = i.i_item_sk
where d.d_month_seq =
    (select distinct d_month_seq
     from date_dim
     where d_year = 2002
       and d_moy = 3)
  and i.i_current_price > 1.2 *
    (select avg(j.i_current_price)
     from item j
     where j.i_category = i.i_category)
group by a.ca_state
having count(*) >= 10
order by cnt,
         a.ca_state
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN411
-- =================================================================
select i_item_id,
       avg(ss_quantity) agg1,
       avg(ss_list_price) agg2,
       avg(ss_coupon_amt) agg3,
       avg(ss_sales_price) agg4
from store_sales,
     customer_demographics,
     date_dim,
     item,
     promotion
where ss_sold_date_sk = d_date_sk
  and ss_item_sk = i_item_sk
  and ss_cdemo_sk = cd_demo_sk
  and ss_promo_sk = p_promo_sk
  and cd_gender = 'F'
  and cd_marital_status = 'W'
  and cd_education_status = 'College'
  and (p_channel_email = 'N'
       or p_channel_event = 'N')
  and d_year = 2001
group by i_item_id
order by i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN412
-- =================================================================
SELECT s_store_name,
       SUM(ss_net_profit)
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN
  (SELECT DISTINCT SUBSTR(ca_zip, 1, 5) AS ca_zip
   FROM customer_address
   WHERE SUBSTR(ca_zip, 1, 5) IN
       (SELECT ca_zip
        FROM
          (SELECT SUBSTR(ca_zip, 1, 5) AS ca_zip,
                  COUNT(*) AS cnt
           FROM customer_address
           JOIN customer ON ca_address_sk = c_current_addr_sk
           WHERE c_preferred_cust_flag = 'Y'
           GROUP BY ca_zip
           HAVING COUNT(*) > 10) AS A1) ) AS V1 ON SUBSTR(s_zip, 1, 1) = SUBSTR(V1.ca_zip, 1, 1)
WHERE d_qoy = 2
  AND d_year = 1998
GROUP BY s_store_name
ORDER BY s_store_name
LIMIT 100 ;

-- =================================================================
-- Query ID: TPCDSN413
-- =================================================================
select
  (select case
              when count(*) > 1071 then avg(ss_ext_tax)
              else avg(ss_net_paid_inc_tax)
          end
   from store_sales
   where ss_quantity between 1 and 20) as bucket1,

  (select case
              when count(*) > 39161 then avg(ss_ext_tax)
              else avg(ss_net_paid_inc_tax)
          end
   from store_sales
   where ss_quantity between 21 and 40) as bucket2,

  (select case
              when count(*) > 29434 then avg(ss_ext_tax)
              else avg(ss_net_paid_inc_tax)
          end
   from store_sales
   where ss_quantity between 41 and 60) as bucket3,

  (select case
              when count(*) > 6568 then avg(ss_ext_tax)
              else avg(ss_net_paid_inc_tax)
          end
   from store_sales
   where ss_quantity between 61 and 80) as bucket4,

  (select case
              when count(*) > 21216 then avg(ss_ext_tax)
              else avg(ss_net_paid_inc_tax)
          end
   from store_sales
   where ss_quantity between 81 and 100) as bucket5
from reason
where r_reason_sk = 1 ;

-- =================================================================
-- Query ID: TPCDSN414
-- =================================================================
select cd_gender,
       cd_marital_status,
       cd_education_status,
       count(*) cnt1,
       cd_purchase_estimate,
       count(*) cnt2,
       cd_credit_rating,
       count(*) cnt3,
       cd_dep_count,
       count(*) cnt4,
       cd_dep_employed_count,
       count(*) cnt5,
       cd_dep_college_count,
       count(*) cnt6
from customer c,
     customer_address ca,
     customer_demographics
where c.c_current_addr_sk = ca.ca_address_sk
  and ca_county in ('Fairfield County',
                    'Campbell County',
                    'Washtenaw County',
                    'Escambia County',
                    'Cleburne County',
                    'United States',
                    '1')
  and cd_demo_sk = c.c_current_cdemo_sk
  and exists
    (select 1
     from store_sales,
          date_dim
     where c.c_customer_sk = ss_customer_sk
       and ss_sold_date_sk = d_date_sk
       and d_year = 1999
       and d_moy between 1 and 12)
  and (exists
         (select 1
          from web_sales,
               date_dim
          where c.c_customer_sk = ws_bill_customer_sk
            and ws_sold_date_sk = d_date_sk
            and d_year = 1999
            and d_moy between 1 and 12)
       or exists
         (select 1
          from catalog_sales,
               date_dim
          where c.c_customer_sk = cs_ship_customer_sk
            and cs_sold_date_sk = d_date_sk
            and d_year = 1999
            and d_moy between 1 and 12))
group by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
order by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN415
-- =================================================================
with year_total as
  (select c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          c_preferred_cust_flag customer_preferred_cust_flag,
          c_birth_country customer_birth_country,
          c_login customer_login,
          c_email_address customer_email_address,
          d_year dyear,
          sum(ss_ext_list_price - ss_ext_discount_amt) year_total,
          's' sale_type
   from customer,
        store_sales,
        date_dim
   where c_customer_sk = ss_customer_sk
     and ss_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   union all select c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    c_preferred_cust_flag customer_preferred_cust_flag,
                    c_birth_country customer_birth_country,
                    c_login customer_login,
                    c_email_address customer_email_address,
                    d_year dyear,
                    sum(ws_ext_list_price - ws_ext_discount_amt) year_total,
                    'w' sale_type
   from customer,
        web_sales,
        date_dim
   where c_customer_sk = ws_bill_customer_sk
     and ws_sold_date_sk = d_date_sk
   group by c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year)
select t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_email_address
from year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.dyear = 1999
  and t_s_secyear.dyear = 1999
  and t_w_firstyear.dyear = 1999
  and t_w_secyear.dyear = 1999
  and t_s_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case
          when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total
          else 0.0
      end >= case
                 when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total
                 else 0.0
             end
order by t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_email_address
limit 100;

-- =================================================================
-- Query ID: TPCDSN416
-- =================================================================
select i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       sum(ws_ext_sales_price) as itemrevenue,
       sum(ws_ext_sales_price) * 100 / sum(sum(ws_ext_sales_price)) over (partition by i_class) as revenueratio
from web_sales,
     item,
     date_dim
where ws_item_sk = i_item_sk
  and exists
    (select 1
     from (
           values ('Men'), ('Books'), ('Electronics')) as categories(category)
     where categories.category = i_category)
  and ws_sold_date_sk = d_date_sk
  and d_date between cast('2001-06-15' as date) and (cast('2001-06-15' as date) + interval '30' day)
group by i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
order by i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN417
-- =================================================================
select avg(ss_quantity),
       avg(ss_ext_sales_price),
       avg(ss_ext_wholesale_cost),
       sum(ss_ext_wholesale_cost)
from store_sales,
     store,
     customer_demographics,
     household_demographics,
     customer_address,
     date_dim
where s_store_sk = ss_store_sk
  and ss_sold_date_sk = d_date_sk
  and d_year = 2001
  and ((ss_hdemo_sk = hd_demo_sk
        and cd_demo_sk = ss_cdemo_sk
        and cd_marital_status = 'M'
        and cd_education_status = 'College'
        and ss_sales_price between 100.00 and 150.00
        and hd_dep_count = 3)
       or (ss_hdemo_sk = hd_demo_sk
           and cd_demo_sk = ss_cdemo_sk
           and cd_marital_status = 'D'
           and cd_education_status = 'Primary'
           and ss_sales_price between 50.00 and 100.00
           and hd_dep_count = 1)
       or (ss_hdemo_sk = hd_demo_sk
           and cd_demo_sk = ss_cdemo_sk
           and cd_marital_status = 'W'
           and cd_education_status = '2 yr Degree'
           and ss_sales_price between 150.00 and 200.00
           and hd_dep_count = 1))
  and ((ss_addr_sk = ca_address_sk
        and ca_country = 'United States'
        and exists
          (select 1
           from customer_address ca
           where ca.ca_state in ('IL',
                                 'TN',
                                 'TX')
             and ca.ca_address_sk = ss_addr_sk)
        and ss_net_profit between 100 and 200)
       or (ss_addr_sk = ca_address_sk
           and ca_country = 'United States'
           and exists
             (select 1
              from customer_address ca
              where ca.ca_state in ('WY',
                                    'OH',
                                    'ID')
                and ca.ca_address_sk = ss_addr_sk)
           and ss_net_profit between 150 and 300)
       or (ss_addr_sk = ca_address_sk
           and ca_country = 'United States'
           and exists
             (select 1
              from customer_address ca
              where ca.ca_state in ('MS',
                                    'SC',
                                    'IA')
                and ca.ca_address_sk = ss_addr_sk)
           and ss_net_profit between 50 and 250)) ;

-- =================================================================
-- Query ID: TPCDSN418
-- =================================================================
select *
from customer;

-- =================================================================
-- Query ID: TPCDSN419
-- =================================================================
select ca_zip,
       sum(cs_sales_price)
from catalog_sales,
     customer,
     customer_address,
     date_dim
where cs_bill_customer_sk = c_customer_sk
  and c_current_addr_sk = ca_address_sk
  and (exists
         (select 1
          from customer_address
          where substr(ca_zip, 1, 5) in ('85669',
                                         '86197',
                                         '88274',
                                         '83405',
                                         '86475',
                                         '85392',
                                         '85460',
                                         '80348',
                                         '81792'))
       or ca_state in ('CA',
                       'WA',
                       'GA')
       or cs_sales_price > 500)
  and cs_sold_date_sk = d_date_sk
  and d_qoy = 2
  and d_year = 2001
group by ca_zip
order by ca_zip
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN420
-- =================================================================
select count(distinct cs_order_number) as "order count",
       sum(cs_ext_ship_cost) as "total shipping cost",
       sum(cs_net_profit) as "total net profit"
from catalog_sales cs1,
     date_dim,
     customer_address,
     call_center
where d_date between '2002-4-01' and (cast('2002-4-01' as date) + interval '60' day)
  and cs1.cs_ship_date_sk = d_date_sk
  and cs1.cs_ship_addr_sk = ca_address_sk
  and ca_state = 'PA'
  and cs1.cs_call_center_sk = cc_call_center_sk
  and cc_county in ('Williamson County',
                    'Williamson County',
                    'Williamson County',
                    'Williamson County',
                    'Williamson County')
  and cs1.cs_order_number in
    (select cs2.cs_order_number
     from catalog_sales cs2
     where cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
  and cs1.cs_order_number not in
    (select cr1.cr_order_number
     from catalog_returns cr1)
order by count(distinct cs_order_number)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN421
-- =================================================================
select i_item_id,
       i_item_desc,
       s_state,
       count(ss_quantity) as store_sales_quantitycount,
       avg(ss_quantity) as store_sales_quantityave,
       stddev_samp(ss_quantity) as store_sales_quantitystdev,
       stddev_samp(ss_quantity) / avg(ss_quantity) as store_sales_quantitycov,
       count(sr_return_quantity) as store_returns_quantitycount,
       avg(sr_return_quantity) as store_returns_quantityave,
       stddev_samp(sr_return_quantity) as store_returns_quantitystdev,
       stddev_samp(sr_return_quantity) / avg(sr_return_quantity) as store_returns_quantitycov,
       count(cs_quantity) as catalog_sales_quantitycount,
       avg(cs_quantity) as catalog_sales_quantityave,
       stddev_samp(cs_quantity) as catalog_sales_quantitystdev,
       stddev_samp(cs_quantity) / avg(cs_quantity) as catalog_sales_quantitycov
from store_sales,
     store_returns,
     catalog_sales,
     date_dim d1,
     date_dim d2,
     date_dim d3,
     store,
     item
where d1.d_quarter_name = '2001Q1'
  and d1.d_date_sk = ss_sold_date_sk
  and i_item_sk = ss_item_sk
  and s_store_sk = ss_store_sk
  and ss_customer_sk = sr_customer_sk
  and ss_item_sk = sr_item_sk
  and ss_ticket_number = sr_ticket_number
  and sr_returned_date_sk = d2.d_date_sk
  and d2.d_quarter_name in ('2001Q1',
                            '2001Q2',
                            '2001Q3')
  and sr_customer_sk = cs_bill_customer_sk
  and sr_item_sk = cs_item_sk
  and cs_sold_date_sk = d3.d_date_sk
  and d3.d_quarter_name in ('2001Q1',
                            '2001Q2',
                            '2001Q3')
group by i_item_id,
         i_item_desc,
         s_state
order by i_item_id,
         i_item_desc,
         s_state
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN422
-- =================================================================
select i_item_id,
       ca_country,
       ca_state,
       ca_county,
       avg(cast(cs_quantity as decimal(12, 2))) agg1,
       avg(cast(cs_list_price as decimal(12, 2))) agg2,
       avg(cast(cs_coupon_amt as decimal(12, 2))) agg3,
       avg(cast(cs_sales_price as decimal(12, 2))) agg4,
       avg(cast(cs_net_profit as decimal(12, 2))) agg5,
       avg(cast(c_birth_year as decimal(12, 2))) agg6,
       avg(cast(cd1.cd_dep_count as decimal(12, 2))) agg7
from catalog_sales,
     customer_demographics cd1,
     customer_demographics cd2,
     customer,
     customer_address,
     date_dim,
     item
where cs_sold_date_sk = d_date_sk
  and cs_item_sk = i_item_sk
  and cs_bill_cdemo_sk = cd1.cd_demo_sk
  and cs_bill_customer_sk = c_customer_sk
  and cd1.cd_gender = 'F'
  and cd1.cd_education_status = 'Primary'
  and c_current_cdemo_sk = cd2.cd_demo_sk
  and c_current_addr_sk = ca_address_sk
  and c_birth_month in (1,
                        3,
                        7,
                        11,
                        10,
                        4)
  and d_year = 2001
  and ca_state in ('AL',
                   'MO',
                   'TN',
                   'GA',
                   'MT',
                   'IN',
                   'CA')
group by rollup (i_item_id,
                 ca_country,
                 ca_state,
                 ca_county)
order by ca_country,
         ca_state,
         ca_county,
         i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN423
-- =================================================================
select i_brand_id brand_id,
       i_brand brand,
       i_manufact_id,
       i_manufact,
       sum(ss_ext_sales_price) ext_price
from date_dim,
     store_sales,
     item,
     customer,
     customer_address,
     store
where d_date_sk = ss_sold_date_sk
  and ss_item_sk = i_item_sk
  and i_manager_id = 14
  and d_moy = 11
  and d_year = 2002
  and ss_customer_sk = c_customer_sk
  and c_current_addr_sk = ca_address_sk
  and substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
  and ss_store_sk = s_store_sk
group by i_brand,
         i_brand_id,
         i_manufact_id,
         i_manufact
order by ext_price desc,
         i_brand,
         i_brand_id,
         i_manufact_id,
         i_manufact
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN424
-- =================================================================
select i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       sum(cs_ext_sales_price) as itemrevenue,
       sum(cs_ext_sales_price) * 100 / sum(sum(cs_ext_sales_price)) over (partition by i_class) as revenueratio
from catalog_sales,
     item,
     date_dim
where cs_item_sk = i_item_sk
  and exists
    (select 1
     from (
           values ('Books'), ('Music'), ('Sports')) as categories(category)
     where categories.category = i_category)
  and cs_sold_date_sk = d_date_sk
  and d_date between cast('2002-06-18' as date) and (cast('2002-06-18' as date) + interval '30' day)
group by i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
order by i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN425
-- =================================================================
select *
from
  (select w_warehouse_name,
          i_item_id,
          sum(case
                  when (cast(d_date as date) < cast ('1999-06-22' as date)) then inv_quantity_on_hand
                  else 0
              end) as inv_before,
          sum(case
                  when (cast(d_date as date) >= cast ('1999-06-22' as date)) then inv_quantity_on_hand
                  else 0
              end) as inv_after
   from inventory,
        warehouse,
        item,
        date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk = inv_item_sk
     and inv_warehouse_sk = w_warehouse_sk
     and inv_date_sk = d_date_sk
     and d_date between (cast ('1999-06-22' as date) - interval '30' day) and (cast ('1999-06-22' as date) + interval '30' day)
   group by w_warehouse_name,
            i_item_id) x
where (case
           when inv_before > 0 then inv_after / inv_before
           else null
       end) between 2.0 / 3.0 and 3.0 / 2.0
order by w_warehouse_name,
         i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN426
-- =================================================================
select i_product_name,
       i_brand,
       i_class,
       i_category,
       avg(inv_quantity_on_hand) qoh
from inventory,
     date_dim,
     item
where inv_date_sk = d_date_sk
  and inv_item_sk = i_item_sk
  and d_month_seq between 1200 and 1200 + 11
group by rollup(i_product_name, i_brand, i_class, i_category)
order by qoh,
         i_product_name,
         i_brand,
         i_class,
         i_category
limit 100;

-- =================================================================
-- Query ID: TPCDSN427
-- =================================================================
select *
from customer;

-- =================================================================
-- Query ID: TPCDSN428
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'khaki'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN429
-- =================================================================
with ssales as
  (select c_last_name,
          c_first_name,
          s_store_name,
          ca_state,
          s_state,
          i_color,
          i_current_price,
          i_manager_id,
          i_units,
          i_size,
          sum(ss_net_paid) netpaid
   from store_sales,
        store_returns,
        store,
        item,
        customer,
        customer_address
   where ss_ticket_number = sr_ticket_number
     and ss_item_sk = sr_item_sk
     and ss_customer_sk = c_customer_sk
     and ss_item_sk = i_item_sk
     and ss_store_sk = s_store_sk
     and c_current_addr_sk = ca_address_sk
     and c_birth_country <> upper(ca_country)
     and s_zip = ca_zip
     and s_market_id = 2
   group by c_last_name,
            c_first_name,
            s_store_name,
            ca_state,
            s_state,
            i_color,
            i_current_price,
            i_manager_id,
            i_units,
            i_size)
select c_last_name,
       c_first_name,
       s_store_name,
       sum(netpaid) paid
from ssales
where i_color = 'seashell'
group by c_last_name,
         c_first_name,
         s_store_name
having sum(netpaid) >
  (select 0.05 * avg(netpaid)
   from ssales)
order by c_last_name,
         c_first_name,
         s_store_name ;

-- =================================================================
-- Query ID: TPCDSN430
-- =================================================================
select i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       max(ss_net_profit) as store_sales_profit,
       max(sr_net_loss) as store_returns_loss,
       max(cs_net_profit) as catalog_sales_profit
from store_sales,
     store_returns,
     catalog_sales,
     date_dim d1,
     date_dim d2,
     date_dim d3,
     store,
     item
where d1.d_moy = 4
  and d1.d_year = 1999
  and d1.d_date_sk = ss_sold_date_sk
  and i_item_sk = ss_item_sk
  and s_store_sk = ss_store_sk
  and ss_customer_sk = sr_customer_sk
  and ss_item_sk = sr_item_sk
  and ss_ticket_number = sr_ticket_number
  and sr_returned_date_sk = d2.d_date_sk
  and d2.d_moy between 4 and 10
  and d2.d_year = 1999
  and sr_customer_sk = cs_bill_customer_sk
  and sr_item_sk = cs_item_sk
  and cs_sold_date_sk = d3.d_date_sk
  and d3.d_moy between 4 and 10
  and d3.d_year = 1999
group by i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
order by i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
limit 100;

-- =================================================================
-- Query ID: TPCDSN431
-- =================================================================
select i_item_id,
       avg(cs_quantity) agg1,
       avg(cs_list_price) agg2,
       avg(cs_coupon_amt) agg3,
       avg(cs_sales_price) agg4
from catalog_sales,
     customer_demographics,
     date_dim,
     item,
     promotion
where cs_sold_date_sk = d_date_sk
  and cs_item_sk = i_item_sk
  and cs_bill_cdemo_sk = cd_demo_sk
  and cs_promo_sk = p_promo_sk
  and cd_gender = 'M'
  and cd_marital_status = 'W'
  and cd_education_status = 'Unknown'
  and (p_channel_email = 'N'
       or p_channel_event = 'N')
  and d_year = 2002
group by i_item_id
order by i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN432
-- =================================================================
select i_item_id,
       s_state,
       grouping(s_state) g_state,
       avg(ss_quantity) agg1,
       avg(ss_list_price) agg2,
       avg(ss_coupon_amt) agg3,
       avg(ss_sales_price) agg4
from store_sales,
     customer_demographics,
     date_dim,
     store,
     item
where ss_sold_date_sk = d_date_sk
  and ss_item_sk = i_item_sk
  and ss_store_sk = s_store_sk
  and ss_cdemo_sk = cd_demo_sk
  and cd_gender = 'M'
  and cd_marital_status = 'W'
  and cd_education_status = 'Secondary'
  and d_year = 1999
  and s_state = 'TN'
group by rollup (i_item_id,
                 s_state)
order by i_item_id,
         s_state
limit 100;

-- =================================================================
-- Query ID: TPCDSN433
-- =================================================================
select *
from
  (select avg(ss_list_price) B1_LP,
          count(ss_list_price) B1_CNT,
          count(distinct ss_list_price) B1_CNTD
   from store_sales
   where ss_quantity between 0 and 5
     and (ss_list_price between 107 and 117
          or ss_coupon_amt between 1319 and 2319
          or ss_wholesale_cost between 60 and 80) ) B1,

  (select avg(ss_list_price) B2_LP,
          count(ss_list_price) B2_CNT,
          count(distinct ss_list_price) B2_CNTD
   from store_sales
   where ss_quantity between 6 and 10
     and (ss_list_price between 23 and 33
          or ss_coupon_amt between 825 and 1825
          or ss_wholesale_cost between 43 and 63) ) B2,

  (select avg(ss_list_price) B3_LP,
          count(ss_list_price) B3_CNT,
          count(distinct ss_list_price) B3_CNTD
   from store_sales
   where ss_quantity between 11 and 15
     and (ss_list_price between 74 and 84
          or ss_coupon_amt between 4381 and 5381
          or ss_wholesale_cost between 57 and 77) ) B3,

  (select avg(ss_list_price) B4_LP,
          count(ss_list_price) B4_CNT,
          count(distinct ss_list_price) B4_CNTD
   from store_sales
   where ss_quantity between 16 and 20
     and (ss_list_price between 89 and 99
          or ss_coupon_amt between 3117 and 4117
          or ss_wholesale_cost between 68 and 88) ) B4,

  (select avg(ss_list_price) B5_LP,
          count(ss_list_price) B5_CNT,
          count(distinct ss_list_price) B5_CNTD
   from store_sales
   where ss_quantity between 21 and 25
     and (ss_list_price between 58 and 68
          or ss_coupon_amt between 9402 and 10402
          or ss_wholesale_cost between 38 and 58) ) B5,

  (select avg(ss_list_price) B6_LP,
          count(ss_list_price) B6_CNT,
          count(distinct ss_list_price) B6_CNTD
   from store_sales
   where ss_quantity between 26 and 30
     and (ss_list_price between 64 and 74
          or ss_coupon_amt between 5792 and 6792
          or ss_wholesale_cost between 73 and 93) ) B6
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN434
-- =================================================================
select i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       max(ss_quantity) as store_sales_quantity,
       max(sr_return_quantity) as store_returns_quantity,
       max(cs_quantity) as catalog_sales_quantity
from store_sales,
     store_returns,
     catalog_sales,
     date_dim d1,
     date_dim d2,
     date_dim d3,
     store,
     item
where d1.d_moy = 4
  and d1.d_year = 1998
  and d1.d_date_sk = ss_sold_date_sk
  and i_item_sk = ss_item_sk
  and s_store_sk = ss_store_sk
  and ss_customer_sk = sr_customer_sk
  and ss_item_sk = sr_item_sk
  and ss_ticket_number = sr_ticket_number
  and sr_returned_date_sk = d2.d_date_sk
  and d2.d_moy between 4 and 4 + 3
  and d2.d_year = 1998
  and sr_customer_sk = cs_bill_customer_sk
  and sr_item_sk = cs_item_sk
  and cs_sold_date_sk = d3.d_date_sk
  and d3.d_year in (1998,
                    1998 + 1,
                    1998 + 2)
group by i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
order by i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN435
-- =================================================================
WITH customer_total_return AS
  (SELECT wr_returning_customer_sk AS ctr_customer_sk,
          ca_state AS ctr_state,
          Sum(wr_return_amt) AS ctr_total_return
   FROM web_returns,
        date_dim,
        customer_address
   WHERE wr_returned_date_sk = d_date_sk
     AND d_year = 2000
     AND wr_returning_addr_sk = ca_address_sk
   GROUP BY wr_returning_customer_sk,
            ca_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       c_preferred_cust_flag,
       c_birth_day,
       c_birth_month,
       c_birth_year,
       c_birth_country,
       c_login,
       c_email_address,
       c_last_review_date,
       ctr_total_return
FROM customer_total_return ctr1,
     customer_address,
     customer
WHERE ctr1.ctr_total_return >
    (SELECT Avg(ctr_total_return) * 1.2
     FROM customer_total_return ctr2
     WHERE ctr1.ctr_state = ctr2.ctr_state)
  AND ca_address_sk = c_current_addr_sk
  AND ca_state = 'IN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         c_preferred_cust_flag,
         c_birth_day,
         c_birth_month,
         c_birth_year,
         c_birth_country,
         c_login,
         c_email_address,
         c_last_review_date,
         ctr_total_return
LIMIT 100;

-- =================================================================
-- Query ID: TPCDSN436
-- =================================================================
with ss as
  (select ca_county,
          d_qoy,
          d_year,
          sum(ss_ext_sales_price) as store_sales
   from store_sales,
        date_dim,
        customer_address
   where ss_sold_date_sk = d_date_sk
     and ss_addr_sk = ca_address_sk
   group by ca_county,
            d_qoy,
            d_year),
     ws as
  (select ca_county,
          d_qoy,
          d_year,
          sum(ws_ext_sales_price) as web_sales
   from web_sales,
        date_dim,
        customer_address
   where ws_sold_date_sk = d_date_sk
     and ws_bill_addr_sk = ca_address_sk
   group by ca_county,
            d_qoy,
            d_year)
select ss1.ca_county,
       ss1.d_year,
       ws2.web_sales / ws1.web_sales web_q1_q2_increase,
       ss2.store_sales / ss1.store_sales store_q1_q2_increase,
       ws3.web_sales / ws2.web_sales web_q2_q3_increase,
       ss3.store_sales / ss2.store_sales store_q2_q3_increase
from ss ss1,
     ss ss2,
     ss ss3,
     ws ws1,
     ws ws2,
     ws ws3
where ss1.d_qoy = 1
  and ss1.d_year = 1999
  and ss1.ca_county = ss2.ca_county
  and ss2.d_qoy = 2
  and ss2.d_year = 1999
  and ss2.ca_county = ss3.ca_county
  and ss3.d_qoy = 3
  and ss3.d_year = 1999
  and ss1.ca_county = ws1.ca_county
  and ws1.d_qoy = 1
  and ws1.d_year = 1999
  and ws1.ca_county = ws2.ca_county
  and ws2.d_qoy = 2
  and ws2.d_year = 1999
  and ws1.ca_county = ws3.ca_county
  and ws3.d_qoy = 3
  and ws3.d_year = 1999
  and case
          when ws1.web_sales > 0 then ws2.web_sales / ws1.web_sales
          else null
      end > case
                when ss1.store_sales > 0 then ss2.store_sales / ss1.store_sales
                else null
            end
  and case
          when ws2.web_sales > 0 then ws3.web_sales / ws2.web_sales
          else null
      end > case
                when ss2.store_sales > 0 then ss3.store_sales / ss2.store_sales
                else null
            end
order by store_q2_q3_increase;

-- =================================================================
-- Query ID: TPCDSN437
-- =================================================================
select sum(cs_ext_discount_amt) as "excess discount amount"
from catalog_sales,
     item,
     date_dim
where i_manufact_id = 722
  and i_item_sk = cs_item_sk
  and d_date between '2001-03-09' and (cast('2001-03-09' as date) + interval '90' day)
  and d_date_sk = cs_sold_date_sk
  and cs_ext_discount_amt >
    (select 1.3 * avg(cs_ext_discount_amt)
     from catalog_sales,
          date_dim
     where cs_item_sk = i_item_sk
       and d_date between '2001-03-09' and (cast('2001-03-09' as date) + interval '90' day)
       and d_date_sk = cs_sold_date_sk )
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN438
-- =================================================================
with ss as
  (select i_manufact_id,
          sum(ss_ext_sales_price) total_sales
   from store_sales,
        date_dim,
        customer_address,
        item
   where exists
       (select 1
        from item
        where i_manufact_id = item.i_manufact_id
          and i_category in ('Books') )
     and ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and d_year = 2001
     and d_moy = 3
     and ss_addr_sk = ca_address_sk
     and ca_gmt_offset = -5
   group by i_manufact_id),
     cs as
  (select i_manufact_id,
          sum(cs_ext_sales_price) total_sales
   from catalog_sales,
        date_dim,
        customer_address,
        item
   where exists
       (select 1
        from item
        where i_manufact_id = item.i_manufact_id
          and i_category in ('Books') )
     and cs_item_sk = i_item_sk
     and cs_sold_date_sk = d_date_sk
     and d_year = 2001
     and d_moy = 3
     and cs_bill_addr_sk = ca_address_sk
     and ca_gmt_offset = -5
   group by i_manufact_id),
     ws as
  (select i_manufact_id,
          sum(ws_ext_sales_price) total_sales
   from web_sales,
        date_dim,
        customer_address,
        item
   where exists
       (select 1
        from item
        where i_manufact_id = item.i_manufact_id
          and i_category in ('Books') )
     and ws_item_sk = i_item_sk
     and ws_sold_date_sk = d_date_sk
     and d_year = 2001
     and d_moy = 3
     and ws_bill_addr_sk = ca_address_sk
     and ca_gmt_offset = -5
   group by i_manufact_id)
select i_manufact_id,
       sum(total_sales) total_sales
from
  (select *
   from ss
   union all select *
   from cs
   union all select *
   from ws) tmp1
group by i_manufact_id
order by total_sales
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN439
-- =================================================================
select c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
from
  (select ss_ticket_number,
          ss_customer_sk,
          count(*) cnt
   from store_sales,
        date_dim,
        store,
        household_demographics
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_store_sk = store.s_store_sk
     and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     and (date_dim.d_dom between 1 and 3
          or date_dim.d_dom between 25 and 28)
     and (household_demographics.hd_buy_potential = '1001-5000'
          or household_demographics.hd_buy_potential = '0-500')
     and household_demographics.hd_vehicle_count > 0
     and (case
              when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
              else null
          end) > 1.2
     and date_dim.d_year in (2000,
                             2001,
                             2002)
     and store.s_county = 'Williamson County'
   group by ss_ticket_number,
            ss_customer_sk) dn,
     customer
where ss_customer_sk = c_customer_sk
  and cnt between 15 and 20
order by c_last_name,
         c_first_name,
         c_salutation,
         c_preferred_cust_flag desc,
         ss_ticket_number ;

-- =================================================================
-- Query ID: TPCDSN440
-- =================================================================
select ca_state,
       cd_gender,
       cd_marital_status,
       cd_dep_count,
       count(*) cnt1,
       avg(cd_dep_count),
       stddev_samp(cd_dep_count),
       sum(cd_dep_count),
       cd_dep_employed_count,
       count(*) cnt2,
       avg(cd_dep_employed_count),
       stddev_samp(cd_dep_employed_count),
       sum(cd_dep_employed_count),
       cd_dep_college_count,
       count(*) cnt3,
       avg(cd_dep_college_count),
       stddev_samp(cd_dep_college_count),
       sum(cd_dep_college_count)
from customer c,
     customer_address ca,
     customer_demographics
where c.c_current_addr_sk = ca.ca_address_sk
  and cd_demo_sk = c.c_current_cdemo_sk
  and c.c_customer_sk in
    (select ss_customer_sk
     from store_sales,
          date_dim
     where ss_sold_date_sk = d_date_sk
       and d_year = 1999
       and d_qoy < 4)
  and (c.c_customer_sk in
         (select ws_bill_customer_sk
          from web_sales,
               date_dim
          where ws_sold_date_sk = d_date_sk
            and d_year = 1999
            and d_qoy < 4)
       or c.c_customer_sk in
         (select cs_ship_customer_sk
          from catalog_sales,
               date_dim
          where cs_sold_date_sk = d_date_sk
            and d_year = 1999
            and d_qoy < 4))
group by ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
order by ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN441
-- =================================================================
select *
from customer;

-- =================================================================
-- Query ID: TPCDSN442
-- =================================================================
select i_item_id,
       i_item_desc,
       i_current_price
from item,
     inventory,
     date_dim,
     catalog_sales
where i_current_price between 29 and 29 + 30
  and inv_item_sk = i_item_sk
  and d_date_sk = inv_date_sk
  and d_date between cast('2002-03-29' as date) and (cast('2002-03-29' as date) + interval '60' day)
  and i_manufact_id in (393,
                        174,
                        251,
                        445)
  and inv_quantity_on_hand between 100 and 500
  and cs_item_sk = i_item_sk
group by i_item_id,
         i_item_desc,
         i_current_price
order by i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN443
-- =================================================================
select count(*)
from
  (select c_last_name,
          c_first_name,
          d_date
   from store_sales,
        date_dim,
        customer
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_customer_sk = customer.c_customer_sk
     and d_month_seq between 1189 and 1189 + 11 intersect
     select c_last_name,
            c_first_name,
            d_date
     from catalog_sales,
          date_dim,
          customer where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
     and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
     and d_month_seq between 1189 and 1189 + 11 intersect
     select c_last_name,
            c_first_name,
            d_date
     from web_sales,
          date_dim,
          customer where web_sales.ws_sold_date_sk = date_dim.d_date_sk
     and web_sales.ws_bill_customer_sk = customer.c_customer_sk
     and d_month_seq between 1189 and 1189 + 11 ) hot_cust
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN444
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN445
-- =================================================================
with inv as
  (select w_warehouse_name,
          w_warehouse_sk,
          i_item_sk,
          d_moy,
          stdev,
          mean,
          case mean
              when 0 then null
              else stdev / mean
          end cov
   from
     (select w_warehouse_name,
             w_warehouse_sk,
             i_item_sk,
             d_moy,
             stddev_samp(inv_quantity_on_hand) stdev,
             avg(inv_quantity_on_hand) mean
      from inventory,
           item,
           warehouse,
           date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year = 2000
      group by w_warehouse_name,
               w_warehouse_sk,
               i_item_sk,
               d_moy) foo
   where case mean
             when 0 then 0
             else stdev / mean
         end > 1)
select inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
from inv inv1,
     inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk = inv2.w_warehouse_sk
  and inv1.d_moy = 1
  and inv2.d_moy = 1 + 1
  and inv1.cov > 1.5
order by inv1.w_warehouse_sk,
         inv1.i_item_sk,
         inv1.d_moy,
         inv1.mean,
         inv1.cov,
         inv2.d_moy,
         inv2.mean,
         inv2.cov ;

-- =================================================================
-- Query ID: TPCDSN446
-- =================================================================
select w_state,
       i_item_id,
       sum(case
               when (d_date < '2001-05-02') then cs_sales_price - coalesce(cr_refunded_cash, 0)
               else 0
           end) as sales_before,
       sum(case
               when (d_date >= '2001-05-02') then cs_sales_price - coalesce(cr_refunded_cash, 0)
               else 0
           end) as sales_after
from catalog_sales
left outer join catalog_returns on (cs_order_number = cr_order_number
                                    and cs_item_sk = cr_item_sk), warehouse,
                                                                  item,
                                                                  date_dim
where i_current_price between 0.99 and 1.49
  and i_item_sk = cs_item_sk
  and cs_warehouse_sk = w_warehouse_sk
  and cs_sold_date_sk = d_date_sk
  and d_date between '2001-04-02' and '2001-06-01'
group by w_state,
         i_item_id
order by w_state,
         i_item_id
limit 100;

-- =================================================================
-- Query ID: TPCDSN447
-- =================================================================
select distinct(i_product_name)
from item i1
where i_manufact_id between 704 and 704 + 40
  and exists
    (select 1
     from item
     where i_manufact = i1.i_manufact
       and ((i_category = 'Women'
             and (i_color = 'forest'
                  or i_color = 'lime')
             and (i_units = 'Pallet'
                  or i_units = 'Pound')
             and (i_size = 'economy'
                  or i_size = 'small'))
            or (i_category = 'Women'
                and (i_color = 'navy'
                     or i_color = 'slate')
                and (i_units = 'Gross'
                     or i_units = 'Bunch')
                and (i_size = 'extra large'
                     or i_size = 'petite'))
            or (i_category = 'Men'
                and (i_color = 'powder'
                     or i_color = 'sky')
                and (i_units = 'Dozen'
                     or i_units = 'Lb')
                and (i_size = 'N/A'
                     or i_size = 'large'))
            or (i_category = 'Men'
                and (i_color = 'maroon'
                     or i_color = 'smoke')
                and (i_units = 'Ounce'
                     or i_units = 'Case')
                and (i_size = 'economy'
                     or i_size = 'small')))
       or (i_manufact = i1.i_manufact
           and ((i_category = 'Women'
                 and (i_color = 'dark'
                      or i_color = 'aquamarine')
                 and (i_units = 'Ton'
                      or i_units = 'Tbl')
                 and (i_size = 'economy'
                      or i_size = 'small'))
                or (i_category = 'Women'
                    and (i_color = 'frosted'
                         or i_color = 'plum')
                    and (i_units = 'Dram'
                         or i_units = 'Box')
                    and (i_size = 'extra large'
                         or i_size = 'petite'))
                or (i_category = 'Men'
                    and (i_color = 'papaya'
                         or i_color = 'peach')
                    and (i_units = 'Bundle'
                         or i_units = 'Carton')
                    and (i_size = 'N/A'
                         or i_size = 'large'))
                or (i_category = 'Men'
                    and (i_color = 'firebrick'
                         or i_color = 'sienna')
                    and (i_units = 'Cup'
                         or i_units = 'Each')
                    and (i_size = 'economy'
                         or i_size = 'small')))) )
order by i_product_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN448
-- =================================================================
select dt.d_year,
       item.i_category_id,
       item.i_category,
       sum(ss_ext_sales_price)
from date_dim dt,
     store_sales,
     item
where dt.d_date_sk = store_sales.ss_sold_date_sk
  and store_sales.ss_item_sk = item.i_item_sk
  and item.i_manager_id = 1
  and dt.d_moy = 11
  and dt.d_year = 1998
group by dt.d_year,
         item.i_category_id,
         item.i_category
order by sum(ss_ext_sales_price) desc,dt.d_year,
                                      item.i_category_id,
                                      item.i_category
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN449
-- =================================================================
select s_store_name,
       s_store_id,
       sum(case
               when (d_day_name = 'Sunday') then ss_sales_price
               else null
           end) sun_sales,
       sum(case
               when (d_day_name = 'Monday') then ss_sales_price
               else null
           end) mon_sales,
       sum(case
               when (d_day_name = 'Tuesday') then ss_sales_price
               else null
           end) tue_sales,
       sum(case
               when (d_day_name = 'Wednesday') then ss_sales_price
               else null
           end) wed_sales,
       sum(case
               when (d_day_name = 'Thursday') then ss_sales_price
               else null
           end) thu_sales,
       sum(case
               when (d_day_name = 'Friday') then ss_sales_price
               else null
           end) fri_sales,
       sum(case
               when (d_day_name = 'Saturday') then ss_sales_price
               else null
           end) sat_sales
from date_dim,
     store_sales,
     store
where d_date_sk = ss_sold_date_sk
  and s_store_sk = ss_store_sk
  and s_gmt_offset = -5
  and d_year = 2000
group by s_store_name,
         s_store_id
order by s_store_name,
         s_store_id,
         sun_sales,
         mon_sales,
         tue_sales,
         wed_sales,
         thu_sales,
         fri_sales,
         sat_sales
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN450
-- =================================================================
select asceding.rnk,
       i1.i_product_name best_performing,
       i2.i_product_name worst_performing
from
  (select *
   from
     (select item_sk,
             rank() over (
                          order by rank_col asc) rnk
      from
        (select ss_item_sk item_sk,
                avg(ss_net_profit) rank_col
         from store_sales ss1
         where ss_store_sk = 4
         group by ss_item_sk
         having avg(ss_net_profit) > 0.9 *
           (select avg(ss_net_profit) rank_col
            from store_sales
            where ss_store_sk = 4
              and ss_hdemo_sk is not null
            group by ss_store_sk)) V1) V11
   where rnk < 11 ) asceding,

  (select *
   from
     (select item_sk,
             rank() over (
                          order by rank_col desc) rnk
      from
        (select ss_item_sk item_sk,
                avg(ss_net_profit) rank_col
         from store_sales ss1
         where ss_store_sk = 4
         group by ss_item_sk
         having avg(ss_net_profit) > 0.9 *
           (select avg(ss_net_profit) rank_col
            from store_sales
            where ss_store_sk = 4
              and ss_hdemo_sk is not null
            group by ss_store_sk)) V2) V21
   where rnk < 11 ) descending,
     item i1,
     item i2
where asceding.rnk = descending.rnk
  and i1.i_item_sk = asceding.item_sk
  and i2.i_item_sk = descending.item_sk
order by asceding.rnk
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN451
-- =================================================================
select ca_zip,
       ca_city,
       sum(ws_sales_price)
from web_sales,
     customer,
     customer_address,
     date_dim,
     item
where ws_bill_customer_sk = c_customer_sk
  and c_current_addr_sk = ca_address_sk
  and ws_item_sk = i_item_sk
  and (substr(ca_zip, 1, 5) in ('85669',
                                '86197',
                                '88274',
                                '83405',
                                '86475',
                                '85392',
                                '85460',
                                '80348',
                                '81792')
       or exists
         (select 1
          from item
          where item.i_item_id = i_item_id
            and i_item_sk in (2,
                              3,
                              5,
                              7,
                              11,
                              13,
                              17,
                              19,
                              23,
                              29)))
  and ws_sold_date_sk = d_date_sk
  and d_qoy = 1
  and d_year = 2000
group by ca_zip,
         ca_city
order by ca_zip,
         ca_city
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN452
-- =================================================================
select c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       amt,
       profit
from
  (select ss_ticket_number,
          ss_customer_sk,
          ca_city bought_city,
          sum(ss_coupon_amt) amt,
          sum(ss_net_profit) profit
   from store_sales,
        date_dim,
        store,
        household_demographics,
        customer_address
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_store_sk = store.s_store_sk
     and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     and store_sales.ss_addr_sk = customer_address.ca_address_sk
     and (household_demographics.hd_dep_count = 8
          or household_demographics.hd_vehicle_count = 0)
     and exists
       (select 1
        from date_dim dd
        where dd.d_date_sk = date_dim.d_date_sk
          and dd.d_dow in (4,
                           0)
          and dd.d_year in (2000,
                            2000 + 1,
                            2000 + 2) )
     and store.s_city in ('Midway',
                          'Fairview',
                          'Fairview',
                          'Midway',
                          'Fairview')
   group by ss_ticket_number,
            ss_customer_sk,
            ss_addr_sk,
            ca_city) dn,
     customer,
     customer_address current_addr
where ss_customer_sk = c_customer_sk
  and customer.c_current_addr_sk = current_addr.ca_address_sk
  and current_addr.ca_city <> bought_city
order by c_last_name,
         c_first_name,
         ca_city,
         bought_city,
         ss_ticket_number
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN453
-- =================================================================
with v1 as
  (select i_category,
          i_brand,
          s_store_name,
          s_company_name,
          d_year,
          d_moy,
          sum(ss_sales_price) sum_sales,
          avg(sum(ss_sales_price)) over (partition by i_category,
                                                      i_brand,
                                                      s_store_name,
                                                      s_company_name,
                                                      d_year) avg_monthly_sales,
                                        rank() over (partition by i_category,
                                                                  i_brand,
                                                                  s_store_name,
                                                                  s_company_name
                                                     order by d_year,
                                                              d_moy) rn
   from item,
        store_sales,
        date_dim,
        store
   where ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and ss_store_sk = s_store_sk
     and (d_year = 2000
          or (d_year = 1999
              and d_moy = 12)
          or (d_year = 2001
              and d_moy = 1))
   group by i_category,
            i_brand,
            s_store_name,
            s_company_name,
            d_year,
            d_moy),
     v2 as
  (select v1.s_store_name,
          v1.s_company_name,
          v1.d_year,
          v1.avg_monthly_sales,
          v1.sum_sales,
          v1_lag.sum_sales psum,
          v1_lead.sum_sales nsum
   from v1
   join v1 v1_lag on v1.i_category = v1_lag.i_category
   and v1.i_brand = v1_lag.i_brand
   and v1.s_store_name = v1_lag.s_store_name
   and v1.s_company_name = v1_lag.s_company_name
   and v1.rn = v1_lag.rn + 1
   join v1 v1_lead on v1.i_category = v1_lead.i_category
   and v1.i_brand = v1_lead.i_brand
   and v1.s_store_name = v1_lead.s_store_name
   and v1.s_company_name = v1_lead.s_company_name
   and v1.rn = v1_lead.rn - 1)
select *
from v2
where d_year = 2000
  and avg_monthly_sales > 0
  and case
          when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
          else null
      end > 0.1
order by sum_sales - avg_monthly_sales,
         nsum
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN454
-- =================================================================
select sum (ss_quantity)
from store_sales,
     store,
     customer_demographics,
     customer_address,
     date_dim
where s_store_sk = ss_store_sk
  and ss_sold_date_sk = d_date_sk
  and d_year = 2001
  and ((cd_demo_sk = ss_cdemo_sk
        and cd_marital_status = 'S'
        and cd_education_status = 'Secondary'
        and ss_sales_price between 100.00 and 150.00)
       or (cd_demo_sk = ss_cdemo_sk
           and cd_marital_status = 'M'
           and cd_education_status = '2 yr Degree'
           and ss_sales_price between 50.00 and 100.00)
       or (cd_demo_sk = ss_cdemo_sk
           and cd_marital_status = 'D'
           and cd_education_status = 'Advanced Degree'
           and ss_sales_price between 150.00 and 200.00))
  and ((ss_addr_sk = ca_address_sk
        and ca_country = 'United States'
        and ca_state in ('ND',
                         'NY',
                         'SD')
        and ss_net_profit between 0 and 2000)
       or (ss_addr_sk = ca_address_sk
           and ca_country = 'United States'
           and ca_state in ('MD',
                            'GA',
                            'KS')
           and ss_net_profit between 150 and 3000)
       or (ss_addr_sk = ca_address_sk
           and ca_country in ('CO',
                              'MN',
                              'NC')
           and ss_net_profit between 50 and 25000)) ;

-- =================================================================
-- Query ID: TPCDSN455
-- =================================================================
select channel,
       item,
       return_ratio,
       return_rank,
       currency_rank
from
  (select 'web' as channel,
          web.item,
          web.return_ratio,
          web.return_rank,
          web.currency_rank
   from
     (select item,
             return_ratio,
             currency_ratio,
             rank() over (
                          order by return_ratio) as return_rank,
                         rank() over (
                                      order by currency_ratio) as currency_rank
      from
        (select ws.ws_item_sk as item,
                (cast(sum(coalesce(wr.wr_return_quantity, 0)) as decimal(15, 4)) / cast(sum(coalesce(ws.ws_quantity, 0)) as decimal(15, 4))) as return_ratio,
                (cast(sum(coalesce(wr.wr_return_amt, 0)) as decimal(15, 4)) / cast(sum(coalesce(ws.ws_net_paid, 0)) as decimal(15, 4))) as currency_ratio
         from web_sales ws
         left outer join web_returns wr on (ws.ws_order_number = wr.wr_order_number
                                            and ws.ws_item_sk = wr.wr_item_sk), date_dim
         where wr.wr_return_amt > 10000
           and ws.ws_net_profit > 1
           and ws.ws_net_paid > 0
           and ws.ws_quantity > 0
           and ws_sold_date_sk = d_date_sk
           and d_year = 1998
           and d_moy = 11
         group by ws.ws_item_sk) in_web) web
   where (web.return_rank <= 10
          or web.currency_rank <= 10)
   union select 'catalog' as channel,
                catalog.item,
                catalog.return_ratio,
                catalog.return_rank,
                catalog.currency_rank
   from
     (select item,
             return_ratio,
             currency_ratio,
             rank() over (
                          order by return_ratio) as return_rank,
                         rank() over (
                                      order by currency_ratio) as currency_rank
      from
        (select cs.cs_item_sk as item,
                (cast(sum(coalesce(cr.cr_return_quantity, 0)) as decimal(15, 4)) / cast(sum(coalesce(cs.cs_quantity, 0)) as decimal(15, 4))) as return_ratio,
                (cast(sum(coalesce(cr.cr_return_amount, 0)) as decimal(15, 4)) / cast(sum(coalesce(cs.cs_net_paid, 0)) as decimal(15, 4))) as currency_ratio
         from catalog_sales cs
         left outer join catalog_returns cr on (cs.cs_order_number = cr.cr_order_number
                                                and cs.cs_item_sk = cr.cr_item_sk), date_dim
         where cr.cr_return_amount > 10000
           and cs.cs_net_profit > 1
           and cs.cs_net_paid > 0
           and cs.cs_quantity > 0
           and cs_sold_date_sk = d_date_sk
           and d_year = 1998
           and d_moy = 11
         group by cs.cs_item_sk) in_cat) catalog
   where (catalog.return_rank <= 10
          or catalog.currency_rank <= 10)
   union select 'store' as channel,
                store.item,
                store.return_ratio,
                store.return_rank,
                store.currency_rank
   from
     (select item,
             return_ratio,
             currency_ratio,
             rank() over (
                          order by return_ratio) as return_rank,
                         rank() over (
                                      order by currency_ratio) as currency_rank
      from
        (select sts.ss_item_sk as item,
                (cast(sum(coalesce(sr.sr_return_quantity, 0)) as decimal(15, 4)) / cast(sum(coalesce(sts.ss_quantity, 0)) as decimal(15, 4))) as return_ratio,
                (cast(sum(coalesce(sr.sr_return_amt, 0)) as decimal(15, 4)) / cast(sum(coalesce(sts.ss_net_paid, 0)) as decimal(15, 4))) as currency_ratio
         from store_sales sts
         left outer join store_returns sr on (sts.ss_ticket_number = sr.sr_ticket_number
                                              and sts.ss_item_sk = sr.sr_item_sk), date_dim
         where sr.sr_return_amt > 10000
           and sts.ss_net_profit > 1
           and sts.ss_net_paid > 0
           and sts.ss_quantity > 0
           and ss_sold_date_sk = d_date_sk
           and d_year = 1998
           and d_moy = 11
         group by sts.ss_item_sk) in_store) store
   where (store.return_rank <= 10
          or store.currency_rank <= 10) ) as tmp
order by 1,
         4,
         5,
         2
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN456
-- =================================================================
select s_store_name,
       s_company_id,
       s_street_number,
       s_street_name,
       s_street_type,
       s_suite_number,
       s_city,
       s_county,
       s_state,
       s_zip,
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk <= 30) then 1
               else 0
           end) as "30 days",
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk > 30)
                    and (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1
               else 0
           end) as "31-60 days",
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk > 60)
                    and (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1
               else 0
           end) as "61-90 days",
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk > 90)
                    and (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1
               else 0
           end) as "91-120 days",
       sum(case
               when (sr_returned_date_sk - ss_sold_date_sk > 120) then 1
               else 0
           end) as ">120 days"
from store_sales
join store_returns on ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and ss_customer_sk = sr_customer_sk
join store on ss_store_sk = s_store_sk
join date_dim d1 on ss_sold_date_sk = d1.d_date_sk
join date_dim d2 on sr_returned_date_sk = d2.d_date_sk
where d2.d_year = 2001
  and d2.d_moy = 8
group by s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
order by s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN457
-- =================================================================
WITH web_v1 as
  (select ws_item_sk item_sk,
          d_date,
          sum(sum(ws_sales_price)) over (partition by ws_item_sk
                                         order by d_date rows between unbounded preceding and current row) cume_sales
   from web_sales,
        date_dim
   where ws_sold_date_sk = d_date_sk
     and d_month_seq between 1212 and 1223
     and ws_item_sk is not NULL
   group by ws_item_sk,
            d_date),
     store_v1 as
  (select ss_item_sk item_sk,
          d_date,
          sum(sum(ss_sales_price)) over (partition by ss_item_sk
                                         order by d_date rows between unbounded preceding and current row) cume_sales
   from store_sales,
        date_dim
   where ss_sold_date_sk = d_date_sk
     and d_month_seq between 1212 and 1223
     and ss_item_sk is not NULL
   group by ss_item_sk,
            d_date)
select *
from
  (select item_sk,
          d_date,
          web_sales,
          store_sales,
          max(web_sales) over (partition by item_sk
                               order by d_date rows between unbounded preceding and current row) web_cumulative,
                              max(store_sales) over (partition by item_sk
                                                     order by d_date rows between unbounded preceding and current row) store_cumulative
   from
     (select case
                 when web.item_sk is not null then web.item_sk
                 else store.item_sk
             end item_sk,
             case
                 when web.d_date is not null then web.d_date
                 else store.d_date
             end d_date,
             web.cume_sales web_sales,
             store.cume_sales store_sales
      from web_v1 web
      full outer join store_v1 store on (web.item_sk = store.item_sk
                                         and web.d_date = store.d_date)) x) y
where web_cumulative > store_cumulative
order by item_sk,
         d_date
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN458
-- =================================================================
select dt.d_year,
       item.i_brand_id brand_id,
       item.i_brand brand,
       sum(ss_ext_sales_price) ext_price
from date_dim dt
join store_sales on dt.d_date_sk = store_sales.ss_sold_date_sk
join item on store_sales.ss_item_sk = item.i_item_sk
where item.i_manager_id = 1
  and dt.d_moy = 12
  and dt.d_year = 2000
group by dt.d_year,
         item.i_brand,
         item.i_brand_id
order by dt.d_year,
         ext_price desc,
         brand_id
limit 100;

-- =================================================================
-- Query ID: TPCDSN459
-- =================================================================
select *
from
  (select i_manufact_id,
          sum(ss_sales_price) sum_sales,
          avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
   from item,
        store_sales,
        date_dim,
        store
   where ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and ss_store_sk = s_store_sk
     and exists
       (select 1
        from date_dim dd
        where dd.d_month_seq = d_month_seq
          and d_month_seq in (1186,
                              1186 + 1,
                              1186 + 2,
                              1186 + 3,
                              1186 + 4,
                              1186 + 5,
                              1186 + 6,
                              1186 + 7,
                              1186 + 8,
                              1186 + 9,
                              1186 + 10,
                              1186 + 11) )
     and ((i_category in ('Books',
                          'Children',
                          'Electronics')
           and i_class in ('personal',
                           'portable',
                           'reference',
                           'self-help')
           and i_brand in ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9'))
          or (i_category in ('Women',
                             'Music',
                             'Men')
              and i_class in ('accessories',
                              'classical',
                              'fragrances',
                              'pants')
              and i_brand in ('amalgimporto #1',
                              'edu packscholar #1',
                              'exportiimporto #1',
                              'importoamalg #1')))
   group by i_manufact_id,
            d_qoy) tmp1
where case
          when avg_quarterly_sales > 0 then abs(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
          else null
      end > 0.1
order by avg_quarterly_sales,
         sum_sales,
         i_manufact_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN460
-- =================================================================
with my_customers as
  (select distinct c_customer_sk,
                   c_current_addr_sk
   from
     (select cs_sold_date_sk sold_date_sk,
             cs_bill_customer_sk customer_sk,
             cs_item_sk item_sk
      from catalog_sales
      union all select ws_sold_date_sk sold_date_sk,
                       ws_bill_customer_sk customer_sk,
                       ws_item_sk item_sk
      from web_sales) cs_or_ws_sales,
        item,
        date_dim,
        customer
   where sold_date_sk = d_date_sk
     and item_sk = i_item_sk
     and i_category = 'Music'
     and i_class = 'country'
     and c_customer_sk = cs_or_ws_sales.customer_sk
     and d_moy = 1
     and d_year = 1999 ),
     my_revenue as
  (select c_customer_sk,
          sum(ss_ext_sales_price) as revenue
   from my_customers,
        store_sales,
        customer_address,
        store,
        date_dim
   where c_current_addr_sk = ca_address_sk
     and ca_county = s_county
     and ca_state = s_state
     and ss_sold_date_sk = d_date_sk
     and c_customer_sk = ss_customer_sk
     and d_month_seq between
       (select distinct d_month_seq + 1
        from date_dim
        where d_year = 1999
          and d_moy = 1) and
       (select distinct d_month_seq + 3
        from date_dim
        where d_year = 1999
          and d_moy = 1)
   group by c_customer_sk),
     segments as
  (select cast((revenue / 50) as int) as segment
   from my_revenue)
select segment,
       count(*) as num_customers,
       segment*50 as segment_base
from segments
group by segment
order by segment,
         num_customers
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN461
-- =================================================================
select i_brand_id brand_id,
       i_brand brand,
       sum(ss_ext_sales_price) ext_price
from item
join store_sales on ss_item_sk = i_item_sk
join date_dim on d_date_sk = ss_sold_date_sk
where i_manager_id = 52
  and d_moy = 11
  and d_year = 2000
group by i_brand,
         i_brand_id
order by ext_price desc,
         i_brand_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN462
-- =================================================================
with ss as
  (select i_item_id,
          sum(ss_ext_sales_price) total_sales
   from store_sales,
        date_dim,
        customer_address,
        item
   where exists
       (select 1
        from item
        where i_item_id = item.i_item_id
          and i_color in ('powder',
                          'orchid',
                          'pink'))
     and ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and d_year = 2000
     and d_moy = 3
     and ss_addr_sk = ca_address_sk
     and ca_gmt_offset = -6
   group by i_item_id),
     cs as
  (select i_item_id,
          sum(cs_ext_sales_price) total_sales
   from catalog_sales,
        date_dim,
        customer_address,
        item
   where exists
       (select 1
        from item
        where i_item_id = item.i_item_id
          and i_color in ('powder',
                          'orchid',
                          'pink'))
     and cs_item_sk = i_item_sk
     and cs_sold_date_sk = d_date_sk
     and d_year = 2000
     and d_moy = 3
     and cs_bill_addr_sk = ca_address_sk
     and ca_gmt_offset = -6
   group by i_item_id),
     ws as
  (select i_item_id,
          sum(ws_ext_sales_price) total_sales
   from web_sales,
        date_dim,
        customer_address,
        item
   where exists
       (select 1
        from item
        where i_item_id = item.i_item_id
          and i_color in ('powder',
                          'orchid',
                          'pink'))
     and ws_item_sk = i_item_sk
     and ws_sold_date_sk = d_date_sk
     and d_year = 2000
     and d_moy = 3
     and ws_bill_addr_sk = ca_address_sk
     and ca_gmt_offset = -6
   group by i_item_id)
select i_item_id,
       sum(total_sales) total_sales
from
  (select *
   from ss
   union all select *
   from cs
   union all select *
   from ws) tmp1
group by i_item_id
order by total_sales,
         i_item_id
limit 100;

-- =================================================================
-- Query ID: TPCDSN463
-- =================================================================
with v1 as
  (select i_category,
          i_brand,
          cc_name,
          d_year,
          d_moy,
          sum(cs_sales_price) sum_sales,
          avg(sum(cs_sales_price)) over (partition by i_category,
                                                      i_brand,
                                                      cc_name,
                                                      d_year) avg_monthly_sales,
                                        rank() over (partition by i_category,
                                                                  i_brand,
                                                                  cc_name
                                                     order by d_year,
                                                              d_moy) rn
   from item,
        catalog_sales,
        date_dim,
        call_center
   where cs_item_sk = i_item_sk
     and cs_sold_date_sk = d_date_sk
     and cc_call_center_sk = cs_call_center_sk
     and (d_year = 2001
          or (d_year = 2000
              and d_moy = 12)
          or (d_year = 2002
              and d_moy = 1))
   group by i_category,
            i_brand,
            cc_name,
            d_year,
            d_moy),
     v2 as
  (select v1.i_category,
          v1.i_brand,
          v1.cc_name,
          v1.d_year,
          v1.avg_monthly_sales,
          v1.sum_sales,
          v1_lag.sum_sales psum,
          v1_lead.sum_sales nsum
   from v1
   join v1 v1_lag on v1.i_category = v1_lag.i_category
   and v1.i_brand = v1_lag.i_brand
   and v1.cc_name = v1_lag.cc_name
   and v1.rn = v1_lag.rn + 1
   join v1 v1_lead on v1.i_category = v1_lead.i_category
   and v1.i_brand = v1_lead.i_brand
   and v1.cc_name = v1_lead.cc_name
   and v1.rn = v1_lead.rn - 1)
select *
from v2
where d_year = 2001
  and avg_monthly_sales > 0
  and case
          when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
          else null
      end > 0.1
order by sum_sales - avg_monthly_sales,
         avg_monthly_sales
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN464
-- =================================================================
with ss_items as
  (select i_item_id item_id,
          sum(ss_ext_sales_price) ss_item_rev
   from store_sales,
        item,
        date_dim
   where ss_item_sk = i_item_sk
     and exists
       (select 1
        from date_dim
        where d_week_seq =
            (select d_week_seq
             from date_dim
             where d_date = '1998-11-19')
          and d_date = date_dim.d_date)
     and ss_sold_date_sk = d_date_sk
   group by i_item_id),
     cs_items as
  (select i_item_id item_id,
          sum(cs_ext_sales_price) cs_item_rev
   from catalog_sales,
        item,
        date_dim
   where cs_item_sk = i_item_sk
     and exists
       (select 1
        from date_dim
        where d_week_seq =
            (select d_week_seq
             from date_dim
             where d_date = '1998-11-19')
          and d_date = date_dim.d_date)
     and cs_sold_date_sk = d_date_sk
   group by i_item_id),
     ws_items as
  (select i_item_id item_id,
          sum(ws_ext_sales_price) ws_item_rev
   from web_sales,
        item,
        date_dim
   where ws_item_sk = i_item_sk
     and exists
       (select 1
        from date_dim
        where d_week_seq =
            (select d_week_seq
             from date_dim
             where d_date = '1998-11-19')
          and d_date = date_dim.d_date)
     and ws_sold_date_sk = d_date_sk
   group by i_item_id)
select ss_items.item_id,
       ss_item_rev,
       ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 ss_dev,
       cs_item_rev,
       cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 cs_dev,
       ws_item_rev,
       ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 ws_dev,
       (ss_item_rev + cs_item_rev + ws_item_rev) / 3 average
from ss_items,
     cs_items,
     ws_items
where ss_items.item_id = cs_items.item_id
  and ss_items.item_id = ws_items.item_id
  and ss_item_rev between 0.1 * cs_item_rev and 2 * cs_item_rev
  and ss_item_rev between 0.1 * ws_item_rev and 2 * ws_item_rev
  and cs_item_rev between 0.1 * ss_item_rev and 2 * ss_item_rev
  and cs_item_rev between 0.1 * ws_item_rev and 2 * ws_item_rev
  and ws_item_rev between 0.1 * ss_item_rev and 2 * ss_item_rev
  and ws_item_rev between 0.1 * cs_item_rev and 2 * cs_item_rev
order by item_id,
         ss_item_rev
limit 100;

-- =================================================================
-- Query ID: TPCDSN465
-- =================================================================
with wss as
  (select d_week_seq,
          ss_store_sk,
          sum(case
                  when (d_day_name = 'Sunday') then ss_sales_price
                  else null
              end) sun_sales,
          sum(case
                  when (d_day_name = 'Monday') then ss_sales_price
                  else null
              end) mon_sales,
          sum(case
                  when (d_day_name = 'Tuesday') then ss_sales_price
                  else null
              end) tue_sales,
          sum(case
                  when (d_day_name = 'Wednesday') then ss_sales_price
                  else null
              end) wed_sales,
          sum(case
                  when (d_day_name = 'Thursday') then ss_sales_price
                  else null
              end) thu_sales,
          sum(case
                  when (d_day_name = 'Friday') then ss_sales_price
                  else null
              end) fri_sales,
          sum(case
                  when (d_day_name = 'Saturday') then ss_sales_price
                  else null
              end) sat_sales
   from store_sales,
        date_dim
   where d_date_sk = ss_sold_date_sk
   group by d_week_seq,
            ss_store_sk)
select s_store_name1,
       s_store_id1,
       d_week_seq1,
       sun_sales1 / sun_sales2,
       mon_sales1 / mon_sales2,
       tue_sales1 / tue_sales2,
       wed_sales1 / wed_sales2,
       thu_sales1 / thu_sales2,
       fri_sales1 / fri_sales2,
       sat_sales1 / sat_sales2
from
  (select s_store_name s_store_name1,
          wss.d_week_seq d_week_seq1,
          s_store_id s_store_id1,
          sun_sales sun_sales1,
          mon_sales mon_sales1,
          tue_sales tue_sales1,
          wed_sales wed_sales1,
          thu_sales thu_sales1,
          fri_sales fri_sales1,
          sat_sales sat_sales1
   from wss,
        store,
        date_dim d
   where d.d_week_seq = wss.d_week_seq
     and ss_store_sk = s_store_sk
     and d_month_seq between 1195 and 1195 + 11 ) y,

  (select s_store_name s_store_name2,
          wss.d_week_seq d_week_seq2,
          s_store_id s_store_id2,
          sun_sales sun_sales2,
          mon_sales mon_sales2,
          tue_sales tue_sales2,
          wed_sales wed_sales2,
          thu_sales thu_sales2,
          fri_sales fri_sales2,
          sat_sales sat_sales2
   from wss,
        store,
        date_dim d
   where d.d_week_seq = wss.d_week_seq
     and ss_store_sk = s_store_sk
     and d_month_seq between 1195 + 12 and 1195 + 23 ) x
where s_store_id1 = s_store_id2
  and d_week_seq1 = d_week_seq2 - 52
order by s_store_name1,
         s_store_id1,
         d_week_seq1
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN466
-- =================================================================
with ss as
  (select i_item_id,
          sum(ss_ext_sales_price) total_sales
   from store_sales,
        date_dim,
        customer_address,
        item
   where exists
       (select 1
        from item
        where i_item_id = item.i_item_id
          and i_category in ('Jewelry'))
     and ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and d_year = 2000
     and d_moy = 10
     and ss_addr_sk = ca_address_sk
     and ca_gmt_offset = -5
   group by i_item_id),
     cs as
  (select i_item_id,
          sum(cs_ext_sales_price) total_sales
   from catalog_sales,
        date_dim,
        customer_address,
        item
   where exists
       (select 1
        from item
        where i_item_id = item.i_item_id
          and i_category in ('Jewelry'))
     and cs_item_sk = i_item_sk
     and cs_sold_date_sk = d_date_sk
     and d_year = 2000
     and d_moy = 10
     and cs_bill_addr_sk = ca_address_sk
     and ca_gmt_offset = -5
   group by i_item_id),
     ws as
  (select i_item_id,
          sum(ws_ext_sales_price) total_sales
   from web_sales,
        date_dim,
        customer_address,
        item
   where exists
       (select 1
        from item
        where i_item_id = item.i_item_id
          and i_category in ('Jewelry'))
     and ws_item_sk = i_item_sk
     and ws_sold_date_sk = d_date_sk
     and d_year = 2000
     and d_moy = 10
     and ws_bill_addr_sk = ca_address_sk
     and ca_gmt_offset = -5
   group by i_item_id)
select i_item_id,
       sum(total_sales) total_sales
from
  (select *
   from ss
   union all select *
   from cs
   union all select *
   from ws) tmp1
group by i_item_id
order by i_item_id,
         total_sales
limit 100;

-- =================================================================
-- Query ID: TPCDSN467
-- =================================================================
select promotions,
       total,
       cast(promotions as decimal(15, 4)) / cast(total as decimal(15, 4)) * 100
from
  (select sum(ss_ext_sales_price) promotions
   from store_sales
   join store on ss_store_sk = s_store_sk
   join promotion on ss_promo_sk = p_promo_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   join customer on ss_customer_sk = c_customer_sk
   join customer_address on ca_address_sk = c_current_addr_sk
   join item on ss_item_sk = i_item_sk
   where ca_gmt_offset = -5
     and i_category = 'Home'
     and (p_channel_dmail = 'Y'
          or p_channel_email = 'Y'
          or p_channel_tv = 'Y')
     and s_gmt_offset = -5
     and d_year = 2000
     and d_moy = 12 ) promotional_sales,

  (select sum(ss_ext_sales_price) total
   from store_sales
   join store on ss_store_sk = s_store_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   join customer on ss_customer_sk = c_customer_sk
   join customer_address on ca_address_sk = c_current_addr_sk
   join item on ss_item_sk = i_item_sk
   where ca_gmt_offset = -5
     and i_category = 'Home'
     and s_gmt_offset = -5
     and d_year = 2000
     and d_moy = 12 ) all_sales
order by promotions,
         total
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN468
-- =================================================================
select substr(w_warehouse_name, 1, 20),
       sm_type,
       web_name,
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk <= 30) then 1
               else 0
           end) as "30 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 30)
                    and (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1
               else 0
           end) as "31-60 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 60)
                    and (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1
               else 0
           end) as "61-90 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 90)
                    and (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1
               else 0
           end) as "91-120 days",
       sum(case
               when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1
               else 0
           end) as ">120 days"
from web_sales,
     warehouse,
     ship_mode,
     web_site,
     date_dim
where d_month_seq between 1223 and 1223 + 11
  and ws_ship_date_sk = d_date_sk
  and ws_warehouse_sk = w_warehouse_sk
  and ws_ship_mode_sk = sm_ship_mode_sk
  and ws_web_site_sk = web_site_sk
group by substr(w_warehouse_name, 1, 20),
         sm_type,
         web_name
order by substr(w_warehouse_name, 1, 20),
         sm_type,
         web_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN469
-- =================================================================
select *
from
  (select i_manager_id,
          sum(ss_sales_price) sum_sales,
          avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
   from item,
        store_sales,
        date_dim,
        store
   where ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and ss_store_sk = s_store_sk
     and exists
       (select 1
        from date_dim d
        where d.d_month_seq = date_dim.d_month_seq
          and d_month_seq in (1222,
                              1222 + 1,
                              1222 + 2,
                              1222 + 3,
                              1222 + 4,
                              1222 + 5,
                              1222 + 6,
                              1222 + 7,
                              1222 + 8,
                              1222 + 9,
                              1222 + 10,
                              1222 + 11))
     and ((i_category in ('Books',
                          'Children',
                          'Electronics')
           and i_class in ('personal',
                           'portable',
                           'reference',
                           'self-help')
           and i_brand in ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9')) or(i_category in ('Women', 'Music', 'Men')
                                                       and i_class in ('accessories', 'classical', 'fragrances', 'pants')
                                                       and i_brand in ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')))
   group by i_manager_id,
            d_moy) tmp1
where case
          when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales
          else null
      end > 0.1
order by i_manager_id,
         avg_monthly_sales,
         sum_sales
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN470
-- =================================================================
with cs_ui as
  (select cs_item_sk,
          sum(cs_ext_list_price) as sale,
          sum(cr_refunded_cash + cr_reversed_charge + cr_store_credit) as refund
   from catalog_sales,
        catalog_returns
   where cs_item_sk = cr_item_sk
     and cs_order_number = cr_order_number
   group by cs_item_sk
   having sum(cs_ext_list_price) > 2 * sum(cr_refunded_cash + cr_reversed_charge + cr_store_credit)),
     cross_sales as
  (select i_product_name product_name,
          i_item_sk item_sk,
          s_store_name store_name,
          s_zip store_zip,
          ad1.ca_street_number b_street_number,
          ad1.ca_street_name b_street_name,
          ad1.ca_city b_city,
          ad1.ca_zip b_zip,
          ad2.ca_street_number c_street_number,
          ad2.ca_street_name c_street_name,
          ad2.ca_city c_city,
          ad2.ca_zip c_zip,
          d1.d_year as syear,
          d2.d_year as fsyear,
          d3.d_year s2year,
          count(*) cnt,
          sum(ss_wholesale_cost) s1,
          sum(ss_list_price) s2,
          sum(ss_coupon_amt) s3
   FROM store_sales,
        store_returns,
        cs_ui,
        date_dim d1,
        date_dim d2,
        date_dim d3,
        store,
        customer,
        customer_demographics cd1,
        customer_demographics cd2,
        promotion,
        household_demographics hd1,
        household_demographics hd2,
        customer_address ad1,
        customer_address ad2,
        income_band ib1,
        income_band ib2,
        item
   WHERE ss_store_sk = s_store_sk
     AND ss_sold_date_sk = d1.d_date_sk
     AND ss_customer_sk = c_customer_sk
     AND ss_cdemo_sk = cd1.cd_demo_sk
     AND ss_hdemo_sk = hd1.hd_demo_sk
     AND ss_addr_sk = ad1.ca_address_sk
     AND ss_item_sk = i_item_sk
     AND ss_item_sk = sr_item_sk
     AND ss_ticket_number = sr_ticket_number
     AND ss_item_sk = cs_ui.cs_item_sk
     AND c_current_cdemo_sk = cd2.cd_demo_sk
     AND c_current_hdemo_sk = hd2.hd_demo_sk
     AND c_current_addr_sk = ad2.ca_address_sk
     AND c_first_sales_date_sk = d2.d_date_sk
     AND c_first_shipto_date_sk = d3.d_date_sk
     AND ss_promo_sk = p_promo_sk
     AND hd1.hd_income_band_sk = ib1.ib_income_band_sk
     AND hd2.hd_income_band_sk = ib2.ib_income_band_sk
     AND cd1.cd_marital_status <> cd2.cd_marital_status
     AND i_color IN ('orange',
                     'lace',
                     'lawn',
                     'misty',
                     'blush',
                     'pink')
     AND i_current_price BETWEEN 48 AND 48 + 10
     AND i_current_price BETWEEN 48 + 1 AND 48 + 15
   group by i_product_name,
            i_item_sk,
            s_store_name,
            s_zip,
            ad1.ca_street_number,
            ad1.ca_street_name,
            ad1.ca_city,
            ad1.ca_zip,
            ad2.ca_street_number,
            ad2.ca_street_name,
            ad2.ca_city,
            ad2.ca_zip,
            d1.d_year,
            d2.d_year,
            d3.d_year)
select cs1.product_name,
       cs1.store_name,
       cs1.store_zip,
       cs1.b_street_number,
       cs1.b_street_name,
       cs1.b_city,
       cs1.b_zip,
       cs1.c_street_number,
       cs1.c_street_name,
       cs1.c_city,
       cs1.c_zip,
       cs1.syear,
       cs1.cnt,
       cs1.s1 as s11,
       cs1.s2 as s21,
       cs1.s3 as s31,
       cs2.s1 as s12,
       cs2.s2 as s22,
       cs2.s3 as s32,
       cs2.syear,
       cs2.cnt
from cross_sales cs1,
     cross_sales cs2
where cs1.item_sk = cs2.item_sk
  and cs1.syear = 1999
  and cs2.syear = 1999 + 1
  and cs2.cnt <= cs1.cnt
  and cs1.store_name = cs2.store_name
  and cs1.store_zip = cs2.store_zip
order by cs1.product_name,
         cs1.store_name,
         cs2.cnt,
         cs1.s1,
         cs2.s1 ;

-- =================================================================
-- Query ID: TPCDSN471
-- =================================================================
select s_store_name,
       i_item_desc,
       sc.revenue,
       i_current_price,
       i_wholesale_cost,
       i_brand
from store,
     item,

  (select ss_store_sk,
          avg(revenue) as ave
   from
     (select ss_store_sk,
             ss_item_sk,
             sum(ss_sales_price) as revenue
      from store_sales,
           date_dim
      where ss_sold_date_sk = d_date_sk
        and d_month_seq between 1176 and 1176 + 11
      group by ss_store_sk,
               ss_item_sk) sa
   group by ss_store_sk) sb,

  (select ss_store_sk,
          ss_item_sk,
          sum(ss_sales_price) as revenue
   from store_sales,
        date_dim
   where ss_sold_date_sk = d_date_sk
     and d_month_seq between 1176 and 1176 + 11
   group by ss_store_sk,
            ss_item_sk) sc
where sb.ss_store_sk = sc.ss_store_sk
  and sc.revenue <= 0.1 * sb.ave
  and s_store_sk = sc.ss_store_sk
  and i_item_sk = sc.ss_item_sk
order by s_store_name,
         i_item_desc
limit 100;

-- =================================================================
-- Query ID: TPCDSN472
-- =================================================================
select w_warehouse_name,
       w_warehouse_sq_ft,
       w_city,
       w_county,
       w_state,
       w_country,
       ship_carriers,
       year,
       sum(jan_sales) as jan_sales,
       sum(feb_sales) as feb_sales,
       sum(mar_sales) as mar_sales,
       sum(apr_sales) as apr_sales,
       sum(may_sales) as may_sales,
       sum(jun_sales) as jun_sales,
       sum(jul_sales) as jul_sales,
       sum(aug_sales) as aug_sales,
       sum(sep_sales) as sep_sales,
       sum(oct_sales) as oct_sales,
       sum(nov_sales) as nov_sales,
       sum(dec_sales) as dec_sales,
       sum(jan_sales / w_warehouse_sq_ft) as jan_sales_per_sq_foot,
       sum(feb_sales / w_warehouse_sq_ft) as feb_sales_per_sq_foot,
       sum(mar_sales / w_warehouse_sq_ft) as mar_sales_per_sq_foot,
       sum(apr_sales / w_warehouse_sq_ft) as apr_sales_per_sq_foot,
       sum(may_sales / w_warehouse_sq_ft) as may_sales_per_sq_foot,
       sum(jun_sales / w_warehouse_sq_ft) as jun_sales_per_sq_foot,
       sum(jul_sales / w_warehouse_sq_ft) as jul_sales_per_sq_foot,
       sum(aug_sales / w_warehouse_sq_ft) as aug_sales_per_sq_foot,
       sum(sep_sales / w_warehouse_sq_ft) as sep_sales_per_sq_foot,
       sum(oct_sales / w_warehouse_sq_ft) as oct_sales_per_sq_foot,
       sum(nov_sales / w_warehouse_sq_ft) as nov_sales_per_sq_foot,
       sum(dec_sales / w_warehouse_sq_ft) as dec_sales_per_sq_foot,
       sum(jan_net) as jan_net,
       sum(feb_net) as feb_net,
       sum(mar_net) as mar_net,
       sum(apr_net) as apr_net,
       sum(may_net) as may_net,
       sum(jun_net) as jun_net,
       sum(jul_net) as jul_net,
       sum(aug_net) as aug_net,
       sum(sep_net) as sep_net,
       sum(oct_net) as oct_net,
       sum(nov_net) as nov_net,
       sum(dec_net) as dec_net
from
  (select w_warehouse_name,
          w_warehouse_sq_ft,
          w_city,
          w_county,
          w_state,
          w_country,
          'ORIENTAL' || ',' || 'BOXBUNDLES' as ship_carriers,
          d_year as year,
          sum(case
                  when d_moy = 1 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as jan_sales,
          sum(case
                  when d_moy = 2 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as feb_sales,
          sum(case
                  when d_moy = 3 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as mar_sales,
          sum(case
                  when d_moy = 4 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as apr_sales,
          sum(case
                  when d_moy = 5 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as may_sales,
          sum(case
                  when d_moy = 6 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as jun_sales,
          sum(case
                  when d_moy = 7 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as jul_sales,
          sum(case
                  when d_moy = 8 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as aug_sales,
          sum(case
                  when d_moy = 9 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as sep_sales,
          sum(case
                  when d_moy = 10 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as oct_sales,
          sum(case
                  when d_moy = 11 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as nov_sales,
          sum(case
                  when d_moy = 12 then ws_ext_sales_price * ws_quantity
                  else 0
              end) as dec_sales,
          sum(case
                  when d_moy = 1 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as jan_net,
          sum(case
                  when d_moy = 2 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as feb_net,
          sum(case
                  when d_moy = 3 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as mar_net,
          sum(case
                  when d_moy = 4 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as apr_net,
          sum(case
                  when d_moy = 5 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as may_net,
          sum(case
                  when d_moy = 6 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as jun_net,
          sum(case
                  when d_moy = 7 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as jul_net,
          sum(case
                  when d_moy = 8 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as aug_net,
          sum(case
                  when d_moy = 9 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as sep_net,
          sum(case
                  when d_moy = 10 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as oct_net,
          sum(case
                  when d_moy = 11 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as nov_net,
          sum(case
                  when d_moy = 12 then ws_net_paid_inc_ship * ws_quantity
                  else 0
              end) as dec_net
   from web_sales,
        warehouse,
        date_dim,
        time_dim,
        ship_mode
   where ws_warehouse_sk = w_warehouse_sk
     and ws_sold_date_sk = d_date_sk
     and ws_sold_time_sk = t_time_sk
     and ws_ship_mode_sk = sm_ship_mode_sk
     and d_year = 2001
     and t_time between 42970 and 42970 + 28800
     and sm_carrier in ('ORIENTAL',
                        'BOXBUNDLES')
   group by w_warehouse_name,
            w_warehouse_sq_ft,
            w_city,
            w_county,
            w_state,
            w_country,
            d_year
   union all select w_warehouse_name,
                    w_warehouse_sq_ft,
                    w_city,
                    w_county,
                    w_state,
                    w_country,
                    'ORIENTAL' || ',' || 'BOXBUNDLES' as ship_carriers,
                    d_year as year,
                    sum(case
                            when d_moy = 1 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as jan_sales,
                    sum(case
                            when d_moy = 2 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as feb_sales,
                    sum(case
                            when d_moy = 3 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as mar_sales,
                    sum(case
                            when d_moy = 4 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as apr_sales,
                    sum(case
                            when d_moy = 5 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as may_sales,
                    sum(case
                            when d_moy = 6 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as jun_sales,
                    sum(case
                            when d_moy = 7 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as jul_sales,
                    sum(case
                            when d_moy = 8 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as aug_sales,
                    sum(case
                            when d_moy = 9 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as sep_sales,
                    sum(case
                            when d_moy = 10 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as oct_sales,
                    sum(case
                            when d_moy = 11 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as nov_sales,
                    sum(case
                            when d_moy = 12 then cs_ext_list_price * cs_quantity
                            else 0
                        end) as dec_sales,
                    sum(case
                            when d_moy = 1 then cs_net_paid * cs_quantity
                            else 0
                        end) as jan_net,
                    sum(case
                            when d_moy = 2 then cs_net_paid * cs_quantity
                            else 0
                        end) as feb_net,
                    sum(case
                            when d_moy = 3 then cs_net_paid * cs_quantity
                            else 0
                        end) as mar_net,
                    sum(case
                            when d_moy = 4 then cs_net_paid * cs_quantity
                            else 0
                        end) as apr_net,
                    sum(case
                            when d_moy = 5 then cs_net_paid * cs_quantity
                            else 0
                        end) as may_net,
                    sum(case
                            when d_moy = 6 then cs_net_paid * cs_quantity
                            else 0
                        end) as jun_net,
                    sum(case
                            when d_moy = 7 then cs_net_paid * cs_quantity
                            else 0
                        end) as jul_net,
                    sum(case
                            when d_moy = 8 then cs_net_paid * cs_quantity
                            else 0
                        end) as aug_net,
                    sum(case
                            when d_moy = 9 then cs_net_paid * cs_quantity
                            else 0
                        end) as sep_net,
                    sum(case
                            when d_moy = 10 then cs_net_paid * cs_quantity
                            else 0
                        end) as oct_net,
                    sum(case
                            when d_moy = 11 then cs_net_paid * cs_quantity
                            else 0
                        end) as nov_net,
                    sum(case
                            when d_moy = 12 then cs_net_paid * cs_quantity
                            else 0
                        end) as dec_net
   from catalog_sales,
        warehouse,
        date_dim,
        time_dim,
        ship_mode
   where cs_warehouse_sk = w_warehouse_sk
     and cs_sold_date_sk = d_date_sk
     and cs_sold_time_sk = t_time_sk
     and cs_ship_mode_sk = sm_ship_mode_sk
     and d_year = 2001
     and t_time between 42970 AND 42970 + 28800
     and sm_carrier in ('ORIENTAL',
                        'BOXBUNDLES')
   group by w_warehouse_name,
            w_warehouse_sq_ft,
            w_city,
            w_county,
            w_state,
            w_country,
            d_year) x
group by w_warehouse_name,
         w_warehouse_sq_ft,
         w_city,
         w_county,
         w_state,
         w_country,
         ship_carriers,
         year
order by w_warehouse_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN473
-- =================================================================
select *
from
  (select i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sumsales,
          rank() over (partition by i_category
                       order by sumsales desc) rk
   from
     (select i_category,
             i_class,
             i_brand,
             i_product_name,
             d_year,
             d_qoy,
             d_moy,
             s_store_id,
             sum(coalesce(ss_sales_price * ss_quantity, 0)) sumsales
      from store_sales,
           date_dim,
           store,
           item
      where ss_sold_date_sk = d_date_sk
        and ss_item_sk = i_item_sk
        and ss_store_sk = s_store_sk
        and d_month_seq between 1217 and 1217 + 11
      group by rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)) dw1) dw2
where rk <= 100
order by i_category,
         i_class,
         i_brand,
         i_product_name,
         d_year,
         d_qoy,
         d_moy,
         s_store_id,
         sumsales,
         rk
limit 100;

-- =================================================================
-- Query ID: TPCDSN474
-- =================================================================
select c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       extended_price,
       extended_tax,
       list_price
from
  (select ss_ticket_number,
          ss_customer_sk,
          ca_city bought_city,
          sum(ss_ext_sales_price) extended_price,
          sum(ss_ext_list_price) list_price,
          sum(ss_ext_tax) extended_tax
   from store_sales,
        date_dim,
        store,
        household_demographics,
        customer_address
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_store_sk = store.s_store_sk
     and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     and store_sales.ss_addr_sk = customer_address.ca_address_sk
     and date_dim.d_dom between 1 and 2
     and (household_demographics.hd_dep_count = 3
          or household_demographics.hd_vehicle_count = 4)
     and date_dim.d_year in (1998,
                             1998 + 1,
                             1998 + 2)
     and store.s_city in ('Fairview',
                          'Midway')
   group by ss_ticket_number,
            ss_customer_sk,
            ss_addr_sk,
            ca_city) dn,
     customer,
     customer_address current_addr
where ss_customer_sk = c_customer_sk
  and customer.c_current_addr_sk = current_addr.ca_address_sk
  and current_addr.ca_city <> bought_city
order by c_last_name,
         ss_ticket_number
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN475
-- =================================================================
select cd_gender,
       cd_marital_status,
       cd_education_status,
       count(*) cnt1,
       cd_purchase_estimate,
       count(*) cnt2,
       cd_credit_rating,
       count(*) cnt3
from customer c,
     customer_address ca,
     customer_demographics
where c.c_current_addr_sk = ca.ca_address_sk
  and ca_state in ('IL',
                   'TX',
                   'ME')
  and cd_demo_sk = c.c_current_cdemo_sk
  and c.c_customer_sk in
    (select ss_customer_sk
     from store_sales,
          date_dim
     where ss_sold_date_sk = d_date_sk
       and d_year = 2002
       and d_moy between 1 and 1 + 2)
  and (not exists
         (select *
          from web_sales,
               date_dim
          where c.c_customer_sk = ws_bill_customer_sk
            and ws_sold_date_sk = d_date_sk
            and d_year = 2002
            and d_moy between 1 and 1 + 2)
       and not exists
         (select *
          from catalog_sales,
               date_dim
          where c.c_customer_sk = cs_ship_customer_sk
            and cs_sold_date_sk = d_date_sk
            and d_year = 2002
            and d_moy between 1 and 1 + 2))
group by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
order by cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN476
-- =================================================================
select *
from customer;

-- =================================================================
-- Query ID: TPCDSN477
-- =================================================================
select i_brand_id brand_id,
       i_brand brand,
       t_hour,
       t_minute,
       sum(ext_price) ext_price
from item,

  (select ws_ext_sales_price as ext_price,
          ws_sold_date_sk as sold_date_sk,
          ws_item_sk as sold_item_sk,
          ws_sold_time_sk as time_sk
   from web_sales
   join date_dim on d_date_sk = ws_sold_date_sk
   where d_moy = 12
     and d_year = 2002
   union all select cs_ext_sales_price as ext_price,
                    cs_sold_date_sk as sold_date_sk,
                    cs_item_sk as sold_item_sk,
                    cs_sold_time_sk as time_sk
   from catalog_sales
   join date_dim on d_date_sk = cs_sold_date_sk
   where d_moy = 12
     and d_year = 2002
   union all select ss_ext_sales_price as ext_price,
                    ss_sold_date_sk as sold_date_sk,
                    ss_item_sk as sold_item_sk,
                    ss_sold_time_sk as time_sk
   from store_sales
   join date_dim on d_date_sk = ss_sold_date_sk
   where d_moy = 12
     and d_year = 2002 ) tmp,
     time_dim
where sold_item_sk = i_item_sk
  and i_manager_id = 1
  and time_sk = t_time_sk
  and (t_meal_time = 'breakfast'
       or t_meal_time = 'dinner')
group by i_brand,
         i_brand_id,
         t_hour,
         t_minute
order by ext_price desc,
         i_brand_id ;

-- =================================================================
-- Query ID: TPCDSN478
-- =================================================================
select i_item_desc,
       w_warehouse_name,
       d1.d_week_seq,
       sum(case
               when p_promo_sk is null then 1
               else 0
           end) no_promo,
       sum(case
               when p_promo_sk is not null then 1
               else 0
           end) promo,
       count(*) total_cnt
from catalog_sales
join inventory on (cs_item_sk = inv_item_sk)
join warehouse on (w_warehouse_sk = inv_warehouse_sk)
join item on (i_item_sk = cs_item_sk)
join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
join date_dim d2 on (inv_date_sk = d2.d_date_sk)
join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
left outer join promotion on (cs_promo_sk = p_promo_sk)
left outer join catalog_returns on (cr_item_sk = cs_item_sk
                                    and cr_order_number = cs_order_number)
where d1.d_week_seq = d2.d_week_seq
  and inv_quantity_on_hand < cs_quantity
  and d3.d_date > d1.d_date
  and hd_buy_potential = '1001-5000'
  and d1.d_year = 2002
  and cd_marital_status = 'W'
group by i_item_desc,
         w_warehouse_name,
         d1.d_week_seq
order by total_cnt desc,
         i_item_desc,
         w_warehouse_name,
         d_week_seq
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN479
-- =================================================================
select c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
from
  (select ss_ticket_number,
          ss_customer_sk,
          count(*) cnt
   from store_sales,
        date_dim,
        store,
        household_demographics
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_store_sk = store.s_store_sk
     and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     and date_dim.d_dom between 1 and 2
     and (household_demographics.hd_buy_potential = '1001-5000'
          or household_demographics.hd_buy_potential = '5001-10000')
     and household_demographics.hd_vehicle_count > 0
     and case
             when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
             else null
         end > 1
     and date_dim.d_year in (2000,
                             2000 + 1,
                             2000 + 2)
     and store.s_county in ('Williamson County',
                            'Williamson County',
                            'Williamson County',
                            'Williamson County')
   group by ss_ticket_number,
            ss_customer_sk) dj,
     customer
where ss_customer_sk = c_customer_sk
  and cnt between 1 and 5
order by cnt desc,
         c_last_name asc ;

-- =================================================================
-- Query ID: TPCDSN480
-- =================================================================
with year_total as
  (select c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          d_year as year,
          max(ss_net_paid) year_total,
          's' sale_type
   from customer,
        store_sales,
        date_dim
   where c_customer_sk = ss_customer_sk
     and ss_sold_date_sk = d_date_sk
     and d_year in (1999,
                    2000)
   group by c_customer_id,
            c_first_name,
            c_last_name,
            d_year
   union all select c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    d_year as year,
                    max(ws_net_paid) year_total,
                    'w' sale_type
   from customer,
        web_sales,
        date_dim
   where c_customer_sk = ws_bill_customer_sk
     and ws_sold_date_sk = d_date_sk
     and d_year in (1999,
                    2000)
   group by c_customer_id,
            c_first_name,
            c_last_name,
            d_year)
select t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name
from year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
  and t_s_firstyear.customer_id = t_w_secyear.customer_id
  and t_s_firstyear.customer_id = t_w_firstyear.customer_id
  and t_s_firstyear.sale_type = 's'
  and t_w_firstyear.sale_type = 'w'
  and t_s_secyear.sale_type = 's'
  and t_w_secyear.sale_type = 'w'
  and t_s_firstyear.year = 1999
  and t_s_secyear.year = 1999
  and t_w_firstyear.year = 1999
  and t_w_secyear.year = 1999
  and t_s_firstyear.year_total > 0
  and t_w_firstyear.year_total > 0
  and case
          when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total
          else null
      end >= case
                 when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total
                 else null
             end
order by 1,
         3,
         2
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN481
-- =================================================================
WITH all_sales AS
  (SELECT d_year,
          i_brand_id,
          i_class_id,
          i_category_id,
          i_manufact_id,
          SUM(sales_cnt) AS sales_cnt,
          SUM(sales_amt) AS sales_amt
   FROM
     (SELECT d_year,
             i_brand_id,
             i_class_id,
             i_category_id,
             i_manufact_id,
             cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
             cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt
      FROM catalog_sales
      JOIN item ON i_item_sk = cs_item_sk
      JOIN date_dim ON d_date_sk = cs_sold_date_sk
      LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number
                                    AND cs_item_sk = cr_item_sk)
      WHERE i_category = 'Sports'
      UNION SELECT d_year,
                   i_brand_id,
                   i_class_id,
                   i_category_id,
                   i_manufact_id,
                   ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,
                   ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt
      FROM store_sales
      JOIN item ON i_item_sk = ss_item_sk
      JOIN date_dim ON d_date_sk = ss_sold_date_sk
      LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number
                                  AND ss_item_sk = sr_item_sk)
      WHERE i_category = 'Sports'
      UNION SELECT d_year,
                   i_brand_id,
                   i_class_id,
                   i_category_id,
                   i_manufact_id,
                   ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,
                   ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt
      FROM web_sales
      JOIN item ON i_item_sk = ws_item_sk
      JOIN date_dim ON d_date_sk = ws_sold_date_sk
      LEFT JOIN web_returns ON (ws_order_number = wr_order_number
                                AND ws_item_sk = wr_item_sk)
      WHERE i_category = 'Sports') sales_detail
   GROUP BY d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id)
SELECT prev_yr.d_year AS prev_year,
       curr_yr.d_year AS year,
       curr_yr.i_brand_id,
       curr_yr.i_class_id,
       curr_yr.i_category_id,
       curr_yr.i_manufact_id,
       prev_yr.sales_cnt AS prev_yr_cnt,
       curr_yr.sales_cnt AS curr_yr_cnt,
       curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
       curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM all_sales curr_yr,
     all_sales prev_yr
WHERE curr_yr.i_brand_id = prev_yr.i_brand_id
  AND curr_yr.i_class_id = prev_yr.i_class_id
  AND curr_yr.i_category_id = prev_yr.i_category_id
  AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
  AND curr_yr.d_year = 2002
  AND prev_yr.d_year = 2002-1
  AND CAST(curr_yr.sales_cnt AS DECIMAL(17, 2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17, 2)) < 0.9
ORDER BY sales_cnt_diff,
         sales_amt_diff
limit 100;

-- =================================================================
-- Query ID: TPCDSN482
-- =================================================================
select channel,
       col_name,
       d_year,
       d_qoy,
       i_category,
       COUNT(*) sales_cnt,
       SUM(ext_sales_price) sales_amt
FROM
  (SELECT 'store' as channel,
          'ss_customer_sk' col_name,
                           d_year,
                           d_qoy,
                           i_category,
                           ss_ext_sales_price ext_sales_price
   FROM store_sales,
        item,
        date_dim
   WHERE ss_customer_sk IS not NULL
     AND ss_sold_date_sk = d_date_sk
     AND ss_item_sk = i_item_sk
   UNION ALL SELECT 'web' as channel,
                    'ws_promo_sk' col_name,
                                  d_year,
                                  d_qoy,
                                  i_category,
                                  ws_ext_sales_price ext_sales_price
   FROM web_sales,
        item,
        date_dim
   WHERE ws_promo_sk IS not NULL
     AND ws_sold_date_sk = d_date_sk
     AND ws_item_sk = i_item_sk
   UNION ALL SELECT 'catalog' as channel,
                    'cs_bill_customer_sk' col_name,
                                          d_year,
                                          d_qoy,
                                          i_category,
                                          cs_ext_sales_price ext_sales_price
   FROM catalog_sales,
        item,
        date_dim
   WHERE cs_bill_customer_sk IS not NULL
     AND cs_sold_date_sk = d_date_sk
     AND cs_item_sk = i_item_sk) foo
GROUP BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
ORDER BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN483
-- =================================================================
with ss as
  (select s_store_sk,
          sum(ss_ext_sales_price) as sales,
          sum(ss_net_profit) as profit
   from store_sales,
        date_dim,
        store
   where ss_sold_date_sk = d_date_sk
     and d_date between cast('2000-08-10' as date) and (cast('2000-08-10' as date) + interval '30' day)
     and ss_store_sk = s_store_sk
   group by s_store_sk),
     sr as
  (select s_store_sk,
          sum(sr_return_amt) as returns,
          sum(sr_net_loss) as profit_loss
   from store_returns,
        date_dim,
        store
   where sr_returned_date_sk = d_date_sk
     and d_date between cast('2000-08-10' as date) and (cast('2000-08-10' as date) + interval '30' day)
     and sr_store_sk = s_store_sk
   group by s_store_sk),
     cs as
  (select cs_call_center_sk,
          sum(cs_ext_sales_price) as sales,
          sum(cs_net_profit) as profit
   from catalog_sales,
        date_dim
   where cs_sold_date_sk = d_date_sk
     and d_date between cast('2000-08-10' as date) and (cast('2000-08-10' as date) + interval '30' day)
   group by cs_call_center_sk),
     cr as
  (select cr_call_center_sk,
          sum(cr_return_amount) as returns,
          sum(cr_net_loss) as profit_loss
   from catalog_returns,
        date_dim
   where cr_returned_date_sk = d_date_sk
     and d_date between cast('2000-08-10' as date) and (cast('2000-08-10' as date) + interval '30' day)
   group by cr_call_center_sk),
     ws as
  (select wp_web_page_sk,
          sum(ws_ext_sales_price) as sales,
          sum(ws_net_profit) as profit
   from web_sales,
        date_dim,
        web_page
   where ws_sold_date_sk = d_date_sk
     and d_date between cast('2000-08-10' as date) and (cast('2000-08-10' as date) + interval '30' day)
     and ws_web_page_sk = wp_web_page_sk
   group by wp_web_page_sk),
     wr as
  (select wp_web_page_sk,
          sum(wr_return_amt) as returns,
          sum(wr_net_loss) as profit_loss
   from web_returns,
        date_dim,
        web_page
   where wr_returned_date_sk = d_date_sk
     and d_date between cast('2000-08-10' as date) and (cast('2000-08-10' as date) + interval '30' day)
     and wr_web_page_sk = wp_web_page_sk
   group by wp_web_page_sk)
select channel,
       id,
       sum(sales) as sales,
       sum(returns) as returns,
       sum(profit) as profit
from
  (select 'store channel' as channel,
          ss.s_store_sk as id,
          sales,
          coalesce(returns, 0) as returns,
          (profit - coalesce(profit_loss, 0)) as profit
   from ss
   left join sr on ss.s_store_sk = sr.s_store_sk
   union all select 'catalog channel' as channel,
                    cs_call_center_sk as id,
                    sales,
                    returns,
                    (profit - profit_loss) as profit
   from cs,
        cr
   union all select 'web channel' as channel,
                    ws.wp_web_page_sk as id,
                    sales,
                    coalesce(returns, 0) returns,
                                         (profit - coalesce(profit_loss, 0)) as profit
   from ws
   left join wr on ws.wp_web_page_sk = wr.wp_web_page_sk) x
group by rollup (channel,
                 id)
order by channel,
         id
limit 100;

-- =================================================================
-- Query ID: TPCDSN484
-- =================================================================
with ws as
  (select d_year AS ws_sold_year,
          ws_item_sk,
          ws_bill_customer_sk ws_customer_sk,
          sum(ws_quantity) ws_qty,
          sum(ws_wholesale_cost) ws_wc,
          sum(ws_sales_price) ws_sp
   from web_sales
   left join web_returns on wr_order_number = ws_order_number
   and ws_item_sk = wr_item_sk
   join date_dim on ws_sold_date_sk = d_date_sk
   where wr_order_number is null
   group by d_year,
            ws_item_sk,
            ws_bill_customer_sk),
     cs as
  (select d_year AS cs_sold_year,
          cs_item_sk,
          cs_bill_customer_sk cs_customer_sk,
          sum(cs_quantity) cs_qty,
          sum(cs_wholesale_cost) cs_wc,
          sum(cs_sales_price) cs_sp
   from catalog_sales
   left join catalog_returns on cr_order_number = cs_order_number
   and cs_item_sk = cr_item_sk
   join date_dim on cs_sold_date_sk = d_date_sk
   where cr_order_number is null
   group by d_year,
            cs_item_sk,
            cs_bill_customer_sk),
     ss as
  (select d_year AS ss_sold_year,
          ss_item_sk,
          ss_customer_sk,
          sum(ss_quantity) ss_qty,
          sum(ss_wholesale_cost) ss_wc,
          sum(ss_sales_price) ss_sp
   from store_sales
   left join store_returns on sr_ticket_number = ss_ticket_number
   and ss_item_sk = sr_item_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   where sr_ticket_number is null
   group by d_year,
            ss_item_sk,
            ss_customer_sk)
select ss_customer_sk,
       round(ss_qty / (coalesce(ws_qty, 0) + coalesce(cs_qty, 0)), 2) ratio,
       ss_qty store_qty,
       ss_wc store_wholesale_cost,
       ss_sp store_sales_price,
       coalesce(ws_qty, 0) + coalesce(cs_qty, 0) other_chan_qty,
       coalesce(ws_wc, 0) + coalesce(cs_wc, 0) other_chan_wholesale_cost,
       coalesce(ws_sp, 0) + coalesce(cs_sp, 0) other_chan_sales_price
from ss
left join ws on (ws_sold_year = ss_sold_year
                 and ws_item_sk = ss_item_sk
                 and ws_customer_sk = ss_customer_sk)
left join cs on (cs_sold_year = ss_sold_year
                 and cs_item_sk = ss_item_sk
                 and cs_customer_sk = ss_customer_sk)
where (coalesce(ws_qty, 0) > 0
       or coalesce(cs_qty, 0) > 0)
  and ss_sold_year = 1998
order by ss_customer_sk,
         ss_qty desc,
         ss_wc desc,
         ss_sp desc,
         other_chan_qty,
         other_chan_wholesale_cost,
         other_chan_sales_price,
         ratio
limit 100;

-- =================================================================
-- Query ID: TPCDSN485
-- =================================================================
select c_last_name,
       c_first_name,
       substr(s_city, 1, 30),
       ss_ticket_number,
       amt,
       profit
from
  (select ss_ticket_number,
          ss_customer_sk,
          store.s_city,
          sum(ss_coupon_amt) amt,
          sum(ss_net_profit) profit
   from store_sales,
        date_dim,
        store,
        household_demographics
   where store_sales.ss_sold_date_sk = date_dim.d_date_sk
     and store_sales.ss_store_sk = store.s_store_sk
     and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
     and (household_demographics.hd_dep_count = 7
          or household_demographics.hd_vehicle_count > -1)
     and date_dim.d_dow = 4
     and date_dim.d_year in (2000,
                             2001,
                             2002)
     and store.s_number_employees between 200 and 295
   group by ss_ticket_number,
            ss_customer_sk,
            store.s_city) ms,
     customer
where ss_customer_sk = c_customer_sk
order by c_last_name,
         c_first_name,
         substr(s_city, 1, 30),
         profit
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN486
-- =================================================================
with ssr as
  (select s_store_id as store_id,
          sum(ss_ext_sales_price) as sales,
          sum(coalesce(sr_return_amt, 0)) as returns,
          sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
   from store_sales
   left outer join store_returns on (ss_item_sk = sr_item_sk
                                     and ss_ticket_number = sr_ticket_number),date_dim,
                                                                              store,
                                                                              item,
                                                                              promotion
   where ss_sold_date_sk = d_date_sk
     and d_date between cast('2002-08-14' as date) and (cast('2002-08-14' as date) + interval '30' day)
     and ss_store_sk = s_store_sk
     and ss_item_sk = i_item_sk
     and i_current_price > 50
     and ss_promo_sk = p_promo_sk
     and p_channel_tv = 'N'
   group by s_store_id),
     csr as
  (select cp_catalog_page_id as catalog_page_id,
          sum(cs_ext_sales_price) as sales,
          sum(coalesce(cr_return_amount, 0)) as returns,
          sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
   from catalog_sales
   left outer join catalog_returns on (cs_item_sk = cr_item_sk
                                       and cs_order_number = cr_order_number),date_dim,
                                                                              catalog_page,
                                                                              item,
                                                                              promotion
   where cs_sold_date_sk = d_date_sk
     and d_date between cast('2002-08-14' as date) and (cast('2002-08-14' as date) + interval '30' day)
     and cs_catalog_page_sk = cp_catalog_page_sk
     and cs_item_sk = i_item_sk
     and i_current_price > 50
     and cs_promo_sk = p_promo_sk
     and p_channel_tv = 'N'
   group by cp_catalog_page_id),
     wsr as
  (select web_site_id,
          sum(ws_ext_sales_price) as sales,
          sum(coalesce(wr_return_amt, 0)) as returns,
          sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
   from web_sales
   left outer join web_returns on (ws_item_sk = wr_item_sk
                                   and ws_order_number = wr_order_number),date_dim,
                                                                          web_site,
                                                                          item,
                                                                          promotion
   where ws_sold_date_sk = d_date_sk
     and d_date between cast('2002-08-14' as date) and (cast('2002-08-14' as date) + interval '30' day)
     and ws_web_site_sk = web_site_sk
     and ws_item_sk = i_item_sk
     and i_current_price > 50
     and ws_promo_sk = p_promo_sk
     and p_channel_tv = 'N'
   group by web_site_id)
select channel,
       id,
       sum(sales) as sales,
       sum(returns) as returns,
       sum(profit) as profit
from
  (select 'store channel' as channel,
          'store' || store_id as id,
          sales,
          returns,
          profit
   from ssr
   union all select 'catalog channel' as channel,
                    'catalog_page' || catalog_page_id as id,
                    sales,
                    returns,
                    profit
   from csr
   union all select 'web channel' as channel,
                    'web_site' || web_site_id as id,
                    sales,
                    returns,
                    profit
   from wsr) x
group by rollup (channel,
                 id)
order by channel,
         id
limit 100;

-- =================================================================
-- Query ID: TPCDSN487
-- =================================================================
with customer_total_return as
  (select cr_returning_customer_sk as ctr_customer_sk,
          ca_state as ctr_state,
          sum(cr_return_amt_inc_tax) as ctr_total_return
   from catalog_returns,
        date_dim,
        customer_address
   where cr_returned_date_sk = d_date_sk
     and d_year = 2001
     and cr_returning_addr_sk = ca_address_sk
   group by cr_returning_customer_sk,
            ca_state)
select c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       ca_street_number,
       ca_street_name,
       ca_street_type,
       ca_suite_number,
       ca_city,
       ca_county,
       ca_state,
       ca_zip,
       ca_country,
       ca_gmt_offset,
       ca_location_type,
       ctr_total_return
from customer_total_return ctr1,
     customer_address,
     customer
where ctr1.ctr_total_return >
    (select avg(ctr_total_return) * 1.2
     from customer_total_return ctr2
     where ctr1.ctr_state = ctr2.ctr_state)
  and ca_address_sk = c_current_addr_sk
  and ca_state = 'TN'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id,
         c_salutation,
         c_first_name,
         c_last_name,
         ca_street_number,
         ca_street_name,
         ca_street_type,
         ca_suite_number,
         ca_city,
         ca_county,
         ca_state,
         ca_zip,
         ca_country,
         ca_gmt_offset,
         ca_location_type,
         ctr_total_return
limit 100;

-- =================================================================
-- Query ID: TPCDSN488
-- =================================================================
select i_item_id,
       i_item_desc,
       i_current_price
from item,
     inventory,
     date_dim,
     store_sales
where i_current_price between 58 and 58 + 30
  and inv_item_sk = i_item_sk
  and d_date_sk = inv_date_sk
  and d_date between cast('2001-01-13' as date) and (cast('2001-01-13' as date) + interval '60' day)
  and i_manufact_id in (259,
                        559,
                        580,
                        485)
  and inv_quantity_on_hand between 100 and 500
  and ss_item_sk = i_item_sk
group by i_item_id,
         i_item_desc,
         i_current_price
order by i_item_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN489
-- =================================================================
with sr_items as
  (select i_item_id item_id,
          sum(sr_return_quantity) sr_item_qty
   from store_returns,
        item,
        date_dim
   where sr_item_sk = i_item_sk
     and exists
       (select 1
        from date_dim
        where d_week_seq in
            (select d_week_seq
             from date_dim
             where d_date in ('2001-07-13',
                              '2001-09-10',
                              '2001-11-16',
                              '2000-12-14'))
          and d_date = date_dim.d_date)
     and sr_returned_date_sk = d_date_sk
   group by i_item_id),
     cr_items as
  (select i_item_id item_id,
          sum(cr_return_quantity) cr_item_qty
   from catalog_returns,
        item,
        date_dim
   where cr_item_sk = i_item_sk
     and exists
       (select 1
        from date_dim
        where d_week_seq in
            (select d_week_seq
             from date_dim
             where d_date in ('2001-07-13',
                              '2001-09-10',
                              '2001-11-16',
                              '2000-12-14'))
          and d_date = date_dim.d_date)
     and cr_returned_date_sk = d_date_sk
   group by i_item_id),
     wr_items as
  (select i_item_id item_id,
          sum(wr_return_quantity) wr_item_qty
   from web_returns,
        item,
        date_dim
   where wr_item_sk = i_item_sk
     and exists
       (select 1
        from date_dim
        where d_week_seq in
            (select d_week_seq
             from date_dim
             where d_date in ('2001-07-13',
                              '2001-09-10',
                              '2001-11-16',
                              '2000-12-14'))
          and d_date = date_dim.d_date)
     and wr_returned_date_sk = d_date_sk
   group by i_item_id)
select sr_items.item_id,
       sr_item_qty,
       sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 sr_dev,
       cr_item_qty,
       cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 cr_dev,
       wr_item_qty,
       wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 wr_dev,
       (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 average
from sr_items,
     cr_items,
     wr_items
where sr_items.item_id = cr_items.item_id
  and sr_items.item_id = wr_items.item_id
order by sr_items.item_id,
         sr_item_qty
limit 100;

-- =================================================================
-- Query ID: TPCDSN490
-- =================================================================
select c_customer_id as customer_id,
       coalesce(c_last_name, '') || ',' || coalesce(c_first_name, '') as customername
from customer,
     customer_address,
     customer_demographics,
     household_demographics,
     income_band,
     store_returns
where ca_city = 'Woodland'
  and c_current_addr_sk = ca_address_sk
  and ib_lower_bound >= 60306
  and ib_upper_bound <= 60306 + 50000
  and ib_income_band_sk = hd_income_band_sk
  and cd_demo_sk = c_current_cdemo_sk
  and hd_demo_sk = c_current_hdemo_sk
  and sr_cdemo_sk = cd_demo_sk
order by c_customer_id
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN491
-- =================================================================
select substr(r_reason_desc, 1, 20),
       avg(ws_quantity),
       avg(wr_refunded_cash),
       avg(wr_fee)
from web_sales,
     web_returns,
     web_page,
     customer_demographics cd1,
     customer_demographics cd2,
     customer_address,
     date_dim,
     reason
where ws_web_page_sk = wp_web_page_sk
  and ws_item_sk = wr_item_sk
  and ws_order_number = wr_order_number
  and ws_sold_date_sk = d_date_sk
  and d_year = 1998
  and cd1.cd_demo_sk = wr_refunded_cdemo_sk
  and cd2.cd_demo_sk = wr_returning_cdemo_sk
  and ca_address_sk = wr_refunded_addr_sk
  and r_reason_sk = wr_reason_sk
  and ((cd1.cd_marital_status = 'D'
        and cd1.cd_marital_status = cd2.cd_marital_status
        and cd1.cd_education_status = 'Primary'
        and cd1.cd_education_status = cd2.cd_education_status
        and ws_sales_price between 100.00 and 150.00)
       or (cd1.cd_marital_status = 'S'
           and cd1.cd_marital_status = cd2.cd_marital_status
           and cd1.cd_education_status = 'College'
           and cd1.cd_education_status = cd2.cd_education_status
           and ws_sales_price between 50.00 and 100.00)
       or (cd1.cd_marital_status = 'U'
           and cd1.cd_marital_status = cd2.cd_marital_status
           and cd1.cd_education_status = 'Advanced Degree'
           and cd1.cd_education_status = cd2.cd_education_status
           and ws_sales_price between 150.00 and 200.00))
  and ((ca_country = 'United States'
        and ca_state in ('NC',
                         'TX',
                         'IA')
        and ws_net_profit between 100 and 200)
       or (ca_country = 'United States'
           and ca_state in ('WI',
                            'WV',
                            'GA')
           and ws_net_profit between 150 and 300)
       or (ca_country = 'United States'
           and ca_state in ('OK',
                            'VA',
                            'KY')
           and ws_net_profit between 50 and 250))
group by r_reason_desc
order by substr(r_reason_desc, 1, 20),
         avg(ws_quantity),
         avg(wr_refunded_cash),
         avg(wr_fee)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN492
-- =================================================================
select *
from
  (select sum(ws_net_paid) as total_sum,
          i_category,
          i_class,
          grouping(i_category) + grouping(i_class) as lochierarchy,
          rank() over (partition by grouping(i_category) + grouping(i_class),
                                    case
                                        when grouping(i_class) = 0 then i_category
                                    end
                       order by sum(ws_net_paid) desc) as rank_within_parent
   from web_sales,
        date_dim d1,
        item
   where d1.d_month_seq between 1186 and 1186 + 11
     and d1.d_date_sk = ws_sold_date_sk
     and i_item_sk = ws_item_sk
   group by rollup(i_category, i_class)) as tmp
order by lochierarchy desc,
         case
             when lochierarchy = 0 then i_category
         end,
         rank_within_parent
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN493
-- =================================================================
select count(*)
from (
        (select distinct c_last_name,
                         c_first_name,
                         d_date
         from store_sales,
              date_dim,
              customer
         where store_sales.ss_sold_date_sk = date_dim.d_date_sk
           and store_sales.ss_customer_sk = customer.c_customer_sk
           and d_month_seq between 1202 and 1202 + 11)
      except
        (select distinct c_last_name,
                         c_first_name,
                         d_date
         from catalog_sales,
              date_dim,
              customer
         where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
           and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
           and d_month_seq between 1202 and 1202 + 11)
      except
        (select distinct c_last_name,
                         c_first_name,
                         d_date
         from web_sales,
              date_dim,
              customer
         where web_sales.ws_sold_date_sk = date_dim.d_date_sk
           and web_sales.ws_bill_customer_sk = customer.c_customer_sk
           and d_month_seq between 1202 and 1202 + 11)) cool_cust ;

-- =================================================================
-- Query ID: TPCDSN494
-- =================================================================
select
  (select count(*)
   from store_sales
   join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ss_sold_time_sk = time_dim.t_time_sk
   join store on ss_store_sk = s_store_sk
   where time_dim.t_hour = 8
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 0
           and household_demographics.hd_vehicle_count <= 2)
          or (household_demographics.hd_dep_count = -1
              and household_demographics.hd_vehicle_count <= 1)
          or (household_demographics.hd_dep_count = 3
              and household_demographics.hd_vehicle_count <= 5))
     and store.s_store_name = 'ese') as h8_30_to_9,

  (select count(*)
   from store_sales
   join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ss_sold_time_sk = time_dim.t_time_sk
   join store on ss_store_sk = s_store_sk
   where time_dim.t_hour = 9
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 0
           and household_demographics.hd_vehicle_count <= 2)
          or (household_demographics.hd_dep_count = -1
              and household_demographics.hd_vehicle_count <= 1)
          or (household_demographics.hd_dep_count = 3
              and household_demographics.hd_vehicle_count <= 5))
     and store.s_store_name = 'ese') as h9_to_9_30,

  (select count(*)
   from store_sales
   join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ss_sold_time_sk = time_dim.t_time_sk
   join store on ss_store_sk = s_store_sk
   where time_dim.t_hour = 9
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 0
           and household_demographics.hd_vehicle_count <= 2)
          or (household_demographics.hd_dep_count = -1
              and household_demographics.hd_vehicle_count <= 1)
          or (household_demographics.hd_dep_count = 3
              and household_demographics.hd_vehicle_count <= 5))
     and store.s_store_name = 'ese') as h9_30_to_10,

  (select count(*)
   from store_sales
   join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ss_sold_time_sk = time_dim.t_time_sk
   join store on ss_store_sk = s_store_sk
   where time_dim.t_hour = 10
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 0
           and household_demographics.hd_vehicle_count <= 2)
          or (household_demographics.hd_dep_count = -1
              and household_demographics.hd_vehicle_count <= 1)
          or (household_demographics.hd_dep_count = 3
              and household_demographics.hd_vehicle_count <= 5))
     and store.s_store_name = 'ese') as h10_to_10_30,

  (select count(*)
   from store_sales
   join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ss_sold_time_sk = time_dim.t_time_sk
   join store on ss_store_sk = s_store_sk
   where time_dim.t_hour = 10
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 0
           and household_demographics.hd_vehicle_count <= 2)
          or (household_demographics.hd_dep_count = -1
              and household_demographics.hd_vehicle_count <= 1)
          or (household_demographics.hd_dep_count = 3
              and household_demographics.hd_vehicle_count <= 5))
     and store.s_store_name = 'ese') as h10_30_to_11,

  (select count(*)
   from store_sales
   join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ss_sold_time_sk = time_dim.t_time_sk
   join store on ss_store_sk = s_store_sk
   where time_dim.t_hour = 11
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 0
           and household_demographics.hd_vehicle_count <= 2)
          or (household_demographics.hd_dep_count = -1
              and household_demographics.hd_vehicle_count <= 1)
          or (household_demographics.hd_dep_count = 3
              and household_demographics.hd_vehicle_count <= 5))
     and store.s_store_name = 'ese') as h11_to_11_30,

  (select count(*)
   from store_sales
   join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ss_sold_time_sk = time_dim.t_time_sk
   join store on ss_store_sk = s_store_sk
   where time_dim.t_hour = 11
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = 0
           and household_demographics.hd_vehicle_count <= 2)
          or (household_demographics.hd_dep_count = -1
              and household_demographics.hd_vehicle_count <= 1)
          or (household_demographics.hd_dep_count = 3
              and household_demographics.hd_vehicle_count <= 5))
     and store.s_store_name = 'ese') as h11_30_to_12,

  (select count(*)
   from store_sales
   join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
   join time_dim on ss_sold_time_sk = time_dim.t_time_sk
   join store on ss_store_sk = s_store_sk
   where time_dim.t_hour = 12
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = 0
           and household_demographics.hd_vehicle_count <= 2)
          or (household_demographics.hd_dep_count = -1
              and household_demographics.hd_vehicle_count <= 1)
          or (household_demographics.hd_dep_count = 3
              and household_demographics.hd_vehicle_count <= 5))
     and store.s_store_name = 'ese') as h12_to_12_30 ;

-- =================================================================
-- Query ID: TPCDSN495
-- =================================================================
select *
from
  (select i_category,
          i_class,
          i_brand,
          s_store_name,
          s_company_name,
          d_moy,
          sum(ss_sales_price) sum_sales,
          avg(sum(ss_sales_price)) over (partition by i_category,
                                                      i_brand,
                                                      s_store_name,
                                                      s_company_name) avg_monthly_sales
   from item,
        store_sales,
        date_dim,
        store
   where ss_item_sk = i_item_sk
     and ss_sold_date_sk = d_date_sk
     and ss_store_sk = s_store_sk
     and d_year in (2001)
     and ((i_category in ('Books',
                          'Children',
                          'Electronics')
           and i_class in ('history',
                           'school-uniforms',
                           'audio'))
          or (i_category in ('Men',
                             'Sports',
                             'Shoes')
              and i_class in ('pants',
                              'tennis',
                              'womens')))
   group by i_category,
            i_class,
            i_brand,
            s_store_name,
            s_company_name,
            d_moy) tmp1
where case
          when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales)
          else null
      end > 0.1
order by sum_sales - avg_monthly_sales,
         s_store_name
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN496
-- =================================================================
select cast(amc as decimal(15, 4)) / cast(pmc as decimal(15, 4)) am_pm_ratio
from
  (select count(*) amc
   from web_sales,
        household_demographics,
        time_dim,
        web_page
   where ws_sold_time_sk = time_dim.t_time_sk
     and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
     and ws_web_page_sk = web_page.wp_web_page_sk
     and time_dim.t_hour between 12 and 12 + 1
     and household_demographics.hd_dep_count = 6
     and web_page.wp_char_count between 5000 and 5200) at,

  (select count(*) pmc
   from web_sales,
        household_demographics,
        time_dim,
        web_page
   where ws_sold_time_sk = time_dim.t_time_sk
     and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
     and ws_web_page_sk = web_page.wp_web_page_sk
     and time_dim.t_hour between 14 and 14 + 1
     and household_demographics.hd_dep_count = 6
     and web_page.wp_char_count between 5000 and 5200) pt
order by am_pm_ratio
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN497
-- =================================================================
select cc_call_center_id Call_Center,
       cc_name Call_Center_Name,
       cc_manager Manager,
       sum(cr_net_loss) Returns_Loss
from call_center,
     catalog_returns,
     date_dim,
     customer,
     customer_address,
     customer_demographics,
     household_demographics
where cr_call_center_sk = cc_call_center_sk
  and cr_returned_date_sk = d_date_sk
  and cr_returning_customer_sk = c_customer_sk
  and cd_demo_sk = c_current_cdemo_sk
  and hd_demo_sk = c_current_hdemo_sk
  and ca_address_sk = c_current_addr_sk
  and d_year = 2000
  and d_moy = 12
  and ((cd_marital_status = 'M'
        and cd_education_status = 'Advanced Degree ') or(cd_marital_status = 'W'
                                                         and cd_education_status = 'Unnknown'))
  and hd_buy_potential like 'Unknown%'
  and ca_gmt_offset = -7
group by cc_call_center_id,
         cc_name,
         cc_manager,
         cd_marital_status,
         cd_education_status
order by sum(cr_net_loss) desc ;

-- =================================================================
-- Query ID: TPCDSN498
-- =================================================================
select sum(ws_ext_discount_amt) as "Excess Discount Amount"
from web_sales,
     item,
     date_dim
where i_manufact_id = 393
  and i_item_sk = ws_item_sk
  and d_date between '2000-02-01' and (cast('2000-02-01' as date) + interval '90' day)
  and d_date_sk = ws_sold_date_sk
  and ws_ext_discount_amt >
    (SELECT 1.3 * avg(ws_ext_discount_amt)
     FROM web_sales,
          date_dim
     WHERE ws_item_sk = i_item_sk
       and d_date between '2000-02-01' and (cast('2000-02-01' as date) + interval '90' day)
       and d_date_sk = ws_sold_date_sk )
order by sum(ws_ext_discount_amt)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN499
-- =================================================================
select ss_customer_sk,
       sum(act_sales) sumsales
from
  (select ss_item_sk,
          ss_ticket_number,
          ss_customer_sk,
          case
              when sr_return_quantity is not null then (ss_quantity - sr_return_quantity) * ss_sales_price
              else (ss_quantity * ss_sales_price)
          end act_sales
   from store_sales
   left outer join store_returns on (sr_item_sk = ss_item_sk
                                     and sr_ticket_number = ss_ticket_number)
   join reason on sr_reason_sk = r_reason_sk
   where r_reason_desc = 'Package was damaged') t
group by ss_customer_sk
order by sumsales,
         ss_customer_sk
limit 100;

-- =================================================================
-- Query ID: TPCDSN500
-- =================================================================
select count(distinct ws_order_number) as "order count",
       sum(ws_ext_ship_cost) as "total shipping cost",
       sum(ws_net_profit) as "total net profit"
from web_sales ws1,
     date_dim,
     customer_address,
     web_site
where d_date between '2002-5-01' and (cast('2002-5-01' as date) + interval '60' day)
  and ws1.ws_ship_date_sk = d_date_sk
  and ws1.ws_ship_addr_sk = ca_address_sk
  and ca_state = 'OK'
  and ws1.ws_web_site_sk = web_site_sk
  and web_company_name = 'pri'
  and ws1.ws_order_number in
    (select ws2.ws_order_number
     from web_sales ws2
     where ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
  and not exists
    (select *
     from web_returns wr1
     where ws1.ws_order_number = wr1.wr_order_number)
order by count(distinct ws_order_number)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN501
-- =================================================================
with ws_wh as
  (select ws1.ws_order_number,
          ws1.ws_warehouse_sk wh1,
          ws2.ws_warehouse_sk wh2
   from web_sales ws1,
        web_sales ws2
   where ws1.ws_order_number = ws2.ws_order_number
     and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk )
select count(distinct ws_order_number) as "order count",
       sum(ws_ext_ship_cost) as "total shipping cost",
       sum(ws_net_profit) as "total net profit"
from web_sales ws1,
     date_dim,
     customer_address,
     web_site
where d_date between '2001-4-01' and (cast('2001-4-01' as date) + interval '60' day)
  and ws1.ws_ship_date_sk = d_date_sk
  and ws1.ws_ship_addr_sk = ca_address_sk
  and ca_state = 'VA'
  and ws1.ws_web_site_sk = web_site_sk
  and web_company_name = 'pri'
  and exists
    (select 1
     from ws_wh
     where ws1.ws_order_number = ws_wh.ws_order_number)
  and exists
    (select 1
     from web_returns,
          ws_wh
     where wr_order_number = ws_wh.ws_order_number
       and ws1.ws_order_number = wr_order_number)
order by count(distinct ws_order_number)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN502
-- =================================================================
select count(*)
from store_sales
join household_demographics on ss_hdemo_sk = household_demographics.hd_demo_sk
join time_dim on ss_sold_time_sk = time_dim.t_time_sk
join store on ss_store_sk = s_store_sk
where time_dim.t_hour = 8
  and time_dim.t_minute >= 30
  and household_demographics.hd_dep_count = 0
  and store.s_store_name = 'ese'
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN503
-- =================================================================
with ssci as
  (select ss_customer_sk customer_sk,
          ss_item_sk item_sk
   from store_sales,
        date_dim
   where ss_sold_date_sk = d_date_sk
     and d_month_seq between 1199 and 1199 + 11
   group by ss_customer_sk,
            ss_item_sk),
     csci as
  (select cs_bill_customer_sk customer_sk,
          cs_item_sk item_sk
   from catalog_sales,
        date_dim
   where cs_sold_date_sk = d_date_sk
     and d_month_seq between 1199 and 1199 + 11
   group by cs_bill_customer_sk,
            cs_item_sk)
select sum(case
               when ssci.customer_sk is not null
                    and csci.customer_sk is null then 1
               else 0
           end) store_only,
       sum(case
               when ssci.customer_sk is null
                    and csci.customer_sk is not null then 1
               else 0
           end) catalog_only,
       sum(case
               when ssci.customer_sk is not null
                    and csci.customer_sk is not null then 1
               else 0
           end) store_and_catalog
from ssci
full outer join csci on (ssci.customer_sk = csci.customer_sk
                         and ssci.item_sk = csci.item_sk)
limit 100 ;

-- =================================================================
-- Query ID: TPCDSN504
-- =================================================================
select i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       sum(ss_ext_sales_price) as itemrevenue,
       sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price)) over (partition by i_class) as revenueratio
from store_sales,
     item,
     date_dim
where ss_item_sk = i_item_sk
  and exists
    (select 1
     from item
     where i_category in ('Men',
                          'Sports',
                          'Jewelry'))
  and ss_sold_date_sk = d_date_sk
  and exists
    (select 1
     from date_dim
     where d_date between cast('1999-02-05' as date) and (cast('1999-02-05' as date) + interval '30' day))
group by i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
order by i_category,
         i_class,
         i_item_id,
         i_item_desc,
         revenueratio ;

-- =================================================================
-- Query ID: TPCDSN505
-- =================================================================
select substr(w_warehouse_name, 1, 20),
       sm_type,
       cc_name,
       sum(case
               when (cs_ship_date_sk - cs_sold_date_sk <= 30) then 1
               else 0
           end) as "30 days",
       sum(case
               when (cs_ship_date_sk - cs_sold_date_sk > 30)
                    and (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1
               else 0
           end) as "31-60 days",
       sum(case
               when (cs_ship_date_sk - cs_sold_date_sk > 60)
                    and (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1
               else 0
           end) as "61-90 days",
       sum(case
               when (cs_ship_date_sk - cs_sold_date_sk > 90)
                    and (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1
               else 0
           end) as "91-120 days",
       sum(case
               when (cs_ship_date_sk - cs_sold_date_sk > 120) then 1
               else 0
           end) as ">120 days"
from catalog_sales,
     warehouse,
     ship_mode,
     call_center,
     date_dim
where d_month_seq between 1194 and 1194 + 11
  and cs_ship_date_sk = d_date_sk
  and cs_warehouse_sk = w_warehouse_sk
  and cs_ship_mode_sk = sm_ship_mode_sk
  and cs_call_center_sk = cc_call_center_sk
group by substr(w_warehouse_name, 1, 20),
         sm_type,
         cc_name
order by substr(w_warehouse_name, 1, 20),
         sm_type,
         cc_name
limit 100 ;
