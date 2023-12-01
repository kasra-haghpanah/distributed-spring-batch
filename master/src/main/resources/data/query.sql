---------------------------------------------------------------------------------------
CREATE VIEW sales_by_year as
select year,
       sum(sale)
from (SELECT year, (eu_sales + global_sales + jp_sales + na_sales + other_sales) as sale
      from video_game_sales) as tb_sale
group by year;
---------------------------------------------------------------------------------------
SELECT * FROM sales_by_year;
---------------------------------------------------------------------------------------
SELECT sum_sale, year
from (select year,
             sum(sale) sum_sale
      from (SELECT year, (eu_sales + global_sales + jp_sales + na_sales + other_sales) as sale
            from video_game_sales) as tb_sale
      group by year) as sale_by_year
order by sum_sale desc
limit 40 offset 0;
---------------------------------------------------------------------------------------
SELECT sum(sale_by_year.sales) sales, sale_by_year.platform
from (select tb_sale.year, sum(tb_sale.sales1) sales, tb_sale.platform
      from (SELECT year, platform, (eu_sales + global_sales + jp_sales + na_sales + other_sales) as sales1
            from video_game_sales) as tb_sale
     group by tb_sale.year) as sale_by_year
group by sale_by_year.platform;
---------------------------------------------------------------------------------------
REPLACE INTO year_platform_report
select yp1.year,
       yp1.platform, (
           select sum(vgs.global_sales) from video_game_sales vgs
           where vgs.platform = yp1.platform and vgs.year = yp1.year
       )
from year_platform_report as yp1;
---------------------------------------------------------------------------------------