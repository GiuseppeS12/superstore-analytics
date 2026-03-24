with profit_month_ordered as (select sum(profit) as profit_per_month, year(order_date) as year_, month(order_date) as month_ from fact_table
group by year(order_date),month(order_date)
order by year(order_date) ASC, month(order_date) asc),

# Profitto medio mensile
averaged_month_profit as (
select avg(profit_per_month) as monthly_avg_profit, STDDEV(profit_per_month) as monthly_stddev from profit_month_ordered)
,
# Profitto medio cumulativo mensile
acm as (
select row_number() over () as month_enum,sum(profit_per_month) over (order by year_ asc, month_ asc) as monthly_cumulative_profit, sum(monthly_avg_profit) over (order by year_ asc, month_ asc) as monthly_avg_cumulative_profit from profit_month_ordered
cross join averaged_month_profit
)

select * from acm;
