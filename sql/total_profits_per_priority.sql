select sum(f.profit) as total_profit_per_priority, f.order_priority from fact_table f
GROUP by f.order_priority
order by total_profit_per_priority desc;
