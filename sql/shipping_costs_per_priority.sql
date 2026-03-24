select sum(f.shipping_cost) as shipping_cost_per_priority, f.order_priority from fact_table f
GROUP by f.order_priority
order by shipping_cost_per_priority desc;
