select f.profit, f.shipping_cost from fact_table f
where f.profit > 0
order by f.shipping_cost asc;
