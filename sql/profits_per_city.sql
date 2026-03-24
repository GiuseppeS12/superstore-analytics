select sum(f.profit) as profit_per_city, r.city as city from fact_table f
join region_data r on f.region_id = r.region_id
GROUP by r.city
order by profit_per_city desc;
