with aggregated_profits as 
(select sum(f.profit) AS total_profit, c.segment AS segment, c.customer_id AS customer_id,
      (row_number() OVER (PARTITION BY c.segment ORDER BY sum(f.profit) desc)) AS row_num from fact_table f
      join customer_data c ON f.customer_id = c.customer_id
      group by c.segment, c.customer_id
)
  
# customer_id col maggior profitto per ogni segmento
select total_profit,segment,customer_id from aggregated_profits
where row_num = 1
order by total_profit DESC;
