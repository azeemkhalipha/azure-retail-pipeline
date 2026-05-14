select
    country,
    count(distinct invoice_no)   as total_orders,
    round(sum(total_revenue), 2) as total_revenue
from {{ ref('stg_retail') }}
group by country
order by total_revenue desc
