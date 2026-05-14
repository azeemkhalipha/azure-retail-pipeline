select
    invoice_year,
    invoice_month,
    round(sum(total_revenue), 2) as total_revenue,
    count(distinct invoice_no)   as total_orders
from {{ ref('stg_retail') }}
group by invoice_year, invoice_month
order by invoice_year, invoice_month
