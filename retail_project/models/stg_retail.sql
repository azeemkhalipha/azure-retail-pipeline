select
    cast(Invoice          as varchar)          as invoice_no,
    cast(StockCode        as varchar)          as stock_code,
    cast(Description      as varchar)          as description,
    cast(Quantity         as integer)          as quantity,
    cast(InvoiceDate      as date)             as invoice_date,
    cast(Price            as double)           as unit_price,
    cast("Customer ID"    as varchar)          as customer_id,
    cast(Country          as varchar)          as country,
    round(Quantity * Price, 2)                 as total_revenue,
    year(cast(InvoiceDate as date))            as invoice_year,
    month(cast(InvoiceDate as date))           as invoice_month
from read_csv_auto('/Users/azeemkhalipha/azure-retail-pipeline/raw_data/online_retail_II.csv')
where Invoice not like 'C%'
  and Quantity > 0
  and Price > 0
  and "Customer ID" is not null
