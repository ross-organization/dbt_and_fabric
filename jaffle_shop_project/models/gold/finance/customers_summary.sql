select  
    customer_id,
    order_date,
    count(*) as number_of_orders
from {{ ref('stg__orders')}}
group by customer_id, order_date