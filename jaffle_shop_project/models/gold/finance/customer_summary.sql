select  
    customer_id,
    order_date
from {{ ref('stg__orders')}}
group by customer_id, order_date