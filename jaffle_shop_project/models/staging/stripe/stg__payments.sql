select
    ID as payment_id,
    ORDERID  as order_id,
    PAYMENTMETHOD as payment_method,
    STATUS as status,
    {{ cents_to_dollars("AMOUNT") }} as amount,
    CREATED as payment_date
from {{source('stripe','payments')}}