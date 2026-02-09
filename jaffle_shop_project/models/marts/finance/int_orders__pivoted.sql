{%- set payment_methods_sql -%}
        select distinct payment_method from {{ ref('stg__payments') }}
     {%- endset -%}
     {%- if execute -%}
        {%- set results = run_query(payment_methods_sql) -%}
        {%- set payment_methods = results.columns[0].values() -%}
    {%- else -%}
        {%- set payment_methods = [] -%}
    {%- endif -%}

with payments as 
(
    select * from {{ ref('stg__payments') }}
    where status = "success"
),
pivoted as 
(
     select order_id,
    {%- for method in payment_methods -%}
        sum(case when payment_method =  '{{ method }}' then amount else 0 end) as {{ method }}_amount
        {%- if not loop.last -%}
            ,
        {%- endif -%}
    {%- endfor -%}    
     from payments
     group by order_id
)