drop table IF EXISTS follain.vend_customers;
create table follain.vend_customers as
select
data->'customer'->>'id' as customer_id,
initcap(lower(regexp_replace(max(data->>'customer_name'), '\s+', ' ', 'g'))) as customer_name,
max(data->'customer'->>'first_name') as first_name,
max(data->'customer'->>'last_name') as last_name,
max(data->'customer'->>'company_name') as company_name,
max(data->'customer'->>'phone') as phone,
max(data->'customer'->>'mobile') as mobile,
max(data->'customer'->>'fax') as fax,
max(data->'customer'->>'email') as email,
max(data->'customer'->>'physical_address1') as physical_address1,
max(data->'customer'->>'physical_address2') as physical_address2,
max(data->'customer'->>'physical_suburb') as physical_suburb,
max(data->'customer'->>'physical_city') as physical_city,
max(data->'customer'->>'physical_postcode') as physical_postcode,
max(data->'customer'->>'physical_state') as physical_state,
max(data->'customer'->>'physical_country_id') as physical_country_id,
max(data->'customer'->>'postal_address1') as postal_address1,
max(data->'customer'->>'postal_address2') as postal_address2,
max(data->'customer'->>'postal_suburb') as postal_suburb,
max(data->'customer'->>'postal_city') as postal_city,
max(data->'customer'->>'postal_postcode') as postal_postcode,
max(data->'customer'->>'postal_state') as postal_state,
max(data->'customer'->>'postal_country_id') as postal_country_id,
max(data->'customer'->>'enable_loyalty') as enable_loyalty,
max(data->'customer'->>'loyalty_balance') as loyalty_balance,
max(data->'customer'->>'updated_at') as updated_at,
max(data->'customer'->>'deleted_at') as deleted_at,
max(data->'customer'->>'balance') as balance,
max(data->'customer'->>'year_to_date') as year_to_date,
max(data->'customer'->>'date_of_birth') as date_of_birth,
max(data->'customer'->>'sex') as sex,
max(data->'customer'->'contact'->>'company_name') as contact_company_name,
max(data->'customer'->'contact'->>'phone') as contact_phone,
case when length(max(data->'customer'->'contact'->>'email')) < 1
then concat(initcap(lower(regexp_replace(max(data->>'customer_name'), '\s+', ' ', 'g'))),'@NOMAIL')
else lower(max(data->'customer'->'contact'->>'email')) end as contact_email
FROM
follain.vend_raw_sales r
GROUP BY customer_id;

GRANT SELECT ON TABLE follain.vend_customers TO public;

CREATE INDEX customer_id
  ON follain.vend_customers
  USING btree
  (customer_id COLLATE pg_catalog."default");


drop table  IF EXISTS follain.vend_sales;
create table follain.vend_sales as
SELECT
r.id as order_id,
(line->>'id')::varchar as row_id,
case when (r.data->>'register_id') ='7e3ee1b4-c935-11e2-a415-bc764e10976c' 
and (r.data#>'{register_sale_payments,0}'->>'label') like '%Online' 
then '060f02b1-c86d-11e6-fcd2-794aed89bcda' else (r.data->>'register_id') end ::varchar as register_id,
(r.data->>'register_id') ::varchar as old_register_id,
(r.data->>'customer_id')::varchar as customer_id,
case when length(r.data->'customer'->'contact'->>'email') < 1
then concat(initcap(regexp_replace(lower(r.data->>'customer_name'), '\s+', ' ', 'g')),'@NOMAIL')
else lower(r.data->'customer'->'contact'->>'email') end as contact_email,
initcap(regexp_replace(lower(r.data->>'customer_name'), '\s+', ' ', 'g')) as customer_name,
(r.data->>'sale_date')::timestamptz as sale_date,
(r.data->>'created_at')::timestamptz as created_at,
(r.data->>'total_price')::numeric as order_total_price,
(r.data->>'total_cost')::numeric as order_total_cost,
(r.data->>'total_tax')::numeric as order_total_tax,
(r.data->>'tax_name')::varchar as order_tax_name,
(r.data->>'status')::varchar as order_status,
(r.data->>'invoice_number')::varchar as invoice_number,
(r.data->>'return_for')::varchar as order_return_for,
(r.data->'totals'->>'total_payment')::numeric as order_total_payment,
(r.data#>'{register_sale_payments,0}'->>'name') as payment_type,
(r.data#>'{register_sale_payments,0}'->>'label') as payment_label,
case when (r.data#>'{register_sale_payments,0}'->>'label') like '%Online' then 'eComm' else 'B&M' end store_type,
(line->>'product_id')::varchar as product_id,
(line->>'sequence')::integer as line_item_sequence,
case when (line->>'product_id') ='0861f40c-32bc-11e3-a29a-bc305bf5da20' then (line->>'quantity')::numeric/640 
else(line->>'quantity')::numeric end as quantity,
(line->>'price')::numeric as price,
(line->>'cost')::numeric as cost,
(line->>'discount')::numeric as discount,
(line->>'loyalty_value')::numeric as loyalty_value,
(line->>'tax')::numeric as tax,
(line->>'tax_id') as tax_id,
(line->>'tax_name') as tax_name,
(line->>'tax_rate')::numeric as tax_rate,
(line->>'tax_total')::numeric as tax_total,
(line->>'price_total')::numeric as price_total,
(line->>'display_retail_price_tax_inclusive') as display_retail_price_tax_inclusive,
(line->>'status')::varchar as line_status
FROM
follain.vend_raw_sales r
JOIN LATERAL json_array_elements( (r.data->'register_sale_products')::json ) line(child) ON TRUE;

GRANT SELECT ON TABLE follain.vend_sales TO public;

CREATE INDEX sales_product_id
  ON follain.vend_sales
  USING btree
  (product_id COLLATE pg_catalog."default");

CREATE INDEX sales_contact_email
  ON follain.vend_sales
  USING btree
  (contact_email COLLATE pg_catalog."default");

drop table  IF EXISTS follain.vend_products cascade;

create table follain.vend_products as
SELECT
id as product_id,
(r.data->>'name') as name,
(r.data->>'date') as date,
(r.data->>'type') as type,
(r.data->>'sku') as sku,
(r.data->>'active') as active,
(r.data->>'supplier_name') as supplier_name,
(r.data->>'supplier_code') as supplier_code,
(r.data->>'brand_name') as brand_name,
(r.data->>'tags') as tags,
(r.data->>'variant_option_one_name') as variant_option_one_name,
(r.data->>'variant_option_one_value') as variant_option_one_value,
(r.data->>'price')::numeric as price,
(r.data->>'tax')::numeric as tax,
(r.data->>'tax_rate')::numeric as tax_rate
FROM follain.vend_raw_products r;

GRANT SELECT ON TABLE follain.vend_products TO public;

CREATE INDEX pproduct_id
  ON follain.vend_products
  USING btree
  (product_id COLLATE pg_catalog."default" DESC);


insert into follain.vend_product_inventory 
select 
p.id as product_id,
obj->>'outlet_id' as outlet_id,
(obj->>'count')::decimal as ATS,
(obj->>'reorder_point') as reorder_point,
(obj->>'restock_level') as restock_level,
p.asof_time
from  follain.vend_raw_products p 
JOIN LATERAL json_array_elements( (p.data->'inventory')::json ) obj(child) ON TRUE
left join follain.vend_product_inventory a on a.product_id=p.id  and a.outlet_id=(obj->>'outlet_id') and a.ats=(obj->>'count')::decimal
and a.asof_time=(select max(asof_time) from follain.vend_product_inventory m where a.product_id=m.product_id and a.outlet_id=m.outlet_id)
where a.product_id is null;


drop table  IF EXISTS follain.vend_stock_movements cascade;
create table follain.vend_stock_movements as
SELECT
r.id as sm_id,
(r.data->>'name') as name,
(r.data->>'type') as type,
(r.data->>'outlet_id') as outlet_id,
(r.data->>'source_outlet_id') as source_outlet_id,
(r.data->>'supplier_id')as supplier_id,
(r.data->>'received_at')::timestamptz as received_at,
(r.data->>'created_at')::timestamptz as created_at,
(r.data->>'status') as status,
(line->>'product_id') as product_id,
(line->>'name') as product_name,
(line->>'count')::numeric as count,
(line->>'received')::numeric as received,
(line->>'cost')::numeric as cost
FROM
follain.vend_raw_stock_movements r
JOIN LATERAL json_array_elements( (r.data->'products')::json ) line(child) ON TRUE;

GRANT SELECT ON TABLE follain.vend_stock_movements TO public;
drop table if exists follain.vend_supplier;
create table follain.vend_supplier as
SELECT
r.id as supplier_id,
(r.data->>'name') as supplier_name,
(r.data->>'description') as description,
(r.data->>'company_name') as company_name
FROM follain.vend_raw_supplier r;
GRANT SELECT ON TABLE follain.vend_supplier TO public;
CREATE INDEX supplier_sup_id
  ON follain.vend_supplier
  USING btree
  (supplier_id COLLATE pg_catalog."default");

insert into follain.vend_outlets 
SELECT
r.id as outlet_id,
(r.data->>'name') as location,
(r.data->>'physical_city') as physical_city,
(r.data->>'physical_address1') as physical_address1,
(r.data->>'physical_address2') as physical_address2,
(r.data->>'physical_postcode') as physical_postcode,
(r.data->>'physical_state') as physical_state
FROM follain.vend_raw_outlets r where r.id not in ( select outlet_id from follain.vend_outlets );

insert into  follain.vend_registers 
SELECT
(r.data->>'id') as register_id,
(r.data->>'name') as name,
(r.data->>'outlet_id') as outlet_id
FROM follain.vend_raw_registers r where (r.data->>'id') not in (select register_id from follain.vend_registers );
