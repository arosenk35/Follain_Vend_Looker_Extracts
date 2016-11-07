drop table follain.vend_raw_sales;
create table follain.vend_raw_sales (
	id varchar(100) PRIMARY KEY,
	data JSONB NOT NULL
);
drop table follain.vend_raw_products;
create table follain.vend_raw_products (
	id varchar(100) PRIMARY KEY,
	data JSONB NOT NULL
);
drop table follain.vend_raw_stock_movements ;
create table follain.vend_raw_stock_movements (
	id varchar(100) PRIMARY KEY,
	data JSONB NOT NULL
);
drop table follain.vend_raw_registers;
create table follain.vend_raw_registers (
	id varchar(100) PRIMARY KEY,
	data JSONB NOT NULL
);
drop table follain.vend_raw_outlets;
create table follain.vend_raw_outlets (
	id varchar(100) PRIMARY KEY,
	data JSONB NOT NULL
);
drop table follain.vend_raw_customers;
create table follain.vend_raw_customers (
	id varchar(100) PRIMARY KEY,
	data JSONB NOT NULL
);