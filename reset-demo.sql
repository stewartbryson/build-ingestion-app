drop application if exists brokerage_bronze;
DROP APPLICATION PACKAGE if exists brokerage_bronze_package;

drop application if exists brokerage_silver;
DROP APPLICATION PACKAGE if exists brokerage_silver_package;

drop application if exists brokerage_gold;
DROP APPLICATION PACKAGE if exists brokerage_gold_package;

create or replace database brokerage;

create schema brokerage.reference;

create table brokerage.reference.status_type
as select * from tpcdi.digen.status_type;

create table brokerage.reference.industry
as select * from tpcdi.digen.industry;