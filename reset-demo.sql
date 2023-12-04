create or replace database brokerage;

create schema brokerage.reference;

create table brokerage.reference.status_type
as select * from tpcdi.digen.status_type;

create table brokerage.reference.industry
as select * from tpcdi.digen.industry;