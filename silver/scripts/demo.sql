
drop application if exists brokerage_silver;
drop application package if exists brokerage_silver_package;

CREATE APPLICATION PACKAGE brokerage_silver_package;
  
show application packages;
use application package brokerage_silver_package;
CREATE SCHEMA stage_content;

use schema stage_content;

CREATE OR REPLACE STAGE brokerage_silver_package.stage_content.package_content
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|' SKIP_HEADER = 1)
  directory = (enable = true);

PUT file:///Users/stewartbryson/Source/building-blocks-app/silver/manifest.yml @brokerage_silver_package.stage_content.package_content overwrite=true auto_compress=false;
PUT file:///Users/stewartbryson/Source/building-blocks-app/silver/scripts/setup.sql @brokerage_silver_package.stage_content.package_content/scripts overwrite=true auto_compress=false;
PUT file:///Users/stewartbryson/Source/building-blocks-app/silver/README.md @brokerage_silver_package.stage_content.package_content overwrite=true auto_compress=false;

-- share other layers to the application
GRANT REFERENCE_USAGE ON DATABASE brokerage
  TO SHARE IN APPLICATION PACKAGE brokerage_silver_package;

create schema brokerage_silver_package.shared_bronze;

  create or replace view brokerage_silver_package.shared_bronze.daily_market
  as select * from brokerage.bronze.daily_market;

  create or replace view brokerage_silver_package.shared_bronze.company
  as select * from brokerage.bronze.company;

  create or replace view brokerage_silver_package.shared_bronze.security
  as select * from brokerage.bronze.security;

  create or replace view brokerage_silver_package.shared_bronze.financial
  as select * from brokerage.bronze.financial;

  grant usage on schema brokerage_silver_package.shared_bronze
  to share in application package brokerage_silver_package;

  grant select on all views in schema brokerage_silver_package.shared_bronze
  to share in application package brokerage_silver_package;

-- share reference data
create schema if not exists brokerage_silver_package.shared_reference;

  create or replace view brokerage_silver_package.shared_reference.status_type
  as select * from brokerage.reference.status_type;

  create or replace view brokerage_silver_package.shared_reference.industry
  as select * from brokerage.reference.industry;

  grant usage on schema brokerage_silver_package.shared_reference
  to share in application package brokerage_silver_package;

  grant select on all views in schema brokerage_silver_package.shared_reference
  to share in application package brokerage_silver_package;

drop application if exists brokerage_silver;

CREATE APPLICATION BROKERAGE_SILVER
  FROM APPLICATION PACKAGE BROKERAGE_SILVER_PACKAGE
  USING '@brokerage_silver_package.stage_content.package_content';

-- wrap a scheduled task around this procedure
call brokerage_silver.pipeline_code.run_pipeline();

-- create external views from the application
use database brokerage;

create schema if not exists brokerage.silver;

create or replace view brokerage.silver.daily_market
as select * from brokerage_silver.published.daily_market;

create or replace view brokerage.silver.companies
as select * from brokerage_silver.published.companies;

create or replace view brokerage.silver.securities
as select * from brokerage_silver.published.securities;

create or replace view brokerage.silver.financials
as select * from brokerage_silver.published.financials;