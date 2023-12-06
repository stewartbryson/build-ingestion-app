drop application if exists brokerage_gold;
drop application package if exists brokerage_gold_package;

CREATE APPLICATION PACKAGE brokerage_gold_package;
  
show application packages;
use application package brokerage_gold_package;
CREATE SCHEMA stage_content;

use schema stage_content;

CREATE OR REPLACE STAGE brokerage_gold_package.stage_content.package_content
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|' SKIP_HEADER = 1)
  directory = (enable = true);

PUT file:///Users/stewartbryson/Source/building-blocks-app/gold/manifest.yml @brokerage_gold_package.stage_content.package_content overwrite=true auto_compress=false;
PUT file:///Users/stewartbryson/Source/building-blocks-app/gold/scripts/setup.sql @brokerage_gold_package.stage_content.package_content/scripts overwrite=true auto_compress=false;
PUT file:///Users/stewartbryson/Source/building-blocks-app/gold/streamlit/exchange_analytics.py @brokerage_gold_package.stage_content.package_content/streamlit overwrite=true auto_compress=false;
PUT file:///Users/stewartbryson/Source/building-blocks-app/gold/README.md @brokerage_gold_package.stage_content.package_content overwrite=true auto_compress=false;

-- share other layers to the application
GRANT REFERENCE_USAGE ON DATABASE brokerage
  TO SHARE IN APPLICATION PACKAGE brokerage_gold_package;

create schema brokerage_gold_package.shared_silver;

  create or replace view brokerage_gold_package.shared_silver.daily_market
  as select * from brokerage.silver.daily_market;

  create or replace view brokerage_gold_package.shared_silver.companies
  as select * from brokerage.silver.companies;

  create or replace view brokerage_gold_package.shared_silver.securities
  as select * from brokerage.silver.securities;

  create or replace view brokerage_gold_package.shared_silver.financials
  as select * from brokerage.silver.financials;

  grant usage on schema brokerage_gold_package.shared_silver
  to share in application package brokerage_gold_package;

  grant select on all views in schema brokerage_gold_package.shared_silver
  to share in application package brokerage_gold_package;

-- create the application
CREATE APPLICATION BROKERAGE_GOLD
  FROM APPLICATION PACKAGE BROKERAGE_GOLD_PACKAGE
  USING '@brokerage_gold_package.stage_content.package_content';

-- wrap a scheduled task around this procedure
call brokerage_gold.pipeline_code.run_pipeline();

-- create external views from the application
use database brokerage;

create schema if not exists brokerage.gold;

create or replace view brokerage.gold.dim_company
as select * from brokerage_gold.published.dim_company;

create or replace view brokerage.gold.dim_security
as select * from brokerage_gold.published.dim_security;

create or replace view brokerage.gold.fact_market_history
as select * from brokerage_gold.published.fact_market_history;

create or replace view brokerage.gold.rep_exchange
as select * from brokerage_gold.published.rep_exchange;

create or replace view brokerage.gold.rep_sp_rating
as select * from brokerage_gold.published.rep_sp_rating;
