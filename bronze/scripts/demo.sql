CREATE APPLICATION PACKAGE brokerage_bronze_package;
show application packages;
use application package brokerage_bronze_package;
CREATE SCHEMA stage_content;

use schema stage_content;

CREATE OR REPLACE STAGE brokerage_bronze_package.stage_content.package_content
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|' SKIP_HEADER = 1)
  directory = (enable = true);

PUT file:///Users/stewartbryson/Source/building-blocks-app/bronze/manifest.yml @brokerage_bronze_package.stage_content.package_content overwrite=true auto_compress=false;
PUT file:///Users/stewartbryson/Source/building-blocks-app/bronze/scripts/setup.sql @brokerage_bronze_package.stage_content.package_content/scripts overwrite=true auto_compress=false;
PUT file:///Users/stewartbryson/Source/building-blocks-app/bronze/README.md @brokerage_bronze_package.stage_content.package_content overwrite=true auto_compress=false;

CREATE APPLICATION BROKERAGE_BRONZE
  FROM APPLICATION PACKAGE BROKERAGE_BRONZE_PACKAGE
  USING '@brokerage_bronze_package.stage_content.package_content';

PUT file:///Users/stewartbryson/dev/app-files/daily_market/* @brokerage_bronze.published.daily_market auto_compress=true;
PUT file:///Users/stewartbryson/dev/app-files/finwire/* @brokerage_bronze.staged.finwire auto_compress=true;

-- these are the procedures to wrap a scheduled task around.
call brokerage_bronze.pipeline_code.ingest_files();
call brokerage_bronze.pipeline_code.run_pipeline();

-- create external views from the application
use database brokerage;

create schema if not exists brokerage.bronze;

create or replace view brokerage.bronze.daily_market
as select * from brokerage_bronze.published.daily_market;

create or replace view brokerage.bronze.company
as select * from brokerage_bronze.published.company;

create or replace view brokerage.bronze.security
as select * from brokerage_bronze.published.security;

create or replace view brokerage.bronze.financial
as select * from brokerage_bronze.published.financial;