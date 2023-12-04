drop application brokerage_silver;
DROP APPLICATION PACKAGE brokerage_silver_package;
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

create schema brokerage_silver_package.bronze;

create view brokerage_silver_package.bronze.daily_market
as select * from brokerage.bronze.daily_market;

create view brokerage_silver_package.bronze.company
as select * from brokerage.bronze.company;

create view brokerage_silver_package.bronze.security
as select * from brokerage.bronze.security;

create view brokerage_silver_package.bronze.financial
as select * from brokerage.bronze.financial;

grant usage on schema brokerage_silver_package.bronze
to share in application package brokerage_silver_package;

grant select on all views in schema brokerage_silver_package.bronze
to share in application package brokerage_silver_package;

CREATE APPLICATION BROKERAGE_SILVER
  FROM APPLICATION PACKAGE BROKERAGE_SILVER_PACKAGE
  USING '@brokerage_silver_package.stage_content.package_content';

call brokerage_silver.pipeline_code.run_pipeline();