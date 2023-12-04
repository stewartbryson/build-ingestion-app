-- Setup script for the Brokerage Bronze application.
-- Create the application role
CREATE APPLICATION ROLE if not exists app_public;

-- configuration section
-- simple generic methods to register callbacks
create or alter versioned schema config_code;

    grant usage on schema config_code to application role app_public;

    -- this callback is used by the UI to ultimately bind a reference that expects one value
    create or replace procedure config_code.register_single_callback(ref_name string, operation string, ref_or_alias string)
    returns string
    language sql
    as $$
        begin
            case (operation)
                when 'ADD' then
                    select system$set_reference(:ref_name, :ref_or_alias);
                when 'REMOVE' then
                    select system$remove_reference(:ref_name);
                when 'CLEAR' then
                    select system$remove_reference(:ref_name);
                else
                    return 'Unknown operation: ' || operation;
            end case;
            system$log('debug', 'register_single_callback: ' || operation || ' succeeded');
            return 'Operation ' || operation || ' succeeded';
        end;
    $$;

    grant usage on procedure config_code.register_single_callback(string, string, string)
        to application role app_public;

-- stage schema
create schema if not exists staged;
grant usage on schema staged to application role app_public;

-- asset schema
create schema if not exists published;
grant usage on schema published to application role app_public;

-- load tables
create table if not exists staged.finwire (
    line varchar
);
grant all on table staged.finwire to application role app_public;
create table if not exists published.daily_market (
	DM_DATE DATE,
	DM_S_SYMB VARCHAR(16777216),
	DM_CLOSE FLOAT,
	DM_HIGH FLOAT,
	DM_LOW FLOAT,
	DM_VOL FLOAT
);
grant all on table published.daily_market to application role app_public;

create stage if not exists staged.finwire directory = (enable = true)
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|');
grant read, write on stage staged.finwire to application role app_public;
create stage if not exists published.daily_market directory = (enable = true)
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|');
grant read, write on stage published.daily_market to application role app_public;

-- pipeline code
create or alter versioned schema pipeline_code;

    grant usage on schema pipeline_code to application role app_public;

    create or replace procedure pipeline_code.ingest_files()
    returns varchar
    language sql
    as
    $$
        begin
            copy into staged.finwire from @staged.finwire;
            copy into published.daily_market from @published.daily_market;
            return 'Files ingested.';
        end;
    $$
    ;
    grant usage on procedure pipeline_code.ingest_files()
    to application role app_public;

    create or replace procedure pipeline_code.run_pipeline()
    returns varchar
    language sql
    as
    $$
        begin
            -- extract the shared columns
            create or replace table staged.finwire_shared
            as
            SELECT 
                line,
                substring(line, 16, 3) AS rec_type,
                to_timestamp(substring(line, 0, 15), 'yyyymmdd-hhmiss') AS pts
            FROM staged.finwire;

            -- create the published tables
            create or replace table published.company
            as
            SELECT "PTS",
                substring("LINE", 19, 60) AS "COMPANY_NAME",
                substring("LINE", 79, 10) AS "CIK",
                substring("LINE", 89, 4) AS "STATUS",
                substring("LINE", 93, 2) AS "INDUSTRY_ID",
                substring("LINE", 95, 4) AS "SP_RATING",
                TRY_CAST (trim(substring("LINE", 99, 8)) AS DATE) AS "FOUNDING_DATE",
                substring("LINE", 107, 80) AS "ADDRESS_LINE1",
                substring("LINE", 187, 80) AS "ADDRESS_LINE2",
                substring("LINE", 267, 12) AS "POSTAL_CODE",
                substring("LINE", 279, 25) AS "CITY",
                substring("LINE", 304, 20) AS "STATE_PROVINCE",
                substring("LINE", 324, 24) AS "COUNTRY",
                substring("LINE", 348, 46) AS "CEO_NAME",
                substring("LINE", 394, 150) AS "DESCRIPTION"
            FROM staged.finwire_shared
            WHERE ("REC_TYPE" = 'CMP');

            CREATE OR REPLACE TABLE published.security AS
            SELECT "PTS",
                substring("LINE", 19, 15) AS "SYMBOL",
                substring("LINE", 34, 6) AS "ISSUE_TYPE",
                substring("LINE", 40, 4) AS "STATUS",
                substring("LINE", 44, 70) AS "NAME",
                substring("LINE", 114, 6) AS "EX_ID",
                substring("LINE", 120, 13) AS "SH_OUT",
                substring("LINE", 133, 8) AS "FIRST_TRADE_DATE",
                substring("LINE", 141, 8) AS "FIRST_EXCHANGE_DATE",
                substring("LINE", 149, 12) AS "DIVIDEND",
                substring("LINE", 161, 60) AS "CO_NAME_OR_CIK"
            FROM staged.finwire_shared
            WHERE ("REC_TYPE" = 'SEC');

            CREATE OR REPLACE TABLE published.financial AS
            SELECT "PTS",
                substring("LINE", 19, 4) AS "YEAR",
                substring("LINE", 23, 1) AS "QUARTER",
                substring("LINE", 24, 8) AS "QUARTER_START_DATE",
                substring("LINE", 32, 8) AS "POSTING_DATE",
                substring("LINE", 40, 17) AS "REVENUE",
                substring("LINE", 57, 17) AS "EARNINGS",
                substring("LINE", 74, 12) AS "EPS",
                substring("LINE", 86, 12) AS "DILUTED_EPS",
                substring("LINE", 98, 12) AS "MARGIN",
                substring("LINE", 110, 17) AS "INVENTORY",
                substring("LINE", 127, 17) AS "ASSETS",
                substring("LINE", 144, 17) AS "LIABILITIES",
                substring("LINE", 161, 13) AS "SH_OUT",
                substring("LINE", 174, 13) AS "DILUTED_SH_OUT",
                substring("LINE", 187, 60) AS "CO_NAME_OR_CIK"
            FROM staged.finwire_shared
            WHERE ("REC_TYPE" = 'FIN');
            
            -- grant to app role
            grant all on all tables in schema staged to application role app_public;
            grant all on all tables in schema published to application role app_public;
            return 'Pipeline successfully executed.';
        end;
    $$
    ;
    grant usage on procedure pipeline_code.run_pipeline()
    to application role app_public;

 
