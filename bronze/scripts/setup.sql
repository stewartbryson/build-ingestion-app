-- Setup script for the Brokerage Bronze application.
-- Create the application role
CREATE APPLICATION ROLE if not exists app_public;

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

            -- company
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

            -- security
            CREATE OR REPLACE TABLE published.security AS
            with s as (
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
                WHERE ("REC_TYPE" = 'SEC')
            ),
            s1 as (
                select *,
                try_to_number(co_name_or_cik) as try_cik
                from s
            )
            select  
                pts,
                symbol,
                issue_type,
                status,
                name,
                ex_id,
                to_number(sh_out) as sh_out,
                to_date(first_trade_date,'yyyymmdd') as first_trade_date,
                to_date(first_exchange_date,'yyyymmdd') as first_exchange_date,
                cast(dividend as float) as dividend,
                try_cik cik,
                case when try_cik is null then co_name_or_cik else null end company_name
            from s1;

            -- financial
            CREATE OR REPLACE TABLE published.financial AS
            with s as (
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
                WHERE ("REC_TYPE" = 'FIN')
            ),
            s1 as (
                select 
                    *,
                    try_to_number(co_name_or_cik) as try_cik
                from s
            )
            select 
                pts,
                to_number(year) as year,
                to_number(quarter) as quarter,
                to_date(quarter_start_date,'yyyymmdd') as quarter_start_date,
                to_date(posting_date,'yyyymmdd') as posting_date,
                cast(revenue as float) as revenue,
                cast(earnings as float) as earnings,
                cast(eps as float) as eps,
                cast(diluted_eps as float) as diluted_eps,
                cast(margin as float) as margin,
                cast(inventory as float) as inventory,
                cast(assets as float) as assets,
                cast(liabilities as float) as liabilities,
                to_number(sh_out) as sh_out,
                to_number(diluted_sh_out) as diluted_sh_out,
                try_cik cik,
                case when try_cik is null then co_name_or_cik else null end company_name
            from s1;
            
            -- grant to app role
            grant all on all tables in schema staged to application role app_public;
            grant all on all tables in schema published to application role app_public;
            return 'Pipeline successfully executed.';
        end;
    $$
    ;
    grant usage on procedure pipeline_code.run_pipeline()
    to application role app_public;

 
