-- Setup script for the Brokerage Silver application.
-- Create the application role
CREATE APPLICATION ROLE if not exists app_public;

-- access to other layers
create or alter versioned schema silver;

    create view silver.daily_market
    as select * from brokerage_gold_package.shared_silver.daily_market;

    create view silver.companies
    as select * from brokerage_gold_package.shared_silver.companies;

    create view silver.securities
    as select * from brokerage_gold_package.shared_silver.securities;

    create view silver.financials
    as select * from brokerage_gold_package.shared_silver.financials;

-- asset schema
create schema if not exists published;
    grant usage on schema published to application role app_public;


-- pipeline code
create or alter versioned schema pipeline_code;

    grant usage on schema pipeline_code to application role app_public;

    create or replace procedure pipeline_code.run_pipeline()
    returns varchar
    language sql
    as
    $$
        begin

            -- dim_company
            create or replace table published.dim_company
            as
            select
                md5(cast(coalesce(cast(company_id as TEXT), '_surrogate_key_null_') || '-' || coalesce(cast(effective_timestamp as TEXT), '_surrogate_key_null_') as TEXT)) sk_company_id,
                company_id,
                status,
                name,
                industry,
                ceo,
                address_line1,
                address_line2,
                postal_code,
                city,
                state_province,
                country,
                description,
                founding_date,
                sp_rating,
                case
                    when
                        sp_rating in (
                            'BB',
                            'B',
                            'CCC',
                            'CC',
                            'C',
                            'D',
                            'BB+',
                            'B+',
                            'CCC+',
                            'BB-',
                            'B-',
                            'CCC-'
                        )
                    then true
                    else false
                end as is_lowgrade,
                effective_timestamp,
                end_timestamp,
                is_current
            from silver.companies;

            -- dim_security
            create or replace table published.dim_security
            as
            with s1 as (
                select
                    symbol,
                    issue_type issue,
                    s.status,
                    s.name,
                    exchange_id,
                    sk_company_id,
                    shares_outstanding,
                    first_trade_date,
                    first_exchange_date,
                    dividend,
                    s.effective_timestamp,
                    s.end_timestamp,
                    s.IS_CURRENT
                from
                    silver.securities s
                join
                    published.dim_company c
                on 
                    s.company_id = c.company_id
                and
                    s.effective_timestamp between c.effective_timestamp and c.end_timestamp
            )
            select
                md5(cast(coalesce(cast(symbol as TEXT), '_surrogate_key_null_') || '-' || coalesce(cast(effective_timestamp as TEXT), '_surrogate_key_null_') as TEXT)) sk_security_id,
                *
            from
                s1;

            -- fact_market_history
            create or replace table published.fact_market_history
            as
            with s1 as (
            select
                sk_company_id,
                f.company_id,
                QUARTER_START_DATE,
                sum(eps) over (
                    partition by f.company_id
                    order by QUARTER_START_DATE
                    rows between 4 preceding and current row
                ) - eps sum_basic_eps
            from silver.financials f
            join published.dim_company c 
            on f.company_id = c.company_id
            and f.effective_timestamp between c.effective_timestamp and c.end_timestamp
            ) 
            SELECT
                s.sk_security_id,
                s.sk_company_id,
                dm_date sk_date_id,
                (s.dividend / dmh.dm_close) / 100 yield,
                fifty_two_week_high,
                fifty_two_week_high_date sk_fifty_two_week_high_date,
                fifty_two_week_low,
                fifty_two_week_low_date sk_fifty_two_week_low_date,
                dm_close closeprice,
                dm_high dayhigh,
                dm_low daylow,
                dm_vol volume
            FROM silver.daily_market dmh
            JOIN published.dim_security s
            ON s.symbol = dmh.dm_s_symb
                AND dmh.dm_date between s.effective_timestamp and s.end_timestamp
            LEFT JOIN s1 f
            USING (sk_company_id);

            -- report
            create or replace table published.rep_exchange
            as
            select
                *,
                rank() over (order by fifty_two_week_high_avg desc) fifty_two_week_high_rank,
                rank() over (order by fifty_two_week_low_avg) fifty_two_week_low_rank
            from
            (select distinct 
                exchange_id,
                avg(fifty_two_week_high) as fifty_two_week_high_avg,
                avg(fifty_two_week_low) as fifty_two_week_low_avg,
                avg(shares_outstanding) as shares_outstanding_avg
            from published.fact_market_history
            join published.dim_company using (sk_company_id)
            join published.dim_security using (sk_security_id)
            group by all);

            -- report
            create or replace table published.rep_sp_rating
            as
            select
                *,
                rank() over (order by fifty_two_week_high_avg desc) fifty_two_week_high_rank,
                rank() over (order by fifty_two_week_low_avg) fifty_two_week_low_rank
            from
            (select distinct 
                sp_rating,
                avg(fifty_two_week_high) as fifty_two_week_high_avg,
                avg(fifty_two_week_low) as fifty_two_week_low_avg,
                avg(shares_outstanding) as shares_outstanding_avg
            from published.fact_market_history
            join published.dim_company using (sk_company_id)
            join published.dim_security using (sk_security_id)
            group by all);
            
            -- grant to app role
            grant all on all tables in schema published to application role app_public;
            return 'Pipeline successfully executed.';
        end;
    $$
    ;
    grant usage on procedure pipeline_code.run_pipeline()
    to application role app_public;

create or alter versioned schema ui;

    CREATE STREAMLIT ui.exchange_analytics
    FROM '/streamlit'
    MAIN_FILE = '/exchange_analytics.py'
    ;

    grant usage on schema ui to application role app_public;
    GRANT USAGE ON STREAMLIT ui.exchange_analytics TO APPLICATION ROLE app_public;