-- Setup script for the Brokerage Silver application.
-- Create the application role
CREATE APPLICATION ROLE if not exists app_public;

-- access to other layers
create or alter versioned schema bronze;

    create view bronze.daily_market
    as select * from brokerage_silver_package.shared_bronze.daily_market;

    create view bronze.company
    as select * from brokerage_silver_package.shared_bronze.company;

    create view bronze.security
    as select * from brokerage_silver_package.shared_bronze.security;

    create view bronze.financial
    as select * from brokerage_silver_package.shared_bronze.financial;

-- share reference data
create or alter versioned schema reference;

    create or replace view reference.status_type
    as select * from brokerage_silver_package.shared_reference.status_type;

    create or replace view reference.industry
    as select * from brokerage_silver_package.shared_reference.industry;


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

            -- companies
            create or replace table published.companies
            as
            select
                cik as company_id,
                st.st_name status,
                company_name name,
                ind.in_name industry,
                ceo_name ceo,
                address_line1,
                address_line2,
                postal_code,
                city,
                state_province,
                country,
                description,
                founding_date,
                sp_rating,
                pts as effective_timestamp,
                ifnull(
                    timestampadd(
                        'millisecond',
                        -1,
                        lag(pts) over (
                            partition by company_id
                            order by
                            pts desc
                        )
                    ),
                    to_timestamp('9999-12-31 23:59:59.999')
                ) as end_timestamp,
                CASE
                    WHEN (
                        row_number() over (
                            partition by company_id
                            order by
                            pts desc
                        ) = 1
                    ) THEN TRUE
                    ELSE FALSE
                END as IS_CURRENT
            from bronze.company cmp
            join reference.status_type st on cmp.status = st.st_id
            join reference.industry ind on cmp.industry_id = ind.in_id;

            -- daily_market
            create or replace table published.daily_market
            as
            with
            s1 as (
                select
                    -- dm_date,
                    min(dm_low) over (
                        partition by dm_s_symb
                        order by dm_date asc
                        rows between 364 preceding and 0 following  -- CURRENT ROW
                    ) fifty_two_week_low,
                    max(dm_high) over (
                        partition by dm_s_symb
                        order by dm_date asc
                        rows between 364 preceding and 0 following  -- CURRENT ROW
                    ) fifty_two_week_high,
                    *
                from bronze.daily_market
            ),
            s2 as (
                select a.*, 
                    b.dm_date as fifty_two_week_low_date, 
                    c.dm_date as fifty_two_week_high_date
                from s1 a
                join
                    s1 b
                    on a.dm_s_symb = b.dm_s_symb
                    and a.fifty_two_week_low = b.dm_low
                    and b.dm_date between add_months(a.dm_date, -12) and a.dm_date
                join
                    s1 c
                    on a.dm_s_symb = c.dm_s_symb
                    and a.fifty_two_week_high = c.dm_high
                    and c.dm_date between add_months(a.dm_date, -12) and a.dm_date
            )
            select *
            from s2
            qualify
                row_number() over (
                    partition by dm_s_symb, dm_date
                    order by fifty_two_week_low_date, fifty_two_week_high_date
                ) = 1;

            -- financials
            create or replace table published.financials
            as
            with s1 as (
                select
                    YEAR,
                    QUARTER,
                    QUARTER_START_DATE,
                    POSTING_DATE,
                    REVENUE,
                    EARNINGS,
                    EPS,
                    DILUTED_EPS,
                    MARGIN,
                    INVENTORY,
                    ASSETS,
                    LIABILITIES,
                    SH_OUT,
                    DILUTED_SH_OUT,
                    coalesce(c1.name,c2.name) company_name,
                    coalesce(c1.company_id, c2.company_id) company_id,
                    pts as effective_timestamp
                from bronze.financial s 
                left join published.companies c1
                on s.cik = c1.company_id
                and pts between c1.effective_timestamp and c1.end_timestamp
                left join published.companies c2
                on s.company_name = c2.name
                and pts between c2.effective_timestamp and c2.end_timestamp
            )
            select
                *,
                ifnull(
                    timestampadd(
                    'millisecond',
                    -1,
                    lag(effective_timestamp) over (
                        partition by company_id
                        order by
                        effective_timestamp desc
                    )
                    ),
                    to_timestamp('9999-12-31 23:59:59.999')
                ) as end_timestamp,
                CASE
                    WHEN (
                        row_number() over (
                            partition by company_id
                            order by
                            effective_timestamp desc
                        ) = 1
                    ) THEN TRUE
                    ELSE FALSE
                END as IS_CURRENT
            from s1;

            -- securities
            create or replace table published.securities
            as
            select
                symbol,
                issue_type,
                case s.status
                    when 'ACTV' then 'Active'
                    when 'INAC' then 'Inactive'
                    else null
                end status,
                s.name,
                ex_id exchange_id,
                sh_out shares_outstanding,
                first_trade_date,
                first_exchange_date,
                dividend,
                coalesce(c1.name,c2.name) company_name,
                coalesce(c1.company_id, c2.company_id) company_id,
                pts as effective_timestamp,
                ifnull(
                    timestampadd(
                    'millisecond',
                    -1,
                    lag(pts) over (
                        partition by symbol
                        order by
                        pts desc
                    )
                    ),
                    to_timestamp('9999-12-31 23:59:59.999')
                ) as end_timestamp,
                CASE
                    WHEN (
                        row_number() over (
                            partition by symbol
                            order by
                            pts desc
                        ) = 1
                    ) THEN TRUE
                    ELSE FALSE
                END as IS_CURRENT
            from bronze.security s 
            left join published.companies c1
            on s.cik = c1.company_id
            and pts between c1.effective_timestamp and c1.end_timestamp
            left join published.companies c2
            on s.company_name = c2.name
            and pts between c2.effective_timestamp and c2.end_timestamp;
            
            -- grant to app role
            grant all on all tables in schema published to application role app_public;
            return 'Pipeline successfully executed.';
        end;
    $$
    ;
    grant usage on procedure pipeline_code.run_pipeline()
    to application role app_public;

 
