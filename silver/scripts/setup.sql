-- Setup script for the Brokerage Silver application.
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
create schema if not exists static;

    grant usage on schema static to application role app_public;

    -- static tables
    create table if not exists static.STATUS_TYPE (
        ST_ID VARCHAR,
        ST_NAME VARCHAR
    );
    insert into static.status_type values ('ACTV','Active');
    insert into static.status_type values ('CMPT','Completed');
    insert into static.status_type values ('CNCL','Canceled');
    insert into static.status_type values ('PNDG','Pending');
    insert into static.status_type values ('SBMT','Submitted');
    insert into static.status_type values ('INAC','Inactive');

    create or replace TABLE static.INDUSTRY (
        IN_ID VARCHAR,
        IN_NAME VARCHAR,
        IN_SC_ID VARCHAR
    );
    insert into static.industry values ('AA','Misc. Capital Goods','FN');
    insert into static.industry values ('AC','Retail (Drugs)','TC');
    insert into static.industry values ('AD','Schools','FN');
    insert into static.industry values ('AE','Casinos & Gaming','CC');
    insert into static.industry values ('AM','Aerospace & Defense','BM');
    insert into static.industry values ('AP','Motion Pictures','SV');
    insert into static.industry values ('AR','Food Processing','HC');
    insert into static.industry values ('AT','Retail (Grocery)','BM');
    insert into static.industry values ('AV','Photography','TC');
    insert into static.industry values ('BA','Schools','BM');
    insert into static.industry values ('BC','Airline','TC');
    insert into static.industry values ('BD','Furniture & Fixtures','UT');
    insert into static.industry values ('BN','Computer Networks','FN');
    insert into static.industry values ('BS','Major Drugs','HC');
    insert into static.industry values ('CA','Healthcare Facilities','TR');
    insert into static.industry values ('CC','Railroads','TC');
    insert into static.industry values ('CD','Constr. - Supplies & Fixtures','SV');
    insert into static.industry values ('CE','Construction Services','TC');
    insert into static.industry values ('CF','Air Courier','CN');
    insert into static.industry values ('CG','Metal Mining','SV');
    insert into static.industry values ('CH','Fish/Livestock','FN');
    insert into static.industry values ('CK','Chemical Manufacturing','SV');
    insert into static.industry values ('CL','Computer Storage Devices','TR');
    insert into static.industry values ('CM','Constr. - Supplies & Fixtures','FN');
    insert into static.industry values ('CN','Printing & Publishing','TC');
    insert into static.industry values ('CO','Office Supplies','TR');
    insert into static.industry values ('CP','Metal Mining','SV');
    insert into static.industry values ('CR','Major Drugs','TC');
    insert into static.industry values ('CS','Real Estate Operations','SV');
    insert into static.industry values ('CT','S&Ls/Savings Banks','CC');
    insert into static.industry values ('CU','Auto & Truck Manufacturers','TC');
    insert into static.industry values ('CV','Natural Gas Utilities','SV');
    insert into static.industry values ('CX','Trucking','TC');
    insert into static.industry values ('DD','Construction Services','TR');
    insert into static.industry values ('DR','Biotechnology & Drugs','SV');
    insert into static.industry values ('EI','Retail (Grocery)','SV');
    insert into static.industry values ('EU','Retail (Catalog & Mail Order)','BM');
    insert into static.industry values ('FF','Beverages (Non-Alcoholic)','BM');
    insert into static.industry values ('FL','Retail (Catalog & Mail Order)','CN');
    insert into static.industry values ('FO','Computer Hardware','FN');
    insert into static.industry values ('FP','Crops','TC');
    insert into static.industry values ('FR','Money Center Banks','HC');
    insert into static.industry values ('FW','Conglomerates','CC');
    insert into static.industry values ('GR','Natural Gas Utilities','SV');
    insert into static.industry values ('GS','Auto & Truck Parts','TR');
    insert into static.industry values ('HF','Retail (Grocery)','EN');
    insert into static.industry values ('HI','Furniture & Fixtures','EN');
    insert into static.industry values ('HM','Computer Services','FN');
    insert into static.industry values ('IA','Jewelry & Silverware','UT');
    insert into static.industry values ('IL','Auto & Truck Manufacturers','SV');
    insert into static.industry values ('IM','Waste Management Services','TC');
    insert into static.industry values ('IP','Iron & Steel','TR');
    insert into static.industry values ('IS','Airline','BM');
    insert into static.industry values ('IV','Airline','SV');
    insert into static.industry values ('JS','Photography','SV');
    insert into static.industry values ('MC','Chemicals - Plastics & Rubber','HC');
    insert into static.industry values ('MD','Containers & Packaging','FN');
    insert into static.industry values ('ME','Constr. & Agric. Machinery','FN');
    insert into static.industry values ('MF','Rental & Leasing','SV');
    insert into static.industry values ('MG','Footwear','BM');
    insert into static.industry values ('MH','Communications Services','TC');
    insert into static.industry values ('MM','Retail (Drugs)','BM');
    insert into static.industry values ('MP','Misc. Capital Goods','CC');
    insert into static.industry values ('MS','Rental & Leasing','FN');
    insert into static.industry values ('MT','Photography','BM');
    insert into static.industry values ('NG','Biotechnology & Drugs','BM');
    insert into static.industry values ('NM','Water Utilities','FN');
    insert into static.industry values ('OE','Audio & Video Equipment','CN');
    insert into static.industry values ('OI','Oil Well Services & Equipment','SV');
    insert into static.industry values ('OO','Scientific & Technical Instr.','SV');
    insert into static.industry values ('OS','Water Transportation','TR');
    insert into static.industry values ('OW','Oil & Gas - Integrated','CN');
    insert into static.industry values ('PA','Regional Banks','HC');
    insert into static.industry values ('PG','Insurance (Miscellaneous)','BM');
    insert into static.industry values ('PH','Paper & Paper Products','SV');
    insert into static.industry values ('PP','Construction - Raw Materials','SV');
    insert into static.industry values ('PR','Tobacco','EN');
    insert into static.industry values ('PS','Beverages (Alcoholic)','HC');
    insert into static.industry values ('RA','Containers & Packaging','FN');
    insert into static.industry values ('RB','Jewelry & Silverware','CN');
    insert into static.industry values ('RE','Motion Pictures','UT');
    insert into static.industry values ('RL','Software & Programming','SV');
    insert into static.industry values ('RM','Office Equipment','CG');
    insert into static.industry values ('RN','Non-Metallic Mining','TC');
    insert into static.industry values ('RP','Audio & Video Equipment','FN');
    insert into static.industry values ('RR','Personal & Household Products','TR');
    insert into static.industry values ('RS','Insurance (Life)','SV');
    insert into static.industry values ('RT','Scientific & Technical Instr.','CC');
    insert into static.industry values ('SB','Money Center Banks','BM');
    insert into static.industry values ('SC','Broadcasting & Cable TV','FN');
    insert into static.industry values ('SM','Personal & Household Products','CC');
    insert into static.industry values ('SP','Recreational Products','EN');
    insert into static.industry values ('SS','Motion Pictures','CC');
    insert into static.industry values ('ST','Misc. Transportation','SV');
    insert into static.industry values ('TH','Auto & Truck Parts','EN');
    insert into static.industry values ('TI','Tires','HC');
    insert into static.industry values ('TN','Software & Programming','CN');
    insert into static.industry values ('TO','S&Ls/Savings Banks','SV');
    insert into static.industry values ('TR','Misc. Capital Goods','BM');
    insert into static.industry values ('WM','Insurance (Life)','CC');
    insert into static.industry values ('WT','Retail (Speciality)','EN');
    insert into static.industry values ('WU','Retail (Apparel)','CC');

    grant all on all tables in schema static to application role app_public;

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

            -- create the published tables
            create or replace table published.daily_market
            as
            SELECT *
            FROM reference('daily_market');
            
            -- grant to app role
            grant all on all tables in schema published to application role app_public;
            return 'Pipeline successfully executed.';
        end;
    $$
    ;
    grant usage on procedure pipeline_code.run_pipeline()
    to application role app_public;

 
