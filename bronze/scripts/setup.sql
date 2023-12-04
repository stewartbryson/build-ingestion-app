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

-- asset schema
create schema if not exists published;
grant usage on schema published to application role app_public;

-- load tables
create stage if not exists published.finwire directory = (enable = true)
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|');
create stage if not exists published.daily_market directory = (enable = true)
  FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|');
create table if not exists published.finwire (
    line varchar
);
grant all on table published.finwire to application role app_public;
create table if not exists published.daily_market (
	DM_DATE DATE,
	DM_S_SYMB VARCHAR(16777216),
	DM_CLOSE FLOAT,
	DM_HIGH FLOAT,
	DM_LOW FLOAT,
	DM_VOL FLOAT
);
grant all on table published.daily_market to application role app_public;

-- pipeline code
create or alter versioned schema pipeline_code;

    -- stored procedure to create a task that copies file data
    -- task creation must be deferred to after app install because
    -- it depends on a privilege being granted by the user via the UI
    create or replace procedure pipeline_code.create_ingestion_task()
    returns varchar
    language sql
    as
    $$
        begin
            system$log_info('creating task pipeline_code.ingest_files...');
            create or replace task pipeline_code.ingest_files
              warehouse = reference('pipeline_warehouse')
              schedule = '60 minute'
            as
            begin
                copy into published.finwire from @published.finwire;
                copy into published.daily_market from @published.daily_market;
            end;
        exception
            when other then
                system$log_error('create_ingestion_task(): ' || sqlerrm);
        end;
    $$
    ;
    grant usage on procedure pipeline_code.create_ingestion_task()
    to application role app_public;

    -- stored procedure to resume or suspend check_warnings_every_minute task
    -- this stored procedure will be called from the UI
    -- create or replace procedure pipeline_code.update_ingest_files_task_status(enable boolean)
    -- returns varchar
    -- language sql
    -- as
    -- $$
    --     begin
    --         if (enable) then
    --             system$log_info('starting ingest_files task');
    --             alter task if exists pipeline_code.ingest_files resume;
    --         else
    --             system$log_info('stopping ingest_files task');
    --             alter task if exists pipeline_code.ingest_files suspend;
    --         end if;
    --     exception
    --         when other then
    --             system$log_error('update_ingest_files_task_status(): ' || sqlerrm);
    --     end;
    -- $$
    -- ;

    -- grant usage on procedure pipeline_code.update_ingest_files_task_status()
    -- to application role app_public;
