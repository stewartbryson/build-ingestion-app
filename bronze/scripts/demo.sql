PUT file:///Users/stewartbryson/dev/tpcdi-output/Batch1/DailyMarket.txt @brokerage_bronze.published.daily_market overwrite=true auto_compress=false;
PUT file:///Users/stewartbryson/dev/tpcdi-output/Batch1/FINWIRE1967Q1 @brokerage_bronze.staged.finwire overwrite=true auto_compress=false;
call brokerage_bronze.pipeline_code.ingest_files();
call brokerage_bronze.pipeline_code.run_pipeline();