-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Initial Pull of Stocks History From ADLS Gen 2

-- COMMAND ----------

-- %python
-- path = "/mnt/stocks_adlsgen2/nasdaq-stocks-history-load"
-- stock_info_delta = spark.read.format("csv").option("InferSchema","true").option("header","true").load(path)
-- stock_info_delta.createOrReplaceTempView("STOCK_HISTORY_LOAD")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Initial Delta Table Definition Statement and History Load

-- COMMAND ----------

-- CREATE TABLE STOCK_AGG_DAY_LOAD(
--   created_date DATE,
--   open FLOAT,
--   high FLOAT,
--   low FLOAT,
--   close FLOAT,
--   ticker VARCHAR(10),
--   stock VARCHAR(50)
-- ) ;

-- INSERT INTO STOCK_AGG_DAY_LOAD(
--   created_date,
--   open,
--   high,
--   low,
--   close,
--   ticker,
--   stock
-- ) 
-- SELECT index as created_date,
--        open,
--        high,
--        low,
--        close,
--        ticker,
--        stock
--        FROM STOCK_HISTORY_LOAD;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Delta Table Update Load Query 

-- COMMAND ----------

MERGE INTO STOCK_AGG_DAY_LOAD AS TGT
USING (
  SELECT high_low_temp.st_recieved_date,
         open_temp.open,
         high_low_temp.high,
         high_low_temp.low,
         close_temp.close,
         high_low_temp.ticker,
         high_low_temp.stock
         FROM
         (
            SELECT max(PRICE) AS high,
                min(PRICE) AS low,
                ticker,
                stock,
                DATE(st_recieved_timestamp) AS st_recieved_date
                FROM 
                STOCKS_RLTM_LOAD 
                GROUP BY ticker,stock,DATE(st_recieved_timestamp)) AS high_low_temp
         LEFT JOIN (SELECT * FROM (
                      SELECT TICKER,
                      DATE(st_recieved_timestamp) AS st_recieved_date,
                      PRICE as open,
                      ROW_NUMBER() OVER(PARTITION BY TICKER,DATE(st_recieved_timestamp) ORDER BY st_recieved_timestamp) AS r_num
                      FROM STOCKS_RLTM_LOAD) WHERE r_num = 1) AS open_temp
          ON (open_temp.ticker = high_low_temp.ticker AND open_temp.st_recieved_date = high_low_temp.st_recieved_date)
          LEFT JOIN (SELECT * FROM (
                      SELECT TICKER,
                      DATE(st_recieved_timestamp) AS st_recieved_date,
                      PRICE as close,
                      ROW_NUMBER() OVER(PARTITION BY TICKER,DATE(st_recieved_timestamp) ORDER BY st_recieved_timestamp DESC) AS r_num
                      FROM STOCKS_RLTM_LOAD) WHERE r_num = 1) AS close_temp
          ON (close_temp.ticker = high_low_temp.ticker AND close_temp.st_recieved_date = high_low_temp.st_recieved_date)) AS delta_load_temp
          ON (TGT.created_date = delta_load_temp.st_recieved_date AND TGT.ticker = delta_load_temp.ticker)
          WHEN MATCHED THEN
          UPDATE SET 
            TGT.created_date = delta_load_temp.st_recieved_date,
            TGT.open = delta_load_temp.open,
            TGT.high = delta_load_temp.high,
            TGT.low = delta_load_temp.low,
            TGT.close = delta_load_temp.close,
            TGT.ticker = delta_load_temp.ticker,
            TGT.stock = delta_load_temp.stock
          WHEN NOT MATCHED THEN 
          INSERT(TGT.created_date,
                 TGT.open,
                 TGT.high,
                 TGT.low,
                 TGT.close,
                 TGT.ticker,
                 TGT.stock)
          VALUES(delta_load_temp.st_recieved_date,
                 delta_load_temp.open,
                 delta_load_temp.high,
                 delta_load_temp.low,
                 delta_load_temp.close,
                 delta_load_temp.ticker,
                 delta_load_temp.stock)


-- COMMAND ----------


