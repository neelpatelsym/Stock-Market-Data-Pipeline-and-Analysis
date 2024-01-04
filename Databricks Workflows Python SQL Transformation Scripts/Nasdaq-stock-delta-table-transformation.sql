-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Initial Delta Table Definition Statement

-- COMMAND ----------

-- CREATE TABLE STOCKS_RLTM_LOAD (
--  ticker VARCHAR(25),
--  stock VARCHAR(50),
--  price FLOAT,
--  stream_type VARCHAR(25),
--  st_recieved_timestamp TIMESTAMP
-- )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Delta Table Update Load Query

-- COMMAND ----------

MERGE INTO STOCKS_RLTM_LOAD AS TGT
USING (SELECT STKS_PL.*,STKS_TIK_INF.stock FROM STOCK_PULL AS STKS_PL
       JOIN STOCKS_TICKER_NAME_INFO AS STKS_TIK_INF 
       ON STKS_PL.ticker = STKS_TIK_INF.ticker) AS TEMP 
       ON (TGT.ticker = TEMP.ticker AND TGT.st_recieved_timestamp = TEMP.StRecievdTimeStamp)
       WHEN MATCHED THEN UPDATE SET
       TGT.TICKER = TEMP.TICKER,
       TGT.STOCK = TEMP.STOCK,
       TGT.PRICE = TEMP.PRICE,
       TGT.STREAM_TYPE = TEMP.STREAM_TYPE,
       TGT.ST_RECIEVED_TIMESTAMP = CAST(TEMP.StRecievdTimeStamp AS TIMESTAMP)
       WHEN NOT MATCHED THEN INSERT(
        TGT.TICKER,
        TGT.STOCK,
        TGT.PRICE,
        TGT.STREAM_TYPE,
        TGT.ST_RECIEVED_TIMESTAMP
       ) 
       VALUES(
        TEMP.TICKER,
        TEMP.STOCK,
        TEMP.PRICE,
        TEMP.STREAM_TYPE,
        CAST(TEMP.StRecievdTimeStamp AS TIMESTAMP)
       )


-- COMMAND ----------


