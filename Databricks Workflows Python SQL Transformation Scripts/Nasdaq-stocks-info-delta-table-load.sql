-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Initial Delta Table Definition Statement

-- COMMAND ----------

-- CREATE TABLE STOCK_INFO_LOAD (
--   market_cap_intraday VARCHAR(10),
--   enterprise_value VARCHAR(10),
--   trailing_PE FLOAT,
--   forward_PE FLOAT,
--   PEG_ratio_5_yr_expected FLOAT,
--   price_sales_ttm FLOAT,
--   price_book_mrq FLOAT,
--   enterprise_value_revenue FLOAT,
--   enterprise_value_EBITDA FLOAT,
--   ticker VARCHAR(10),
--   stock VARCHAR(50),
--   created_date DATE
-- )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Pulling Stock Information From ADLS Gen 2 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC path = "/mnt/stocks_adlsgen2/nasdaq-stocks-info-ingest/stock_info.csv"
-- MAGIC stock_info_delta = spark.read.format("csv").option("InferSchema","true").option("header","true").load(path)
-- MAGIC stock_info_delta.createOrReplaceTempView("STOCK_INFO_DELTA")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Delta Table Update Load Query 

-- COMMAND ----------

MERGE INTO STOCK_INFO_LOAD AS TGT
USING (SELECT * FROM STOCK_INFO_DELTA) AS TEMP
ON (TGT.created_date = TEMP.date AND TGT.ticker = TEMP.ticker)
WHEN MATCHED THEN UPDATE SET 
 TGT.market_cap_intraday = TEMP.Market_Cap_intraday,
 TGT.enterprise_value = TEMP.Enterprise_Value,
 TGT.trailing_PE = TEMP.Trailing_PE,
 TGT.forward_PE = TEMP.Forward_PE,
 TGT.PEG_ratio_5_yr_expected = TEMP.PEG_Ratio_5_yr_expected,
 TGT.price_sales_ttm = TEMP.PriceSales_ttm,
 TGT.price_book_mrq = TEMP.PriceBook_mrq,
 TGT.enterprise_value_revenue = TEMP.Enterprise_ValueRevenue,
 TGT.enterprise_value_EBITDA = TEMP.Enterprise_ValueEBITDA,
 TGT.ticker = TEMP.ticker,
 TGT.stock = TEMP.stock,
 TGT.created_date = TEMP.date
WHEN NOT MATCHED THEN INSERT (
 TGT.market_cap_intraday,
 TGT.enterprise_value,
 TGT.trailing_PE,
 TGT.forward_PE,
 TGT.PEG_ratio_5_yr_expected,
 TGT.price_sales_ttm,
 TGT.price_book_mrq,
 TGT.enterprise_value_revenue,
 TGT.enterprise_value_EBITDA,
 TGT.ticker,
 TGT.stock,
 TGT.created_date
)
VALUES (
  TEMP.Market_Cap_intraday,
  TEMP.Enterprise_Value,
  TEMP.Trailing_PE,
  TEMP.Forward_PE,
  TEMP.PEG_Ratio_5_yr_expected,
  TEMP.PriceSales_ttm,
  TEMP.PriceBook_mrq,
  TEMP.Enterprise_ValueRevenue,
  TEMP.Enterprise_ValueEBITDA,
  TEMP.ticker,
  TEMP.stock,
  TEMP.date
)

-- COMMAND ----------


