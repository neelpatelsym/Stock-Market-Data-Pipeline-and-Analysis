# Stock Market Data Pipline and Analysis

A stock market data pipeline with Azure Event hub, Azure Stream Analytics, ADLS Gen 2, Azure Cosmos DB, Azure Databricks, Key Vault, Power-BI and much more!

## Description

### Objective

The project involves streaming real-time Nasdaq stock market data from Finance APIs and establishing a data pipeline using Lambda architecture. This pipeline will consume the live data, perform transformations, and visualize real-time stock market prices on a Power BI dashboard. The data processing occurs in both real-time for immediate analytics and in batch mode for daily scheduled jobs. The daily batch jobs focus on computing aggregates to facilitate predictive analytics for making informed decisions regarding portfolio investments.

### Data Ingestion and Real time processing

Real-time data is introduced into Azure Event Hub through a Python script, which extracts information from Yahoo Finance APIs at 30-second intervals. Through the Azure Stream Analytics Service, the real-time data undergoes transformation to include an ingested datetime stamp and is subsequently deposited into Azure Cosmos DB, functioning as the Online Transaction Processing (OLTP) database in this project. The Databricks workflow, STOCKS_RLTM_PULL_LOAD_WF_RUNS, retrieves data from this OLTP database, performs necessary processing, and conducts Change Data Capture (CDC) to load delta records into the STOCKS_RLTM_LOAD Databricks Hive Metastore table. Data in real-time is extracted from this table and presented on a Power BI dashboard.

### Batch Processing

The Databricks workflow STOCK_BATCH_DAILY_LOAD retrieves information from the STOCKS_RLTM_LOAD table. It performs the aggregation of stock metrics and subsequently loads the data into the STOCK_AGG_DAY_LOAD table. This table serves as a source for extracting data to conduct predictive investment analysis. 

An additional workflow, STOCK_INFO_LOAD, captures stock-related details such as Market Cap, Enterprise Value EBITDA, P/E ratio, Price Book MRQ, etc., from Yahoo Finance APIs. The extracted data undergoes a cleaning process before being transferred to Azure Data Lake Storage Gen 2 (ADLS Gen 2). Subsequently, the data is processed and stored in the STOCK_INFO_LOAD table. This critical stock information is then visualized alongside real-time data.

### Data Visualization

![Real time stock analysis dashboard snip](https://github.com/neelpdesai/Stock-Market-Data-Pipeline-and-Analysis/assets/137664550/209a618f-273b-493c-b1b6-af45499c6d21)


### Tools & Technologies

- Cloud - [**Microsoft Azure**](https://azure.microsoft.com/)
- Stock APIs - [**Yahoo Fin**](https://developer.yahoo.com/api/)
- Stream Ingestion - [**Azure Event Hub**]
- Stream Processing - [**Azure Stream Analytics**]
- Online Transaction Processing (OLTP) - [**Azure Cosmos DB**]
- Orchestration - [**Azure Databricks**]
- Transformation - [**Azure Databricks**]
- Data Lake - [**Azure Data Lake Gen2**]
- Data Warehouse - [**Azure Databricks Hive Metastore**]
- Data Visualization - [**Power BI**]
- Language - [**Python**](https://www.python.org)

### Architecture
The Stock Market Data Pipeline is structured around the λ (lambda) architecture, comprising distinct layers for efficient data processing. The pipeline encompasses an ingestion layer, batch layer, speed layer, serving layer, and analytics layer. The diagram below illustrates the flow and interaction between these layers.
![Architecture](https://github.com/neelpdesai/Stock-Market-Data-Pipeline-and-Analysis/assets/137664550/08c76e58-5869-4e0a-a18c-09275dfd8a30)
