/*
Here are links to help you get started with Stream Analytics Query Language:
Common query patterns - https://go.microsoft.com/fwLink/?LinkID=619153
Query language - https://docs.microsoft.com/stream-analytics-query/query-language-elements-azure-stream-analytics
*/
SELECT
    ticker,
    price,
    System.Timestamp() t,
    SUBSTRING(DATEADD(hour, -5, System.Timestamp()), 1, 10) + ' ' + SUBSTRING(DATEADD(hour, -5, System.Timestamp()), 12, 11) AS StRecievdTimeStamp,
    'Real Time' stream_type
INTO
    [nasdaq-stocks-rltm]
FROM
    [nasdaq-stock-eventhub]