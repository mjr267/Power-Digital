CREATE OR REPLACE TABLE
  `Regression.auto_regression`
PARTITION BY
  range_Bucket(client_ID,
    GENERATE_ARRAY(1000,5000,1))
CLUSTER BY
  client_name,
  data_source_type_name,
  date OPTIONS (require_partition_filter=TRUE) AS
WITH
  base AS(
  SELECT
    date,
    data_source_type_name,
    Source__Medium___TEMPLATE,
    IFNULL (REGEXP_EXTRACT(data_source_name,"(^.*?) -.*$"),
      data_source_name) AS Client_Name,
    client_ID,
    Cost,
    Clicks,
    Impressions,
    new_users__Google_Analytics AS `New_Users`,
    Platform_Conversions,
    Platform_Revenue,
    Transactions__Google_Analytics AS `Transactions`,
    Transaction_Revenue__Google_Analytics AS `Transaction_Revenue`
  FROM
    `Funnel_export_TEMPLATE_GA_STANDARD_PUBLISHER.All_Data_Master`
  WHERE
    client_id IS NOT NULL
    AND date BETWEEN DATE_SUB(CURRENT_DATE(),INTERVAL 2 YEAR)
    AND CURRENT_DATE() ),
  Channel AS (
  SELECT
    DISTINCT Source_medium,
    custom_channel_grouping_v2
  FROM
    `AnalyticsTeam.Channel_Grouping_Mapping` )
SELECT
  date,
  data_source_type_name,
  Client_Name,
  Client_ID,
  SUM(Cost) AS Cost,
  SUM(Clicks) AS Clicks,
  SUM(Impressions) AS Impressions,
  SUM(New_Users) AS New_Users,
  SUM(Platform_Conversions) AS Platform_Conversions,
  SUM(Platform_Revenue) AS Platfor_Revenue,
  SUM(Transactions) AS Transactions,
  SUM(Transaction_Revenue) AS Transaction_Revenue,
  ---
  Channel.custom_channel_grouping_v2
FROM
  base
LEFT OUTER JOIN
  Channel
ON
  base.Source__Medium___TEMPLATE = Channel.Source_medium
GROUP BY
  date,
  Channel.custom_channel_grouping_v2,
  data_source_type_name,
  Client_Name,
  Client_ID