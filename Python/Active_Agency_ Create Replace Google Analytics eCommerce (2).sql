  /* 
This is the datasource for the Pre/Post PDM view in the Analytics dataset.
last update: 11/18/21 by Mattan
*/
CREATE OR REPLACE TABLE
  `pdm-funnel-export.Agency.Google_Analytics_ecommerce`
PARTITION BY
  date
CLUSTER BY
  data_source_name,
  data_source_type_name AS
SELECT
  date,
  data_source_type_name,
  data_source_name,
  CAST(SUM(cost) AS float64) AS `Cost`,
  CAST(SUM(transactions__google_analytics) AS int64) AS `Transactions`,
  CAST(SUM(transaction_revenue__google_analytics) AS float64) AS `Revenue`,
  CAST(SUM(sessions__google_analytics) AS int64) AS `Sessions`,
  CAST(SUM(new_users__google_analytics) AS Int64) AS `New_Users`,
  SUM(bounces__Google_Analytics) AS `Bounces`,
  Client_ID,
  Industry,
  Default_channel_grouping__Google_analytics,
  Source__Medium___TEMPLATE AS `Source_Medium`,
  Source__Google_Analytics AS `Source`,
  Medium__Google_Analytics AS `Medium`,
  Channel_Grouping___TEMPLATE,
  Paid_Media___Template
FROM
  `Funnel_export_TEMPLATE_GA_STANDARD_PUBLISHER.All_Data_Master`
WHERE
  Client_ID IS NOT NULL
GROUP BY
  date,
  data_source_type_name,
  data_source_name,
  Client_ID,
  Industry,
  Default_channel_grouping__Google_Analytics,
  Source__Medium___TEMPLATE,
  Source,
  Medium,
  Channel_Grouping___TEMPLATE,
  Paid_Media___Template