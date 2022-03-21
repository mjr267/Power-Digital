CREATE OR REPLACE TABLE
  `pdm-funnel-export.ReportViews.IndustryView_Operating_System`
PARTITION BY
  Date
CLUSTER BY
  Industry,
  Business_Type,
  Data_Source_Name,
  Default_channel_grouping__Google_Analytics OPTIONS (require_partition_filter = TRUE) AS
SELECT
  Date,
  Data_Source_Name,
  Business_Type,
  Business_Type_ID,
  Industry_OLD,
  Industry,
  CAST(Industry_ID AS INT64) AS `Industry_ID`,
  Data_Source_type_name,
  Client_ID,
  Default_channel_grouping__Google_Analytics,
  Operating_system__Google_Analytics,
  Operating_system_version__Google_Analytics,
  Device_category__Google_Analytics,
  Browser__Google_Analytics,
  SUM(Sessions__Google_Analytics) AS Sessions__Google_Analytics,
  SUM(New_users__Google_Analytics) AS New_users__Google_Analytics,
  SUM(Transactions__Google_Analytics) AS Transactions__Google_Analytics,
  SUM(Transaction_revenue__Google_Analytics) AS Transaction_revenue__Google_Analytics
FROM
  `pdm-funnel-export.Funnel_export_TEMPLATE_GOOGLE_ANALYTICS_OPERATING_SYSTEM.All_Data_Master`
WHERE
  client_id IS NOT NULL
GROUP BY
  Date,
  Business_Type,
  Business_Type_ID,
  Industry_OLD,
  Industry,
  Industry_ID,
  Data_Source_type_name,
  Client_ID,
  Default_channel_grouping__Google_Analytics,
  Operating_system__Google_Analytics,
  Operating_system_version__Google_Analytics,
  Device_category__Google_Analytics,
  Browser__Google_Analytics,
  Data_Source_Name