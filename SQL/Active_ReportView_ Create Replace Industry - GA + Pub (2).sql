CREATE OR REPLACE TABLE
  `pdm-funnel-export.ReportViews.IndustryView_GA_Pub`
PARTITION BY
  Date
CLUSTER BY
  Industry,
  Business_Type,
  Data_Source_Name,
  Channel_Grouping___Template OPTIONS (require_partition_filter = TRUE)AS
SELECT
  Data_source_name,
  Channel_Grouping___TEMPLATE,
  Campaign_Tactic___eCommerce___TEMPLATE,
  Business_Type,
  Business_Type_ID,
  Industry_OLD,
  Industry,
  CAST(Industry_ID AS INT64) AS `Industry_ID`,
  Device_category__Google_Analytics,
  ifnull(REGEXP_EXTRACT(Landing_Page__Google_Analytics,"(^.*?)\\?.*$"),
    Landing_Page__Google_Analytics) AS Landing_Page,
  date,
  Paid_Media___Template,
  SUM(Impressions) AS `Impressions`,
  SUM(Clicks) AS `Clicks`,
  SUM(Paid_Media___Transactions) AS `Paid_Media_Transactions`,
  SUM(Paid_Media___Revenue) AS `Paid_Media_Revenue`,
  SUM(Cost) AS `Cost`,
  SUM(Sessions__Google_Analytics) AS `Sessions`,
  SUM(Transaction_revenue__Google_Analytics) AS `Transaction_Revenue`,
  SUM(CAST(Transactions__Google_Analytics AS INT64)) AS `Transactions`,
  SUM( Platform_Conversions) AS `Platform_Conversions`,
  SUM( Platform_Revenue) AS `Platform_Revenue`,
  SUM(New_Users__Google_Analytics) AS `New_Users`,
  SUM(bounces__Google_Analytics) AS `Bounces`,
  SUM(pageviews__Google_Analytics) AS Pageviews,
  SUM(time_on_Page__google_analytics) AS Time_on_Page
FROM
  `pdm-funnel-export.Funnel_export_TEMPLATE_GA_STANDARD_PUBLISHER`.`All_Data_Master`
WHERE
  Client_ID IS NOT NULL
GROUP BY
  Data_source_name,
  Channel_Grouping___TEMPLATE,
  Campaign_Tactic___eCommerce___TEMPLATE,
  Business_Type,
  Business_Type_ID,
  Industry_OLD,
  Industry,
  Industry_ID,
  Device_category__Google_Analytics,
  Landing_Page,
  date,
  Paid_Media___Template