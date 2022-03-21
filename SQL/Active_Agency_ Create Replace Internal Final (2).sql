 -- Creates date partioned table to ensure query optimizations
CREATE OR REPLACE TABLE
  `pdm-funnel-export.Agency.internal-final`
PARTITION BY
  date
CLUSTER BY
  Client_Name,
  Default_Channel_Grouping__Google_Analytics,
  Opportunity_Stage AS
  -- Pulling in Revenue & Default Channel Grouping from GA: Standard dataset
SELECT
  date,
  -- This consolidates clients into a single name for situations where they have multiple variations
  (CASE
      WHEN REGEXP_EXTRACT(Data_Source_Name, "(.*?)-.*$") IS NULL THEN Data_Source_Name
    ELSE
    REGEXP_EXTRACT(Data_Source_Name, "(.*?)-.*$")
  END
    ) AS `Client_Name`,
  Stage___Official__Salesforce_Opportunities AS `Opportunity_Stage`,
  SUM(Number_of_Leads__Salesforce_Leads) AS `Number_of_Leads`,
  SUM(Number_of_Opportunities__Salesforce_Opportunities) AS `Number_of_Opportunities`,
  CAST(NULL AS string) AS `Default_Channel_Grouping__Google_Analytics`,
  SUM(CAST(NULL AS int64)) AS `Transaction_Revenue__Google_Analytics`,
  MIN(CAST(Client_ID AS int64)) AS `Client_ID`,
  Industry,
  Business_Type,
  SUM(CAST( NULL AS Float64)) AS `Amazon_Revenue`
FROM
  `pdm-funnel-export.Funnel_export_TEMPLATE_SALESFORCE_PUBLISHER.All_Data_Master` a
WHERE
  client_id IS NOT NULL
GROUP BY
  date,
  Client_Name,
  Opportunity_Stage,
  Default_Channel_Grouping__Google_Analytics,
  industry,
  Business_Type
UNION ALL
  -- Pulling in Leads,Opportunities, Opportunity Stage from SalesForce dataset
SELECT
  date,
  -- This consolidates clients into a single name for situations where they have multiple variations
  (CASE
      WHEN REGEXP_EXTRACT(Data_Source_Name, "(.*?)-.*$") IS NULL THEN Data_Source_Name
    ELSE
    REGEXP_EXTRACT(Data_Source_Name, "(.*?)-.*$")
  END
    ) AS `Client_Name`,
  CAST(NULL AS string) AS `Opportunity_Stage`,
  SUM(NULL) AS `Number_of_Leads`,
  SUM(NULL) AS `Number_of_Opportunities`,
  Default_Channel_Grouping__Google_Analytics,
  SUM(Transaction_Revenue__Google_Analytics) AS `Transaction_Revenue__Google_Analytics`,
  MIN(CAST(Client_ID AS int64)) AS `Client_ID`,
  industry,
  Business_Type,
  SUM(CAST( NULL AS Float64)) AS `Amazon_Revenue`
FROM
  `pdm-funnel-export.Funnel_export_TEMPLATE_GOOGLE_ANALYTICS_STANDARD.All_Data_Master` b
WHERE
  client_id IS NOT NULL
GROUP BY
  date,
  Client_Name,
  Opportunity_Stage,
  Default_Channel_Grouping__Google_Analytics,
  industry,
  Business_Type
UNION ALL
  -- Pulling in Amazon Revenue from Amazon dataset. Data starts as of 6/1/20201
SELECT
  Date AS `date`,
  ProfileName AS `Client_Name`,
  CAST(NULL AS string) AS `Opportunity_Stage`,
  SUM(NULL) AS `Number_of_Leads`,
  SUM(NULL) AS `Number_of_Opportunities`,
  CAST(NULL AS string) AS `Default_Channel_Grouping__Google_Analytics`,
  CAST(NULL AS Float64) AS `Transaction_Revenue__Google_Analytics`,
  MIN(CAST(NULL AS int64)) AS `Client_ID`,
  CAST(NULL AS string) AS `industry`,
  CAST(NULL AS string) AS `Business_Type`,
  SUM(CAST( Sales14d AS Float64)) AS `Amazon_Revenue`
FROM
  `pdm-funnel-export.AmazonPacvue.Report_Type_Campaign` c
WHERE
  profileID IS NOT NULL
GROUP BY
  date,
  Client_Name,
  Opportunity_Stage,
  Default_Channel_Grouping__Google_Analytics,
  industry,
  Business_Type