CREATE OR REPLACE TABLE
  `pdm-funnel-export.Clients.ActiveClientData`
PARTITION BY
  date
CLUSTER BY
  data_source_name AS
WITH
  Adelphic AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_ADELPHIC.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Affiliate_Commission_Junction AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_AFFILIATE_COMMISSION_JUNCTION.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Affiliate_Impact_Radius AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_AFFILIATE_IMPACT_RADIUS.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Affiliate_Pepperjam AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_AFFILIATE_PEPPERJAM.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Affiliate_Rakuten_Affiliate_Network AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_AFFILIATE_RAKUTEN_AFFILIATE_NETWORK.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Affiliate_Shareasale AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_AFFILIATE_SHAREASALE.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Criteo AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_CRITEO.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Doubleclick_Campaign_Manager AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_DOUBLECLICK_CAMPAIGN_MANAGER.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Facebook_1_Day_View_28_Day_Click AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_FACEBOOK_1_DAY_VIEW_28_DAY_CLICK.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Linkedin_Standard AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_LINKEDIN_STANDARD.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Pinterest AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_PINTEREST.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  PPC_Ad_Level AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_PPC_AD_LEVEL.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Snapchat AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_SNAPCHAT.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Stackadapt AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_STACKADAPT.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Steelhouse AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_STEELHOUSE.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Tiktok AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_TIKTOK.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Tune_Network AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_TUNE_NETWORK.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  Twitter AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    ROUND(SUM(cost),2) AS cost,
    0 AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_TWITTER.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC),
  GoogleAnalytics AS (
  SELECT
    date,
    data_source_name,
    data_source_type_name,
    client_Id,
    0 AS cost,
    ROUND(SUM(Transaction_revenue__Google_Analytics),2) AS revenue
  FROM
    pdm-funnel-export.Funnel_export_TEMPLATE_GOOGLE_ANALYTICS_STANDARD.All_Data_Master
  WHERE
    (client_id IS NOT NULL
      OR client_id IS NULL)
  GROUP BY
    date,
    year,
    data_source_name,
    data_source_type_name,
    client_id
  ORDER BY
    data_source_name ASC,
    year ASC)
SELECT
  *
FROM (
  SELECT
    *
  FROM
    adelphic
  UNION ALL
  SELECT
    *
  FROM
    Affiliate_Commission_Junction
  UNION ALL
  SELECT
    *
  FROM
    Affiliate_Impact_Radius
  UNION ALL
  SELECT
    *
  FROM
    Affiliate_Pepperjam
  UNION ALL
  SELECT
    *
  FROM
    Affiliate_Rakuten_Affiliate_Network
  UNION ALL
  SELECT
    *
  FROM
    Affiliate_Shareasale
  UNION ALL
  SELECT
    *
  FROM
    Criteo
  UNION ALL
  SELECT
    *
  FROM
    Doubleclick_Campaign_Manager
  UNION ALL
  SELECT
    *
  FROM
    Facebook_1_Day_View_28_Day_Click
  UNION ALL
  SELECT
    *
  FROM
    Linkedin_Standard
  UNION ALL
  SELECT
    *
  FROM
    Pinterest
  UNION ALL
  SELECT
    *
  FROM
    PPC_Ad_Level
  UNION ALL
  SELECT
    *
  FROM
    Snapchat
  UNION ALL
  SELECT
    *
  FROM
    Stackadapt
  UNION ALL
  SELECT
    *
  FROM
    Steelhouse
  UNION ALL
  SELECT
    *
  FROM
    Tiktok
  UNION ALL
  SELECT
    *
  FROM
    Tune_Network
  UNION ALL
  SELECT
    *
  FROM
    Twitter
  UNION ALL
  SELECT
    *
  FROM
    GoogleAnalytics)