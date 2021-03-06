  -- partition by Date to provide query optimization at the agency level
CREATE OR REPLACE TABLE
  `ReportViews.AgencyCreative`
PARTITION BY
  Date
CLUSTER BY
  Ad_Creative_Link_unique,
  data_source_name,
  ad_set_name__Facebook_Ads,
  Ad_Name__Facebook_ads OPTIONS (require_partition_filter = TRUE) AS
  -- view into pdm-funnel-export.Funnel_export_TEMPLATE_FACEBOOK_1_DAY_VIEW_28_DAY_CLICK.All_Data_Master
  -- this adds a unique create image url for each row
  -- based on the combination of client id, ad name, ad set name
WITH
  BASE AS (
    -- adds last good url based on Date
  SELECT
    Client_ID,
    adname,
    adsetname,
    FIRST_VALUE(url) -- url is non-null here
    OVER (PARTITION BY Client_ID, adname, adsetname ORDER BY Date DESC) last_good_url
  FROM (
      -- this select aliases columns for readability
      -- and applies not null conditions
    SELECT
      Client_ID,
      Ad_Name__Facebook_Ads AS `adname`,
      Ad_Set_Name__Facebook_Ads AS `adsetname`,
      Ad_Creative_Image_URL__Facebook_Ads AS `url`,
      Date,
    FROM
      `pdm-funnel-export.Funnel_export_TEMPLATE_FACEBOOK_1_DAY_VIEW_28_DAY_CLICK.All_Data_Master`
    WHERE
      Client_ID IS NOT NULL
      AND Ad_Name__Facebook_Ads IS NOT NULL
      AND Ad_Set_Name__Facebook_Ads IS NOT NULL
      AND Ad_Creative_Image_URL__Facebook_Ads IS NOT NULL ) ),
  UNIQUE_AD_CREATIVE AS (
    -- this produces 1 row per client_id/adname/adsetname
    -- choosing the url based on last good url
  SELECT
    Client_ID,
    adname,
    adsetname,
    ANY_VALUE(last_good_url) AS `Ad_Creative_Link_unique` -- all rows the same so any_value
  FROM
    BASE
  GROUP BY
    Client_ID,
    adname,
    adsetname )
SELECT
  a.Ad_Account_ID__Facebook_Ads,
  a.Ad_Creative_Asset_Ad_Formats__Facebook_Ads,
  a.Ad_Creative_Asset_Body_Texts__Facebook_Ads,
  a.Ad_Creative_Asset_Image_Hashes__Facebook_Ads,
  a.Ad_Creative_Asset_Links__Facebook_Ads,
  a.Ad_Creative_Asset_Title_Texts__Facebook_Ads,
  a.Ad_Creative_Image_Hash__Facebook_Ads,
  a.Ad_Creative_Image_Layer_Hash__Facebook_Ads,
  a.Ad_Creative_Image_URL__Facebook_Ads,
  a.Ad_Creative_Link__Facebook_Ads,
  a.Ad_Creative_Link_Call_To_Action_Type__Facebook_Ads,
  a.Ad_Creative_Link_Description__Facebook_Ads,
  a.Ad_Creative_Link_Message__Facebook_Ads,
  a.Ad_Creative_Link_Name__Facebook_Ads,
  a.Ad_Creative_Template_Link__Facebook_Ads,
  a.Ad_Creative_Thumbnail_URL__Facebook_Ads,
  a.Ad_Creative_URL_Parameters__Facebook_Ads,
  a.Ad_Creative_Video_Call_To_Action_Type__Facebook_Ads,
  a.Ad_Creative_Video_Data_Image_Hash__Facebook_Ads,
  a.Ad_Creative_Video_ID__Facebook_Ads,
  a.Ad_Creative_Video_Link__Facebook_Ads,
  a.Ad_Creative_Video_Link_Description__Facebook_Ads,
  a.Ad_Creative_Video_Message__Facebook_Ads,
  a.Ad_Creative_Video_Picture_URL__Facebook_Ads,
  a.Ad_Creative_Video_Title__Facebook_Ads,
  a.Ad_Effective_Status__Facebook_Ads,
  a.Ad_ID__Facebook_Ads,
  a.Ad_Name__Facebook_Ads,
  a.Ad_Preview_Shareable_Link__Facebook_Ads,
  a.Ad_set_Effective_Status__Facebook_Ads,
  a.Ad_Set_ID__Facebook_Ads,
  a.Ad_Set_Name__Facebook_Ads,
  SUM(a.Amount_Spent__Facebook_Ads) AS `Amount_Spent__Facebook_Ads`,
  a.Business_Type,
  a.Business_Type_ID,
  SUM(a.Button_Clicks__Facebook_Ads) AS `Button_Clicks__Facebook_Ads`,
  a.Buying_Type__Facebook_Ads,
  a.Campaign,
  a.Campaign_Effective_Status__Facebook_Ads,
  a.Campaign_ID__Facebook_Ads,
  a.Campaign_Name__Facebook_Ads,
  a.Campaign_Objective__Facebook_Ads,
  SUM(a.Clicks) AS `Clicks`,
  SUM(a.Clicks_all__Facebook_Ads) AS `Clicks_all__Facebook_Ads`,
  a.Client_ID,
  a.client_status,
  a.Content_Views__Facebook_Ads,
  a.Content_Views_Conversion_Value__Facebook_Ads,
  SUM(a.Cost) AS `Cost`,
  a.Creative_ID__Facebook_Ads,
  a.Creative_Object_Type__Facebook_Ads,
  a.Currency,
  a.Data_Origin_Identifier,
  a.Data_Source,
  a.Data_Source_definition_label,
  a.Data_Source_description,
  a.Data_Source_id,
  a.Data_Source_name,
  a.Data_Source_type,
  a.Data_Source_type_name,
  a.Date,
  a.Day_of_week,
  a.Device_Platform__Facebook_Ads,
  a.Funnel_Account_id,
  a.Funnel_Account_name,
  SUM(a.Impressions) AS `Impressions`,
  SUM(a.Impressions__Facebook_Ads) AS `Impressions__Facebook_Ads`,
  a.Industry,
  a.Industry_ID,
  a.Industry_OLD,
  SUM(a.Landing_Page_Views__Facebook_Ads) AS `Landing_Page_Views__Facebook_Ads`,
  SUM(a.Leads__Facebook_Ads) AS `Leads__Facebook_Ads`,
  SUM(a.Leads_Conversion_Value__Facebook_Ads) AS `Leads_Conversion_Value__Facebook_Ads`,
  SUM(a.Leads_Form__Facebook_Ads) AS `Leads_Form__Facebook_Ads`,
  a.Link__Facebook_Ads,
  SUM(a.Link_Clicks__Facebook_Ads) AS `Link_Clicks__Facebook_Ads`,
  a.Media_type,
  a.Month,
  a.Month_number,
  SUM(a.Outbound_Clicks__Facebook_Ads) AS `Outbound_Clicks__Facebook_Ads`,
  a.Paid_Social___Ad_Name_Level_Detail___Creative___TEMPLATE,
  a.Paid_Social___Ad_Name_Level_Detail___Headline_Copy___TEMPLATE,
  a.Paid_Social___Ad_Name_Level_Detail___Post_Launch___TEMPLATE,
  a.Paid_Social___Ad_Name_Level_Detail___Post_Type___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Age___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Audience_Identifier___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Campaign___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Gender___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Geo___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Objective___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Placement___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Target_Type___TEMPLATE,
  a.Paid_Social___Campaign_Level_Detail___Campaign_Name___TEMPLATE,
  a.Paid_Social___Campaign_Level_Detail___Department___TEMPLATE,
  a.Paid_Social___Campaign_Level_Detail___Funnel___TEMPLATE,
  a.Paid_Social___Campaign_Level_Detail___Objective___TEMPLATE,
  a.Paid_Social___Post_Copy___All_Copy___TEMPLATE,
  a.Paid_Social___Post_Copy___All_Copy_Combined___TEMPLATE,
  a.Paid_Social___Post_Copy___CTA___TEMPLATE,
  a.Paid_Social___Post_Copy___Description___TEMPLATE,
  a.Paid_Social___Post_Copy___Message___TEMPLATE,
  a.Paid_Social___Post_Copy___Name___TEMPLATE,
  SUM(a.Purchases__Facebook_Ads) AS `Purchases__Facebook_Ads`,
  SUM(a.Purchases_Conversion_Value__Facebook_Ads) AS `Purchases_Conversion_Value__Facebook_Ads`,
  SUM(a.Registrations_Completed__Facebook_Ads) AS `Registrations_Completed__Facebook_Ads`,
  SUM(a.Registrations_Completed_Conversion_Value__Facebook_Ads) AS `Registration_Completed_Conversion_Value__Facebook_Ads`,
  SUM(a.Video_Plays__Facebook_Ads) AS `Video_Plays__Facebook_Ads`,
  SUM(a.Video_thruplay__Facebook_Ads) AS `Video_thruplay__Facebook_Ads`,
  SUM(a.Video_Watches_at_100__Facebook_Ads) AS `Video_Watches_at_100__Facebook_Ads`,
  SUM(a.Video_Watches_at_25__Facebook_Ads) AS `Video_Watches_at_25__Facebook_Ads`,
  SUM(a.Video_Watches_at_50__Facebook_Ads) AS `Video_Watch_at_50__Facebook_Ads`,
  SUM(a.Video_Watches_at_75__Facebook_Ads) AS `Video_Watches_at_75__Facebook_Ads`,
  SUM(a.Video_Watches_at_95__Facebook_Ads) AS `Video_Watches_at_95__Facebook_Ads`,
  SUM(a.n_3_Second_Video_Views__Facebook_Ads) AS `n_3_Second_Video_Views__Facebook_Ads`,
  b.Ad_Creative_Link_unique
FROM
  `pdm-funnel-export.Funnel_export_TEMPLATE_FACEBOOK_1_DAY_VIEW_28_DAY_CLICK.All_Data_Master` a
LEFT OUTER JOIN
  UNIQUE_AD_CREATIVE b
ON
  b.adname = a.Ad_Name__Facebook_Ads
  AND b.adsetname = a.Ad_Set_Name__Facebook_Ads
  AND b.Client_ID = a.Client_ID
WHERE
  a.client_Id IS NOT NULL
  AND b.client_id IS NOT NULL
GROUP BY
  a.Ad_Account_ID__Facebook_Ads,
  a.Ad_Creative_Asset_Ad_Formats__Facebook_Ads,
  a.Ad_Creative_Asset_Body_Texts__Facebook_Ads,
  a.Ad_Creative_Asset_Image_Hashes__Facebook_Ads,
  a.Ad_Creative_Asset_Links__Facebook_Ads,
  a.Ad_Creative_Asset_Title_Texts__Facebook_Ads,
  a.Ad_Creative_Image_Hash__Facebook_Ads,
  a.Ad_Creative_Image_Layer_Hash__Facebook_Ads,
  a.Ad_Creative_Image_URL__Facebook_Ads,
  a.Ad_Creative_Link__Facebook_Ads,
  a.Ad_Creative_Link_Call_To_Action_Type__Facebook_Ads,
  a.Ad_Creative_Link_Description__Facebook_Ads,
  a.Ad_Creative_Link_Message__Facebook_Ads,
  a.Ad_Creative_Link_Name__Facebook_Ads,
  a.Ad_Creative_Template_Link__Facebook_Ads,
  a.Ad_Creative_Thumbnail_URL__Facebook_Ads,
  a.Ad_Creative_URL_Parameters__Facebook_Ads,
  a.Ad_Creative_Video_Call_To_Action_Type__Facebook_Ads,
  a.Ad_Creative_Video_Data_Image_Hash__Facebook_Ads,
  a.Ad_Creative_Video_ID__Facebook_Ads,
  a.Ad_Creative_Video_Link__Facebook_Ads,
  a.Ad_Creative_Video_Link_Description__Facebook_Ads,
  a.Ad_Creative_Video_Message__Facebook_Ads,
  a.Ad_Creative_Video_Picture_URL__Facebook_Ads,
  a.Ad_Creative_Video_Title__Facebook_Ads,
  a.Ad_Effective_Status__Facebook_Ads,
  a.Ad_ID__Facebook_Ads,
  a.Ad_Name__Facebook_Ads,
  a.Ad_Preview_Shareable_Link__Facebook_Ads,
  a.Ad_set_Effective_Status__Facebook_Ads,
  a.Ad_Set_ID__Facebook_Ads,
  a.Ad_Set_Name__Facebook_Ads,
  a.Business_Type,
  a.Business_Type_ID,
  a.Buying_Type__Facebook_Ads,
  a.Campaign,
  a.Campaign_Effective_Status__Facebook_Ads,
  a.Campaign_ID__Facebook_Ads,
  a.Campaign_Name__Facebook_Ads,
  a.Campaign_Objective__Facebook_Ads,
  a.Client_ID,
  a.client_status,
  a.Content_Views__Facebook_Ads,
  a.Content_Views_Conversion_Value__Facebook_Ads,
  a.Creative_ID__Facebook_Ads,
  a.Creative_Object_Type__Facebook_Ads,
  a.Currency,
  a.Data_Origin_Identifier,
  a.Data_Source,
  a.Data_Source_definition_label,
  a.Data_Source_description,
  a.Data_Source_id,
  a.Data_Source_name,
  a.Data_Source_type,
  a.Data_Source_type_name,
  a.Date,
  a.Day_of_week,
  a.Device_Platform__Facebook_Ads,
  a.Funnel_Account_id,
  a.Funnel_Account_name,
  a.Industry,
  a.Industry_ID,
  a.Industry_OLD,
  a.Link__Facebook_Ads,
  a.Media_type,
  a.Month,
  a.Month_number,
  a.Paid_Social___Ad_Name_Level_Detail___Creative___TEMPLATE,
  a.Paid_Social___Ad_Name_Level_Detail___Headline_Copy___TEMPLATE,
  a.Paid_Social___Ad_Name_Level_Detail___Post_Launch___TEMPLATE,
  a.Paid_Social___Ad_Name_Level_Detail___Post_Type___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Age___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Audience_Identifier___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Campaign___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Gender___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Geo___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Objective___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Placement___TEMPLATE,
  a.Paid_Social___Ad_Set_Level_Detail___Target_Type___TEMPLATE,
  a.Paid_Social___Campaign_Level_Detail___Campaign_Name___TEMPLATE,
  a.Paid_Social___Campaign_Level_Detail___Department___TEMPLATE,
  a.Paid_Social___Campaign_Level_Detail___Funnel___TEMPLATE,
  a.Paid_Social___Campaign_Level_Detail___Objective___TEMPLATE,
  a.Paid_Social___Post_Copy___All_Copy___TEMPLATE,
  a.Paid_Social___Post_Copy___All_Copy_Combined___TEMPLATE,
  a.Paid_Social___Post_Copy___CTA___TEMPLATE,
  a.Paid_Social___Post_Copy___Description___TEMPLATE,
  a.Paid_Social___Post_Copy___Message___TEMPLATE,
  a.Paid_Social___Post_Copy___Name___TEMPLATE,
  b.Ad_Creative_Link_unique