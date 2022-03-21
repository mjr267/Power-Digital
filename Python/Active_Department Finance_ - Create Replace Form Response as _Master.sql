CREATE OR REPLACE TABLE
  `Department_Finance.Variable_Ad_Spend_Form_Responses_Master` AS
SELECT
  CAST(Timestamp AS date) AS `Date_Form_Response`,
  CAST(Email_Address AS STRING) AS `Email_Address_Form_Response`,
  CAST(TRIM(What_Is_The_Name_Of_The_Client) AS STRING) AS `Client_Name_Form_Response`,
  Tier_1,
  Tier_2,
  Tier_3,
  Tier_4,
  Tier_5,
  Tier_6,
  Tier_7,
  Tier_8,
  Tier_9,
  Tier_10,
  CAST(Is_this_customer_on_the_tiered_or___of_spend_model_ AS STRING) AS `orig_Model_Type_Form_Response`,
  CAST (Is_There_A_Minimum_Paid_Fee__If__Yes___Enter_The_Value AS FLOAT64) AS `Minimum_Fee_Form_Response`,
  CAST (I_Agree_To_Append___Exclude__To_All_Campaigns_That_Should_Not_Be_Included_For_Billing_Purposes AS STRING) AS `Append_Agreement_Form_Response`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_1,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_1_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_1,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_1_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_1,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_1_Max`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_2,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_2_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_2,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_2_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_2,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_2_Max`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_3,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_3_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_3,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_3_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_3,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_3_Max`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_4,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_4_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_4,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_4_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_4,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_4_Max`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_5,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_5_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_5,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_5_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_5,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_5_Max`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_6,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_6_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_6,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_6_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_6,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_6_Max`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_7,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_7_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_7,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_7_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_7,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_7_Max`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_8,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_8_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_8,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_8_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_8,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_8_Max`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_9,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_9_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_9,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_9_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_9,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_9_Max`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_10,"\\%",""),"(^.*?)-.*?-.*$") AS float64) / 100 AS `Tier_10_Percent`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_10,"\\%",""),"^.*?-(.*?)-.*$") AS float64) AS `Tier_10_Min`,
  CAST(REGEXP_EXTRACT(REGEXP_REPLACE(Tier_10,"\\%",""),"^.*?-.*?-(.*$)") AS float64) AS `Tier_10_Max`,
  CASE
    WHEN Is_this_customer_on_the_tiered_or___of_spend_model_ LIKE '%Tiered%' THEN 'Tiered'
    WHEN Is_this_customer_on_the_tiered_or___of_spend_model_ LIKE '%of Spend%' THEN '% Of Spend'
    
    else Is_this_customer_on_the_tiered_or___of_spend_model_
END
  AS `Model_Type`
FROM
  `Department_Finance.Variable_Ad_Spend_Form_Responses`