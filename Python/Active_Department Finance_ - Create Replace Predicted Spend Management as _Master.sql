CREATE OR REPLACE TABLE
  `Department_Finance.Variable_Ad_Spend_Predicted_Spend_Management_Master` AS
SELECT
  trim(Funnel_Name) AS `Client_Name_Predicted`,
  CAST(Next_Month_Ad_Spend AS FLOAT64) AS `Next_Month_Ad_Spend`,
  CAST(Next_Month_Management_Fee AS FLOAT64) AS `Next_Month_Management_Fee`,
  CAST(Management_Fee___Model_Value AS FLOAT64) AS `Management_Fee___Model_Value`,
  CAST(Commission_Spend_Threshold AS STRING) AS `Commission_Spend_Threshold`
FROM
  `Department_Finance.Variable_Ad_Spend_Predicted_Spend_Management`