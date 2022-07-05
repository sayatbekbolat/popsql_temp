SELECT DISTINCT 
    TRIM(Channel_Name) AS Channel_Name,
    LOWER(REPLACE(REPLACE(TRIM(Channel_Name),' ','_'),'.','_')) AS content,
FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_Budgets_tbl`