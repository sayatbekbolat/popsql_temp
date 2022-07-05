--------------------------/ INSERT NEW Date /------------------------------
INSERT INTO `gusa-dc-dwh.IR_data.IR_Leads` ( oid, action_date, promo_code, revenue, click_date, ref_type, media_id, media, subid1, subid2, subid3, subid4, ip_address, ref_url)
(
/*WITH Last_Date AS (SELECT MAX(CAST(CAST(action_date AS DATETIME) AS DATE)) max_date FROM `gusa-dc-dwh.IR_data.IR_Leads`)
,file_to_use AS ( SELECT  DISTINCT FILE_NAME 
  FROM ( SELECT PARSE_DATE('%Y%m%d', SUBSTR(REGEXP_EXTRACT(CAST(_FILE_NAME AS STRING), '([0-9]+)'),1,8)) AS File_Date,_FILE_NAME AS FILE_NAME FROM `gusa-dc-dwh.IR_data.Leads_Temp` ), Last_Date
  WHERE File_Date >= DATE_ADD(max_date, INTERVAL 2 DAY)
)  */

SELECT DISTINCT oid, action_date, promo_code, revenue, click_date, ref_type, media_id, media, subid1, subid2, subid3, subid4, ip_address, ref_url
FROM `gusa-dc-dwh.IR_data.Leads_Temp` WHERE oid NOT IN (SELECT DISTINCT oid FROM `gusa-dc-dwh.IR_data.IR_Leads`)
)
;

INSERT INTO `gusa-dc-dwh.IR_data.IR_Sales` ( oid, action_date, promo_code, revenue, click_date, ref_type, media_id, media, subid1, subid2, subid3, subid4, ip_address, ref_url)
(
/*WITH Last_Date AS (SELECT MAX(CAST(CAST(action_date AS DATETIME) AS DATE)) max_date FROM `gusa-dc-dwh.IR_data.IR_Sales`)
,file_to_use AS (
  SELECT  DISTINCT FILE_NAME 
  FROM ( SELECT PARSE_DATE('%Y%m%d', SUBSTR(REGEXP_EXTRACT(CAST(_FILE_NAME AS STRING), '([0-9]+)'),1,8)) AS File_Date,_FILE_NAME AS FILE_NAME FROM `gusa-dc-dwh.IR_data.Sales_Temp` ), Last_Date
  WHERE File_Date >= DATE_ADD(max_date, INTERVAL 2 DAY)
)  */

SELECT DISTINCT oid, action_date, promo_code, revenue, click_date, ref_type, media_id, media, subid1, subid2, subid3, subid4, ip_address, ref_url
FROM `gusa-dc-dwh.IR_data.Sales_Temp` WHERE oid NOT IN (SELECT DISTINCT oid FROM `gusa-dc-dwh.IR_data.IR_Sales`)
)
/**/
;


------------------------- / Direct_Marketing Budget / -------------------------------------
DELETE FROM `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Loc`  WHERE Platform IS NOT NULL;
INSERT INTO `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Loc` ( Platform, Type, Details, Website, Total_budget, Planned_Month, Planned_Year, Date, Timeline, Acct__Mgr_, Fee, Concept, Status, Affid, IR_name, Notes, string_field_16 )
(
SELECT Platform, Type,TRIM(Details) AS Details, Website, Total_budget, Planned_Month, Planned_Year, PARSE_DATE('%Y%B%d',CONCAT(Planned_Year,Planned_Month,'01')) AS Date,Timeline, Acct__Mgr_, Fee, Concept, Status, Affid, IR_name, Notes, string_field_16 
FROM `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Budget` 
--WHERE Planned_Month is not null AND Planned_Month ='November' AND Planned_Year = 2020
)

------------------------- / IR_Influencers_Naming_LOC / -------------------------------------
;
DELETE FROM `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming_LOC` WHERE Influence_Name IS NOT NULL;
INSERT INTO `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming_LOC` (Influence_Name, Affid, IR_Media_Name,AcctMgr)
(
SELECT TRIM(Influence_Name) AS Influence_Name, Affid, IR_Media_Name,D_Aff	 AS AcctMgr
FROM `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming`
)


------------------------- / Influencer_Budget_Ready / -------------------------------------
;
DELETE FROM `gusa-dc-dwh.Influencers_Data.Influencer_Budget_Ready_new` WHERE Influencer_Name IS NOT NULL;
INSERT INTO `gusa-dc-dwh.Influencers_Data.Influencer_Budget_Ready_new` (Influencer_Name,Budget,AcctMgr,Planned_Date, Budget_Date, ToDate,Fee_Type,action_cost)
(
WITH Influcer_budget_base AS (
  SELECT  IF(Type='LTB','LTB',TRIM(Details)) AS Details, 
          Planned_Month,
          Planned_Year , 
          TRIM(Timeline) AS Timeline ,
          Total_budget,
          PARSE_DATE('%Y%B%d',CONCAT(Planned_Year,Planned_Month,'01')) Planned_Date, 
          IF(Type='LTB','LTB',Acct__Mgr_) AS AcctMgr,
          SPLIT(TRIM(Timeline), '.')[safe_ordinal(1)] AS dday,
          SPLIT(TRIM(Timeline), '.')[safe_ordinal(2)] AS dmonth,
          SPLIT(TRIM(Timeline), '.')[safe_ordinal(3)] AS dyear,
          CASE 
            WHEN LENGTH(TRIM(Timeline))<=8 THEN 'DATE'
            WHEN LOWER(Timeline) LIKE ('%pending%') THEN 'Pending'
            ELSE 'TEXT......'
           END AS correct_date,
           CASE 
            WHEN LOWER(fee) LIKE ('%revsh%') AND LOWER(fee) LIKE ('%flat%') THEN "Flat + Commission"
            WHEN LOWER(fee) LIKE ('%comm%') AND LOWER(fee) LIKE ('%flat%') THEN "Flat + Commission"
            WHEN LOWER(fee) LIKE ('%comm%') OR  LOWER(fee) LIKE ('%revsh%') THEN "Commission Only"  
            WHEN LOWER(fee) LIKE ('%flat%') THEN 'Flat'
            ELSE 'check'
          END AS Fee_Type
  FROM `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Loc`               
  WHERE LOWER(Type) IN ('influencers','ltb') 
  --AND Planned_Month ='November' AND Planned_Year = 2020
  -- AND Acct__Mgr_ = 'Lisa' AND Total_budget = 0   

  --    AND Details IN ('faceovermatter','Jelian Mercado','Stephanie Soo') -- 'Jelian Mercado',Stephanie Soo
 )

, Influencer_budget_ready_data AS (
  SELECT Influencer_Name, Budget, AcctMgr, Planned_Date, CAST(Budget_Date AS DATE) Budget_Date, Fee_Type,
         CASE
           WHEN Fee_Type = 'Commission Only' THEN DATE_SUB(DATE_ADD(DATE_TRUNC(Budget_Date,MONTH),INTERVAL 1 MONTH),INTERVAL 1 DAY)
           ELSE IFNULL(LEAD(DATE_SUB(Budget_Date,INTERVAL 1 DAY), 1) OVER (  PARTITION BY Influencer_Name  ORDER BY Budget_Date ASC ),DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))
         END AS ToDate,          
  FROM (
        SELECT  Details AS Influencer_Name,Planned_Month,Planned_Year ,Timeline,SUM(Total_budget) As Budget,Planned_Date, AcctMgr,Fee_Type,
                CASE 
                  WHEN correct_date = 'DATE' THEN  PARSE_DATE('%y-%m-%d',CONCAT(CAST(dyear AS INT64),'-',CAST(dmonth AS INT64),'-',CAST(dday AS INT64)))
                  ELSE DATE_SUB(DATE_ADD(DATE_TRUNC(Planned_Date,Month),INTERVAL 1 MONTH),INTERVAL 1 DAY)
                END  AS Budget_Date
        FROM Influcer_budget_base
        GROUP BY 1,2,3,4,6,7,8,9
        ORDER BY 1,2,6
      )
  WHERE Budget_Date > '2017-01-01'
  ORDER BY 1,2,6
  --AND Influencer_Name = 'IknowLee'
)

, Action_Cost AS (
  SELECT DISTINCT
    DATE_TRUNC(CAST(date AS DATE),MONTH) AS Planned_Date, 
    CAST(date AS DATE) AS Budget_Date,
    IF( Influence_Name IS NULL,impact_media , Influence_Name ) AS Influencer_Name,
    impact_action_cost AS action_cost, B.IR_Media_Name,
    AcctMgr
  FROM `gusa-funnel-dwh.funnel_dwh.funnel_data_*` A
  LEFT JOIN `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming_LOC` B
  ON impact_media = IR_Media_Name
  WHERE impact_type IS NOT NULL
  AND date >= '2019-01-01'
  AND impact_action_cost > 0
  AND B.IR_Media_Name IS NOT NULL
  --AND A.IR_Media_Name = 'IknowLee'
  --AND impact_media LIKE ('%Dan%')
  ORDER BY 1 DESC
)
, Extra_cost AS (
      SELECT AC.Influencer_Name, AC.Budget_Date AS Action_Date, A.Budget_Date, ToDate, Fee_Type, AC.Budget_Date BETWEEN A.Budget_Date AND ToDate AS in_Range, action_cost,AC.AcctMgr
      FROM Action_Cost AC
      LEFT JOIN Influencer_budget_ready_data A ON AC.Influencer_Name=A.Influencer_Name AND AC.Budget_Date>=A.Budget_Date AND AC.Budget_Date<=A.ToDate
      --AND A.Influencer_Name='IknowLee'
    --  GROUP BY 1,2,3
      --HAVING action_cost > 0
      ORDER BY 1,3,2 
)        
,Influencers_Inc_Action_cost As (

  SELECT Influencer_Name,Budget,AcctMgr,Planned_Date, IF(Fee_Type='Commission Only',Planned_Date,Budget_Date) As Budget_Date, ToDate,Fee_Type,IFNULL(action_cost,0) AS action_cost
  FROM Influencer_budget_ready_data
  LEFT JOIN (SELECT Influencer_Name,Budget_Date,ToDate,SUM(action_cost) action_cost FROM Extra_cost WHERE in_Range = TRUE GROUP BY 1,2,3) USING(Influencer_Name,Budget_Date,ToDate) 
  --WHERE Influencer_Name = 'bookslikewhoa' 
  ORDER BY 1
)

, Action_cost_comission_only AS (
  SELECT 
    Influencer_Name,
    0.0 AS Budget,
    AcctMgr,
    DATE_TRUNC(Action_Date,MONTH) AS Planned_Date,
    DATE_TRUNC(Action_Date,MONTH) AS Budget_Date, 
    "Commission Only"  AS Fee_Type,    
    SUM(action_cost) AS action_cost
  FROM Extra_cost AC
  WHERE in_Range IS NULL
  GROUP BY 1,2,3,4,5
  
)        

SELECT Influencer_Name,Budget,AcctMgr,Planned_Date, Budget_Date,
       CASE
          WHEN Fee_Type = 'Commission Only' THEN DATE_SUB(DATE_ADD(DATE_TRUNC(Budget_Date,MONTH),INTERVAL 1 MONTH),INTERVAL 1 DAY)
          ELSE IFNULL(LEAD(DATE_SUB(Budget_Date,INTERVAL 1 DAY), 1) OVER (  PARTITION BY Influencer_Name  ORDER BY Budget_Date ASC ),DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))
       END AS ToDate,Fee_Type,action_cost
     
FROM (
SELECT * FROM Action_cost_comission_only
  
 UNION ALL 

SELECT Influencer_Name,Budget,AcctMgr,Planned_Date, Budget_Date, Fee_Type,action_cost FROM Influencers_Inc_Action_cost
)
  )
;

-----------------/ Aff Spend fixed / -------------------------

DELETE FROM `gusa-funnel-dwh.Direct_Marketing.Aff_Fixed_Spend_GSC_LOC` WHERE Platform IS NOT NULL;

INSERT INTO `gusa-funnel-dwh.Direct_Marketing.Aff_Fixed_Spend_GSC_LOC` (Platform, Type, Partner, Website, Total_budget, Planned_Month, Planned_Year, Affid, Partner_name, Note, Date)
SELECT Platform, Type, Partner, Website, Total_budget, Planned_Month, Planned_Year, Affid, Partner_name, Note,PARSE_DATE('%Y%B%d',CONCAT(Planned_Year,Planned_Month,'01')) AS Date
FROM `gusa-funnel-dwh.Direct_Marketing.Aff_Fixed_Spend_GSC` 
WHERE Partner	IS NOT NULL


;
-----------------/ Aff Naming / -------------------------
DELETE FROM `gusa-funnel-dwh.Direct_Marketing.Aff_Naming_Loc` WHERE Partner IS NOT NULL;
INSERT INTO `gusa-funnel-dwh.Direct_Marketing.Aff_Naming_Loc` ( Partner, IR_Partner_Name, Type, GA_Medium, Affid)

SELECT
  string_field_0 AS Partner,
  string_field_1 As IR_Partner_Name,
  string_field_2 AS Type,
  string_field_3 AS GA_Medium,
  string_field_4 AS Affid
FROM `gusa-funnel-dwh.Direct_Marketing.Aff_Naming`
WHERE string_field_0 IS NOT NULL
;

----------- / Google Analytics Medium to Partner/Influencer /----------------------------

DELETE FROM `gusa-funnel-dwh.Direct_Marketing.Influencer_GA_Naming` WHERE Name IS NOT NULL;
INSERT INTO `gusa-funnel-dwh.Direct_Marketing.Influencer_GA_Naming` (Name,Medium,Type)
 (
        SELECT Influence_Name AS Name, D_name AS Medium, 'Influencer' AS Type
        FROM `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming` 
        WHERE D_name IS NOT NULL
        UNION ALL
        SELECT Partner AS Name, IF(GA_Medium IS NULL,IR_Partner_Name,GA_Medium) AS Medium , 'Affiliate' AS Type
        FROM `gusa-funnel-dwh.Direct_Marketing.Aff_Naming_Loc`
        
 ) 
;

-------------Inf Process 2022---------------
DELETE FROM `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Budget_Loc_2022`  WHERE Platform IS NOT NULL;
INSERT INTO `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Budget_Loc_2022` (Channel_Name, Platform, Type, Channel, Total_Budget, Planned_Month, Planned_Year, Status, Date, Timeline, Notes, Acct__Mgr_, Fee, Contract_Status, Affid, IR_Name)
(
SELECT Channel_Name, Platform, Type, Channel, Total_Budget, Planned_Month, Planned_Year, Status, PARSE_DATE('%Y%B%d',CONCAT(Planned_Year,Planned_Month,'01')) AS Date, Timeline, Notes, Acct__Mgr_, Fee, Contract_Status, Affid, IR_Name 
FROM `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Budget_2022` 
);


DELETE FROM `gusa-dc-dwh.Influencers_Data.Influencer_Budget_Ready_2022` WHERE Influencer_Name IS NOT NULL;
INSERT INTO `gusa-dc-dwh.Influencers_Data.Influencer_Budget_Ready_2022` (Influencer_Name,Budget,AcctMgr,Planned_Date, Budget_Date, ToDate,Fee_Type,action_cost)
(
  WITH Influcer_budget_base AS (
    SELECT IF(Type='LTB','LTB',TRIM(Channel_Name)) AS Channel_Name, 
      Planned_Month,
      Planned_Year , 
      Status,
      Timeline,
      Total_budget,
      Date AS Planned_Date, 
      IF(Type='LTB','LTB',Acct__Mgr_) AS AcctMgr,
      CASE 
        WHEN LOWER(Status) = 'live' THEN 'DATE'
        WHEN LOWER(Status) LIKE '%pending%' THEN 'Pending'
        ELSE 'TEXT......'
      END AS correct_date,
      CASE 
        WHEN LOWER(fee) LIKE ('%revsh%') AND LOWER(fee) LIKE ('%flat%') THEN "Flat + Commission"
        WHEN LOWER(fee) LIKE ('%comm%') AND LOWER(fee) LIKE ('%flat%') THEN "Flat + Commission"
        WHEN LOWER(fee) LIKE ('%comm%') OR  LOWER(fee) LIKE ('%revsh%') THEN "Commission Only"  
        WHEN LOWER(fee) LIKE ('%flat%') THEN 'Flat'
        ELSE 'check'
      END AS Fee_Type
    FROM `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Budget_Loc_2022`               
    WHERE LOWER(Type) IN ('influencers','ltb') 
  )

  , Influencer_budget_ready_data AS (
    SELECT Channel_Name, Budget, AcctMgr, Planned_Date, CAST(Budget_Date AS DATE) Budget_Date, Fee_Type,
      CASE
        WHEN Fee_Type = 'Commission Only' THEN DATE_SUB(DATE_ADD(DATE_TRUNC(Budget_Date,MONTH),INTERVAL 1 MONTH),INTERVAL 1 DAY)
        ELSE IFNULL(LEAD(DATE_SUB(Budget_Date,INTERVAL 1 DAY), 1) OVER ( PARTITION BY Channel_Name  ORDER BY Budget_Date ASC ),DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))
      END AS ToDate,          
    FROM (
      SELECT  Channel_Name,Planned_Month,Planned_Year,Timeline,SUM(Total_budget) As Budget,Planned_Date, AcctMgr,Fee_Type,
              CASE 
                WHEN correct_date = 'DATE' THEN Timeline
                ELSE DATE_SUB(DATE_ADD(DATE_TRUNC(Planned_Date,Month),INTERVAL 1 MONTH),INTERVAL 1 DAY)
              END  AS Budget_Date
      FROM Influcer_budget_base
      GROUP BY 1,2,3,4,6,7,8,9
      ORDER BY 1,2,6
      )
    WHERE Budget_Date > '2017-01-01'
    ORDER BY 1,2,6
    )

  , Action_Cost AS (
    SELECT DISTINCT
      DATE_TRUNC(CAST(date AS DATE),MONTH) AS Planned_Date, 
      CAST(date AS DATE) AS Budget_Date,
      IF( Influence_Name IS NULL,impact_media , Influence_Name ) AS Channel_Name,
      impact_action_cost AS action_cost, B.IR_Media_Name,
      AcctMgr
    FROM `gusa-funnel-dwh.funnel_dwh.funnel_data_*` A
    LEFT JOIN `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming_LOC` B ON impact_media = IR_Media_Name
    WHERE impact_type IS NOT NULL
      AND date >= '2019-01-01'
      AND impact_action_cost > 0
      AND B.IR_Media_Name IS NOT NULL
    ORDER BY 1 DESC
    )

  , Extra_cost AS (
      SELECT AC.Channel_Name, AC.Budget_Date AS Action_Date, A.Budget_Date, ToDate, Fee_Type, AC.Budget_Date BETWEEN A.Budget_Date AND ToDate AS in_Range, action_cost,AC.AcctMgr
      FROM Action_Cost AC
      LEFT JOIN Influencer_budget_ready_data A ON AC.Channel_Name=A.Channel_Name AND AC.Budget_Date>=A.Budget_Date AND AC.Budget_Date<=A.ToDate
      ORDER BY 1,3,2 
    )        

  , Influencers_Inc_Action_cost As (
    SELECT Channel_Name,Budget,AcctMgr,Planned_Date, IF(Fee_Type='Commission Only',Planned_Date,Budget_Date) As Budget_Date, ToDate,Fee_Type,IFNULL(action_cost,0) AS action_cost
    FROM Influencer_budget_ready_data
    LEFT JOIN (SELECT Channel_Name,Budget_Date,ToDate,SUM(action_cost) action_cost FROM Extra_cost WHERE in_Range = TRUE GROUP BY 1,2,3) USING(Channel_Name,Budget_Date,ToDate) 
    ORDER BY 1
    )

  , Action_cost_comission_only AS (
    SELECT 
      Channel_Name,
      0.0 AS Budget,
      AcctMgr,
      DATE_TRUNC(Action_Date,MONTH) AS Planned_Date,
      DATE_TRUNC(Action_Date,MONTH) AS Budget_Date, 
      "Commission Only"  AS Fee_Type,    
      SUM(action_cost) AS action_cost
    FROM Extra_cost AC
    WHERE in_Range IS NULL
    GROUP BY 1,2,3,4,5
    )        

  SELECT Channel_Name AS Influencer_Name,Budget,AcctMgr,Planned_Date, Budget_Date,
    CASE
      WHEN Fee_Type = 'Commission Only' THEN DATE_SUB(DATE_ADD(DATE_TRUNC(Budget_Date,MONTH),INTERVAL 1 MONTH),INTERVAL 1 DAY)
      ELSE IFNULL(LEAD(DATE_SUB(Budget_Date,INTERVAL 1 DAY), 1) OVER (  PARTITION BY Channel_Name  ORDER BY Budget_Date ASC ),DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))
    END AS ToDate,Fee_Type,action_cost
      
  FROM (
    SELECT * FROM Action_cost_comission_only
    
    UNION ALL 

    SELECT Channel_Name AS Influencer_Name,Budget,AcctMgr,Planned_Date, Budget_Date, Fee_Type,action_cost FROM Influencers_Inc_Action_cost
    )
  ORDER BY 4,5
);






/**/