WITH Influcer_budget_base AS (
  SELECT  IF(Type='LTB','LTB',TRIM(Channel_Name)) AS Channel_Name, 
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
      SELECT AC.Channel_Name, AC.Budget_Date AS Action_Date, A.Budget_Date, ToDate, Fee_Type, AC.Budget_Date BETWEEN A.Budget_Date AND ToDate AS in_Range, action_cost,AC.AcctMgr
      FROM Action_Cost AC
      LEFT JOIN Influencer_budget_ready_data A ON AC.Channel_Name=A.Channel_Name AND AC.Budget_Date>=A.Budget_Date AND AC.Budget_Date<=A.ToDate
      ORDER BY 1,3,2 
)        
,Influencers_Inc_Action_cost As (

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


/**/