WITH Influencer_budget_base AS (
    SELECT  TRIM(Channel_Name) AS Details, 
        Planned_Month,
        Planned_Year, 
        CAST(TRIM(REPLACE(REPLACE(Total_budget,'$',''),',','')) AS FLOAT64) AS Total_budget,
        PARSE_DATE('%Y%B%d',CONCAT(Planned_Year,Planned_Month,'01')) Planned_Date, 
        Acct_Mgr AS AcctMgr,
        IF(Live_date IS NOT NULL,PARSE_DATE('%F',Live_date),CAST(NULL AS DATE)) AS Live_date,
        CASE 
            WHEN Live_date IS NOT NULL THEN 'DATE'
            WHEN LOWER(Status) LIKE ('%pending%') THEN 'Pending'
            ELSE 'TEXT......'
        END AS correct_date,
        'Flat' AS Fee_Type,
    FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_Budgets_tbl`             
    WHERE Channel_Name IS NOT NULL 
        AND Channel_Name NOT LIKE '%Budget%'
 )

, Influencer_budget_ready_data AS (
    SELECT Influencer_Name, Budget, AcctMgr, Planned_Date, Budget_Date, Fee_Type,
            CASE
                WHEN Fee_Type = 'Commission Only' THEN DATE_SUB(DATE_ADD(DATE_TRUNC(Budget_Date,MONTH),INTERVAL 1 MONTH),INTERVAL 1 DAY)
                ELSE IFNULL(LEAD(DATE_SUB(Budget_Date,INTERVAL 1 DAY), 1) OVER (  PARTITION BY Influencer_Name  ORDER BY Budget_Date ASC ),DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))
            END AS ToDate,          
    FROM (
        SELECT  Details AS Influencer_Name,Planned_Month,Planned_Year,SUM(Total_budget) As Budget,Planned_Date, AcctMgr,Fee_Type,
                CASE 
                    WHEN correct_date = 'DATE' THEN Live_date
                    ELSE DATE_SUB(DATE_ADD(DATE_TRUNC(Planned_Date,Month),INTERVAL 1 MONTH),INTERVAL 1 DAY)   
                END AS Budget_Date
        FROM Influencer_budget_base
        GROUP BY 1,2,3,5,6,7,8
        ORDER BY 1,2,5
        )
    WHERE Budget_Date > '2022-05-01'
    ORDER BY 1,2,5
    )

--, Action_Cost AS (
    SELECT DISTINCT
        DATE_TRUNC(CAST(date AS DATE),MONTH) AS Planned_Date, 
        CAST(date AS DATE) AS Budget_Date,
        IF( Influence_Name IS NULL,impact_media , Influence_Name ) AS Influencer_Name,
        impact_action_cost AS action_cost, B.IR_Media_Name,
        AcctMgr
    FROM `gusa-funnel-dwh.funnel_dwh.funnel_data_*` A
    LEFT JOIN `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming_LOC` B ON impact_media = IR_Media_Name
    WHERE impact_type IS NOT NULL
        AND date >= '2019-01-01'
        AND impact_action_cost > 0
        AND B.IR_Media_Name IS NOT NULL
        --AND A.IR_Media_Name = 'IknowLee'
        --AND impact_media LIKE ('%Dan%')
    ORDER BY 1 DESC
/*)

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



/**/