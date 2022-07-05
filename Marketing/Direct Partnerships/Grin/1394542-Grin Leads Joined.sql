--MERGE INTO `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Leads_Combined` T
--USING(
    SELECT DISTINCT subscriber_id, subscriber_email, Datetime, A.Channel_Name, Plat , Budget_Date
    FROM(

        SELECT DISTINCT A.subscriber_id, A.subscriber_email, A.Datetime, TRIM(Channel_Name) AS Channel_Name, A.Plat
        FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Leads` A
        LEFT JOIN `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Leads` B ON A.subscriber_id = CAST(B.subscriber_id AS STRING)
        WHERE B.subscriber_id IS NULL

        UNION ALL

        SELECT DISTINCT CAST(A.subscriber_id AS STRING) AS subscriber_id, A.subscriber_email, A.Datetime, TRIM(Affid) AS Channel_Name, A.Plat
        FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Leads` A
        LEFT JOIN `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Leads` B ON B.subscriber_id = CAST(A.subscriber_id AS STRING)
        WHERE B.subscriber_id IS NULL

        UNION ALL

        SELECT DISTINCT CAST(A.subscriber_id AS STRING) AS subscriber_id, A.subscriber_email, A.Datetime, TRIM(Channel_Name) AS Channel_Name, 'Both' AS Plat
        FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Leads` A
        INNER JOIN `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Leads` B ON B.subscriber_id = CAST(A.subscriber_id AS STRING)
        WHERE B.subscriber_id IS NULL
        ) A
    LEFT JOIN (SELECT DISTINCT Influencer_Name AS Channel_Name,Budget, Planned_Date, Budget_Date, ToDate  FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_Budget_Ready_View`) B 
        ON IFNULL(A.Channel_Name,'xxxx')=IFNULL(B.Channel_Name,'xxxx')
    WHERE CAST(Datetime AS DATE) >= Budget_Date AND  CAST(Datetime AS DATE) <= ToDate
    AND A.Channel_Name IS NOT NULL
    /*) S ON IFNULL(T.subscriber_id,'zzz')= IFNULL(S.subscriber_id,'zzz') AND IFNULL(T.Channel_Name,'zzz')=IFNULL(S.Channel_Name,'zzz') AND T.Plat=S.Plat

WHEN MATCHED AND T.Budget_Date != S.Budget_Date OR T.Budget_Date IS NULL THEN UPDATE SET T.Budget_Date = S.Budget_Date


WHEN NOT MATCHED THEN INSERT (subscriber_id, subscriber_email, Datetime, Channel_Name, Plat , Budget_Date) VALUES (subscriber_id, subscriber_email, Datetime, Channel_Name, Plat , Budget_Date)

/**/