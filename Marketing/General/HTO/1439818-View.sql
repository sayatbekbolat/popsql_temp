/*MERGE INTO `gusa-funnel-dwh.HTO_Data.HTO_All_Sales_Cohort_30D` T
USING(*/
    WITH DB AS (
        SELECT
            T.backendVisitorId,
            Date,
            T.Datetime,
            T.Media,
            T.Campaign_Type,
            T.campaign,
            T.adContent,
            T.keyword,
            B.Datetime AS t_Date,
            B.Media AS t_Media,
            B.Campaign_Type AS t_Campaign_Type,
            B.campaign AS t_campaign,
            B.adContent AS t_adContent,
            B.keyword AS t_keyword,
            transactionId,
            isHTO,
            DATE_DIFF(
                MAX(Date) OVER (PARTITION BY T.backendVisitorId,transactionId),
                MIN(Date) OVER (PARTITION BY T.backendVisitorId,transactionId),
                DAY) AS Days_To_Conversion,
            FIRST_VALUE(T.Media) OVER (PARTITION BY T.backendVisitorId,transactionId ORDER BY T.Datetime) AS First_Media,
            FIRST_VALUE(T.Media) OVER (PARTITION BY T.backendVisitorId,transactionId ORDER BY T.Datetime DESC) AS Last_Media,
            FIRST_VALUE(T.campaign) OVER (PARTITION BY T.backendVisitorId,transactionId ORDER BY T.Datetime) AS First_Campaign,
            FIRST_VALUE(T.campaign) OVER (PARTITION BY T.backendVisitorId,transactionId ORDER BY T.Datetime DESC) AS Last_Campaign,
            FIRST_VALUE(T.adContent) OVER (PARTITION BY T.backendVisitorId,transactionId ORDER BY T.Datetime) AS First_AdContent,
            FIRST_VALUE(T.adContent) OVER (PARTITION BY T.backendVisitorId,transactionId ORDER BY T.Datetime DESC) AS Last_AdContent,
            FIRST_VALUE(T.keyword) OVER (PARTITION BY T.backendVisitorId,transactionId ORDER BY T.Datetime) AS First_Keyword,
            FIRST_VALUE(T.keyword) OVER (PARTITION BY T.backendVisitorId,transactionId ORDER BY T.Datetime DESC) AS Last_Keyword,
        FROM `gusa-funnel-dwh.HTO_Data.HTO_cohort_assist_traffic` T
        LEFT JOIN `gusa-funnel-dwh.HTO_Data.HTO_cohort_assist_transactions` B ON T.backendVisitorId = B.backendVisitorId AND DATE_DIFF(B.Datetime, T.Datetime, DAY) BETWEEN 0 AND 30
        WHERE transactionId IS NOT NULL
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
        ORDER BY 1,9,3
        )

    SELECT 
        backendVisitorId,
        t_Date,
        transactionId,
        isHTO,
        t_Campaign_Type,
        t_campaign,
        t_adContent,
        t_keyword,
        First_Media,
        Last_Media,
        First_Campaign,
        Last_Campaign,
        First_AdContent,
        Last_AdContent,
        First_Keyword,
        Last_Keyword,
        DATETIME_DIFF(MAX(Datetime),MIN(Datetime),DAY) AS time_to_conversion,
        STRING_AGG(Media,'>>') AS Media_Funnel,
        STRING_AGG(Platform,'>>') AS Platform_Funnel,
        STRING_AGG(Campaign_Type,'>>') AS Campaign_Type_Funnel,
        STRING_AGG(campaign,'>>') AS Campaign__Funnel,
        STRING_AGG(adContent,'>>') AS AdContent_Funnel,
        STRING_AGG(keyword,'>>') AS Keyword_Funnel,
    FROM DB
    LEFT JOIN `gusa-funnel-dwh.GA_Media_Ref.Media_Type` USING(Media)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
/*) S ON T.transactionId = S.transactionId

WHEN NOT MATCHED THEN INSERT (backendVisitorId, t_Date, transactionId, isHTO, t_Campaign_Type, t_campaign, t_adContent, t_keyword, First_Media, Last_Media, First_Campaign, Last_Campaign, First_AdContent, Last_AdContent, First_Keyword, Last_Keyword, time_to_conversion, Media_Funnel, Platform_Funnel, Campaign_Type_Funnel, Campaign__Funnel, AdContent_Funnel, Keyword_Funnel) 
    VALUES (backendVisitorId, t_Date, transactionId, isHTO, t_Campaign_Type, t_campaign, t_adContent, t_keyword, First_Media, Last_Media, First_Campaign, Last_Campaign, First_AdContent, Last_AdContent, First_Keyword, Last_Keyword, time_to_conversion, Media_Funnel, Platform_Funnel, Campaign_Type_Funnel, Campaign__Funnel, AdContent_Funnel, Keyword_Funnel)







/**/