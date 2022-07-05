DECLARE days INT64 DEFAULT 3;
DECLARE startDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL days DAY));
DECLARE endDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

DECLARE daysP INT64 DEFAULT 33;
DECLARE startDateP STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL daysP DAY));
DECLARE endDateP STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

WITH base AS (
    SELECT DISTINCT
        scd.value AS backendVisitorId,
        DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
        DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Datetime,
        Media,
        IF(LOWER(trafficsource.campaign) LIKE '%hto%' OR LOWER(trafficsource.adContent) LIKE '%hto%' OR LOWER(trafficsource.keyword) LIKE '%hto%','HTO Campaign','Non HTO Campaign') AS Campaign_Type,
        trafficsource.campaign,
        trafficsource.adContent,
        trafficsource.keyword,
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(A.customDimensions) AS scd
    LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
        ON LOWER(trafficsource.source)=E.source AND LOWER(trafficSource.medium)=E.medium AND IFNULL(LOWER(trafficSource.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDateP AND endDateP
        And geoNetwork.country = 'United States'
        AND scd.index = 10
    ORDER BY 1,3
    )

, tr AS (
    SELECT DISTINCT
        scd.value AS backendVisitorId,
        DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Datetime,
        hits.transaction.transactionId,
        Media,
        IF(LOWER(trafficsource.campaign) LIKE '%hto%' OR LOWER(trafficsource.adContent) LIKE '%hto%' OR LOWER(trafficsource.keyword) LIKE '%hto%','HTO Campaign','Non HTO Campaign') AS Campaign_Type,
        trafficsource.campaign,
        trafficsource.adContent,
        trafficsource.keyword,
        cd.value AS isHTO,
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(A.customDimensions) AS scd, UNNEST(hits) AS hits, UNNEST(hits.customDimensions) AS cd
    LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
        ON LOWER(trafficsource.source)=E.source AND LOWER(trafficSource.medium)=E.medium AND IFNULL(LOWER(trafficSource.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
        AND hits.transaction.transactionId IS NOT NULL
        And geoNetwork.country = 'United States'
        AND cd.index = 19
        AND scd.index = 10
    ORDER BY 1
    )

, DB AS (
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
    STRING_AGG(Media,'>>') AS Media_Funnel,
    STRING_AGG(Platform,'>>') AS Platform_Funnel,
    STRING_AGG(Campaign_Type,'>>') AS Campaign_Type_Funnel,
    STRING_AGG(campaign,'>>') AS Campaign__Funnel,
    STRING_AGG(adContent,'>>') AS AdContent_Funnel,
    STRING_AGG(keyword,'>>') AS Keyword_Funnel,
FROM DB
LEFT JOIN `gusa-funnel-dwh.GA_Media_Ref.Media_Type` USING(Media)
GROUP BY 1,2,3,4,5,6,7,8,9,10









/**/