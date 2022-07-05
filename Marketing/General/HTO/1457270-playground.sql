WITH base AS (
    SELECT DISTINCT
        scd.value AS backendVisitorId,
        DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
        IF(EXTRACT(HOUR FROM DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles")) IN (8,9,10), 'View', 'Regular') AS Hour_Type,
        Media,
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(A.customDimensions) AS scd
    LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
        ON LOWER(trafficsource.source)=E.source AND LOWER(trafficSource.medium)=E.medium AND IFNULL(LOWER(trafficSource.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN '20220521' AND '20220523'
        AND DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") = '2022-05-22'
        And geoNetwork.country = 'United States'
        AND scd.index = 10
        AND Media IN ('Bing Brand','Bing Organic','Direct','Google Brand','Google Organic')
    ORDER BY 1,3
)

, trans AS (
    SELECT DISTINCT
        scd.value AS backendVisitorId,
        hits.transaction.transactionId,
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(A.customDimensions) AS scd, UNNEST(hits) AS hits
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN '20220522' AND '20220527'
        AND DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") >= '2022-05-23'
        AND hits.transaction.transactionId IS NOT NULL
        AND scd.index = 10
    ORDER BY 1
)

SELECT DISTINCT
    T.backendVisitorId,
    Hour_Type,
    transactionId,
    
FROM base T
LEFT JOIN trans B ON T.backendVisitorId = B.backendVisitorId
--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16





/**/