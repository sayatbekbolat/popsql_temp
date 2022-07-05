DECLARE days INT64 DEFAULT 14; 
DECLARE startDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL days DAY));
DECLARE endDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

WITH Mails AS (
    SELECT CAST(TO_HEX(MD5(Email_address)) AS STRING) AS customer_email, CAST(TO_HEX(MD5(LOWER(Email_address))) AS STRING) AS lower_customer_email, IFNULL(Last_change,channels_email_subscribeStatus) AS subscribeStatus
    FROM (
        SELECT DISTINCT 
            channels_email_address AS Email_address, 
            A.channels_email_subscribeStatus ,
            FIRST_VALUE(B.channels_email_subscribeStatus) OVER (PARTITION BY LOWER(channels_email_address) ORDER BY TIMESTAMP(IFNULL(B.channels_email_subinfo_ts,B.channels_email_unsubinfo_ts)) DESC) Last_change

        FROM `gusa-bronto-dwh.NL_Data_2021.Cordail_Contacts_DB` A
        FULL OUTER JOIN `gusa-bronto-dwh.NL_Data_2021.Cordail_Contacts_Daily_Activity` B USING (channels_email_address)
        )
    )

, HashedMail AS (
    SELECT DISTINCT
        fullVisitorId,
        visitNumber,
        scd.value AS hashed,
        IFNULL(M.subscribeStatus,M2.subscribeStatus) AS subscribeStatus,
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(A.customDimensions) AS scd
    LEFT JOIN Mails M ON scd.value = M.customer_email
    LEFT JOIN Mails M2 ON scd.value = M2.lower_customer_email
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN  startDate AND endDate
        AND scd.index = 46
    )

, Trans AS (
    SELECT DISTINCT
        DATETIME(TIMESTAMP_MICROS((visitStartTime*1000000)), "America/Los_Angeles") AS datetime,
        fullVisitorId AS fullVisitorId,
        visitNumber AS visitNumber,
        hits.transaction.transactionId,
        Platform,
        media_name_group AS Media_Group,
        Media,
        hashed,
        subscribeStatus,
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(hits) AS hits
    LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
        ON LOWER(trafficsource.source)=E.source AND LOWER(trafficSource.medium)=E.medium AND IFNULL(LOWER(trafficSource.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)
    LEFT JOIN `gusa-funnel-dwh.GA_Media_Ref.Media_Type` USING(Media)
    LEFT JOIN HashedMail USING(fullVisitorId,visitNumber)
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN  startDate AND endDate
        AND hits.transaction.transactionId IS NOT NULL
    )

, Carts AS (
    SELECT 
        DATETIME(TIMESTAMP_MICROS((visitStartTime*1000000)), "America/Los_Angeles") AS datetime,
        A.fullVisitorId,
        A.visitNumber,
        transactionId,
        M.Platform,
        M.media_name_group AS Media_Group,
        M.Media,
        H.hashed,
        H.subscribeStatus,
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(hits) AS hits
    LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
        ON LOWER(trafficsource.source)=E.source AND LOWER(trafficSource.medium)=E.medium AND IFNULL(LOWER(trafficSource.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)
    LEFT JOIN `gusa-funnel-dwh.GA_Media_Ref.Media_Type` M USING(Media)
    LEFT JOIN HashedMail H USING(fullVisitorId,visitNumber)
    LEFT JOIN Trans T ON A.fullVisitorId = T.fullVisitorId
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN  startDate AND endDate
        AND LOWER(hits.eventInfo.eventCategory) LIKE '%eec%' 
        AND LOWER(hits.eventInfo.eventAction) LIKE '%cart%'
        AND LOWER(hits.eventInfo.eventLabel) LIKE '%products%'
        AND (LOWER(hits.page.pagePath) LIKE '%checkout/cart%' OR LOWER(hits.page.pagePath) LIKE '%checkout/onepage%')
        AND (T.fullVisitorId IS NULL OR A.visitNumber < T.visitNumber)
    GROUP BY 1,2,3,4,5,6,7,8,9
    ORDER BY 1,2,4
    )

, Comb AS (
    SELECT *, COUNT(*) OVER (PARTITION BY fullVisitorId) AS touch_points,
    FROM (
        SELECT * FROM Carts
        UNION ALL
        SELECT * FROM Trans
        )
    ORDER BY fullVisitorId,datetime
    )

SELECT 
    --fullVisitorId,
    --transactionId,
    --touch_points,
    IFNULL(subscribeStatus,'unidentified') AS subscribeStatus,
    IF(touch_points = 1, 'Single Touch Point', 'Multiple Touch Points') AS funnel_length,
    IF(LOWER(media_funnel) LIKE '%abn%','Includes ABN',"Doesn't Include ABN") AS contains_abn,
    IF(LOWER(ARRAY_REVERSE(SPLIT(media_funnel,'>>'))[SAFE_ORDINAL(1)]) LIKE '%abn%', 'ABN Last Media', 'Non ABN Last Media') AS abn_last,
    COUNT(DISTINCT fullVisitorId) AS Users,
    COUNT(DISTINCT transactionId) AS Transactions,
    --media_funnel,
FROM (
    SELECT 
        fullVisitorId,
        transactionId,
        touch_points, 
        subscribeStatus,
        STRING_AGG(Media,'>>') AS media_funnel,
    FROM Comb
    --WHERE touch_points != 1
    GROUP BY 1,2,3,4
    )
GROUP BY 1,2,3,4
    /**/