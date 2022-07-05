DECLARE days INT64 DEFAULT 3;
DECLARE startDateGA STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL days DAY));
DECLARE endDateGA STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

DELETE FROM `gusa-dc-dwh.Minipages.Minipages_Daily_Data` WHERE Date >= PARSE_DATE('%Y%m%d',startDateGA);
INSERT INTO `gusa-dc-dwh.Minipages.Minipages_Daily_Data` (Date, deviceCategory, page, Media, Users, Sessions, Session_Bounces, Session_Time, Page_Time, Continued_To_Additional_Pages, General_Click, CTA_Click, Product_Click, Logo_Click, Category_Click, Content_Click, Footer_Click, Klarna_Click, NL_subs, Popup_subs, Onetap_subs, ATC, transactions)
(
WITH lp AS (
    SELECT
        fullVisitorId,
        visitId,
        Media,
        bounces,
        pages,
        REGEXP_EXTRACT(pagePath,r"[^?]*") AS page,
        `gusa-funnel-dwh.GA_Media_Daily.LP_Type_func` (REGEXP_EXTRACT(pagePath,r"[^?]*")) AS landing_Page_Type,
        DATETIME_DIFF(DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000 + last_event*1000), "America/New_York"),DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000), "America/New_York"),SECOND) AS Session_Time,
        DATETIME_DIFF(DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000 + last_page_event*1000), "America/New_York"),DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000), "America/New_York"),SECOND) AS Page_Time, 
    FROM (
        SELECT
            fullVisitorId,
            visitId,
            visitStartTime,
            hits.time,
            hits.hitNumber,
            hits.page.pagePath,
            hits.page.hostname,
            Media,
            totals.bounces,
            RANK() OVER (PARTITION BY fullVisitorId, visitId ORDER BY hits.hitNumber) AS ranking,
            FIRST_VALUE(hits.time) OVER (PARTITION BY fullVisitorId, visitId ORDER BY hits.hitNumber DESC) AS last_event,
            FIRST_VALUE(hits.time) OVER (PARTITION BY fullVisitorId, visitId, hits.page.pagePath ORDER BY hits.hitNumber DESC) AS last_page_event,
            COUNT(DISTINCT REGEXP_EXTRACT(hits.page.pagePath,r"[^?]*")) OVER (PARTITION BY fullVisitorId, visitId) AS pages
        FROM `gusa-dwh.12571860.ga_sessions_*` , UNNEST(hits) AS hits
        LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
            ON LOWER(trafficSource.Source)=E.source AND LOWER(trafficSource.Medium)=E.medium AND IFNULL(LOWER(trafficSource.Campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDateGA AND endDateGA
            AND hits.page.pagePath IS NOT NULL
        GROUP BY 1,2,3,4,5,6,7,8,9
        ) A
    WHERE ranking = 1
        AND LOWER(hostname) LIKE '%minipage%'
    )

, final AS (
    SELECT  
        DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/New_York") AS Date,
        lp.page,
        Media,
        fullVisitorId,
        visitId,
        device.deviceCategory,
        COUNT(DISTINCT IF(pages > 1,CONCAT(fullVisitorId,visitId),NULL)) AS Continued_To_Additional_Pages,
        COUNT(DISTINCT 
            IF(hits.eventInfo.eventCategory LIKE '%Marketing LP%' AND LOWER(hits.eventInfo.eventAction) LIKE '%click%' AND LOWER(hits.eventInfo.eventAction) NOT LIKE '%color%', fullVisitorId, NULL
            )) AS General_Click,
        COUNT(DISTINCT IF(hits.eventInfo.eventCategory LIKE '%Marketing LP%' AND hits.eventInfo.eventAction LIKE '%CTA Click%',fullVisitorId,NULL)) AS CTA_Click,
        COUNT(DISTINCT IF(hits.eventInfo.eventCategory LIKE '%Marketing LP%' AND hits.eventInfo.eventAction LIKE '%Product Click%',fullVisitorId,NULL)) AS Product_Click,
        COUNT(DISTINCT IF(hits.eventInfo.eventCategory LIKE '%Marketing LP%' AND hits.eventInfo.eventAction LIKE '%Logo Click%',fullVisitorId,NULL)) AS Logo_Click,
        COUNT(DISTINCT IF(hits.eventInfo.eventCategory LIKE '%Marketing LP%' AND hits.eventInfo.eventAction LIKE '%Category Click%',fullVisitorId,NULL)) AS Category_Click,
        COUNT(DISTINCT IF(hits.eventInfo.eventCategory LIKE '%Marketing LP%' AND hits.eventInfo.eventAction LIKE '%Content Click%',fullVisitorId,NULL)) AS Content_Click,
        COUNT(DISTINCT IF(hits.eventInfo.eventCategory LIKE '%Marketing LP%' AND hits.eventInfo.eventAction LIKE '%Footer Click%',fullVisitorId,NULL)) AS Footer_Click,
        COUNT(DISTINCT IF(hits.eventInfo.eventCategory LIKE '%Marketing LP%' AND hits.eventInfo.eventAction LIKE '%Klarna Click%',fullVisitorId,NULL)) AS Klarna_Click,

        COUNT(DISTINCT IF(
            (LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subsc%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%') 
            OR
            (LOWER(hits.eventInfo.eventCategory) = 'newsletter registration' AND LOWER(hits.eventInfo.eventAction) = 'user subscribed'),
        CONCAT(fullVisitorId,visitId), NULL)) AS NL_subs,

        COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subsc%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%', CONCAT(fullVisitorId,visitId), NULL)) AS Popup_subs,    
        COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventCategory) = 'newsletter registration' AND LOWER(hits.eventInfo.eventAction) = 'user subscribed' AND LOWER(hits.eventInfo.eventLabel) LIKE '%one tap%', CONCAT(fullVisitorId,visitId), NULL)) AS Onetap_subs, 
 
        COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventLabel) LIKE '%addtocart%',fullVisitorId,NULL)) AS ATC,
        COUNT(DISTINCT hits.transaction.transactionId) AS Transactions,
        
    
    FROM `gusa-dwh.12571860.ga_sessions_*` A,UNNEST(hits) AS hits
    INNER JOIN lp USING (fullVisitorId,visitId)
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDateGA AND endDateGA
        AND geoNetwork.country != 'Israel'
    GROUP BY 1,2,3,4,5,6
    )
    
SELECT  
    Date,
    deviceCategory,
    A.page,
    A.Media,
    COUNT(DISTINCT fullVisitorId) AS Users,
    COUNT(DISTINCT CONCAT(fullVisitorId,visitId)) AS Sessions,
    COUNT(DISTINCT IF (Bounces>0,CONCAT(fullVisitorId,visitId),NULL)) AS Session_Bounces,
    SUM(Session_Time) AS Session_Time,
    SUM(Page_Time) AS Page_Time,
    COUNT(DISTINCT IF(Continued_To_Additional_Pages>0,fullVisitorId,NULL)) AS Continued_To_Additional_Pages,
    COUNT(DISTINCT IF(General_Click>0,fullVisitorId,NULL)) AS General_Click,
    COUNT(DISTINCT IF(CTA_Click>0,fullVisitorId,NULL)) AS CTA_Click,
    COUNT(DISTINCT IF(Product_Click>0,fullVisitorId,NULL)) AS Product_Click,
    COUNT(DISTINCT IF(Logo_Click>0,fullVisitorId,NULL)) AS Logo_Click,
    COUNT(DISTINCT IF(Category_Click>0,fullVisitorId,NULL)) AS Category_Click,
    COUNT(DISTINCT IF(Content_Click>0,fullVisitorId,NULL)) AS Content_Click,
    COUNT(DISTINCT IF(Footer_Click>0,fullVisitorId,NULL)) AS Footer_Click,
    COUNT(DISTINCT IF(Klarna_Click>0,fullVisitorId,NULL)) AS Klarna_Click,
    COUNT(DISTINCT IF(NL_subs>0,fullVisitorId,NULL)) AS NL_subs,    
    COUNT(DISTINCT IF(Popup_subs>0,fullVisitorId,NULL)) AS Popup_subs,    
    COUNT(DISTINCT IF(Onetap_subs>0,fullVisitorId,NULL)) AS Onetap_subs,   
    COUNT(DISTINCT IF(ATC>0,fullVisitorId,NULL)) AS ATC,
    SUM(transactions) AS transactions,
FROM final A
LEFT JOIN lp USING(fullVisitorId,visitId)
GROUP BY 1,2,3,4
);