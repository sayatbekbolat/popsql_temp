DECLARE days INT64 DEFAULT 30; 

DELETE FROM `gusa-dc-dwh.HP_Analysis.hp_slider_imp` WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL days DAY);
INSERT INTO `gusa-dc-dwh.HP_Analysis.hp_slider_imp` (date, Media, deviceCategory, country, region, isFirstVisit, eventLabel, eventAction,
      Unique_Events, transactions, transactionRevenue, Visits, hits, pageviews, timeOnSite, imp, Unique_Users)
(
    WITH Unique_User AS (
        SELECT Date, deviceCategory, country, region, Media,
            COUNT(DISTINCT fullvisitorId) AS Unique_Users,
            COUNT(DISTINCT CONCAT(fullvisitorId,visitNumber)) AS Visits,
            SUM(DATETIME_DIFF(last_hit,DateT,SECOND)) AS timeOnSite,
            SUM(pages) AS pageviews,
        FROM (
            SELECT DISTINCT
                DATE(TIMESTAMP_MICROS(visitStartTime*1000000),'America/New_York') AS Date, 
                DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000),'America/New_York') AS DateT, 
                device.deviceCategory,
                geoNetwork.country ,
                geoNetwork.region,
                Media,
                fullVisitorId,
                visitNumber,
                COUNT(DISTINCT REGEXP_EXTRACT(hits.page.pagePath,r"[^?]*")) AS pages,
                MAX(DATETIME(TIMESTAMP_MICROS((visitStartTime*1000000) + (hits.time*1000)),'America/New_York')) AS last_hit,
            FROM `gusa-dwh.12571860.ga_sessions_*` , UNNEST(hits) AS hits
            LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
                ON LOWER(trafficSource.Source)=E.source AND LOWER(trafficSource.Medium)=E.medium AND IFNULL(LOWER(trafficSource.Campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)  
            WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') >= FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL days DAY))
            AND LOWER(hits.eventInfo.eventAction) LIKE '%hp banner impression%'
            GROUP BY 1,2,3,4,5,6,7,8
            )
        GROUP BY 1,2,3,4,5
        )

    , trans AS (
        SELECT DISTINCT
            fullVisitorId,
            visitNumber,
            hits.transaction.transactionId,
            hits.transaction.transactionRevenue/1000000 AS transactionRevenue,
        FROM `gusa-dwh.12571860.ga_sessions_*` , UNNEST(hits) AS hits
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') >= FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL days DAY))
            AND hits.transaction.transactionId IS NOT NULL
    )

    SELECT date, Media, deviceCategory, country, region, isFirstVisit, eventLabel, eventAction,  Unique_Events, transactions, transactionRevenue, Visits, hits, pageviews, timeOnSite, imp, Unique_Users
    FROM(
        SELECT 
            Date, 
            isFirstVisit,
            deviceCategory,
            country ,
            region,
            eventLabel,
            eventAction, 
            Media,
            SUM(Unique_Events) AS Unique_Events,            
            COUNT(DISTINCT transactionId) As transactions,
            SUM(transactionRevenue) As transactionRevenue,
            SUM(hits) AS hits,
            SUM(imp) AS imp
        FROM (
            SELECT  
                DATE(TIMESTAMP_MICROS(visitStartTime*1000000),'America/New_York') AS Date, 
                if(visitnumber=1,'y','n') As isFirstVisit,
                device.deviceCategory,
                geoNetwork.country ,
                geoNetwork.region,
                LOWER(hits.eventInfo.eventLabel) AS eventLabel,
                LOWER(hits.eventInfo.eventAction) AS eventAction, 
                Media,
                fullVisitorId,
                visitNumber,
                COUNT(DISTINCT fullVisitorId) AS Unique_Events,            
                SUM(totals.hits) AS hits,
                COUNT(*) AS imp
            FROM `gusa-dwh.12571860.ga_sessions_*` , UNNEST(hits) AS hits
            LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
                ON LOWER(trafficSource.Source)=E.source AND LOWER(trafficSource.Medium)=E.medium AND IFNULL(LOWER(trafficSource.Campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)  
            WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') >= FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL days DAY))
            AND LOWER(hits.eventInfo.eventAction) LIKE '%hp banner impression%'
            GROUP BY 1,2,3,4,5,6,7,8,9,10
            )
        LEFT JOIN trans USING (fullVisitorId,visitNumber)
        GROUP BY 1,2,3,4,5,6,7,8
        )

    LEFT JOIN Unique_User USING ( Date, deviceCategory, country, region, Media)
    ORDER BY Date, deviceCategory, country, region
    ) 
     
  
/*  */