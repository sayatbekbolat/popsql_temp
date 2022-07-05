DECLARE GSRowNumber INT64 DEFAULT 4;
DECLARE eventName STRING DEFAULT (SELECT Event_Name FROM (SELECT Event_Name,ROW_NUMBER() OVER () AS rowNumber FROM `gusa-dc-dwh.Offline_Events_GA_Data.Events_List_GS`) WHERE rowNumber=GSRowNumber);
DECLARE eventDate DATE DEFAULT (SELECT Date FROM (SELECT Date,ROW_NUMBER() OVER () AS rowNumber FROM `gusa-dc-dwh.Offline_Events_GA_Data.Events_List_GS`) WHERE rowNumber=GSRowNumber);
DECLARE eventSpend FLOAT64 DEFAULT (SELECT Spend FROM (SELECT Spend,ROW_NUMBER() OVER () AS rowNumber FROM `gusa-dc-dwh.Offline_Events_GA_Data.Events_List_GS`) WHERE rowNumber=GSRowNumber);
DECLARE startDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(eventDate, INTERVAL 28 DAY));
DECLARE startDateLT STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(DATE_TRUNC(eventDate,ISOWEEK), INTERVAL 28 DAY)); 
DECLARE endDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_ADD(eventDate, INTERVAL 7 DAY));

--------------------Hourly Short Tail-------------
DELETE FROM `gusa-dc-dwh.Offline_Events_GA_Data.Events_Hourly_Data` WHERE Event_Date = eventDate;
INSERT INTO `gusa-dc-dwh.Offline_Events_GA_Data.Events_Hourly_Data` (Event_Name, Event_Date, Date, Day, Hour, Media, Device, country, Event_Spend, Users, Visits, New_Users, NL_subs, SMS_subs, Unique_ATC, ATC, Cart, Initiate_Checkout, Transactions, Revenue) 
(

SELECT 
    eventName AS Event_Name,
    eventDate AS Event_Date,
    DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
    FORMAT_DATE('%A',DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles")) AS Day,
    EXTRACT(HOUR FROM DATETIME(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles")) AS Hour,
    Media,
    device.deviceCategory AS Device,
    geoNetwork.country,
    eventSpend AS Event_Spend,

    COUNT(DISTINCT fullVisitorId) AS Users,
    COUNT(DISTINCT CONCAT(fullVisitorId,visitId)) AS Visits,
    COUNT(DISTINCT IF(visitNumber = 1,fullVisitorId,NULL)) AS New_Users,
    
    COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subsc%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%', CONCAT(fullVisitorId,visitId), NULL)) AS NL_subs,
    COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subsc%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%', CONCAT(fullVisitorId,visitId), NULL)) AS SMS_subs,
    
    COUNT(DISTINCT IF(hits.eventInfo.eventAction = 'Cart' AND hits.eventInfo.eventLabel = 'AddToCart', CONCAT(fullVisitorId,visitId), NULL)) AS Unique_ATC,
    COUNT(IF(hits.eventInfo.eventAction = 'Cart' AND hits.eventInfo.eventLabel = 'AddToCart', CONCAT(fullVisitorId,visitId), NULL)) AS ATC,
    
    COUNT(DISTINCT IF(LOWER(hits.page.pagePath) LIKE '%checkout/cart%', CONCAT(fullVisitorId,visitId), NULL)) AS Cart,
    COUNT(DISTINCT IF(LOWER(hits.page.pagePath) LIKE '%checkout/onepage%', CONCAT(fullVisitorId,visitId), NULL)) AS Initiate_Checkout,

    COUNT(DISTINCT hits.transaction.transactionId) AS Transactions,
    SUM(hits.transaction.transactionRevenue/1000000) as Revenue,
FROM `gusa-dwh.12571860.ga_sessions_*`, UNNEST(hits) AS hits

LEFT JOIN
    (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
    ON LOWER(trafficSource.source)=E.source AND LOWER(trafficSource.medium)=E.medium AND IFNULL(LOWER(trafficSource.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)

WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
GROUP BY 1,2,3,4,5,6,7,8,9
);


--------------------Daily Long Tail-------------
DELETE FROM `gusa-dc-dwh.Offline_Events_GA_Data.Events_Data` WHERE Event_Date = eventDate;
INSERT INTO `gusa-dc-dwh.Offline_Events_GA_Data.Events_Data` (Event_Name, Event_Date, Date, Media, Device, country, Event_Spend, Users, Visits, New_Users, NL_subs, SMS_subs, Unique_ATC, ATC, Cart, Initiate_Checkout, Transactions, Revenue) 
(

SELECT 
    eventName AS Event_Name,
    eventDate AS Event_Date,
    DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
    Media,
    device.deviceCategory AS Device,
    geoNetwork.country,
    eventSpend AS Event_Spend,

    COUNT(DISTINCT fullVisitorId) AS Users,
    COUNT(DISTINCT CONCAT(fullVisitorId,visitId)) AS Visits,
    COUNT(DISTINCT IF(visitNumber = 1,fullVisitorId,NULL)) AS New_Users,
    
    COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subsc%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%', CONCAT(fullVisitorId,visitId), NULL)) AS NL_subs,
    COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subsc%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%', CONCAT(fullVisitorId,visitId), NULL)) AS SMS_subs,
    
    COUNT(DISTINCT IF(hits.eventInfo.eventAction = 'Cart' AND hits.eventInfo.eventLabel = 'AddToCart', CONCAT(fullVisitorId,visitId), NULL)) AS Unique_ATC,
    COUNT(IF(hits.eventInfo.eventAction = 'Cart' AND hits.eventInfo.eventLabel = 'AddToCart', CONCAT(fullVisitorId,visitId), NULL)) AS ATC,
    
    COUNT(DISTINCT IF(LOWER(hits.page.pagePath) LIKE '%checkout/cart%', CONCAT(fullVisitorId,visitId), NULL)) AS Cart,
    COUNT(DISTINCT IF(LOWER(hits.page.pagePath) LIKE '%checkout/onepage%', CONCAT(fullVisitorId,visitId), NULL)) AS Initiate_Checkout,

    COUNT(DISTINCT hits.transaction.transactionId) AS Transactions,
    SUM(hits.transaction.transactionRevenue/1000000) as Revenue,
FROM `gusa-dwh.12571860.ga_sessions_*`, UNNEST(hits) AS hits

LEFT JOIN
    (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
    ON LOWER(trafficSource.source)=E.source AND LOWER(trafficSource.medium)=E.medium AND IFNULL(LOWER(trafficSource.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") 
        AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)
    LEFT JOIN `

WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDateLT AND endDate
GROUP BY 1,2,3,4,5,6,7
); 
    
    
    
    
/**/