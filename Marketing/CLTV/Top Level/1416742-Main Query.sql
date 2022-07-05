DECLARE days INT64 DEFAULT 3;
DECLARE startDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL days DAY));
DECLARE endDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

--------------Email Cohort Table update-------------
DELETE FROM `gusa-dc-dwh.NL_Subs.Email_Cohort_tbl` WHERE TRUE;
INSERT INTO `gusa-dc-dwh.NL_Subs.Email_Cohort_tbl` ( Date, Media_Group, Orders_D7_wD0, Orders_D7, Orders_D30, Orders_D90, Orders_D180, Orders_Y1, Orders_Y2, Orders_Y3, Orders, Revenue_D7_wD0, Revenue_D7, Revenue_D30, Revenue_D90, Revenue_D180, Revenue_Y1, Revenue_Y2, Revenue_Y3, Revenue)
(
  SELECT Date, Media_Group, Orders_D7_wD0, Orders_D7, Orders_D30, Orders_D90, Orders_D180, Orders_Y1, Orders_Y2, Orders_Y3, Orders, Revenue_D7_wD0, Revenue_D7, Revenue_D30, Revenue_D90, Revenue_D180, Revenue_Y1, Revenue_Y2, Revenue_Y3, Revenue
  FROM `gusa-dc-dwh.NL_Subs.Email_Cohort_View`
)
;

------------------NL Subscriptions Report---------------
DELETE FROM `gusa-dc-dwh.NL_Subs.NL_Rolling_Report` WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL days DAY);
INSERT INTO `gusa-dc-dwh.NL_Subs.NL_Rolling_Report` (Date, Device, Traffic_Type, Platform, Media_Group, landing_Page_Type, NL_Sub_Page, SMS_Sub_Page, Users, Users_With_Popup, Users_With_SMS_Popup, NL_Subscribers, Non_NL_Subscribers, SMS_Subscribers, Non_SMS_Subscribers, Popup_Shown, SMS_Popup_Shown, NL_Subscribers_Popup_Shown, NL_Non_Subscribers_Popup_Shown, NL_subs, Footer_NL_subs, SMS_Subscribers_Popup_Shown, SMS_Non_Subscribers_Popup_Shown, SMS_subs, ATC, Lens_Funnel_Start, Initiate_Checkout, Transactions, Revenue)

(
    WITH LP AS (
        SELECT
            fullVisitorId,
            visitNumber,
            `gusa-funnel-dwh.GA_Media_Daily.LP_Type_func` (REGEXP_EXTRACT(pagePath,r"[^?]*")) AS landing_Page_Type,
        FROM (
            SELECT
                fullVisitorId,
                visitNumber,
                hits.hitNumber,
                hits.page.pagePath,
                RANK() OVER (PARTITION BY fullVisitorId, visitNumber ORDER BY hits.hitNumber) AS ranking,
            FROM `gusa-dwh.12571860.ga_sessions_*` , UNNEST(hits) AS hits
            WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
                AND hits.page.pagePath IS NOT NULL
            GROUP BY 1,2,3,4
            ) A
        WHERE ranking = 1
        )
    
    , Sub_Base AS (
        SELECT 
            fullVisitorId,
            visitNumber,
            IF(LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%',TRUE,FALSE) AS NL_Pop,
            IF(LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%',TRUE,FALSE) AS SMS_Pop,
        FROM `gusa-dwh.12571860.ga_sessions_*` A,UNNEST(hits) AS hits
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
            AND hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%'
        GROUP BY 1,2,3,4
        )
    /*
    , Checkout_Subs AS (
        SELECT DISTINCT
            fullVisitorId,
            visitNumber,
        FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(hits) AS hits, UNNEST(hits.customDimensions) AS cd
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
            AND hits.eventInfo.eventLabel LIKE '%CTA - Place Order%'
            AND cd.index = 55
            AND cd.value = 'Marked'
        )
    */
    , Subs_Pages AS (
        SELECT DISTINCT
            fullVisitorId AS id,
            DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
            `gusa-funnel-dwh.GA_Media_Daily.LP_Type_func` (REGEXP_EXTRACT(hits.page.pagePath,r"[^?]*")) AS NL_Sub_Page,
        FROM `gusa-dwh.12571860.ga_sessions_*` A,UNNEST(hits) AS hits
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
            AND ((LOWER(hits.eventInfo.eventCategory) = 'popups' 
                    AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subs%' 
                    AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%'
                    AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%'
                    AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%') 
                OR (LOWER(hits.eventInfo.eventAction) LIKE '%newsletter subscription%' 
                    AND LOWER(hits.eventInfo.eventLabel) LIKE '%success%' 
                    ))
        )
        
    , SMS_Subs_Pages AS (
        SELECT DISTINCT
            fullVisitorId AS id,
            DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
            `gusa-funnel-dwh.GA_Media_Daily.LP_Type_func` (REGEXP_EXTRACT(hits.page.pagePath,r"[^?]*")) AS SMS_Sub_Page,
        FROM `gusa-dwh.12571860.ga_sessions_*` A,UNNEST(hits) AS hits
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
            AND LOWER(hits.eventInfo.eventCategory) = 'popups' 
            AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subs%' 
            AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%'
            AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%'
            AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%'
        )
    
    , FM AS (
        SELECT DISTINCT
            Date,
            fullVisitorId AS id,
            Media,
        FROM(
            SELECT
                DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
                fullVisitorId,
                visitNumber,
                visitStartTime,
                trafficsource.source,
                trafficSource.medium,
                trafficSource.campaign,
                trafficSource.adwordsClickInfo.customerId,
                MIN(visitNumber) OVER (PARTITION BY DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles"), fullVisitorId ORDER BY visitNumber ASC) AS min_visit,
            FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(hits) AS hits
            WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
            GROUP BY 1,2,3,4,5,6,7,8
            ORDER BY fullVisitorId, visitNumber ASC
            ) A
        LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
            ON LOWER(A.source)=E.source AND LOWER(A.medium)=E.medium AND IFNULL(LOWER(A.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(A.customerId,0)=IFNULL(E.adwords_account,0)
        WHERE visitNumber = min_visit
        )
    
    SELECT 
        DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
        IF(LOWER(device.deviceCategory) = 'mobile','Mobile','Desktop') AS Device,
        CASE 
            WHEN trafficSource.adwordsClickInfo.adGroupId IN (115064498040,107604022469,2019949134,51589055885,56408369201,51903570859,2020282614,1801140054,50658777102,120255706777,76768043425,24554403054) THEN 'Stable'
            ELSE 'Other'
        END AS Traffic_Type,
        Platform,
        media_name_group AS Media_Group,
        --Media,
        landing_Page_Type,
        /*Popup_Device,
        Popup_Type,*/
        NL_Sub_Page,
        SMS_Sub_Page,
        COUNT(DISTINCT fullVisitorId) AS Users,
        COUNT(DISTINCT IF(NL_Pop,Sub_Base.fullVisitorId,NULL)) AS Users_With_Popup,
        COUNT(DISTINCT IF(SMS_Pop,Sub_Base.fullVisitorId,NULL)) AS Users_With_SMS_Popup,
        
        COUNT(DISTINCT IF(P.id IS NOT NULL AND hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS NL_Subscribers,
        COUNT(DISTINCT IF(P.id IS NULL AND hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS Non_NL_Subscribers,
        
        COUNT(DISTINCT IF(P.id IS NOT NULL AND hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS SMS_Subscribers,
        COUNT(DISTINCT IF(P.id IS NULL AND hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS Non_SMS_Subscribers,
        
        COUNT(IF(hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS Popup_Shown,
        COUNT(IF(hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS SMS_Popup_Shown,
            
        COUNT(IF(P.id IS NOT NULL AND hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS NL_Subscribers_Popup_Shown,
        COUNT(IF(P.id IS NULL AND hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS NL_Non_Subscribers_Popup_Shown,
        COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subs%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%', CONCAT(fullVisitorId), NULL)
            ) AS NL_subs,
            
        COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventAction) LIKE '%newsletter subscription%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%success%', CONCAT(fullVisitorId), NULL)) AS Footer_NL_subs,
        
        COUNT(IF(S.id IS NOT NULL AND hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS SMS_Subscribers_Popup_Shown,
        COUNT(IF(S.id IS NULL AND hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty%',fullVisitorId,NULL)
            ) AS SMS_Non_Subscribers_Popup_Shown,
        COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subs%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%', CONCAT(fullVisitorId), NULL)
            ) AS SMS_subs,
            
        COUNT(DISTINCT IF(hits.eventInfo.eventAction = 'Cart' AND hits.eventInfo.eventLabel = 'AddToCart', CONCAT(fullVisitorId), NULL)) AS ATC,
        COUNT(DISTINCT IF(LOWER(hits.eventInfo.eventAction) LIKE '%lens funnel%' AND LOWER(hits.eventInfo.eventAction) LIKE '%step 1%', CONCAT(fullVisitorId), NULL)) AS Lens_Funnel_Start,
        COUNT(DISTINCT IF(LOWER(hits.page.pagePath) LIKE '%checkout/onepage%', CONCAT(fullVisitorId), NULL)) AS Initiate_Checkout,
        COUNT(DISTINCT hits.transaction.transactionId) AS Transactions,
        SUM(hits.transaction.transactionRevenue/1000000) as Revenue,
        
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(hits) AS hits
    LEFT JOIN Sub_Base USING (fullVisitorId,visitNumber)
    LEFT JOIN Subs_Pages P ON fullVisitorId = P.id AND DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") = P.Date
    LEFT JOIN SMS_Subs_Pages S ON fullVisitorId = S.id AND DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") = S.Date
    LEFT JOIN LP USING (fullVisitorId,visitNumber)
    LEFT JOIN FM  ON fullVisitorId = FM.id AND DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") = FM.Date
    LEFT JOIN `gusa-funnel-dwh.GA_Media_Ref.Media_Type` USING(Media)
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
    GROUP BY 1,2,3,4,5,6,7,8
);
    
---------------NL popup data--------------------
DELETE FROM `gusa-dc-dwh.NL_Subs.NL_Popup_Location_Data` WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL days DAY);
INSERT INTO `gusa-dc-dwh.NL_Subs.NL_Popup_Location_Data` (Date, Device, Traffic_Type, Media, landing_Page_Type, Page_Type, eventAction, Users, NL_Popup_Shown, SMS_Popup_Shown, NL_subs, SMS_subs)

(
    WITH DB AS (
        SELECT DISTINCT
            fullVisitorId,
            visitNumber,
        FROM `gusa-dwh.12571860.ga_sessions_*` A,UNNEST(hits) AS hits
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
            AND hits.eventInfo.eventCategory = 'popups' 
            AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' 
            AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' 
            AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%'
            AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup-showed%'
            AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty-popup-showed%'
        )
    
    
    , LP AS (
        SELECT
            fullVisitorId,
            visitNumber,
            `gusa-funnel-dwh.GA_Media_Daily.LP_Type_func` (REGEXP_EXTRACT(pagePath,r"[^?]*")) AS landing_Page_Type,
        FROM (
            SELECT
                fullVisitorId,
                visitNumber,
                hits.hitNumber,
                hits.page.pagePath,
                RANK() OVER (PARTITION BY fullVisitorId, visitNumber ORDER BY hits.hitNumber) AS ranking,
            FROM `gusa-dwh.12571860.ga_sessions_*` , UNNEST(hits) AS hits
            INNER JOIN DB USING(fullVisitorId,visitNumber)
            WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
                AND hits.page.pagePath IS NOT NULL
            GROUP BY 1,2,3,4
            ) A
        WHERE ranking = 1
        )
    
    , FM AS (
        SELECT DISTINCT
            Date,
            fullVisitorId AS id,
            Media,
        FROM(
            SELECT
                DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
                fullVisitorId,
                visitNumber,
                visitStartTime,
                trafficsource.source,
                trafficSource.medium,
                trafficSource.campaign,
                trafficSource.adwordsClickInfo.customerId,
                MIN(visitNumber) OVER (PARTITION BY DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles"), fullVisitorId ORDER BY visitNumber ASC) AS min_visit,
            FROM `gusa-dwh.12571860.ga_sessions_*` A
            INNER JOIN DB USING(fullVisitorId,visitNumber)
            WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
            GROUP BY 1,2,3,4,5,6,7,8
            ORDER BY fullVisitorId, visitNumber ASC
            ) A
        LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
            ON LOWER(A.source)=E.source AND LOWER(A.medium)=E.medium AND IFNULL(LOWER(A.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(A.customerId,0)=IFNULL(E.adwords_account,0)
        WHERE visitNumber = min_visit
        )
    
    SELECT 
        DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
        IF(LOWER(device.deviceCategory) = 'mobile','Mobile','Desktop') AS Device,
        CASE 
            WHEN trafficSource.adwordsClickInfo.adGroupId IN (115064498040,107604022469,2019949134,51589055885,56408369201,51903570859,2020282614,1801140054,50658777102,120255706777,76768043425,24554403054) THEN 'Stable'
            ELSE 'Other'
        END AS Traffic_Type,
        Media,
        landing_Page_Type,
        `gusa-funnel-dwh.GA_Media_Daily.LP_Type_func` (REGEXP_EXTRACT(hits.page.pagePath,r"[^?]*")) AS Page_Type,
        hits.eventInfo.eventAction,
        COUNT(DISTINCT fullVisitorId) AS Users,
        COUNT(IF(hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%',fullVisitorId,NULL)) AS NL_Popup_Shown,
        COUNT(IF(hits.eventInfo.eventCategory = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%show%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%',fullVisitorId,NULL)) AS SMS_Popup_Shown,
        COUNT(IF(LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subs%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%', CONCAT(fullVisitorId), NULL)) AS NL_subs,
        COUNT(IF(LOWER(hits.eventInfo.eventCategory) = 'popups' AND LOWER(hits.eventInfo.eventLabel) LIKE '%-subs%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%' AND LOWER(hits.eventInfo.eventLabel) LIKE '%sms%' AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%', CONCAT(fullVisitorId), NULL)) AS SMS_subs,

        
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(hits) AS hits
    INNER JOIN DB USING (fullVisitorId,visitNumber)
    LEFT JOIN LP USING (fullVisitorId,visitNumber)
    LEFT JOIN FM ON DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles")=FM.Date AND fullVisitorId = id
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
        AND hits.eventInfo.eventCategory = 'popups' 
        AND (LOWER(hits.eventInfo.eventLabel) LIKE '%show%' OR LOWER(hits.eventInfo.eventLabel) LIKE '%-subs%')
        AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%reminder%' 
        AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%remainder%'
        AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%subscribed-popup%'
        AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%ty-popup%'
        AND LOWER(hits.eventInfo.eventLabel) NOT LIKE '%error%'
    GROUP BY 1,2,3,4,5,6,7
);
    
/**/