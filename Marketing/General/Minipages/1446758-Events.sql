DECLARE days INT64 DEFAULT 3;
DECLARE startDateGA STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL days DAY));
DECLARE endDateGA STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

/*DELETE FROM `gusa-dc-dwh.Minipages.Minipages_Daily_Events_Data` WHERE Date >= PARSE_DATE('%Y%m%d',startDateGA);
INSERT INTO `gusa-dc-dwh.Minipages.Minipages_Daily_Events_Data` (Date, deviceCategory, Media, eventAction, eventLabel, Page, Users, Total)
(*/
    SELECT  
        DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/New_York") AS Date,
        device.deviceCategory,
        Media,
        hits.eventInfo.eventAction,
        IF(hits.eventInfo.eventLabel LIKE '%v2%',NULL,hits.eventInfo.eventLabel) AS eventLabel,
        REGEXP_EXTRACT(hits.page.pagePath,r"[^?]*") AS Page,
        COUNT(DISTINCT fullVisitorId) AS Users,
        COUNT(*) AS Total,
    FROM `gusa-dwh.12571860.ga_sessions_*` A,UNNEST(hits) AS hits
    LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
        ON LOWER(trafficSource.Source)=E.source AND LOWER(trafficSource.Medium)=E.medium AND IFNULL(LOWER(trafficSource.Campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDateGA AND endDateGA
        AND geoNetwork.country != 'Israel'
        AND LOWER(hits.page.hostname) LIKE '%minipage%'
        AND hits.eventInfo.eventCategory LIKE '%Marketing LP%'
    GROUP BY 1,2,3,4,5,6
--);