DECLARE i INT64 DEFAULT 0;
DECLARE runDate DATE;
DECLARE runLength DEFAULT (SELECT COUNT(*) FROM (
    WITH splitted AS (
        SELECT *
        FROM UNNEST( SPLIT(RPAD('',(SELECT DATE_DIFF(DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(),Month), INTERVAL 1 MONTH),INTERVAL 1 DAY),CAST('2021-10-12' AS DATE),DAY)
        --DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DAY)
            )+1,'.'),''))
        )
        
    , with_row_numbers AS (
        SELECT ROW_NUMBER() OVER() AS pos,* 
        FROM splitted
        )
        
    , calendar_day AS (
        SELECT DATE_ADD(CAST('2021-10-12' AS DATE),
            --(SELECT DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), 
            INTERVAL (pos - 1) DAY) AS Date
        FROM  with_row_numbers)
    
    SELECT DISTINCT ROW_NUMBER() OVER () AS row_number, Date,
          DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) AS max_date
    FROM calendar_day
    WHERE Date <= DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
    ORDER BY Date --DESC
    ));

WHILE i < runLength DO
    SET i = i + 1;
    
        WITH splitted AS (
            SELECT *
            FROM UNNEST( SPLIT(RPAD('',(SELECT DATE_DIFF(DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(),Month), INTERVAL 1 MONTH),INTERVAL 1 DAY),CAST('2021-10-12' AS DATE),DAY)
            --DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DAY)
                )+1,'.'),''))
            )
        
        , with_row_numbers AS (
            SELECT ROW_NUMBER() OVER() AS pos,* 
            FROM splitted
            )
            
        , calendar_day AS (
            SELECT DATE_ADD(CAST('2021-10-12' AS DATE),
                --(SELECT DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)), 
                INTERVAL (pos - 1) DAY) AS Date
            FROM  with_row_numbers)
        
        , dates AS (
            SELECT DISTINCT ROW_NUMBER() OVER () AS row_num, Date,
                  DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) AS max_date
            FROM calendar_day
            WHERE Date <= DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
            ORDER BY Date --DESC
            )
            
        SELECT Date FROM dates WHERE row_num = i
        );
    
    INSERT INTO `gusa-funnel-dwh.Tiktok.Tiktok_Userbase_Cohort_D30` (base_date, base_visitorId, base_visit, campaign, adContent, keyword, device)
    (
    SELECT base_date, base_visitorId, base_visit, campaign, adContent, keyword, device FROM (
        SELECT DISTINCT DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS base_date, fullVisitorId AS base_visitorId,
            FIRST_VALUE(visitNumber) OVER (PARTITION BY DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles"), fullvisitorId ORDER BY VISITSTARTTIME) AS base_visit,
            FIRST_VALUE(trafficSource.campaign) OVER (PARTITION BY DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles"), fullvisitorId ORDER BY VISITSTARTTIME) AS campaign,
            FIRST_VALUE(trafficSource.adContent) OVER (PARTITION BY DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles"), fullvisitorId ORDER BY VISITSTARTTIME) AS adContent,
            FIRST_VALUE(trafficSource.keyword) OVER (PARTITION BY DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles"), fullvisitorId ORDER BY VISITSTARTTIME) AS keyword,
            FIRST_VALUE(REPLACE(device.devicecategory,'tablet','desktop')) OVER (PARTITION BY DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles"), fullvisitorId ORDER BY VISITSTARTTIME) AS device,
            DATE_DIFF(DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles"),last_visit,DAY) AS days,
        FROM `gusa-dwh.12571860.ga_sessions_*` ga
        LEFT JOIN (SELECT base_visitorId AS fullVisitorId,MAX(base_date) AS last_visit FROM `gusa-funnel-dwh.Tiktok.Tiktok_Userbase_Cohort_D30` GROUP BY 1) U USING(fullVisitorId)
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') = FORMAT_DATE('%Y%m%d',runDate)
            AND LOWER(trafficsource.source) LIKE '%tiktok%' AND LOWER(trafficsource.medium) NOT LIKE '%referrer%'
        )
    WHERE days >=30 OR days IS NULL
    );
    
END WHILE;




--DELETE FROM `gusa-funnel-dwh.Tiktok.Tiktok_Userbase_Cohort_D30` WHERE TRUE;