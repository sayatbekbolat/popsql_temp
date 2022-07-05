DECLARE daysCohort INT64 DEFAULT 120;
DECLARE startDateCohort STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL daysCohort DAY));
DECLARE endDateCohort STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));




SELECT FrameType,EyewearType,LensType,
    SUM(Item_Qty_Ordered) AS items,
FROM `gusa-dwh.Admin.Order_Items`
WHERE CAST(Order_Number AS STRING) IN (
    SELECT DISTINCT
        hits.transaction.transactionId
    FROM `gusa-dwh.12571860.ga_sessions_*` ga, UNNEST(hits) AS hits
    LEFT JOIN (SELECT * FROM `gusa-funnel-dwh.Taboola.Taboola_Userbase_Cohort_D28` WHERE base_date >= PARSE_DATE('%Y%m%d',startDateCohort)) db 
        ON fullVisitorId = base_visitorId AND DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") >= base_date AND DATE_DIFF(DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles"),base_date,DAY) < 28  
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDateCohort AND endDateCohort
        AND visitNumber >= base_visit)
GROUP BY 1,2,3

/**/