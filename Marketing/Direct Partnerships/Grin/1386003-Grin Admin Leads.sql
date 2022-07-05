SELECT 
    subscriber_id,
    DATE_TRUNC(CAST(FORMAT_TIMESTAMP('%Y-%m-%d %X',TIMESTAMP_ADD(change_status_at, INTERVAL -8 HOUR )) AS DATETIME),DAY) AS Datetime,
    subscriber_email,
    code AS Affid
FROM 
    (SELECT * FROM `gusa-dwh.backend.newsletter_subscriber` 
    LEFT JOIN `gusa-dwh.backend.customy_affiliatebanner_affiliate` ON affiliate_id = entity_id
    WHERE (LOWER(code) LIKE '%lp2774%' OR LOWER(code) LIKE '%lp2720%')
        AND CAST(FORMAT_TIMESTAMP('%Y-%m-%d %X',TIMESTAMP_ADD(change_status_at, INTERVAL -8 HOUR )) AS DATETIME) > '2022-05-01'
    ) A 
INNER JOIN  (SELECT DISTINCT Affid FROM `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming_LOC` WHERE Affid IS NOT NULL) B ON Affid = code




/*
SELECT * FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Leads`


SELECT * FROM `gusa-dwh.backend.newsletter_subscriber` 
LEFT JOIN `gusa-dwh.backend.customy_affiliatebanner_affiliate` ON affiliate_id = entity_id
WHERE subscriber_id = 18017746




/**/