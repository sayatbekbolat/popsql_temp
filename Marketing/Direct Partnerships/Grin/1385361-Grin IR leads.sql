SELECT DISTINCT oid AS subscriber_id, CAST(TO_HEX(MD5(C.subscriber_email)) AS STRING) AS subscriber_email, action_date AS Datetime, A.subid4, Channel_Name
    FROM(
        SELECT 
        oid, DATE_TRUNC(CAST(action_date AS DATETIME),DAY) AS action_date, DATE_TRUNC(CAST(click_date AS DATETIME),DAY) AS click_date,  TRIM(media) AS media, subid1, subid2,subid3, subid4
        FROM `gusa-dc-dwh.IR_data.IR_Leads` 
        WHERE LOWER(media) LIKE '%creatorsgm-lp2774%' OR LOWER(media) LIKE '%creators-lp2720%'
        ORDER BY 2 DESC
        ) A
    LEFT JOIN `gusa-dwh.backend.newsletter_subscriber` C ON oid = CAST(subscriber_id AS STRING)
    LEFT JOIN `gusa-funnel-dwh.Direct_Marketing.GRIN_sharedId_channel_list` ON LOWER(A.subid4) = LOWER(sharedId)
    INNER JOIN (
        SELECT oid, MAX(DATE_TRUNC(CAST(click_date AS DATETIME),DAY)) AS click_date, TRIM(media) AS media, subid4  
        FROM `gusa-dc-dwh.IR_data.IR_Leads` GROUP BY oid, media, subid4) USING(oid, media, subid4, click_date)
    WHERE TRIM(media) IN (
                    SELECT DISTINCT TRIM(IR_Media_Name) FROM `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming_LOC` 
                    WHERE IR_Media_Name IS NOT NULL)
        AND DATETIME_DIFF( action_date, click_date, DAY) <= 90