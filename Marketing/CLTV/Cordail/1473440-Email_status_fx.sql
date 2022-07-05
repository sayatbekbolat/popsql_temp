--DECLARE email STRING DEFAULT 'mbredd66@gmail.com';
--DECLARE eDate DATE DEFAULT '2022-07-01';

CREATE OR REPLACE FUNCTION `gusa-bronto-dwh.NL_Data_2021.Email_status_by_date`(email STRING,eDate DATE) 
RETURNS STRING AS (
  (SELECT DISTINCT FIRST_VALUE(channels_email_subscribeStatus) OVER (ORDER BY DATETIME_ADD(DATETIME(ts),INTERVAL UTC_2_PST HOUR) DESC )
    --DISTINCT channels_email_address, COUNT(*)
  FROM( 
    SELECT 
      channels_email_address, channels_email_subscribeStatus,
      IF(channels_email_subscribeStatus='unsubscribed',TIMESTAMP(channels_email_unsubinfo_ts),TIMESTAMP(channels_email_subinfo_ts)) ts 
    FROM `gusa-bronto-dwh.NL_Data_2021.Cordail_Contacts_DB`
    WHERE LOWER(channels_email_address) = LOWER(email)
    UNION ALL 
    SELECT 
      channels_email_address, channels_email_subscribeStatus, 
      IF(channels_email_subscribeStatus='unsubscribed',TIMESTAMP(channels_email_unsubinfo_ts),TIMESTAMP(channels_email_subinfo_ts)) ts 
    FROM `gusa-bronto-dwh.NL_Data_2021.Cordail_Contacts_Daily_Activity` 
    WHERE LOWER(channels_email_address) = LOWER(email)
  )
  LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` B ON B.Date = DATE(ts)
  WHERE DATETIME_ADD(DATETIME(ts),INTERVAL UTC_2_PST HOUR) < eDate
--LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON 
--GROUP BY 1
--HAVING COUNT(*) > 1
))