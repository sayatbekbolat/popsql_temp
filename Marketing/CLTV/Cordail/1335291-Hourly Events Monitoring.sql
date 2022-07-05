SELECT  
            DATETIME_TRUNC(DATETIME(TIMESTAMP(time)),HOUR) As DateTime, A.action AS Type,
            DATETIME(TIMESTAMP_ADD(DATETIME_TRUNC(DATETIME(TIMESTAMP(time)),HOUR), INTERVAL UTC_2_PST HOUR)) Datetime_PST,
            DATETIME(TIMESTAMP_ADD(DATETIME_TRUNC(DATETIME(TIMESTAMP(time)),HOUR), INTERVAL UTC_2_EST HOUR)) Datetime_EST,
            DATETIME(TIMESTAMP_ADD(DATETIME_TRUNC(DATETIME(TIMESTAMP(time)),HOUR), INTERVAL B.UTC_2_ILS HOUR)) Datetime_ILS,
             COUNT(*) AS Amount
      FROM `gusa-bronto-dwh.Cordail_Data.message_activity` A
      LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` B ON DATE(TIMESTAMP(time)) =  Date
      WHERE DATE(TIMESTAMP(time)) > DATE_SUB(CURRENT_DATE() ,INTERVAL 10 DAY)
      GROUP BY 1,2,3,4,5

      UNION ALL

      SELECT  
            DATETIME_TRUNC(DATETIME(TIMESTAMP(time)),HOUR) As DateTime, 'Message_Sent' AS Type,
            DATETIME(TIMESTAMP_ADD(DATETIME_TRUNC(DATETIME(TIMESTAMP(time)),HOUR), INTERVAL UTC_2_PST HOUR)) Datetime_PST,
            DATETIME(TIMESTAMP_ADD(DATETIME_TRUNC(DATETIME(TIMESTAMP(time)),HOUR), INTERVAL UTC_2_EST HOUR)) Datetime_EST,
            DATETIME(TIMESTAMP_ADD(DATETIME_TRUNC(DATETIME(TIMESTAMP(time)),HOUR), INTERVAL B.UTC_2_ILS HOUR)) Datetime_ILS,
             COUNT(*) AS Amount
      FROM `gusa-bronto-dwh.Cordail_Data.message_activity_sent` A
      LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` B ON DATE(TIMESTAMP(time)) =  Date
      WHERE DATE(TIMESTAMP(time)) > DATE_SUB(CURRENT_DATE() ,INTERVAL 10 DAY)
      GROUP BY  1,2,3,4,5

      ORDER BY 5,1 DESC

      /**/