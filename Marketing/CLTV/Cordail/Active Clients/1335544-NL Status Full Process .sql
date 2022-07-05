DECLARE startDate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);
DECLARE endDate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY);

----------------------------------/ NL_Sub_Creation_Date /----------------------------------



INSERT INTO `gusa-bronto-dwh.NL_Data_2021.NL_Sub_Creation_Date` ( subscriber_email, Datetime_Created) 
(
    WITH First_sub_date_Magento AS (
        SELECT  
            LOWER(subscriber_email) AS subscriber_email,
            CAST(MIN(change_status_at) AS DATETIME) AS  Datetime_Created, 
        FROM  `gusa-dwh.backend.newsletter_subscriber`  
        LEFT JOIN `gusa-dwh.backend.SubscriberStatus_Mapping` ON subscriber_status=ID
        WHERE Name = 'SUBSCRIBED'
            AND CAST(change_status_at As DATE) >= startDate      
            AND LOWER(subscriber_email) NOT IN (SELECT subscriber_email FROM `gusa-bronto-dwh.NL_Data_2021.NL_Sub_Creation_Date`)
        GROUP BY 1
    )
    , cordial_data AS ( 
        SELECT 
            LOWER(channels_email_address) AS subscriber_email, 
            DATETIME_ADD(DATETIME(TIMESTAMP(IFNULL(channels_email_subinfo_ts,channels_email_unsubinfo_ts))),INTERVAL UTC_2_PST HOUR) AS Datetime_Created
        FROM `gusa-bronto-dwh.NL_Data_2021.Cordail_Contacts_Daily_Activity` A
        LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` B ON B.Date = DATE(TIMESTAMP(IFNULL(channels_email_subinfo_ts,channels_email_unsubinfo_ts)))
        WHERE DATE(_PARTITIONTIME) >= startDate 
            AND LOWER(channels_email_address) NOT IN (SELECT DISTINCT LOWER(subscriber_email) FROM `gusa-bronto-dwh.NL_Data_2021.NL_Sub_Creation_Date`)
            AND LOWER(channels_email_address) NOT IN (SELECT DISTINCT subscriber_email FROM First_sub_date_Magento)

    )

    SELECT  subscriber_email, Datetime_Created FROM  First_sub_date_Magento 

    UNION ALL

    SELECT subscriber_email, Datetime_Created FROM cordial_data

)
;
----------------------------------/ NL_Sub_change date /----------------------------------

MERGE INTO `gusa-bronto-dwh.NL_Data_2021.NL_Change_Date` T USING (
WITH Chanaged_status_sub AS (
    SELECT 
        subscriber_email,
        Datetime_Changed,
        DATE(Datetime_Changed) AS Date_Changed,
        'SUB' AS Change_to
    FROM (
        SELECT DISTINCT 
            LOWER(channels_email_address) AS subscriber_email,
            DATETIME_ADD(DATETIME(TIMESTAMP(IFNULL(channels_email_subinfo_ts,channels_email_unsubinfo_ts))),INTERVAL UTC_2_PST HOUR) AS Datetime_Changed
        FROM `gusa-bronto-dwh.NL_Data_2021.Cordail_Contacts_Daily_Activity` 
         LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` B ON B.Date = DATE(TIMESTAMP(IFNULL(channels_email_subinfo_ts,channels_email_unsubinfo_ts)))
        WHERE DATE(TIMESTAMP(IFNULL(channels_email_subinfo_ts,channels_email_unsubinfo_ts))) >= startDate
            AND channels_email_subscribeStatus != 'subscribed'
    )
)
,Chanaged_status_unsub AS (

    SELECT 
        subscriber_email,
        Datetime_Changed,
        DATE(Datetime_Changed) AS Date_Changed,
        'UNSUB' AS Change_to
    FROM (
        SELECT DISTINCT 
            LOWER(channels_email_address) AS subscriber_email,
            DATETIME(TIMESTAMP(IFNULL(channels_email_subinfo_ts,channels_email_unsubinfo_ts))) AS Datetime_Changed,
        FROM `gusa-bronto-dwh.NL_Data_2021.Cordail_Contacts_Daily_Activity` 
        WHERE DATE(TIMESTAMP(IFNULL(channels_email_subinfo_ts,channels_email_unsubinfo_ts))) >= startDate
            AND channels_email_subscribeStatus != 'unsubscribed'
    )
)
    
    
    SELECT DISTINCT 
        subscriber_email,  
        FIRST_VALUE(Datetime_Changed) OVER (PARTITION BY subscriber_email, Date_Changed ORDER BY Datetime_Changed DESC) AS Datetime_Changed,   
        'SUB' AS Change_to
    FROM `gusa-bronto-dwh.NL_Data_2021.NL_Sub_Creation_Date` 
    INNER JOIN Chanaged_status_sub USING (subscriber_email)
    
  
    UNION ALL

    SELECT DISTINCT 
        subscriber_email,  
        FIRST_VALUE(Datetime_Changed) OVER (PARTITION BY subscriber_email, Date_Changed ORDER BY Datetime_Changed DESC) AS Datetime_Changed,   
        'UNSUB' AS Change_to
    FROM `gusa-bronto-dwh.NL_Data_2021.NL_Sub_Creation_Date` 
    INNER JOIN Chanaged_status_unsub USING (subscriber_email)

    
) S 

ON T.subscriber_email = S.subscriber_email AND T.Datetime_Changed=S.Datetime_Changed

WHEN NOT MATCHED THEN INSERT( subscriber_email, Datetime_Changed, Change_to ) VALUES ( subscriber_email, Datetime_Changed, Change_to)

;
-----------------------------------/ Active_Clients_All /----------------------------------

MERGE INTO `gusa-bronto-dwh.NL_Data_2021.Active_Clients_All` T
USING (
WITH splitted AS (
    SELECT *
    FROM UNNEST( SPLIT(RPAD('',(SELECT DATE_DIFF(DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(),Month), INTERVAL 1 MONTH),INTERVAL 1 DAY),CAST('2008-01-01' AS DATE),DAY)    )+1,'.'),''))),with_row_numbers AS (
    SELECT ROW_NUMBER() OVER() AS pos,* FROM splitted
    )
,calendar_day AS (
    SELECT DATE_ADD(CAST('2008-01-01' AS DATE), INTERVAL (pos - 1) DAY) AS Date FROM  with_row_numbers)
,month_date AS (
    SELECT DISTINCT DATE_TRUNC(Date,MONTH) AS MonthID FROM calendar_day)
, Created_sub AS (
    SELECT subscriber_email AS customer_email,Datetime_Created,DATE(DATE_TRUNC(Datetime_Created,MONTH)) AS First_Sub_MonthID
    FROM `gusa-bronto-dwh.NL_Data_2021.NL_Sub_Creation_Date`
    )
,status_change AS (
    
        SELECT *,
            DATE(IFNULL(LEAD(Datetime_Changed_from) OVER (PARTITION BY customer_email ORDER BY Datetime_Changed ASC ),DATE_TRUNC(CURRENT_DATE(),MONTH) )) Datetime_Changed_to,
        FROM(
            SELECT customer_email,Datetime_Created AS Datetime_Changed, DATE(DATE_TRUNC(Datetime_Created,MONTH)) AS Datetime_Changed_from, 'Active' AS Change_to
            FROM Created_sub
  
            UNION ALL
            
            SELECT 
                subscriber_email AS customer_email,Datetime_Changed,  DATE(DATE_TRUNC(Datetime_Changed,MONTH)) AS Datetime_Changed_from, IF(Change_to="SUB","Active","inActive") AS Change_to
            FROM `gusa-bronto-dwh.NL_Data_2021.NL_Change_Date`
            --WHERE subscriber_email='sean.kantor@gmail.com'
            ) A 

)
,was_subed AS(
    SELECT customer_email, Datetime_Changed_from AS MonthID, TRUE AS Was_Sub
    FROM status_change
    WHERE Change_to = 'Active'
    )
,First_Order_date AS (
  SELECT DISTINCT 
    
    MIN(DATE_TRUNC(DATE(Creation_Datetime),MONTH)) AS Order_Date, 
    LOWER(Orders.customer_email) As customer_email, 
    --CASE WHEN Orders.EyewearType = 'Contact Lenses' THEN 'Contact Lenses' WHEN Orders.EyewearType IN ('Product Upgrade','Accessories') THEN 'Other' ELSE 'Eyeglasses'  END AS Product_Type, 
    IF(MIN(DATE_TRUNC(DATE(Creation_Datetime),MONTH))<MIN(First_Sub_MonthID),'inActive','Active') AS Status
    
  FROM `gusa-dwh.Admin.Order_Items` Orders
  LEFT JOIN Created_sub ON  LOWER(Orders.customer_email) =  LOWER(Created_sub.customer_email)
  WHERE LOWER(Orders.customer_email) != ""
  --AND Orders.customer_email='sean.kantor@gmail.com'
  GROUP BY 2
)


SELECT DISTINCT
    FD.customer_email, Order_date, month_date.MonthID,
    IFNULL(FIRST_VALUE(Change_to) OVER (PARTITION BY FD.customer_email,month_date.MonthID ORDER BY Datetime_Changed DESC),'inActive') AS Last_Status_in_month,
    Was_sub
FROM First_Order_date FD, month_date
LEFT JOIN status_change SC ON SC.customer_email =  FD.customer_email AND  Monthid Between Datetime_Changed_from AND Datetime_Changed_to----- us join to set status
LEFT JOIN was_subed WS  ON WS.customer_email =  FD.customer_email AND WS.Monthid=month_date.Monthid
WHERE --Product_Type = 'Contact Lenses'  AND 
    month_date.MonthID >= DATE_TRUNC(Order_Date,MONTH)
    --AND month_date.Monthid Between Datetime_Changed_from AND Datetime_Changed_to 
ORDER BY 1,MonthID
) S
ON T.customer_email = S.customer_email AND T.MonthID = S.MonthID 

WHEN NOT MATCHED THEN 
    INSERT (customer_email,Order_date, MonthID,Last_Status_in_month,Was_sub )
    VALUES (customer_email,Order_date, MonthID,Last_Status_in_month,Was_sub )

WHEN MATCHED AND (T.Last_Status_in_month != S.Last_Status_in_month OR IFNULL(T.Was_sub,TRUE) != IFNULL(S.Was_sub,TRUE))
    THEN UPDATE SET Last_Status_in_month = S.Last_Status_in_month, Was_sub = S.Was_sub  
     

;
--------------------------------- Active Clients CL ------------------------
MERGE INTO `gusa-bronto-dwh.NL_Data_2021.Active_Clients_CL` T
USING (
WITH splitted AS (
    SELECT *
    FROM UNNEST( SPLIT(RPAD('',(SELECT DATE_DIFF(DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(),Month), INTERVAL 1 MONTH),INTERVAL 1 DAY),CAST('2008-01-01' AS DATE),DAY)    )+1,'.'),''))
    )
,with_row_numbers AS (SELECT ROW_NUMBER() OVER() AS pos,* FROM splitted)
,calendar_day AS (
SELECT DATE_ADD(CAST('2008-01-01' AS DATE), INTERVAL (pos - 1) DAY) AS Date FROM  with_row_numbers)
,month_date AS (
    SELECT DISTINCT DATE_TRUNC(Date,MONTH) AS MonthID FROM calendar_day
    )
, Created_sub AS (
    SELECT subscriber_email AS customer_email,Datetime_Created,DATE(DATE_TRUNC(Datetime_Created,MONTH)) AS First_Sub_MonthID
    FROM `gusa-bronto-dwh.NL_Data_2021.NL_Sub_Creation_Date`
    )
,status_change AS (
    
        SELECT *,
            DATE(IFNULL(LEAD(Datetime_Changed_from) OVER (PARTITION BY customer_email ORDER BY Datetime_Changed ASC ),DATE_TRUNC(CURRENT_DATE(),MONTH) )) Datetime_Changed_to,
        FROM(
            SELECT customer_email,Datetime_Created AS Datetime_Changed, DATE(DATE_TRUNC(Datetime_Created,MONTH)) AS Datetime_Changed_from, 'Active' AS Change_to
            FROM Created_sub
            --WHERE customer_email='sean.kantor@gmail.com'
            UNION ALL
            
            SELECT 
                subscriber_email AS customer_email,Datetime_Changed,  DATE(DATE_TRUNC(Datetime_Changed,MONTH)) AS Datetime_Changed_from, IF(Change_to="SUB","Active","inActive") AS Change_to
            FROM `gusa-bronto-dwh.NL_Data_2021.NL_Change_Date`
            --WHERE subscriber_email='sean.kantor@gmail.com'
            ) A 

)
,was_subed AS(
    SELECT customer_email, Datetime_Changed_from AS MonthID, TRUE AS Was_Sub
    FROM status_change
    WHERE Change_to = 'Active'
    )
,First_Order_date AS (
  SELECT DISTINCT 
    
    MIN(DATE_TRUNC(DATE(Creation_Datetime),MONTH)) AS Order_Date, 
    LOWER(Orders.customer_email) As customer_email, 
    CASE 
        WHEN Orders.EyewearType = 'Contact Lenses' THEN 'Contact Lenses'
        WHEN Orders.EyewearType IN ('Product Upgrade','Accessories') THEN 'Other'
    ELSE 'Eyeglasses'
    END AS Product_Type, 
    IF(MIN(DATE_TRUNC(DATE(Creation_Datetime),MONTH))<MIN(First_Sub_MonthID),'inActive','Active') AS Status
    
  FROM `gusa-dwh.Admin.Order_Items` Orders
  LEFT JOIN Created_sub ON  LOWER(Orders.customer_email) =  LOWER(Created_sub.customer_email)
  WHERE LOWER(Orders.customer_email) != ""
  --AND Orders.customer_email='sean.kantor@gmail.com'
  GROUP BY 2,3
)


SELECT DISTINCT
    FD.customer_email, Order_date, month_date.MonthID,
    IFNULL(FIRST_VALUE(Change_to) OVER (PARTITION BY FD.customer_email,month_date.MonthID ORDER BY Datetime_Changed DESC),'inActive') AS Last_Status_in_month,
    Was_sub
FROM First_Order_date FD, month_date
LEFT JOIN status_change SC ON SC.customer_email =  FD.customer_email AND  Monthid Between Datetime_Changed_from AND Datetime_Changed_to----- us join to set status
LEFT JOIN was_subed WS  ON WS.customer_email =  FD.customer_email AND WS.Monthid=month_date.Monthid
WHERE Product_Type = 'Contact Lenses'
    --AND FD.customer_email='sean.kantor@gmail.com'
    AND month_date.MonthID >= DATE_TRUNC(Order_Date,MONTH)
    --AND Monthid Between Datetime_Changed_from AND Datetime_Changed_to 
ORDER BY 1,MonthID
) S
ON T.customer_email = S.customer_email AND T.MonthID = S.MonthID 

WHEN NOT MATCHED THEN 
    INSERT (customer_email,Order_date, MonthID,Last_Status_in_month,Was_sub )
    VALUES (customer_email,Order_date, MonthID,Last_Status_in_month,Was_sub )

WHEN MATCHED AND (T.Last_Status_in_month != S.Last_Status_in_month OR IFNULL(T.Was_sub,TRUE) != IFNULL(S.Was_sub,TRUE))
    THEN UPDATE SET Last_Status_in_month = S.Last_Status_in_month, Was_sub = S.Was_sub  
 ;   
/**/

------------------------------- Active client Glasses ------------------------------------------
MERGE INTO `gusa-bronto-dwh.NL_Data_2021.Active_Clients` T
USING (
WITH splitted AS (
    SELECT *
    FROM UNNEST( SPLIT(RPAD('',(SELECT DATE_DIFF(DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(),Month), INTERVAL 1 MONTH),INTERVAL 1 DAY),CAST('2008-01-01' AS DATE),DAY)    )+1,'.'),''))
    )
,with_row_numbers AS (SELECT ROW_NUMBER() OVER() AS pos,* FROM splitted)
,calendar_day AS (
SELECT DATE_ADD(CAST('2008-01-01' AS DATE), INTERVAL (pos - 1) DAY) AS Date FROM  with_row_numbers)
,month_date AS (
    SELECT DISTINCT DATE_TRUNC(Date,MONTH) AS MonthID FROM calendar_day
    )
, Created_sub AS (
    SELECT subscriber_email AS customer_email,Datetime_Created,DATE(DATE_TRUNC(Datetime_Created,MONTH)) AS First_Sub_MonthID
    FROM `gusa-bronto-dwh.NL_Data_2021.NL_Sub_Creation_Date`
    )
,status_change AS (
    
        SELECT *,
            DATE(IFNULL(LEAD(Datetime_Changed_from) OVER (PARTITION BY customer_email ORDER BY Datetime_Changed ASC ),DATE_TRUNC(CURRENT_DATE(),MONTH) )) Datetime_Changed_to,
        FROM(
            SELECT customer_email,Datetime_Created AS Datetime_Changed, DATE(DATE_TRUNC(Datetime_Created,MONTH)) AS Datetime_Changed_from, 'Active' AS Change_to
            FROM Created_sub
            --WHERE customer_email='sean.kantor@gmail.com'
            UNION ALL
            
            SELECT 
                subscriber_email AS customer_email,Datetime_Changed,  DATE(DATE_TRUNC(Datetime_Changed,MONTH)) AS Datetime_Changed_from, IF(Change_to="SUB","Active","inActive") AS Change_to
            FROM `gusa-bronto-dwh.NL_Data_2021.NL_Change_Date`
            --WHERE subscriber_email='sean.kantor@gmail.com'
            ) A 

)
,was_subed AS(
    SELECT customer_email, Datetime_Changed_from AS MonthID, TRUE AS Was_Sub
    FROM status_change
    WHERE Change_to = 'Active'
    )
,First_Order_date AS (
  SELECT DISTINCT 
    
    MIN(DATE_TRUNC(DATE(Creation_Datetime),MONTH)) AS Order_Date, 
    LOWER(Orders.customer_email) As customer_email, 
    CASE 
        WHEN Orders.EyewearType = 'Contact Lenses' THEN 'Contact Lenses'
        WHEN Orders.EyewearType IN ('Product Upgrade','Accessories') THEN 'Other'
    ELSE 'Eyeglasses'
    END AS Product_Type, 
    IF(MIN(DATE_TRUNC(DATE(Creation_Datetime),MONTH))<MIN(First_Sub_MonthID),'inActive','Active') AS Status
    
  FROM `gusa-dwh.Admin.Order_Items` Orders
  LEFT JOIN Created_sub ON  LOWER(Orders.customer_email) =  LOWER(Created_sub.customer_email)
  WHERE LOWER(Orders.customer_email) != ""
  --AND Orders.customer_email='sean.kantor@gmail.com'
  GROUP BY 2,3
)


SELECT DISTINCT
    FD.customer_email, Order_date, month_date.MonthID,
    IFNULL(FIRST_VALUE(Change_to) OVER (PARTITION BY FD.customer_email,month_date.MonthID ORDER BY Datetime_Changed DESC),'inActive') AS Last_Status_in_month,
    Was_sub
FROM First_Order_date FD, month_date
LEFT JOIN status_change SC ON SC.customer_email =  FD.customer_email AND  Monthid Between Datetime_Changed_from AND Datetime_Changed_to----- us join to set status
LEFT JOIN was_subed WS  ON WS.customer_email =  FD.customer_email AND WS.Monthid=month_date.Monthid
WHERE Product_Type = 'Eyeglasses'
    --AND FD.customer_email='sean.kantor@gmail.com'
    AND month_date.MonthID >= DATE_TRUNC(Order_Date,MONTH)
    --AND Monthid Between Datetime_Changed_from AND Datetime_Changed_to 
ORDER BY 1,MonthID
) S
ON T.customer_email = S.customer_email AND T.MonthID = S.MonthID 

WHEN NOT MATCHED THEN 
    INSERT (customer_email,Order_date, MonthID,Last_Status_in_month,Was_sub )
    VALUES (customer_email,Order_date, MonthID,Last_Status_in_month,Was_sub )

WHEN MATCHED AND (T.Last_Status_in_month != S.Last_Status_in_month OR IFNULL(T.Was_sub,TRUE) != IFNULL(S.Was_sub,TRUE))
    THEN UPDATE SET Last_Status_in_month = S.Last_Status_in_month, Was_sub = S.Was_sub  
    
/**/