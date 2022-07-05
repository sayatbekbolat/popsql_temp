DECLARE days INT64 DEFAULT 10;   

DECLARE dateUpdate DATE DEFAULT DATE_SUB(CURRENT_DATE(),INTERVAL days DAY);

DELETE FROM `gusa-bronto-dwh.newsletter_base_date.Email_segment_LOC` WHERE TRUE;
INSERT INTO `gusa-bronto-dwh.newsletter_base_date.Email_segment_LOC` (Newsletter_Category,Newsletter_Segment)
(SELECT string_field_0 AS Newsletter_Category, string_field_1 AS Newsletter_Segment FROM `gusa-bronto-dwh.newsletter_base_date.Email_segment`)
;

DELETE FROM `gusa-bronto-dwh.newsletter_base_date.Banner_fixed_Name_LOC` WHERE TRUE;
INSERT INTO `gusa-bronto-dwh.newsletter_base_date.Banner_fixed_Name_LOC` (Link,New_Name,Type,Location)
(SELECT string_field_0 AS Link, string_field_1 AS New_Name, string_field_2 AS Type, string_field_3 AS Location FROM `gusa-bronto-dwh.newsletter_base_date.Banner_fixed_Name`)
;

----------------------------------------// Coredail  Message Sent //----------------------------------------------------------------

DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_sent` WHERE DATE(Sent_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_sent` 
(Sent_datetime_UTC,Sent_Datetime_PST,email, action, message_name, mdtID, msID, experiment, variant, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
(
  SELECT Sent_datetime_UTC,Sent_Datetime_PST,email, action, message_name, mdtID, msID, experiment, variant,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_no_of_order`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS noOfOrders,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_time_since_last_order`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS daySinceLastOrder,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_product_type`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS productType,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_lens_type`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS lensType,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_brand_type`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS brandType ,
    IFNULL(mTags,'["None"]') AS mTags
    FROM (
          SELECT DISTINCT 
              DATETIME(TIMESTAMP(time)) AS Sent_datetime_UTC, 
              DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) Sent_Datetime_PST,
              email, action, message_name, mdtID, msID, experiment, variant, mTags
          FROM `gusa-bronto-dwh.Cordail_Data.message_activity_sent`
          LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
          WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate       
            AND email IS NOT NULL 
            AND DATETIME(TIMESTAMP(time)) IS NOT NULL 
        ) 
) 

;


----------------------------------------// Coredail  Message Open //----------------------------------------------------------------

DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_open` WHERE DATE(Open_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_open` (Open_datetime_UTC, Open_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags) 
--MERGE INTO `gusa-bronto-dwh.Cordail_Data.message_open` T USING
( 
    SELECT DISTINCT 
      DATETIME(TIMESTAMP(time)) AS Open_datetime_UTC,
      DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) Open_Datetime_PST,
      Sent_Datetime_UTC, Sent_Datetime_PST, email, message_name,  msID,  A.Device_type AS Device, A.Device_platform AS Device_OS,
      noOfOrders, daySinceLastOrder, productType, lensType, brandType,
      IFNULL(mTags,'["None"]') AS mTags
    FROM `gusa-bronto-dwh.Cordail_Data.message_activity_new` A 
    LEFT JOIN `gusa-bronto-dwh.Cordail_Data.message_sent` B USING (email, message_name, msID)
    LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
    WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate 
      AND DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) < CURRENT_DATE()
      AND Sent_Datetime_PST IS NOT NULL
      AND A.action = 'open'
      AND email IS NOT NULL 
)


;

----------------------------------------// Coredail Message Clicks //----------------------------------------------------------------
DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_clicks` WHERE DATE(click_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_clicks` (click_datetime_UTC, click_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, linkUrl, linkname, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)

(
  SELECT DISTINCT 
    DATETIME(TIMESTAMP(time)) AS click_datetime_UTC,  
    DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) click_Datetime_PST,
    Sent_Datetime_UTC,  Sent_Datetime_PST,
    email, message_name, msID, link AS linkUrl, data_crdl_key AS linkname, A.Device_type AS Device, A.Device_platform AS Device_OS,
    noOfOrders, daySinceLastOrder, productType, lensType, brandType,
    IFNULL(mTags,'["None"]') AS mTags
  FROM `gusa-bronto-dwh.Cordail_Data.message_activity` A
  LEFT JOIN `gusa-bronto-dwh.Cordail_Data.message_sent` B USING (email, message_name,msID)
  LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
  WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate
    AND DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) < CURRENT_DATE()
    AND A.action = 'click'
    AND Sent_Datetime_UTC IS NOT NULL
    AND email IS NOT NULL
) 


;
----------------------------------------// Coredail Message optout //----------------------------------------------------------------

DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_optout` WHERE DATE(optout_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_optout` (optout_datetime_UTC, optout_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType)

(
  SELECT DISTINCT 
    DATETIME(TIMESTAMP(time)) AS optout_datetime_UTC,  
    DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) optout_Datetime_PST,
    Sent_Datetime_UTC,  Sent_Datetime_PST,
    email, message_name, msID, A.Device_type AS Device, A.Device_platform AS Device_OS,
    noOfOrders, daySinceLastOrder, productType, lensType, brandType,

  FROM `gusa-bronto-dwh.Cordail_Data.message_activity` A
  LEFT JOIN `gusa-bronto-dwh.Cordail_Data.message_sent` B USING (email, message_name,msID)
  LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
  WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate
    AND DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) < CURRENT_DATE()
    AND A.action = 'optout'
    AND Sent_Datetime_UTC IS NOT NULL
    AND email IS NOT NULL
) 


;


----------------------------------------// Coredail Message bounce //----------------------------------------------------------------
DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_bounce` WHERE DATE(bounce_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_bounce`  ( bounce_datetime_UTC, bounce_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, noOfOrders, daySinceLastOrder, productType, lensType, brandType)

(
  SELECT DISTINCT 
    DATETIME(TIMESTAMP(time)) AS bounce_datetime_UTC,  
    DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) bounce_Datetime_PST,
    Sent_Datetime_UTC,  Sent_Datetime_PST,
    email, message_name, msID,
    noOfOrders, daySinceLastOrder, productType, lensType, brandType
  FROM `gusa-bronto-dwh.Cordail_Data.message_activity` A
  LEFT JOIN `gusa-bronto-dwh.Cordail_Data.message_sent` B USING (email, message_name,msID)
  LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
  WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate
    AND DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) < CURRENT_DATE()
    AND A.action = 'bounce'
    AND Sent_Datetime_UTC IS NOT NULL
    AND email IS NOT NULL
) 

;

----------------------------------------// Coredail Message Click Order //---------------------------------------------------------------


DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_click_order` WHERE DATE(ActionDateTime) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_click_order` (EmailAddress, DeliveryDate, ActionDateTime, MessageName, mTags,noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , 
          Date, Date_Time, Revenue, Rank, Coupon, linkUrl , Order_Att, linkname,Revenue_Contacts)
 
(
WITH custs AS (  
    SELECT email AS EmailAddress, DeliveryDate,  ActionDateTime, message_name AS MessageName,mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType, linkUrl, linkname
    FROM (
        SELECT DISTINCT email , Sent_Datetime_PST AS DeliveryDate, 
                        click_Datetime_PST AS  ActionDateTime, 
                        message_name , noOfOrders, daySinceLastOrder, productType, lensType, brandType, linkUrl, linkname , Device, Device_OS,
                        mTags
        FROM `gusa-bronto-dwh.Cordail_Data.message_clicks` 
        WHERE DATE(click_Datetime_PST) >= dateUpdate
        ) 
  )
  , Admin_orders AS (
        SELECT 
          customer_email, CAST(Order_Number AS STRING) AS Order_id, DATE(Creation_Datetime) As Date, DATETIME(Creation_Datetime) AS Date_Time, 
          SUM(itemRevenue) As Revenue, IFNULL(Order_Coupon ,'No Coupon') AS Coupon,
          SUM(IF(EyewearType='Contact Lenses',itemRevenue,0)) AS Revenue_Contacts
        FROM `gusa-dwh.Admin.Order_Items` 
        WHERE DATE(Creation_Datetime) >= dateUpdate 
        AND LOWER(customer_email) IN (SELECT DISTINCT LOWER(EmailAddress) FROM custs)
        GROUP BY 1,2,3,4,6
   )
   , GA_Sales AS (
      SELECT *
      FROM `gusa-funnel-dwh.Assist_Tables.Order_id_last_ckick_media` 
      WHERE Order_id IN (SELECT DISTINCT Order_id FROM Admin_orders)
      AND LOWER(media) LIKE '%newsletter%'
  )
  --, Joined_Data AS (
    SELECT EmailAddress, DeliveryDate, ActionDateTime, MessageName, mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , 
          Date, Date_Time, Revenue, Rank, Coupon, linkUrl , Order_Att, linkname,Revenue_Contacts
    FROM (
      SELECT 
        EmailAddress, DeliveryDate, ActionDateTime, MessageName,mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , Date, Date_Time, Revenue, linkUrl,linkname, Coupon,  
        IF(GA_Sales.Order_id IS NULL, 'Assisted','Last Click') AS Order_Att,Revenue_Contacts,
        RANK() OVER(PARTITION BY EmailAddress,Order_id ORDER BY DeliveryDate ASC,ActionDateTime ASC) AS Rank
      FROM custs
      LEFT JOIN Admin_orders ON LOWER(EmailAddress)=LOWER(customer_email) --AND DATETIME_ADD(ActionDateTime, INTERVAL 3 HOUR)  = Admin_orders.Date_Time
      LEFT JOIN GA_Sales USING (Order_id)
      WHERE Date_Time >= ActionDateTime AND DATETIME_DIFF(Date_Time,ActionDateTime,MINUTE) BETWEEN 0 AND 480 
    )
    WHERE Rank = 1
)

;
 
--------------------/ Coredail  Full Table Update / ---------------------- 
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_cordail` WHERE DeliveryDate >= dateUpdate;

MERGE INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_cordail` T USING (

WITH message_list AS (
        SELECT DISTINCT MessageName, DeliveryDate
        FROM (
                ----- Cordail Data
                SELECT DISTINCT message_name AS MessageName, DATE(Sent_Datetime_PST) AS DeliveryDate FROM `gusa-bronto-dwh.Cordail_Data.message_sent`  WHERE Sent_Datetime_PST >= dateUpdate  
                UNION ALL
                SELECT DISTINCT message_name AS MessageName, DATE(Sent_Datetime_PST) AS DeliveryDate FROM `gusa-bronto-dwh.Cordail_Data.message_open` WHERE Open_Datetime_PST >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT message_name AS MessageName, DATE(Sent_Datetime_PST) AS DeliveryDate FROM `gusa-bronto-dwh.Cordail_Data.message_clicks` WHERE DATE( click_Datetime_PST ) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT message_name AS MessageName, DATE(Sent_Datetime_PST) AS DeliveryDate FROM `gusa-bronto-dwh.Cordail_Data.message_optout` WHERE DATE( optout_Datetime_PST ) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT message_name AS MessageName, DATE(Sent_Datetime_PST) AS DeliveryDate FROM `gusa-bronto-dwh.Cordail_Data.message_bounce` WHERE DATE( bounce_Datetime_PST ) >= dateUpdate
                
            )
    )
   ,delivery AS (

        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate,  message_name AS MessageName, IFNULL(mTags,'["None"]') AS mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType, COUNT(*) AS Sent
        FROM `gusa-bronto-dwh.Cordail_Data.message_sent` A      
        INNER JOIN message_list MN ON A.Message_Name=MN.MessageName AND DATE(Sent_Datetime_PST) =DeliveryDate
        GROUP BY 1, 2,3,4,5,6,7,8
    )
   ,opens AS (
        
        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate,message_name AS MessageName,IFNULL(mTags,'["None"]') AS mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS open, COUNT(DISTINCT Email) AS Unique_open, SUM(IF( DATE(Sent_Datetime_PST)= DATE(Open_Datetime_PST), 1,0)) AS Open_D0, 
               COUNT(DISTINCT IF(DATE(Sent_Datetime_PST)= DATE(Open_Datetime_PST), Email ,NULL)) AS Unique_open_D0
        FROM `gusa-bronto-dwh.Cordail_Data.message_open` A
        INNER JOIN message_list MN ON MessageName=message_name AND MN.DeliveryDate= DATE(Sent_Datetime_PST)
        WHERE DATE(Sent_Datetime_PST) IS NOT NULL
        GROUP BY 1, 2, 3,4,5,6,7 ,8
    )
    ,clicks AS (
        
        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate,message_name AS MessageName, IFNULL(mTags,'["None"]') AS mTags,noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS clicks, COUNT(DISTINCT email) AS Unique_clicks, 
               SUM(IF(DATE(Sent_Datetime_PST)= DATE(Click_Datetime_PST), 1,0)) AS clicks_D0, COUNT(DISTINCT IF(DATE(Sent_Datetime_PST)= DATE(Click_Datetime_PST), Email ,NULL)) AS Unique_click_D0
        FROM `gusa-bronto-dwh.Cordail_Data.message_clicks` A
        INNER JOIN message_list MN ON MessageName=message_name AND MN.DeliveryDate= DATE(Sent_Datetime_PST)
        WHERE DATE(Sent_Datetime_PST) IS NOT NULL
        GROUP BY 1, 2, 3,4,5,6,7 ,8
       
        
    )
     , click_order AS (
        
        SELECT DATE(A.DeliveryDate) AS DeliveryDate, A.MessageName,IFNULL(mTags,'["None"]') AS mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS orders, SUM(Revenue) AS revenue, 
               SUM(IF(DATE(A.DeliveryDate)=DATE(ActionDateTIME), 1,0)) AS Orders_D0, 
               SUM(IF(DATE(A.DeliveryDate)=DATE(ActionDateTIME), revenue,0)) AS revenue_D0,        
               SUM(Revenue_Contacts) As Contacts_Revenue,
               SUM(IF(DATE(A.DeliveryDate)=DATE(ActionDateTIME), Revenue_Contacts,0)) AS Contacts_Revenue_D0,     
        FROM `gusa-bronto-dwh.Cordail_Data.message_click_order` A
        INNER JOIN message_list MN ON A.MessageName=MN.MessageName AND DATE(A.DeliveryDate)=MN.DeliveryDate
        WHERE DATE(A.DeliveryDate) IS NOT NULL
        GROUP BY 1, 2, 3,4,5,6,7 ,8
        
    )
    ,unsub AS (
        
        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate, MessageName,'["None"]' AS mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS unsub
        FROM `gusa-bronto-dwh.Cordail_Data.message_optout` A
        INNER JOIN message_list MN ON MessageName=message_name AND MN.DeliveryDate= DATE(Sent_Datetime_PST)
        WHERE DATE(Sent_Datetime_PST) IS NOT NULL
        GROUP BY 1, 2, 3,4,5,6,7 ,8
        
    )
    ,bounce AS (
        
        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate, MessageName, '["None"]' AS mTags,noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS bounce, 0 AS hard_bounce, 0 AS soft_bounce
        FROM `gusa-bronto-dwh.Cordail_Data.message_bounce`
        INNER JOIN message_list MN ON MessageName=message_name AND MN.DeliveryDate= DATE(Sent_Datetime_PST)
        WHERE DATE(Sent_Datetime_PST) IS NOT NULL
        GROUP BY 1, 2, 3,4,5,6,7 ,8
       
    )
        SELECT 'Cordail' AS Platform,*
        FROM delivery
        LEFT JOIN opens USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
        LEFT JOIN clicks USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
        LEFT JOIN click_order USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
        LEFT JOIN unsub USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
        LEFT JOIN bounce USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
  --      WHERE Sent IS NOT NULL AND open IS NOT NULL AND clicks IS NOT NULL AND unsub IS NOT NULL AND bounce IS NOT NULL
          
) S
ON S.DeliveryDate=T.DeliveryDate AND T.MessageName=S.MessageName AND IFNULL(T.noOfOrders,'zzz')  = IFNULL(S.noOfOrders,'zzz')  AND IFNULL(T.daySinceLastOrder,'zzz') = IFNULL(S.daySinceLastOrder,'zzz')  AND IFNULL(T.productType,'zzz') = IFNULL(S.productType,'zzz') AND IFNULL(T.lensType,'zzz')  = IFNULL(S.lensType,'zzz') AND IFNULL(T.brandType,'zzz') = IFNULL(S.brandType,'zzz') AND IFNULL(T.mTags,'zzz')=IFNULL(S.mTags,'zzz')

WHEN NOT MATCHED THEN 
INSERT (Platform,DeliveryDate,MessageName,mTags,noOfOrders,daySinceLastOrder,productType,lensType,brandType,sent,open,Unique_open,Open_D0,Unique_open_D0,
        clicks,Unique_clicks,clicks_D0,Unique_click_D0,unsub,bounce,hard_bounce,soft_bounce,orders,revenue,orders_D0,revenue_D0,Contacts_Revenue,Contacts_Revenue_D0)
VALUES (Platform, DeliveryDate,MessageName,mTags,noOfOrders,daySinceLastOrder,productType,lensType,brandType,sent,open,Unique_open,Open_D0,Unique_open_D0,
        clicks,Unique_clicks,clicks_D0,Unique_click_D0,unsub,bounce,hard_bounce,soft_bounce,orders,revenue,orders_D0,revenue_D0,Contacts_Revenue,Contacts_Revenue_D0)

WHEN MATCHED THEN UPDATE SET
 T.Sent = S.Sent, T.open = S.open, T.Unique_open = S.Unique_open, T.Open_D0 = S.Open_D0 , T.Unique_open_D0 = S.Unique_open_D0, 
 T.clicks = S.clicks, T.Unique_clicks = S.Unique_clicks, T.clicks_D0 = S.clicks_D0, T.Unique_click_D0 = S.Unique_click_D0,
 T.unsub = S.unsub,
 T.bounce = S.bounce, T.hard_bounce = S.hard_bounce, T.soft_bounce = S.soft_bounce,
 T.orders = S.orders, T.revenue = S.revenue, T.orders_D0 = S.orders_D0, T.revenue_D0 = S.revenue_D0,
 T.Contacts_Revenue = S.Contacts_Revenue, T.Contacts_Revenue_D0 = S.Contacts_Revenue_D0, T.mTags=S.mTags
 
;

-----------------------------/ full table Joined from View/------------------
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_table` WHERE TRUE;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_table`  
(Platform, Final_message_name, DeliveryDate, MessageName, mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Sent, open, Unique_open, Open_D0, Unique_open_D0, clicks, Unique_clicks, clicks_D0, Unique_click_D0, unsub, unsub_D0, bounce, hard_bounce, soft_bounce, orders, revenue, orders_D0, revenue_D0, message_name, Newsletter_Category, Message_Camapaign, Message_Date, Offer, Target_Audience, Segment, Event, Email_Length, Creative_Type, Glasses_Type, CMG, Subject_line, Preview_line, Call_to_Action, Button_Color, No_of_Products, orientation, time_slot, Time_Zone, AB_Test_Version,Order_Att,Contacts_Revenue,Contacts_Revenue_D0)
 (
SELECT Platform, Final_message_name, DeliveryDate, MessageName,mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Sent, open, Unique_open, Open_D0, Unique_open_D0, clicks, Unique_clicks, clicks_D0, Unique_click_D0, unsub, unsub_D0, bounce, hard_bounce,     
        soft_bounce, orders, revenue, orders_D0, revenue_D0, message_name, Newsletter_Category, Message_Camapaign, Message_Date, Offer, Target_Audience, Segment, Event, Email_Length, Creative_Type, Glasses_Type, CMG, Subject_line, Preview_line, Call_to_Action, 
        Button_Color, No_of_Products, orientation, time_slot, Time_Zone, AB_Test_Version ,Order_Att,Contacts_Revenue,Contacts_Revenue_D0 
FROM `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_view` 
WHERE DeliveryDate < CURRENT_DATE())
;

--------------------------------/Cordail full table update action date /--------------------  
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_action_date_Cordail` WHERE DATE(ActionDate) >= dateUpdate;
SELECT MAX(DATE(ActionDate)) FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_action_date_Cordail`; 

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_action_date_Cordail` (ActionDate,MessageName,mTags,noOfOrders,daySinceLastOrder,productType,lensType,brandType,open,Unique_open,clicks,Unique_clicks,orders,revenue,unsub,Contacts_Revenue)
 (
    WITH message_list AS (
        SELECT DISTINCT MessageName,mTags, ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType
        FROM (
                SELECT DISTINCT message_name AS MessageName,IFNULL(mTags,'["None"]') AS mTags, DATE(Sent_Datetime_PST) AS DeliveryDate, DATE(Open_Datetime_PST) AS ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                    FROM `gusa-bronto-dwh.Cordail_Data.message_open`  WHERE DATE(Open_Datetime_PST)  >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT message_name AS MessageName, IFNULL(mTags,'["None"]') AS mTags,DATE(Sent_Datetime_PST) AS DeliveryDate, DATE(Click_Datetime_PST) AS ActionDate ,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                  FROM `gusa-bronto-dwh.Cordail_Data.message_clicks` WHERE DATE(Click_Datetime_PST) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT MessageName, IFNULL(mTags,'["None"]') AS mTags,DeliveryDate, DATE(ActionDateTIME) AS ActionDate ,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                  FROM `gusa-bronto-dwh.Cordail_Data.message_click_order` WHERE DATE(ActionDateTIME) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT message_name AS MessageName,'["None"]' AS mTags, DATE(Sent_Datetime_PST) AS DeliveryDate, DATE(optout_Datetime_PST ) AS ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                  FROM `gusa-bronto-dwh.Cordail_Data.message_optout`  WHERE DATE(optout_Datetime_PST ) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT message_name AS MessageName, '["None"]' AS mTags,DATE(Sent_Datetime_PST) AS DeliveryDate, DATE(bounce_Datetime_PST) AS ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                  FROM `gusa-bronto-dwh.Cordail_Data.message_bounce`  WHERE DATE(bounce_Datetime_PST)  >= dateUpdate
            )
        WHERE MessageName IS NOT NULL -- AND MessageName = '16.8.20_Contacts-vip65_Retention_S4_D_L_I_MC_HB_SE_SE_SN_white_8_S_MO_EST_NONE'
    )
    ,opens AS (
        SELECT DATE(Open_Datetime_PST) AS ActionDate, message_name AS MessageName,IFNULL(A.mTags,'["None"]') AS mTags, A.noOfOrders, A.daySinceLastOrder, A.productType, A.lensType, A.brandType,
               COUNT(*) AS open, COUNT(DISTINCT Email) AS Unique_open
        FROM `gusa-bronto-dwh.Cordail_Data.message_open` A
        INNER JOIN message_list MN ON MN.MessageName=A.message_name AND MN.ActionDate=DATE(Open_Datetime_PST) AND MN.noOfOrders=A.noOfOrders 
          AND MN.daySinceLastOrder=A.daySinceLastOrder AND MN.lensType=A.lensType AND MN.brandType=A.brandType AND MN.mTags=A.mTags 
        GROUP BY 1, 2, 3,4,5,6,7,8
    )
    ,clicks_data AS (
       SELECT ActionDate, MessageName,mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType,clicks,Unique_clicks
       FROM (
              SELECT DATE(Click_Datetime_PST) AS ActionDate, message_name AS MessageName,IFNULL(mTags,'["None"]') AS mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
                    COUNT(*) AS clicks, COUNT(DISTINCT Email) AS Unique_clicks
              FROM `gusa-bronto-dwh.Cordail_Data.message_clicks`  A
              GROUP BY 1, 2, 3,4,5,6,7,8
              )
       INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
       
    )
    ,click_order AS (
        SELECT ActionDate, MessageName,mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType,orders,revenue,Contacts_Revenue
        FROM (
              SELECT CAST(A.ActionDateTIME AS DATE) AS ActionDate, A.MessageName, IFNULL(mTags,'["None"]') AS mTags,A.noOfOrders, A.daySinceLastOrder, A.productType, A.lensType, A.brandType, A.Order_Att,
                     COUNT(*) AS orders, SUM(Revenue) AS revenue,  SUM(Revenue_Contacts) As Contacts_Revenue           
              FROM `gusa-bronto-dwh.Cordail_Data.message_click_order` A 
              GROUP BY 1, 2, 3,4,5,6,7,8,9
              )
        INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
        
    )
    ,unsubs AS (
        SELECT ActionDate, MessageName, mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType,unsub FROM (
        SELECT DATE(optout_Datetime_PST ) aS ActionDate, message_name AS MessageName, '["None"]' AS mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS unsub      
        FROM `gusa-bronto-dwh.Cordail_Data.message_optout`
        GROUP BY 1,2,3,4,5,6,7,8
        )
        INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
        
    )
    
        SELECT ActionDate,MessageName,mTags, noOfOrders,daySinceLastOrder,productType,lensType,brandType,open,Unique_open,clicks,Unique_clicks,orders,revenue,unsub, Contacts_Revenue

        FROM message_list
        LEFT JOIN opens USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags) 
        LEFT JOIN clicks_data USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
        LEFT JOIN click_order USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
        LEFT JOIN unsubs USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,mTags)
  --      WHERE Sent IS NOT NULL AND open IS NOT NULL AND clicks IS NOT NULL AND unsub IS NOT NULL AND bounce IS NOT NULL
    
)
;  
 
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_table_action_date` WHERE TRUE;
 
INSERT INTO `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_table_action_date` 
( Platform, Final_message_name,ActionDate, MessageName, mTags,noOfOrders, daySinceLastOrder, productType, lensType, brandType, open, Unique_open, clicks, Unique_clicks, orders, revenue,  message_name, Newsletter_Category, Message_Camapaign, Message_Date, Offer, Target_Audience, Segment, Event, Email_Length, Creative_Type, Glasses_Type, CMG, Subject_line, Preview_line, Call_to_Action, Button_Color, No_of_Products, orientation, time_slot, Time_Zone, AB_Test_Version,unsub, Contacts_Revenue)
 (
SELECT Platform,Final_message_name,ActionDate, MessageName, mTags, noOfOrders, daySinceLastOrder, productType, lensType, brandType, open, Unique_open, clicks, Unique_clicks, orders, revenue,  message_name, Newsletter_Category, Message_Camapaign, Message_Date, Offer, Target_Audience, Segment, Event, Email_Length, Creative_Type, Glasses_Type, CMG, Subject_line, Preview_line, Call_to_Action, Button_Color, No_of_Products, orientation, time_slot, Time_Zone, AB_Test_Version, unsub ,Contacts_Revenue
FROM `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_view_action_date` 
WHERE ActionDate < CURRENT_DATE())

/**/