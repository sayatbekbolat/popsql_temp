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

-------------------------// Delivery Date // -----------------------------------------

MERGE INTO `gusa-bronto-dwh.newsletter_base_date.Newsletter_Del_Date` T
USING 
(
  SELECT DISTINCT MessageName,
         DATE(CAST(SUBSTR(SPLIT(SUBSTR(MessageName,1,8),'.')[SAFE_OFFSET(2)],1,2) AS INT64)+2000,
         CAST(SPLIT(SUBSTR(MessageName,1,8),'.')[SAFE_OFFSET(1)] AS INT64),
         CAST(SPLIT(SUBSTR(MessageName,1,8),'.')[SAFE_OFFSET(0)] AS INT64)) AS  DDate    
  FROM (
        SELECT DISTINCT custom_optimaxeyewear_bronto_outboundactivities_messageName AS MessageName, REGEXP_REPLACE(SUBSTR(TRIM(custom_optimaxeyewear_bronto_outboundactivities_messageName),1,1),r'[0-9.]', '') AS Date
        FROM `gusa-bronto-dwh.Bronto_Data_Outbound.bronto_outbound_*` 
        WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m',DATE_SUB(CURRENT_DATE(),INTERVAL 30 DAY))  AND FORMAT_DATE('%Y%m',CURRENT_DATE())  
          AND custom_optimaxeyewear_bronto_outboundactivities_messageName IS NOT NULL
          AND custom_optimaxeyewear_bronto_outboundactivities_messageName != ''
          AND custom_optimaxeyewear_bronto_outboundactivities_messageName NOT LIKE '%test%'

        UNION ALL
        SELECT DISTINCT custom_optimaxeyewear_bronto_inboundactivities_messageName AS MessageName, REGEXP_REPLACE(SUBSTR(TRIM(custom_optimaxeyewear_bronto_inboundactivities_messageName),1,1),r'[0-9.]', '') AS Date
        FROM `gusa-bronto-dwh.Bronto_Data_Inbound.bronto_Inbound_*` 
        WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m',DATE_SUB(CURRENT_DATE(),INTERVAL 30 DAY))  AND FORMAT_DATE('%Y%m',CURRENT_DATE())  
          AND custom_optimaxeyewear_bronto_inboundactivities_messageName IS NOT NULL
          AND custom_optimaxeyewear_bronto_inboundactivities_messageName != ''
          AND custom_optimaxeyewear_bronto_inboundactivities_messageName NOT LIKE '%test%'
        )
  WHERE Date = ''
  AND MessageName IS NOT NULL
) S
ON S.messageName = T.messageName

WHEN NOT MATCHED THEN INSERT(messageName,DDate) VALUES(messageName,DDate)

;
---------------------------// Newsletter Deleviry // ----------------------------
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_del` WHERE  date >= dateUpdate;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_del` (date,messageName,deliveryType,noOfOrders, daySinceLastOrder,productType, lensType,brandType,  Sent )
(
    SELECT DISTINCT
        IF(Date>Create_Date,Date,Create_Date) AS date, 
        messageName, 
        deliveryType, 
        IF(noOfOrders IS NULL,'Unknown',noOfOrders) AS noOfOrders,
        IF(daySinceLastOrder IS NULL,'Unknown',daySinceLastOrder) AS daySinceLastOrder, 
        IF(productType IS NULL,'Unknown',productType) AS productType,
        IF(lensType IS NULL,'Unknown',lensType) AS lensType,
        IF(brandType IS NULL,'Unknown',brandType) AS brandType,
        COUNT(*) AS Sent
    FROM(
        SELECT 
            CAST(Date AS DATE) AS Date,
            IF(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart,"T")[offset(0)] AS DATE)) AS Create_Date,
            custom_optimaxeyewear_bronto_outboundactivities_emailAddress AS customer_mail,
            custom_optimaxeyewear_bronto_outboundactivities_messageName AS messageName,
            custom_optimaxeyewear_bronto_outboundactivities_deliveryType AS deliveryType,
            IFNULL(`gusa-dc-dwh.User_Seg_V2.email_no_of_order`(custom_optimaxeyewear_bronto_outboundactivities_emailAddress,IF(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart,"T")[offset(0)] AS DATE))),'Unknown') AS noOfOrders,
            IFNULL(`gusa-dc-dwh.User_Seg_V2.email_time_since_last_order`(custom_optimaxeyewear_bronto_outboundactivities_emailAddress,IF(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart,"T")[offset(0)] AS DATE))),'Unknown') AS daySinceLastOrder,
            IFNULL(`gusa-dc-dwh.User_Seg_V2.email_product_type`(custom_optimaxeyewear_bronto_outboundactivities_emailAddress,IF(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart,"T")[offset(0)] AS DATE))),'Unknown') AS productType,
            IFNULL(`gusa-dc-dwh.User_Seg_V2.email_lens_type`(custom_optimaxeyewear_bronto_outboundactivities_emailAddress,IF(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart,"T")[offset(0)] AS DATE))),'Unknown') AS lensType,
            IFNULL(`gusa-dc-dwh.User_Seg_V2.email_brand_type`(custom_optimaxeyewear_bronto_outboundactivities_emailAddress,IF(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_outboundactivities_deliveryStart,"T")[offset(0)] AS DATE))),'Unknown') AS brandType
        FROM  `gusa-bronto-dwh.Bronto_Data_Outbound.bronto_outbound_*`
        
        WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m',CAST(dateUpdate AS DATE)) AND FORMAT_DATE('%Y%m',CURRENT_DATE())  
        AND CAST(Date AS DATE) > (SELECT MAX(date) FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_del`)
        AND LOWER(custom_optimaxeyewear_bronto_outboundactivities_deliveryType) != 'test'--
     
    ) 
    GROUP BY 1,2,3,4,4,5,6,7,8
)


;
-------------------------// Message Open //---------------------------------------------

DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_open` WHERE ActionDate >= dateUpdate;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_open` (DeliveryDate,ActionDate,EmailAddress,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType)
(
SELECT DISTINCT
        DeliveryDate, ActionDate, EmailAddress, MessageName, 
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_no_of_order`(EmailAddress,DeliveryDate),'Unknown') AS noOfOrders,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_time_since_last_order`(EmailAddress,DeliveryDate),'Unknown') AS daySinceLastOrder,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_product_type`(EmailAddress,DeliveryDate),'Unknown') AS productType,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_lens_type`(EmailAddress,DeliveryDate),'Unknown') AS lensType,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_brand_type`(EmailAddress,DeliveryDate),'Unknown') AS brandType
FROM (   
    SELECT   
            IFNULL(DDate,IF(custom_optimaxeyewear_bronto_inboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_inboundactivities_deliveryStart,"T")[offset(0)] AS DATE)) )AS DeliveryDate,
            CAST(date AS DATE) AS ActionDate,
            custom_optimaxeyewear_bronto_inboundactivities_emailAddress AS EmailAddress,
            custom_optimaxeyewear_bronto_inboundactivities_messageName AS MessageName
    FROM `gusa-bronto-dwh.Bronto_Data_Inbound.bronto_Inbound_*` 
    LEFT JOIN `gusa-bronto-dwh.newsletter_base_date.Newsletter_Del_Date` ON MessageName=custom_optimaxeyewear_bronto_inboundactivities_messageName
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m',DATE_SUB(CURRENT_DATE(),INTERVAL 30 DAY))  AND FORMAT_DATE('%Y%m',CURRENT_DATE())  
    AND CAST(Date AS DATE) > (SELECT MAX(ActionDate) FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_open`)
    AND LOWER(custom_optimaxeyewear_bronto_inboundactivities_activityType) = 'open'
    --AND custom_optimaxeyewear_bronto_inboundactivities_messageName LIKE '8.3.20%'
    
    AND custom_optimaxeyewear_bronto_inboundactivities_messageName IS NOT NULL
  )
     
)
;

-------------------------// New Message click // -------------------------------------------

DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_click_new` WHERE ActionDateTime >= dateUpdate;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_click_new` (DeliveryDate,ActionDateTime,EmailAddress,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType,linkUrl,linkname)
(
WITH base_data AS (
      SELECT DISTINCT
              
             IFNULL(DDate,IF(custom_optimaxeyewear_bronto_inboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_inboundactivities_deliveryStart,"T")[offset(0)] AS DATE)) )AS DeliveryDate,
              DATETIME(CAST(date AS DATE),CAST(SUBSTR(custom_optimaxeyewear_bronto_inboundactivities_createdTime,1,8) AS TIME)) ActionDateTime,
              custom_optimaxeyewear_bronto_inboundactivities_emailAddress AS EmailAddress,
              custom_optimaxeyewear_bronto_inboundactivities_messageName AS MessageName,
              custom_optimaxeyewear_bronto_inboundactivities_linkUrl  AS linkUrl,
              custom_optimaxeyewear_bronto_inboundactivities_linkName AS linkname
      FROM `gusa-bronto-dwh.Bronto_Data_Inbound.bronto_Inbound_*` 
      LEFT JOIN `gusa-bronto-dwh.newsletter_base_date.Newsletter_Del_Date` ON MessageName=custom_optimaxeyewear_bronto_inboundactivities_messageName
      WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m',DATE_SUB(CURRENT_DATE(),INTERVAL 30 DAY))  AND FORMAT_DATE('%Y%m',CURRENT_DATE())  
      AND DATETIME(CAST(date AS DATE),CAST(SUBSTR(custom_optimaxeyewear_bronto_inboundactivities_createdTime,1,8) AS TIME)) > (SELECT MAX(ActionDateTime) FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_click_new`)
      AND LOWER(custom_optimaxeyewear_bronto_inboundactivities_activityType) = 'click'
      --AND LOWER(custom_optimaxeyewear_bronto_inboundactivities_messageName) LIKE '1.1.20%'
      AND custom_optimaxeyewear_bronto_inboundactivities_messageName IS NOT NULL
),
clicks AS (
        SELECT 
                DeliveryDate, ActionDateTime , EmailAddress, MessageName,linkUrl, linkname,
                IFNULL(`gusa-dc-dwh.User_Seg_V2.email_no_of_order`(EmailAddress,DeliveryDate),'Unknown') AS noOfOrders,
                IFNULL(`gusa-dc-dwh.User_Seg_V2.email_time_since_last_order`(EmailAddress,DeliveryDate),'Unknown') AS daySinceLastOrder,
                IFNULL(`gusa-dc-dwh.User_Seg_V2.email_product_type`(EmailAddress,DeliveryDate),'Unknown') AS productType,
                IFNULL(`gusa-dc-dwh.User_Seg_V2.email_lens_type`(EmailAddress,DeliveryDate),'Unknown') AS lensType,
                IFNULL(`gusa-dc-dwh.User_Seg_V2.email_brand_type`(EmailAddress,DeliveryDate),'Unknown') AS brandType,
                
        FROM base_data
       )   

SELECT DeliveryDate, ActionDateTime, EmailAddress, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,linkUrl,linkname
FROM clicks A
)
;

------------------------ // Click Orders //-----------------------------------------------------
DELETE FROm `gusa-bronto-dwh.newsletter_base_date.Newsletter_click_orders` WHERE CAST(ActionDateTime as DATE) >= dateUpdate;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.Newsletter_click_orders` (EmailAddress, DeliveryDate, ActionDateTime, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , 
Date, Date_Time, Revenue, Rank, Coupon, linkUrl,Order_Att,linkname,Revenue_Contacts)
(
WITH custs AS (  
  SELECT EmailAddress, DeliveryDate,  ActionDateTime, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, linkUrl, linkname
  FROM (
  SELECT DISTINCT EmailAddress, DeliveryDate, 
                  DATETIME_SUB(ActionDateTime, INTERVAL 3 HOUR) AS ActionDateTime, 
                  MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, linkUrl,linkname ,
    RANK() OVER(PARTITION BY EmailAddress,MessageName,DeliveryDate,IF(MessageName LIKE '%PPS%', ActionDateTime, DATETIME_SUB(ActionDateTime, INTERVAL 3 HOUR)) ORDER BY linkUrl ASC) AS Rank
  FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_click_new`  
  WHERE  DATETIME_SUB(ActionDateTime, INTERVAL 6 HOUR) >= DATETIME_SUB(CURRENT_DATETIME(),INTERVAL 400 DAY)
  ) WHERE RANK = 1
)
, Admin_orders AS (
      SELECT customer_email, CAST(Order_Number AS STRING) AS Order_id, DATE(Creation_Datetime) As Date, DATETIME(Creation_Datetime) AS Date_Time, SUM(itemRevenue) As Revenue, IFNULL(Order_Coupon ,'No Coupon') AS Coupon,
              SUM(IF(EyewearType='Contact Lenses',itemRevenue,0)) AS Revenue_Contacts
     -- SELECT DISTINCT EyewearType  
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
, Joined_Data AS (
  SELECT 
    EmailAddress, DeliveryDate, ActionDateTime, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , Date, Date_Time, Revenue, linkUrl,linkname, Coupon,  
    IF(GA_Sales.Order_id IS NULL, 'Assisted','Last Click') AS Order_Att,Revenue_Contacts
  FROM custs
  LEFT JOIN Admin_orders ON LOWER(EmailAddress)=LOWER(customer_email) --AND DATETIME_SUB(ActionDateTime, INTERVAL 3 HOUR)  = Admin_orders.Date_Time 
  LEFT JOIN GA_Sales USING (Order_id)
  WHERE Date_Time >= ActionDateTime
  AND CAST(Date_Time AS DATE) = CAST(ActionDateTime AS DATE)
  AND DATETIME_DIFF(Date_Time,ActionDateTime,MINUTE) <= 480 
 )

SELECT EmailAddress, DeliveryDate, ActionDateTime, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , 
        Date, Date_Time, Revenue, Rank, Coupon, linkUrl , Order_Att, linkname,Revenue_Contacts
FROM (SELECT RANK() OVER(PARTITION BY EmailAddress,Order_id ORDER BY ActionDateTime ASC) AS Rank, *  FROM Joined_Data )
WHERE Rank = 1
)

;
-------------------------// newsletter_unsub // -------------------------------------------
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_unsub` WHERE ActionDate >= dateUpdate;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_unsub` (DeliveryDate,ActionDate,EmailAddress,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType)
(
SELECT 
        DeliveryDate, ActionDate, EmailAddress, MessageName, 
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_no_of_order`(EmailAddress,DeliveryDate),'Unknown') AS noOfOrders,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_time_since_last_order`(EmailAddress,DeliveryDate),'Unknown') AS daySinceLastOrder,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_product_type`(EmailAddress,DeliveryDate),'Unknown') AS productType,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_lens_type`(EmailAddress,DeliveryDate),'Unknown') AS lensType,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_brand_type`(EmailAddress,DeliveryDate),'Unknown') AS brandType
FROM (    
    SELECT    
            IFNULL(DDate,IF(custom_optimaxeyewear_bronto_inboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_inboundactivities_deliveryStart,"T")[offset(0)] AS DATE)) )AS DeliveryDate,
            CAST(date AS DATE) AS ActionDate,
            custom_optimaxeyewear_bronto_inboundactivities_emailAddress AS EmailAddress,
            custom_optimaxeyewear_bronto_inboundactivities_messageName AS MessageName,
    FROM `gusa-bronto-dwh.Bronto_Data_Inbound.bronto_Inbound_*` 
    LEFT JOIN `gusa-bronto-dwh.newsletter_base_date.Newsletter_Del_Date` ON MessageName=custom_optimaxeyewear_bronto_inboundactivities_messageName
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m',DATE_SUB(CURRENT_DATE(),INTERVAL 30 DAY))  AND FORMAT_DATE('%Y%m',CURRENT_DATE())  
    AND CAST(Date AS DATE) > (SELECT MAX(ActionDate) FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_unsub`)
    AND custom_optimaxeyewear_bronto_inboundactivities_activityType = 'unsubscribe'
    AND custom_optimaxeyewear_bronto_inboundactivities_messageName IS NOT NULL
  )
  )
;

-------------------------// newsletter bounce // -------------------------------------------
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_bounce` WHERE ActionDate >= dateUpdate;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_bounce` (DeliveryDate,ActionDate,EmailAddress,MessageName,BounceReason,noOfOrders,daySinceLastOrder,productType,lensType,brandType)
(
SELECT 
        DeliveryDate, ActionDate, EmailAddress, MessageName, BounceReason,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_no_of_order`(EmailAddress,DeliveryDate),'Unknown') AS noOfOrders,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_time_since_last_order`(EmailAddress,DeliveryDate),'Unknown') AS daySinceLastOrder,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_product_type`(EmailAddress,DeliveryDate),'Unknown') AS productType,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_lens_type`(EmailAddress,DeliveryDate),'Unknown') AS lensType,
        IFNULL(`gusa-dc-dwh.User_Seg_V2.email_brand_type`(EmailAddress,DeliveryDate),'Unknown') AS brandType
FROM (  
      SELECT    
              IFNULL(DDate,IF(custom_optimaxeyewear_bronto_inboundactivities_deliveryStart = "",CAST(NULL AS DATE),CAST(SPLIT(custom_optimaxeyewear_bronto_inboundactivities_deliveryStart,"T")[offset(0)] AS DATE)) )AS DeliveryDate,
              CAST(date AS DATE) AS ActionDate,
              custom_optimaxeyewear_bronto_inboundactivities_emailAddress AS EmailAddress,
              custom_optimaxeyewear_bronto_inboundactivities_messageName AS MessageName,
              BounceReason
      FROM `gusa-bronto-dwh.Bronto_Data_Inbound.bronto_Inbound_*` A
      LEFT JOIN `gusa-bronto-dwh.Newsletter_data.NL_bounce_reason` USING (custom_optimaxeyewear_bronto_inboundactivities_bounceReason)
      LEFT JOIN `gusa-bronto-dwh.newsletter_base_date.Newsletter_Del_Date` ON MessageName=custom_optimaxeyewear_bronto_inboundactivities_messageName
      WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m',DATE_SUB(CURRENT_DATE(),INTERVAL 30 DAY))  AND FORMAT_DATE('%Y%m',CURRENT_DATE())  
      AND CAST(Date AS DATE) > (SELECT MAX(ActionDate) FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_bounce`)
      AND custom_optimaxeyewear_bronto_inboundactivities_activityType = 'bounce'
     
      AND custom_optimaxeyewear_bronto_inboundactivities_messageName IS NOT NULL

  )
)
;

----------------------------------------// Coredail  Message Sent //----------------------------------------------------------------

DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_sent` WHERE DATE(Sent_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_sent` (Sent_datetime_UTC,Sent_Datetime_PST,email, action, message_name, mdtID, msID, experiment, variant, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
--MERGE INTO `gusa-bronto-dwh.Cordail_Data.message_sent` T USING
(
  SELECT Sent_datetime_UTC,Sent_Datetime_PST,email, action, message_name, mdtID, msID, experiment, variant,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_no_of_order`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS noOfOrders,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_time_since_last_order`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS daySinceLastOrder,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_product_type`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS productType,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_lens_type`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS lensType,
    IFNULL(`gusa-bronto-dwh.Cordail_Data.email_brand_type`(LOWER(Email),DATE(Sent_Datetime_PST)),'Unknown') AS brandType 
    FROM (
          SELECT DISTINCT 
              DATETIME(TIMESTAMP(time)) AS Sent_datetime_UTC, 
              DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) Sent_Datetime_PST,
              email, action, message_name, mdtID, msID, experiment, variant
          FROM `gusa-bronto-dwh.Cordail_Data.message_activity_sent`
          LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
          WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate       
            AND email IS NOT NULL 
            AND DATETIME(TIMESTAMP(time)) IS NOT NULL 
        )
) 
--S ON T.Sent_Datetime_PST = S.Sent_Datetime_PST AND T.msID = S.msID AND T.email = S.email WHEN NOT MATCHED THEN INSERT (Sent_datetime_UTC,Sent_Datetime_PST,email, action, message_name, mdtID, msID, experiment, variant, noOfOrders, daySinceLastOrder, productType, lensType, brandType)  VALUES (Sent_datetime_UTC,Sent_Datetime_PST,email, action, message_name, mdtID, msID, experiment, variant, noOfOrders, daySinceLastOrder, productType, lensType, brandType)

;


----------------------------------------// Coredail  Message Open //----------------------------------------------------------------

DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_open` WHERE DATE(Open_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_open` (Open_datetime_UTC, Open_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
--MERGE INTO `gusa-bronto-dwh.Cordail_Data.message_open` T USING
( 
    SELECT DISTINCT 
      DATETIME(TIMESTAMP(time)) AS Open_datetime_UTC,
      DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) Open_Datetime_PST,
      Sent_Datetime_UTC, Sent_Datetime_PST, email, message_name,  msID,  A.Device_type AS Device, A.Device_platform AS Device_OS,
      noOfOrders, daySinceLastOrder, productType, lensType, brandType
    FROM `gusa-bronto-dwh.Cordail_Data.message_activity` A 
    LEFT JOIN `gusa-bronto-dwh.Cordail_Data.message_sent` B USING (email, message_name, msID)
    LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
    WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate 
      AND DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) < CURRENT_DATE()
      AND Sent_Datetime_PST IS NOT NULL
      AND A.action = 'open'
      AND email IS NOT NULL 
)

--S ON T.Open_Datetime_PST = S.Open_Datetime_PST AND T.email = S.email AND T.msID = S.msID WHEN NOT MATCHED THEN   INSERT (Open_datetime_UTC, Open_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType) VALUES (Open_datetime_UTC, Open_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType)

;

----------------------------------------// Coredail Message Clicks //----------------------------------------------------------------
DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_clicks` WHERE DATE(click_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_clicks` (click_datetime_UTC, click_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, linkUrl, linkname, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
--MERGE INTO `gusa-bronto-dwh.Cordail_Data.message_clicks` T USING
(
  SELECT DISTINCT 
    DATETIME(TIMESTAMP(time)) AS click_datetime_UTC,  
    DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) click_Datetime_PST,
    Sent_Datetime_UTC,  Sent_Datetime_PST,
    email, message_name, msID, link AS linkUrl, data_crdl_key AS linkname, A.Device_type AS Device, A.Device_platform AS Device_OS,
    noOfOrders, daySinceLastOrder, productType, lensType, brandType
  FROM `gusa-bronto-dwh.Cordail_Data.message_activity` A
  LEFT JOIN `gusa-bronto-dwh.Cordail_Data.message_sent` B USING (email, message_name,msID)
  LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
  WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate
    AND DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) < CURRENT_DATE()
    AND A.action = 'click'
    AND Sent_Datetime_UTC IS NOT NULL
    AND email IS NOT NULL
) 
-- S ON T.click_Datetime_PST = S.click_Datetime_PST AND S.email = T.email AND S.msID = T.msID WHEN NOT MATCHED THEN INSERT (click_datetime_UTC, click_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, linkUrl, linkname, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType) VALUES (click_datetime_UTC, click_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, linkUrl, linkname, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType)

;
----------------------------------------// Coredail Message optout //----------------------------------------------------------------

DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_optout` WHERE DATE(optout_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_optout` (optout_datetime_UTC, optout_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
--MERGE INTO `gusa-bronto-dwh.Cordail_Data.message_optout` T USING
(
  SELECT DISTINCT 
    DATETIME(TIMESTAMP(time)) AS optout_datetime_UTC,  
    DATETIME(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) optout_Datetime_PST,
    Sent_Datetime_UTC,  Sent_Datetime_PST,
    email, message_name, msID, A.Device_type AS Device, A.Device_platform AS Device_OS,
    noOfOrders, daySinceLastOrder, productType, lensType, brandType
  FROM `gusa-bronto-dwh.Cordail_Data.message_activity` A
  LEFT JOIN `gusa-bronto-dwh.Cordail_Data.message_sent` B USING (email, message_name,msID)
  LEFT JOIN `gusa-funnel-dwh.Assist_Tables.Time_Zone_Diff` ON DATE(TIMESTAMP(time)) =  Date
  WHERE DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) >= dateUpdate
    AND DATE(TIMESTAMP_ADD(TIMESTAMP(time), INTERVAL UTC_2_PST HOUR)) < CURRENT_DATE()
    AND A.action = 'optout'
    AND Sent_Datetime_UTC IS NOT NULL
    AND email IS NOT NULL
) 
-- S ON T. optout_Datetime_PST = S.optout_Datetime_PST AND T.email = S.email AND T.msID = S.msID WHEN NOT MATCHED THEN   INSERT ( optout_datetime_UTC, optout_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType)   VALUES ( optout_datetime_UTC, optout_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, Device, Device_OS, noOfOrders, daySinceLastOrder, productType, lensType, brandType)

;


----------------------------------------// Coredail Message bounce //----------------------------------------------------------------
DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_bounce` WHERE DATE(bounce_Datetime_PST) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_bounce`  ( bounce_datetime_UTC, bounce_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
--MERGE INTO `gusa-bronto-dwh.Cordail_Data.message_bounce` T USING
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
-- S ON T.bounce_Datetime_PST = S.bounce_Datetime_PST AND T.email = S.email AND S.msID = T.msID WHEN NOT MATCHED THEN INSERT ( bounce_datetime_UTC, bounce_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, noOfOrders, daySinceLastOrder, productType, lensType, brandType)   VALUES ( bounce_datetime_UTC, bounce_Datetime_PST, Sent_Datetime_UTC,  Sent_Datetime_PST,email, message_name, msID, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
;

----------------------------------------// Coredail Message Click Order //---------------------------------------------------------------


DELETE FROM `gusa-bronto-dwh.Cordail_Data.message_click_order` WHERE DATE(ActionDateTime) >= dateUpdate;
INSERT INTO `gusa-bronto-dwh.Cordail_Data.message_click_order` (EmailAddress, DeliveryDate, ActionDateTime, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , 
          Date, Date_Time, Revenue, Rank, Coupon, linkUrl , Order_Att, linkname,Revenue_Contacts)
--MERGE INTO `gusa-bronto-dwh.Cordail_Data.message_click_order` T USING
(
WITH custs AS (  
    SELECT email AS EmailAddress, DeliveryDate,  ActionDateTime, message_name AS MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, linkUrl, linkname
    FROM (
        SELECT DISTINCT email , Sent_Datetime_PST AS DeliveryDate, 
                        click_Datetime_PST AS  ActionDateTime, 
                        message_name , noOfOrders, daySinceLastOrder, productType, lensType, brandType, linkUrl, linkname , Device, Device_OS,
          --RANK() OVER(PARTITION BY email,message_name,Sent_Datetime_UTC ORDER BY click_Datetime_PST ASC) AS Rank
        FROM `gusa-bronto-dwh.Cordail_Data.message_clicks` 
        WHERE DATE(click_Datetime_PST) >= dateUpdate
        ) 
      --WHERE RANK = 1
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
    SELECT EmailAddress, DeliveryDate, ActionDateTime, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , 
          Date, Date_Time, Revenue, Rank, Coupon, linkUrl , Order_Att, linkname,Revenue_Contacts
    FROM (
      SELECT 
        EmailAddress, DeliveryDate, ActionDateTime, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , Date, Date_Time, Revenue, linkUrl,linkname, Coupon,  
        IF(GA_Sales.Order_id IS NULL, 'Assisted','Last Click') AS Order_Att,Revenue_Contacts,
        RANK() OVER(PARTITION BY EmailAddress,Order_id ORDER BY DeliveryDate ASC,ActionDateTime ASC) AS Rank
      FROM custs
      LEFT JOIN Admin_orders ON LOWER(EmailAddress)=LOWER(customer_email) --AND DATETIME_ADD(ActionDateTime, INTERVAL 3 HOUR)  = Admin_orders.Date_Time
      LEFT JOIN GA_Sales USING (Order_id)
      WHERE Date_Time >= ActionDateTime AND DATETIME_DIFF(Date_Time,ActionDateTime,MINUTE) BETWEEN 0 AND 480 
    )
    WHERE Rank = 1
)
-- S ON T.EmailAddress = S.EmailAddress AND T.DeliveryDate = S.DeliveryDate AND T.ActionDateTime=S.ActionDateTime AND T.MessageName=S.MessageName AND T.Order_id = S.Order_id
-- WHEN NOT MATCHED THEN INSERT (EmailAddress, DeliveryDate, ActionDateTime, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , Date, Date_Time, Revenue, Rank, Coupon, linkUrl , Order_Att, linkname,Revenue_Contacts) VALUES (EmailAddress, DeliveryDate, ActionDateTime, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Order_id , Date, Date_Time, Revenue, Rank, Coupon, linkUrl , Order_Att, linkname,Revenue_Contacts)
--WHEN MATCHED AND (T.Revenue != S.Revenue OR T.Revenue_Contacts != S.Revenue_Contacts) THEN UPDATE SET  Revenue = S.Revenue, Revenue_Contacts = S.Revenue_Contacts

;

--------------------/ Bronto Full Table Update / ----------------------

DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table` WHERE DeliveryDate >= dateUpdate;

MERGE INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table` T 
USING (
    WITH message_list AS (
        SELECT DISTINCT MessageName, DeliveryDate
        FROM (
                SELECT DISTINCT MessageName, date AS DeliveryDate FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_del` WHERE date >= dateUpdate  
                UNION ALL
                SELECT DISTINCT MessageName, DeliveryDate FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_open` WHERE ActionDate >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT MessageName, DeliveryDate FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_click_new` WHERE CAST(ActionDateTIME AS DATE) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT MessageName, DeliveryDate FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_unsub` WHERE ActionDate >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT MessageName, DeliveryDate FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_bounce` WHERE ActionDate >= dateUpdate
            )
       --WHERE MessageName IS NOT NULL   AND LOWER(MessageName) LIKE '%_s47_%'
    )
   ,delivery AS (
        SELECT date AS DeliveryDate, A.MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, SUM(sent) AS Sent
        FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_del` A      
        INNER JOIN message_list MN ON A.MessageName=MN.MessageName AND date=DeliveryDate

        GROUP BY date, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
    )
    ,opens AS (
        SELECT DeliveryDate,MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS open, COUNT(DISTINCT EmailAddress) AS Unique_open, SUM(IF(DeliveryDate=ActionDate, 1,0)) AS Open_D0, COUNT(DISTINCT IF(DeliveryDate=ActionDate, EmailAddress,NULL)) AS Unique_open_D0
        FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_open`
        INNER JOIN message_list MN USING(MessageName,DeliveryDate)
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
    )
    ,clicks AS (
       SELECT DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS clicks, COUNT(DISTINCT EmailAddress) AS Unique_clicks, 
               SUM(IF(DeliveryDate=CAST(ActionDateTIME AS DATE), 1,0)) AS clicks_D0, COUNT(DISTINCT IF(DeliveryDate=CAST(ActionDateTIME AS DATE), EmailAddress,NULL)) AS Unique_click_D0
        FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_click_new`
        INNER JOIN message_list MN USING(MessageName,DeliveryDate)
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
    )
    , click_order AS (
        SELECT DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS orders, SUM(Revenue) AS revenue, 
               SUM(IF(DeliveryDate=CAST(ActionDateTIME AS DATE), 1,0)) AS Orders_D0, 
               SUM(IF(DeliveryDate=CAST(ActionDateTIME AS DATE), revenue,0)) AS revenue_D0,        
               SUM(Revenue_Contacts) As Contacts_Revenue,
               SUM(IF(DeliveryDate=CAST(ActionDateTIME AS DATE), Revenue_Contacts,0)) AS Contacts_Revenue_D0,        
        FROM `gusa-bronto-dwh.newsletter_base_date.Newsletter_click_orders`
        INNER JOIN message_list MN USING(MessageName,DeliveryDate)
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
    )
    ,unsub AS (
        SELECT DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS unsub
        FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_unsub`
        INNER JOIN message_list MN USING(MessageName,DeliveryDate)
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
    )
    ,bounce AS (
        SELECT DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS bounce, SUM(IF(BounceReason!='Soft',1,0)) AS hard_bounce, SUM(IF(BounceReason='Soft',1,0)) AS soft_bounce
        FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_bounce`
        INNER JOIN message_list MN USING(MessageName,DeliveryDate)
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
    )
        SELECT *
        FROM delivery     
        LEFT JOIN opens USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN clicks USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN click_order USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN unsub USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN bounce USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
  --      WHERE Sent IS NOT NULL AND open IS NOT NULL AND clicks IS NOT NULL AND unsub IS NOT NULL AND bounce IS NOT NULL
          
) S
ON S.DeliveryDate=T.DeliveryDate AND T.MessageName=S.MessageName AND IFNULL(T.noOfOrders,'zzz')  = IFNULL(S.noOfOrders,'zzz')  AND IFNULL(T.daySinceLastOrder,'zzz') = IFNULL(S.daySinceLastOrder,'zzz') 
AND IFNULL(T.productType,'zzz') = IFNULL(S.productType,'zzz') AND IFNULL(T.lensType,'zzz')  = IFNULL(S.lensType,'zzz') AND IFNULL(T.brandType,'zzz') = IFNULL(S.brandType,'zzz') 

WHEN NOT MATCHED THEN 
INSERT (DeliveryDate,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType,sent,open,Unique_open,Open_D0,Unique_open_D0,
        clicks,Unique_clicks,clicks_D0,Unique_click_D0,unsub,bounce,hard_bounce,soft_bounce,orders,revenue,orders_D0,revenue_D0,Contacts_Revenue,Contacts_Revenue_D0)
VALUES (DeliveryDate,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType,Sent,open,Unique_open,Open_D0,Unique_open_D0,
        clicks,Unique_clicks,clicks_D0,Unique_click_D0,unsub,bounce,hard_bounce,soft_bounce,orders,revenue,orders_D0,revenue_D0,Contacts_Revenue,Contacts_Revenue_D0)

WHEN MATCHED THEN UPDATE SET
 T.Sent = S.Sent, T.open = S.open, T.Unique_open = S.Unique_open, T.Open_D0 = S.Open_D0 , T.Unique_open_D0 = S.Unique_open_D0, 
 T.clicks = S.clicks, T.Unique_clicks = S.Unique_clicks, T.clicks_D0 = S.clicks_D0, T.Unique_click_D0 = S.Unique_click_D0,
 T.unsub = S.unsub,
 T.bounce = S.bounce, T.hard_bounce = S.hard_bounce, T.soft_bounce = S.soft_bounce,
 T.orders = S.orders, T.revenue = S.revenue, T.orders_D0 = S.orders_D0, T.revenue_D0 = S.revenue_D0,
 T.Contacts_Revenue = S.Contacts_Revenue, T.Contacts_Revenue_D0 = S.Contacts_Revenue_D0
 
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
       --WHERE MessageName IS NOT NULL   AND LOWER(MessageName) LIKE '%_s47_%'
    )
   ,delivery AS (

        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate,  message_name AS MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, COUNT(*) AS Sent
        FROM `gusa-bronto-dwh.Cordail_Data.message_sent` A      
        INNER JOIN message_list MN ON A.Message_Name=MN.MessageName AND DATE(Sent_Datetime_PST) =DeliveryDate
        GROUP BY 1, 2,3,4,5,6,7  
    )
   ,opens AS (
        
        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate,message_name AS MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS open, COUNT(DISTINCT Email) AS Unique_open, SUM(IF( DATE(Sent_Datetime_PST)= DATE(Open_Datetime_PST), 1,0)) AS Open_D0, 
               COUNT(DISTINCT IF(DATE(Sent_Datetime_PST)= DATE(Open_Datetime_PST), Email ,NULL)) AS Unique_open_D0
        FROM `gusa-bronto-dwh.Cordail_Data.message_open` A
        INNER JOIN message_list MN ON MessageName=message_name AND MN.DeliveryDate= DATE(Sent_Datetime_PST)
        WHERE DATE(Sent_Datetime_PST) IS NOT NULL
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType 
    )
    ,clicks AS (
        
        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate,message_name AS MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS clicks, COUNT(DISTINCT email) AS Unique_clicks, 
               SUM(IF(DATE(Sent_Datetime_PST)= DATE(Click_Datetime_PST), 1,0)) AS clicks_D0, COUNT(DISTINCT IF(DATE(Sent_Datetime_PST)= DATE(Click_Datetime_PST), Email ,NULL)) AS Unique_click_D0
        FROM `gusa-bronto-dwh.Cordail_Data.message_clicks` A
        INNER JOIN message_list MN ON MessageName=message_name AND MN.DeliveryDate= DATE(Sent_Datetime_PST)
        WHERE DATE(Sent_Datetime_PST) IS NOT NULL
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
       
        
    )
     , click_order AS (
        
        SELECT DATE(A.DeliveryDate) AS DeliveryDate, A.MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS orders, SUM(Revenue) AS revenue, 
               SUM(IF(DATE(A.DeliveryDate)=DATE(ActionDateTIME), 1,0)) AS Orders_D0, 
               SUM(IF(DATE(A.DeliveryDate)=DATE(ActionDateTIME), revenue,0)) AS revenue_D0,        
               SUM(Revenue_Contacts) As Contacts_Revenue,
               SUM(IF(DATE(A.DeliveryDate)=DATE(ActionDateTIME), Revenue_Contacts,0)) AS Contacts_Revenue_D0,     
        FROM `gusa-bronto-dwh.Cordail_Data.message_click_order` A
        INNER JOIN message_list MN ON A.MessageName=MN.MessageName AND DATE(A.DeliveryDate)=MN.DeliveryDate
        WHERE DATE(A.DeliveryDate) IS NOT NULL
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
        
    )
    ,unsub AS (
        
        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS unsub
        FROM `gusa-bronto-dwh.Cordail_Data.message_optout` A
        INNER JOIN message_list MN ON MessageName=message_name AND MN.DeliveryDate= DATE(Sent_Datetime_PST)
        WHERE DATE(Sent_Datetime_PST) IS NOT NULL
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
        
    )
    ,bounce AS (
        
        SELECT DATE(Sent_Datetime_PST) AS DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS bounce, 0 AS hard_bounce, 0 AS soft_bounce
        FROM `gusa-bronto-dwh.Cordail_Data.message_bounce`
        INNER JOIN message_list MN ON MessageName=message_name AND MN.DeliveryDate= DATE(Sent_Datetime_PST)
        WHERE DATE(Sent_Datetime_PST) IS NOT NULL
        GROUP BY DeliveryDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
       
    )
        SELECT 'Cordail' AS Platform,*
        FROM delivery
        LEFT JOIN opens USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN clicks USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN click_order USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN unsub USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN bounce USING (DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
  --      WHERE Sent IS NOT NULL AND open IS NOT NULL AND clicks IS NOT NULL AND unsub IS NOT NULL AND bounce IS NOT NULL
          
) S
ON S.DeliveryDate=T.DeliveryDate AND T.MessageName=S.MessageName AND IFNULL(T.noOfOrders,'zzz')  = IFNULL(S.noOfOrders,'zzz')  AND IFNULL(T.daySinceLastOrder,'zzz') = IFNULL(S.daySinceLastOrder,'zzz') 
AND IFNULL(T.productType,'zzz') = IFNULL(S.productType,'zzz') AND IFNULL(T.lensType,'zzz')  = IFNULL(S.lensType,'zzz') AND IFNULL(T.brandType,'zzz') = IFNULL(S.brandType,'zzz') 

WHEN NOT MATCHED THEN 
INSERT (DeliveryDate,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType,sent,open,Unique_open,Open_D0,Unique_open_D0,
        clicks,Unique_clicks,clicks_D0,Unique_click_D0,unsub,bounce,hard_bounce,soft_bounce,orders,revenue,orders_D0,revenue_D0,Contacts_Revenue,Contacts_Revenue_D0)
VALUES (DeliveryDate,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType,Sent,open,Unique_open,Open_D0,Unique_open_D0,
        clicks,Unique_clicks,clicks_D0,Unique_click_D0,unsub,bounce,hard_bounce,soft_bounce,orders,revenue,orders_D0,revenue_D0,Contacts_Revenue,Contacts_Revenue_D0)

WHEN MATCHED THEN UPDATE SET
 T.Sent = S.Sent, T.open = S.open, T.Unique_open = S.Unique_open, T.Open_D0 = S.Open_D0 , T.Unique_open_D0 = S.Unique_open_D0, 
 T.clicks = S.clicks, T.Unique_clicks = S.Unique_clicks, T.clicks_D0 = S.clicks_D0, T.Unique_click_D0 = S.Unique_click_D0,
 T.unsub = S.unsub,
 T.bounce = S.bounce, T.hard_bounce = S.hard_bounce, T.soft_bounce = S.soft_bounce,
 T.orders = S.orders, T.revenue = S.revenue, T.orders_D0 = S.orders_D0, T.revenue_D0 = S.revenue_D0,
 T.Contacts_Revenue = S.Contacts_Revenue, T.Contacts_Revenue_D0 = S.Contacts_Revenue_D0
 
;

-----------------------------/ full table Joined from View/------------------
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_table` WHERE TRUE;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_table` 
(Platform, Final_message_name, DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Sent, open, Unique_open, Open_D0, Unique_open_D0, clicks, Unique_clicks, clicks_D0, Unique_click_D0, unsub, unsub_D0, bounce, hard_bounce, soft_bounce, orders, revenue, orders_D0, revenue_D0, message_name, Newsletter_Category, Message_Camapaign, Message_Date, Offer, Target_Audience, Segment, Event, Email_Length, Creative_Type, Glasses_Type, CMG, Subject_line, Preview_line, Call_to_Action, Button_Color, No_of_Products, orientation, time_slot, Time_Zone, AB_Test_Version,Order_Att,Contacts_Revenue,Contacts_Revenue_D0)
 (
SELECT Platform, Final_message_name, DeliveryDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, Sent, open, Unique_open, Open_D0, Unique_open_D0, clicks, Unique_clicks, clicks_D0, Unique_click_D0, unsub, unsub_D0, bounce, hard_bounce,     
        soft_bounce, orders, revenue, orders_D0, revenue_D0, message_name, Newsletter_Category, Message_Camapaign, Message_Date, Offer, Target_Audience, Segment, Event, Email_Length, Creative_Type, Glasses_Type, CMG, Subject_line, Preview_line, Call_to_Action, 
        Button_Color, No_of_Products, orientation, time_slot, Time_Zone, AB_Test_Version ,Order_Att,Contacts_Revenue,Contacts_Revenue_D0 
FROM `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_view` 
WHERE DeliveryDate < CURRENT_DATE())
;

-----------------------------/Bronto full table update action date/------------------

DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_action_date` WHERE ActionDate >= dateUpdate;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_action_date` (ActionDate,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType,open,Unique_open,clicks,Unique_clicks,orders,revenue,unsub,Contacts_Revenue)
 (
    WITH message_list AS (
        SELECT DISTINCT MessageName, ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType
        FROM (
                SELECT DISTINCT MessageName, DeliveryDate, ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_open` WHERE ActionDate >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT MessageName, DeliveryDate, CAST(ActionDateTIME AS DATE) AS ActionDate ,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_click_new` WHERE CAST(ActionDateTIME AS DATE) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT MessageName, DeliveryDate, CAST(ActionDateTIME AS DATE) AS ActionDate ,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                FROM `gusa-bronto-dwh.newsletter_base_date.Newsletter_click_orders` WHERE CAST(ActionDateTIME AS DATE) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT MessageName, DeliveryDate, ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_unsub` WHERE ActionDate >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT MessageName, DeliveryDate, ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_bounce` WHERE ActionDate >= dateUpdate
            )
        WHERE MessageName IS NOT NULL -- AND MessageName = '16.8.20_Contacts-vip65_Retention_S4_D_L_I_MC_HB_SE_SE_SN_white_8_S_MO_EST_NONE'
    )
    ,opens AS (
        SELECT ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS open, COUNT(DISTINCT EmailAddress) AS Unique_open
        FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_open`
        INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        GROUP BY ActionDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
    )
    ,clicks_data AS (
       SELECT ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,clicks,Unique_clicks
       FROM (
              SELECT CAST(ActionDateTIME AS DATE) AS ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
                    COUNT(*) AS clicks, COUNT(DISTINCT EmailAddress) AS Unique_clicks
              FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_click_new` A
              GROUP BY ActionDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType 
              )
       INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType)
       
    )
    ,click_order AS (
        SELECT ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,orders,revenue,Contacts_Revenue
        FROM (
              SELECT CAST(A.ActionDateTIME AS DATE) AS ActionDate, A.MessageName, A.noOfOrders, A.daySinceLastOrder, A.productType, A.lensType, A.brandType, A.Order_Att,
                     COUNT(*) AS orders, SUM(Revenue) AS revenue,  SUM(Revenue_Contacts) As Contacts_Revenue           
              FROM `gusa-bronto-dwh.newsletter_base_date.Newsletter_click_orders` A 
              GROUP BY ActionDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType,Order_Att
              )
        INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        
    )
    ,unsubs AS (
        SELECT ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS unsub
        FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_unsub`
        INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        GROUP BY ActionDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType
    )
    
        SELECT ActionDate,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType,open,Unique_open,clicks,Unique_clicks,orders,revenue,unsub, Contacts_Revenue

        FROM message_list
        LEFT JOIN opens USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType) 
        LEFT JOIN clicks_data USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN click_order USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN unsubs USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
  --      WHERE Sent IS NOT NULL AND open IS NOT NULL AND clicks IS NOT NULL AND unsub IS NOT NULL AND bounce IS NOT NULL
    
)

  ; 
--------------------------------/Cordail full table update action date /--------------------  
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_action_date_Cordail` WHERE DATE(ActionDate) >= dateUpdate;
SELECT MAX(DATE(ActionDate)) FROM `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_action_date_Cordail`;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.newsletter_full_table_action_date_Cordail` (ActionDate,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType,open,Unique_open,clicks,Unique_clicks,orders,revenue,unsub,Contacts_Revenue)
 (
    WITH message_list AS (
        SELECT DISTINCT MessageName, ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType
        FROM (
                SELECT DISTINCT message_name AS MessageName, DATE(Sent_Datetime_PST) AS DeliveryDate, DATE(Open_Datetime_PST) AS ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                    FROM `gusa-bronto-dwh.Cordail_Data.message_open`  WHERE DATE(Open_Datetime_PST)  >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT message_name AS MessageName, DATE(Sent_Datetime_PST) AS DeliveryDate, DATE(Click_Datetime_PST) AS ActionDate ,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                  FROM `gusa-bronto-dwh.Cordail_Data.message_clicks` WHERE DATE(Click_Datetime_PST) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT MessageName, DeliveryDate, DATE(ActionDateTIME) AS ActionDate ,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                  FROM `gusa-bronto-dwh.Cordail_Data.message_click_order` WHERE DATE(ActionDateTIME) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT message_name AS MessageName, DATE(Sent_Datetime_PST) AS DeliveryDate, DATE(optout_Datetime_PST ) AS ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                  FROM `gusa-bronto-dwh.Cordail_Data.message_optout`  WHERE DATE(optout_Datetime_PST ) >= dateUpdate 
                UNION ALL 
                SELECT DISTINCT message_name AS MessageName, DATE(Sent_Datetime_PST) AS DeliveryDate, DATE(bounce_Datetime_PST) AS ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType 
                  FROM `gusa-bronto-dwh.Cordail_Data.message_bounce`  WHERE DATE(bounce_Datetime_PST)  >= dateUpdate
            )
        WHERE MessageName IS NOT NULL -- AND MessageName = '16.8.20_Contacts-vip65_Retention_S4_D_L_I_MC_HB_SE_SE_SN_white_8_S_MO_EST_NONE'
    )
    ,opens AS (
        SELECT DATE(Open_Datetime_PST) AS ActionDate, message_name AS MessageName, A.noOfOrders, A.daySinceLastOrder, A.productType, A.lensType, A.brandType,
               COUNT(*) AS open, COUNT(DISTINCT Email) AS Unique_open
        FROM `gusa-bronto-dwh.Cordail_Data.message_open` A
        INNER JOIN message_list MN ON MN.MessageName=A.message_name AND MN.ActionDate=DATE(Open_Datetime_PST) AND MN.noOfOrders=A.noOfOrders 
          AND MN.daySinceLastOrder=A.daySinceLastOrder AND MN.lensType=A.lensType AND MN.brandType=A.brandType
        GROUP BY 1, 2, 3,4,5,6,7
    )
    ,clicks_data AS (
       SELECT ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,clicks,Unique_clicks
       FROM (
              SELECT DATE(Click_Datetime_PST) AS ActionDate, message_name AS MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
                    COUNT(*) AS clicks, COUNT(DISTINCT Email) AS Unique_clicks
              FROM `gusa-bronto-dwh.Cordail_Data.message_clicks`  A
              GROUP BY ActionDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType 
              )
       INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType)
       
    )
    ,click_order AS (
        SELECT ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,orders,revenue,Contacts_Revenue
        FROM (
              SELECT CAST(A.ActionDateTIME AS DATE) AS ActionDate, A.MessageName, A.noOfOrders, A.daySinceLastOrder, A.productType, A.lensType, A.brandType, A.Order_Att,
                     COUNT(*) AS orders, SUM(Revenue) AS revenue,  SUM(Revenue_Contacts) As Contacts_Revenue           
              FROM `gusa-bronto-dwh.Cordail_Data.message_click_order` A 
              GROUP BY ActionDate, MessageName, noOfOrders,daySinceLastOrder,productType,lensType,brandType,Order_Att
              )
        INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        
    )
    ,unsubs AS (
        SELECT ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,unsub FROM (
        SELECT DATE(optout_Datetime_PST ) aS ActionDate, message_name AS MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType,
               COUNT(*) AS unsub      
        FROM `gusa-bronto-dwh.Cordail_Data.message_optout`
        GROUP BY 1,2,3,4,5,6,7)
        INNER JOIN message_list MN USING(MessageName,ActionDate,noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        
    )
    
        SELECT ActionDate,MessageName,noOfOrders,daySinceLastOrder,productType,lensType,brandType,open,Unique_open,clicks,Unique_clicks,orders,revenue,unsub, Contacts_Revenue

        FROM message_list
        LEFT JOIN opens USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType) 
        LEFT JOIN clicks_data USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN click_order USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
        LEFT JOIN unsubs USING (ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType)
  --      WHERE Sent IS NOT NULL AND open IS NOT NULL AND clicks IS NOT NULL AND unsub IS NOT NULL AND bounce IS NOT NULL
    
)
;  
 
DELETE FROM `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_table_action_date` WHERE TRUE;

INSERT INTO `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_table_action_date` 
( Platform, Final_message_name,ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, open, Unique_open, clicks, Unique_clicks, orders, revenue,  message_name, Newsletter_Category, Message_Camapaign, Message_Date, Offer, Target_Audience, Segment, Event, Email_Length, Creative_Type, Glasses_Type, CMG, Subject_line, Preview_line, Call_to_Action, Button_Color, No_of_Products, orientation, time_slot, Time_Zone, AB_Test_Version,unsub, Contacts_Revenue)
 (
SELECT Platform,Final_message_name,ActionDate, MessageName, noOfOrders, daySinceLastOrder, productType, lensType, brandType, open, Unique_open, clicks, Unique_clicks, orders, revenue,  message_name, Newsletter_Category, Message_Camapaign, Message_Date, Offer, Target_Audience, Segment, Event, Email_Length, Creative_Type, Glasses_Type, CMG, Subject_line, Preview_line, Call_to_Action, Button_Color, No_of_Products, orientation, time_slot, Time_Zone, AB_Test_Version, unsub ,Contacts_Revenue
FROM `gusa-bronto-dwh.newsletter_base_date.Bronto_data_joined_view_action_date` 
WHERE ActionDate < CURRENT_DATE())

/**/


/**/