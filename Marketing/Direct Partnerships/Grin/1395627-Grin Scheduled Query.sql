----------------GRIN budget--------------
DELETE FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_Budgets_tbl` WHERE TRUE;
INSERT INTO `gusa-funnel-dwh.Direct_Marketing.GRIN_Budgets_tbl` (Channel_Name, Platform, Medium, Channel, Digital_campaign_link, Total_budget, Planned_Month, Planned_Year, Status, Live_date, Second_IG_Date, Acct_Mgr, Auto_Status, Mail, Coupon_Code, Pending_Draft_Date)
(
    SELECT Channel_Name, Platform, Medium, Channel, Digital_campaign_link, Total_budget, Planned_Month, Planned_Year, Status, Live_date, Second_IG_Date, Acct_Mgr, Auto_Status, Mail, Coupon_Code, Pending_Draft_Date FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_Budgets_GS`
);

---------------sharedId/content update--------------
INSERT INTO `gusa-funnel-dwh.Direct_Marketing.GRIN_sharedId_channel_list` (Channel_Name, sharedId)
(
    SELECT DISTINCT 
        TRIM(Channel_Name) AS Channel_Name,
        LOWER(REPLACE(REPLACE(TRIM(Channel_Name),' ','_'),'.','_')) AS sharedId,
    FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_Budgets_tbl`
    WHERE TRIM(LOWER(Channel_Name)) NOT IN (SELECT DISTINCT LOWER(Channel_Name) FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_sharedId_channel_list`)
);


-------------Batch inf Leads-------------------------
--DELETE FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Leads` WHERE TRUE;
MERGE INTO `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Leads` T
USING(
    SELECT DISTINCT oid AS subscriber_id, CAST(TO_HEX(MD5(LOWER(C.subscriber_email))) AS STRING) AS subscriber_email, action_date AS Datetime, A.subid4, Channel_Name, 'IR' AS Plat,
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
) S ON S.subscriber_id = T.subscriber_id AND LOWER(S.Channel_Name) = LOWER(T.Channel_Name)

WHEN MATCHED AND T.subscriber_email != S.subscriber_email THEN UPDATE SET T.subscriber_email = S.subscriber_email
WHEN MATCHED AND T.Channel_Name IS NULL THEN DELETE
WHEN NOT MATCHED THEN INSERT ( subscriber_id, subscriber_email, Datetime, Channel_Name, Plat)  VALUES( subscriber_id, subscriber_email, Datetime, Channel_Name, Plat)
;



-------------Batch inf Admin Leads-------------------------
--DELETE FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Leads` WHERE TRUE;
MERGE INTO `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Leads` T
USING(
    SELECT 
        subscriber_id,
        CAST(TO_HEX(MD5(LOWER(subscriber_email))) AS STRING) AS subscriber_email,
        DATE_TRUNC(CAST(FORMAT_TIMESTAMP('%Y-%m-%d %X',TIMESTAMP_ADD(change_status_at, INTERVAL -8 HOUR )) AS DATETIME),DAY) AS Datetime,
        code AS Affid,
        'Admin' AS Plat,
    FROM 
        (SELECT * FROM `gusa-dwh.backend.newsletter_subscriber` 
        LEFT JOIN `gusa-dwh.backend.customy_affiliatebanner_affiliate` ON affiliate_id = entity_id
        WHERE (LOWER(code) LIKE '%lp2774%' OR LOWER(code) LIKE '%lp2720%')
            AND CAST(FORMAT_TIMESTAMP('%Y-%m-%d %X',TIMESTAMP_ADD(change_status_at, INTERVAL -8 HOUR )) AS DATETIME) > '2022-05-01'
        ) A 
    INNER JOIN (SELECT DISTINCT Affid FROM `gusa-funnel-dwh.Direct_Marketing.IR_Influencers_Naming_LOC` WHERE Affid IS NOT NULL) B ON Affid = code
) S ON S.subscriber_id = T.subscriber_id AND LOWER(S.Affid) = LOWER(T.Affid)

WHEN MATCHED AND T.subscriber_email != S.subscriber_email THEN UPDATE SET T.subscriber_email = S.subscriber_email
WHEN MATCHED AND T.Affid IS NULL THEN DELETE
WHEN NOT MATCHED THEN INSERT ( subscriber_id, subscriber_email, Datetime, Affid, Plat)  VALUES( subscriber_id, subscriber_email, Datetime, Affid, Plat)
;


-----------Batch inf Leads Combined-----------------------
--DELETE FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Leads_Combined` WHERE TRUE;
MERGE INTO `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Leads_Combined` T
USING(
    SELECT DISTINCT subscriber_id, subscriber_email, Datetime, A.Channel_Name, Plat , Budget_Date
    FROM(

        SELECT DISTINCT A.subscriber_id, A.subscriber_email, A.Datetime, TRIM(Channel_Name) AS Channel_Name, A.Plat
        FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Leads` A
        LEFT JOIN `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Leads` B ON A.subscriber_id = CAST(B.subscriber_id AS STRING)
        WHERE B.subscriber_id IS NULL

        UNION ALL

        SELECT DISTINCT CAST(A.subscriber_id AS STRING) AS subscriber_id, A.subscriber_email, A.Datetime, TRIM(Affid) AS Channel_Name, A.Plat
        FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Leads` A
        LEFT JOIN `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Leads` B ON B.subscriber_id = CAST(A.subscriber_id AS STRING)
        WHERE B.subscriber_id IS NULL

        UNION ALL

        SELECT DISTINCT CAST(A.subscriber_id AS STRING) AS subscriber_id, A.subscriber_email, A.Datetime, TRIM(Channel_Name) AS Channel_Name, 'Both' AS Plat
        FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Leads` A
        INNER JOIN `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Leads` B ON B.subscriber_id = CAST(A.subscriber_id AS STRING)
        WHERE B.subscriber_id IS NULL
        ) A
    LEFT JOIN (SELECT DISTINCT Influencer_Name AS Channel_Name,Budget, Planned_Date, Budget_Date, ToDate  FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_Budget_Ready_View`) B 
        ON IFNULL(A.Channel_Name,'xxxx')=IFNULL(B.Channel_Name,'xxxx')
    WHERE CAST(Datetime AS DATE) >= Budget_Date AND  CAST(Datetime AS DATE) <= ToDate
    AND A.Channel_Name IS NOT NULL
    ) S ON IFNULL(T.subscriber_id,'zzz')= IFNULL(S.subscriber_id,'zzz') AND IFNULL(T.Channel_Name,'zzz')=IFNULL(S.Channel_Name,'zzz') AND T.Plat=S.Plat

WHEN MATCHED AND T.Budget_Date != S.Budget_Date OR T.Budget_Date IS NULL THEN UPDATE SET T.Budget_Date = S.Budget_Date
WHEN NOT MATCHED THEN INSERT (subscriber_id, subscriber_email, Datetime, Channel_Name, Plat , Budget_Date) VALUES (subscriber_id, subscriber_email, Datetime, Channel_Name, Plat , Budget_Date)
;

-------------Batch inf Sales-------------------------
--DELETE FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Sales` WHERE TRUE;
MERGE `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Sales`  T
USING(
    WITH db AS (
        SELECT DISTINCT oid, promo_code, revenue, action_date,  click_date,  media  ,subid1, subid2, subid3, subid4, Channel_Name, IF(DATETIME_DIFF(action_date, click_date,DAY)<=1,'C','P') AS Sale_type
        FROM (
            SELECT DISTINCT oid, promo_code, revenue, CAST(action_date AS DATETIME) AS action_date,  CAST(click_date AS DATETIME) AS click_date,  TRIM(media) AS media, subid1, subid2, subid3, subid4 
            FROM `gusa-dc-dwh.IR_data.IR_Sales` 
            WHERE (LOWER(media) LIKE '%creatorsgm-lp2774%' OR LOWER(media) LIKE '%creators-lp2720%')
                AND LOWER(promo_code) NOT LIKE 'inf100%'
                AND LOWER(promo_code) NOT LIKE 'ytsc%'
                AND LOWER(promo_code) NOT LIKE 'igsc%'
            )
        LEFT JOIN `gusa-funnel-dwh.Direct_Marketing.GRIN_sharedId_channel_list` ON LOWER(subid4) = LOWER(sharedId)
        INNER JOIN (SELECT oid, TRIM(media) AS media, subid4, MAX(CAST(click_date AS DATETIME)) AS click_date FROM `gusa-dc-dwh.IR_data.IR_Sales` GROUP BY 1,2,3) USING (oid,media,click_date,subid4)
        WHERE click_date >= '2022-05-01'
        )

    SELECT CAST(Order_Number AS STRING) AS Order_id, B.Store, B.Device, A.Channel_Name, CAST(TO_HEX(MD5(LOWER(B.customer_email))) AS STRING) AS customer_email, B.Order_Coupon AS coupon_code, DATE(Creation_Datetime) AS Date, DATETIME(Creation_Datetime) Date_Time, Sale_type,
        CAST(SUM(Item_Qty_Ordered) AS INT64) AS items, SUM(Item_Price) AS Order_Full_Price, SUM(ItemRevenue ) AS Revenue, SUM(ItemRevenue)+SUM(Item_Tax) AS Revenue_Inc_Tax, SUM(Item_Tax) Tax,
    FROM db A
    LEFT JOIN `gusa-dwh.Admin.Order_Items` B ON oid = CAST(B.Order_Number AS STRING)
    WHERE B.Order_Number IS NOT NULL  
    GROUP BY 1,2,3,4,5,6,7,8,9 
    ) S ON T.Order_id=S.Order_id AND T.Channel_Name=S.Channel_Name

WHEN MATCHED AND S.Sale_type!=T.Sale_type THEN UPDATE SET Sale_type = S.Sale_type

WHEN NOT MATCHED THEN INSERT ( Order_id, Store, Device, Channel_Name, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax,Sale_type) 
    VALUES( Order_id, Store, Device, Channel_Name, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax,Sale_type)
;



-------------Batch inf Admin Sales-------------------------
--DELETE FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Sales` WHERE TRUE;
MERGE `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Sales` T
USING (
    SELECT CAST(Order_Number AS STRING) AS Order_id, Store, Device, affiliate_code AS Channel_Name, CAST(TO_HEX(MD5(LOWER(customer_email))) AS STRING) AS customer_email, Order_Coupon AS coupon_code,
        DATE(Creation_Datetime) AS Date, DATETIME(Creation_Datetime) Date_Time,
        CAST(SUM(Item_Qty_Ordered) AS INT64) AS items, SUM(Item_Price) AS Order_Full_Price, SUM(ItemRevenue ) AS Revenue, SUM(ItemRevenue )+SUM(Item_Tax) AS Revenue_Inc_Tax,
    FROM `gusa-dwh.Admin.Order_Items`  
    WHERE  DATE(Creation_Datetime)  >= '2022-05-01'
        AND (LOWER(affiliate_code) LIKE '%lp2774%' OR LOWER(affiliate_code) LIKE '%lp2720%')
                AND LOWER(Order_Coupon) NOT LIKE 'inf100%'
                AND LOWER(Order_Coupon) NOT LIKE 'ytsc%'
                AND LOWER(Order_Coupon) NOT LIKE 'igsc%'
    GROUP BY 1,2,3,4,5,6,7,8
    ) S 
ON T.Order_id=S.Order_id AND T.Channel_Name=S.Channel_Name

WHEN NOT MATCHED THEN
INSERT (Order_id, Store, Device, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax,Channel_Name )
VALUES(Order_id, Store, Device, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax,Channel_Name )
;


-----------Batch inf Sales Combined-----------------------
--DELETE FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Sales_Combined` WHERE TRUE;
MERGE `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Sales_Combined` T 
USING(   
      SELECT Order_id, Store, Device, A.Channel_Name, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax, Budget_Date,Plat, Sale_Type, '' AS Sub_Date
      FROM(
            --- / Sales Only in IR Table / ----------
            SELECT DISTINCT Order_id, Store, Device, A.Channel_Name, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax,'IR' As Plat, Sale_Type
            FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Sales` A
            LEFT JOIN (SELECT Order_id, Channel_Name FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Sales`) B
            USING(Order_id)
            WHERE B.Order_id IS NULL
            UNION ALL

            SELECT DISTINCT A.Order_id, A.Store, Device, TRIM(A.Channel_Name) AS Channel_Name, A.customer_email, A.coupon_code, A.Date, A.Date_Time, A.items, A.Order_Full_Price, A.Revenue, A.Revenue_Inc_Tax,'Admin' AS Plat, 'C' AS Sale_Type
            FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Sales` A
            LEFT JOIN (SELECT Order_id, Channel_Name  FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Sales`) B
            USING(Order_id)
            WHERE B.Order_id IS NULL 
            UNION ALL

            SELECT DISTINCT A.Order_id, A.Store, A.Device, TRIM(B.Channel_Name) AS Channel_Name, A.customer_email, A.coupon_code, A.Date, A.Date_Time, A.items, A.Order_Full_Price, A.Revenue, A.Revenue_Inc_Tax,'Both' AS Plat, 'C' AS Sale_Type
            FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Admin_Sales` A
            INNER JOIN `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_IR_Sales` B
            USING(Order_id)
      ) A
      LEFT JOIN (SELECT DISTINCT Influencer_Name AS Channel_Name,Budget, Planned_Date, Budget_Date, ToDate  FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_Budget_Ready_View`) B 
        ON IFNULL(A.Channel_Name,'xxxx')=IFNULL(B.Channel_Name,'xxxx')
      AND CAST(Date_time AS DATE) >= Budget_Date AND  CAST(Date_time AS DATE) <= ToDate
      ORDER BY Date_Time
      
     
) S ON T.Order_id=S.Order_id AND T.Channel_Name=S.Channel_Name

WHEN MATCHED AND T.Budget_Date != S.Budget_Date OR T.Budget_Date IS NULL THEN UPDATE SET T.Budget_Date = S.Budget_Date

WHEN NOT MATCHED THEN INSERT ( Order_id, Store, Device, Channel_Name, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax, Budget_Date, Plat, Sale_Type, Sub_Date ) 
  VALUES(Order_id, Store, Device, Channel_Name, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax, Budget_Date, Plat, Sale_Type, Sub_Date)   
;


-----------Batch inf Converted Leads-----------------------
--DELETE FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Converted_Leads` WHERE TRUE;
MERGE INTO `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Converted_Leads` T
USING(

      SELECT DISTINCT Order_id, Store, Device,A.Channel_Name, customer_email, coupon_code, CAST(Order_Date AS DATE) AS Date, Order_Date AS Date_Time, 
             items, Order_Full_Price, Revenue,Revenue_Inc_Tax,Budget_Date,    
             'Conv Lead' AS Plat,Sub_Date,'P' AS Sale_type
      FROM (
            SELECT DISTINCT CAST(TO_HEX(MD5(LOWER(subscriber_email))) AS STRING) AS customer_email, MAX(Datetime) AS Sub_Date, TRIM(Channel_Name) AS Channel_Name,
            FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Leads_Combined`     
            GROUP BY 1,3
            ) A
      LEFT JOIN 
           (SELECT DATE(Creation_Datetime) AS Order_Date, CAST(TO_HEX(MD5(LOWER(customer_email))) AS STRING) AS customer_email ,CAST(Order_Number AS STRING) AS Order_id , New_Returning AS Client_Type,
                  Store, Device, Order_Coupon AS coupon_code,
                  DATETIME(Creation_Datetime) Date_Time, 
                  CAST(SUM(Item_Qty_Ordered) AS INT64) AS items, SUM(ItemRevenue ) AS Revenue, SUM(Item_Tax) Tax, SUM(Item_Price) AS Order_Full_Price, SUM(ItemRevenue )+SUM(Item_Tax) AS Revenue_Inc_Tax
            FROM `gusa-dwh.Admin.Order_Items`  
            WHERE LOWER(affiliate_code) NOT LIKE '%lp2774%' AND LOWER(affiliate_code) NOT LIKE '%lp2720%'
                  AND DATE(Creation_Datetime)  >= '2022-05-01'
            GROUP BY 1,2,3,4,5,6,7,8
            ) B USING(customer_email)
      LEFT JOIN (SELECT Order_id , TRIM(Channel_Name) as Channel_Name  FROM `gusa-dc-dwh.Influencers_Data.Influencers_Affid_Batch_Sales_Combined`) C USING(Order_id, Channel_Name)
      LEFT JOIN (SELECT DISTINCT TRIM(Influencer_Name) as Channel_Name, Budget, Planned_Date, Budget_Date, ToDate  FROM `gusa-funnel-dwh.Direct_Marketing.GRIN_Budget_Ready_View` ) D
            ON IFNULL(A.Channel_Name,'xxxx')=IFNULL(D.Channel_Name,'xxxx') AND CAST(Sub_Date AS DATE) >= Budget_Date AND  CAST(Sub_Date AS DATE) <= ToDate 
      WHERE Sub_Date <= Order_Date AND C.Order_id IS NULL
      ) S  ON T.customer_email = S.customer_email AND T.Order_id=S.Order_id AND T.Channel_Name = S.Channel_Name AND IFNULL(T.Budget_Date,'2020-01-01')=IFNULL(S.Budget_Date,'2020-01-01')

WHEN MATCHED AND T.Budget_Date !=S.Budget_Date OR T.Budget_Date IS NULL THEN UPDATE SET T.Budget_Date =S.Budget_Date

WHEN NOT MATCHED THEN
      INSERT ( Order_id, Store, Device, Channel_Name, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax, Budget_Date, Plat, Sub_Date, Sale_type )
      VALUES (Order_id, Store, Device, Channel_Name, customer_email, coupon_code, Date, Date_Time, items, Order_Full_Price, Revenue, Revenue_Inc_Tax, Budget_Date, Plat, Sub_Date, Sale_type )
;