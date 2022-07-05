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

    SELECT CAST(Order_Number AS STRING) AS Order_id, B.Store, B.Device, A.Channel_Name, CAST(TO_HEX(MD5(B.customer_email)) AS STRING) AS customer_email, B.Order_Coupon AS coupon_code, DATE(Creation_Datetime) AS Date, DATETIME(Creation_Datetime) Date_Time, Sale_type,
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






/**/