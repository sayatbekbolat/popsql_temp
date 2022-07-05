SELECT CAST(Order_Number AS STRING) AS Order_id, Store, affiliate_code, Device, LOWER(customer_email) AS customer_email, Order_Coupon AS coupon_code,
        DATE(Creation_Datetime) AS Date, DATETIME(Creation_Datetime) Date_Time, 
        CAST(SUM(Item_Qty_Ordered) AS INT64) AS items, SUM(ItemRevenue ) AS Revenue, SUM(Item_Tax) Tax, SUM(Item_Price) AS Order_Full_Price, SUM(ItemRevenue )+SUM(Item_Tax) AS Revenue_Inc_Tax
    FROM `gusa-dwh.Admin.Order_Items`  
    WHERE  DATE(Creation_Datetime)  >= '2022-05-01'
        AND (LOWER(affiliate_code) LIKE '%lp2774%' OR LOWER(affiliate_code) LIKE '%lp2720%')
                AND LOWER(Order_Coupon) NOT LIKE 'inf100%'
                AND LOWER(Order_Coupon) NOT LIKE 'ytsc%'
                AND LOWER(Order_Coupon) NOT LIKE 'igsc%'
    GROUP BY 1,2,3,4,5,6,7,8