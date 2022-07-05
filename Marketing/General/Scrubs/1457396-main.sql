SELECT DISTINCT Order_Number, Order_SubTotal, SUM(ItemRevenue) AS Revenue, SUM(ItemRevenue+Item_Gift_Certificate_Amount) AS Revenue_with_GC, DATE(Creation_Datetime) AS Date FROM `gusa-dwh.Admin.Order_Items`
WHERE Creation_Datetime BETWEEN '2022-05-01' AND '2022-06-01'
GROUP BY 1,2,5