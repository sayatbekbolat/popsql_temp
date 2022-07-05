WITH ordersDB AS 
    ( 
        SELECT 
            LOWER(A.customer_email) AS EmailAddress, 
            CAST(A.Order_Number AS STRING) AS Order_id, 
            New_Returning AS NewRet_TotalGUSA, 
            CO.Rank, 
            IF(CO.Rank>1,'Return','New') As New_Ret, 
            Cohort_Date,
            SUM(IF(FrameType = 'Contact Lenses',ItemRevenue,0)) AS Contacts_Revenue,
            SUM(IF(FrameType != 'Contact Lenses',ItemRevenue,0)) AS Glasses_Revenue
        FROM `gusa-dwh.Admin.Order_Items` A
        LEFT JOIN `gusa-dwh.BI.Contacts_Orders_Number` CO ON LOWER(A.customer_email) = LOWER(CO.customer_email) AND A.Order_Number = CO.Order_Number
        LEFT JOIN (SELECT customer_email,Creation_Datetime AS Cohort_Date FROM `gusa-dwh.BI.Contacts_Orders_Number` WHERE Rank = 1) COD ON LOWER(A.customer_email) = LOWER(COD.customer_email)
        WHERE A.Creation_Datetime BETWEEN '2020-01-01' AND DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
        GROUP BY 1,2,3,4,5,6

    )

 
SELECT 
  Date_Time,Date,CAST(TO_HEX(MD5(A.EmailAddress)) AS STRING) AS EmailAddress,C.Final_message_name,Newsletter_Category , 
  CASE
          WHEN Newsletter_Category IN ('Cart Abandon','Product Abandon','Category Abandon','Post Purchase Contact Lenses','Welcome') AND
              LOWER(C.Final_message_name) LIKE '%contact%' THEN 'Auto'
          WHEN LOWER(C.Final_message_name) LIKE '%pps-contact%' THEN 'Auto'   
          WHEN Newsletter_Category IN ('Contacts Cashback Reorder') THEN 'Reorder'
          WHEN LOWER(C.Final_message_name) LIKE '%re-order_series_contacts%' THEN 'Reorder'
          WHEN Newsletter_Category LIKE '%Contact%' THEN 'Newsletter Contacts'
          ELSE 'Eyelasses Emails'
      END AS Newsletter_Segment,
      Contacts_Revenue,
      Glasses_Revenue,
      New_Ret,
      NewRet_TotalGUSA,
      Cohort_Date
FROM (
        SELECT 
            DATE(ActionDateTime) AS Date_Time, 
            EmailAddress,
            linkUrl,
            Order_id,
            DATE(ActionDateTime) AS Date,
            messageName 
        FROM `gusa-bronto-dwh.newsletter_base_date.Newsletter_click_orders` 
        WHERE ActionDateTime BETWEEN '2020-01-01' AND DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
        UNION ALL

        SELECT 
            DATE(ActionDateTime) AS Date_Time, 
            EmailAddress,
            linkUrl,
            Order_id,
            DATE(ActionDateTime) AS Date,
            messageName 
        FROM `gusa-bronto-dwh.Cordail_Data.message_click_order` 
        WHERE ActionDateTime BETWEEN '2020-01-01' AND DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
) A
LEFT JOIN ordersDB ON LOWER(A.EmailAddress)=ordersDB.EmailAddress AND A.Order_id=ordersDB.Order_id
LEFT JOIN `gusa-bronto-dwh.Newsletter_data.Newsletter_Final_Message_name`  C ON LOWER( messageName ) = LOWER ( C.message_name)
LEFT JOIN `gusa-bronto-dwh.Newsletter_data.Newsletter_Category_view`  D ON D.Final_message_name = c.Final_message_name
LEFT JOIN `gusa-bronto-dwh.Newsletter_data.Newsletter_Campaign_view` E ON E.Final_message_name = c.Final_message_name

AND C.Final_message_name IS NOT NULL