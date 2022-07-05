DELETE FROM `gusa-bronto-dwh.Email_click_analysis.testing_groups_LOC` WHERE TRUE;
INSERT INTO `gusa-bronto-dwh.Email_click_analysis.testing_groups_LOC` (TestName,Ver,Type)
(SELECT * FROM `gusa-bronto-dwh.Email_click_analysis.testing_groups` 
WHERE TestName != 'Test Name')
;

DELETE FROM `gusa-bronto-dwh.Newsletter_data.Newsletter_fixed_name_loc` WHERE TRUE;
INSERT INTO `gusa-bronto-dwh.Newsletter_data.Newsletter_fixed_name_loc` (message_name,new_message_name)
(SELECT * FROM `gusa-bronto-dwh.Newsletter_data.Newsletter_fixed_name` )
;

DELETE FROM `gusa-bronto-dwh.Email_click_analysis.AB_test_Click_message` WHERE TRUE;
INSERT INTO `gusa-bronto-dwh.Email_click_analysis.AB_test_Click_message` (Final_message_name,EmailAddress,click_datetime_UTC,click_Datetime_PST,Sent_Datetime_UTC,Sent_Datetime_PST,
message_name,msID,linkUrl,linkname,Device,Device_OS,noOfOrders,daySinceLastOrder,productType,lensType,brandType,mTags,Bulk_Message_name,Message_Date,Offer,Target_Audience,Segment,Event,Email_Length,Creative_Type,Glasses_Type,CMG,Subject_line,Preview_line,Call_to_Action,Button_Color,No_of_Products,orientation,time_slot,Time_Zone,Order_id,TestName,AB_Test_Version)
( 
  SELECT DISTINCT C.Final_message_name ,CAST(TO_HEX(MD5(LOWER(A.Email))) AS STRING) AS EmailAddress,  A.* except(Email), F.* except(AB_Test_Version), B.Order_id, G.TestName,TRIM(AB_Test_Version) AS AB_Test_Version
  FROM `gusa-bronto-dwh.Cordail_Data.message_clicks`  A
  LEFT JOIN `gusa-bronto-dwh.Cordail_Data.message_click_order` B ON A.Sent_Datetime_PST=B.DeliveryDate AND A.click_Datetime_PST=B.ActionDateTime AND A.email=B.EmailAddress AND A.message_name=B.MessageName AND IFNULL(A.linkUrl,'zzz') = IFNULL(B.linkUrl,'zzz')
  LEFT JOIN `gusa-bronto-dwh.Newsletter_data.Newsletter_Final_Message_name`  C
  ON LOWER( A.message_name ) = LOWER ( C.message_name)
  LEFT JOIN `gusa-bronto-dwh.Newsletter_data.Newsletter_Category_view`  D ON D.Final_message_name = c.Final_message_name
  LEFT JOIN `gusa-bronto-dwh.Newsletter_data.Newsletter_Campaign_view` E ON E.Final_message_name = c.Final_message_name
  LEFT JOIN `gusa-bronto-dwh.newsletter_base_date.Bulk_sub_att` F ON F.Bulk_Message_name = c.Final_message_name
  INNER JOIN `gusa-bronto-dwh.Email_click_analysis.testing_groups_LOC` G ON TRIM(LOWER(G.Ver))=TRIM(LOWER(F.AB_Test_Version))
  WHERE A.Sent_Datetime_PST >= '2022-01-01' 
  AND LOWER(A.message_name) NOT IN (SELECT LOWER(Email_exclude) FROM `gusa-bronto-dwh.Email_click_analysis.testing_Exclude_emails`)
  --AND LOWER(F.AB_Test_Version) = 'regular-template'
);

DELETE FROM `gusa-bronto-dwh.Email_click_analysis.AB_test_message_sent` WHERE TRUE;
INSERT INTO `gusa-bronto-dwh.Email_click_analysis.AB_test_message_sent`(Sent_Date,message_name,Sent)
(
  SELECT DATE(Sent_Datetime_PST) AS Sent_Date, message_name, COUNT(*) AS Sent
  FROM `gusa-bronto-dwh.Cordail_Data.message_sent`
  WHERE LOWER(message_name) in (SELECT DISTINCT LOWER(message_name) FROM `gusa-bronto-dwh.Email_click_analysis.AB_test_Click_message`)
  GROUP BY 1,2
);

DELETE FROM `gusa-bronto-dwh.Email_click_analysis.AB_test_orders` WHERE TRUE;
INSERT INTO `gusa-bronto-dwh.Email_click_analysis.AB_test_orders` (EmailAddress,
  Order_Number, Device, Item_Qty_Ordered, Order_Coupon,  Item_Name,  FrameType, Lens, brand, EyewearType, Package, LensType, LensTypeDetailed, PackageDetailed, Eye_Sun, New_Returning, ItemRevenue)
(
  SELECT CAST(TO_HEX(MD5(LOWER(customer_email))) AS STRING) AS EmailAddress,
    Order_Number, Device, Item_Qty_Ordered, Order_Coupon,  Item_Name,  FrameType, Lens, brand, EyewearType, Package, LensType, LensTypeDetailed, 
    PackageDetailed, Eye_Sun, New_Returning, ItemRevenue
  FROM `gusa-dwh.Admin.Order_Items` 
  WHERE CAST(Order_Number AS STRING) IN 
    (SELECT DISTINCT Order_id 
    FROM `gusa-bronto-dwh.Email_click_analysis.AB_test_Click_message`
    WHERE Sent_Datetime_PST >= '2022-01-01'
    AND Order_id IS NOT NULL)
)


/**/