DECLARE days INT64 DEFAULT 31;
DECLARE startDate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL days DAY);
DECLARE endDate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

------------------Google Ads raw cohort update------------------------
--DELETE FROM `gusa-funnel-dwh.Google_PPC.Google_Cohort_2021` WHERE Date IS NOT NULL;
INSERT INTO `gusa-funnel-dwh.Google_PPC.Google_Cohort_2021` (Cohort_Date, Date, Campaign_ID__AdWords, Ad_Group_ID__AdWords, Ad_ID__AdWords, Device__AdWords, Leads, ATC, New_Customer_Conversions, Conversions, Conversion_Value)

(
SELECT 
    DATE_ADD(CURRENT_DATE(),INTERVAL -1 DAY) AS Cohort_Date,
    PARSE_DATE('%F',Date) AS Date, 
    Campaign_ID__AdWords, 
    Ad_Group_ID__AdWords,
    Ad_ID__AdWords,
    Device__AdWords,
    SUM(lead_Shopping__Main_Acount__AdWords) AS Leads,
    SUM(Cart_Shopping__Main_Acount__AdWords) AS ATC,
    SUM(New_Customer_Purchase_GlassesUSA___Ad_Level__AdWords) AS New_Customer_Conversions,
    SUM(Conversions__AdWords) AS Conversions,
    SUM(Conv__Value__sell_Shopping__Main_Acount__AdWords) AS Conversion_Value,
FROM `gusa-funnel-dwh.Funnel_export_Google_Acount.funnel_data_*`
WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN FORMAT_DATE('%Y%m',startDate) AND FORMAT_DATE('%Y%m',endDate)
    AND PARSE_DATE('%F',Date) Between startDate AND endDate
    AND Connection_name = 'GlassesUSA - Ad Level'
    AND (Conversions__AdWords > 0 OR New_Customer_Purchase_GlassesUSA___Ad_Level__AdWords > 0 OR Cart_Shopping__Main_Acount__AdWords > 0 OR lead_Shopping__Main_Acount__AdWords > 0)
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT 
    DATE_ADD(CURRENT_DATE(),INTERVAL -1 DAY) AS Cohort_Date,
    Date, 
    Campaign_ID__AdWords, 
    CAST(NULL AS STRING) Ad_Group_ID__AdWords,
    CAST(NULL AS STRING) Ad_ID__AdWords,
    Device AS Device__AdWords,
    SUM(lead_GlassesUSA___Campaign_Level___2883597982__AdWords) AS Leads,
    SUM(Add_To_Cart_GlassesUSA___Campaign_Level___2883597982__AdWords) AS ATC,
    CAST(NULL AS INT64) AS New_Customer_Conversions,
    SUM(Conversions__AdWords) AS Conversions,
    SUM(Conv__Value__AdWords) AS Conversion_Value,
FROM `gusa-funnel-dwh.Funnel_export_Google_Account_Campaign_Level.funnel_data_*` A
WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN FORMAT_DATE('%Y%m',startDate) AND FORMAT_DATE('%Y%m',endDate)
    AND Date Between startDate AND endDate
    AND Campaign_ID__AdWords NOT IN (SELECT DISTINCT Campaign_ID__AdWords FROM `gusa-funnel-dwh.Funnel_export_Google_Acount.funnel_data_*`
                WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN FORMAT_DATE('%Y%m',startDate) AND FORMAT_DATE('%Y%m',endDate)
                )
    AND (Conversions__AdWords > 0 OR Add_To_Cart_GlassesUSA___Campaign_Level___2883597982__AdWords > 0 OR lead_GlassesUSA___Campaign_Level___2883597982__AdWords > 0)
GROUP BY 1,2,3,4,5,6
);


--------------------Google Ads final cohort table---------------
DELETE FROM `gusa-funnel-dwh.Google_PPC.Google_Cohort_Final_2021` WHERE Date >= startDate;
INSERT INTO `gusa-funnel-dwh.Google_PPC.Google_Cohort_Final_2021` (Date, Campaign_ID, Ad_Group_ID, Ad_ID, Device, Ad_Name, Ad_Group_Name, Campaign_Name, Ad_group_type, impressions, top_impressions, absolute_top_impressions, Clicks, Spend, Conv, Conv_Value)
(
    SELECT 
        PARSE_DATE('%F',Date) AS Date,
        Ad_ID__AdWords AS Ad_id,
        Ad__AdWords AS Ad_Name,
        Ad_Group_ID__AdWords AS Ad_Group_ID,
        Ad_Group_Name__AdWords AS Ad_Group_Name ,
        Campaign_ID__AdWords AS Campaign_ID	,
        Campaign__AdWords AS Campaign_Name,
        Ad_Group_Type__AdWords AS Ad_group_type,
        Device__AdWords AS Device,
        SUM(Impressions__AdWords) AS  impressions,
        SUM(Total_top_impressions__AdWords) AS top_impressions,
        SUM(Total_absolute_top_impressions__AdWords) AS absolute_top_impressions,
        SUM(Clicks__AdWords) AS Clicks,
        SUM(Cost__AdWords) as Spend,
        SUM(Conversions__AdWords) As Conv,
        SUM(Conv__Value__sell_Shopping__Main_Acount__AdWords) As Conv_Value,
    FROM `gusa-funnel-dwh.Funnel_export_Google_Acount.funnel_data_*` A
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN FORMAT_DATE('%Y%m',startDate) AND FORMAT_DATE('%Y%m',endDate)
        AND PARSE_DATE('%F',Date) Between startDate AND endDate
        AND Connection_Name='GlassesUSA - Ad Level'
    GROUP BY 1,2,3,4,5,6,7,8,9

    UNION ALL

    SELECT 
        Date,
        CAST(NULL AS STRING) AS Ad_id,
        CAST(NULL AS STRING) AS Ad_Name,
        CAST(NULL AS STRING) AS Ad_Group_ID,
        CAST(NULL AS STRING) AS Ad_Group_Name ,
        Campaign_ID__AdWords AS Campaign_ID	,
        Campaign__AdWords AS Campaign_Name,
        CAST(NULL AS STRING) AS Ad_group_type,
        Device,
        SUM(Impressions__AdWords) AS  impressions,
        SUM(Total_top_impressions__AdWords) AS top_impressions,
        SUM(Total_absolute_top_impressions__AdWords) AS absolute_top_impressions,
        SUM(Clicks__AdWords) AS Clicks,
        SUM(Cost__AdWords) as Spend,
        SUM(Conversions__AdWords) As Conv,
        SUM(Conv__Value__AdWords) As Conv_Value,
    FROM `gusa-funnel-dwh.Funnel_export_Google_Account_Campaign_Level.funnel_data_*` A
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN FORMAT_DATE('%Y%m',startDate) AND FORMAT_DATE('%Y%m',endDate)
        AND Date Between startDate AND endDate
        AND Campaign_ID__AdWords NOT IN (SELECT DISTINCT Campaign_ID__AdWords FROM `gusa-funnel-dwh.Funnel_export_Google_Acount.funnel_data_*`
                    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN FORMAT_DATE('%Y%m',startDate) AND FORMAT_DATE('%Y%m',endDate)
                    )
    GROUP BY 1,2,3,4,5,6,7,8,9
);




/**/