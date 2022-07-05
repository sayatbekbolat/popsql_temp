WITH all_db AS (
    SELECT * FROM (
        SELECT 
        traffic_source AS Platform,
        dim_1crfd95a1n7up_Marketing_Channel AS Marketing_Channel,
        dim_1cqdafkd2rr0u_Google_Segment AS Media,
        dim_1dr31j709h2ra_Product AS Product_Group,
        dim_1cql3gfsi7hu2_Device As Device, 
        CAST(date AS DATE) AS Date, 
        SUM(common_cost) AS Spend,
        SUM(cf1cqlkd1tph1sa_Conversion) AS Conversions,
        SUM(cf1cqlkl0ljikhs_Conversion_Revenue) AS Conversion_Revenue
    
        FROM
        `gusa-funnel-dwh.funnel_dwh.funnel_data_*`

        WHERE CAST(date AS DATE) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
        AND dim_1cqdafkd2rr0u_Google_Segment NOT IN ('Bronto','Impact')
        AND campaign NOT IN ( SELECT Media FROM `gusa-funnel-dwh.Direct_Marketing.Exclude_list_from_spend_loc`)
        GROUP BY 1,2,3,4,5,6
        
        UNION ALL 

        SELECT T.Platform, 'Acquisition' AS Marketing_Channel, T.Media, IF(Product_Type LIKE '%Contact%', 'Contacts', Product_Type) AS Product_Group, Device, Date,
            SUM(Cost__AdWords) AS Spend,
            SUM(Conversions__AdWords) AS Conversions,
            SUM(Conv__Value__AdWords) AS Conversion_Revenue,
        FROM `gusa-funnel-dwh.Funnel_export_Google_Account_Campaign_Level.funnel_data_*` A
        LEFT JOIN (SELECT * EXCEPT (source,medium) FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media` WHERE LOWER(source) = 'google' AND LOWER(medium) = 'cpc') M ON LOWER(Campaign__AdWords) = LOWER(campaign) AND IFNULL(CAST(Ad_Account_Customer_ID__AdWords AS INT64),0)=IFNULL(adwords_account,0)
        LEFT JOIN `gusa-funnel-dwh.GA_Media_Ref.Media_Type` T USING(Media)
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN '202206' AND FORMAT_DATE('%Y%m',DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))
            AND Campaign__AdWords NOT IN (SELECT DISTINCT campaign FROM `gusa-funnel-dwh.funnel_dwh.funnel_data_*`
                WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN '202206' AND FORMAT_DATE('%Y%m',DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))
                )
        GROUP BY 1,2,3,4,5,6

        UNION ALL
        
        SELECT 
           'LiveIntent' As Platform,'Acquisition'  AS Marketing_Channel,'Liveintent' As Media,'Glasses' AS Product_Group,'Desktop' AS Device,
           Date,SUM( AdvertiserSpent ) AS Spend,SUM( Conversions ) AS Conversions,SUM( Revenue ) AS Conversion_Revenue
        FROM `gusa-dc-dwh.Liveintent.liveintent_full_data`
        WHERE DATE IS NOT NULL
        AND Date >= '2021-09-01'
        AND Date NOT IN (SELECT DISTINCT CAST(Date AS DATE) FROM `gusa-funnel-dwh.funnel_dwh.funnel_data_*` WHERE dim_1cqdafkd2rr0u_Google_Segment = "Liveintent" AND Date >= '2021-09-01')
        GROUP BY 1,2,3,4,5,6
        ORDER BY Date ASC
        
        )
    ORDER BY date DESC
    )

SELECT Date, DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS Max_Date, Platform, Marketing_Channel, Media, Product_Group, Device, Spend, Conversions, Conversion_Revenue
FROM all_db A 
WHERE Media IS NOT NULL
    
UNION ALL

SELECT Date, Max_Date, Platform, Marketing_Channel, Media, Product_Group, Device, Spend, Conversions, Conversion_Revenue 
FROM `gusa-funnel-dwh.Tableau_Data.Manual_Spend_GS_LOC`    
WHERE Date IS NOT NULL

ORDER BY 1 DESC,3,7


/**/