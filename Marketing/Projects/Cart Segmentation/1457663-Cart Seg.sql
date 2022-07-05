DECLARE days INT64 DEFAULT 14; 
DECLARE startDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL days DAY));
DECLARE endDate STRING DEFAULT FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

WITH trans AS (
    SELECT DISTINCT
        DATETIME(TIMESTAMP_MICROS((visitStartTime*1000000)+(hits.time*1000)), "America/Los_Angeles") AS tr_datetime,
        fullVisitorId AS tr_fullVisitorId,
        visitNumber AS tr_visitNumber,
        hits.transaction.transactionId,
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(hits) AS hits
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN  startDate AND endDate
        AND hits.transaction.transactionId IS NOT NULL
    )

, base AS (
    SELECT 
        fullVisitorId,
        visitNumber,
        transactionId,
        MAX(hits.hitNumber) AS hitNumber,
        MAX(DATETIME(TIMESTAMP_MICROS((visitStartTime*1000000)+(hits.time*1000)), "America/Los_Angeles")) AS datetime,
    FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(hits) AS hits
    LEFT JOIN Trans T ON fullVisitorId = tr_fullVisitorId AND visitNumber <= tr_visitNumber
        AND DATETIME_DIFF(tr_datetime,DATETIME(TIMESTAMP_MICROS((visitStartTime*1000000)+(hits.time*1000)), "America/Los_Angeles"),MINUTE) <= 60
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN  startDate AND endDate
        AND LOWER(hits.eventInfo.eventCategory) LIKE '%eec%' 
        AND LOWER(hits.eventInfo.eventAction) LIKE '%cart%'
        AND LOWER(hits.eventInfo.eventLabel) LIKE '%products%'
        AND (LOWER(hits.page.pagePath) LIKE '%checkout/cart%' OR LOWER(hits.page.pagePath) LIKE '%checkout/onepage%')
    GROUP BY 1,2,3
    ORDER BY 1,2,4
    )

, products AS (
    SELECT 
        Date,
        deviceCategory,
        fullVisitorId,
        visitNumber,
        transactionId,
        Platform,
        Media_Group,
        Media,

        COUNT(*) AS totalProducts,
        SUM(TotalProductPrice) AS totalPrice,
        
        COUNT(IF(Eyewear_Type = 'Accessories',Eyewear_Type,NULL)) AS Accessories,
        COUNT(IF(Eyewear_Type = 'upgrade',Eyewear_Type,NULL)) AS Upgrades,
        COUNT(IF(Eyewear_Type = 'Contacts',Eyewear_Type,NULL)) AS Contacts,
        COUNT(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand NOT IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND IsPremium != '1' AND lens NOT IN ('Progressive','Bifocal'),Eyewear_Type,NULL)) AS Non_Premium_Non_Multi,
        COUNT(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand NOT IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND IsPremium != '1' AND lens IN ('Progressive','Bifocal'),Eyewear_Type,NULL)) AS Non_Premium_Multi,
        COUNT(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand NOT IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND IsPremium = '1' AND lens NOT IN ('Progressive','Bifocal'),Eyewear_Type,NULL)) AS Premium_Non_Multi,
        COUNT(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand NOT IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND IsPremium = '1' AND lens IN ('Progressive','Bifocal'),Eyewear_Type,NULL)) AS Premium_Multi,
        COUNT(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND lens NOT IN ('Progressive','Bifocal'),Eyewear_Type,NULL)) AS MAP_Non_Multi,
        COUNT(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND lens IN ('Progressive','Bifocal'),Eyewear_Type,NULL)) AS MAP_Multi,
        
        SUM(IF(Eyewear_Type = 'Accessories',TotalProductPrice,NULL)) AS Accessories_Price,
        SUM(IF(Eyewear_Type = 'upgrade',TotalProductPrice,NULL)) AS Upgrades_Price,
        SUM(IF(Eyewear_Type = 'Contacts',TotalProductPrice,NULL)) AS Contacts_Price,
        SUM(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand NOT IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND IsPremium != '1' AND lens NOT IN ('Progressive','Bifocal'),TotalProductPrice,NULL)) AS Non_Premium_Non_Multi_Price,
        SUM(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand NOT IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND IsPremium != '1' AND lens IN ('Progressive','Bifocal'),TotalProductPrice,NULL)) AS Non_Premium_Multi_Price,
        SUM(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand NOT IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND IsPremium = '1' AND lens NOT IN ('Progressive','Bifocal'),TotalProductPrice,NULL)) AS Premium_Non_Multi_Price,
        SUM(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand NOT IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND IsPremium = '1' AND lens IN ('Progressive','Bifocal'),TotalProductPrice,NULL)) AS Premium_Multi_Price,
        SUM(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND lens NOT IN ('Progressive','Bifocal'),TotalProductPrice,NULL)) AS MAP_Non_Multi_Price,
        SUM(IF(Eyewear_Type NOT IN ('Contacts','Accessories','upgrade') AND productBrand IN ('Ray-Ban','Oakley','Persol','Costa Del Mar') AND lens IN ('Progressive','Bifocal'),TotalProductPrice,NULL)) AS MAP_Multi_Price,
    FROM (
        SELECT 
            DATE(TIMESTAMP_MICROS(visitStartTime*1000000), "America/Los_Angeles") AS Date,
            device.deviceCategory,
            A.fullVisitorId,
            A.visitNumber,
            Platform,
            media_name_group AS Media_Group,
            Media,
            transactionId,
            product.productBrand,
            product.productprice/1000000 AS TotalProductPrice,
            MAX(CASE 
                WHEN cd.index=23 THEN cd.value 
            END) AS lens, 
            MAX(CASE
                WHEN cd.index=18 THEN cd.value 
            END) AS IsPremium,
            MAX(CASE
                WHEN cd.index=60 THEN cd.value 
            END) AS Eyewear_Type,
        FROM `gusa-dwh.12571860.ga_sessions_*` A, UNNEST(hits) AS hits, UNNEST(hits.product) as product, UNNEST(product.customdimensions) as cd
        INNER JOIN base B ON A.fullVisitorId = B.fullVisitorId AND A.visitNumber = B.visitNumber AND hits.hitNumber = B.hitNumber
        LEFT JOIN (SELECT DISTINCT source, medium, campaign, adwords_account, Media  FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media`) E
            ON LOWER(trafficsource.source)=E.source AND LOWER(trafficSource.medium)=E.medium AND IFNULL(LOWER(trafficSource.campaign),"zzzz")=IFNULL(E.campaign,"zzzz") AND IFNULL(trafficSource.adwordsClickInfo.customerId,0)=IFNULL(E.adwords_account,0)
        LEFT JOIN `gusa-funnel-dwh.GA_Media_Ref.Media_Type` USING(Media)
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN startDate AND endDate
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        ORDER BY 1,2,3
        )
    GROUP BY 1,2,3,4,5,6,7,8
    )

SELECT 
    Date,
    deviceCategory,
    Platform,
    Media_Group,
    Media,

    IF(totalProducts = 1, '1', IF(totalProducts = 2, '2', IF(totalProducts > 2, '>2', '0'))) AS Amount_Of_Products,
   
    IF(Accessories = 1, '1', IF(Accessories = 2, '2', IF(Accessories > 2, '>2', '0'))) AS Accessories,
    IF(Upgrades = 1, '1', IF(Upgrades = 2, '2', IF(Upgrades > 2, '>2', '0'))) AS Upgrades,
    IF(Contacts = 1, '1', IF(Contacts = 2, '2', IF(Contacts > 2, '>2', '0'))) AS Contacts,
    IF(Non_Premium_Non_Multi = 1, '1', IF(Non_Premium_Non_Multi = 2, '2', IF(Non_Premium_Non_Multi > 2, '>2', '0'))) AS Non_Premium_Non_Multi,
    IF(Non_Premium_Multi = 1, '1', IF(Non_Premium_Multi = 2, '2', IF(Non_Premium_Multi > 2, '>2', '0'))) AS Non_Premium_Multi,
    IF(Premium_Non_Multi = 1, '1', IF(Premium_Non_Multi = 2, '2', IF(Premium_Non_Multi > 2, '>2', '0'))) AS Premium_Non_Multi,
    IF(Premium_Multi = 1, '1', IF(Premium_Multi = 2, '2', IF(Premium_Multi > 2, '>2', '0'))) AS Premium_Multi,
    IF(MAP_Non_Multi = 1, '1', IF(MAP_Non_Multi = 2, '2', IF(MAP_Non_Multi > 2, '>2', '0'))) AS MAP_Non_Multi,
    IF(MAP_Multi = 1, '1', IF(MAP_Multi = 2, '2', IF(MAP_Multi > 2, '>2', '0'))) AS MAP_Multi,

    COUNT(DISTINCT fullVisitorId) AS Users,
    COUNT(DISTINCT CONCAT(fullVisitorId,visitNumber)) AS Sessions,

    COUNT(DISTINCT IF(transactionId IS NOT NULL, fullVisitorId, NULL)) AS Purchasing_Users,
    COUNT(DISTINCT IF(transactionId IS NOT NULL, CONCAT(fullVisitorId,visitNumber), NULL)) AS Purchasing_Sessions,
    COUNT(DISTINCT transactionId) AS Orders,

FROM products
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
















/**/