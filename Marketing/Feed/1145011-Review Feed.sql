WITH orders AS (
    SELECT Creation_Datetime, Order_Number, FinalShortSKU, customer_email,
        IF(LAG(Creation_Datetime) OVER (PARTITION BY customer_email, FinalShortSKU ORDER BY Creation_Datetime ASC) IS NULL,
            DATETIME_SUB(Creation_Datetime,INTERVAL 1 YEAR),
            LAG(Creation_Datetime) OVER (PARTITION BY customer_email, FinalShortSKU ORDER BY Creation_Datetime ASC)
            ) AS prev,
        IF(LEAD(Creation_Datetime) OVER (PARTITION BY customer_email, FinalShortSKU ORDER BY Creation_Datetime ASC) IS NULL,
            DATETIME_ADD(Creation_Datetime,INTERVAL 1 YEAR),
            LEAD(Creation_Datetime) OVER (PARTITION BY customer_email, FinalShortSKU ORDER BY Creation_Datetime ASC)
            ) AS post,
    FROM `gusa-dwh.Admin.Order_Items`
    --WHERE LOWER(customer_email) = 'davkitiya093@yahoo.com'
    )

, orders2 AS (
    SELECT Creation_Datetime, Order_Number, customer_email,
        IF(LAG(Creation_Datetime) OVER (PARTITION BY customer_email ORDER BY Creation_Datetime ASC) IS NULL,
            DATETIME_SUB(Creation_Datetime,INTERVAL 1 YEAR),
            LAG(Creation_Datetime) OVER (PARTITION BY customer_email ORDER BY Creation_Datetime ASC)
            ) AS prev,
        IF(LEAD(Creation_Datetime) OVER (PARTITION BY customer_email ORDER BY Creation_Datetime ASC) IS NULL,
            DATETIME_ADD(Creation_Datetime,INTERVAL 1 YEAR),
            LEAD(Creation_Datetime) OVER (PARTITION BY customer_email ORDER BY Creation_Datetime ASC)
            ) AS post,
    FROM `gusa-dwh.Admin.Order_Items`
    --WHERE LOWER(customer_email) = 'davkitiya093@yahoo.com'
    GROUP BY 1,2,3
    )


SELECT review_id, reviewer_name, reviewer_id, 
    CONCAT(DATE(review_timestamp),'T',TIME(review_timestamp),
        IF(DATETIME_DIFF(DATETIME(review_timestamp,'America/Los_Angeles'),DATETIME(review_timestamp),HOUR) < 10,
            CONCAT('-0',DATETIME_DIFF(DATETIME(review_timestamp),DATETIME(review_timestamp,'America/Los_Angeles'),HOUR)),
            CAST(DATETIME_DIFF(DATETIME(review_timestamp,'America/Los_Angeles'),DATETIME(review_timestamp),HOUR) AS STRING)),
        ':00') AS review_timestamp, 
    title, content, review_url, min, max, overall, sku, brand, gtin, mpn, product_name, product_url, is_spam, collection_method, transaction_id,
    IF(LEFT(pros,1)=',', RIGHT(pros,LENGTH(pros)-1), pros) AS pros,
    IF(LEFT(cons,1)=',', RIGHT(cons,LENGTH(cons)-1), cons) AS cons,
    
FROM (    
    SELECT DISTINCT
        A.id AS review_id,
        nickname AS reviewer_name,
        author_id AS reviewer_id,
        A.created_at AS review_timestamp,
        title,
        detail AS content,
        CONCAT('https://www.glassesusa.com/',url_key,'.html') AS review_url,
        1 AS min,
        5 AS max,
        overall,
        
        sku,
        C.brand,
        G.UPC AS gtin,
        M.MPN AS mpn,
        C.name AS product_name,
        CONCAT('https://www.glassesusa.com/',url_key,'.html') AS product_url,
        'false' AS is_spam,
        'post_fulfillment' AS collection_method,
        IF(O.Order_Number IS NULL,O2.Order_Number,O.Order_Number) AS transaction_id,
        
        CONCAT(IF(recommend,'Would recommend to others',''),
            IF(fit = 2,',True to size',''),
            IF(A.style IS NOT NULL,CONCAT(',',
                CASE 
                    WHEN A.style = 1 THEN 'Chic'
                    WHEN A.style = 2 THEN 'Vintage'
                    WHEN A.style = 3 THEN 'Classic'
                END,' style'),''),
            IF(quality = 3,',High Quality','')
            ) AS pros,
        
        CONCAT(
            IF(fit != 2 AND fit IS NOT NULL,CONCAT(',',IF(fit=1,'Loose','Tight'),' fit'),''),
            IF(quality = 1 AND quality IS NOT NULL,',Low Quality','')
            ) AS cons,
        
    FROM `gusa-dwh.backend.reviewms_ms_review` A
    LEFT JOIN `gusa-dwh.backend.reviewms_ms_author` B ON author_id = B.id
    LEFT JOIN `gusa-dwh.Admin.Inventory` C USING(product_id) 
    LEFT JOIN `gusa-dc-dwh.Marketing.gtin` G USING(sku)
    LEFT JOIN `gusa-dc-dwh.Marketing.MPN` M ON sku = M.id
    LEFT JOIN `gusa-dc-dwh.Marketing.inStock` USING(sku)
    LEFT JOIN orders O ON LOWER(email) = LOWER(customer_email) AND sku = FinalShortSKU AND DATETIME(A.created_at,'America/Los_Angeles') BETWEEN prev AND post
    LEFT JOIN orders2 O2 ON LOWER(email) = LOWER(O2.customer_email) AND DATETIME(A.created_at,'America/Los_Angeles') BETWEEN O2.prev AND O2.post
    
    WHERE /*DATE(A.created_at) >= '2022-01-07' AND*/ status_id = 1
        --AND LOWER(email) = 'deanders2000@yahoo.com'
)