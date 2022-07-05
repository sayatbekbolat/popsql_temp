--assisting columns
WITH base AS 
    SELECT A.*,K.*,
        
        --Check if product name contains the color assigned to it
        IF(
            STRPOS(
                name,
                REPLACE(IF(STRPOS(color,',')>0,
                        LEFT(color,STRPOS(color,',')-2),
                        color),'Shiny ','')
                ) > 0
            OR
            STRPOS(
                name,
                REPLACE(IF(STRPOS(color,',')>0,
                        TRIM(SPLIT(color)[ORDINAL(2)]),
                        color),'Shiny ','')
                ) > 0
            , true, false) AS has_color_in_name,
        
        
        IF(
            STRPOS(shape,',')>0,
                LEFT(shape,STRPOS(shape,',')-2),
                shape) AS single_shape,
        
        --hardcode check for sport category
        IF(
            category_ids LIKE '%234%','Reading',
            IF(category_ids LIKE '%119%','Sport','0')
            ) AS sport_reading,
           
        --removing colors, signs and spaces from product name
        TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                CASE
                                    WHEN ARRAY_LENGTH(SPLIT(REPLACE(color, ' , ',' '), ' ')) = 5 THEN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(REPLACE(name,'Muse X Hilary Duff','Hilary Duff Muse')),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (5)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (4)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (3)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (2)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (1)],'')          
                                    WHEN ARRAY_LENGTH(SPLIT(REPLACE(color, ' , ',' '), ' ')) = 4 THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(REPLACE(name,'Muse X Hilary Duff','Hilary Duff Muse')),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (4)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (3)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (2)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (1)],'')          
                                    WHEN ARRAY_LENGTH(SPLIT(REPLACE(color, ' , ',' '), ' ')) = 3 THEN REPLACE(REPLACE(REPLACE(TRIM(REPLACE(name,'Muse X Hilary Duff','Hilary Duff Muse')),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (3)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (2)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (1)],'')
                                    WHEN ARRAY_LENGTH(SPLIT(REPLACE(color, ' , ',' '), ' ')) = 2 THEN REPLACE(REPLACE(TRIM(REPLACE(name,'Muse X Hilary Duff','Hilary Duff Muse')),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (2)],''),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (1)],'')
                                    WHEN ARRAY_LENGTH(SPLIT(REPLACE(color, ' , ',' '), ' ')) = 1 THEN REPLACE(TRIM(REPLACE(name,'Muse X Hilary Duff','Hilary Duff Muse')),SPLIT(REPLACE(color, ' , ',' '), ' ')[ORDINAL (1)],'')
                                END,
                                REPLACE(color,' , ',' '),''),
                            'w/',''),
                        '/',''),
                    ',',''),
                '   ',' '),
            '  ',' ')
            ) AS base_name, 
        
        --constructing product category    
        CONCAT(
        IF(LOWER(eyewear_type) = 'eyeglasses',
            'Eyeglasses',
            'Sunglasses'
            ), ' > ',
        IF(brand IS NULL OR brand = '','GlassesUSA',brand), ' > ',
        REPLACE(shape,' , ','/'), ' > ',
        IF(category_ids LIKE '%159%' OR LOWER(name) LIKE '%kids%','Kids',
            IF(LOWER(frame_size) IN ('pre-teens','juniors','toddlers'),'Kids',
                'Adult')
            )
        ) AS product_type,
        
        --expanded eyewear type
        IF(LOWER(eyewear_type) = 'eyeglasses',
            'Eyeglasses',
            IF(eligible_for_rx_lenses != 'Yes','Sunglasses (Non-RX)','Sunglasses (RX-able)')
            ) AS eyewear_type,
        
        CONCAT('https://www.glassesusa.com/',url_key,'.html') AS link,
        CONCAT('https://m.glassesusa.com/',url_key,'.html') AS mobile_link,
        attribute_set,
        IF(extlab = 'DSCO', 'DSCO', NULL) AS dsco,
        extlab,
        
        ARRAY_REVERSE(SPLIT(front_image,'/')) AS modified_front_image,

        IF(P.Price IS NULL, A.price, P.Price) AS fixed_price,
        IF(P.Final_Price IS NULL, A.final_price, P.Final_Price) AS fixed_final_price,

        CAST(NULL AS STRING) AS valid_for_hto,
        
  
    FROM `gusa-dc-dwh.Marketing.inStock` A
    LEFT JOIN `gusa-dc-dwh.Marketing.Keywords` K ON sku = Unique_ID
    LEFT JOIN (SELECT sku, frame_type, IFNULL(eyewear_type,'Eyeglasses') AS eyewear_type, eligible_for_rx_lenses, attribute_set, extlab_id AS extlab FROM `gusa-dwh.Admin.Inventory` GROUP BY 1,2,3,4,5,6) I ON LOWER(A.sku) = LOWER(I.sku)
    LEFT JOIN `gusa-dc-dwh.Marketing.GS_Prices_Fix` P ON LOWER(A.sku) = LOWER(P.sku)
    WHERE CAST(A.qty AS INT64) > 0
        AND (LOWER(A.sku) NOT LIKE '%lenses%'
        OR LOWER(A.sku) NOT LIKE '%nico%'
        OR LOWER(A.sku) NOT LIKE '%snap-%'
        OR LOWER(A.sku) NOT LIKE '%product%')
        AND SUBSTR(A.sku,3,1) = '-'
        AND A.sku NOT LIKE '73-%'
)
 
    
SELECT
    sku AS id,
    'in stock' AS availability, /*only in stock was filtered in the cte*/
    qty AS quantity_to_sell_on_facebook,
    'new' AS condition,
    
    --constructing the description
    CONCAT(
        TRIM(REPLACE(
            REPLACE(
                IF(has_color_in_name, /*if color exists, don't add it again*/
                    CONCAT(name,' ',TRIM(IF(STRPOS(shape,',')>0,LEFT(shape,STRPOS(shape,',')-2),shape)),' ',TRIM(Keyword)),
                    CONCAT(name,' ',TRIM(color),' ',TRIM(IF(STRPOS(shape,',')>0,LEFT(shape,STRPOS(shape,',')-2),shape)),' ',TRIM(Keyword))
                    )
                ,' , ','/'),
            'Muse X Hilary Duff',
            'Hilary Duff Muse')),
        ' - Price includes high quality frames, standard prescription lenses, shipping, free case and cloth. ',
        frame_size,' ',
        color,' ',
        frame_type,' ',
        framematerial,' ',
        TRIM(Keyword),' ',
        IF(gender='Men',' for Men.',IF(gender='Women',' for Women.','.')),' These ',
        shape,' shaped ',
        TRIM(Keyword),' are great for ',
        face_shape,' shaped faces. Look great in your stylish ',
        TRIM(Keyword),'. Only $',
        ROUND(CAST(final_price AS FLOAT64),2),
        IF(SKU_Prefix = '62-','',' - includes shipping!')
        ) AS description,
    
    IF(front_image IS NOT NULL AND front_image != '',CONCAT('https://optimaxweb.glassesusa.com/image/upload/w_600,h_600,c_pad,b_white,f_auto,q_auto/media/catalog/product/',CONCAT(modified_front_image[SAFE_ORDINAL(3)],'/',modified_front_image[SAFE_ORDINAL(2)],'/',modified_front_image[SAFE_ORDINAL(1)])),NULL) AS image_link,
    
    CONCAT(link,
        IF(`gusa-dc-dwh.Marketing.Promo_Code_func` (product_badge,brand,SKU_Prefix,category_ids,'facebook',attribute_set,sku,NULL) = 'No Promo','',CONCAT('?promo=',`gusa-dc-dwh.Marketing.Promo_Code_func` (product_badge,brand,SKU_Prefix,category_ids,'facebook',attribute_set,sku,NULL)))
        ) AS link,
    
    REPLACE(TRIM(name),'Muse X Hilary Duff','Hilary Duff Muse') AS title,
    CONCAT(ROUND(CAST(fixed_price AS FLOAT64),2),' USD') AS price,
    G.UPC AS gtin, /*GTIN from table*/
    M.MPN AS mpn, /*MPN from table*/
    IF(brand IS NULL OR brand = '','GlassesUSA',brand) AS brand,
    
    IF(side_image IS NOT NULL AND side_image != '', 
        CONCAT('https://optimaxweb.glassesusa.com/image/upload/w_600,h_600,c_pad,b_white,f_auto,q_auto/media/catalog/product',side_image,IF(angle_image IS NOT NULL AND angle_image != '', CONCAT(',https://optimaxweb.glassesusa.com/image/upload/w_600,h_600,c_pad,b_white,f_auto,q_auto/media/catalog/product',angle_image),'')),
        IF(angle_image IS NOT NULL AND angle_image != '', CONCAT('https://optimaxweb.glassesusa.com/image/upload/w_600,h_600,c_pad,b_white,f_auto,q_auto/media/catalog/product',angle_image),NULL) 
        ) AS additional_image_link,
    
    --Adults/Kids    
    IF(category_ids LIKE '%159%' OR LOWER(base_name) LIKE '%kids%','Kids',
        IF(LOWER(frame_size) IN ('pre-teens','juniors','toddlers'),'Kids','Adult')
        ) AS age_group,
    
    REPLACE(color,' , ','/') AS color,
    CAST(NULL AS DATE) AS expiration_date,
    
    --Male/Female/Unisex
    IF(LOWER(gender) IN ('men','boys'),'Male',
        IF(LOWER(gender) IN ('women','girls'),'Female','Unisex')
        ) AS gender,
    
    REPLACE(TRIM(name),'Muse X Hilary Duff','Hilary Duff Muse') AS item_group_id,
    IF(LOWER(eyewear_type) LIKE '%sunglasses%','Apparel & Accessories > Clothing Accessories > Sunglasses','Health & Beauty > Personal Care > Vision Care > Eyeglasses') AS google_product_category,    
    frame_size AS size,
    framematerial AS material,
    CAST(NULL AS STRING) AS pattern,
    IF(product_badge IS NULL OR product_badge = '','Home Brand',product_badge) AS product_type,
    
    --recalculating the final price by the promo rules
    CONCAT(ROUND(
        IF(`gusa-dc-dwh.Marketing.Promo_Code_func` (product_badge,brand,SKU_Prefix,category_ids,'facebook',attribute_set,sku,NULL) = 'No Promo',
            CAST(fixed_final_price AS FLOAT64),
            CAST(fixed_final_price AS FLOAT64) * (1-`gusa-dc-dwh.Marketing.Promo_Code_Discount_func` (product_badge,brand,SKU_Prefix,category_ids,'facebook',attribute_set,sku,NULL))),2),
        ' USD') AS sale_price,
    
    CAST(NULL AS DATE) AS sale_price_effective_date,
    CAST(NULL AS FLOAT64) AS shipping,
    '0.50 lb' AS shipping_weight,
    CAST(NULL AS STRING) AS shipping_size,
   
    IF(product_badge LIKE '%Special Sale%', 'Special Sale',product_badge) AS custom_label_0,
    TRIM(base_name) AS custom_label_1,
    extlab AS custom_label_2,
    eyewear_type AS custom_label_3,

FROM base B
LEFT JOIN `gusa-dc-dwh.Marketing.gtin` G USING(sku)
LEFT JOIN `gusa-dc-dwh.Marketing.MPN` M ON B.sku = M.id
LEFT JOIN `gusa-dc-dwh.Marketing.pla_top_prods_loc` T USING(sku)
ORDER BY sku