DECLARE
    StartDate,
    EndDate STRING;

DECLARE
    PageNumber INT64;

SET
    StartDate = '20220601';

SET
    EndDate = '20220601';

SET
    PageNumber = 6;

SELECT
    *
FROM
    (
        WITH
            X_first_pages AS(
                SELECT
                    visitorid,
                    VisitNumber,
                    hitnumber
                FROM
                    (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY visitorid,
                                VisitNumber
                                ORDER BY
                                    hitnumber ASC
                            ) AS PageNum
                        FROM
(
                                SELECT
                                    CAST(cd.value AS STRING) AS visitorid,
                                    VisitNumber,
                                    hitnumber,
                                    hits.page.pagePath
                                FROM
                                    `gusa-dwh.12571860.ga_sessions_*`
                                    LEFT JOIN
                                UNNEST
(customDimensions) AS cd
                                    LEFT JOIN
                                UNNEST
(hits) AS hits
                                WHERE
                                    REGEXP_EXTRACT(_TABLE_SUFFIX, r '[0-9]+') BETWEEN StartDate AND EndDate
                                    AND hits.type = 'PAGE'
                                    AND CD.INDEX = 10
                            )
                    )
                WHERE
                    PageNum = PageNumber --- Means we will take all pages until the Xth page
            ),
            all_visitor_stats AS (
                SELECT
                    CAST(cd.value AS STRING) AS visitorid,
                    device.deviceCategory AS device_category,
                    ANY_VALUE(
                        CASE
                            WHEN SUBSTR(hits_product.productSKU, 0, 2) IN (
                                '34',
                                '44',
                                '91',
                                '36',
                                '62',
                                '63',
                                '46',
                                '96',
                                '92',
                                '93'
                            ) THEN 1 #premium
                            WHEN SUBSTR(hits_product.productSKU, 0, 2) NOT IN (
                                '34',
                                '44',
                                '91',
                                '36',
                                '62',
                                '63',
                                '46',
                                '96',
                                '92',
                                '93'
                            ) THEN 0 #NOT premium
                            ELSE NULL
                        END
                    ) AS Target
                FROM
                    `gusa-dwh.12571860.ga_sessions_*`
                    LEFT JOIN
                UNNEST
(customDimensions) AS cd
                    LEFT JOIN
                UNNEST
(hits) AS hits
                    LEFT JOIN
                UNNEST
(hits.product) AS hits_product
                WHERE
                    REGEXP_EXTRACT(_TABLE_SUFFIX, r '[0-9]+') BETWEEN StartDate AND EndDate
                    AND visitNumber = 1
                    AND HITS.ecommerceaction.action_type = '6'
                    AND cd.INDEX = 10
                    AND NOT (
                        SUBSTR(hits_product.productSKU, 1, 1) = '1'
                        AND SUBSTR(hits_product.productSKU, 4, 1) = '-'
                    ) -- NOT CONTACT LENSES
                    AND NOT (
                        SUBSTR(hits_product.productSKU, 1, 1) = '2'
                        AND SUBSTR(hits_product.productSKU, 4, 1) = '-'
                    ) -- NOT ACCESSORIES
                GROUP BY
                    visitorid,
                    device_category
                HAVING
                    COUNT(
                        DISTINCT CASE
                            WHEN SUBSTR(hits_product.productSKU, 0, 2) IN (
                                '34',
                                '44',
                                '91',
                                '36',
                                '62',
                                '63',
                                '46',
                                '96',
                                '92',
                                '93'
                            ) THEN 1 #premium
                            WHEN SUBSTR(hits_product.productSKU, 0, 2) NOT IN (
                                '34',
                                '44',
                                '91',
                                '36',
                                '62',
                                '63',
                                '46',
                                '96',
                                '92',
                                '93'
                            ) THEN 0 #NOT premium
                            ELSE NULL
                        END
                    ) = 1
            ),
            avg_product_prices AS (
                SELECT
                    A.visitorid,
                    AVG(productPrice / 1000000) AS AvgPrice,
                    MIN(productPrice / 1000000) AS MinPrice
                FROM
                    (
                        SELECT
                            CAST(cd.value AS STRING) AS visitorid,
                            hits_product.productPrice AS productPrice,
                            VisitNumber,
                            hitnumber
                        FROM
                            `gusa-dwh.12571860.ga_sessions_*`
                            LEFT JOIN
                        UNNEST
(customDimensions) AS cd
                            LEFT JOIN
                        UNNEST
(hits) AS hits
                            LEFT JOIN
                        UNNEST
(hits.product) AS hits_product
                        WHERE
                            REGEXP_EXTRACT(_TABLE_SUFFIX, r '[0-9]+') BETWEEN StartDate AND EndDate
                            AND visitNumber = 1
                            AND hits.ecommerceaction.action_type = '2' -- can use eventlabel
                            AND cd.INDEX = 10
                    ) prices
                    JOIN X_first_pages A ON A.visitorid = prices.visitorid
                    AND A.VisitNumber = prices.VisitNumber
                    AND prices.hitnumber < A.hitnumber
                GROUP BY
                    visitorid
            ),
            is_premium_in_cart AS (
                SELECT
                    A.visitorid,
                    CASE
                        WHEN COUNTIF(
                            SUBSTR(productSKU, 0, 2) IN (
                                '34',
                                '44',
                                '91',
                                '36',
                                '62',
                                '63',
                                '46',
                                '96',
                                '92',
                                '93'
                            )
                        ) > 0 THEN 1
                        ELSE 0
                    END AS premium_in_cart,
                FROM
(
                        SELECT
                            CAST(cd.value AS STRING) AS visitorid,
                            hits_product.productSKU as productSKU,
                            VisitNumber,
                            hitnumber
                        FROM
                            `gusa-dwh.12571860.ga_sessions_*`,
                        UNNEST
(customDimensions) AS cd,
                        UNNEST
(hits) AS hits,
                        UNNEST
(hits.product) AS hits_product
                        WHERE
                            REGEXP_EXTRACT(_TABLE_SUFFIX, r '[0-9]+') BETWEEN StartDate AND EndDate
                            AND visitNumber = 1
                            AND HITS.ecommerceaction.action_type = '3'
                            AND cd.INDEX = 10
                    ) P
                    JOIN X_first_pages A ON A.visitorid = P.visitorid
                    AND A.VisitNumber = P.VisitNumber
                    AND P.hitnumber < A.hitnumber
                GROUP BY
                    visitorid
            ),
            contain_35 AS (
                SELECT
                    A.visitorid,
                    CASE
                        WHEN COUNTIF(
                            hits_type = 'PAGE'
                            AND pagepath LIKE '%35%'
                            AND cd_index = 48
                            AND hits_cd_value = 'PRODUCT'
                        ) > 0 THEN 1
                        ELSE 0
                    END AS HouseProductCount,
                FROM
                    (
                        SELECT
                            CAST(cd.value AS STRING) AS visitorid,
                            hits.type AS hits_type,
                            hits.PAGE.pagepath as pagepath,
                            hits_cd.INDEX AS cd_index,
                            hits_cd.VALUE AS hits_cd_value,
                            VisitNumber,
                            hitnumber
                        FROM
                            `gusa-dwh.12571860.ga_sessions_*`,
                        UNNEST
(customDimensions) AS cd,
                        UNNEST
(hits) AS hits,
                        UNNEST
(hits.customDimensions) AS hits_cd
                        WHERE
                            REGEXP_EXTRACT(_TABLE_SUFFIX, r '[0-9]+') BETWEEN StartDate AND EndDate
                            AND visitNumber = 1
                            AND cd.INDEX = 10
                    ) P
                    JOIN X_first_pages A ON A.visitorid = P.visitorid
                    AND A.VisitNumber = P.VisitNumber
                    AND P.hitnumber < A.hitnumber
                GROUP BY
                    visitorid
            ),
            premiums AS (
                SELECT
                    A.visitorid,
                    CASE
                        WHEN COUNTIF(
                            cd_index = 48
                            AND hits_cd_value = 'PRODUCT'
                        ) = 0 THEN 0
                        ELSE COUNTIF(
                            hits_type = 'PAGE'
                            AND (
                                pagepath LIKE '%/34-%'
                                OR pagepath LIKE '%/44-%'
                                OR pagepath LIKE '%/91-%'
                                OR pagepath LIKE '%/36-%'
                                OR pagepath LIKE '%/62-%'
                                OR pagepath LIKE '%/63-%'
                                OR pagepath LIKE '%/46-%'
                                OR pagepath LIKE '%/96-%'
                                OR pagepath LIKE '%/92-%'
                                OR pagepath LIKE '%/93-%'
                            )
                            AND cd_index = 48
                            AND hits_cd_value = 'PRODUCT'
                        ) / COUNTIF(
                            cd_index = 48
                            AND hits_cd_value = 'PRODUCT'
                        )
                    END AS PremiumPDPPercentage,
                    COUNTIF(
                        hits_type = 'PAGE'
                        AND (
                            pagepath LIKE '%/34-%'
                            OR pagepath LIKE '%/44-%'
                            OR pagepath LIKE '%/91-%'
                            OR pagepath LIKE '%/36-%'
                            OR pagepath LIKE '%/62-%'
                            OR pagepath LIKE '%/63-%'
                            OR pagepath LIKE '%/46-%'
                            OR pagepath LIKE '%/96-%'
                            OR pagepath LIKE '%/92-%'
                            OR pagepath LIKE '%/93-%'
                        )
                        AND cd_index = 48
                        AND hits_cd_value = 'PRODUCT'
                    ) AS PremiumProductPageCount,
                    COUNTIF(
                        hits_type = 'PAGE'
                        AND cd_index = 48
                        AND hits_cd_value = 'PRODUCT'
                    ) AS ProductPageCount,
                    CASE
                        WHEN COUNTIF(
                            cd_index = 48
                            AND hits_cd_value = 'CATEGORY'
                        ) = 0 THEN 0
                        ELSE COUNTIF(
                            hits_type = 'PAGE'
                            AND (
                                pagepath LIKE '%product_badge=127%'
                                OR pagepath LIKE '%brand=%'
                                OR pagepath LIKE '%ray-ban%'
                                OR pagepath LIKE '%prada-eyewear%'
                                OR pagepath LIKE '%gucci-eyewear%'
                                OR pagepath LIKE '%versace-eyewear%'
                                OR pagepath LIKE '%michael-kors-eyewear%'
                                OR pagepath LIKE '%coach-eyewear%'
                                OR pagepath LIKE '%oakley-glasses%'
                                OR pagepath LIKE '%designer-glasses%'
                            )
                            AND cd_index = 48
                            AND hits_cd_value = 'CATEGORY'
                        ) / COUNTIF(
                            cd_index = 48
                            AND hits_cd_value = 'CATEGORY'
                        )
                    END AS PremiumCategoryPagePercentage,
                    COUNTIF(
                        hits_type = 'PAGE'
                        AND (
                            pagepath LIKE '%product_badge=127%'
                            OR pagepath LIKE '%brand=%'
                            OR pagepath LIKE '%ray-ban%'
                            OR pagepath LIKE '%prada-eyewear%'
                            OR pagepath LIKE '%gucci-eyewear%'
                            OR pagepath LIKE '%versace-eyewear%'
                            OR pagepath LIKE '%michael-kors-eyewear%'
                            OR pagepath LIKE '%coach-eyewear%'
                            OR pagepath LIKE '%oakley-glasses%'
                            OR pagepath LIKE '%designer-glasses%'
                        )
                        AND cd_index = 48
                        AND hits_cd_value = 'CATEGORY'
                    ) AS PremiumCategoryPageCount,
                    COUNTIF(
                        hits_type = 'PAGE'
                        AND cd_index = 48
                        AND hits_cd_value = 'CATEGORY'
                    ) AS CategoryPageCount,
                    COUNTIF(
                        event_category LIKE 'Category -%'
                        AND event_action = 'Filter - Brand'
                        AND event_label NOT IN ('Open', 'Closed')
                    ) AS FilterByBrandCount
                FROM
                    (
                        SELECT
                            CAST(cd.value AS STRING) AS visitorid,
                            hits.type AS hits_type,
                            hits.PAGE.pagepath as pagepath,
                            hits_cd.INDEX AS cd_index,
                            hits_cd.VALUE AS hits_cd_value,
                            VisitNumber,
                            hitnumber,
                            hits.eventinfo.eventCategory AS event_category,
                            hits.eventinfo.eventAction AS event_action,
                            hits.eventinfo.eventLabel AS event_label
                        FROM
                            `gusa-dwh.12571860.ga_sessions_*`,
                        UNNEST
(customDimensions) AS cd,
                        UNNEST
(hits) AS hits,
                        UNNEST
(hits.customDimensions) AS hits_cd
                        WHERE
                            visitnumber = 1
                            AND REGEXP_EXTRACT(_TABLE_SUFFIX, R '[0-9]+') BETWEEN StartDate AND EndDate
                            AND cd.INDEX = 10
                    ) P
                    JOIN X_first_pages A ON A.visitorid = P.visitorid
                    AND A.VisitNumber = P.VisitNumber
                    AND P.hitnumber < A.hitnumber
                GROUP BY
                    visitorid
            )
        SELECT
            *
        EXCEPT
(visitorid, Target),
        Target
        FROM
            all_visitor_stats
            LEFT JOIN premiums USING (visitorid)
            LEFT JOIN avg_product_prices USING (visitorid)
            LEFT JOIN is_premium_in_cart USING (visitorid)
            LEFT JOIN contain_35 USING (visitorid)
    )