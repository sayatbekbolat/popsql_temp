SELECT DISTINCT
    visitor_id,
    DATETIME(created_at,"America/Los_Angeles") AS PST_date,
    increment_id AS oid,
    revenue,
    user_pseudo_id,
    country,
FROM (
    SELECT DISTINCT
        visitor_id, 
        increment_id,
        created_at,
        revenue,
        COUNT(revenue) OVER (PARTITION BY revenue) AS similar_revenue,
        COUNT(DISTINCT increment_id) OVER (PARTITION BY revenue) AS orders,
        DATETIME_DIFF(FIRST_VALUE(created_at) OVER (PARTITION BY visitor_id ORDER BY created_at DESC),FIRST_VALUE(created_at) OVER (PARTITION BY visitor_id ORDER BY created_at),MINUTE) AS first_to_last,
    FROM `gusa-dwh.backend_realtime.order`
    WHERE DATETIME(created_at) >= DATETIME_SUB((SELECT MAX(DATETIME(created_at)) FROM `gusa-dwh.backend_realtime.order`), INTERVAL 30 MINUTE)
    ORDER BY 4 Desc,1
    )
LEFT JOIN `gusa-dwh.12571860.ExcludedCountries` C USING(country)
WHERE orders > 2 AND similar_revenue = 1
    AND C.country IS NULL
ORDER BY 1,3

/**/