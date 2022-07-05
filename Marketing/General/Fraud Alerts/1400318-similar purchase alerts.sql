WITH GA AS (
    SELECT DISTINCT
        user_pseudo_id,
        up.value.string_value AS visitor_id,
        geo.country,
    FROM `gusa-dwh.analytics_257412397.events_*` GA
    LEFT JOIN UNNEST(user_properties) AS up
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') >= FORMAT_DATE("%Y%m%d", DATETIME_SUB(CURRENT_DATETIME("America/Los_Angeles"), INTERVAL 1 HOUR))
        AND DATETIME(TIMESTAMP_MICROS(event_timestamp), "UTC") >= DATETIME_SUB(CURRENT_DATETIME("UTC"), INTERVAL 30 DAY)
        AND up.key = 'backendVisitorID'
    ORDER BY 1
  )

SELECT DISTINCT
    visitor_id,
    DATETIME(created_at,"America/Los_Angeles") AS PST_date,
    increment_id AS oid,
    user_pseudo_id,
    country,
FROM (
    SELECT DISTINCT
        visitor_id, 
        increment_id,
        created_at,
        user_pseudo_id,
        country,
        COUNT(DISTINCT revenue) OVER (PARTITION BY visitor_id) AS revenue,
        COUNT(DISTINCT increment_id) OVER (PARTITION BY visitor_id) AS orders,
        DATETIME_DIFF(FIRST_VALUE(created_at) OVER (PARTITION BY visitor_id ORDER BY created_at DESC),FIRST_VALUE(created_at) OVER (PARTITION BY visitor_id ORDER BY created_at),MINUTE) AS first_to_last,
    FROM `gusa-dwh.backend_realtime.order`
    LEFT JOIN GA USING (visitor_id)
    WHERE DATE(created_at) >= DATETIME_SUB((SELECT MAX(DATETIME(created_at)) FROM `gusa-dwh.backend_realtime.order`), INTERVAL 30 MINUTE)
        AND LENGTH(visitor_id) > 5
    ORDER BY 5 Desc,1
    )
LEFT JOIN `gusa-dwh.12571860.ExcludedCountries` C USING(country)
WHERE orders > 2 AND revenue = 1
    AND C.country IS NULL
ORDER BY 1,3