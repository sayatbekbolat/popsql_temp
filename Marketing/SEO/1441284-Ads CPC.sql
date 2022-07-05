DECLARE days INT64 DEFAULT 7;
DECLARE startDate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL days DAY);
DECLARE endDate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

WITH Ads AS (
    SELECT PARSE_DATE('%F',Date) AS Date, 
        Search_Term__AdWords AS Query__Google_Search_Console,
        Device__AdWords AS Device__Google_Search_Console,
        SUM(Cost__AdWords) AS Paid_Cost,
        SUM(Impressions__AdWords) AS Paid_Impressions,
        SUM(Clicks__AdWords) AS Paid_Clicks,
        SUM(Conversions__AdWords) AS Paid_Conversions,
        SUM(Total_Conv__Value__AdWords) AS Paid_Conversions_Value,
    FROM `gusa-funnel-dwh.Funnel_export_search_term.funnel_data_*`
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') >= FORMAT_DATE('%Y%m',startDate)
        AND PARSE_DATE('%F',Date) BETWEEN startDate AND endDate
        AND (Cost__AdWords > 0 OR Conversions__AdWords > 0)
        AND Search_Term__AdWords = 'glasses'
    GROUP BY 1,2,3
    )

SELECT Date,
    Device__Google_Search_Console AS Device,
    Query__Google_Search_Console AS Query,
    `gusa-funnel-dwh.SEO_Data.SEO_Keywords_Classification_Func` (Query__Google_Search_Console,Keyword) AS Category,
    Impressions__Google_Search_Console AS Impressions,
    Clicks__Google_Search_Console AS Clicks,
    Total_Position__Google_Search_Console AS Total_Position,
    Paid_Cost,
    Paid_Impressions,
    Paid_Clicks,
    Paid_Conversions,
    Paid_Conversions_Value,
FROM `gusa-funnel-dwh.funnel_export_search_console.funnel_export_search_console_*` 
LEFT JOIN `gusa-funnel-dwh.SEO_Data.SEO_Top_Industry_Keywords_tbl` ON Query__Google_Search_Console = Keyword
LEFT JOIN Ads USING(Date,Device__Google_Search_Console,Query__Google_Search_Console)
WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') >= FORMAT_DATE('%Y%m',startDate)
    AND Date BETWEEN startDate AND endDate
    AND Country__Google_Search_Console = 'usa' 
    AND (Impressions__Google_Search_Console >= 10 OR Clicks__Google_Search_Console >0)
ORDER BY Impressions__Google_Search_Console DESC



/**/