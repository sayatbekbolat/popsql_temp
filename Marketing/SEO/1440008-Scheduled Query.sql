DECLARE days INT64 DEFAULT 3;
DECLARE startDate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL days DAY);
DECLARE endDate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

------------------------Top Keywords Update------------------
DELETE FROM `gusa-funnel-dwh.SEO_Data.SEO_Top_Industry_Keywords_tbl` WHERE TRUE;
INSERT INTO `gusa-funnel-dwh.SEO_Data.SEO_Top_Industry_Keywords_tbl` (Keyword)
(
    SELECT Keyword FROM `gusa-funnel-dwh.SEO_Data.SEO_Top_Industry_Keywords_GS`
    WHERE Keyword IS NOT NULL
);

---------------------Query Data-------------------------------
DELETE FROM `gusa-funnel-dwh.SEO_Data.GSC_Query_Data` WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL days DAY);
INSERT INTO `gusa-funnel-dwh.SEO_Data.GSC_Query_Data` (Date, Device, Query, Category, Impressions, Clicks, Total_Position)
(
SELECT Date,
    Device__Google_Search_Console AS Device,
    Query__Google_Search_Console AS Query,
    `gusa-funnel-dwh.SEO_Data.SEO_Keywords_Classification_Func` (Query__Google_Search_Console,Keyword) AS Category,
    Impressions__Google_Search_Console AS Impressions,
    Clicks__Google_Search_Console AS Clicks,
    Total_Position__Google_Search_Console AS Total_Position,
FROM `gusa-funnel-dwh.funnel_export_search_console.funnel_export_search_console_*` 
LEFT JOIN `gusa-funnel-dwh.SEO_Data.SEO_Top_Industry_Keywords_tbl` ON Query__Google_Search_Console = Keyword
WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') >= FORMAT_DATE('%Y%m',startDate)
    AND Date BETWEEN startDate AND endDate
    AND Country__Google_Search_Console = 'usa' 
    AND (Impressions__Google_Search_Console >= 10 OR Clicks__Google_Search_Console >0)
ORDER BY Impressions__Google_Search_Console DESC
);


-------------------Query Page Data-------------------
DELETE FROM `gusa-funnel-dwh.SEO_Data.GSC_Query_Page_Data` WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL days DAY);
INSERT INTO `gusa-funnel-dwh.SEO_Data.GSC_Query_Page_Data` (Date, Device, Query, Category, Page, Impressions, Clicks, Total_Position)
(
SELECT Date,
    Device__Google_Search_Console AS Device,
    Query__Google_Search_Console AS Query,
    `gusa-funnel-dwh.SEO_Data.SEO_Keywords_Classification_Func` (Query__Google_Search_Console,Keyword) AS Category,
    Page__Google_Search_Console AS Page,
    Impressions__Google_Search_Console AS Impressions,
    Clicks__Google_Search_Console AS Clicks,
    Total_Position__Google_Search_Console AS Total_Position,
FROM `gusa-funnel-dwh.funnel_export_search_console_page.funnel_export_search_console_page_*` 
LEFT JOIN `gusa-funnel-dwh.SEO_Data.SEO_Top_Industry_Keywords_tbl` ON Query__Google_Search_Console = Keyword
WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') >= FORMAT_DATE('%Y%m',startDate)
    AND Date BETWEEN startDate AND endDate
    AND Country__Google_Search_Console = 'usa' 
    AND (Impressions__Google_Search_Console >= 5 OR Clicks__Google_Search_Console >0)
ORDER BY Impressions__Google_Search_Console DESC
);









/**/