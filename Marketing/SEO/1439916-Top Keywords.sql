DELETE FROM `gusa-funnel-dwh.SEO_Data.SEO_Top_Industry_Keywords_tbl` WHERE TRUE;
INSERT INTO `gusa-funnel-dwh.SEO_Data.SEO_Top_Industry_Keywords_tbl` (Keyword)
(
    SELECT Keyword FROM `gusa-funnel-dwh.SEO_Data.SEO_Top_Industry_Keywords_GS`
    WHERE Keyword IS NOT NULL
)