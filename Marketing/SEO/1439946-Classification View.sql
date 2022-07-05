CREATE OR REPLACE FUNCTION `gusa-funnel-dwh.SEO_Data.SEO_Keywords_Classification_Func`(query STRING, top STRING) RETURNS STRING AS (
CASE
        WHEN (query LIKE '%glassesusa%' OR query LIKE '%glasses usa%' OR query LIKE '%glasses.usa%' OR query LIKE '%glaasesusa%' OR query LIKE '%glaases usa%' OR query LIKE '%galssesusa%' OR query LIKE '%galsses usa%')
            AND query NOT LIKE '%eyeglasses usa%' AND query NOT LIKE '%sunglasses usa%' THEN 'Brand'
        WHEN top IS NOT NULL THEN 'Top Industry Keyword'
        ELSE 'Other'
    END
);