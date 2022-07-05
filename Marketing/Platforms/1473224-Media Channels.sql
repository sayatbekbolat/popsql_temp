--=============== // Upload New Media not in table // ==================--

INSERT INTO `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy` (source,medium,campaign,adwords_account)
 
 SELECT DISTINCT source,medium,campaign,adwords_account
 FROM(

        SELECT DISTINCT 
        lower(trafficSource.source) AS source,
        lower(trafficSource.medium) AS medium,
        lower(trafficSource.campaign) AS campaign,
        trafficSource.adwordsClickInfo.customerId AS adwords_account
        FROM
            `gusa-dwh.12571860.ga_sessions_*`
        WHERE
        REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') > FORMAT_DATE("%Y%m%d",DATE_ADD(CURRENT_DATE(), INTERVAL -4 DAY))
      )
LEFT JOIN 
        (SELECT DISTINCT 
                source AS sourceA, medium AS mediumA, campaign AS campaignA, adwords_account AS adwords_accountA
        FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy`)

ON LOWER(source) = sourceA AND LOWER(medium) = mediumA AND LOWER(campaign)  = campaignA AND IFNULL(adwords_account,0) = IFNULL(adwords_accountA,0)

WHERE sourceA IS NULL
;
-- DELETE FROM `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media` WHERE source IS NOT NULL
--;

--=============== // Create hierarchy into new table // ==================--
INSERT INTO `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media` ( source, medium, campaign, adwords_account, Media )
(
WITH Hierarchy AS (
    SELECT adwords_account, campaign, medium, source 
    FROM  `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy`
)
,Message_Cat AS (
    SELECT DISTINCT
        campaign,
        `gusa-bronto-dwh.Newsletter_data.Email_category`(Final_Message_name) AS News_Cat
    FROM Hierarchy
    LEFT JOIN `gusa-bronto-dwh.Newsletter_data.Newsletter_Final_Message_name_LOC` ON LOWER(campaign) = LOWER(message_name)
    WHERE source LIKE 'newsl%' OR source LIKE 'glassesu%' OR source = 'Dynamic Yield recommendations'
)
SELECT
  A.source,
  A.medium,
  A.campaign,
  A.adwords_account,
  CASE
  ----------------------------------  Google --------------------------------------------------
    WHEN ((A.source = 'google' AND A.adwords_account = 4609197729) OR A.source in ('youtube.com','yt-direct-leads')) AND LOWER(A.campaign) LIKE '%geox%' THEN 'Youtube Geox'
    WHEN ((A.source = 'google' AND A.adwords_account = 4609197729) OR A.source in ('youtube.com','yt-direct-leads')) AND A.campaign LIKE '%contact%' THEN 'Youtube Contacts'
    WHEN (A.source = 'google' AND A.adwords_account = 4609197729) OR A.source in ('youtube.com','yt-direct-leads')   THEN 'Youtube'
    WHEN A.source = 'google' AND A.campaign like ('%trueview%') THEN 'Youtube'
    WHEN A.adwords_account = 4737858003 AND A.campaign like ('%yt%') THEN 'Youtube'
    WHEN A.source = 'google' AND A.campaign like '%hto%' THEN 'Google Generic - HTO'
    WHEN A.source = 'google' AND A.campaign like '%discovery%' AND A.campaign like '%contact%' THEN 'Google Discovery - Contacts'
    WHEN A.source = 'google' AND A.campaign like '%discovery%' THEN 'Google Discovery'
    WHEN A.source = 'google' AND A.adwords_account = 7309461647 THEN 'Google GDN'
    WHEN A.source = 'google' AND A.adwords_account = 7971451044 THEN 'YouTube Unskipable'
    WHEN A.source = 'google' AND A.adwords_account = 4737858003 AND A.campaign like ('%rmkt%') AND A.campaign LIKE ('au%') THEN 'Google Retargeting AU/CA'
    WHEN A.source = 'google' AND A.adwords_account = 4737858003 AND A.campaign like ('%rmkt%') AND A.campaign LIKE ('ca%') THEN 'Google Retargeting AU/CA'
    WHEN A.source = 'google' AND A.adwords_account = 4737858003 AND A.campaign like ('%rmkt%') THEN 'Google Retargeting'
    WHEN A.source = 'google' AND A.adwords_account = 4737858003 THEN 'Google GDN'
    WHEN A.source = 'google' AND A.medium = 'organic' THEN 'Google Organic'
    WHEN A.source = 'google' AND A.campaign LIKE ('%conta%') AND A.campaign LIKE ('%p.max%') AND A.campaign LIKE ('%private lab%') THEN 'Google Private Label P.Max - Contacts'
    WHEN A.source = 'google' AND A.campaign LIKE ('%conta%') AND A.campaign LIKE ('%p.max%') AND A.campaign NOT LIKE ('%private lab%') THEN 'Google P.Max - Contacts'
    WHEN A.source = 'google' AND A.campaign LIKE ('%conta%') AND A.campaign LIKE ('%private lab%') THEN 'Google Private Label - Contacts'
    WHEN A.source = 'google' AND A.campaign LIKE ('%pla%') AND A.campaign LIKE ('au%') THEN 'Google PLA AU'
    WHEN A.source = 'google' AND A.campaign LIKE ('%pla%') AND A.campaign LIKE ('ca%') THEN 'Google PLA CA'
    WHEN A.source = 'google' AND A.campaign LIKE ('%pla%') AND A.campaign LIKE ('%conta%') THEN 'Google PLA Contacts'
    WHEN A.source = 'google' AND A.campaign LIKE ('%pla%')  THEN 'Google PLA'
    WHEN A.source = 'google' AND A.campaign LIKE ('%glassesusa%') AND A.campaign LIKE ('au%') THEN 'Google Brand AU'
    WHEN A.source = 'google' AND A.campaign LIKE ('%glassesusa%') AND A.campaign LIKE ('ca%') THEN 'Google Brand CA'
    WHEN A.source = 'google' AND A.campaign LIKE ('%glassesusa%') THEN 'Google Brand'
    WHEN A.source = 'google' AND A.campaign LIKE ('%Bid Brad Abs Top Imp Share%') THEN 'Google Brand'
    WHEN A.source = 'google' AND A.campaign LIKE ('au%') THEN 'Google Generic AU'
    WHEN A.source = 'google' AND A.campaign LIKE ('ca%') THEN 'Google Generic CA'
    WHEN A.source = 'google' AND A.campaign LIKE ('%contact%') AND A.campaign NOT LIKE ('%pla%') THEN 'Google Generic - Contacts'
    WHEN A.source = 'dfa' OR A.source = 'dbm' THEN 'Google - DV360'
    WHEN A.source = 'google' THEN 'Google Generic'
  
  ----------------------------------  Bing --------------------------------------------------
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.campaign LIKE ('%conta%') AND A.campaign LIKE ('%private lab%') THEN 'Bing Private Label - Contacts'
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND LOWER(A.campaign) like '%pla %' AND A.campaign like '%conta%' THEN 'Bing PLA - Contacts' --new
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND LOWER(A.campaign) like '%pla %' THEN 'Bing PLA'  --new
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.campaign like ('%contact%') THEN 'Bing Generic - Contacts'
    
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.campaign like '%audience-network%' AND A.campaign like '%hto%' THEN 'Bing Aud Net - HTO'
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.campaign like '%audience-network%' THEN 'Bing Aud Net'
    
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.campaign like ('%glassesusa%') AND A.campaign like ('au%') THEN 'Bing Brand AU'
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.campaign like ('%glassesusa%') AND A.campaign like ('ca%') THEN 'Bing Brand CA'
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.campaign like ('%glassesusa%') THEN 'Bing Brand'   
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.campaign like ('au%') THEN 'Bing Generic AU'
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.campaign like ('ca%') THEN 'Bing Generic CA'   
    
    WHEN (A.source = 'bing' OR A.source = 'yahoo') AND A.medium = 'organic' THEN 'Bing Organic'
    WHEN (A.source = 'bing' OR A.source = 'yahoo') THEN 'Bing Generic'
   
   ----------------------------------  Facebook --------------------------------------------------    
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%subs%')  THEN 'Facebook Subs'
    WHEN A.source LIKE 'facebook%' AND A.medium IN ('fb-cust') THEN 'Facebook Cust'
    WHEN A.source LIKE 'facebook%' AND A.medium IN ('fb-rtgt','rtrgt') THEN 'Facebook Retargeting'
    WHEN A.source LIKE 'facebook%' AND A.medium IN ('fb-subs') THEN 'Facebook Subs'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%rtgt%')  THEN 'Facebook Retargeting'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%rmkt%')  THEN 'Facebook Retention'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%hto%')  THEN 'Facebook NAQ - HTO'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%transition%') THEN 'Facebook Trans'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('ts%')  THEN 'Facebook TS'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('tube%')  THEN 'Facebook TS'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%conta%')  THEN 'Facebook Contacts'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%lux%') AND A.campaign Not like ('%notlux%')  THEN 'Facebook NAQ - Lux'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%oakley%') AND A.campaign Not like ('%notlux%') THEN 'Facebook NAQ - Lux'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%ray%') AND A.campaign Not like ('%notlux%') THEN 'Facebook NAQ - Lux'
    WHEN A.source LIKE 'facebook%' AND (A.campaign  LIKE ('%chase-%') OR A.campaign  LIKE ('%_chase_%'))  THEN 'Facebook Chase'
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('sm%')  THEN 'Facebook NAQ - Social Method' 
    WHEN A.source LIKE 'facebook%' AND A.campaign  LIKE ('%cust%')  THEN 'Facebook Customers'
    WHEN A.source LIKE 'facebook%' AND A.medium IN ('fb-organic') AND A.campaign LIKE '%fans%' THEN 'Facebook Fans'
    WHEN A.source LIKE 'facebook%' AND A.medium IN ('fb-organic') THEN 'Facebook Organic'
    WHEN A.source LIKE 'facebook%' AND A.medium NOT IN ('fb-sponsored' ,'fb-rtgt' ,'fb-sponsered','fb-cust','rtrgt','fb-subs','fb-spo','fb-sponsore','fb-rmkt') THEN 'Facebook Brand'
    
    WHEN LOWER(A.source) IN ('m.facebook.com' , 'l.facebook.com')  THEN 'Facebook Ref'
    WHEN LOWER(A.source) = 'l.instagram.com'   THEN 'Instagram Ref'
 
    WHEN A.source LIKE 'facebook%'  THEN 'Facebook NAQ'
    
    ----------------------------------  NewLetter --------------------------------------------------    
    WHEN (A.source LIKE 'newsl%' OR A.source LIKE 'glassesu%' OR A.source = 'Dynamic Yield recommendations') AND News_Cat IN ('Cart Abandon','Category Abandon','Product Abandon') THEN  'Newsletter Abn Trigger'
        
    WHEN (A.source LIKE 'newsl%' OR A.source LIKE 'glassesu%' OR A.source = 'Dynamic Yield recommendations') 
        AND News_Cat IN ('Retention','Bulk Churned','Churned','Cashback Reorder','Post Purchase') THEN  'Newsletter Glasses Retention'
    WHEN (A.source LIKE 'newsl%' OR A.source LIKE 'glassesu%' OR A.source = 'Dynamic Yield recommendations') 
        AND News_Cat IN ('Leads','Multi','Workflow','Bulk Non Specified','Re-Engagement','Welcome','Bulk Welcome','Other','Prescription Scanner') THEN  'Newsletter Glasses NAQ' 

    WHEN (A.source LIKE 'newsl%' OR A.source LIKE 'glassesu%' OR A.source = 'Dynamic Yield recommendations') 
        AND News_Cat IN ('Post Purchase Contact Lenses') THEN  'Newsletter Contacts Retention'
    WHEN (A.source LIKE 'newsl%' OR A.source LIKE 'glassesu%' OR A.source = 'Dynamic Yield recommendations') 
        AND News_Cat IN ('Contacts','Contacts Other') THEN  'Newsletter Contacts NAQ' 
    WHEN (A.source LIKE 'newsl%' OR A.source LIKE 'glassesu%' OR A.source = 'Dynamic Yield recommendations') THEN  'Newsletter Other'
    
    WHEN LOWER(A.source) = 'sms' AND LOWER(A.medium) = 'cart_abandonment' AND LOWER(A.campaign) LIKE '%contact%' THEN 'SMS Contacts Abn'
    WHEN LOWER(A.source) = 'sms' AND LOWER(A.medium) = 'cart_abandonment' THEN 'SMS Glasses Abn'
    WHEN LOWER(A.source) = 'sms' AND LOWER(A.medium) LIKE '%reorder%' AND LOWER(A.campaign) LIKE '%contact%' THEN 'SMS Contacts Retention'
    WHEN LOWER(A.source) = 'sms' AND LOWER(A.medium) LIKE '%reorder%' THEN 'SMS Glasses Retention'
    WHEN LOWER(A.source) = 'sms' AND (LOWER(A.medium) LIKE '%sms_keyword_banner%' OR LOWER(A.medium) LIKE '%sms_welcome%') AND LOWER(A.campaign) LIKE '%contact%' THEN 'SMS Contacts Welcome'
    WHEN LOWER(A.source) = 'sms' AND (LOWER(A.medium) LIKE '%sms_keyword_banner%' OR LOWER(A.medium) LIKE '%sms_welcome%') THEN 'SMS Glasses Welcome'
    WHEN LOWER(A.source) = 'sms' AND LOWER(A.medium) LIKE '%manual%' AND LOWER(A.campaign) LIKE '%contact%' THEN 'SMS Contacts Manual'
    WHEN LOWER(A.source) = 'sms' AND LOWER(A.medium) LIKE '%manual%' THEN 'SMS Glasses Manual'
    WHEN LOWER(A.source) = 'sms'  THEN 'SMS Other'
    
    WHEN LOWER(A.source) = 'cordial' THEN 'Cordial Newsletter'
    WHEN LOWER(A.source) = 'box_insert' THEN 'Box Insert'
    
    ----------------------------------  Partnerships --------------------------------------------------    
    WHEN A.source LIKE 'cj%' or A.source LIKE 'ir%' or A.source = 'groupon.com' OR LOWER(A.source) = 'addshoppers' OR LOWER(A.source) LIKE '%paypal%' THEN 'Partnerships' --new
    
    ----------------------------------  Criteo --------------------------------------------------    
    WHEN LOWER(A.source) LIKE 'cri%' AND LOWER(A.medium) LIKE '%naq%' THEN 'Criteo NAQ'
    WHEN A.source LIKE 'cri%' AND A.campaign LIKE '%mid%' THEN 'Criteo Mid'
    WHEN A.source LIKE 'cri%' AND A.medium LIKE '%contact%' THEN 'Criteo - Lower Funnel - Contacts'
    WHEN A.source LIKE 'cri%' THEN 'Criteo - Lower Funnel'
    WHEN A.source = 'ads.us.criteo.com' THEN 'Criteo - Lower Funnel'
    
    ----------------------------------  Gemini --------------------------------------------------    
    
    WHEN A.source LIKE 'gemini%' AND (A.medium = 'synd' OR A.medium = 'native') THEN 'Gemini - Native'
    WHEN A.source LIKE 'gemini%' AND (A.medium = 'rtgt') THEN 'Gemini - RTGT'
    WHEN A.source LIKE 'gemini%' AND (LOWER(A.medium) = 'naq') THEN 'Gemini NAQ'
    WHEN A.source LIKE 'yahoo' AND (LOWER(A.medium) = 'cpc') THEN 'Gemini NAQ'
    WHEN A.source LIKE 'yahoo' AND (LOWER(A.medium) = 'organic') THEN 'Gemini Organic'
    WHEN A.source LIKE 'gemini%' THEN 'Gemini Search'
    
    ----------------------------------  Influencers --------------------------------------------------    
    WHEN LOWER(A.source) = 'grin' OR LOWER(A.medium) = 'grin' THEN 'Grin'
    WHEN A.source = 'youtube' OR A.medium = 'youtube' OR A.source = 'ltb' OR A.source = 'blog' OR A.source = 'podcast' THEN 'Influencers'
    WHEN A.source LIKE 'instagram%' AND A.medium NOT LIKE ('social%') AND A.medium NOT LIKE ('gusa%') AND A.medium NOT LIKE ('%organic%') AND A.medium != ('(not set)') THEN 'Influencers'
    WHEN A.source = 'tiktok' AND A.medium NOT LIKE '%sponsored%' AND A.medium NOT LIKE '%referral%' AND A.medium NOT LIKE '%gusa-%' AND A.medium NOT LIKE '%rtgt%' THEN 'Influencers'
    

    ----------------------------------  Instagram --------------------------------------------------   
     WHEN A.source LIKE 'instagram%'  THEN 'Instagram'
     
     ----------------------------------  Tiktok --------------------------------------------------   
     WHEN LOWER(A.source) LIKE '%tiktok%'  THEN 'Tiktok'
     
     ----------------------------------  Liveintent ----------------------------------------------
     WHEN A.source LIKE 'livei%' AND LOWER(A.campaign) LIKE '%trans%' THEN 'Liveintent Transitions'
     WHEN A.source LIKE 'livei%'  THEN 'Liveintent'
     
     ----------------------------------  Native --------------------------------------------------   
     WHEN A.source LIKE 'mediaf%'  THEN 'Media Force'
     WHEN A.source LIKE 'poweri%'  THEN 'Power Inbox'
     WHEN A.source LIKE 'twitter%' AND A.medium  IN ('twitter-organic') THEN 'Twitter Organic'
     WHEN A.source LIKE 'twitter%'  THEN 'Twitter'
     WHEN A.source LIKE 'triplel%'  THEN 'Triplelift'
     WHEN A.source LIKE 'snap%'  THEN 'SnapChat'
     WHEN A.source LIKE 'out%' OR A.source = 'ob' THEN 'Outbrain'
     
     ----------------------------------  Taboola --------------------------------------------------   
     WHEN (A.source LIKE 'tbl%' OR A.source LIKE 'taboola%') AND LOWER(A.medium) LIKE '%rtgt%' THEN 'Taboola - RTGT'
     WHEN (A.source LIKE 'tbl%' OR A.source LIKE 'taboola%') AND A.campaign LIKE '%hto%' THEN 'Taboola - HTO'
     WHEN A.source LIKE 'tbl%' OR A.source LIKE 'taboola%' THEN 'Taboola'
     
     -------------------------------------Pinterest-----------------------------------
     WHEN A.source LIKE 'pint%' and (A.medium like ('%remarketing%')) THEN 'Pinterest RMKT'   
     WHEN A.source LIKE 'pint%' and (A.medium like ('%retargeting%')) THEN 'Pinterest RTGT'
     WHEN A.source LIKE 'pint%' and A.medium in ('pint_organic', 'posts','social','fb-organic','pinterest-organic')  THEN 'Pinterest Organic'
     WHEN A.source LIKE 'pint%' and (LOWER(A.medium) LIKE '%shop%' OR LOWER(A.medium) LIKE '%referral%')  THEN 'Pinterest Shop'
     WHEN A.source LIKE 'pint%' THEN 'Pinterest NAQ'
     
     ----------------------------------  Single Settings --------------------------------------------------   
     WHEN A.source LIKE '%tvsquared%' THEN 'TVsquared'
     WHEN A.source = '(direct)'   THEN 'Direct'  
     WHEN A.source LIKE 'social%'   THEN 'Brand'  
     WHEN A.source = '(not set)'   THEN 'Unknown'  
     WHEN A.source LIKE 'ref%' or A.source = 'undefined' THEN 'Unknown'  
     WHEN A.source LIKE 'muse%'  THEN 'MuseHD'
     WHEN A.source = 'organic'  THEN 'Organic'
     WHEN A.source = 'vizury'  THEN 'Vizury'
     WHEN A.source = 'shopmessage' THEN 'ShopMessage'
     WHEN A.source = 'amazon'  THEN 'Amazon'
     WHEN A.source = 'tv'  THEN 'TV'
     WHEN A.medium = 'organic'  THEN 'Organic'
     WHEN A.source LIKE ('life%') THEN 'Lifescript'
     ELSE 'Other'
  END AS Media
FROM  Hierarchy A
LEFT JOIN Message_Cat USING(campaign)
LEFT JOIN `gusa-funnel-dwh.GA_Media_Ref.GA_Media_Hierarchy_with_Media` B
    ON   A.source=B.source AND A.medium=B.medium AND IFNULL(A.campaign,'zzzz')=IFNULL(B.campaign,'zzzz') AND IFNULL(A.adwords_account,0)=IFNULL(B.adwords_account,0)

WHERE B.source IS NULL
)