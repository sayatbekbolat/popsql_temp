SELECT Hased_email As id, TRIM(subscriber_email) AS email, 
       CAST(REPLACE(REPLACE(REPLACE(RecProducts,'[','["'),']','"]'),',','","') AS STRING) As recs

FROM `gusa-dwh.Recommendations.EmailBaseRecommendations`
WHERE RecProducts != '[]'
-- AND UpdateDateTimeIST = (SELECT MAX(UpdateDateTimeIST) FROM `gusa-dwh.Recommendations.EmailBaseRecommendations`)