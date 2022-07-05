DELETE FROM `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Budget_Loc_2022`  WHERE Platform IS NOT NULL;
INSERT INTO `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Budget_Loc_2022` (Channel_Name, Platform, Type, Channel, Total_Budget, Planned_Month, Planned_Year, Status, Date, Timeline, Notes, Acct__Mgr_, Fee, Contract_Status, Affid, IR_Name)
(
SELECT Channel_Name, Platform, Type, Channel, Total_Budget, Planned_Month, Planned_Year, Status, PARSE_DATE('%Y%B%d',CONCAT(Planned_Year,Planned_Month,'01')) AS Date, Timeline, Notes, Acct__Mgr_, Fee, Contract_Status, Affid, IR_Name 
FROM `gusa-funnel-dwh.Direct_Marketing.Direct_Marketing_Budget_2022` 
)