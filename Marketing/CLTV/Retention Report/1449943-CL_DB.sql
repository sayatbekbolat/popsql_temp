SELECT Monthid,
  Active + Inactive As Total,
   Active,
  LEAD(Active) OVER (ORDER BY Monthid DESC) AS Open_active,
  Active - LEAD(Active) OVER (ORDER BY Monthid DESC) Active_diff,
  Inactive,
  LEAD(inActive) OVER (ORDER BY Monthid DESC) AS Open_inactive,
  inActive - LEAD(inActive) OVER (ORDER BY Monthid DESC) inactive_diff,
  (Active - LEAD(Active) OVER (ORDER BY Monthid DESC)) + (inActive - LEAD(inActive) OVER (ORDER BY Monthid DESC)) Delta
FROM(
SELECT Monthid, SUM(IF(Last_Status_in_month="Active",1,0)) Active, SUM(IF(Last_Status_in_month="Active",0,1)) InActive,
FROM `gusa-bronto-dwh.NL_Data_2021.Active_Clients_CL` -- `gusa-bronto-dwh.NL_Data_2021.Active_Clients`
WHERE MonthID Between '2019-12-01' And DATE_TRUNC(CURRENT_DATE,MONTH)
GROUP BY 1)
ORDER BY 1,2