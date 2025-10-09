select COUNT(DISTINCT customerid) 
from soil_testing.policies p 
where year(submittedDate)='2025' and month(submittedDate)='09' and companyRegionId=1