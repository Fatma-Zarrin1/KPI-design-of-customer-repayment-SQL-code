Use CREDIT_MFI
go


SELECT 
    Month_id,
  

    FORMAT(CE_AS_PER_AC / 100, 'P2') AS [CE_AS_PER_AC_%],
    FORMAT(CE_AS_PER_Amt / 100, 'P2') AS [CE_AS_PER_Amt_%]

from(
select Month_id,
Cast(nullif(COUNT(Case when Coll_Status in ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') then 1 else null end),0) as float)/
cast(nullif(COUNT(account_no),0) as float)*100 CE_AS_PER_AC,

Cast(nullif(SUM(Case when Coll_Status in ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') then Coll_Amt else null end),0) as float)/
cast(nullif(SUM(Demandamt),0) as float)*100 CE_AS_PER_Amt

from AllBank_Demand
Where Demand_Status = 'Schedule'
and Product_Specification = 'Post_Covid'
and Month_id >= '202508'
group by month_id
) A


-----------------------------***************---Bank collection% Under performers---*******************-----------------------------------------


WITH RawCounts AS (
    SELECT 
        Month_id, 
        Bank,
        -- Numerators: Count/Sum of collected items
        COUNT(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN 1 END) * 1.0 AS Coll_Ac,
        SUM(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt ELSE 0 END) * 1.0 AS Coll_Amt,
        -- Denominators: Totals
        COUNT(account_no) AS Total_Ac,
        SUM(Demandamt) AS Total_Demand
    FROM AllBank_Demand
    WHERE Demand_Status = 'Schedule' AND Product_Specification = 'Post_Covid' and Month_id >= '202507'
    GROUP BY Month_id, Bank
	
),
mid as(
SELECT 
    Month_id, 
    Bank,
    -- 'P2' automatically multiplies by 100 and adds '%'
    FORMAT(Coll_Ac / NULLIF(Total_Ac, 0), 'P2') AS [CE_AS_PER_AC_%],
    FORMAT(Coll_Amt / NULLIF(Total_Demand, 0), 'P2') AS [CE_AS_PER_Amt_%]
FROM RawCounts
)

select Bank, 
[202507],[202508],
[202509],[202510],[202511],
[202512]
from (select  Month_id, 
    Bank,[CE_AS_PER_Amt_%] from mid)
	 as sourcetable
Pivot(max([CE_AS_PER_Amt_%]) for month_id in ([202507],[202508],[202509],[202510],[202511],[202512])) as pivottable;
;



--------------***************************----MOM-----****************************************************--------------------------


With Ratio_Calc
As
(
SELECT 
Bank,
     SUM(CASE WHEN Month_id = '202511' then Demandamt else 0 end) AS Total_LM_Demand,
	 SUM(CASE WHEN Month_id = '202512' then Demandamt else 0 end) AS Total_CM_Demand,
	  SUM(CASE WHEN Month_id = '202511' and Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt ELSE 0 END) * 1.0 AS Coll_LM_Amt,
	  	  SUM(CASE WHEN Month_id = '202512' and Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt ELSE 0 END) * 1.0 AS Coll_CM_Amt
        --COUNT(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN 1 END) * 1.0/NULLIF(COUNT(account_no),0) AS CE_AC_Ratio,
        --SUM(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt ELSE 0 END) * 1.0/NULLIF(SUM(Demandamt),0) AS CE_Amt_Ratio

    FROM AllBank_Demand
    WHERE Demand_Status = 'Schedule' 
      AND Product_Specification = 'Post_Covid' 
      AND Month_id IN ('202511','202512')
	  GROUP BY Bank
	 
),

Percent_Calc
As
(
SELECT Bank,Total_CM_Demand,Coll_CM_Amt,Total_LM_Demand,Coll_LM_Amt,
--CE_AC_Ratio, 
--LAG(CE_AC_Ratio) OVER (PARTITION BY BANK ORDER BY Month_id) AS Prev_CE_AC_Ratio,

(Coll_CM_Amt / NULLIF(Total_CM_Demand, 0)) AS CE_Amt_Ratio,
(Coll_LM_Amt / NULLIF(Total_LM_Demand, 0)) AS Prev_CE_AMT_Ratio
from Ratio_Calc
)

SELECT 
 Bank,Total_CM_Demand,Coll_CM_Amt,
 FORMAT(CE_Amt_Ratio,'P2') [CURRENT_MONTH_CE_Amt_%],
 Total_LM_Demand,Coll_LM_Amt,
--FORMAT(CE_AC_Ratio,'P2') [CURRENT_MONTH_CE_AC_%],
--FORMAT(Prev_CE_AC_Ratio,'P2') [PRE_MONTH_CE_AC_%],
--FORMAT((CE_AC_Ratio - Prev_CE_AC_Ratio) / NULLIF(Prev_CE_AC_Ratio, 0), 'P2') AS [MoM_Growth_Ac%],

FORMAT(Prev_CE_AMT_Ratio,'P2') [PRE_MONTH_CE_Amt_%],
FORMAT((CE_Amt_Ratio - Prev_CE_AMT_Ratio) / NULLIF(Prev_CE_AMT_Ratio, 0), 'P2') AS [MoM_Growth_Amt%]

FROM Percent_Calc;
--WHERE Prev_CE_AMT_Ratio IS NOT NULL;


-----------------****************--------------YoY---------------*****************------------------


With Ratio_Calc
As
(
SELECT 
Bank,
     SUM(CASE WHEN Month_id = '202412' then Demandamt else 0 end) AS Total_LYCM_Demand,
	 SUM(CASE WHEN Month_id = '202512' then Demandamt else 0 end) AS Total_CYCM_Demand,
	  SUM(CASE WHEN Month_id = '202412' and Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt ELSE 0 END) * 1.0 AS Coll_LYCM_Amt,
	  	  SUM(CASE WHEN Month_id = '202512' and Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt ELSE 0 END) * 1.0 AS Coll_CYCM_Amt
        --COUNT(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN 1 END) * 1.0/NULLIF(COUNT(account_no),0) AS CE_AC_Ratio,
        --SUM(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt ELSE 0 END) * 1.0/NULLIF(SUM(Demandamt),0) AS CE_Amt_Ratio

    FROM AllBank_Demand
    WHERE Demand_Status = 'Schedule' 
      AND Product_Specification = 'Post_Covid' 
      AND Month_id IN ('202412','202512')
	  GROUP BY Bank 
),

Percent_Calc
As
(
SELECT Bank,Total_CYCM_Demand,Coll_CYCM_Amt,Total_LYCM_Demand,Coll_LYCM_Amt,
--CE_AC_Ratio, 
--LAG(CE_AC_Ratio) OVER (PARTITION BY BANK ORDER BY Month_id) AS Prev_CE_AC_Ratio,

(Coll_CYCM_Amt / NULLIF(Total_CYCM_Demand, 0)) AS CE_Amt_Ratio,
(Coll_LYCM_Amt / NULLIF(Total_LYCM_Demand, 0)) AS Prev_CE_AMT_Ratio
from Ratio_Calc
)

SELECT 
Bank,Total_CYCM_Demand,Coll_CYCM_Amt,
 FORMAT(CE_Amt_Ratio,'P2') [CURRENT_MONTH_CE_Amt_%],
 Total_LYCM_Demand,Coll_LYCM_Amt,
--FORMAT(CE_AC_Ratio,'P2') [CURRENT_MONTH_CE_AC_%],
--FORMAT(Prev_CE_AC_Ratio,'P2') [PRE_MONTH_CE_AC_%],
--FORMAT((CE_AC_Ratio - Prev_CE_AC_Ratio) / NULLIF(Prev_CE_AC_Ratio, 0), 'P2') AS [YoY_Growth_Ac%],

FORMAT(Prev_CE_AMT_Ratio,'P2') [PRE_YR_MONTH_CE_Amt_%],
FORMAT((CE_Amt_Ratio - Prev_CE_AMT_Ratio) / NULLIF(Prev_CE_AMT_Ratio, 0), 'P2') AS [YoY_Growth_Amt%]

FROM Percent_Calc
WHERE Bank NOT IN ('Annapurna','Piramal');

-----------************************------Contribution% and impact------**************************------------

With
CTE1 AS
( select Bank,
SUM(Demandamt) AS Partner_Demand,
SUM(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt END) AS Partner_Collected_Amt,
(SUM(Demandamt) -
SUM(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt END)) AS Partner_Shortfall
from allbank_Demand
where Month_id = '202512'
and Demand_Status = 'Schedule'
and Product_Specification = 'Post_Covid'
group by Bank),

CTE2 AS
(
select Bank, Partner_Demand,Partner_Shortfall,
Partner_Demand * 1.0/ SUM(Partner_Demand) OVER ()  AS Demand_Share,
Partner_Shortfall * 1.0 / SUM(Partner_Shortfall) OVER () As Shortfall_Share,
(Partner_Shortfall * 1.0 / SUM(Partner_Shortfall) OVER() -
Partner_Demand * 1.0 / SUM(Partner_Demand) OVER ()) AS Performance_Gap

from CTE1
--group by Bank,Partner_Demand,Partner_Shortfall
)

SELECT Bank,Partner_Demand,Partner_Shortfall,
FORMAT(Demand_Share,'P2') AS  [Demand_Share%],
FORMAT(Shortfall_Share,'P2') AS [Shortfall_Share%],
FORMAT(Performance_Gap,'P2') AS [Performance_Gap%],
RANK() OVER( ORDER BY Performance_Gap DESC) Performance_Drag_Rank

FROM CTE2;

------------------************************---CE% By Loan Ticket Size----*****************************------------------------


WITH DB_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Account_no order by Disb_Date DESC) AS rn
    FROM Customer_wise_Disb
),
Base_Data AS (

        SELECT d.Bank,d.account_no,d.Demandamt,d.Coll_Amt,d.Coll_Status,
        case when LEFT(CONVERT(VARCHAR,d.Disb_Date,112),6) = LEFT(CONVERT(VARCHAR,db.Disb_Date,112),6) then db.Dis_Amt else null end Dis_Amount
        FROM allbank_Demand d
        LEFT JOIN DB_dedup db
          ON d.account_no = db.Account_no
         AND db.rn = 1	
         where d.Month_id = '202512'
        and d.Demand_Status = 'Schedule'
        and d.Product_Specification = 'Post_Covid'
  ),
  Loan_Bucketed AS(
  SELECT *,
        CASE 
		    WHEN Dis_Amount < 30000 THEN '<30K'
			WHEN Dis_Amount BETWEEN 30000 AND 50000 THEN '30K - 50K'
			WHEN Dis_Amount BETWEEN 50000 AND 80000 THEN '50K - 80K'
			WHEN Dis_Amount BETWEEN 80000 AND 100000 THEN '80K - 1L'
			WHEN Dis_Amount > 100000 THEN '>1L'  ELSE 'DB MISSING' END AS LOAN_TICKET_SIZE
        
  FROM Base_Data )
  
  SELECT LOAN_TICKET_SIZE, SUM(Demandamt) Total_Demand,SUM(Coll_Amt) Collected_Amt,
                         Format(SUM(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt ELSE 0 END) * 1.0 / nullif(SUM(Demandamt),0),'P2') [CE_AS_PER_Amt_%]
  
  FROM Loan_Bucketed

  WHERE LOAN_TICKET_SIZE<>'DB MISSING'
  GROUP BY LOAN_TICKET_SIZE ;


------*****************----------Contribution to Shortfall by Loan Size--------*********************----------


WITH DB_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Account_no order by Disb_Date DESC) AS rn
    FROM Customer_wise_Disb
),
Base_Data AS (

        SELECT d.Bank,d.account_no,d.Demandamt,d.Coll_Amt,d.Coll_Status,
        case when LEFT(CONVERT(VARCHAR,d.Disb_Date,112),6) = LEFT(CONVERT(VARCHAR,db.Disb_Date,112),6) then db.Dis_Amt else null end Dis_Amount
        FROM allbank_Demand d
        LEFT JOIN DB_dedup db
          ON d.account_no = db.Account_no
         AND db.rn = 1	
         where d.Month_id = '202512'
        and d.Demand_Status = 'Schedule'
        and d.Product_Specification = 'Post_Covid'
  ),
  Loan_Bucketed AS(
  SELECT *,
        CASE 
		    WHEN Dis_Amount < 30000 THEN '<30K'
			WHEN Dis_Amount BETWEEN 30000 AND 50000 THEN '30K - 50K'
			WHEN Dis_Amount BETWEEN 50000 AND 80000 THEN '50K - 80K'
			WHEN Dis_Amount BETWEEN 80000 AND 100000 THEN '80K - 1L'
			WHEN Dis_Amount > 100000 THEN '>1L'  ELSE 'DB MISSING' END AS LOAN_TICKET_SIZE
        
  FROM Base_Data ),

  Loan_Agg AS
  (
  SELECT LOAN_TICKET_SIZE, SUM(Demandamt) Total_Demand,SUM(Coll_Amt) Collected_Amt,
                         (SUM(Demandamt) -
                          SUM(CASE WHEN Coll_Status IN ('A_Collected','C_Advance_Adjusted','E_DBR_Closed') THEN Coll_Amt END)) AS Shortfall
  
  FROM Loan_Bucketed

  WHERE LOAN_TICKET_SIZE<>'DB MISSING'
  GROUP BY LOAN_TICKET_SIZE)

  SELECT LOAN_TICKET_SIZE,Total_Demand,Collected_Amt,Shortfall,
FORMAT(Total_Demand * 1.0/ SUM(Total_Demand) OVER (),'P2') AS Demand_Share,
FORMAT(Shortfall * 1.0 / SUM(Shortfall) OVER (),'P2') As Shortfall_Share,
FORMAT((Shortfall * 1.0 / SUM(Shortfall) OVER() -
Total_Demand * 1.0 / SUM(Total_Demand) OVER ()),'P2') AS Performance_Gap
  
FROM Loan_Agg;



