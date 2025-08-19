DROP TABLE IF EXISTS format_N{niveau}_{period}_{indic}_page3 ;
WITH kpi AS (
SELECT N{niveau}_C_ENTITE
      ,satisfaction_contact
	  ,CAST(CASE WHEN {freq} IS NULL OR {freq} = 0 THEN 'NI'
	             WHEN {freq} < {threshold_NI_NS}   THEN 'NS'
	                                               ELSE
			     CASE WHEN satisfaction_contact = 'VAR_1' THEN CAST(ROUND(CAST({indicateur} as numeric), 1) AS VARCHAR(4))
	                                                      ELSE CAST(TO_CHAR(100.0*{indicateur}, 'fm99%') AS VARCHAR(4))
	              END
	         END AS VARCHAR(4)) AS kpi
      {fmt_evol}
FROM calculs_N{niveau}_{period}_page3
),
kpi_b as (
SELECT N{niveau}_C_ENTITE
      ,satisfaction_contact
	  {kpi}
from kpi
)
SELECT N{niveau}_C_ENTITE  AS entite
       ,{niveau} AS niveau
	   {flag_periode}
	   ,MAX(CASE WHEN satisfaction_contact= 'VAR_1' THEN kpi END) AS VAR_1
	   ,MAX(CASE WHEN satisfaction_contact= 'VAR_2' THEN kpi END) AS VAR_2
	   ,MAX(CASE WHEN satisfaction_contact= 'VAR_3' THEN kpi END) AS VAR_3
	   ,MAX(CASE WHEN satisfaction_contact= 'VAR_4' THEN kpi END) AS VAR_4
INTO format_N{niveau}_{period}_{indic}_page3
FROM kpi_b
GROUP BY N{niveau}_C_ENTITE ;