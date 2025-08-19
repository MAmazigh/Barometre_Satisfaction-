DROP TABLE IF EXISTS format_N{niveau}_{period}_{indic}_page4 ;
WITH kpi AS (
SELECT N{niveau}_C_ENTITE
      ,prestation_impact
	  ,CAST(CASE WHEN {freq} IS NULL OR {freq} = 0 THEN 'NI'
	             WHEN {freq} < {threshold_NI_NS}   THEN 'NS'
	                                               ELSE CAST(TO_CHAR(100.0*{indicateur}, 'fm99%') AS VARCHAR(4))
	         END AS VARCHAR(4)) AS kpi
      {fmt_evol}
FROM calculs_N{niveau}_{period}_page4
),
kpi_b as (
SELECT N{niveau}_C_ENTITE
      ,prestation_impact
	  {kpi}
from kpi
)
SELECT N{niveau}_C_ENTITE  AS entite
       ,{niveau} AS niveau
	   {flag_periode}
	   ,MAX(CASE WHEN prestation_impact= 'VAR_1' THEN kpi END) AS VAR_1
	   ,MAX(CASE WHEN prestation_impact= 'VAR_2' THEN kpi END) AS VAR_2
	   ,MAX(CASE WHEN prestation_impact= 'VAR_3' THEN kpi END) AS VAR_3
	   ,MAX(CASE WHEN prestation_impact= 'VAR_4' THEN kpi END) AS VAR_4
	   ,MAX(CASE WHEN prestation_impact= 'VAR_5' THEN kpi END) AS VAR_5
	   ,MAX(CASE WHEN prestation_impact= 'VAR_6' THEN kpi END) AS VAR_6
INTO format_N{niveau}_{period}_{indic}_page4
FROM kpi_b
GROUP BY N{niveau}_C_ENTITE ;