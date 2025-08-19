DROP TABLE IF EXISTS format_N{niveau}_{period}_{indic}_page2 ;
WITH kpi AS (
SELECT N{niveau}_C_ENTITE
      ,Q1
	  ,TO_CHAR({freq}, '9 999 999') AS volumes
	  ,CAST(CASE WHEN {freq} IS NULL OR {freq} = 0 THEN 'NI'
	             WHEN {freq} < {threshold_NI_NS}   THEN 'NS'
	                                               ELSE
			     CASE WHEN Q1 = 'VAR_2' THEN CAST(ROUND(100.0*{indicateur}) AS VARCHAR(4))
	                                    ELSE CAST(TO_CHAR(100.0*{indicateur}, 'fm99%') AS VARCHAR(4))
	              END
	         END AS VARCHAR(4)) AS kpi
      {fmt_evol}
FROM calculs_N{niveau}_{period}_page2
),
kpi_b as (
SELECT N{niveau}_C_ENTITE
      ,Q1
	  ,volumes
	  {kpi}
from kpi
)
SELECT N{niveau}_C_ENTITE  AS entite
       ,{niveau} AS niveau
	   {flag_periode}
	   ,MAX(volumes)                            AS VAR_1
	   ,MAX(CASE WHEN Q1= 'VAR_2' THEN kpi END) AS VAR_2
	   ,MAX(CASE WHEN Q1= 'VAR_3' THEN kpi END) AS VAR_3
	   ,MAX(CASE WHEN Q1= 'VAR_4' THEN kpi END) AS VAR_4
	   ,MAX(CASE WHEN Q1= 'VAR_5' THEN kpi END) AS VAR_5
INTO format_N{niveau}_{period}_{indic}_page2
FROM kpi_b
GROUP BY N{niveau}_C_ENTITE ;