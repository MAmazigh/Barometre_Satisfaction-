DROP TABLE IF EXISTS format_N{niveau}_{period}_{indic}_page5 ;
WITH kpi AS (
SELECT N{niveau}_C_ENTITE
      ,mode_contact_valide
	  ,CAST(CASE WHEN {freq} IS NULL OR {freq} = 0 THEN 'NI'
	             WHEN {freq} < {threshold_NI_NS}   THEN 'NS'
	                                               ELSE CAST(ROUND(CAST({indicateur} AS numeric), 1) AS VARCHAR(4))
	         END AS VARCHAR(4)) AS kpi
      {fmt_evol}
FROM calculs_N{niveau}_{period}_page5
),
kpi_b as (
SELECT N{niveau}_C_ENTITE
      ,mode_contact_valide
	  {kpi}
from kpi
)
SELECT N{niveau}_C_ENTITE  AS entite
       ,{niveau} AS niveau
	   {flag_periode}
	   ,MAX(CASE WHEN mode_contact_valide= 'VAR_0' THEN kpi END) AS VAR_1
	   ,MAX(CASE WHEN mode_contact_valide= 'VAR_1' THEN kpi END) AS VAR_2
	   ,MAX(CASE WHEN mode_contact_valide= 'VAR_2' THEN kpi END) AS VAR_3
	   ,MAX(CASE WHEN mode_contact_valide= 'VAR_3' THEN kpi END) AS VAR_4
	   ,MAX(CASE WHEN mode_contact_valide= 'VAR_4' THEN kpi END) AS VAR_5
	   ,MAX(CASE WHEN mode_contact_valide= 'VAR_5' THEN kpi END) AS VAR_6
	   ,MAX(CASE WHEN mode_contact_valide= 'VAR_6' THEN kpi END) AS VAR_7
	   ,MAX(CASE WHEN mode_contact_valide= 'VAR_7' THEN kpi END) AS VAR_8
	   ,MAX(CASE WHEN mode_contact_valide= 'VAR_8' THEN kpi END) AS VAR_9
INTO format_N{niveau}_{period}_{indic}_page5
FROM kpi_b
GROUP BY N{niveau}_C_ENTITE ;