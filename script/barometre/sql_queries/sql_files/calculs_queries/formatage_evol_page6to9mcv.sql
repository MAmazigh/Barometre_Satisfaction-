DROP TABLE IF EXISTS {table_output_mcv} ;
/* formattage page 6 à 9 temps 1 */
WITH CTE AS (
SELECT col_name, N{niveau}_C_ENTITE
      ,mode_contact_valide
,CASE WHEN evol_indicateur < -1.96 THEN '-'
      WHEN evol_indicateur BETWEEN -1.96 AND 1.96 THEN '='
	  WHEN evol_indicateur > 1.96 THEN '+'
  END as valeurs_e
,CAST(CASE WHEN freq_mc IN (NULL, 0)        THEN 'NI'
	       WHEN freq_mc < {threshold_NI_NS} THEN 'NS'
	       ELSE
	           CASE WHEN col_name in ({list_item_pct})
	                THEN CAST(TO_CHAR(100.0*indicateur_mc, 'fm99%') AS VARCHAR(4))
	                ELSE CAST(ROUND(CAST(indicateur_mc as numeric), 1) AS VARCHAR(4))
	            END
	         END AS VARCHAR(4)) AS kpi
FROM {table_input_mcv}
),
/* formattage page 6 à 9 temps 2 : completion des mode_contact_valide et les mettre en colonne */
list_entities AS (
SELECT DISTINCT N{niveau}_C_ENTITE
           FROM cte
       ORDER BY N{niveau}_c_entite
),
list_entities_key_id as (
SELECT N{niveau}_C_ENTITE
      ,ROW_NUMBER() OVER (ORDER BY N{niveau}_C_ENTITE) AS id_entity
	  ,0 AS key
FROM list_entities
),
list_items as (
SELECT DISTINCT col_name, 0 as key
           FROM cte
),
list_mcv as (
select cast(generate_series(1, 8) as text) as mode_contact_valide, 0 as key
),
skull as (
SELECT e.N{niveau}_C_ENTITE
	  ,e.id_entity
	  ,i.col_name
	  ,m.mode_contact_valide
FROM
       list_entities_key_id as e
FULL JOIN
       list_items as i
ON
       e.key = i.key
FULL JOIN
       list_mcv as m
ON
       m.key = e.key
ORDER BY 1, 2, 3, 4
),
completion as (
select s.N{niveau}_C_ENTITE, s.id_entity, s.col_name
      , s.mode_contact_valide
	  ,CASE WHEN c.kpi IN ('NI', 'NS') THEN c.kpi
	        WHEN c.kpi IS NULL         THEN 'NI'
	                                   ELSE c.kpi||'|'||c.valeurs_e
	    END AS kpi
  FROM skull s
LEFT JOIN cte AS c
	   ON s.N{niveau}_C_ENTITE = c.N{niveau}_C_ENTITE
      AND s.col_name = c.col_name
      AND s.mode_contact_valide = c.mode_contact_valide
)
SELECT  s.N{niveau}_C_ENTITE, s.id_entity, s.col_name
	   ,MAX(CASE WHEN s.mode_contact_valide= '1' THEN s.kpi END) AS VAR_5
	   ,MAX(CASE WHEN s.mode_contact_valide= '2' THEN s.kpi END) AS VAR_6
	   ,MAX(CASE WHEN s.mode_contact_valide= '3' THEN s.kpi END) AS VAR_7
	   ,MAX(CASE WHEN s.mode_contact_valide= '4' THEN s.kpi END) AS VAR_8
	   ,MAX(CASE WHEN s.mode_contact_valide= '5' THEN s.kpi END) AS VAR_9
	   ,MAX(CASE WHEN s.mode_contact_valide= '6' THEN s.kpi END) AS VAR_10
	   ,MAX(CASE WHEN s.mode_contact_valide= '7' THEN s.kpi END) AS VAR_11
	   ,MAX(CASE WHEN s.mode_contact_valide= '8' THEN s.kpi END) AS VAR_12
INTO {table_output_mcv}
FROM completion AS s
GROUP BY s.N{niveau}_C_ENTITE, s.id_entity, s.col_name
ORDER BY s.N{niveau}_C_ENTITE, s.id_entity, s.col_name ;
DROP TABLE IF EXISTS {final_table} ;
SELECT m.id_entity
     , g.N{niveau}_C_ENTITE
	 , g.col_name
	 , CASE WHEN g.kpi in ('NS', 'NI') THEN NULL
	                                   ELSE g.valeurs_e
	    END AS valeurs_e
	 ,g.kpi
	 ,m.var_5,m.var_6,m.var_7,m.var_8,m.var_9,m.var_10,m.var_11,m.var_12
INTO {final_table}
FROM {table_output} as g
JOIN {table_output_mcv} as m
  ON g.col_name=m.col_name
 AND g.N{niveau}_C_ENTITE=m.N{niveau}_C_ENTITE
ORDER BY 1, 2 ;
DROP TABLE {table_input_mcv}, {table_output}, {table_output_mcv} ;