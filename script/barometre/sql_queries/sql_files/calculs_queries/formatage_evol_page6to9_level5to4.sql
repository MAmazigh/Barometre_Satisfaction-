DROP TABLE IF EXISTS {table_output} ;
WITH CTE AS (
SELECT g.col_name
	  ,d.N{niveau}_C_ENTITE
      ,g.N{niveau_inf}_C_ENTITE
	  ,d.tri
      ,CASE WHEN g.kpi in ('NS', 'NI') THEN NULL
	                                   ELSE g.valeurs_e
	    END AS valeurs_e
      ,g.kpi
FROM {table_input_niveau_inf} AS g
JOIN tab_ref_N{niveau} AS d
ON g.N{niveau_inf}_C_ENTITE = d.N{niveau_inf}_C_ENTITE
),
cte_for_level AS (
SELECT col_name
	  ,N{niveau}_C_ENTITE
      ,N{niveau_inf}_C_ENTITE
	  ,tri
      ,CASE WHEN kpi in ('NS', 'NI') THEN kpi
	                                 ELSE kpi||'|'||valeurs_e
	    END AS kpi
FROM cte
),
tranpose_for_level as (
SELECT DISTINCT col_name, N{niveau}_C_ENTITE
	  {list_var_partition_by}
FROM cte_for_level
)
SELECT g.N{niveau}_C_ENTITE, g.col_name, g.valeurs_e, g.kpi
       {list_var_transposed}
  INTO {table_output}
  FROM {table_input} AS g
  JOIN tranpose_for_level as t
    ON g.N{niveau}_C_ENTITE = t.N{niveau}_C_ENTITE
   AND g.col_name = t.col_name
ORDER BY col_name ;