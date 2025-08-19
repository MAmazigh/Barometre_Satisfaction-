DROP TABLE IF EXISTS {table_output} ;
SELECT col_name, N{niveau}_C_ENTITE
      ,CASE WHEN evol_indicateur < -1.96                THEN '-'
            WHEN evol_indicateur BETWEEN -1.96 AND 1.96 THEN '='
	        WHEN evol_indicateur > 1.96                 THEN '+'
        END as valeurs_e
/*
	  ,CAST(CASE WHEN freq_mc IN (NULL, 0)          THEN 'NI'
	             WHEN freq_mc < {threshold_NI_NS}   THEN 'NS'
	             ELSE CAST(ROUND(CAST(indicateur_mc AS numeric), 1) AS VARCHAR(4))
	         END AS VARCHAR(4)) AS kpi */
      ,CAST(CASE WHEN freq_mc IN (NULL, 0)        THEN 'NI'
	             WHEN freq_mc < {threshold_NI_NS} THEN 'NS'
	             ELSE
	                 CASE WHEN col_name in ({list_item_pct})
	                      THEN CAST(TO_CHAR(100.0*indicateur_mc, 'fm99%') AS VARCHAR(4))
	                      ELSE CAST(ROUND(CAST(indicateur_mc as numeric), 1) AS VARCHAR(4))
	                  END
	         END AS VARCHAR(4)) AS kpi
INTO {table_output}
FROM {table_input} ;
DROP TABLE {table_input} ;