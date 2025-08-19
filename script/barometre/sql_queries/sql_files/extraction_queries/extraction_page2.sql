DROP TABLE IF EXISTS extraction_N{niveau}_{period}_page2 ;
SELECT N{niveau}_C_ENTITE
	  ,CASE 
	        {flag_mc}
			{flag_mp}
			{flag_ap}
		END AS indic
	  ,CASE  
			WHEN CAST(Q1 AS INT) IN (0, 1, 2, 3, 4, 5, 6) THEN 'VAR_5' /* Detractor */
			WHEN CAST(Q1 AS INT) IN (7, 8)                THEN 'VAR_4' /* Passive */
			WHEN CAST(Q1 AS INT) IN (9, 10)               THEN 'VAR_3' /* Promotor */
			                                              ELSE NULL
		END AS Q1
INTO extraction_N{niveau}_{period}_page2
FROM public.satisfaction 
WHERE {where_mc}
	  {where_mp}
	  {where_ap}
;