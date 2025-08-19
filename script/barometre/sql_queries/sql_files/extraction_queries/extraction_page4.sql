DROP TABLE IF EXISTS extraction_N{niveau}_{period}_page4 ;
SELECT N{niveau}_C_ENTITE
	  ,CASE 
	        {flag_mc}
			{flag_mp}
			{flag_ap}
		END AS indic
	   ,CASE
			WHEN Q91 = '1' THEN 'VAR_1'  /* Q91_Au_dela */
			WHEN Q91 = '2' THEN 'VAR_2'  /* Q91_Conforme */
			WHEN Q91 = '3' THEN 'VAR_3'  /* Q91_Dessous */
			               ELSE NULL
		END AS Q91
	  ,CASE
			WHEN Q90 = '1' THEN 'VAR_4' /* Q90_Renforcée */
			WHEN Q90 = '2' THEN 'VAR_5' /* Q90_Stable */
			WHEN Q90 = '3' THEN 'VAR_6' /* Q90_Déteriorée */
			               ELSE NULL
		END AS Q90
INTO extraction_N{niveau}_{period}_page4
FROM public.satisfaction 
WHERE {where_mc}
	  {where_mp}
	  {where_ap}
;