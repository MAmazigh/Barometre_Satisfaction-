DROP TABLE IF EXISTS extraction_N{niveau}_{period}_page3 ;
SELECT N{niveau}_C_ENTITE
	  ,CASE 
	        {flag_mc}
			{flag_mp}
			{flag_ap}
		END AS indic
	   ,satis_glob_cont
	  ,CASE
			WHEN satis_glob_cont IN (0, 1, 2, 3, 4, 5, 6) THEN 'VAR_4' /* Les insatisfaits */
			WHEN satis_glob_cont IN (7, 8)                THEN 'VAR_3' /* Les médians */
			WHEN satis_glob_cont IN (9, 10)               THEN 'VAR_2' /* Les plus satisfaits */
			                                              ELSE NULL
		END AS satisfaction_contact
INTO extraction_N{niveau}_{period}_page3
FROM public.satisfaction 
WHERE {where_mc}
	  {where_mp}
	  {where_ap}
;