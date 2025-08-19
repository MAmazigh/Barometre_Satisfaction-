DROP TABLE IF EXISTS extraction_N{niveau}_{period}_page5 ;
SELECT N{niveau}_C_ENTITE
	  ,CASE 
	        {flag_mc}
			{flag_mp}
			{flag_ap}
		END AS indic
	  , satis_glob_cont
      /* variable mode_contact_valide takes 8 categories */
	  , regexp_split_to_table(mode_contact_valide, ';') AS mode_contact_valide
INTO extraction_N{niveau}_{period}_page5
FROM public.satisfaction 
WHERE {where_mc}
	  {where_mp}
	  {where_ap}
;