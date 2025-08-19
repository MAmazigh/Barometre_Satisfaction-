DROP TABLE IF EXISTS extraction_N{niveau}_{period}_page9_mcv ;
SELECT N{niveau}_C_ENTITE
      , regexp_split_to_table(mode_contact_valide, ';') as mode_contact_valide
	  ,CASE 
	        {flag_mc}
			{flag_mp}
			{flag_ap}
		END AS indic
	  /* chapter IV Spécificités commerciales */
	  , item_04200, item_04300, item_04400, item_04500, item_04600, item_04700, item_04800
INTO extraction_N{niveau}_{period}_page9_mcv
FROM public.satisfaction 
WHERE {where_mc}
	  {where_mp}
	  {where_ap}
;