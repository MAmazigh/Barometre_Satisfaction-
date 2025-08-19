DROP TABLE IF EXISTS extraction_N{niveau}_{period}_page7_mcv ;
SELECT N{niveau}_C_ENTITE
      , regexp_split_to_table(mode_contact_valide, ';') as mode_contact_valide
	  ,CASE 
	        {flag_mc}
			{flag_mp}
			{flag_ap}
		END AS indic
	  /* chapter 2 La prise en charge */	 
	  ,item_01700, item_01800, item_01900, item_02000, item_02100, item_02200, item_02300 
INTO extraction_N{niveau}_{period}_page7_mcv
FROM public.satisfaction 
WHERE {where_mc}
	  {where_mp}
	  {where_ap}
;