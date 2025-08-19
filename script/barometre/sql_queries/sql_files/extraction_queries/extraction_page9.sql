DROP TABLE IF EXISTS extraction_N{niveau}_{period}_page9 ;
SELECT N{niveau}_C_ENTITE
	  ,CASE 
	        {flag_mc}
			{flag_mp}
			{flag_ap}
		END AS indic
	  /* chapter IV Spécificités commerciales */
	  , item_04200, item_04300, item_04400, item_04500, item_04600, item_04700, item_04800
INTO extraction_N{niveau}_{period}_page9
FROM public.satisfaction 
WHERE {where_mc}
	  {where_mp}
	  {where_ap}
;