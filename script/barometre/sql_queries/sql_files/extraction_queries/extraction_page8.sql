DROP TABLE IF EXISTS extraction_N{niveau}_{period}_page8 ;
SELECT N{niveau}_C_ENTITE
	  ,CASE 
	        {flag_mc}
			{flag_mp}
			{flag_ap}
		END AS indic
	  /* chapter 3.1 Agency */
	  , item_02600, item_02700, item_02800, item_02900, item_03000
	  /* chapter 3.2 Voice server */
	  , item_03200, item_03300, item_03400
	  /* chapter 3.3 Outgoing call including call back */
	  , item_03700, inter_item_03700, item_03800, item_03900, item_03910
	  /* chapter 3.4 Reason for the inconvenience */
	  , item_03930, item_03940, item_03950, item_03960, item_03970
INTO extraction_N{niveau}_{period}_page8
FROM public.satisfaction 
WHERE {where_mc}
	  {where_mp}
	  {where_ap}
;