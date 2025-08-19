DROP TABLE IF EXISTS extraction_N{niveau}_{period}_page6_mcv ;
SELECT N{niveau}_C_ENTITE
      , regexp_split_to_table(mode_contact_valide, ';') as mode_contact_valide
	  ,CASE 
	        {flag_mc}
			{flag_mp}
			{flag_ap}
		END AS indic
	  /* chapter 1.1 Le relationnel */
	  , item_00300, item_00400, item_00500, item_00600, item_00700
	  /* chapter 1.2 La réponse */
	  , item_00900, item_01000, item_01100
	  /* chapter 1.3 Traitement de la demande */
	  , item_01300, inter_item_01300, item_01400, inter_item_01400
	  , item_01500
INTO extraction_N{niveau}_{period}_page6_mcv
FROM public.satisfaction 
WHERE {where_mc}
	  {where_mp}
	  {where_ap}
;