DROP TABLE IF EXISTS {table_results_level} ;
SELECT {distinct}
	  {entite}
      {short_label_entity}
	  {var_niveau}
	  {list_var}
	  {tri} /* top position for the current level */
	  {_info_}
	  {bloc} /* 0 max level 1 current level 2 lower level 3 current level preceding periods */
	  {tableau_part} /* A, B, C or D */
	  {insert_before}
INTO {table_results_level}
FROM {table_input_level}
{join}
{where}