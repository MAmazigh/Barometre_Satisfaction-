DROP TABLE IF EXISTS {table_results_level} ;
SELECT
	  {entite}
	  {list_var_fixe}
	  {list_var}
INTO {table_results_level}
FROM {table_input_level}
{join}