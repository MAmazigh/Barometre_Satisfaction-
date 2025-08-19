DROP TABLE IF EXISTS calculsa_N{niveau}_{period}_page9{suffixe} ;
WITH cte as (
SELECT indic, N{niveau}_C_ENTITE
       {mcv}
	    , COUNT(item_04200)  AS N_item_04200
		, AVG(item_04200)    AS AVG_item_04200
		, STDDEV(item_04200) AS STD_item_04200
		, COUNT(item_04300)  AS N_item_04300
		, AVG(item_04300)    AS AVG_item_04300
		, STDDEV(item_04300) AS STD_item_04300
		, COUNT(item_04400)  AS N_item_04400
		, AVG(item_04400)    AS AVG_item_04400
		, STDDEV(item_04400) AS STD_item_04400
		, COUNT(item_04500)  AS N_item_04500
		, AVG(item_04500)    AS AVG_item_04500
		, STDDEV(item_04500) AS STD_item_04500
		, COUNT(item_04600)  AS N_item_04600
		, AVG(item_04600)    AS AVG_item_04600
		, STDDEV(item_04600) AS STD_item_04600
		, COUNT(item_04700)  AS N_item_04700
		, AVG(item_04700)    AS AVG_item_04700
		, STDDEV(item_04700) AS STD_item_04700
		, COUNT(item_04800)  AS N_item_04800
		, AVG(item_04800)    AS AVG_item_04800
		, STDDEV(item_04800) AS STD_item_04800
FROM {table_input}
GROUP BY indic, N{niveau}_C_ENTITE {mcv}
),
/* transposition de la table pour faciliter les calculs d'évolution */
transpose as (
SELECT c.N{niveau}_c_entite, c.indic {mcv} ,cjl.*
FROM cte as c
CROSS JOIN LATERAL (
VALUES
(c.n_item_04200, c.std_item_04200, c.avg_item_04200, 'item_04200'),
(c.n_item_04300, c.std_item_04300, c.avg_item_04300, 'item_04300'),
(c.n_item_04400, c.std_item_04400, c.avg_item_04400, 'item_04400'),
(c.n_item_04500, c.std_item_04500, c.avg_item_04500, 'item_04500'),
(c.n_item_04600, c.std_item_04600, c.avg_item_04600, 'item_04600'),
(c.n_item_04700, c.std_item_04700, c.avg_item_04700, 'item_04700'),
(c.n_item_04800, c.std_item_04800, c.avg_item_04800, 'item_04800')
) as cjl (Freq, Std, indicateur, col_name)
ORDER BY N{niveau}_c_entite {mcv}, col_name, indic
),
indicateurs_periodes as (
SELECT indic, N{niveau}_C_ENTITE, col_name
	   {mcv}
	  , SUM(CASE WHEN indic= 'MC' THEN Freq END) OVER (PARTITION BY N{niveau}_C_ENTITE {mcv}, col_name
													       ORDER BY N{niveau}_C_ENTITE {mcv}) AS freq_MC
      , SUM(CASE WHEN indic= 'MC' THEN indicateur END) OVER (PARTITION BY N{niveau}_C_ENTITE {mcv}, col_name
													             ORDER BY N{niveau}_C_ENTITE {mcv}) AS indicateur_MC
      , SUM(CASE WHEN indic= 'MC' THEN STD END) OVER (PARTITION BY N{niveau}_C_ENTITE{mcv} , col_name
													      ORDER BY N{niveau}_C_ENTITE {mcv}) AS STD_MC
	  {calcul_freq_mp}
      {calcul_indicateur_mp}
	  , SUM(CASE WHEN indic= 'AP' THEN Freq END) OVER (PARTITION BY N{niveau}_C_ENTITE {mcv}, col_name
													       ORDER BY N{niveau}_C_ENTITE {mcv}) AS freq_AP
      , SUM(CASE WHEN indic= 'AP' THEN indicateur END) OVER (PARTITION BY N{niveau}_C_ENTITE {mcv}, col_name
													             ORDER BY N{niveau}_C_ENTITE {mcv}) AS indicateur_AP
	  , SUM(CASE WHEN indic= 'AP' THEN STD END) OVER (PARTITION BY N{niveau}_C_ENTITE {mcv} , col_name
													      ORDER BY N{niveau}_C_ENTITE {mcv}) AS STD_AP
FROM transpose
),
indicateurs_evol as (
SELECT p.*
	  ,CASE WHEN freq_mc IN (0, NULL) OR
	             freq_ap IN (0, NULL) OR
	             (std_MC = 0 AND std_AP = 0)
	        THEN NULL
	        ELSE
                 (indicateur_mc-indicateur_ap) / sqrt(abs((std_MC^2/freq_mc) + (std_AP^2/freq_ap)))
	    END AS evol_indicateur
FROM indicateurs_periodes as p
WHERE indic= 'MC'
)
SELECT d.N{niveau}_C_ENTITE {mcv}, d.col_name
       , d.freq_MC, d.indicateur_MC, d.evol_indicateur
INTO calculsa_N{niveau}_{period}_page9{suffixe}
FROM indicateurs_evol as d ;
DROP TABLE {table_input} ;