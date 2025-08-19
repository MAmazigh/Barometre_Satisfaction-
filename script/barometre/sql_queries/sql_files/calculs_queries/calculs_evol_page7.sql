DROP TABLE IF EXISTS calculsa_N{niveau}_{period}_page7{suffixe} ;
WITH cte as (
SELECT indic, N{niveau}_C_ENTITE
       {mcv}
	    , COUNT(item_01700)  AS N_item_01700
		, AVG(item_01700)    AS AVG_item_01700
		, STDDEV(item_01700) AS STD_item_01700
		, COUNT(item_01800)  AS N_item_01800
		, AVG(item_01800)    AS AVG_item_01800
		, STDDEV(item_01800) AS STD_item_01800
		, COUNT(item_01900)  AS N_item_01900
		, AVG(item_01900)    AS AVG_item_01900
		, STDDEV(item_01900) AS STD_item_01900
		, COUNT(item_02000)  AS N_item_02000
		, AVG(item_02000)    AS AVG_item_02000
		, STDDEV(item_02000) AS STD_item_02000
		, COUNT(item_02100)  AS N_item_02100
		, AVG(item_02100)    AS AVG_item_02100
		, STDDEV(item_02100) AS STD_item_02100
		, COUNT(item_02200)  AS N_item_02200
		, AVG(item_02200)    AS AVG_item_02200
		, STDDEV(item_02200) AS STD_item_02200
		, COUNT(item_02300)  AS N_item_02300
		, AVG(item_02300)    AS AVG_item_02300
		, STDDEV(item_02300) AS STD_item_02300
FROM {table_input}
GROUP BY indic, N{niveau}_C_ENTITE {mcv}
),
/* transposition de la table pour faciliter les calculs d'évolution */
transpose as (
SELECT c.N{niveau}_c_entite, c.indic {mcv} ,cjl.*
FROM cte as c
CROSS JOIN LATERAL (
VALUES
(c.n_item_01700, c.std_item_01700, c.avg_item_01700, 'item_01700'),
(c.n_item_01800, c.std_item_01800, c.avg_item_01800, 'item_01800'),
(c.n_item_01900, c.std_item_01900, c.avg_item_01900, 'item_01900'),
(c.n_item_02000, c.std_item_02000, c.avg_item_02000, 'item_02000'),
(c.n_item_02100, c.std_item_02100, c.avg_item_02100, 'item_02100'),
(c.n_item_02200, c.std_item_02200, c.avg_item_02200, 'item_02200'),
(c.n_item_02300, c.std_item_02300, c.avg_item_02300, 'item_02300')
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
INTO calculsa_N{niveau}_{period}_page7{suffixe}
FROM indicateurs_evol as d ;
DROP TABLE {table_input} ;