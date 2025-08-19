DROP TABLE IF EXISTS calculsa_N{niveau}_{period}_page6{suffixe} ;
WITH cte as (
SELECT indic, N{niveau}_C_ENTITE
       {mcv}
        /* 1.1 Le relationnel */
	    , COUNT(item_00300) AS N_item_00300
		, AVG(item_00300)  AS AVG_item_00300
		, STDDEV(item_00300) AS STD_item_00300
		, COUNT(item_00400) AS N_item_00400
		, AVG(item_00400)  AS AVG_item_00400
		, STDDEV(item_00400) AS STD_item_00400
		, COUNT(item_00500) AS N_item_00500
		, AVG(item_00500)  AS AVG_item_00500
		, STDDEV(item_00500) AS STD_item_00500
		, COUNT(item_00600) AS N_item_00600
		, AVG(item_00600)  AS AVG_item_00600
		, STDDEV(item_00600) AS STD_item_00600
		, COUNT(item_00700) AS N_item_00700
		, AVG(item_00700)  AS AVG_item_00700
		, STDDEV(item_00700) AS STD_item_00700
	  /* 1.2 La réponse  */
	    , COUNT(item_00900) AS N_item_00900
		, AVG(item_00900)  AS AVG_item_00900
		, STDDEV(item_00900) AS STD_item_00900
		, COUNT(item_01000) AS N_item_01000
		, AVG(item_01000)  AS AVG_item_01000
		, STDDEV(item_01000) AS STD_item_01000
		, COUNT(item_01100) AS N_item_01100
		, AVG(item_01100)  AS AVG_item_01100
		, STDDEV(item_01100) AS STD_item_01100
	  /* 1.3 Traitement de la demande */
      , COUNT(inter_item_01300) AS N_item_01300
	  , CASE WHEN COUNT(inter_item_01300) IN (0, NULL) THEN 0
	         ELSE 1.0*SUM(item_01300) / COUNT(inter_item_01300)
	     END AS PCT_item_01300
	  , COUNT(inter_item_01400) AS N_item_01400
	  , CASE WHEN COUNT(inter_item_01400) IN (0, NULL) THEN 0
	         ELSE 1.0*SUM(item_01400) / COUNT(inter_item_01400)
	     END AS PCT_item_01400
	  , COUNT(item_01500) AS N_item_01500
	  , AVG(item_01500)  AS AVG_item_01500
	  , STDDEV(item_01500) AS STD_item_01500
FROM {table_input}
GROUP BY indic, N{niveau}_C_ENTITE {mcv}
),
/* transposition de la table pour faciliter les calculs d'évolution */
transpose as (
SELECT c.N{niveau}_c_entite, c.indic {mcv} ,cjl.*
FROM cte as c
CROSS JOIN LATERAL (
VALUES
(c.n_item_00300, c.std_item_00300, c.avg_item_00300, 'item_00300'),
(c.n_item_00400, c.std_item_00400, c.avg_item_00400, 'item_00400'),
(c.n_item_00500, c.std_item_00500, c.avg_item_00500, 'item_00500'),
(c.n_item_00600, c.std_item_00600, c.avg_item_00600, 'item_00600'),
(c.n_item_00700, c.std_item_00700, c.avg_item_00700, 'item_00700'),
(c.n_item_00900, c.std_item_00900, c.avg_item_00900, 'item_00900'),
(c.n_item_01000, c.std_item_01000, c.avg_item_01000, 'item_01000'),
(c.n_item_01100, c.std_item_01100, c.avg_item_01100, 'item_01100'),
(c.n_item_01300, c.pct_item_01300, c.pct_item_01300, 'item_01300'),
(c.n_item_01400, c.pct_item_01400, c.pct_item_01400, 'item_01400'),
(c.n_item_01500, c.std_item_01500, c.avg_item_01500, 'item_01500')
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
indicateurs_moy_evol as (
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
  AND col_name not in ('item_01300', 'item_01400')
),
indicateurs_pct_evol as (
SELECT p.*
	  ,CASE /* grandeur manquante ou effectif null */
	    WHEN freq_mc IN (0, NULL) OR
	         freq_ap IN (0, NULL) OR
	         indicateur_mc IS NULL OR
	         indicateur_ap IS NULL THEN NULL
	    /* 2 grandeurs null */
	    WHEN indicateur_mc = 0 AND indicateur_ap = 0 THEN NULL
	    /* grandeur mc=0 grandeur ap=1 */
	    WHEN indicateur_mc = 0 AND indicateur_ap = 1 THEN
	         (0.001-0.009) / sqrt(abs( (0.001*(1-0.001)/freq_mc) +
									   (0.009*(1-0.009)/freq_ap) ))
        /* grandeur mc=1 grandeur ap=0 */
	    WHEN indicateur_mc = 0 AND indicateur_ap = 1 THEN
	         (0.009-0.001) / sqrt(abs( (0.009*(1-0.009)/freq_mc) +
									   (0.001*(1-0.001)/freq_ap) ))
	    /* pct et effectifs identiques   */
	    WHEN indicateur_mc = indicateur_ap and freq_mc = freq_ap THEN 0
	   /* pct identiques = 1 et effectifs differents   */
	    WHEN indicateur_mc = 1 and indicateur_ap = 1 and freq_mc <> freq_ap THEN 0
	   /* cas general */
        ELSE (indicateur_mc-indicateur_ap) / sqrt(abs( (indicateur_mc*(1-indicateur_mc)/freq_mc) +
												 (indicateur_ap*(1-indicateur_ap)/freq_ap) ))
  END as evol_indicateur
FROM indicateurs_periodes as p
WHERE indic= 'MC'
  AND col_name in ('item_01300', 'item_01400')
),
indicateurs_evol as (
select *
  from indicateurs_moy_evol
UNION ALL
select *
  from indicateurs_pct_evol
)
SELECT d.N{niveau}_C_ENTITE {mcv}, d.col_name
       , d.freq_MC, d.indicateur_MC, d.evol_indicateur
INTO calculsa_N{niveau}_{period}_page6{suffixe}
FROM indicateurs_evol as d ;
DROP TABLE {table_input} ;