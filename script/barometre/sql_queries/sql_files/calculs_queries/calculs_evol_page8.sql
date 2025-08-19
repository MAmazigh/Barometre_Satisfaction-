DROP TABLE IF EXISTS calculsa_N{niveau}_{period}_page8{suffixe} ;
WITH cte as (
SELECT indic, N{niveau}_C_ENTITE
       {mcv}
       /* chapter 3.1 Agency */
	    , COUNT(item_02600) AS N_item_02600
		, AVG(item_02600)  AS AVG_item_02600
		, STDDEV(item_02600) AS STD_item_02600
		, COUNT(item_02700) AS N_item_02700
		, AVG(item_02700)  AS AVG_item_02700
		, STDDEV(item_02700) AS STD_item_02700
		, COUNT(item_02800) AS N_item_02800
		, AVG(item_02800)  AS AVG_item_02800
		, STDDEV(item_02800) AS STD_item_02800
		, COUNT(item_02900) AS N_item_02900
		, AVG(item_02900)  AS AVG_item_02900
		, STDDEV(item_02900) AS STD_item_02900
		, COUNT(item_03000) AS N_item_03000
		, AVG(item_03000)  AS AVG_item_03000
		, STDDEV(item_03000) AS STD_item_03000
	  /* chapter 3.2 Voice server */
	    , COUNT(item_03200) AS N_item_03200
		, AVG(item_03200)  AS AVG_item_03200
		, STDDEV(item_03200) AS STD_item_03200
		, COUNT(item_03300) AS N_item_03300
		, AVG(item_03300)  AS AVG_item_03300
		, STDDEV(item_03300) AS STD_item_03300
		, COUNT(item_03400) AS N_item_03400
		, AVG(item_03400)  AS AVG_item_03400
		, STDDEV(item_03400) AS STD_item_03400
	  /* chapter 3.3 Outgoing call including call back */
        , COUNT(inter_item_03700) AS N_item_03700
	    , CASE WHEN COUNT(inter_item_03700) IN (0, NULL) THEN 0
	           ELSE 1.0*SUM(item_03700) / COUNT(inter_item_03700)
	       END AS PCT_item_03700
	    , COUNT(item_03800) AS N_item_03800
	    , AVG(item_03800)  AS AVG_item_03800
	    , STDDEV(item_03800) AS STD_item_03800
        , COUNT(item_03900) AS N_item_03900
	    , AVG(item_03900)  AS AVG_item_03900
	    , STDDEV(item_03900) AS STD_item_03900
	    , COUNT(item_03910) AS N_item_03910
	    , CASE WHEN COUNT(item_03910) IN (0, NULL) THEN 0
	           ELSE 1.0*SUM(item_03910) / COUNT(item_03910)
	       END AS PCT_item_03910
     /* chapter 3.4 Reason for the inconvenience */
        , COUNT(item_03930) as N_item_03930
	    , CASE WHEN COUNT(item_03930) IN (0, NULL) THEN 0
	           ELSE 1.0*SUM(item_03930) / COUNT(item_03930)
	       END AS PCT_item_03930
	    , COUNT(item_03940) as N_item_03940
        , CASE WHEN COUNT(item_03940) IN (0, NULL) THEN 0
	           ELSE 1.0*SUM(item_03940) / COUNT(item_03940)
	       END AS PCT_item_03940
	    , COUNT(item_03950) as N_item_03950
        , CASE WHEN COUNT(item_03950) IN (0, NULL) THEN 0
	           ELSE 1.0*SUM(item_03950) / COUNT(item_03950)
	       END AS PCT_item_03950
	    , COUNT(item_03960) as N_item_03960
        , CASE WHEN COUNT(item_03960) IN (0, NULL) THEN 0
	           ELSE 1.0*SUM(item_03960) / COUNT(item_03960)
	       END AS PCT_item_03960
	    , COUNT(item_03970) as N_item_03970
        , CASE WHEN COUNT(item_03970) IN (0, NULL) THEN 0
	           ELSE 1.0*SUM(item_03970) / COUNT(item_03970)
	       END AS PCT_item_03970
FROM {table_input}
GROUP BY indic, N{niveau}_C_ENTITE {mcv}
),
/* transposition de la table pour faciliter les calculs d'évolution */
transpose as (
SELECT c.N{niveau}_c_entite, c.indic {mcv} ,cjl.*
FROM cte as c
CROSS JOIN LATERAL (
VALUES
(c.n_item_02600, c.std_item_02600, c.avg_item_02600, 'item_02600'),
(c.n_item_02700, c.std_item_02700, c.avg_item_02700, 'item_02700'),
(c.n_item_02800, c.std_item_02800, c.avg_item_02800, 'item_02800'),
(c.n_item_02900, c.std_item_02900, c.avg_item_02900, 'item_02900'),
(c.n_item_03000, c.std_item_03000, c.avg_item_03000, 'item_03000'),
(c.n_item_03200, c.std_item_03200, c.avg_item_03200, 'item_03200'),
(c.n_item_03300, c.std_item_03300, c.avg_item_03300, 'item_03300'),
(c.n_item_03400, c.std_item_03400, c.avg_item_03400, 'item_03400'),
(c.n_item_03700, c.pct_item_03700, c.pct_item_03700, 'item_03700'),
(c.n_item_03800, c.std_item_03800, c.avg_item_03800, 'item_03800'),
(c.n_item_03900, c.std_item_03900, c.avg_item_03900, 'item_03900'),
(c.n_item_03910, c.pct_item_03910, c.pct_item_03910, 'item_03910'),
(c.n_item_03930, c.pct_item_03930, c.pct_item_03930, 'item_03930'),
(c.n_item_03940, c.pct_item_03940, c.pct_item_03940, 'item_03940'),
(c.n_item_03950, c.pct_item_03950, c.pct_item_03950, 'item_03950'),
(c.n_item_03960, c.pct_item_03960, c.pct_item_03960, 'item_03960'),
(c.n_item_03970, c.pct_item_03970, c.pct_item_03970, 'item_03970')

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
  AND col_name not in ('item_03700', 'item_03910', 'item_03930', 'item_03940', 'item_03950', 'item_03960', 'item_03970')
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
  AND col_name in ('item_03700', 'item_03910', 'item_03930', 'item_03940', 'item_03950', 'item_03960', 'item_03970')
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
INTO calculsa_N{niveau}_{period}_page8{suffixe}
FROM indicateurs_evol as d ;
DROP TABLE {table_input} ;