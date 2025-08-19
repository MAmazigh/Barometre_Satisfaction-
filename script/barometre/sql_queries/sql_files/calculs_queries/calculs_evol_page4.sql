DROP TABLE IF EXISTS calculs_N{niveau}_{period}_page4 ;
WITH indicateurs_q91 AS (
SELECT indic, N{niveau}_C_ENTITE, Q91
      ,COUNT(Q91) AS Freq
	  ,SUM(COUNT(Q91)) OVER (PARTITION BY indic, N{niveau}_C_ENTITE) AS Freq_Total
	  ,COUNT(Q91) / SUM(COUNT(Q91)) OVER (PARTITION BY indic, N{niveau}_C_ENTITE) AS indicateur
  FROM public.extraction_N{niveau}_{period}_page4
 GROUP BY indic, N{niveau}_C_ENTITE, Q91
),
indicateurs_q90 AS (
SELECT indic, N{niveau}_C_ENTITE, Q90
      ,COUNT(Q90) AS Freq
	  ,SUM(COUNT(Q90)) OVER (PARTITION BY indic, N{niveau}_C_ENTITE) AS Freq_Total
	  ,COUNT(Q90) / SUM(COUNT(Q90)) OVER (PARTITION BY indic, N{niveau}_C_ENTITE) AS indicateur
  FROM public.extraction_N{niveau}_{period}_page4
 GROUP BY indic, N{niveau}_C_ENTITE, Q90
),
indicateurs as (
SELECT indic, N{niveau}_C_ENTITE, Q91 as prestation_impact, Freq, Freq_Total, indicateur
  FROM indicateurs_q91
UNION ALL
SELECT indic, N{niveau}_C_ENTITE, Q90 as prestation_impact, Freq, Freq_Total, indicateur
  FROM indicateurs_q90
),
indicateurs_periodes as (
SELECT indic,N{niveau}_C_ENTITE, prestation_impact,
       Freq, Freq_Total, indicateur
	   ,SUM(CASE WHEN indic= 'MC' THEN freq END) OVER (partition by N{niveau}_C_ENTITE, prestation_impact
													  ORDER BY N{niveau}_C_ENTITE, prestation_impact) AS freq_MC
	   ,SUM(CASE WHEN indic= 'MC' THEN indicateur END) OVER (partition by N{niveau}_C_ENTITE, prestation_impact
													  ORDER BY N{niveau}_C_ENTITE, prestation_impact) AS indicateur_MC
	   {calcul_freq_mp}
       {calcul_indicateur_mp}
       ,SUM(CASE WHEN indic= 'AP' THEN freq END) OVER (partition by N{niveau}_C_ENTITE, prestation_impact
													  ORDER BY N{niveau}_C_ENTITE, prestation_impact) AS freq_AP
       ,SUM(CASE WHEN indic= 'AP' THEN indicateur END) OVER (partition by N{niveau}_C_ENTITE, prestation_impact
													  ORDER BY N{niveau}_C_ENTITE, prestation_impact) AS indicateur_AP
FROM indicateurs
),
indicateurs_evol as (
SELECT p.*
	     /* formule évolution (mc-ap) / sqrt(abs( mc*(1-mc)/N_mc + ap*(1-ap)/N_ap)) */
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
)
SELECT d.indic, d.N{niveau}_C_ENTITE, d.prestation_impact,
       d.Freq, d.Freq_Total, d.indicateur
     , d.freq_MC {freq_mp} ,d.freq_AP
     , d.indicateur_MC {indicateur_mp} , d.indicateur_AP
	   /* inversion du raisonnement des évolutions pour les colonnes connotées négatives */
	 , CASE WHEN d.prestation_impact in ('VAR_3', 'VAR_6') THEN -1.0*d.evol_indicateur
	                                                       ELSE d.evol_indicateur
	    END AS evol_indicateur
INTO calculs_N{niveau}_{period}_page4
FROM indicateurs_evol as d;
DROP TABLE extraction_N{niveau}_{period}_page4 ;
