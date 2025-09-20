DROP TABLE IF EXISTS calculs_N{niveau}_{period}_page2 ;
WITH CTE as (
SELECT indic, N{niveau}_C_ENTITE, Q1
	  ,COUNT(Q1) AS Freq 
	  ,SUM(COUNT(Q1)) OVER (PARTITION BY indic, N{niveau}_C_ENTITE) AS Freq_Total 
FROM extraction_N{niveau}_{period}_page2
GROUP BY indic, N{niveau}_C_ENTITE, Q1
),
calculs_n as (
SELECT indic, N{niveau}_C_ENTITE, Q1, Freq, Freq_Total
     , CAST(1.0*Freq/Freq_Total AS FLOAT) AS indicateur
FROM CTE
),
nps as (
SELECT DISTINCT
       indic, N{niveau}_C_ENTITE 
      ,'VAR_2' AS Q1 /* NPS */
	  ,MAX(Freq_Total) as Freq
	  ,MAX(Freq_Total) as Freq_Total
      ,(SUM(CASE WHEN Q1= 'VAR_3' THEN MAX(indicateur) END) OVER (PARTITION BY indic, N{niveau}_C_ENTITE
				                                                      ORDER BY indic, N{niveau}_C_ENTITE) -
	    SUM(CASE WHEN Q1= 'VAR_5' THEN MAX(indicateur) END) OVER (PARTITION BY indic, N{niveau}_C_ENTITE
				                                                      ORDER BY indic, N{niveau}_C_ENTITE)
	   ) AS indicateur
FROM calculs_n
GROUP BY indic, N{niveau}_C_ENTITE, Q1
),
promotneutdetnps as (
SELECT *
  FROM calculs_n
UNION ALL 
SELECT *
  FROM nps
),
indicateurs_periodes as (
SELECT indic,N{niveau}_C_ENTITE, Q1,
       Freq, Freq_Total, indicateur
	   ,SUM(CASE WHEN indic= 'MC' THEN freq END) OVER (partition by N{niveau}_C_ENTITE, Q1 
													       ORDER BY N{niveau}_C_ENTITE, Q1) AS freq_MC
       ,SUM(CASE WHEN indic= 'MC' THEN indicateur END) OVER (partition by N{niveau}_C_ENTITE, Q1
													                  ORDER BY N{niveau}_C_ENTITE, Q1) AS indicateur_MC
	   {calcul_freq_mp}
       {calcul_indicateur_mp}
       ,SUM(CASE WHEN indic= 'AP' THEN freq END) OVER (partition by N{niveau}_C_ENTITE, Q1 
													  ORDER BY N{niveau}_C_ENTITE, Q1) AS freq_AP
       ,SUM(CASE WHEN indic= 'AP' THEN indicateur END) OVER (partition by N{niveau}_C_ENTITE, Q1
													  ORDER BY N{niveau}_C_ENTITE, Q1) AS indicateur_AP
from promotneutdetnps
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
SELECT d.indic, d.N{niveau}_C_ENTITE, d.Q1,
       d.Freq, d.Freq_Total, d.indicateur
       , d.freq_MC {freq_mp} , d.freq_AP
       , d.indicateur_MC {indicateur_mp}, d.indicateur_AP
	   /* inversion du raisonnement des évolutions pour les déctracteurs */
	 , CASE WHEN d.Q1 = 'VAR_5' THEN -1.0*d.evol_indicateur
	                            ELSE d.evol_indicateur
	    END AS evol_indicateur
INTO calculs_N{niveau}_{period}_page2
FROM indicateurs_evol as d ;
DROP TABLE extraction_N{niveau}_{period}_page2 ;



