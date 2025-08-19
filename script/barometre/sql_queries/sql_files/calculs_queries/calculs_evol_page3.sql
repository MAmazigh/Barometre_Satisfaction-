DROP TABLE IF EXISTS calculs_N{niveau}_{period}_page3_a ;
WITH CTE as (
select indic, N{niveau}_C_ENTITE
	   ,satisfaction_contact
	  ,COUNT(satisfaction_contact) AS Freq
	  ,SUM(COUNT(satisfaction_contact)) OVER (PARTITION BY indic, N{niveau}_C_ENTITE) AS Freq_Total
from public.extraction_N{niveau}_{period}_page3
group by indic, N{niveau}_C_ENTITE, satisfaction_contact
),
calculs_page3 as (
SELECT indic, N{niveau}_C_ENTITE, satisfaction_contact, Freq, Freq_Total
     , CAST(1.0*Freq/Freq_Total AS FLOAT) AS indicateur
FROM CTE
),
indicateurs_page3 as (
SELECT indic,N{niveau}_C_ENTITE, satisfaction_contact,
       Freq, Freq_Total, indicateur
	   ,SUM(CASE WHEN indic= 'MC' THEN freq END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS freq_MC
       ,SUM(CASE WHEN indic= 'MC' THEN indicateur END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS indicateur_MC
	   {calcul_freq_mp}
       {calcul_indicateur_mp}
       ,SUM(CASE WHEN indic= 'AP' THEN freq END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS freq_AP
       ,SUM(CASE WHEN indic= 'AP' THEN indicateur END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS indicateur_AP
from calculs_page3
),
indicateurs_evol_page3 as (
SELECT p.*
	     /* formule évolution pct (mc-ap) / sqrt(abs( mc*(1-mc)/N_mc + ap*(1-ap)/N_ap)) */
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
FROM indicateurs_page3 as p
WHERE indic= 'MC'
)
SELECT d.indic, d.N{niveau}_C_ENTITE, d.satisfaction_contact,
       d.Freq, d.Freq_Total, d.indicateur
     , d.freq_MC {freq_mp} ,d.freq_AP
     , d.indicateur_MC {indicateur_mp} , d.indicateur_AP
	   /* inversion du raisonnement des évolutions pour les insatisfaits */
	 , CASE WHEN d.satisfaction_contact = 'VAR_4' THEN -1.0*d.evol_indicateur
	                                                     ELSE d.evol_indicateur
	    END AS evol_indicateur
INTO calculs_N{niveau}_{period}_page3_a
FROM indicateurs_evol_page3 as d
;
/* Calculs de la moyenne avec son evolution */
DROP TABLE IF EXISTS calculs_N{niveau}_{period}_page3_b ;
WITH satisfaction as (
SELECT
       indic, N{niveau}_C_ENTITE
      ,'VAR_1' AS satisfaction_contact
	  ,COUNT(satisfaction_contact) AS Freq
	  ,COUNT(satisfaction_contact) AS Freq_Total
	  ,AVG(satis_glob_cont) as indicateur
	  ,STDDEV(satis_glob_cont) as std
FROM public.extraction_N{niveau}_{period}_page3
GROUP BY indic, N{niveau}_C_ENTITE
),
indicateurs_periodes as (
SELECT indic, N{niveau}_C_ENTITE, satisfaction_contact,
       Freq, Freq_Total, indicateur
	   ,SUM(CASE WHEN indic= 'MC' THEN freq END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS freq_MC
	   ,SUM(CASE WHEN indic= 'MC' THEN std END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS std_MC
	   ,SUM(CASE WHEN indic= 'MC' THEN indicateur END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS indicateur_MC
	   {calcul_freq_mp}
       {calcul_indicateur_mp}
       ,SUM(CASE WHEN indic= 'AP' THEN freq END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS freq_AP
	   ,SUM(CASE WHEN indic= 'AP' THEN std END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS std_AP
       ,SUM(CASE WHEN indic= 'AP' THEN indicateur END) OVER (partition by N{niveau}_C_ENTITE, satisfaction_contact
													  ORDER BY N{niveau}_C_ENTITE, satisfaction_contact) AS indicateur_AP
from satisfaction
),
indicateurs_evol_page3 as (
SELECT p.*
	     /* formule évolution moyenne (mc-ap) / sqrt(abs( std_mc**2/N_mc + std_ap**2/N_ap)) */
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
SELECT d.indic, d.N{niveau}_C_ENTITE, d.satisfaction_contact,
       d.Freq, d.Freq_Total, d.indicateur
     , d.freq_MC {freq_mp} ,d.freq_AP
     , d.indicateur_MC {indicateur_mp} , d.indicateur_AP, d.evol_indicateur
INTO calculs_N{niveau}_{period}_page3_b
FROM indicateurs_evol_page3 as d
;
DROP TABLE IF EXISTS calculs_N{niveau}_{period}_page3 ;
SELECT *
  INTO calculs_N{niveau}_{period}_page3
  FROM calculs_N{niveau}_{period}_page3_a
UNION ALL
SELECT *
  FROM calculs_N{niveau}_{period}_page3_b
ORDER BY N{niveau}_C_ENTITE, satisfaction_contact ;
DROP TABLE calculs_N{niveau}_{period}_page3_a, calculs_N{niveau}_{period}_page3_b
          ,extraction_N{niveau}_{period}_page3 ;