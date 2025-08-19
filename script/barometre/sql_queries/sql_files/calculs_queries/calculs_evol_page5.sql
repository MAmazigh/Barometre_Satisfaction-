DROP TABLE IF EXISTS calculs_N{niveau}_{period}_page5 ;
WITH calculs_modalites_periode as (
SELECT indic, N{niveau}_C_ENTITE
       /* mode_contact_valide takes 8 categories: 1 to 8 here we add the category global: 0 */
      , CASE WHEN GROUPING(mode_contact_valide)  = 1 THEN 'VAR_0'
	                                                 ELSE 'VAR_'||TRIM(BOTH FROM mode_contact_valide)
	     END AS mode_contact_valide
      , COUNT(satis_glob_cont) AS Freq
	  , AVG(satis_glob_cont)   AS indicateur
	  , STDDEV(satis_glob_cont) AS STD
FROM extraction_N{niveau}_{period}_page5
GROUP BY GROUPING SETS((indic, N{niveau}_C_ENTITE), /* add the Global category */
					   (indic, N{niveau}_C_ENTITE, mode_contact_valide))
),
indicateurs_periodes as (
SELECT indic, N{niveau}_C_ENTITE
      , mode_contact_valide
	  , SUM(CASE WHEN indic= 'MC' THEN Freq END) OVER (PARTITION BY N{niveau}_C_ENTITE, mode_contact_valide
													   ORDER BY N{niveau}_C_ENTITE) AS freq_MC
      , SUM(CASE WHEN indic= 'MC' THEN indicateur END) OVER (PARTITION BY N{niveau}_C_ENTITE, mode_contact_valide
													         ORDER BY N{niveau}_C_ENTITE) AS indicateur_MC
      , SUM(CASE WHEN indic= 'MC' THEN STD END) OVER (PARTITION BY N{niveau}_C_ENTITE, mode_contact_valide
													  ORDER BY N{niveau}_C_ENTITE) AS STD_MC
	  {calcul_freq_mp}
      {calcul_indicateur_mp}
	  , SUM(CASE WHEN indic= 'AP' THEN Freq END) OVER (PARTITION BY N{niveau}_C_ENTITE, mode_contact_valide
													   ORDER BY N{niveau}_C_ENTITE) AS freq_AP
      , SUM(CASE WHEN indic= 'AP' THEN indicateur END) OVER (PARTITION BY N{niveau}_C_ENTITE, mode_contact_valide
													         ORDER BY N{niveau}_C_ENTITE) AS indicateur_AP
	  , SUM(CASE WHEN indic= 'AP' THEN STD END) OVER (PARTITION BY N{niveau}_C_ENTITE, mode_contact_valide
													  ORDER BY N{niveau}_C_ENTITE) AS STD_AP
FROM calculs_modalites_periode
),
indicateurs_evol as (
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
SELECT d.N{niveau}_C_ENTITE, d.mode_contact_valide
      , d.freq_MC {freq_mp} ,d.freq_AP
     , d.indicateur_MC {indicateur_mp} , d.indicateur_AP, d.evol_indicateur
INTO calculs_N{niveau}_{period}_page5
FROM indicateurs_evol as d ;
DROP TABLE extraction_N{niveau}_{period}_page5 ;