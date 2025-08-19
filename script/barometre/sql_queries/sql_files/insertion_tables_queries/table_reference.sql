DROP TABLE IF EXISTS tab_ref_N{niveau} ;
WITH CTE AS (
SELECT DISTINCT
       N{niveau}_C_ENTITE
     , N{niveau}_L_ENTITE
	 , N{niveau}_LC_ENTITE
     , N{niveau_inf}_C_ENTITE
     , N{niveau_inf}_L_ENTITE
	 , N{niveau_inf}_LC_ENTITE
     , '{niveau_inf}' as niveau_inferieur
FROM public.structure
WHERE N{niveau}_rapport = '1'
  AND N{niveau_inf}_niv_supp = '1'
)
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY N{niveau}_C_ENTITE ORDER BY N{niveau_inf}_LC_ENTITE) AS tri
INTO tab_ref_N{niveau}
FROM CTE ;