DROP TABLE IF EXISTS frequency_entity_{period} ;
WITH CTE AS (
SELECT N5_C_ENTITE, N4_C_ENTITE, N3_C_ENTITE
FROM public.satisfaction
WHERE {where_mc}
),
frequency_entity AS (
SELECT DISTINCT
       N5_C_ENTITE
      ,COUNT(N5_C_ENTITE) OVER (PARTITION BY N5_C_ENTITE ORDER BY N5_C_ENTITE) AS Freq_N5
      ,N4_C_ENTITE
      ,COUNT(N4_C_ENTITE) OVER (PARTITION BY N4_C_ENTITE ORDER BY N4_C_ENTITE) AS Freq_N4	  
	  ,N3_C_ENTITE
      ,COUNT(N3_C_ENTITE) OVER (PARTITION BY N3_C_ENTITE ORDER BY N3_C_ENTITE) AS Freq_N3
FROM CTE
)
SELECT N5_C_ENTITE
	 , Freq_N5
	 , CASE WHEN Freq_N5 >= 80 THEN 1
	                           ELSE 0
	   END AS Production_N5
	 , N4_C_ENTITE
	 , Freq_N4
	 , CASE WHEN Freq_N4 >= 80 THEN 1
	                           ELSE 0
	   END AS Production_N4
	 , N3_C_ENTITE
	 , Freq_N3
	 , CASE WHEN Freq_N3 >= 80 THEN 1
	                           ELSE 0
	   END AS Production_N3
INTO frequency_entity_{period}
FROM frequency_entity ;