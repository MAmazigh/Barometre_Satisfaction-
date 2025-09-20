# -*- coding: utf-8 -*-
"""
 Function sql_utils.enrich_iterator_with_sql_fragments_for_calculs: enrich SQL query
"""

import pandas as pd
import numpy as np


def enrich_iterator_with_sql_fragments_for_calculs(df: pd.DataFrame, seuil: int) -> pd.DataFrame:
    """

    :rtype: object
    """
    # We base our loops on a lookup table
    # Ajout des indicateurs temporels (MC, MP, AP)
    df_indic = pd.DataFrame({'key': 0, 'indic': ['MC', 'MP', 'AP']})
    iterator = pd.merge(df, df_indic, on='key', how='outer')

    # Suppression de MP si la période est Y ou P
    mask = iterator.period.isin(['Y', 'P']) & (iterator.indic == 'MP')
    iterator = iterator[~mask]

    # Enrichissement des colonnes SQL dynamiques
    iterator['freq'] = iterator['indic'].map(lambda x: f"freq_{x}")
    iterator['threshold_NI_NS'] = seuil
    iterator['indicateur'] = iterator['indic'].map(lambda x: f"indicateur_{x}")
    iterator['fmt_evol'] = np.where(iterator.indic.isin(['MP', 'AP']), '', (
        ", CASE WHEN evol_indicateur < -1.96 THEN '-' "
        "WHEN evol_indicateur BETWEEN -1.96 AND 1.96 THEN '=' "
        "WHEN evol_indicateur > 1.96 THEN '+' END as valeurs_e"
    ))
    iterator['kpi'] = np.where(iterator.indic.isin(['MP', 'AP']), ', kpi',
                               ", CASE WHEN kpi in ('NI', 'NS') THEN kpi ELSE kpi||'|'||valeurs_e END as kpi")
    iterator['flag_periode'] = iterator['indic'].map(lambda x: f", '{x}' AS periode")

    return iterator
