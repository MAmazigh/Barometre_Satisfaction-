# -*- coding: utf-8 -*-
"""
Fonction qui permet de récupérer la liste des périodicités à produire:
Elle se base sur le mois de production courant et renvoie une liste
"""

import pandas as pd
import numpy as np
from typing import List, Dict
from operations_dates import get_range_periodes


def get_liste_periodicite(periode: str) -> List:
    # Mois courant
    # les 2 derniers caractères du paramètre periode (yyyymm) correspondent au mois courant
    mois = int(periode[-2:])

    df_periodicite = pd.DataFrame(range(1, 13), columns=['month_num'])
    df_periodicite['month'] = 1
    df_periodicite['quarter'] = np.where(df_periodicite['month_num'] % 3 == 0, 1, 0)
    df_periodicite['semiyear'] = np.where(df_periodicite['month_num'] % 6 == 0, 1, 0)
    df_periodicite['period_specific'] = np.where(df_periodicite['month_num'] % 9 == 0, 1, 0)
    df_periodicite['year'] = np.where(df_periodicite['month_num'] % 12 == 0, 1, 0)
    df_periodicite.set_index('month_num', inplace=True)
    # on applique un filtre booléen sur le mois en cours pour récupérer en liste les periodicités à produire
    filtre_production = df_periodicite.loc[mois] > 0
    serie_periodicite = df_periodicite.loc[mois][filtre_production]
    periodicites = serie_periodicite.index.to_list()
    liste_periodicite = [x[0].upper() for x in periodicites]

    return liste_periodicite


def get_dict_periodicite(periode: str, list_period: List) -> Dict:
    return {p: get_range_periodes(periode, f'{p}') for p in list_period}
