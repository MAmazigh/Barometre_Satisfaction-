# -*- coding: utf-8 -*-
"""
Fonction qui permet de récupérer la liste des périodicités à produire:
Elle se base sur le mois de production courant et renvoie une liste
"""

import pandas as pd
import numpy as np
from typing import List, Dict
from operations_dates import get_range_periodes


def get_liste_periodicite(periode: str) -> List[str]:
    """
    Génère une liste de périodicités à produire en fonction du mois extrait du paramètre 'periode' (format 'yyyymm').

    Args:
        periode (str): Période au format 'yyyymm'.

    Returns:
        List[str]: Liste des périodicités à produire (ex: ['MONTH', 'QUARTER']).
    """
    # Extraction du mois courant
    mois = int(periode[-2:])

    # Définition des règles de périodicité
    periodicite_rules = {
        'MONTH': lambda m: True,
        'QUARTER': lambda m: m % 3 == 0,
        'SEMIYEAR': lambda m: m % 6 == 0,
        'PERIOD_SPECIFIC': lambda m: m % 9 == 0,
        'YEAR': lambda m: m % 12 == 0
    }

    # Application des règles
    liste_periodicite = [name[0] for name, rule in periodicite_rules.items() if rule(mois)]

    return liste_periodicite


def get_dict_periodicite(periode: str, list_period: List) -> Dict:
    return {p: get_range_periodes(periode, f'{p}') for p in list_period}
