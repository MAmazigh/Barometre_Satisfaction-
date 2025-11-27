# -*- coding: utf-8 -*-
"""
 Fonctions de lecture de fichier excel
"""

from typing import Union, List, Dict

import pandas as pd


def read_excel_file(path: str, sheet_name: str = 0, usecols: List[str] = None, dtype: Dict[str, str] = None) -> pd.DataFrame:
    """Lecture de fichier au format excel

    Parameters
    ----------
    :param path : Chemin du fichier à lire
    :param sheet_name: nom de la sheet à lire
    :param usecols: liste des colonnes à extraire
    :param dtype: types des colonnes
    :return pandas dataframe
    """
    try:
        df = pd.read_excel(path, sheet_name=sheet_name, usecols=['liste_items', 'item_calcule'])
        filtre = df.item_calcule == 'Oui'
        df = df[filtre]
        return df
    except Exception as e:
        print(f"Fichier lu au moment de l'exception: {path}, {e}")
