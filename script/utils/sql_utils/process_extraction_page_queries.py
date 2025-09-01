# -*- coding: utf-8 -*-
"""
 Function sql_utils.process_page_queries: read and execute SQL queries
"""

import pandas as pd
import numpy as np


def process_extraction_page_queries(iterator: pd.DataFrame, page: int, extraction_queries_path: str,
                                    sql_operations) -> None:
    """
    Lit et exécute les requêtes SQL pour une page donnée.

    Args:
        iterator (pd.DataFrame): DataFrame enrichi avec les paramètres.
        page (int): Numéro de la page (entre 2 et 9).
        extraction_queries_path (str): Chemin vers le dossier contenant les fichiers SQL.
        sql_operations: Objet contenant les méthodes read_query et execute_query.
    """
    iterator['query'] = iterator.apply(lambda row: sql_operations.read_query(
        extraction_queries_path, f'extraction_page{page}.sql', format=row.to_dict()), axis=1)

    for query in iterator['query'].dropna():
        sql_operations.execute_query(query)

    if page > 5:
        iterator['query'] = np.where(iterator.niveau == 3, iterator.apply(
            lambda row: sql_operations.read_query(
                extraction_queries_path, f'extraction_page{page}_mcv.sql', format=row.to_dict()), axis=1), '')

        for query in iterator['query']:
            if query:
                sql_operations.execute_query(query)
