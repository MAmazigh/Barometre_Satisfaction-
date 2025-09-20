# -*- coding: utf-8 -*-
"""
 Function sql_utils.process_page_queries: read and execute SQL queries
"""

import pandas as pd


def process_extraction_page_queries(iterator: pd.DataFrame, page: int, extraction_queries_path: str,
                                    sql_operations) -> None:
    """
    Lit et exécute les requêtes SQL pour une page donnée.

    Args:
        iterator (pd.DataFrame): DataFrame enrichi avec les paramètres.
        page (int): Numéro de la page (entre 2 et 9).
        extraction_queries_path (str): Chemin vers le dossier contenant les fichiers SQL.
        sql_operations: Objet contenant les méthodes read_query_blocks et execute_queries.
    """

    for _, row in iterator.iterrows():
        format_dict = row.to_dict()

        # Extraction principale
        queries = sql_operations.read_query_blocks(extraction_queries_path, f'extraction_page{page}.sql',
                                                   format=format_dict)
        sql_operations.execute_queries(queries)

        # Extraction complémentaire pour pages 6 à 9 et niveau 3
        if page > 5 and row['niveau'] == 3:
            queries_mcv = sql_operations.read_query_blocks(extraction_queries_path, f'extraction_page{page}_mcv.sql',
                                                           format=format_dict)
            sql_operations.execute_queries(queries_mcv)

