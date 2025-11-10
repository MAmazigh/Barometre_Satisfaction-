# -*- coding: utf-8 -*-
"""
 Function sql_utils.process_page_queries: read and execute SQL queries
"""

import pandas as pd
import numpy as np


def _build_table_input_level(row):
    """Construit la colonne 'table_input_level'."""
    niveau = row['niveau']
    period = row['period']
    page = row['page']
    passage = row['passage']

    if passage in ['02_NC', '04_NCPP']:
        # Utiliser f-string pour une meilleure lisibilité
        return f"format_calculs_n{niveau}_{period}_page{page} as f"
    else:  # passage == '03_NINF'
        niveau_inf = row['niveau_inf']
        return f"format_calculs_n{niveau_inf}_{period}_page{page} as f"


def _build_sql_join(row):
    """Construit la colonne 'join'."""
    passage = row['passage']
    niveau = row['niveau']
    niveau_inf = row['niveau_inf']

    if passage == '02_NC':
        return f" join tab_ref_n{niveau} as t on t.N{niveau}_c_entite = f.entite"
    elif passage == '03_NINF':
        return f" join tab_ref_n{niveau} as t on t.N{niveau_inf}_c_entite = f.entite"
    else:  # '04_NCPP'
        return ''


def _build_short_label_entity(row):
    """Construit la colonne 'short_label_entity'."""
    passage = row['passage']
    niveau = row['niveau']
    niveau_inf = row['niveau_inf']

    if passage == '02_NC':
        return f",t.N{niveau}_lc_entite as short_label_entity"
    elif passage == '03_NINF':
        return f",t.N{niveau_inf}_lc_entite as short_label_entity"
    else:  # '04_NCPP'
        return ', NULL as short_label_entity'


def _build_tri(row):
    """Construit la colonne 'tri'."""
    passage = row['passage']

    if passage == '02_NC':
        return ', 0 as tri'
    elif passage == '03_NINF':
        return ', t.tri',
    else:  # '04_NCPP'
        return ",CASE WHEN f.periode = 'MP' THEN 1 ELSE 2 END AS tri"


def _build_info_(row):
    """Construit la colonne '_info_'."""
    passage = row['passage']

    if passage == '02_NC':
        return "'boldgrey' as _info_"
    elif passage == '03_NINF':
        return 'NULL as _info_'
    else:  # '04_NCPP'
        return "'bold' as _info_"


def _build_bloc(row):
    """Construit la colonne 'bloc'."""
    passage = row['passage']

    if passage == '02_NC':
        return ', 0 as bloc'
    elif passage == '03_NINF':
        return ', 1 as bloc'
    else:  # '04_NCPP'
        return ', 2 as bloc'


def _build_insert_before(row):
    """Construit la colonne 'insert_before'."""
    passage = row['passage']

    if passage == '02_NC':
        return ', NULL as insert_before'
    elif passage == '03_NINF':
        return ", case when t.tri = 1 then 'linebreak' else NULL end as insert_before"
    else:  # '04_NCPP'
        return ", case when f.periode = 'MP' then 'linebreak' else NULL end as insert_before"


# Avantage : Chaque règle de construction de chaîne est isolée et peut être testée individuellement.
# La fonction enrich_iterator_level5 est maintenant une orchestratrice qui applique ces règles
def enrich_iterator_restitution_level5(df_iterator_nsup: pd.DataFrame) -> pd.DataFrame:
    df = df_iterator_nsup.copy()

    # Dérivations simples
    df['niveau_sup'] = np.where(df['niveau'] == 5, '', df['niveau'] + 1)
    df['niveau_inf'] = df['niveau'] - 1
    df['distinct'] = np.where(df['passage'] == '02_NC', 'DISTINCT', '')

    # Dérivations complexes (Extraction des règles dans des Helpers)
    # L'utilisation de apply est plus lisible ici que les multiples np.select pour les chaînes.
    df['table_input_level'] = df.apply(_build_table_input_level, axis=1)
    df['join'] = df.apply(_build_sql_join, axis=1)
    df['short_label_entity'] = df.apply(_build_short_label_entity, axis=1)
    df['tri'] = df.apply(_build_tri, axis=1)
    df['_info_'] = df.apply(_build_info_, axis=1)
    df['bloc'] = df.apply(_build_bloc, axis=1)
    df['insert_before'] = df.apply(_build_insert_before, axis=1)

    # Retour à des derivations simples de chaînes (pas besoin de helper pour les cas simples)
    df['entite'] = np.where(
        df['passage'] == '03_NINF',
        " t.N" + df['niveau'].astype(str) + "_c_entite as entite",
        'f.entite'
    )

    df['where'] = np.where(df['passage'] == '04_NCPP', "where f.periode <> 'MC'", "where f.periode = 'MC'")

    # Utilisation d'un dictionnaire de mapping
    list_var_map = {
        2: " ,f.var_1, f.var_2, f.var_3, f.var_4, f.var_5 ",
        3: " ,f.var_1, f.var_2, f.var_3, f.var_4 ",
        4: " ,f.var_1, f.var_2, f.var_3, f.var_4, f.var_5, f.var_6 ",
        5: " ,f.var_1, f.var_2, f.var_3, f.var_4, f.var_5, f.var_6, f.var_7, f.var_8, f.var_9 "
    }
    df['list_var'] = df['page'].map(list_var_map)
    df['var_niveau'] = ',f.niveau'
    df['tableau_part'] = ", '" + df['tableau'] + "' as tableau "
    df['table_results_level'] = "N" + df['niveau'].astype(str) + "_" + df['period'] + "_page" + df['page'].astype(
        str) + "_tableau_" + df['tableau']

    return df


