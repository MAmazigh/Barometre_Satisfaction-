# -*- coding: utf-8 -*-
"""
 Function sql_utils.enrich_iterator_with_sql_fragments: enrich SQL query
"""

import pandas as pd
import numpy as np


def enrich_iterator_with_sql_fragments_for_extraction(iterator: pd.DataFrame) -> pd.DataFrame:
    """

    :rtype: object
    """
    # We base our loops on a lookup table
    iterator['flag_mc'] = iterator.apply(
        lambda row: f"WHEN date_reponse BETWEEN '{row['debut_mc']}' AND '{row['fin_mc']}' THEN 'MC'", axis=1)

    iterator['flag_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
        lambda row: f"WHEN date_reponse BETWEEN '{row['debut_mp']}' AND '{row['fin_mp']}' THEN 'MP'", axis=1))

    iterator['flag_ap'] = iterator.apply(
        lambda row: f"WHEN date_reponse BETWEEN '{row['debut_ap']}' AND '{row['fin_ap']}' THEN 'AP'", axis=1)

    iterator['where_mc'] = iterator.apply(
        lambda row: f"date_reponse BETWEEN '{row['debut_mc']}' AND '{row['fin_mc']}' OR ", axis=1)

    iterator['where_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
        lambda row: f"date_reponse BETWEEN '{row['debut_mp']}' AND '{row['fin_mp']}' OR ", axis=1))

    iterator['where_ap'] = iterator.apply(
        lambda row: f"date_reponse BETWEEN '{row['debut_ap']}' AND '{row['fin_ap']}' ", axis=1)

    return iterator
